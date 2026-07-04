use super::*;
use crate::{CatalogInfo, ColumnInfo, TableInfo};

fn star_test_graph() -> Declaration {
    Declaration::from_yaml(
        r"
version: 1
name: star_test
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
  - label: Team
    table: { schema: ops, name: teams }
    key: id
    properties:
      name: team_name
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
",
    )
    .expect("star test graph should parse")
}

fn staged_planning_test_graph() -> Declaration {
    Declaration::from_yaml(
        r"
version: 1
name: staged_planning_test
nodes:
  - label: Person
    table: { schema: ops, name: people }
    key: id
    properties:
      name: full_name
      age: age
  - label: Service
    table: { schema: ops, name: services }
    key: id
    properties:
      name: service_name
relationships:
  - type: KNOWS
    table: { schema: ops, name: knows }
    from: { label: Person, key: person_id }
    to: { label: Person, key: friend_id }
  - type: LIKES
    table: { schema: ops, name: likes }
    from: { label: Person, key: person_id }
    to: { label: Person, key: liked_person_id }
  - type: OWNS
    table: { schema: ops, name: ownerships }
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
",
    )
    .expect("staged planning test graph should parse")
}

fn single_label_person_knows_test_graph() -> Declaration {
    Declaration::from_yaml(
        r"
version: 1
name: single_label_person_knows_test
nodes:
  - label: Person
    table: { schema: ops, name: people }
    key: id
    properties:
      name: full_name
      age: age
relationships:
  - type: KNOWS
    table: { schema: ops, name: knows }
    from: { label: Person, key: person_id }
    to: { label: Person, key: friend_id }
",
    )
    .expect("single-label Person/KNOWS test graph should parse")
}

fn temporal_columns_test_graph() -> Declaration {
    Declaration::from_yaml(
        r"
version: 1
name: temporal_columns_test
nodes:
  - label: Person
    table: { schema: rich, name: people }
    key: id
    properties:
      name: name
      joined: joined
      birthday: birthday
",
    )
    .expect("temporal columns test graph should parse")
}

fn temporal_columns_catalog() -> CatalogInfo {
    CatalogInfo {
        tables: vec![TableInfo {
            schema_name: "rich".to_string(),
            table_name: "people".to_string(),
            description: String::new(),
            guide: String::new(),
            columns: [
                ("id", "Int64"),
                ("name", "Utf8"),
                ("joined", "Timestamp"),
                ("birthday", "Date"),
            ]
            .into_iter()
            .enumerate()
            .map(|(position, (name, data_type))| ColumnInfo {
                name: name.to_string(),
                data_type: data_type.to_string(),
                nullable: true,
                is_virtual: false,
                is_required_filter: false,
                description: String::new(),
                ordinal_position: u32::try_from(position).unwrap_or(u32::MAX),
            })
            .collect(),
            required_filters: Vec::new(),
        }],
        table_functions: Vec::new(),
    }
}

fn typed_float_list_projection(alias: &str, values: Vec<f64>) -> Projection {
    Projection::Expression {
        expression: ScalarExpression::TypedLiteralList {
            literals: values
                .into_iter()
                .map(|value| Literal::Float(OrderedFloat(value)))
                .collect(),
            element_type: LiteralListElementType::Float,
        },
        alias: alias.to_string(),
    }
}

fn date_expression(year: i64, month: i64, day: i64) -> ScalarExpression {
    ScalarExpression::Temporal(TemporalExpr::MakeDate {
        year: Box::new(ScalarExpression::Literal(Literal::Integer(year))),
        month: Box::new(ScalarExpression::Literal(Literal::Integer(month))),
        day: Box::new(ScalarExpression::Literal(Literal::Integer(day))),
    })
}

fn date_from_string_expression(text: &str) -> ScalarExpression {
    ScalarExpression::Temporal(TemporalExpr::DateFromString {
        text: Box::new(ScalarExpression::Literal(Literal::String(text.to_string()))),
    })
}

#[expect(
    clippy::too_many_arguments,
    reason = "Test helper mirrors openCypher localdatetime fields."
)]
fn localdatetime_expression(
    year: i64,
    month: i64,
    day: i64,
    hour: i64,
    minute: i64,
    second: i64,
    millisecond: i64,
    microsecond: i64,
    nanosecond: i64,
) -> ScalarExpression {
    ScalarExpression::Temporal(TemporalExpr::MakeLocalDateTime {
        year: Box::new(ScalarExpression::Literal(Literal::Integer(year))),
        month: Box::new(ScalarExpression::Literal(Literal::Integer(month))),
        day: Box::new(ScalarExpression::Literal(Literal::Integer(day))),
        hour: Box::new(ScalarExpression::Literal(Literal::Integer(hour))),
        minute: Box::new(ScalarExpression::Literal(Literal::Integer(minute))),
        second: Box::new(ScalarExpression::Literal(Literal::Integer(second))),
        millisecond: Box::new(ScalarExpression::Literal(Literal::Integer(millisecond))),
        microsecond: Box::new(ScalarExpression::Literal(Literal::Integer(microsecond))),
        nanosecond: Box::new(ScalarExpression::Literal(Literal::Integer(nanosecond))),
    })
}

fn localdatetime_from_string_expression(text: &str) -> ScalarExpression {
    ScalarExpression::Temporal(TemporalExpr::LocalDateTimeFromString {
        text: Box::new(ScalarExpression::Literal(Literal::String(text.to_string()))),
    })
}

fn localtime_expression(
    hour: i64,
    minute: i64,
    second: i64,
    millisecond: i64,
    microsecond: i64,
    nanosecond: i64,
) -> ScalarExpression {
    ScalarExpression::Temporal(TemporalExpr::MakeLocalTime {
        hour: Box::new(ScalarExpression::Literal(Literal::Integer(hour))),
        minute: Box::new(ScalarExpression::Literal(Literal::Integer(minute))),
        second: Box::new(ScalarExpression::Literal(Literal::Integer(second))),
        millisecond: Box::new(ScalarExpression::Literal(Literal::Integer(millisecond))),
        microsecond: Box::new(ScalarExpression::Literal(Literal::Integer(microsecond))),
        nanosecond: Box::new(ScalarExpression::Literal(Literal::Integer(nanosecond))),
    })
}

fn localtime_from_string_expression(text: &str) -> ScalarExpression {
    ScalarExpression::Temporal(TemporalExpr::LocalTimeFromString {
        text: Box::new(ScalarExpression::Literal(Literal::String(text.to_string()))),
    })
}

fn temporal_component_expression(
    expression: ScalarExpression,
    unit: TemporalComponentUnit,
) -> ScalarExpression {
    ScalarExpression::Temporal(TemporalExpr::Component {
        expression: Box::new(expression),
        unit,
    })
}

fn route_test_graph() -> Declaration {
    Declaration::from_yaml(
        r"
version: 1
name: route_test
nodes:
  - label: Person
    table: { schema: ops, name: people }
    key: id
    properties:
      name: full_name
  - label: Service
    table: { schema: ops, name: services }
    key: id
    properties:
      name: service_name
  - label: Incident
    table: { schema: ops, name: incidents }
    key: id
    properties:
      title: title
  - label: Team
    table: { schema: ops, name: teams }
    key: id
    properties:
      name: team_name
relationships:
  - type: ROUTES
    table: { schema: ops, name: person_service_routes }
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
  - type: ROUTES
    table: { schema: ops, name: service_incident_routes }
    from: { label: Service, key: service_id }
    to: { label: Incident, key: incident_id }
  - type: ESCALATES_TO
    table: { schema: ops, name: person_service_routes }
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
  - type: ESCALATES_TO
    table: { schema: ops, name: person_team_routes }
    from: { label: Person, key: person_id }
    to: { label: Team, key: team_id }
  - type: ESCALATES_TO
    table: { schema: ops, name: service_incident_routes }
    from: { label: Service, key: service_id }
    to: { label: Incident, key: incident_id }
  - type: ESCALATES_TO
    table: { schema: ops, name: team_incident_routes }
    from: { label: Team, key: team_id }
    to: { label: Incident, key: incident_id }
",
    )
    .expect("route test graph should parse")
}

fn fanout_test_graph() -> Declaration {
    Declaration::from_yaml(
        r"
version: 1
name: fanout_test
nodes:
  - label: Person
    table: { schema: ops, name: people }
    key: id
  - label: Service
    table: { schema: ops, name: services }
    key: id
  - label: Team
    table: { schema: ops, name: teams }
    key: id
  - label: Queue
    table: { schema: ops, name: queues }
    key: id
  - label: Incident
    table: { schema: ops, name: incidents }
    key: id
relationships:
  - type: FANS_OUT
    table: { schema: ops, name: person_service_routes }
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
  - type: FANS_OUT
    table: { schema: ops, name: person_team_routes }
    from: { label: Person, key: person_id }
    to: { label: Team, key: team_id }
  - type: FANS_OUT
    table: { schema: ops, name: person_queue_routes }
    from: { label: Person, key: person_id }
    to: { label: Queue, key: queue_id }
  - type: FANS_OUT
    table: { schema: ops, name: service_incident_routes }
    from: { label: Service, key: service_id }
    to: { label: Incident, key: incident_id }
  - type: FANS_OUT
    table: { schema: ops, name: team_incident_routes }
    from: { label: Team, key: team_id }
    to: { label: Incident, key: incident_id }
  - type: FANS_OUT
    table: { schema: ops, name: queue_incident_routes }
    from: { label: Queue, key: queue_id }
    to: { label: Incident, key: incident_id }
",
    )
    .expect("fanout test graph should parse")
}

#[test]
fn compiles_match_where_return_order_limit() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[:OWNS]->(service:Service) \
             WHERE service.tier = 'prod' AND person.active = true \
             RETURN person.name AS owner, service.name AS service \
             ORDER BY service.name DESC LIMIT 10",
    )
    .expect("query should compile");

    assert_eq!(
        plan.nodes,
        vec![
            NodePattern {
                variable: "person".to_string(),
                label: "Person".to_string(),
            },
            NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "OWNS".to_string(),
            left: "person".to_string(),
            direction: Direction::Outgoing,
            right: "service".to_string(),
        }]
    );
    assert_eq!(plan.projections.len(), 2);
    assert_eq!(plan.predicates.len(), 2);
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            }),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
    assert_eq!(plan.limit, Some(10));
    assert_eq!(plan.predicate, None);
}

#[test]
fn compiles_order_by_null_placement() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN service.name AS service, service.tier AS tier \
             ORDER BY service.tier ASC NULLS LAST, service.name DESC NULLS FIRST \
             LIMIT 5",
    )
    .expect("query should compile");

    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                }),
                direction: OrderDirection::Ascending,
                nulls: Some(NullOrder::Last),
            },
            OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                }),
                direction: OrderDirection::Descending,
                nulls: Some(NullOrder::First),
            },
        ]
    );
    assert_eq!(plan.limit, Some(5));
}

#[test]
fn compiles_union_query() {
    let query = compile_cypher_query(
        "MATCH (service:Service) \
             WHERE service.tier = 'prod' \
             RETURN service.name AS item \
             UNION \
             MATCH (person:Person) \
             WHERE person.team = 'platform' \
             RETURN person.name AS item",
    )
    .expect("UNION query should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected union query");
    };
    assert_eq!(projection_names(&union.first), vec!["item".to_string()]);
    assert_eq!(union.branches.len(), 1);
    let branch = union.branches.first().expect("union branch should exist");
    assert!(!branch.all);
    assert_eq!(projection_names(&branch.plan), vec!["item".to_string()]);
}

#[test]
fn compiles_union_all_query() {
    let query = compile_cypher_query(
        "MATCH (service:Service) RETURN service.tier AS tier \
             UNION ALL \
             MATCH (service:Service) RETURN service.tier AS tier",
    )
    .expect("UNION ALL query should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected union query");
    };
    assert_eq!(union.branches.len(), 1);
    let branch = union.branches.first().expect("union branch should exist");
    assert!(branch.all);
}

#[test]
fn compiles_static_unwind_as_union_all_branches() {
    let query = compile_cypher_query(
        "UNWIND ['prod', 'dev'] AS tier \
             MATCH (service:Service) \
             WHERE service.tier = tier \
             RETURN tier AS tier, service.name AS service \
             ORDER BY tier, service",
    )
    .expect("static UNWIND query should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected static UNWIND to expand into a union query");
    };
    assert_eq!(union.branches.len(), 1);
    assert!(union.branches.first().expect("branch").all);
    assert_eq!(
        union.first.projections.first(),
        Some(&Projection::Literal {
            literal: Literal::String("prod".to_string()),
            alias: "tier".to_string(),
        })
    );
    assert_eq!(
        union
            .branches
            .first()
            .expect("static UNWIND branch should exist")
            .plan
            .projections
            .first(),
        Some(&Projection::Literal {
            literal: Literal::String("dev".to_string()),
            alias: "tier".to_string(),
        })
    );
    assert_eq!(
        union.first.predicates,
        vec![PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
        }]
    );
}

#[test]
fn compiles_static_unwind_after_transparent_with_as_union_all_branches() {
    let query = compile_cypher_query(
        "MATCH (service:Service) \
             WITH service \
             UNWIND [1, 2] AS n \
             RETURN service.name AS service, n \
             ORDER BY service, n",
    )
    .expect("WITH-separated static UNWIND query should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected static UNWIND after WITH to expand into a union query");
    };
    assert_eq!(union.branches.len(), 1);
    assert!(union.branches.first().expect("branch").all);
    assert_eq!(
        union.first.projections.get(1),
        Some(&Projection::Literal {
            literal: Literal::Integer(1),
            alias: "n".to_string(),
        })
    );
    assert_eq!(
        union
            .branches
            .first()
            .expect("static UNWIND branch should exist")
            .plan
            .projections
            .get(1),
        Some(&Projection::Literal {
            literal: Literal::Integer(2),
            alias: "n".to_string(),
        })
    );
    assert_eq!(
        union.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::ProjectionAlias("service".to_string()),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::ProjectionAlias("n".to_string()),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_static_unwind_over_list_case_expressions() {
    let query = compile_cypher_query(
        "UNWIND (CASE WHEN true THEN ['prod', 'dev', 'stage'] ELSE ['legacy'] END)[0..2] AS tier \
             MATCH (service:Service) \
             WHERE service.tier = tier \
             RETURN tier AS tier, service.name AS service \
             ORDER BY tier, service",
    )
    .expect("static UNWIND over sliced list CASE should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected static CASE UNWIND to expand into a union query");
    };
    assert_eq!(union.branches.len(), 1);
    assert_eq!(
        union.first.projections.first(),
        Some(&Projection::Literal {
            literal: Literal::String("prod".to_string()),
            alias: "tier".to_string(),
        })
    );
    assert_eq!(
        union
            .branches
            .first()
            .expect("static UNWIND branch should exist")
            .plan
            .projections
            .first(),
        Some(&Projection::Literal {
            literal: Literal::String("dev".to_string()),
            alias: "tier".to_string(),
        })
    );

    let generic = compile_cypher_query(
        "UNWIND CASE 'prod' WHEN 'dev' THEN ['dev'] ELSE coalesce(null, ['prod']) END AS tier \
             MATCH (service:Service) \
             WHERE service.tier = tier \
             RETURN tier AS tier, service.name AS service",
    )
    .expect("static UNWIND over generic list CASE should compile");

    let GraphQuery::Plan(plan) = generic else {
        panic!("single selected static CASE branch should compile as a plan");
    };
    assert_eq!(
        plan.projections.first(),
        Some(&Projection::Literal {
            literal: Literal::String("prod".to_string()),
            alias: "tier".to_string(),
        })
    );
}

#[test]
fn rejects_static_unwind_over_dynamic_list_case_predicates() {
    let error = compile_cypher_query(
        "MATCH (service:Service) \
             UNWIND CASE WHEN service.tier = 'prod' THEN ['prod'] ELSE ['other'] END AS tier \
             RETURN tier",
    )
    .expect_err("dynamic CASE predicate in static UNWIND should be rejected");

    assert!(
        error.to_string().contains(
            "UNWIND over list-valued CASE expressions requires statically foldable WHEN predicates"
        ),
        "{error}"
    );
}

#[test]
fn compiles_duplicate_static_unwind_aggregates_as_outer_union_aggregates() {
    let query = compile_cypher_query(
        "UNWIND ['prod', 'prod', 'dev'] AS tier \
             MATCH (service:Service) \
             WHERE service.tier = tier \
             RETURN tier AS tier, count(*) AS services \
             ORDER BY tier",
    )
    .expect("static UNWIND aggregate query should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected duplicate static UNWIND to expand into a union query");
    };
    assert_eq!(union.branches.len(), 2);
    assert_eq!(
        union.outer_projection,
        Some(GraphUnionOuterProjection {
            items: vec![
                GraphUnionOuterProjectionItem::Column {
                    name: "tier".to_string(),
                },
                GraphUnionOuterProjectionItem::CountAll {
                    alias: "services".to_string(),
                },
            ],
            group_by: vec!["tier".to_string()],
        })
    );
    assert_eq!(projection_names(&union.first), vec!["tier".to_string()]);
}

#[test]
fn compiles_empty_static_unwind_as_forced_empty_plan() {
    let query = compile_cypher_query(
        "UNWIND [] AS tier \
             MATCH (service:Service) \
             RETURN tier AS tier, count(*) AS services",
    )
    .expect("empty static UNWIND query should compile");

    let GraphQuery::Plan(plan) = query else {
        panic!("single empty static UNWIND branch should compile as a plan");
    };
    assert!(matches!(
        plan.predicate,
        Some(PredicateExpression::Boolean(false))
    ));
    assert_eq!(
        plan.projections.first(),
        Some(&Projection::Literal {
            literal: Literal::Null,
            alias: "tier".to_string(),
        })
    );
}

#[test]
fn rejects_dynamic_static_unwind_sources() {
    assert_unsupported(
        "MATCH (service:Service) \
             UNWIND service.tier AS tier \
             RETURN tier",
    );
}

#[test]
fn rejects_dynamic_static_unwind_sources_after_with() {
    assert_unsupported(
        "MATCH (service:Service) \
             WITH service \
             UNWIND [service.tier] AS tier \
             RETURN tier",
    );
}

#[test]
fn compiles_static_unwind_hidden_order_expressions() {
    let query = compile_cypher_query(
        "UNWIND ['prod', 'dev'] AS tier \
             MATCH (service:Service) \
             WHERE service.tier = tier \
             RETURN service.name AS service \
             ORDER BY CASE WHEN tier = 'prod' THEN 0 ELSE 1 END, service",
    )
    .expect("static UNWIND hidden ORDER BY expression should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected static UNWIND to expand into a union query");
    };
    assert_eq!(
        union.first.projection_output_names(),
        vec!["service".to_string(), "__coral_order_0".to_string()]
    );
    assert_eq!(
        union.outer_projection,
        Some(GraphUnionOuterProjection {
            items: vec![GraphUnionOuterProjectionItem::Column {
                name: "service".to_string(),
            }],
            group_by: Vec::new(),
        })
    );
    assert_eq!(
        union.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::ProjectionAlias("__coral_order_0".to_string()),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::ProjectionAlias("service".to_string()),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_static_unwind_outer_order_null_placement() {
    let query = compile_cypher_query(
        "UNWIND ['prod', 'dev'] AS tier \
             MATCH (service:Service) \
             WHERE service.tier = tier \
             RETURN service.name AS service \
             ORDER BY CASE WHEN tier = 'prod' THEN service.name ELSE NULL END NULLS LAST, \
                      service DESC NULLS FIRST",
    )
    .expect("static UNWIND ORDER BY NULLS FIRST/LAST should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected static UNWIND to expand into a union query");
    };
    assert_eq!(
        union.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::ProjectionAlias("__coral_order_0".to_string()),
                direction: OrderDirection::Ascending,
                nulls: Some(NullOrder::Last),
            },
            OrderKey {
                expression: OrderExpression::ProjectionAlias("service".to_string()),
                direction: OrderDirection::Descending,
                nulls: Some(NullOrder::First),
            },
        ]
    );
}

#[test]
fn compiles_static_node_label_alternatives_as_union_all() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team) \
             RETURN entity.name AS name",
    )
    .expect("static node label alternatives should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert_eq!(
        union.first.nodes.first().expect("first node").label,
        "Person"
    );
    assert_eq!(union.branches.len(), 1);
    let branch = union.branches.first().expect("alternative branch");
    assert!(branch.all);
    assert_eq!(
        branch.plan.nodes.first().expect("branch node").label,
        "Team"
    );
    assert_eq!(projection_names(&union.first), vec!["name".to_string()]);
    assert_eq!(projection_names(&branch.plan), vec!["name".to_string()]);
}

#[test]
fn graph_aware_compiles_unlabeled_standalone_node_scan_as_declared_label_union() {
    let graph = star_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (entity) \
             RETURN entity.name AS name \
             ORDER BY entity.name",
    )
    .expect("graph declaration should expand an unlabeled standalone node scan");

    let GraphQuery::Union(union) = query else {
        panic!("unlabeled graph-aware node scan should expand into a union query");
    };
    let labels = std::iter::once(&union.first)
        .chain(union.branches.iter().map(|branch| &branch.plan))
        .map(|plan| plan.nodes.first().expect("branch node").label.as_str())
        .collect::<Vec<_>>();
    assert_eq!(labels, vec!["Person", "Service", "Team"]);
    assert!(union.branches.iter().all(|branch| branch.all));
    assert_eq!(projection_names(&union.first), vec!["name".to_string()]);
}

#[test]
fn deduplicates_static_node_label_alternatives_before_union_expansion() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Person) \
             RETURN entity.name AS name",
    )
    .expect("duplicate static node label alternatives should compile");

    let GraphQuery::Plan(plan) = query else {
        panic!("duplicate static label alternatives should collapse to one graph plan");
    };
    assert_eq!(plan.nodes.first().expect("first node").label, "Person");
}

#[test]
fn deduplicates_static_relationship_type_alternatives_before_union_expansion() {
    let query = compile_cypher_query(
        "MATCH (source:Service)-[relationship:DEPENDS_ON|DEPENDS_ON]->(target:Service) \
             RETURN type(relationship) AS relationship_type",
    )
    .expect("duplicate static relationship type alternatives should compile");

    let GraphQuery::Plan(plan) = query else {
        panic!("duplicate static relationship type alternatives should collapse to one graph plan");
    };
    assert_eq!(
        plan.relationships
            .first()
            .expect("first relationship")
            .relationship_type,
        "DEPENDS_ON"
    );
}

#[test]
fn rejects_static_label_alternatives_that_exceed_branch_cap() {
    let labels = (0..=MAX_PATTERN_ALTERNATIVE_BRANCHES)
        .map(|index| format!("Label{index}"))
        .collect::<Vec<_>>()
        .join("|");
    let cypher = format!("MATCH (entity:{labels}) RETURN entity.name AS name");

    let error = compile_cypher_query(&cypher)
        .expect_err("excessive static label alternatives should be capped");

    assert!(error.to_string().contains("more than 64 branches"));
}

#[test]
fn compiles_static_relationship_type_alternatives_as_union_all() {
    let query = compile_cypher_query(
        "MATCH (source:Service)-[relationship:DEPENDS_ON|OWNS]->(target:Service) \
             RETURN type(relationship) AS relationship_type",
    )
    .expect("static relationship type alternatives should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected static relationship type alternatives to expand into a union query");
    };
    assert_eq!(
        union
            .first
            .relationships
            .first()
            .expect("first relationship")
            .relationship_type,
        "DEPENDS_ON"
    );
    assert_eq!(union.branches.len(), 1);
    let branch = union.branches.first().expect("alternative branch");
    assert!(branch.all);
    assert_eq!(
        branch
            .plan
            .relationships
            .first()
            .expect("branch relationship")
            .relationship_type,
        "OWNS"
    );
}

#[test]
fn compiles_static_label_alternatives_with_outer_count_star() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team) \
             RETURN count(*) AS count",
    )
    .expect("count(*) should compile as an outer union aggregate");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert_eq!(
        union.outer_projection,
        Some(GraphUnionOuterProjection {
            items: vec![GraphUnionOuterProjectionItem::CountAll {
                alias: "count".to_string(),
            }],
            group_by: Vec::new(),
        })
    );
    assert_eq!(
        union.first.projection_output_names(),
        vec!["__coral_count_row".to_string()]
    );
}

#[test]
fn compiles_static_label_alternatives_with_outer_count_star_ordering() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team) \
             RETURN count(*) AS count \
             ORDER BY count(*)",
    )
    .expect("count(*) order expression should compile as an outer union aggregate alias");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert_eq!(
        union.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("count".to_string()),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_static_label_alternatives_with_grouped_count_star() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team) \
             RETURN entity.name AS name, count(*) AS count",
    )
    .expect("grouped count(*) should compile as an outer union aggregate");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert_eq!(
        union.outer_projection,
        Some(GraphUnionOuterProjection {
            items: vec![
                GraphUnionOuterProjectionItem::Column {
                    name: "name".to_string(),
                },
                GraphUnionOuterProjectionItem::CountAll {
                    alias: "count".to_string(),
                },
            ],
            group_by: vec!["name".to_string()],
        })
    );
    assert_eq!(
        union.first.projection_output_names(),
        vec!["name".to_string()]
    );
}

#[test]
fn compiles_static_label_alternatives_with_grouped_count_star_ordering() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team) \
             RETURN entity.name AS name, count(*) AS count \
             ORDER BY count(*) DESC, entity.name",
    )
    .expect("grouped count(*) order expressions should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert_eq!(
        union.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::ProjectionAlias("count".to_string()),
                direction: OrderDirection::Descending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::ProjectionAlias("name".to_string()),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_static_label_alternatives_with_grouped_count_star_first() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team) \
             RETURN count(*) AS count, entity.name AS name",
    )
    .expect("grouped count(*) should preserve RETURN item order");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    let outer_projection = union
        .outer_projection
        .expect("expected an outer union projection");
    assert_eq!(
        outer_projection.output_names(),
        vec!["count".to_string(), "name".to_string()]
    );
    assert_eq!(outer_projection.group_by, vec!["name".to_string()]);
}

#[test]
fn compiles_static_label_alternatives_with_grouped_count_property() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team)-[:OWNS]->(service:Service) \
             RETURN entity.name AS name, count(service.name) AS named_services \
             ORDER BY count(service.name) DESC, name",
    )
    .expect("grouped count(property) should compile as an outer union aggregate");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert_eq!(
        union.first.projection_output_names(),
        vec!["name".to_string(), "__coral_agg_1".to_string()]
    );
    assert_eq!(
        union.outer_projection,
        Some(GraphUnionOuterProjection {
            items: vec![
                GraphUnionOuterProjectionItem::Column {
                    name: "name".to_string(),
                },
                GraphUnionOuterProjectionItem::Aggregate {
                    function: AggregateFunction::Count,
                    source: "__coral_agg_1".to_string(),
                    distinct: false,
                    alias: "named_services".to_string(),
                },
            ],
            group_by: vec!["name".to_string()],
        })
    );
    assert_eq!(
        union.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::ProjectionAlias("named_services".to_string()),
                direction: OrderDirection::Descending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::ProjectionAlias("name".to_string()),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_static_label_alternatives_with_count_node_projection() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team)-[:OWNS]->(service:Service) \
             RETURN entity.name AS name, count(service) AS services \
             ORDER BY count(service) DESC, name",
    )
    .expect("count(node) should compile as an outer union aggregate");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert_eq!(
        union.first.projection_output_names(),
        vec!["name".to_string(), "__coral_agg_1".to_string()]
    );
    assert!(matches!(
        union.first.projections.get(1),
        Some(Projection::Expression {
            expression: ScalarExpression::GraphPresence { variable },
            alias,
        }) if variable == "service" && alias == "__coral_agg_1"
    ));
    assert_eq!(
        union.outer_projection,
        Some(GraphUnionOuterProjection {
            items: vec![
                GraphUnionOuterProjectionItem::Column {
                    name: "name".to_string(),
                },
                GraphUnionOuterProjectionItem::Aggregate {
                    function: AggregateFunction::Count,
                    source: "__coral_agg_1".to_string(),
                    distinct: false,
                    alias: "services".to_string(),
                },
            ],
            group_by: vec!["name".to_string()],
        })
    );
    assert_eq!(
        union.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::ProjectionAlias("services".to_string()),
                direction: OrderDirection::Descending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::ProjectionAlias("name".to_string()),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_static_label_alternatives_with_optional_endpoint_property_aggregates() {
    let query = compile_cypher_query(
        "MATCH (owner:Person|Team) \
             OPTIONAL MATCH (owner)-[ownership:OWNS]->(service:Service) \
             RETURN owner.name AS owner, \
                    count(endNode(ownership).name) AS named_services, \
                    sum(endNode(ownership).risk) AS total_risk \
             ORDER BY owner",
    )
    .expect("optional endpoint property aggregates should compile as outer union aggregates");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert_eq!(
        union.first.projection_output_names(),
        vec![
            "owner".to_string(),
            "__coral_agg_1".to_string(),
            "__coral_agg_2".to_string(),
        ]
    );
    assert!(matches!(
        union.first.projections.get(1),
        Some(Projection::Expression {
            expression: ScalarExpression::PresenceGated {
                presence_variable,
                expression,
            },
            alias,
        }) if presence_variable == "ownership"
            && matches!(
                expression.as_ref(),
                ScalarExpression::Property(PropertyRef { variable, property })
                    if variable == "service" && property == "name"
            )
            && alias == "__coral_agg_1"
    ));
    assert_eq!(
        union.outer_projection,
        Some(GraphUnionOuterProjection {
            items: vec![
                GraphUnionOuterProjectionItem::Column {
                    name: "owner".to_string(),
                },
                GraphUnionOuterProjectionItem::Aggregate {
                    function: AggregateFunction::Count,
                    source: "__coral_agg_1".to_string(),
                    distinct: false,
                    alias: "named_services".to_string(),
                },
                GraphUnionOuterProjectionItem::Aggregate {
                    function: AggregateFunction::Sum,
                    source: "__coral_agg_2".to_string(),
                    distinct: false,
                    alias: "total_risk".to_string(),
                },
            ],
            group_by: vec!["owner".to_string()],
        })
    );
}

#[test]
fn compiles_static_label_alternatives_with_optional_endpoint_identity_aggregates() {
    let query = compile_cypher_query(
        "MATCH (owner:Person|Team) \
             OPTIONAL MATCH (owner)-[ownership:OWNS]->(service:Service) \
             RETURN owner.name AS owner, \
                    count(endNode(ownership)) AS services, \
                    count(DISTINCT endNode(ownership)) AS distinct_services, \
                    collect(endNode(ownership)) AS service_ids \
             ORDER BY owner",
    )
    .expect("optional endpoint identity aggregates should compile as outer union aggregates");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    for index in 1..=3 {
        let expected_alias = format!("__coral_agg_{index}");
        assert!(matches!(
            union.first.projections.get(index),
            Some(Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable,
                    expression,
                },
                alias,
            }) if presence_variable == "ownership"
                && matches!(
                    expression.as_ref(),
                    ScalarExpression::Key { variable } if variable == "service"
                )
                && alias == &expected_alias
        ));
    }
    assert_eq!(
        union.outer_projection,
        Some(GraphUnionOuterProjection {
            items: vec![
                GraphUnionOuterProjectionItem::Column {
                    name: "owner".to_string(),
                },
                GraphUnionOuterProjectionItem::Aggregate {
                    function: AggregateFunction::Count,
                    source: "__coral_agg_1".to_string(),
                    distinct: false,
                    alias: "services".to_string(),
                },
                GraphUnionOuterProjectionItem::Aggregate {
                    function: AggregateFunction::Count,
                    source: "__coral_agg_2".to_string(),
                    distinct: true,
                    alias: "distinct_services".to_string(),
                },
                GraphUnionOuterProjectionItem::Aggregate {
                    function: AggregateFunction::Collect,
                    source: "__coral_agg_3".to_string(),
                    distinct: false,
                    alias: "service_ids".to_string(),
                },
            ],
            group_by: vec!["owner".to_string()],
        })
    );
}

#[test]
fn compiles_static_label_alternatives_with_distinct_count_node_projection() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team)-[:OWNS]->(service:Service) \
             RETURN count(DISTINCT entity) AS owners",
    )
    .expect("distinct graph variable counts should compile through graph identity");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert_eq!(
        union.first.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::GraphIdentity {
                variable: "entity".to_string(),
            },
            alias: "__coral_agg_0".to_string(),
        }]
    );
    assert_eq!(
        union.outer_projection,
        Some(GraphUnionOuterProjection {
            items: vec![GraphUnionOuterProjectionItem::Aggregate {
                function: AggregateFunction::Count,
                source: "__coral_agg_0".to_string(),
                distinct: true,
                alias: "owners".to_string(),
            }],
            group_by: Vec::new(),
        })
    );
}

#[test]
fn compiles_static_label_alternatives_with_collect_graph_variable_projection() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team) \
             RETURN collect(entity) AS entities, collect(DISTINCT entity) AS distinct_entities",
    )
    .expect("collect(node) should compile as an outer union aggregate");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert_eq!(
        union.first.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::GraphIdentity {
                    variable: "entity".to_string(),
                },
                alias: "__coral_agg_0".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::GraphIdentity {
                    variable: "entity".to_string(),
                },
                alias: "__coral_agg_1".to_string(),
            },
        ]
    );
    assert_eq!(
        union.outer_projection,
        Some(GraphUnionOuterProjection {
            items: vec![
                GraphUnionOuterProjectionItem::Aggregate {
                    function: AggregateFunction::Collect,
                    source: "__coral_agg_0".to_string(),
                    distinct: false,
                    alias: "entities".to_string(),
                },
                GraphUnionOuterProjectionItem::Aggregate {
                    function: AggregateFunction::Collect,
                    source: "__coral_agg_1".to_string(),
                    distinct: true,
                    alias: "distinct_entities".to_string(),
                },
            ],
            group_by: Vec::new(),
        })
    );
}

#[test]
fn compiles_static_label_alternatives_with_distinct_count_property() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team)-[:OWNS]->(service:Service) \
             RETURN count(DISTINCT service.name) AS named_services",
    )
    .expect("count(DISTINCT property) should compile as an outer union aggregate");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    let outer_projection = union
        .outer_projection
        .expect("expected an outer union projection");
    assert_eq!(
        outer_projection.items,
        vec![GraphUnionOuterProjectionItem::Aggregate {
            function: AggregateFunction::Count,
            source: "__coral_agg_0".to_string(),
            distinct: true,
            alias: "named_services".to_string(),
        }]
    );
}

#[test]
fn compiles_static_label_alternatives_with_numeric_property_aggregates() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team)-[:OWNS]->(service:Service) \
             RETURN entity.name AS name, \
                    sum(service.risk) AS total_risk, \
                    avg(service.risk) AS average_risk, \
                    min(service.risk) AS lowest_risk, \
                    max(service.risk) AS highest_risk \
             ORDER BY sum(service.risk) DESC",
    )
    .expect("numeric property aggregates should compile as outer union aggregates");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    let outer_projection = union
        .outer_projection
        .expect("expected an outer union projection");
    assert_eq!(
        outer_projection.output_names(),
        vec![
            "name".to_string(),
            "total_risk".to_string(),
            "average_risk".to_string(),
            "lowest_risk".to_string(),
            "highest_risk".to_string(),
        ]
    );
    assert_eq!(
        union.first.projection_output_names(),
        vec![
            "name".to_string(),
            "__coral_agg_1".to_string(),
            "__coral_agg_2".to_string(),
            "__coral_agg_3".to_string(),
            "__coral_agg_4".to_string(),
        ]
    );
    assert_eq!(
        union.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("total_risk".to_string()),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_static_label_alternatives_with_aggregate_expression_targets() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team)-[:OWNS]->(service:Service) \
             RETURN entity.name AS name, \
                    collect(DISTINCT coalesce(service.tier, 'unknown')) AS tiers, \
                    count(coalesce(service.tier, 'unknown')) AS tier_count, \
                    sum(service.risk + 1) AS adjusted_risk \
             ORDER BY sum(service.risk + 1) DESC, name",
    )
    .expect("aggregate expression targets should compile as outer union aggregates");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert_eq!(
        union.first.projection_output_names(),
        vec![
            "name".to_string(),
            "__coral_agg_1".to_string(),
            "__coral_agg_2".to_string(),
            "__coral_agg_3".to_string(),
        ]
    );
    assert!(matches!(
        union.first.projections.get(1),
        Some(Projection::Expression {
            expression: ScalarExpression::Coalesce { .. },
            alias,
        }) if alias == "__coral_agg_1"
    ));
    assert!(matches!(
        union.first.projections.get(3),
        Some(Projection::Expression {
            expression: ScalarExpression::Arithmetic { .. },
            alias,
        }) if alias == "__coral_agg_3"
    ));
    assert_eq!(
        union.outer_projection,
        Some(GraphUnionOuterProjection {
            items: vec![
                GraphUnionOuterProjectionItem::Column {
                    name: "name".to_string(),
                },
                GraphUnionOuterProjectionItem::Aggregate {
                    function: AggregateFunction::Collect,
                    source: "__coral_agg_1".to_string(),
                    distinct: true,
                    alias: "tiers".to_string(),
                },
                GraphUnionOuterProjectionItem::Aggregate {
                    function: AggregateFunction::Count,
                    source: "__coral_agg_2".to_string(),
                    distinct: false,
                    alias: "tier_count".to_string(),
                },
                GraphUnionOuterProjectionItem::Aggregate {
                    function: AggregateFunction::Sum,
                    source: "__coral_agg_3".to_string(),
                    distinct: false,
                    alias: "adjusted_risk".to_string(),
                },
            ],
            group_by: vec!["name".to_string()],
        })
    );
    assert_eq!(
        union.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::ProjectionAlias("adjusted_risk".to_string()),
                direction: OrderDirection::Descending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::ProjectionAlias("name".to_string()),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_static_label_alternatives_with_predicate_aggregate_targets() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team)-[:OWNS]->(service:Service) \
             RETURN entity.name AS name, collect(service.risk > 0.8) AS high_risk_flags",
    )
    .expect("predicate aggregate target should compile as an outer union aggregate");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert!(matches!(
        union.first.projections.get(1),
        Some(Projection::Expression {
            expression: ScalarExpression::Predicate(_),
            alias,
        }) if alias == "__coral_agg_1"
    ));
    assert_eq!(
        union.outer_projection,
        Some(GraphUnionOuterProjection {
            items: vec![
                GraphUnionOuterProjectionItem::Column {
                    name: "name".to_string(),
                },
                GraphUnionOuterProjectionItem::Aggregate {
                    function: AggregateFunction::Collect,
                    source: "__coral_agg_1".to_string(),
                    distinct: false,
                    alias: "high_risk_flags".to_string(),
                },
            ],
            group_by: vec!["name".to_string()],
        })
    );
}

#[test]
fn rejects_static_label_alternatives_with_aggregate_expression_subqueries() {
    assert_unsupported(
        "MATCH (entity:Person|Team)-[:OWNS]->(service:Service) \
             RETURN collect(CASE \
                      WHEN EXISTS { MATCH (service)-[:DEPENDS_ON]->(:Service) } THEN service.name \
                      ELSE 'none' \
                    END) AS services",
    );
}

#[test]
fn compiles_static_label_alternatives_with_collect_property_projection() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team)-[:OWNS]->(service:Service) \
             RETURN entity.name AS name, collect(DISTINCT service.name) AS services \
             ORDER BY name",
    )
    .expect("collect(property) should compile as an outer union aggregate");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert_eq!(
        union.outer_projection,
        Some(GraphUnionOuterProjection {
            items: vec![
                GraphUnionOuterProjectionItem::Column {
                    name: "name".to_string(),
                },
                GraphUnionOuterProjectionItem::Aggregate {
                    function: AggregateFunction::Collect,
                    source: "__coral_agg_1".to_string(),
                    distinct: true,
                    alias: "services".to_string(),
                },
            ],
            group_by: vec!["name".to_string()],
        })
    );
}

#[test]
fn compiles_static_label_alternatives_with_outer_row_modifiers() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team) \
             RETURN entity.name AS name \
             ORDER BY name DESC \
             SKIP 1 \
             LIMIT 5",
    )
    .expect("global row modifiers should compile as outer union modifiers");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert!(union.first.order_by.is_empty());
    assert_eq!(union.first.skip, None);
    assert_eq!(union.first.limit, None);
    assert_eq!(
        union.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("name".to_string()),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
    assert_eq!(union.skip, Some(1));
    assert_eq!(union.limit, Some(5));
}

#[test]
fn compiles_static_label_alternatives_with_outer_distinct() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team) \
             RETURN DISTINCT entity.name AS name \
             ORDER BY name",
    )
    .expect("RETURN DISTINCT should compile as an outer union modifier");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert!(!union.first.distinct);
    assert!(union.distinct);
    assert_eq!(
        union.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("name".to_string()),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_static_label_alternatives_with_projected_global_ordering() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team) \
             RETURN entity.name AS name \
             ORDER BY entity.name",
    )
    .expect("projected global ordering should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert_eq!(
        union.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("name".to_string()),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_static_label_alternatives_with_hidden_global_ordering() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team) \
             RETURN entity.name AS name \
             ORDER BY entity.team",
    )
    .expect("row-preserving hidden global ordering should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert_eq!(
        union.first.projection_output_names(),
        vec!["name".to_string(), "__coral_order_0".to_string()]
    );
    assert_eq!(
        union.outer_projection,
        Some(GraphUnionOuterProjection {
            items: vec![GraphUnionOuterProjectionItem::Column {
                name: "name".to_string(),
            }],
            group_by: Vec::new(),
        })
    );
    assert_eq!(
        union.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("__coral_order_0".to_string()),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn rejects_static_label_alternatives_with_aggregate_hidden_global_ordering() {
    let error = compile_cypher_query(
        "MATCH (entity:Person|Team)-[:OWNS]->(service:Service) \
             RETURN entity.name AS name, count(*) AS services \
             ORDER BY service.name",
    )
    .expect_err("aggregate hidden global ordering should require staged planning");

    assert!(error.to_string().contains("aggregate RETURN"));
}

#[test]
fn compiles_static_label_alternatives_with_terminal_with_projection() {
    let query = compile_cypher_query(
        "MATCH (owner:Person|Team)-[:OWNS]->(service:Service) \
             WITH owner.name AS owner, service.name AS service \
             WHERE service = 'billing-api' \
             RETURN owner, service \
             ORDER BY owner",
    )
    .expect("static alternatives with terminal WITH projection should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert_eq!(
        union.first.projection_output_names(),
        vec!["owner".to_string(), "service".to_string()]
    );
    assert!(union.first.post_projection_predicate.is_some());
    assert!(union.branches.iter().all(|branch| {
        branch.plan.projection_output_names() == vec!["owner".to_string(), "service".to_string()]
            && branch.plan.post_projection_predicate.is_some()
    }));
    assert_eq!(
        union.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("owner".to_string()),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_static_label_alternatives_inside_explicit_union_all() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team) RETURN entity.name AS item \
             UNION ALL \
             MATCH (service:Service) RETURN service.name AS item",
    )
    .expect("static alternatives should flatten into top-level UNION ALL");

    let GraphQuery::Union(union) = query else {
        panic!("expected union query");
    };
    assert_eq!(projection_names(&union.first), vec!["item".to_string()]);
    assert_eq!(union.branches.len(), 2);
    assert!(union.branches.iter().all(|branch| branch.all));
    assert_eq!(
        union
            .branches
            .iter()
            .map(|branch| projection_names(&branch.plan))
            .collect::<Vec<_>>(),
        vec![vec!["item".to_string()], vec!["item".to_string()]]
    );
}

#[test]
fn compiles_static_label_alternatives_inside_explicit_union_distinct() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team) RETURN entity.name AS item \
             UNION \
             MATCH (service:Service) RETURN service.name AS item",
    )
    .expect("static alternatives should flatten into top-level UNION distinct");

    let GraphQuery::Union(union) = query else {
        panic!("expected union query");
    };
    assert!(union.distinct);
    assert_eq!(projection_names(&union.first), vec!["item".to_string()]);
    assert_eq!(union.branches.len(), 2);
    assert!(union.branches.iter().all(|branch| branch.all));
    assert_eq!(
        union
            .branches
            .iter()
            .map(|branch| branch
                .plan
                .nodes
                .first()
                .expect("branch node")
                .label
                .as_str())
            .collect::<Vec<_>>(),
        vec!["Team", "Service"]
    );
}

#[test]
fn rejects_static_label_alternatives_inside_mixed_explicit_union() {
    let error = compile_cypher_query(
        "MATCH (entity:Person|Team) RETURN entity.name AS item \
             UNION \
             MATCH (service:Service) RETURN service.name AS item \
             UNION ALL \
             MATCH (person:Person) RETURN person.name AS item",
    )
    .expect_err("mixed UNION operators need nested grouping for static alternatives");

    assert!(error.to_string().contains("mixed UNION and UNION ALL"));
}

#[test]
fn rejects_static_label_alternatives_with_modifiers_inside_explicit_union_all() {
    let error = compile_cypher_query(
        "MATCH (entity:Person|Team) \
             RETURN entity.name AS item \
             ORDER BY item \
             UNION ALL \
             MATCH (service:Service) RETURN service.name AS item",
    )
    .expect_err("branch-level modifiers need nested grouping");

    assert!(error.to_string().contains("nested union grouping"));
}

#[test]
fn rejects_union_projection_mismatches() {
    let error = compile_cypher_query(
        "MATCH (service:Service) RETURN service.name AS item \
             UNION \
             MATCH (person:Person) RETURN person.name AS person",
    )
    .expect_err("mismatched UNION projections should fail");

    assert!(error.to_string().contains("UNION branch projections"));
}

#[test]
fn single_plan_compile_rejects_union_queries() {
    let error = compile_cypher(
        "MATCH (service:Service) RETURN service.name AS item \
             UNION \
             MATCH (person:Person) RETURN person.name AS item",
    )
    .expect_err("single-plan compiler should reject UNION");

    assert!(error.to_string().contains("compile_cypher"));
}

#[test]
fn compiles_ignored_path_variable_patterns() {
    let plan = compile_cypher(
        "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             RETURN person.name AS owner, service.name AS service",
    )
    .expect("non-materialized path binding should compile");

    assert_eq!(
        plan.nodes,
        vec![
            NodePattern {
                variable: "person".to_string(),
                label: "Person".to_string(),
            },
            NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "OWNS".to_string(),
            left: "person".to_string(),
            direction: Direction::Outgoing,
            right: "service".to_string(),
        }]
    );
    assert_eq!(plan.projections.len(), 2);
}

#[test]
fn compiles_path_length_projection() {
    let plan = compile_cypher(
        "MATCH path = (source:Service)-[:DEPENDS_ON*2]->(target:Service) \
             RETURN source.name AS source, target.name AS target, length(path) AS hops",
    )
    .expect("path length projection should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "source".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("source".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "target".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("target".to_string()),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Integer(2)),
                alias: "hops".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_size_over_path_alias() {
    let plan = compile_cypher(
        "MATCH path = (source:Service)-[:DEPENDS_ON*2]->(target:Service) \
             WHERE size(path) = 2 \
             RETURN source.name AS source, target.name AS target, size(path) AS hops \
             ORDER BY size(path) DESC",
    )
    .expect("size(path) should compile as a path-length alias");

    let path_length = ScalarExpression::Literal(Literal::Integer(2));
    assert_eq!(
        plan.projections.get(2),
        Some(&Projection::Expression {
            expression: path_length.clone(),
            alias: "hops".to_string(),
        })
    );
    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: path_length.clone(),
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(2))),
        }))
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Literal(Literal::Integer(2)),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_size_over_path_element_lists() {
    let plan = compile_cypher(
        "MATCH path = (source:Service)-[:DEPENDS_ON*2]->(target:Service) \
             WHERE size(nodes(path)) = 3 AND size(relationships(path)) = 2 \
             RETURN size(nodes(path)) AS node_count, \
                    size(relationships(path)) AS relationship_count, \
                    size(nodes(path)) + size(relationships(path)) AS path_items \
             ORDER BY size(nodes(path)) DESC",
    )
    .expect("path element-list sizes should compile as folded path metadata");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Integer(3)),
                alias: "node_count".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Integer(2)),
                alias: "relationship_count".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Arithmetic {
                    operator: ArithmeticOperator::Add,
                    left: Box::new(ScalarExpression::Literal(Literal::Integer(3))),
                    right: Box::new(ScalarExpression::Literal(Literal::Integer(2))),
                },
                alias: "path_items".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Literal(Literal::Integer(3)),
                operator: ComparisonOperator::Equal,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(3))),
            })),
            right: Box::new(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Literal(Literal::Integer(2)),
                operator: ComparisonOperator::Equal,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(2))),
            })),
        })
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Literal(Literal::Integer(3)),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_path_metadata_arithmetic() {
    let plan = compile_cypher(
        "MATCH path = (source:Service)-[:DEPENDS_ON*2]->(target:Service) \
             WHERE size(path) + 1 = 3 \
             RETURN source.name AS source, length(path) + 1 AS depth \
             ORDER BY size(path) + 1 DESC",
    )
    .expect("path metadata should compose inside arithmetic expressions");

    let depth = ScalarExpression::Arithmetic {
        operator: ArithmeticOperator::Add,
        left: Box::new(ScalarExpression::Literal(Literal::Integer(2))),
        right: Box::new(ScalarExpression::Literal(Literal::Integer(1))),
    };
    assert_eq!(
        plan.projections.get(1),
        Some(&Projection::Expression {
            expression: depth.clone(),
            alias: "depth".to_string(),
        })
    );
    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: depth.clone(),
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(3))),
        }))
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(depth),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_path_metadata_inside_scalar_functions_and_case() {
    let plan = compile_cypher(
        "MATCH path = (source:Service)-[:DEPENDS_ON*2]->(target:Service) \
             WHERE coalesce(size(path), 0) = 2 \
             RETURN coalesce(length(path), 0) AS hops, \
                    toString(size(path)) AS hops_text, \
                    CASE WHEN length(path) = 2 THEN size(path) ELSE 0 END AS case_hops \
             ORDER BY coalesce(size(path), 0) DESC",
    )
    .expect("path metadata should compose inside scalar functions and CASE");

    let path_length = ScalarExpression::Literal(Literal::Integer(2));
    let coalesced_length = ScalarExpression::Coalesce {
        expressions: vec![
            path_length.clone(),
            ScalarExpression::Literal(Literal::Integer(0)),
        ],
    };

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: coalesced_length.clone(),
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(2))),
        }))
    );
    assert_eq!(
        plan.projections.first(),
        Some(&Projection::Expression {
            expression: coalesced_length.clone(),
            alias: "hops".to_string(),
        })
    );
    assert_eq!(
        plan.projections.get(1),
        Some(&Projection::Expression {
            expression: ScalarExpression::ToString {
                expression: Box::new(path_length.clone()),
            },
            alias: "hops_text".to_string(),
        })
    );
    assert!(matches!(
        plan.projections.get(2),
        Some(Projection::Expression {
            expression: ScalarExpression::Case {
                alternatives,
                else_expression,
            },
            alias,
        }) if alias == "case_hops"
            && matches!(
                alternatives.as_slice(),
                [ScalarCaseAlternative {
                    when: PredicateExpression::ScalarComparison(ScalarPredicate {
                        lhs,
                        operator: ComparisonOperator::Equal,
                        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                            Literal::Integer(2),
                        )),
                    }),
                    then,
                }] if lhs == &path_length && then == &path_length
            )
            && else_expression.as_deref()
                == Some(&ScalarExpression::Literal(Literal::Integer(0)))
    ));
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(coalesced_length),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_date_map_constructor_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (person:Person) \
         RETURN date({year: 1984, month: 10, day: 11}) AS full, \
                date({year: 1984, month: 10}) AS default_day, \
                date({year: 1984}) AS default_month_day, \
                toString(date({year: 1984, month: 10, day: 11})) AS text",
    )
    .expect("literal date map constructors should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: date_expression(1984, 10, 11),
                alias: "full".to_string(),
            },
            Projection::Expression {
                expression: date_expression(1984, 10, 1),
                alias: "default_day".to_string(),
            },
            Projection::Expression {
                expression: date_expression(1984, 1, 1),
                alias: "default_month_day".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::ToString {
                    expression: Box::new(date_expression(1984, 10, 11)),
                },
                alias: "text".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_date_string_constructor_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (person:Person) \
         RETURN date('2020-01-01') AS d, \
                toString(date('2020-01-01')) AS text",
    )
    .expect("literal date string constructors should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: date_from_string_expression("2020-01-01"),
                alias: "d".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::ToString {
                    expression: Box::new(date_from_string_expression("2020-01-01")),
                },
                alias: "text".to_string(),
            },
        ]
    );
}

#[test]
fn rejects_unsupported_date_constructor_forms() {
    for (cypher, expected) in [
        (
            "MATCH (person:Person) RETURN date(person.name) AS d",
            "dynamic date() string argument not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN date(2020) AS d",
            "date() requires a literal map or string argument",
        ),
        (
            "MATCH (person:Person) RETURN date({year: person.age}) AS d",
            "dynamic temporal fields not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN date({year: 2020, week: 1}) AS d",
            "date() temporal field 'week' is not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN date({year: 2020, quarter: 1}) AS d",
            "date() temporal field 'quarter' is not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN date({year: 2020, ordinalDay: 1}) AS d",
            "date() temporal field 'ordinalDay' is not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN date({date: date({year: 2020})}) AS d",
            "date() temporal field 'date' is not supported yet",
        ),
    ] {
        let error = compile_cypher(cypher).expect_err("unsupported date form should be rejected");
        assert!(
            error.to_string().contains(expected),
            "expected {expected:?}, got {error}"
        );
    }
}

#[test]
fn compiles_localdatetime_map_constructor_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (person:Person) \
         RETURN localdatetime({year: 2020, month: 1, day: 15, hour: 12, minute: 34, second: 56}) AS full, \
                localdatetime({year: 2020, month: 1, day: 15}) AS default_time, \
                toString(localdatetime({year: 2020, month: 1, day: 15, hour: 12, minute: 34, second: 56})) AS text",
    )
    .expect("literal localdatetime map constructors should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: localdatetime_expression(2020, 1, 15, 12, 34, 56, 0, 0, 0),
                alias: "full".to_string(),
            },
            Projection::Expression {
                expression: localdatetime_expression(2020, 1, 15, 0, 0, 0, 0, 0, 0),
                alias: "default_time".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::ToString {
                    expression: Box::new(localdatetime_expression(
                        2020, 1, 15, 12, 34, 56, 0, 0, 0,
                    )),
                },
                alias: "text".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_localdatetime_string_constructor_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (person:Person) \
         RETURN localdatetime('2020-01-15T12:34:56') AS d, \
                toString(localdatetime('2020-01-15T12:34:56')) AS text",
    )
    .expect("literal localdatetime string constructors should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: localdatetime_from_string_expression("2020-01-15T12:34:56"),
                alias: "d".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::ToString {
                    expression: Box::new(localdatetime_from_string_expression(
                        "2020-01-15T12:34:56",
                    )),
                },
                alias: "text".to_string(),
            },
        ]
    );
}

#[test]
fn rejects_unsupported_localdatetime_constructor_forms() {
    for (cypher, expected) in [
        (
            "MATCH (person:Person) RETURN localdatetime(person.name) AS d",
            "dynamic localdatetime() string argument not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN localdatetime('2020-01-15T12:34:56Z') AS d",
            "localdatetime() does not accept a timezone; use a naive date-time string",
        ),
        (
            "MATCH (person:Person) RETURN localdatetime('2020-01-15T12:34:56+01:00') AS d",
            "localdatetime() does not accept a timezone; use a naive date-time string",
        ),
        (
            "MATCH (person:Person) RETURN localdatetime(2020) AS d",
            "localdatetime() requires a literal map or string argument",
        ),
        (
            "MATCH (person:Person) RETURN localdatetime({year: person.age}) AS d",
            "dynamic temporal fields not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN localdatetime({month: 1, day: 15}) AS d",
            "localdatetime() map constructor requires a literal integer year",
        ),
        (
            "MATCH (person:Person) RETURN localdatetime({year: 2020, timezone: 'UTC'}) AS d",
            "localdatetime() temporal field 'timezone' is not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN localdatetime({year: 2020, date: date({year: 2020})}) AS d",
            "localdatetime() temporal field 'date' is not supported yet",
        ),
    ] {
        let error =
            compile_cypher(cypher).expect_err("unsupported localdatetime form should be rejected");
        assert!(
            error.to_string().contains(expected),
            "expected {expected:?}, got {error}"
        );
    }
}

#[test]
fn compiles_localtime_map_constructor_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (person:Person) \
         RETURN localtime({hour: 12, minute: 34, second: 56}) AS full, \
                localtime({hour: 12}) AS default_time, \
                toString(localtime({hour: 12, minute: 34, second: 56})) AS text",
    )
    .expect("literal localtime map constructors should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: localtime_expression(12, 34, 56, 0, 0, 0),
                alias: "full".to_string(),
            },
            Projection::Expression {
                expression: localtime_expression(12, 0, 0, 0, 0, 0),
                alias: "default_time".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::ToString {
                    expression: Box::new(localtime_expression(12, 34, 56, 0, 0, 0)),
                },
                alias: "text".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_localtime_string_constructor_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (person:Person) \
         RETURN localtime('12:34:56') AS t, \
                toString(localtime('12:34:56')) AS text",
    )
    .expect("literal localtime string constructors should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: localtime_from_string_expression("12:34:56"),
                alias: "t".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::ToString {
                    expression: Box::new(localtime_from_string_expression("12:34:56")),
                },
                alias: "text".to_string(),
            },
        ]
    );
}

#[test]
fn rejects_unsupported_localtime_constructor_forms() {
    for (cypher, expected) in [
        (
            "MATCH (person:Person) RETURN localtime(person.name) AS t",
            "dynamic localtime() string argument not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN localtime('12:34:56Z') AS t",
            "localtime() does not accept a timezone; use a naive time string",
        ),
        (
            "MATCH (person:Person) RETURN localtime('12:34:56+01:00') AS t",
            "localtime() does not accept a timezone; use a naive time string",
        ),
        (
            "MATCH (person:Person) RETURN localtime(12) AS t",
            "localtime() requires a literal map or string argument",
        ),
        (
            "MATCH (person:Person) RETURN localtime({hour: person.age}) AS t",
            "dynamic temporal fields not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN localtime({minute: 34, second: 56}) AS t",
            "localtime() map constructor requires a literal integer hour",
        ),
        (
            "MATCH (person:Person) RETURN localtime({hour: 12, timezone: 'UTC'}) AS t",
            "localtime() temporal field 'timezone' is not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN localtime({hour: 12, date: date({year: 2020})}) AS t",
            "localtime() temporal field 'date' is not supported yet",
        ),
    ] {
        let error =
            compile_cypher(cypher).expect_err("unsupported localtime form should be rejected");
        assert!(
            error.to_string().contains(expected),
            "expected {expected:?}, got {error}"
        );
    }
}

#[test]
fn compiles_constructed_temporal_component_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (person:Person) \
         RETURN date('2020-01-15').year AS year, \
                date('2020-01-15').month AS month, \
                date('2020-01-15').day AS day, \
                localdatetime('2020-01-15T12:34:56').hour AS hour, \
                localdatetime('2020-01-15T12:34:56').minute AS minute, \
                localdatetime('2020-01-15T12:34:56').second AS second, \
                localdatetime('2020-01-15T12:34:56.789123456').millisecond AS millisecond, \
                localdatetime('2020-01-15T12:34:56.789123456').microsecond AS microsecond, \
                localtime('12:34:56').hour AS timeHour",
    )
    .expect("constructed temporal component access should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: temporal_component_expression(
                    date_from_string_expression("2020-01-15"),
                    TemporalComponentUnit::Year,
                ),
                alias: "year".to_string(),
            },
            Projection::Expression {
                expression: temporal_component_expression(
                    date_from_string_expression("2020-01-15"),
                    TemporalComponentUnit::Month,
                ),
                alias: "month".to_string(),
            },
            Projection::Expression {
                expression: temporal_component_expression(
                    date_from_string_expression("2020-01-15"),
                    TemporalComponentUnit::Day,
                ),
                alias: "day".to_string(),
            },
            Projection::Expression {
                expression: temporal_component_expression(
                    localdatetime_from_string_expression("2020-01-15T12:34:56"),
                    TemporalComponentUnit::Hour,
                ),
                alias: "hour".to_string(),
            },
            Projection::Expression {
                expression: temporal_component_expression(
                    localdatetime_from_string_expression("2020-01-15T12:34:56"),
                    TemporalComponentUnit::Minute,
                ),
                alias: "minute".to_string(),
            },
            Projection::Expression {
                expression: temporal_component_expression(
                    localdatetime_from_string_expression("2020-01-15T12:34:56"),
                    TemporalComponentUnit::Second,
                ),
                alias: "second".to_string(),
            },
            Projection::Expression {
                expression: temporal_component_expression(
                    localdatetime_from_string_expression("2020-01-15T12:34:56.789123456"),
                    TemporalComponentUnit::Millisecond,
                ),
                alias: "millisecond".to_string(),
            },
            Projection::Expression {
                expression: temporal_component_expression(
                    localdatetime_from_string_expression("2020-01-15T12:34:56.789123456"),
                    TemporalComponentUnit::Microsecond,
                ),
                alias: "microsecond".to_string(),
            },
            Projection::Expression {
                expression: temporal_component_expression(
                    localtime_from_string_expression("12:34:56"),
                    TemporalComponentUnit::Hour,
                ),
                alias: "timeHour".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_stored_temporal_component_scalar_expressions_with_catalog() {
    let graph = temporal_columns_test_graph();
    let catalog = temporal_columns_catalog();
    let query = compile_cypher_query_for_graph_with_parameters_and_catalog(
        &graph,
        "MATCH (person:Person) \
         RETURN person.joined.year AS joinedYear, \
                person.birthday.month AS birthdayMonth",
        &BTreeMap::new(),
        &catalog,
    )
    .expect("stored temporal component access should compile with catalog types");
    let GraphQuery::Plan(plan) = query else {
        panic!("stored temporal component query should compile to one plan");
    };

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: temporal_component_expression(
                    ScalarExpression::Property(PropertyRef {
                        variable: "person".to_string(),
                        property: "joined".to_string(),
                    }),
                    TemporalComponentUnit::Year,
                ),
                alias: "joinedYear".to_string(),
            },
            Projection::Expression {
                expression: temporal_component_expression(
                    ScalarExpression::Property(PropertyRef {
                        variable: "person".to_string(),
                        property: "birthday".to_string(),
                    }),
                    TemporalComponentUnit::Month,
                ),
                alias: "birthdayMonth".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_terminal_with_stored_temporal_component_scalar_expression_with_catalog() {
    let graph = temporal_columns_test_graph();
    let catalog = temporal_columns_catalog();
    let query = compile_cypher_query_for_graph_with_parameters_and_catalog(
        &graph,
        "MATCH (person:Person) WITH person.joined AS t RETURN t.year AS year",
        &BTreeMap::new(),
        &catalog,
    )
    .expect("terminal WITH stored temporal component access should compile with catalog types");
    let GraphQuery::Plan(plan) = query else {
        panic!("terminal WITH stored temporal component query should compile to one plan");
    };

    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: temporal_component_expression(
                ScalarExpression::Property(PropertyRef {
                    variable: "person".to_string(),
                    property: "joined".to_string(),
                }),
                TemporalComponentUnit::Year,
            ),
            alias: "year".to_string(),
        }]
    );
}

#[test]
fn rejects_stored_temporal_component_kind_mismatch_with_catalog() {
    let graph = temporal_columns_test_graph();
    let catalog = temporal_columns_catalog();
    let error = compile_cypher_query_for_graph_with_parameters_and_catalog(
        &graph,
        "MATCH (person:Person) RETURN person.birthday.hour AS hour",
        &BTreeMap::new(),
        &catalog,
    )
    .expect_err("stored date hour component should reject");

    assert!(
        error
            .to_string()
            .contains("hour is not supported for date values"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_unsupported_temporal_component_access() {
    for (cypher, expected) in [
        (
            "MATCH (person:Person) RETURN date('2020-01-15').weekYear AS weekYear",
            "weekYear is not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN date('2020-01-15').weekDay AS weekDay",
            "weekDay is not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN date('2020-01-15').ordinalDay AS ordinalDay",
            "ordinalDay is not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN date('2020-01-15').dayOfQuarter AS dayOfQuarter",
            "dayOfQuarter is not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN localtime('12:34:56.789123456').nanosecond AS ns",
            "nanosecond is not supported yet",
        ),
        (
            "MATCH (person:Person) RETURN localtime('12:34:56').year AS year",
            "year is not supported for localtime values",
        ),
        (
            "MATCH (person:Person) WITH person.name AS d RETURN d.year AS year",
            "temporal component access requires a temporal value",
        ),
    ] {
        let error =
            compile_cypher(cypher).expect_err("unsupported temporal component should reject");
        assert!(
            error.to_string().contains(expected),
            "expected {expected:?}, got {error}"
        );
    }
}

#[test]
fn compiles_order_by_path_length() {
    let plan = compile_cypher(
        "MATCH path = (source:Service)-[:DEPENDS_ON*2]->(target:Service) \
             RETURN source.name AS source, target.name AS target \
             ORDER BY length(path) DESC",
    )
    .expect("path length ORDER BY should compile");

    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Literal(Literal::Integer(2)),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_path_length_predicates() {
    let plan = compile_cypher(
        "MATCH path = (source:Service)-[:DEPENDS_ON*2]->(target:Service) \
             WHERE length(path) = 2 AND length(path) IN [1, 2] \
             RETURN source.name AS source, target.name AS target",
    )
    .expect("path length predicates should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Literal(Literal::Integer(2)),
                operator: ComparisonOperator::Equal,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(2))),
            })),
            right: Box::new(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Literal(Literal::Integer(2)),
                operator: ComparisonOperator::In,
                rhs: ScalarPredicateRhs::List(vec![Literal::Integer(1), Literal::Integer(2),]),
            })),
        })
    );
}

#[test]
fn compiles_terminal_with_path_length_projection() {
    let plan = compile_cypher(
        "MATCH path = (source:Service)-[:DEPENDS_ON]->{2}(target:Service) \
             WITH source.name AS source, target.name AS target, length(path) AS hops \
             RETURN source, target, hops",
    )
    .expect("terminal WITH path length projection should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "source".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("source".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "target".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("target".to_string()),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Integer(2)),
                alias: "hops".to_string(),
            },
        ]
    );
}

#[test]
fn rejects_length_over_non_path_variable() {
    for cypher in [
        "MATCH (service:Service) RETURN length(service) AS length",
        "MATCH (service:Service) WHERE length(service) = 1 RETURN service.name AS service",
        "MATCH (service:Service) RETURN service.name AS service ORDER BY length(service)",
    ] {
        let error =
            compile_cypher(cypher).expect_err("length() should only accept bound path variables");

        assert!(
            error
                .to_string()
                .contains("length() argument 'service' is not a bound path variable"),
            "{error}"
        );
    }
}

#[test]
fn compiles_path_element_id_lists() {
    let plan = compile_cypher(
        "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             WHERE nodes(path) IS NOT NULL \
             RETURN nodes(path) AS nodes, relationships(path) AS relationships",
    )
    .expect("fixed path element id lists should compile");

    assert_eq!(
        plan.projection_output_names(),
        vec!["nodes", "relationships"]
    );
}

#[test]
fn compiles_path_element_list_indexes_and_endpoints_as_keys() {
    let plan = compile_cypher(
        "MATCH path = (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE nodes(path)[0] = id(person) \
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
    .expect("path element list scalar access should compile");

    let person_key = ScalarExpression::Key {
        variable: "person".to_string(),
    };
    let service_key = ScalarExpression::Key {
        variable: "service".to_string(),
    };
    let owns_key = ScalarExpression::Key {
        variable: "owns".to_string(),
    };

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: person_key.clone(),
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(person_key.clone()),
        }))
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: person_key.clone(),
                alias: "first_node".to_string(),
            },
            Projection::Expression {
                expression: service_key,
                alias: "last_node".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Null),
                alias: "missing_node".to_string(),
            },
            Projection::Expression {
                expression: owns_key.clone(),
                alias: "first_relationship".to_string(),
            },
            Projection::Expression {
                expression: owns_key.clone(),
                alias: "last_relationship".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Null),
                alias: "missing_relationship".to_string(),
            },
            Projection::Expression {
                expression: person_key.clone(),
                alias: "head_node".to_string(),
            },
            Projection::Expression {
                expression: owns_key.clone(),
                alias: "last_relationship_endpoint".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::Scalar(person_key),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Scalar(owns_key),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_path_element_list_slices_and_reducers_as_key_lists() {
    let plan = compile_cypher(
        "MATCH path = (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN nodes(path)[1..] AS node_tail_slice, \
                    nodes(path)[..1] AS node_prefix_slice, \
                    relationships(path)[..1] AS relationship_prefix_slice, \
                    tail(nodes(path)) AS node_tail, \
                    tail(relationships(path)) AS relationship_tail, \
                    reverse(nodes(path)) AS reversed_nodes, \
                    reverse(relationships(path)) AS reversed_relationships \
             ORDER BY nodes(path)[1..]",
    )
    .expect("path element list slices and reducers should compile");

    let person_key = "person".to_string();
    let service_key = "service".to_string();
    let owns_key = "owns".to_string();

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::GraphKeyList {
                    variables: vec![service_key.clone()],
                },
                alias: "node_tail_slice".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::GraphKeyList {
                    variables: vec![person_key.clone()],
                },
                alias: "node_prefix_slice".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::GraphKeyList {
                    variables: vec![owns_key.clone()],
                },
                alias: "relationship_prefix_slice".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::GraphKeyList {
                    variables: vec![service_key.clone()],
                },
                alias: "node_tail".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::GraphKeyList {
                    variables: Vec::new(),
                },
                alias: "relationship_tail".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::GraphKeyList {
                    variables: vec![service_key.clone(), person_key],
                },
                alias: "reversed_nodes".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::GraphKeyList {
                    variables: vec![owns_key],
                },
                alias: "reversed_relationships".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::GraphKeyList {
                variables: vec![service_key],
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn rejects_path_element_list_size_over_non_path_variable() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             RETURN size(nodes(service)) AS node_count",
    )
    .expect_err("nodes() should require a bound path variable");

    assert!(
        error
            .to_string()
            .contains("nodes() argument 'service' is not a bound path variable"),
        "{error}"
    );
}

#[test]
fn compiles_size_over_named_optional_path_variable() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH path = (service)-[dependency:DEPENDS_ON]->(target:Service) \
             RETURN service.name AS service, size(path) AS path_length \
             ORDER BY size(path)",
    )
    .expect("size(path) should preserve optional path length nullability");

    let expected = ScalarExpression::PresenceGated {
        presence_variable: "dependency".to_string(),
        expression: Box::new(ScalarExpression::Literal(Literal::Integer(1))),
    };
    assert_eq!(
        plan.projections.get(1),
        Some(&Projection::Expression {
            expression: expected.clone(),
            alias: "path_length".to_string(),
        })
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(expected),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_size_over_optional_path_element_lists() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH path = (service)-[dependency:DEPENDS_ON]->(target:Service) \
             RETURN service.name AS service, \
                    size(nodes(path)) AS node_count, \
                    size(relationships(path)) AS relationship_count \
             ORDER BY size(nodes(path))",
    )
    .expect("optional path element-list sizes should preserve nullability");

    let expected_node_count = ScalarExpression::PresenceGated {
        presence_variable: "dependency".to_string(),
        expression: Box::new(ScalarExpression::Literal(Literal::Integer(2))),
    };
    let expected_relationship_count = ScalarExpression::PresenceGated {
        presence_variable: "dependency".to_string(),
        expression: Box::new(ScalarExpression::Literal(Literal::Integer(1))),
    };
    assert_eq!(
        plan.projections.get(1),
        Some(&Projection::Expression {
            expression: expected_node_count.clone(),
            alias: "node_count".to_string(),
        })
    );
    assert_eq!(
        plan.projections.get(2),
        Some(&Projection::Expression {
            expression: expected_relationship_count,
            alias: "relationship_count".to_string(),
        })
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(expected_node_count),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_length_over_named_optional_path_variable() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH path = (service)-[dependency:DEPENDS_ON]->(target:Service) \
             RETURN service.name AS service, length(path) AS path_length \
             ORDER BY length(path)",
    )
    .expect("named optional path length should compile");

    let expected = ScalarExpression::PresenceGated {
        presence_variable: "dependency".to_string(),
        expression: Box::new(ScalarExpression::Literal(Literal::Integer(1))),
    };
    assert_eq!(
        plan.projections.get(1),
        Some(&Projection::Expression {
            expression: expected.clone(),
            alias: "path_length".to_string(),
        })
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(expected),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_length_over_anonymous_optional_path_variable() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH path = (service)-[:DEPENDS_ON]->(target:Service) \
             RETURN service.name AS service, length(path) AS path_length \
             ORDER BY length(path)",
    )
    .expect("anonymous optional path length should compile with an internal presence variable");

    let presence_variable = plan
        .relationships
        .first()
        .expect("anonymous optional relationship should compile")
        .variable
        .as_ref()
        .expect("anonymous optional relationship should receive an internal variable")
        .clone();
    assert!(presence_variable.starts_with("__coral_rel_"));
    let expected = ScalarExpression::PresenceGated {
        presence_variable,
        expression: Box::new(ScalarExpression::Literal(Literal::Integer(1))),
    };
    assert_eq!(
        plan.projections.get(1),
        Some(&Projection::Expression {
            expression: expected.clone(),
            alias: "path_length".to_string(),
        })
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(expected),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_length_over_optional_zero_hop_path_to_new_endpoint() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH path = (service)-[:DEPENDS_ON*0]->(self:Service) \
             RETURN service.name AS service, self.name AS self, length(path) AS path_length \
             ORDER BY size(path)",
    )
    .expect("deterministic optional zero-hop path length should compile");

    assert!(plan.relationships.is_empty());
    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::KeyComparison(KeyPredicate {
            variable: "service".to_string(),
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Key {
                variable: "self".to_string(),
            },
        }))
    );
    assert_eq!(
        plan.projections.get(2),
        Some(&Projection::Expression {
            expression: ScalarExpression::Literal(Literal::Integer(0)),
            alias: "path_length".to_string(),
        })
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Literal(Literal::Integer(0)),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_length_over_optional_zero_hop_path_to_same_bound_endpoint() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH path = (service)-[:DEPENDS_ON*0]->(service) \
             RETURN service.name AS service, length(path) AS path_length",
    )
    .expect("same-bound optional zero-hop path length should compile");

    assert!(plan.relationships.is_empty());
    assert_eq!(
        plan.projections.get(1),
        Some(&Projection::Expression {
            expression: ScalarExpression::Literal(Literal::Integer(0)),
            alias: "path_length".to_string(),
        })
    );
}

#[test]
fn compiles_optional_zero_hop_path_length_between_distinct_bound_endpoints() {
    let plan = compile_cypher(
        "MATCH (source:Service), (target:Service) \
             OPTIONAL MATCH path = (source)-[:DEPENDS_ON*0]->(target) \
             RETURN length(path) AS path_length, size(path) AS path_size \
             ORDER BY length(path)",
    )
    .expect("bound endpoint zero-hop path length should compile as equality-gated metadata");

    let expected = ScalarExpression::Case {
        alternatives: vec![ScalarCaseAlternative {
            when: PredicateExpression::KeyComparison(KeyPredicate {
                variable: "source".to_string(),
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Key {
                    variable: "target".to_string(),
                },
            }),
            then: ScalarExpression::Literal(Literal::Integer(0)),
        }],
        else_expression: Some(Box::new(ScalarExpression::Literal(Literal::Null))),
    };
    assert_eq!(
        plan.projections.first(),
        Some(&Projection::Expression {
            expression: expected.clone(),
            alias: "path_length".to_string(),
        })
    );
    assert_eq!(
        plan.projections.get(1),
        Some(&Projection::Expression {
            expression: expected.clone(),
            alias: "path_size".to_string(),
        })
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(expected),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_optional_zero_hop_path_length_between_bound_cross_label_endpoints() {
    let plan = compile_cypher(
        "MATCH (source:Service), (person:Person) \
             OPTIONAL MATCH path = (source)-[:DEPENDS_ON*0]->(person) \
             RETURN length(path) AS path_length",
    )
    .expect("bound cross-label zero-hop path length should compile as null metadata");

    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::Case {
                alternatives: vec![ScalarCaseAlternative {
                    when: PredicateExpression::Boolean(false),
                    then: ScalarExpression::Literal(Literal::Integer(0)),
                }],
                else_expression: Some(Box::new(ScalarExpression::Literal(Literal::Null))),
            },
            alias: "path_length".to_string(),
        }]
    );
}

#[test]
fn compiles_optional_zero_hop_local_predicates_into_path_presence_gate() {
    let plan = compile_cypher(
        "MATCH (source:Service), (target:Service) \
             OPTIONAL MATCH path = (source)-[:DEPENDS_ON*0]->(target) \
             WHERE source.tier = 'prod' \
             RETURN length(path) AS path_length",
    )
    .expect("bound endpoint zero-hop local predicate should gate path metadata");

    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::Case {
                alternatives: vec![ScalarCaseAlternative {
                    when: PredicateExpression::And {
                        left: Box::new(PredicateExpression::KeyComparison(KeyPredicate {
                            variable: "source".to_string(),
                            operator: ComparisonOperator::Equal,
                            rhs: PredicateRhs::Key {
                                variable: "target".to_string(),
                            },
                        })),
                        right: Box::new(PredicateExpression::Comparison(PropertyPredicate {
                            property: PropertyRef {
                                variable: "source".to_string(),
                                property: "tier".to_string(),
                            },
                            operator: ComparisonOperator::Equal,
                            rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
                        })),
                    },
                    then: ScalarExpression::Literal(Literal::Integer(0)),
                }],
                else_expression: Some(Box::new(ScalarExpression::Literal(Literal::Null))),
            },
            alias: "path_length".to_string(),
        }]
    );
}

#[test]
fn rejects_optional_zero_hop_local_predicates_with_introduced_endpoint() {
    let error = compile_cypher(
        "MATCH (source:Service) \
             OPTIONAL MATCH path = (source)-[:DEPENDS_ON*0]->(self:Service) \
             WHERE self.tier = 'prod' \
             RETURN length(path) AS path_length",
    )
    .expect_err("introduced zero-hop endpoint would require nullable node binding");

    assert!(
        error
            .to_string()
            .contains("nullable zero-hop endpoint binding"),
        "{error}"
    );
}

#[test]
fn anonymous_optional_path_presence_bindings_stay_hidden_from_with_scope() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH path = (service)-[:DEPENDS_ON]->(target:Service) \
             WITH service \
             RETURN service.name AS service",
    )
    .expect("transparent WITH should not require generated optional path bindings");

    assert!(
        plan.relationships
            .iter()
            .filter_map(|relationship| relationship.variable.as_deref())
            .any(|variable| variable.starts_with("__coral_rel_")),
        "anonymous optional path should still have an internal presence binding"
    );
    assert_eq!(
        plan.projections,
        vec![Projection::Property {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            },
            alias: Some("service".to_string()),
        }]
    );
}

#[test]
fn rejects_path_variable_collisions() {
    let error = compile_cypher(
        "MATCH path = (path:Person)-[:OWNS]->(service:Service) \
             RETURN service.name AS service",
    )
    .expect_err("path bindings must not collide with graph variables");

    assert!(
        error.to_string().contains("path variable 'path' conflicts"),
        "{error}"
    );
}

#[test]
fn rejects_graph_variables_that_shadow_in_scope_path_variables() {
    let error = compile_cypher(
        "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             MATCH (path:Person) \
             RETURN path.name AS person",
    )
    .expect_err("graph variables must not shadow in-scope path variables");

    assert!(
        error
            .to_string()
            .contains("graph variable 'path' conflicts with an in-scope path variable"),
        "{error}"
    );
}

#[test]
fn explicit_with_drops_path_variables() {
    let plan = compile_cypher(
        "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             WITH person, service \
             MATCH (path:Person) \
             RETURN path.name AS person",
    )
    .expect("explicit WITH should drop unsupported path values");

    assert!(plan.nodes.iter().any(|node| node.variable == "path"));
}

#[test]
fn compiles_with_star_over_path_variables() {
    let plan = compile_cypher(
        "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             WITH * \
             RETURN person.name AS owner, length(path) AS hops, size(path) AS path_size",
    )
    .expect("WITH * should carry non-materialized path metadata");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "person".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("owner".to_string()),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Integer(1)),
                alias: "hops".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Integer(1)),
                alias: "path_size".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_with_star_where_over_path_metadata() {
    let plan = compile_cypher(
        "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             WITH * WHERE length(path) = 1 AND size(path) = 1 \
             RETURN person.name AS owner",
    )
    .expect("WITH * WHERE should see non-materialized path metadata");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Literal(Literal::Integer(1)),
                operator: ComparisonOperator::Equal,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(1))),
            })),
            right: Box::new(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Literal(Literal::Integer(1)),
                operator: ComparisonOperator::Equal,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(1))),
            })),
        })
    );
}

#[test]
fn rejects_explicit_with_where_over_dropped_path_metadata() {
    let error = compile_cypher(
        "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             WITH person, service WHERE length(path) = 1 \
             RETURN person.name AS owner",
    )
    .expect_err("explicit WITH should drop path metadata before WHERE");

    assert!(
        error
            .to_string()
            .contains("path variable 'path' is not in scope after WITH"),
        "{error}"
    );
}

#[test]
fn rejects_with_star_path_variable_shadowing() {
    let error = compile_cypher(
        "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             WITH * \
             MATCH (path:Person) \
             RETURN path.name AS person",
    )
    .expect_err("WITH * should keep path variable names in scope");

    assert!(
        error
            .to_string()
            .contains("graph variable 'path' conflicts with an in-scope path variable"),
        "{error}"
    );
}

#[test]
fn rejects_path_value_property_projections() {
    let error = compile_cypher(
        "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             RETURN path.name AS path_name",
    )
    .expect_err("path values should not be projected as graph properties");

    assert_path_value_error(&error);
}

#[test]
fn rejects_path_value_property_predicates() {
    let error = compile_cypher(
        "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             WHERE path.name = 'x' \
             RETURN person.name AS owner",
    )
    .expect_err("path values should not be filtered as graph properties");

    assert_path_value_error(&error);
}

#[test]
fn rejects_path_value_property_ordering() {
    let error = compile_cypher(
        "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             RETURN person.name AS owner \
             ORDER BY path.name",
    )
    .expect_err("path values should not be ordered as graph properties");

    assert_path_value_error(&error);
}

#[test]
fn rejects_transparent_with_path_value_predicates_before_dropping_path_values() {
    let error = compile_cypher(
        "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             WITH person, service WHERE path.name = 'x' \
             RETURN person.name AS owner",
    )
    .expect_err("transparent WITH should reject path values before dropping them");

    assert!(
        error
            .to_string()
            .contains("path variable 'path' is not in scope after WITH"),
        "{error}"
    );
}

fn assert_path_value_error(error: &CoreError) {
    assert!(
        error
            .to_string()
            .contains("path variable 'path' cannot be used as a graph value"),
        "{error}"
    );
}

#[test]
fn compiles_transparent_with_pass_through() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WITH service \
             MATCH (service)-[:DEPENDS_ON]->(target:Service) \
             RETURN service.name AS source, target.name AS target \
             ORDER BY source, target",
    )
    .expect("transparent WITH query should compile");

    assert_eq!(
        plan.nodes,
        vec![
            NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "target".to_string(),
                label: "Service".to_string(),
            },
        ]
    );
    assert_eq!(plan.relationships.len(), 1);
    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("source".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "target".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("target".to_string()),
            },
        ]
    );
}

#[test]
fn compiles_transparent_with_where_pass_through() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WITH service \
             WHERE service.tier = 'prod' \
             MATCH (service)-[:DEPENDS_ON]->(target:Service) \
             RETURN service.name AS source, target.name AS target",
    )
    .expect("transparent WITH WHERE query should compile");

    assert_eq!(plan.nodes.len(), 2);
    assert_eq!(plan.relationships.len(), 1);
    assert_eq!(
        plan.predicates,
        vec![PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
        }]
    );
}

#[test]
fn compiles_transparent_with_variable_aliases() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WITH service AS s \
             WHERE s.tier = 'prod' \
             MATCH (s)-[:DEPENDS_ON]->(target:Service) \
             RETURN s.name AS source, target.name AS target",
    )
    .expect("transparent WITH aliases should compile");

    assert_eq!(
        plan.nodes,
        vec![
            NodePattern {
                variable: "s".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "target".to_string(),
                label: "Service".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "DEPENDS_ON".to_string(),
            left: "s".to_string(),
            direction: Direction::Outgoing,
            right: "target".to_string(),
        }]
    );
    assert_eq!(
        plan.predicates,
        vec![PropertyPredicate {
            property: PropertyRef {
                variable: "s".to_string(),
                property: "tier".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
        }]
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "s".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("source".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "target".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("target".to_string()),
            },
        ]
    );
}

#[test]
fn compiles_nonterminal_with_scalar_aliases() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WITH service, service.name AS source_name \
             MATCH (service)-[:DEPENDS_ON]->(target:Service) \
             RETURN source_name, target.name AS target \
             ORDER BY source_name, target",
    )
    .expect("non-terminal WITH scalar aliases should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("source_name".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "target".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("target".to_string()),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "target".to_string(),
                    property: "name".to_string(),
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_nonterminal_with_scalar_alias_predicates_and_hidden_ordering() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WITH service, service.name AS source_name \
             WHERE source_name STARTS WITH 'billing' \
             MATCH (service)-[:DEPENDS_ON]->(target:Service) \
             RETURN target.name AS target \
             ORDER BY source_name, target",
    )
    .expect("WITH scalar aliases should work in WITH WHERE and hidden ORDER BY");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            }),
            operator: ComparisonOperator::StartsWith,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
                "billing".to_string()
            ))),
        }))
    );
    assert_eq!(
        plan.order_by.first().map(|key| &key.expression),
        Some(&OrderExpression::Scalar(ScalarExpression::Property(
            PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            }
        )))
    );
}

#[test]
fn compiles_nonterminal_with_scalar_aliases_from_dropped_graph_variables() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[:OWNS]->(service:Service) \
             WITH service, person.name AS owner \
             MATCH (service)-[:DEPENDS_ON]->(target:Service) \
             RETURN owner, target.name AS target",
    )
    .expect("WITH scalar aliases may preserve dropped graph-variable values");

    let owner = plan
        .projections
        .first()
        .expect("owner projection should exist");
    let Projection::Property {
        property,
        alias: Some(alias),
    } = owner
    else {
        panic!("expected owner property projection, got {owner:?}");
    };
    assert_eq!(alias, "owner");
    assert_eq!(property.property, "name");
    assert!(
        property.variable.starts_with("__coral_hidden_person"),
        "{property:?}"
    );
}

#[test]
fn compiles_nonterminal_with_star_scalar_aliases() {
    let plan = compile_cypher(
        "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             WITH *, service.name AS source_name, length(path) AS hops \
             WHERE source_name STARTS WITH 'billing' AND hops = 1 \
             MATCH (service)-[:DEPENDS_ON]->(target:Service) \
             RETURN source_name, hops, target.name AS target \
             ORDER BY hops, source_name, target",
    )
    .expect("WITH * scalar aliases should compile before later MATCH");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("source_name".to_string()),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Integer(1)),
                alias: "hops".to_string(),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "target".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("target".to_string()),
            },
        ]
    );
    assert_eq!(
        plan.order_by
            .iter()
            .map(|key| &key.expression)
            .collect::<Vec<_>>(),
        vec![
            &OrderExpression::ProjectionAlias("hops".to_string()),
            &OrderExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            }),
            &OrderExpression::Property(PropertyRef {
                variable: "target".to_string(),
                property: "name".to_string(),
            }),
        ]
    );
}

#[test]
fn compiles_transparent_with_relationship_variable_aliases() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WITH person AS p, owns AS rel, service AS s \
             RETURN p.name AS owner, type(rel) AS relationship_type, s.name AS service",
    )
    .expect("transparent WITH relationship aliases should compile");

    assert_eq!(
        plan.nodes,
        vec![
            NodePattern {
                variable: "p".to_string(),
                label: "Person".to_string(),
            },
            NodePattern {
                variable: "s".to_string(),
                label: "Service".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.relationships,
        vec![RelationshipPattern {
            variable: Some("rel".to_string()),
            relationship_type: "OWNS".to_string(),
            left: "p".to_string(),
            direction: Direction::Outgoing,
            right: "s".to_string(),
        }]
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "p".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("owner".to_string()),
            },
            Projection::RelationshipType {
                variable: "rel".to_string(),
                relationship_type: "OWNS".to_string(),
                alias: "relationship_type".to_string(),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "s".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("service".to_string()),
            },
        ]
    );
}

#[test]
fn compiles_transparent_with_dropped_variables() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[:OWNS]->(service:Service) \
             WITH service \
             MATCH (service)-[:DEPENDS_ON]->(target:Service) \
             RETURN service.name AS source, target.name AS target",
    )
    .expect("transparent WITH should allow dropping graph variables");

    assert_eq!(plan.nodes.len(), 3);
    assert!(
        plan.nodes
            .iter()
            .any(|node| node.variable.starts_with("__coral_hidden_person")),
        "{:?}",
        plan.nodes
    );
    assert!(
        plan.relationships
            .first()
            .is_some_and(|relationship| relationship.left.starts_with("__coral_hidden_person")),
        "{:?}",
        plan.relationships
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("source".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "target".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("target".to_string()),
            },
        ]
    );
}

#[test]
fn compiles_transparent_with_rebound_dropped_variable_name() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[:OWNS]->(service:Service) \
             WITH service \
             MATCH (person:Person)-[:OWNS]->(service) \
             RETURN person.name AS owner, service.name AS service",
    )
    .expect("dropped variable names should be reusable after transparent WITH");

    assert!(
        plan.nodes
            .iter()
            .any(|node| node.variable.starts_with("__coral_hidden_person")),
        "{:?}",
        plan.nodes
    );
    assert!(plan.nodes.iter().any(|node| node.variable == "person"));
    assert_eq!(plan.relationships.len(), 2);
}

#[test]
fn compiles_transparent_with_star_pass_through() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WITH * \
             MATCH (service)-[:DEPENDS_ON]->(target:Service) \
             RETURN service.name AS source, target.name AS target",
    )
    .expect("transparent WITH * query should compile");

    assert_eq!(plan.nodes.len(), 2);
    assert_eq!(plan.relationships.len(), 1);
    assert_eq!(plan.projections.len(), 2);
}

#[test]
fn compiles_transparent_with_star_where_pass_through() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WITH * \
             WHERE service.active = true \
             MATCH (service)-[:DEPENDS_ON]->(target:Service) \
             RETURN service.name AS source, target.name AS target",
    )
    .expect("transparent WITH * WHERE query should compile");

    assert_eq!(plan.nodes.len(), 2);
    assert_eq!(plan.relationships.len(), 1);
    assert_eq!(
        plan.predicates,
        vec![PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "active".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Literal(Literal::Boolean(true)),
        }]
    );
}

#[test]
fn compiles_transparent_with_before_return() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WITH service \
             RETURN service.name AS service \
             ORDER BY service",
    )
    .expect("transparent WITH before RETURN should compile");

    assert_eq!(plan.nodes.len(), 1);
    assert_eq!(plan.relationships.len(), 0);
    assert_eq!(plan.projections.len(), 1);
}

#[test]
fn compiles_multihop_optional_match_scope() {
    let plan = compile_cypher(
            "MATCH (service:Service) \
             OPTIONAL MATCH (service)-[:DEPENDS_ON]->(middle:Service)-[:DEPENDS_ON]->(target:Service) \
             RETURN service.name AS service, middle.name AS middle, target.name AS target",
        )
        .expect("multi-hop optional match should compile");

    assert_eq!(plan.relationships.len(), 2);
    assert_eq!(plan.optional_relationships, vec![0, 1]);
    assert_eq!(plan.optional_matches.len(), 1);
    assert_eq!(
        plan.optional_matches
            .first()
            .expect("optional match scope")
            .relationship_indices,
        vec![0, 1]
    );
}

#[test]
fn compiles_multihop_optional_match_between_bound_endpoints_scope() {
    let plan = compile_cypher(
        "MATCH (source:Service), (target:Service) \
             OPTIONAL MATCH (source)-[:DEPENDS_ON]->(middle:Service)-[:DEPENDS_ON]->(target) \
             RETURN source.name AS source, target.name AS target, middle.name AS middle",
    )
    .expect("bound-endpoint multi-hop optional match should compile");

    assert_eq!(
        plan.nodes
            .iter()
            .map(|node| node.variable.as_str())
            .collect::<Vec<_>>(),
        vec!["source", "target", "middle"]
    );
    assert_eq!(plan.relationships.len(), 2);
    assert_eq!(plan.optional_relationships, vec![0, 1]);
    assert_eq!(
        plan.optional_matches,
        vec![OptionalMatchScope {
            node_indices: vec![2],
            relationship_indices: vec![0, 1],
            predicate: None,
        }]
    );
}

#[test]
fn compiles_multiple_match_clauses() {
    let plan = compile_cypher(
        "MATCH (person:Person) \
             WHERE person.team = 'platform' \
             MATCH (person)-[:OWNS]->(service:Service) \
             WHERE service.tier = 'prod' \
             RETURN person.name AS owner, service.name AS service",
    )
    .expect("multiple MATCH clauses should compile");

    assert_eq!(
        plan.nodes,
        vec![
            NodePattern {
                variable: "person".to_string(),
                label: "Person".to_string(),
            },
            NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            },
        ]
    );
    assert_eq!(plan.relationships.len(), 1);
    assert_eq!(plan.predicates.len(), 2);
}

#[test]
fn compiles_terminal_with_projection_aliases() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[:OWNS]->(service:Service) \
             WITH person.name AS owner, count(service) AS services \
             RETURN owner, services \
             ORDER BY services DESC, owner \
             LIMIT 10",
    )
    .expect("terminal WITH projection query should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "person".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("owner".to_string()),
            },
            Projection::Aggregate {
                function: AggregateFunction::Count,
                target: AggregateTarget::VariableKey {
                    variable: "service".to_string(),
                },
                distinct: false,
                alias: "services".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::ProjectionAlias("services".to_string()),
                direction: OrderDirection::Descending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "person".to_string(),
                    property: "name".to_string(),
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
    assert_eq!(plan.limit, Some(10));
}

#[test]
fn compiles_terminal_with_final_return_aliases() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WITH service.tier AS tier, count(service) AS services \
             RETURN tier AS service_tier, services AS total_services \
             ORDER BY total_services DESC, service_tier",
    )
    .expect("terminal WITH final RETURN aliases should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                alias: Some("service_tier".to_string()),
            },
            Projection::Aggregate {
                function: AggregateFunction::Count,
                target: AggregateTarget::VariableKey {
                    variable: "service".to_string(),
                },
                distinct: false,
                alias: "total_services".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::ProjectionAlias("total_services".to_string()),
                direction: OrderDirection::Descending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_terminal_with_reordered_final_return_aliases() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WITH service.tier AS tier, count(service) AS services \
             RETURN services AS total_services, tier AS service_tier \
             ORDER BY total_services DESC, service_tier",
    )
    .expect("terminal WITH final RETURN aliases should reorder projections");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Aggregate {
                function: AggregateFunction::Count,
                target: AggregateTarget::VariableKey {
                    variable: "service".to_string(),
                },
                distinct: false,
                alias: "total_services".to_string(),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                alias: Some("service_tier".to_string()),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::ProjectionAlias("total_services".to_string()),
                direction: OrderDirection::Descending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_terminal_with_return_star_alias_passthrough() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WITH service.tier AS tier, count(service) AS services \
             RETURN * \
             ORDER BY services DESC, tier",
    )
    .expect("terminal WITH RETURN * should pass through scalar aliases");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                alias: Some("tier".to_string()),
            },
            Projection::Aggregate {
                function: AggregateFunction::Count,
                target: AggregateTarget::VariableKey {
                    variable: "service".to_string(),
                },
                distinct: false,
                alias: "services".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::ProjectionAlias("services".to_string()),
                direction: OrderDirection::Descending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_terminal_with_star_and_explicit_projection_aliases() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WITH *, service.name AS name, service.tier AS tier \
             RETURN tier AS service_tier, name AS service_name \
             ORDER BY service_name",
    )
    .expect("terminal WITH * explicit projection aliases should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                alias: Some("service_tier".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("service_name".to_string()),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_terminal_with_star_return_star_and_explicit_projection_aliases() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             WITH *, service.name AS name \
             RETURN * \
             ORDER BY name",
    )
    .expect("terminal WITH * RETURN * explicit aliases should compile");

    assert_eq!(
        plan.projection_output_names(),
        vec![
            "service.__id",
            "service.__labels",
            "service.name",
            "service.tier",
            "name",
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn rejects_terminal_with_star_return_star_with_aggregate_aliases() {
    let graph = star_test_graph();
    let error = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             WITH *, count(service) AS services \
             RETURN *",
    )
    .expect_err("terminal WITH * RETURN * aggregate aliases require grouping");

    assert!(
        error.to_string().contains("aggregate aliases"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_terminal_with_star_return_star_duplicate_aliases() {
    let graph = star_test_graph();
    let error = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             WITH *, service.name AS `service.name` \
             RETURN *",
    )
    .expect_err("terminal WITH * RETURN * duplicate aliases should fail");

    assert!(
        error.to_string().contains("duplicate output column"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_terminal_with_star_return_star_over_path_variables() {
    let graph = star_test_graph();
    let error = compile_cypher_for_graph(
        &graph,
        "MATCH path = (person:Person)-[ownership:OWNS]->(service:Service) \
             WITH *, service.name AS service_name \
             RETURN *",
    )
    .expect_err("terminal WITH * RETURN * should reject unmaterialized path values");

    assert!(
        error
            .to_string()
            .contains("RETURN * cannot carry path variables"),
        "unexpected error: {error}"
    );
}

#[test]
fn compiles_terminal_with_scalar_where_alias_predicates() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[:OWNS]->(service:Service) \
             WITH person.name AS owner, service.tier AS tier \
             WHERE owner STARTS WITH 'Ada' AND tier IN ['prod', 'critical'] \
             RETURN owner, tier",
    )
    .expect("terminal WITH scalar WHERE should compile");

    assert_eq!(
        plan.post_projection_predicate,
        Some(ProjectionPredicateExpression::And {
            left: Box::new(ProjectionPredicateExpression::Comparison(
                ProjectionPredicate {
                    alias: "owner".to_string(),
                    operator: ComparisonOperator::StartsWith,
                    rhs: ProjectionPredicateRhs::Literal(Literal::String("Ada".to_string())),
                },
            )),
            right: Box::new(ProjectionPredicateExpression::Comparison(
                ProjectionPredicate {
                    alias: "tier".to_string(),
                    operator: ComparisonOperator::In,
                    rhs: ProjectionPredicateRhs::List(vec![
                        Literal::String("prod".to_string()),
                        Literal::String("critical".to_string()),
                    ]),
                },
            )),
        })
    );
}

#[test]
fn compiles_terminal_with_aggregate_where_alias_predicates() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[:OWNS]->(service:Service) \
             WITH person.team AS team, count(service) AS services \
             WHERE services > 1 AND team IS NOT NULL \
             RETURN team, services",
    )
    .expect("terminal WITH aggregate WHERE should compile");

    assert_eq!(
        plan.post_projection_predicate,
        Some(ProjectionPredicateExpression::And {
            left: Box::new(ProjectionPredicateExpression::Comparison(
                ProjectionPredicate {
                    alias: "services".to_string(),
                    operator: ComparisonOperator::GreaterThan,
                    rhs: ProjectionPredicateRhs::Literal(Literal::Integer(1)),
                },
            )),
            right: Box::new(ProjectionPredicateExpression::Comparison(
                ProjectionPredicate {
                    alias: "team".to_string(),
                    operator: ComparisonOperator::NotEqual,
                    rhs: ProjectionPredicateRhs::Literal(Literal::Null),
                },
            )),
        })
    );
}

#[test]
fn compiles_terminal_with_distinct_property_projection() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WITH DISTINCT service.tier AS tier \
             RETURN tier \
             ORDER BY tier",
    )
    .expect("terminal WITH DISTINCT projection query should compile");

    assert!(plan.distinct);
    assert_eq!(
        plan.projections,
        vec![Projection::Property {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            },
            alias: Some("tier".to_string()),
        }]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_terminal_with_order_skip_limit() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WITH service.tier AS tier, count(service) AS services \
             ORDER BY services DESC, tier \
             SKIP 1 \
             LIMIT 5 \
             RETURN tier, services",
    )
    .expect("terminal WITH modifiers should compile");

    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::ProjectionAlias("services".to_string()),
                direction: OrderDirection::Descending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
    assert_eq!(plan.skip, Some(1));
    assert_eq!(plan.limit, Some(5));
}

#[test]
fn compiles_terminal_with_graph_variable_modifiers() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WITH service AS s \
             ORDER BY s.risk DESC \
             SKIP 1 \
             LIMIT 2 \
             RETURN s.name AS service, s.risk AS risk",
    )
    .expect("terminal WITH graph variable modifiers should compile");

    assert_eq!(
        plan.nodes,
        vec![NodePattern {
            variable: "s".to_string(),
            label: "Service".to_string(),
        }]
    );
    assert_eq!(plan.predicates, Vec::new());
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "s".to_string(),
                property: "risk".to_string(),
            }),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
    assert_eq!(plan.skip, Some(1));
    assert_eq!(plan.limit, Some(2));
    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "s".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("service".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "s".to_string(),
                    property: "risk".to_string(),
                },
                alias: Some("risk".to_string()),
            },
        ]
    );
}

#[test]
fn compiles_terminal_with_distinct_graph_variable_return() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (:Person)-[:OWNS]->(target:Service) \
             WITH DISTINCT target AS t \
             ORDER BY t.name \
             RETURN t",
    )
    .expect("terminal WITH DISTINCT graph variable return should compile");

    assert!(plan.distinct);
    assert_eq!(plan.nodes.len(), 2);
    assert!(
        plan.nodes
            .iter()
            .any(|node| { node.variable.starts_with("__coral_hidden_") && node.label == "Person" })
    );
    assert!(
        plan.nodes
            .iter()
            .any(|node| node.variable == "t" && node.label == "Service")
    );
    assert_eq!(
        plan.projection_output_names(),
        vec!["t.__id", "t.__labels", "t.name", "t.tier"]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "t".to_string(),
                property: "name".to_string(),
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn rejects_terminal_with_distinct_graph_variable_scalar_return() {
    let error = compile_cypher(
        "MATCH (:Service)-[:DEPENDS_ON]->(target:Service) \
             WITH DISTINCT target \
             RETURN target.name AS target",
    )
    .expect_err("scalar projection after graph-variable WITH DISTINCT should be rejected");

    assert!(
        error
            .to_string()
            .contains("scalar projections require staged query planning"),
        "{error}"
    );
}

#[test]
fn compiles_terminal_with_star_modifiers() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WITH * \
             ORDER BY service.risk DESC \
             LIMIT 1 \
             RETURN service.name AS service",
    )
    .expect("terminal WITH * modifiers should compile");

    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "risk".to_string(),
            }),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
    assert_eq!(plan.limit, Some(1));
    assert_eq!(
        plan.projections,
        vec![Projection::Property {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            },
            alias: Some("service".to_string()),
        }]
    );
}

#[test]
fn compiles_staged_with_order_limit_before_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person) \
             WITH a ORDER BY a.age LIMIT 2 \
             MATCH (a)-[:KNOWS]->(b:Person) \
             RETURN a.name AS a, b.name AS b",
    )
    .expect("staged WITH ORDER BY LIMIT before MATCH should compile");

    let GraphQuery::Staged(staged) = query else {
        panic!("target query should compile to a staged graph query");
    };
    assert_eq!(staged.stages.len(), 1);
    let stage = staged
        .stages
        .first()
        .expect("staged query should have stage 0");
    assert_eq!(
        stage.exports,
        vec![GraphStageExport::NodeKey {
            variable: "a".to_string(),
            column: "a_id".to_string(),
        }]
    );
    assert_eq!(
        stage.plan.projections,
        vec![Projection::Key {
            variable: "a".to_string(),
            alias: "a_id".to_string(),
        }]
    );
    assert_eq!(
        stage.plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "a".to_string(),
                property: "age".to_string(),
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
    assert_eq!(stage.plan.limit, Some(2));
    assert_eq!(
        staged.final_plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "KNOWS".to_string(),
            left: "a".to_string(),
            direction: Direction::Outgoing,
            right: "b".to_string(),
        }]
    );
    assert_eq!(
        staged.final_plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "a".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("a".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "b".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("b".to_string()),
            },
        ]
    );
}

#[test]
fn compiles_staged_with_incoming_final_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person) \
             WITH a ORDER BY a.age LIMIT 2 \
             MATCH (b:Person)-[:KNOWS]->(a) \
             RETURN a.name AS a, b.name AS b",
    )
    .expect("staged route should allow incoming final matches into carried variables");

    let GraphQuery::Staged(staged) = query else {
        panic!("incoming final match should compile to a staged graph query");
    };
    assert_eq!(
        staged.final_plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "KNOWS".to_string(),
            left: "b".to_string(),
            direction: Direction::Outgoing,
            right: "a".to_string(),
        }]
    );
}

#[test]
fn compiles_staged_with_undirected_final_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person) \
             WITH a ORDER BY a.age LIMIT 2 \
             MATCH (a)-[:KNOWS]-(b:Person) \
             RETURN a.name AS a, b.name AS b",
    )
    .expect("staged route should allow undirected final matches from carried variables");

    let GraphQuery::Staged(staged) = query else {
        panic!("undirected final match should compile to a staged graph query");
    };
    assert_eq!(
        staged.final_plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "KNOWS".to_string(),
            left: "a".to_string(),
            direction: Direction::Undirected,
            right: "b".to_string(),
        }]
    );
}

#[test]
fn compiles_staged_with_multihop_final_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person) \
             WITH a ORDER BY a.age LIMIT 2 \
             MATCH (a)-[:KNOWS]->(x:Person)-[:KNOWS]->(b:Person) \
             RETURN a.name AS a, b.name AS b",
    )
    .expect("staged route should allow fixed multi-hop final matches");

    let GraphQuery::Staged(staged) = query else {
        panic!("multi-hop final match should compile to a staged graph query");
    };
    assert_eq!(
        staged.final_plan.relationships,
        vec![
            RelationshipPattern {
                variable: None,
                relationship_type: "KNOWS".to_string(),
                left: "a".to_string(),
                direction: Direction::Outgoing,
                right: "x".to_string(),
            },
            RelationshipPattern {
                variable: None,
                relationship_type: "KNOWS".to_string(),
                left: "x".to_string(),
                direction: Direction::Outgoing,
                right: "b".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_staged_with_incoming_multihop_final_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person) \
             WITH a ORDER BY a.age LIMIT 2 \
             MATCH (x:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(a) \
             RETURN a.name AS a, x.name AS x",
    )
    .expect("staged route should allow incoming fixed multi-hop final matches");

    let GraphQuery::Staged(staged) = query else {
        panic!("incoming multi-hop final match should compile to a staged graph query");
    };
    assert_eq!(
        staged.final_plan.relationships,
        vec![
            RelationshipPattern {
                variable: None,
                relationship_type: "KNOWS".to_string(),
                left: "x".to_string(),
                direction: Direction::Outgoing,
                right: "b".to_string(),
            },
            RelationshipPattern {
                variable: None,
                relationship_type: "KNOWS".to_string(),
                left: "b".to_string(),
                direction: Direction::Outgoing,
                right: "a".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_staged_with_second_relationship_type() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person) \
             WITH a ORDER BY a.age LIMIT 2 \
             MATCH (a)-[:LIKES]->(b:Person) \
             RETURN a.name AS a, b.name AS b",
    )
    .expect("staged route should allow any explicit relationship type");

    let GraphQuery::Staged(staged) = query else {
        panic!("second relationship type should compile to a staged graph query");
    };
    assert_eq!(
        staged.final_plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "LIKES".to_string(),
            left: "a".to_string(),
            direction: Direction::Outgoing,
            right: "b".to_string(),
        }]
    );
}

#[test]
fn compiles_staged_with_multiple_carried_property_returns() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person) \
             WITH a ORDER BY a.age LIMIT 2 \
             MATCH (a)-[:OWNS]->(b:Service) \
             RETURN a.name AS a, a.age AS age, b.name AS b",
    )
    .expect("staged route should rehydrate carried node property columns");

    let GraphQuery::Staged(staged) = query else {
        panic!("multi-property carried return should compile to a staged graph query");
    };
    assert_eq!(
        staged.final_plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "a".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("a".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "a".to_string(),
                    property: "age".to_string(),
                },
                alias: Some("age".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "b".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("b".to_string()),
            },
        ]
    );
}

#[test]
fn compiles_staged_scalar_alias_with_order_limit_before_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person) \
             WITH a.id AS friendId ORDER BY a.age LIMIT 1 \
             MATCH (b:Person) WHERE b.id = friendId \
             RETURN b.name AS name",
    )
    .expect("staged scalar alias WITH ORDER BY LIMIT before MATCH should compile");

    let GraphQuery::Staged(staged) = query else {
        panic!("scalar alias query should compile to a staged graph query");
    };
    assert_eq!(staged.stages.len(), 1);
    let stage = staged
        .stages
        .first()
        .expect("staged query should have stage 0");
    assert_eq!(
        stage.exports,
        vec![GraphStageExport::ScalarValue {
            alias: "friendId".to_string(),
            source: "friendId".to_string(),
        }]
    );
    assert_eq!(
        stage.plan.projections,
        vec![Projection::Property {
            property: PropertyRef {
                variable: "a".to_string(),
                property: "id".to_string(),
            },
            alias: Some("friendId".to_string()),
        }]
    );
    assert_eq!(
        stage.plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "a".to_string(),
                property: "age".to_string(),
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
    assert_eq!(stage.plan.limit, Some(1));
    assert_eq!(
        staged.final_plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Property(PropertyRef {
                variable: "b".to_string(),
                property: "id".to_string(),
            }),
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::StageValue {
                alias: "friendId".to_string(),
            }),
        }))
    );
}

#[test]
fn compiles_staged_scalar_alias_with_labeled_final_target_on_single_label_graph() {
    let graph = single_label_person_knows_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person) \
             WITH a.id AS friendId ORDER BY a.age LIMIT 1 \
             MATCH (b:Person) WHERE b.id = friendId \
             RETURN b.name AS name",
    )
    .expect("explicitly labeled scalar alias target should compile to staged");

    assert!(matches!(query, GraphQuery::Staged(_)));
}

#[test]
fn rejects_staged_scalar_alias_unlabeled_final_target_on_single_label_graph() {
    let graph = single_label_person_knows_test_graph();
    let error = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person) \
             WITH a.id AS y ORDER BY a.age LIMIT 1 \
             MATCH (b) WHERE b.id = y \
             RETURN b.name AS name",
    )
    .expect_err("unlabeled scalar alias target should require broader staged planning");

    assert!(
        error.to_string().contains("staged query planning"),
        "{error}"
    );
}

#[test]
fn compiles_staged_string_scalar_alias_with_skip_limit_before_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person) \
             WITH a.name AS selectedName ORDER BY a.age SKIP 1 LIMIT 1 \
             MATCH (b:Person) WHERE b.name = selectedName \
             RETURN b.name AS name",
    )
    .expect("staged string scalar alias WITH SKIP LIMIT before MATCH should compile");

    let GraphQuery::Staged(staged) = query else {
        panic!("string scalar alias query should compile to a staged graph query");
    };
    let stage = staged
        .stages
        .first()
        .expect("staged query should have stage 0");
    assert_eq!(stage.plan.skip, Some(1));
    assert_eq!(stage.plan.limit, Some(1));
    assert_eq!(
        stage.exports,
        vec![GraphStageExport::ScalarValue {
            alias: "selectedName".to_string(),
            source: "selectedName".to_string(),
        }]
    );
}

#[test]
fn compiles_staged_scalar_alias_return_after_final_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person) \
             WITH a.id AS friendId ORDER BY a.age LIMIT 1 \
             MATCH (b:Person) WHERE b.name = 'Alice' \
             RETURN friendId AS id",
    )
    .expect("staged scalar alias should be usable in final RETURN");

    let GraphQuery::Staged(staged) = query else {
        panic!("scalar alias return query should compile to a staged graph query");
    };
    assert_eq!(
        staged.final_plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::StageValue {
                alias: "friendId".to_string(),
            },
            alias: "id".to_string(),
        }]
    );
}

#[test]
fn keeps_bare_scalar_alias_before_match_transparent() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person) \
             WITH a.id AS friendId \
             MATCH (b:Person) WHERE b.id = friendId \
             RETURN b.name AS name",
    )
    .expect("bare scalar alias without row modifiers should remain transparent");

    assert!(matches!(query, GraphQuery::Plan(_)));
}

#[test]
fn compiles_staged_with_count_aggregation_before_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) \
         WITH a, count(b) AS deg \
         MATCH (a)-[:KNOWS]->(c:Person) \
         RETURN a.name AS name, deg",
    )
    .expect("staged aggregate WITH before MATCH should compile");

    let GraphQuery::Staged(staged) = query else {
        panic!("aggregate stage should compile to a staged graph query");
    };
    let stage = staged
        .stages
        .first()
        .expect("staged query should have stage 0");
    assert_eq!(
        stage.exports,
        vec![
            GraphStageExport::NodeKey {
                variable: "a".to_string(),
                column: "a_id".to_string(),
            },
            GraphStageExport::AggregateValue {
                alias: "deg".to_string(),
                column: "deg".to_string(),
            },
        ]
    );
    assert_eq!(
        stage.plan.projections,
        vec![
            Projection::Key {
                variable: "a".to_string(),
                alias: "a_id".to_string(),
            },
            Projection::Aggregate {
                function: AggregateFunction::Count,
                target: AggregateTarget::VariableKey {
                    variable: "b".to_string(),
                },
                distinct: false,
                alias: "deg".to_string(),
            },
        ]
    );
    assert_eq!(
        staged.final_plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "a".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("name".to_string()),
            },
            Projection::Expression {
                expression: ScalarExpression::StageValue {
                    alias: "deg".to_string(),
                },
                alias: "deg".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_staged_with_sum_aggregation_before_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) \
         WITH a, sum(b.age) AS total_age \
         MATCH (a)-[:KNOWS]->(c:Person) \
         RETURN a.name AS name, total_age",
    )
    .expect("staged sum aggregate WITH before MATCH should compile");

    let GraphQuery::Staged(staged) = query else {
        panic!("sum aggregate stage should compile to a staged graph query");
    };
    assert!(matches!(
        staged
            .stages
            .first()
            .and_then(|stage| stage.plan.projections.get(1)),
        Some(Projection::Aggregate {
            function: AggregateFunction::Sum,
            target: AggregateTarget::Property(PropertyRef { variable, property }),
            alias,
            ..
        }) if variable == "b" && property == "age" && alias == "total_age"
    ));
}

#[test]
fn compiles_staged_with_two_group_keys_before_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) \
         WITH a, b, count(*) AS c \
         MATCH (a)-[:KNOWS]->(b) \
         RETURN a.name AS a, b.name AS b, c",
    )
    .expect("staged aggregate WITH with two group keys should compile");

    let GraphQuery::Staged(staged) = query else {
        panic!("two-key aggregate stage should compile to a staged graph query");
    };
    let stage = staged
        .stages
        .first()
        .expect("staged query should have stage 0");
    assert_eq!(
        stage.exports,
        vec![
            GraphStageExport::NodeKey {
                variable: "a".to_string(),
                column: "a_id".to_string(),
            },
            GraphStageExport::NodeKey {
                variable: "b".to_string(),
                column: "b_id".to_string(),
            },
            GraphStageExport::AggregateValue {
                alias: "c".to_string(),
                column: "c".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_staged_aggregate_alias_in_final_where() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) \
         WITH a, count(b) AS deg \
         MATCH (a)-[:KNOWS]->(c:Person) WHERE deg > 1 \
         RETURN a.name AS name, deg",
    )
    .expect("staged aggregate alias should be usable in final WHERE");

    assert!(matches!(query, GraphQuery::Staged(_)));
}

#[test]
fn compiles_staged_aggregate_order_limit_before_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) \
         WITH a, count(b) AS deg ORDER BY deg DESC LIMIT 1 \
         MATCH (a)-[:KNOWS]->(c:Person) \
         RETURN a.name AS name, deg",
    )
    .expect("staged aggregate WITH ORDER BY/LIMIT should compile");

    let GraphQuery::Staged(staged) = query else {
        panic!("ordered aggregate stage should compile to a staged graph query");
    };
    let stage = staged
        .stages
        .first()
        .expect("staged query should have stage 0");
    assert_eq!(
        stage.plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("deg".to_string()),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
    assert_eq!(stage.plan.limit, Some(1));
}

#[test]
fn compiles_staged_aggregate_with_incoming_final_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) \
         WITH a, count(b) AS deg \
         MATCH (c:Person)-[:KNOWS]->(a) \
         RETURN a.name AS name, deg",
    )
    .expect("staged aggregate route should allow incoming final matches");

    let GraphQuery::Staged(staged) = query else {
        panic!("incoming aggregate final match should compile to a staged graph query");
    };
    assert_eq!(
        staged.final_plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "KNOWS".to_string(),
            left: "c".to_string(),
            direction: Direction::Outgoing,
            right: "a".to_string(),
        }]
    );
}

#[test]
fn compiles_staged_aggregate_with_undirected_final_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) \
         WITH a, count(b) AS deg \
         MATCH (a)-[:KNOWS]-(c:Person) \
         RETURN a.name AS name, deg",
    )
    .expect("staged aggregate route should allow undirected final matches");

    let GraphQuery::Staged(staged) = query else {
        panic!("undirected aggregate final match should compile to a staged graph query");
    };
    assert_eq!(
        staged.final_plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "KNOWS".to_string(),
            left: "a".to_string(),
            direction: Direction::Undirected,
            right: "c".to_string(),
        }]
    );
}

#[test]
fn compiles_staged_aggregate_with_multihop_final_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) \
         WITH a, count(b) AS deg \
         MATCH (a)-[:KNOWS]->(x:Person)-[:KNOWS]->(c:Person) \
         RETURN a.name AS name, c.name AS c, deg",
    )
    .expect("staged aggregate route should allow fixed multi-hop final matches");

    let GraphQuery::Staged(staged) = query else {
        panic!("aggregate multi-hop final match should compile to a staged graph query");
    };
    assert_eq!(
        staged.final_plan.relationships,
        vec![
            RelationshipPattern {
                variable: None,
                relationship_type: "KNOWS".to_string(),
                left: "a".to_string(),
                direction: Direction::Outgoing,
                right: "x".to_string(),
            },
            RelationshipPattern {
                variable: None,
                relationship_type: "KNOWS".to_string(),
                left: "x".to_string(),
                direction: Direction::Outgoing,
                right: "c".to_string(),
            },
        ]
    );
}

#[test]
fn rejects_adjacent_staged_aggregation_shapes() {
    let cases = [
        (
            "distinct aggregate stage",
            "MATCH (a:Person)-[:KNOWS]->(b:Person) \
             WITH DISTINCT a, count(b) AS deg \
             MATCH (a)-[:KNOWS]->(c:Person) \
             RETURN a.name AS name, deg",
        ),
        (
            "scalar alias carry",
            "MATCH (a:Person)-[:KNOWS]->(b:Person) \
             WITH a.name AS name, count(b) AS deg \
             MATCH (a)-[:KNOWS]->(c:Person) \
             RETURN name, deg",
        ),
        (
            "initial WHERE before aggregate WITH",
            "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE b.age > 30 \
             WITH a, count(b) AS deg \
             MATCH (a)-[:KNOWS]->(c:Person) \
             RETURN a.name AS name, deg",
        ),
        (
            "post-aggregate WITH WHERE",
            "MATCH (a:Person)-[:KNOWS]->(b:Person) \
             WITH a, count(b) AS deg WHERE deg > 1 \
             MATCH (a)-[:KNOWS]->(c:Person) \
             RETURN a.name AS name, deg",
        ),
        (
            "graph-object return",
            "MATCH (a:Person)-[:KNOWS]->(b:Person) \
             WITH a, count(b) AS deg \
             MATCH (a)-[:KNOWS]->(c:Person) \
             RETURN a, deg",
        ),
        (
            "unlabeled final target",
            "MATCH (a:Person)-[:KNOWS]->(b:Person) \
             WITH a, count(b) AS deg \
             MATCH (a)-[:KNOWS]->(c) \
             RETURN a.name AS name, deg",
        ),
        (
            "two aggregate aliases",
            "MATCH (a:Person)-[:KNOWS]->(b:Person) \
             WITH a, count(b) AS deg, sum(b.age) AS total_age \
             MATCH (a)-[:KNOWS]->(c:Person) \
             RETURN a.name AS name, deg",
        ),
        (
            "subquery alias stage",
            "MATCH (a:Person)-[:KNOWS]->(b:Person) \
             WITH a, COUNT { MATCH (b)-[:KNOWS]->(:Person) } AS downstream \
             MATCH (a)-[:KNOWS]->(c:Person) \
             RETURN a.name AS name, downstream",
        ),
        (
            "multi-stage aggregate pipeline",
            "MATCH (a:Person)-[:KNOWS]->(b:Person) \
             WITH a, count(b) AS deg \
             MATCH (a)-[:KNOWS]->(c:Person) \
             WITH a, deg \
             MATCH (a)-[:KNOWS]->(d:Person) \
             RETURN a.name AS name, deg",
        ),
        (
            "unlabeled intermediate multi-hop final match",
            "MATCH (a:Person)-[:KNOWS]->(b:Person) \
             WITH a, count(b) AS deg \
             MATCH (a)-[:KNOWS]->(c)-[:KNOWS]->(d:Person) \
             RETURN a.name AS name, deg",
        ),
    ];

    for (name, cypher) in cases {
        assert_staged_aggregation_reject(name, cypher);
    }
}

#[test]
fn rejects_adjacent_staged_with_order_limit_shapes() {
    let cases = [
        (
            "initial WHERE before WITH",
            "MATCH (a:Person) \
             WHERE a.age > 30 \
             WITH a ORDER BY a.age LIMIT 2 \
             MATCH (a)-[:KNOWS]->(b:Person) \
             RETURN a.name AS a, b.name AS b",
        ),
        (
            "graph-object return",
            "MATCH (a:Person) \
             WITH a ORDER BY a.age LIMIT 2 \
             MATCH (a)-[:KNOWS]->(b:Person) \
             RETURN a AS a, b.name AS b",
        ),
        (
            "unverified ORDER BY property",
            "MATCH (a:Person) \
             WITH a ORDER BY a.city LIMIT 2 \
             MATCH (a)-[:KNOWS]->(b:Person) \
             RETURN a.name AS a, b.name AS b",
        ),
        (
            "scalar alias DISTINCT stage",
            "MATCH (a:Person) \
             WITH DISTINCT a.id AS friendId ORDER BY friendId LIMIT 1 \
             MATCH (b:Person) WHERE b.id = friendId \
             RETURN b.name AS b",
        ),
        (
            "subquery alias stage",
            "MATCH (a:Person) \
             WITH COUNT { MATCH (b:Person) } AS total ORDER BY total LIMIT 1 \
             MATCH (b:Person) WHERE b.id = total \
             RETURN b.name AS b",
        ),
        (
            "scalar alias graph-object return",
            "MATCH (a:Person) \
             WITH a.id AS friendId ORDER BY a.age LIMIT 1 \
             MATCH (b:Person) WHERE b.id = friendId \
             RETURN b AS b",
        ),
        (
            "scalar alias unlabeled final target",
            "MATCH (a:Person) \
             WITH a.id AS friendId ORDER BY a.age LIMIT 1 \
             MATCH (b) WHERE b.id = friendId \
             RETURN b.name AS b",
        ),
        (
            "unlabeled intermediate multi-hop final match",
            "MATCH (a:Person) \
             WITH a ORDER BY a.age LIMIT 2 \
             MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c:Person) \
             RETURN a.name AS a, c.name AS c",
        ),
        (
            "unlabeled final multi-hop target",
            "MATCH (a:Person) \
             WITH a ORDER BY a.age LIMIT 2 \
             MATCH (a)-[:KNOWS]->(b:Person)-[:KNOWS]->(c) \
             RETURN a.name AS a, b.name AS b",
        ),
    ];

    for (name, cypher) in cases {
        assert_staged_planning_reject(name, cypher);
    }
}

#[test]
fn rejects_staged_with_variable_length_final_relationship() {
    let graph = staged_planning_test_graph();
    let error = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person) \
         WITH a ORDER BY a.age LIMIT 2 \
         MATCH (a)-[:KNOWS*]->(b:Person) \
         RETURN a.name AS a, b.name AS b",
    )
    .expect_err("variable-length final relationship should remain outside multi-hop staging");

    assert!(
        error
            .to_string()
            .contains("variable-length relationship ranges require finite non-negative bounds"),
        "{error}"
    );
}

#[test]
fn rejects_second_staged_scalar_alias_with() {
    let graph = staged_planning_test_graph();
    let error = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person) \
         WITH a.id AS friendId ORDER BY a.age LIMIT 1 \
         MATCH (b:Person) WHERE b.id = friendId \
         WITH b.name AS name ORDER BY b.age LIMIT 1 \
         MATCH (c:Person) WHERE c.name = name \
         RETURN c.name AS c",
    )
    .expect_err("second staged scalar WITH should remain outside the narrow route");

    assert!(
        error
            .to_string()
            .contains("exactly one MATCH ... WITH ... RETURN query part"),
        "{error}"
    );
}

#[test]
fn rejects_staged_with_unlabeled_final_target() {
    assert_staged_planning_reject(
        "unlabeled final target",
        "MATCH (a:Person) \
         WITH a ORDER BY a.age LIMIT 2 \
         MATCH (a)-[:KNOWS]->(b) \
         RETURN a.name AS a, b.name AS b",
    );
}

#[test]
fn rejects_staged_with_untyped_final_relationship() {
    assert_staged_planning_reject(
        "untyped final relationship",
        "MATCH (a:Person) \
         WITH a ORDER BY a.age LIMIT 2 \
         MATCH (a)-->(b:Person) \
         RETURN a.name AS a, b.name AS b",
    );
}

#[test]
fn rejects_staged_with_limit_zero_before_match() {
    let graph = staged_planning_test_graph();
    let error = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person) \
             WITH a ORDER BY a.age LIMIT 0 \
             MATCH (a)-[:KNOWS]->(b:Person) \
             RETURN b.name AS b",
    )
    .expect_err("LIMIT 0 staged planning remains outside the minimal spike slice");

    assert!(
        error
            .to_string()
            .contains("WITH DISTINCT, ORDER BY, SKIP, and LIMIT before another MATCH require staged query planning"),
        "{error}"
    );
}

fn assert_staged_planning_reject(name: &str, cypher: &str) {
    let graph = staged_planning_test_graph();
    let Err(error) = compile_cypher_query_for_graph(&graph, cypher) else {
        panic!("{name} should require broader staged planning");
    };

    let message = error.to_string();
    assert!(
        message.contains(
            "WITH DISTINCT, ORDER BY, SKIP, and LIMIT before another MATCH require staged query planning"
        ) || message.contains("staged query planning"),
        "{name}: {error}"
    );
}

fn assert_staged_aggregation_reject(name: &str, cypher: &str) {
    let graph = staged_planning_test_graph();
    let Err(error) = compile_cypher_query_for_graph(&graph, cypher) else {
        panic!("{name} should require broader staged aggregation planning");
    };

    assert!(
        error.to_string().contains("staged query planning"),
        "{name}: {error}"
    );
}

#[test]
fn compiles_id_and_type_projections() {
    let plan = compile_cypher(
            "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN id(person) AS person_id, id(owns) AS ownership_id, type(owns) AS relationship_type \
             ORDER BY ownership_id",
        )
        .expect("id() and type() projections should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Key {
                variable: "person".to_string(),
                alias: "person_id".to_string(),
            },
            Projection::Key {
                variable: "owns".to_string(),
                alias: "ownership_id".to_string(),
            },
            Projection::RelationshipType {
                variable: "owns".to_string(),
                relationship_type: "OWNS".to_string(),
                alias: "relationship_type".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("ownership_id".to_string()),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_relationship_endpoint_property_projections() {
    let plan = compile_cypher(
        "MATCH (source:Service)-[dependency:DEPENDS_ON]->(target:Service) \
             RETURN startNode(dependency).name AS source, endNode(dependency).name AS target \
             ORDER BY endNode(dependency).name",
    )
    .expect("relationship endpoint property projections should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "source".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("source".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "target".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("target".to_string()),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "target".to_string(),
                property: "name".to_string(),
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_properties_function_property_lookups() {
    let plan = compile_cypher(
        "MATCH (source:Service)-[dependency:DEPENDS_ON]->(target:Service) \
             WHERE properties(source).tier = properties(target).tier \
             RETURN properties(source).name AS source_name, \
                    properties(startNode(dependency)).name AS start_name, \
                    properties(endNode(dependency)).tier AS end_tier \
             ORDER BY properties(target).name",
    )
    .expect("properties(variable).property lookups should compile as graph properties");

    assert_eq!(
        plan.predicates,
        vec![PropertyPredicate {
            property: PropertyRef {
                variable: "source".to_string(),
                property: "tier".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Property(PropertyRef {
                variable: "target".to_string(),
                property: "tier".to_string(),
            }),
        }]
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "source".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("source_name".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "source".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("start_name".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "target".to_string(),
                    property: "tier".to_string(),
                },
                alias: Some("end_tier".to_string()),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "target".to_string(),
                property: "name".to_string(),
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_string_property_index_lookups() {
    let parameters = BTreeMap::from([
        (
            "tier_key".to_string(),
            CypherParameterValue::Literal(Literal::String("tier".to_string())),
        ),
        (
            "order_key".to_string(),
            CypherParameterValue::Literal(Literal::String("name".to_string())),
        ),
    ]);
    let plan = compile_cypher_with_parameters(
        "MATCH (source:Service)-[dependency:DEPENDS_ON]->(target:Service) \
             WHERE source['tier'] = properties(target)[$tier_key] \
             RETURN source['name'] AS source_name, \
                    properties(startNode(dependency))['name'] AS start_name, \
                    properties(endNode(dependency))['tier'] AS end_tier \
             ORDER BY target[$order_key]",
        &parameters,
    )
    .expect("string property index lookups should compile as graph properties");

    assert_eq!(
        plan.predicates,
        vec![PropertyPredicate {
            property: PropertyRef {
                variable: "source".to_string(),
                property: "tier".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Property(PropertyRef {
                variable: "target".to_string(),
                property: "tier".to_string(),
            }),
        }]
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "source".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("source_name".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "source".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("start_name".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "target".to_string(),
                    property: "tier".to_string(),
                },
                alias: Some("end_tier".to_string()),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "target".to_string(),
                property: "name".to_string(),
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn rejects_non_string_property_indexes() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             RETURN service[0] AS service_name",
    )
    .expect_err("property indexes should require static string keys");

    assert!(
        error
            .to_string()
            .contains("property index lookups require a string literal"),
        "{error}"
    );
}

#[test]
fn compiles_reversed_relationship_endpoint_property_projections() {
    let plan = compile_cypher(
        "MATCH (target:Service)<-[dependency:DEPENDS_ON]-(source:Service) \
             RETURN startNode(dependency).name AS source, endNode(dependency).name AS target",
    )
    .expect("reversed relationship endpoint property projections should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "source".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("source".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "target".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("target".to_string()),
            },
        ]
    );
}

#[test]
fn compiles_relationship_endpoint_properties_in_predicates_and_scalars() {
    let plan = compile_cypher(
        "MATCH (source:Service)-[dependency:DEPENDS_ON]->(target:Service) \
             WHERE startNode(dependency).tier = 'prod' \
             RETURN lower(endNode(dependency).name) AS target \
             ORDER BY endNode(dependency).name",
    )
    .expect("relationship endpoint property scalar expressions should compile");

    assert_eq!(
        plan.predicates,
        vec![PropertyPredicate {
            property: PropertyRef {
                variable: "source".to_string(),
                property: "tier".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
        }]
    );
    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::ToLower {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "target".to_string(),
                    property: "name".to_string(),
                })),
            },
            alias: "target".to_string(),
        }]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "target".to_string(),
                property: "name".to_string(),
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_relationship_endpoint_property_aggregates() {
    let plan = compile_cypher(
        "MATCH (source:Service)-[dependency:DEPENDS_ON]->(target:Service) \
             RETURN count(startNode(dependency).name) AS sources",
    )
    .expect("relationship endpoint aggregate target should compile");

    assert_eq!(
        plan.projections,
        vec![Projection::Aggregate {
            function: AggregateFunction::Count,
            target: AggregateTarget::Property(PropertyRef {
                variable: "source".to_string(),
                property: "name".to_string(),
            }),
            distinct: false,
            alias: "sources".to_string(),
        }]
    );
}

#[test]
fn compiles_undirected_cross_label_relationship_endpoint_properties() {
    let graph = star_test_graph();
    for cypher in [
        "MATCH (person:Person)-[owns:OWNS]-(service:Service) \
             RETURN startNode(owns).name AS owner, endNode(owns).name AS service \
             ORDER BY startNode(owns).name",
        "MATCH (service:Service)-[owns:OWNS]-(person:Person) \
             RETURN startNode(owns).name AS owner, endNode(owns).name AS service \
             ORDER BY startNode(owns).name",
    ] {
        let plan = compile_cypher_for_graph(&graph, cypher)
            .expect("undirected cross-label endpoint properties should compile");

        assert_eq!(
            plan.projections,
            vec![
                Projection::Property {
                    property: PropertyRef {
                        variable: "person".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("owner".to_string()),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("service".to_string()),
                },
            ]
        );
        assert_eq!(
            plan.order_by,
            vec![OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "person".to_string(),
                    property: "name".to_string(),
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            }]
        );
    }
}

#[test]
fn rejects_undirected_cross_label_endpoint_properties_without_graph_context() {
    let error = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]-(service:Service) \
             RETURN startNode(owns).name AS owner",
    )
    .expect_err("undirected cross-label endpoint functions require graph context");

    assert!(
        error.to_string().contains(
            "startNode() over cross-label undirected relationships requires a graph declaration"
        ),
        "{error}"
    );
}

#[test]
fn compiles_relationship_endpoint_property_aggregates_on_optional_relationships() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH (service)-[dependency:DEPENDS_ON]->(dependency_service:Service) \
             RETURN count(endNode(dependency).name) AS named_dependencies, \
                    sum(endNode(dependency).risk) AS dependency_risk",
    )
    .expect("optional relationship endpoint property aggregates should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Aggregate {
                function: AggregateFunction::Count,
                target: AggregateTarget::PresenceGatedProperty {
                    property: PropertyRef {
                        variable: "dependency_service".to_string(),
                        property: "name".to_string(),
                    },
                    presence_variable: "dependency".to_string(),
                },
                distinct: false,
                alias: "named_dependencies".to_string(),
            },
            Projection::Aggregate {
                function: AggregateFunction::Sum,
                target: AggregateTarget::PresenceGatedProperty {
                    property: PropertyRef {
                        variable: "dependency_service".to_string(),
                        property: "risk".to_string(),
                    },
                    presence_variable: "dependency".to_string(),
                },
                distinct: false,
                alias: "dependency_risk".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_collect_over_optional_relationship_endpoint_properties() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH (service)-[dependency:DEPENDS_ON]->(dependency_service:Service) \
             RETURN collect(endNode(dependency).name) AS dependencies",
    )
    .expect("optional endpoint collect should compile to a presence-gated property aggregate");

    assert_eq!(
        plan.projections,
        vec![Projection::Aggregate {
            function: AggregateFunction::Collect,
            target: AggregateTarget::PresenceGatedProperty {
                property: PropertyRef {
                    variable: "dependency_service".to_string(),
                    property: "name".to_string(),
                },
                presence_variable: "dependency".to_string(),
            },
            distinct: false,
            alias: "dependencies".to_string(),
        }]
    );
}

#[test]
fn compiles_optional_relationship_endpoint_property_indexes() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH (service)-[dependency:DEPENDS_ON]->(dependency_service:Service) \
             RETURN endNode(dependency)['name'] AS dependency_name, \
                    properties(startNode(dependency))['name'] AS source_name \
             ORDER BY properties(endNode(dependency))['risk']",
    )
    .expect("optional endpoint property indexes should compile as presence-gated properties");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "dependency".to_string(),
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "dependency_service".to_string(),
                        property: "name".to_string(),
                    })),
                },
                alias: "dependency_name".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "dependency".to_string(),
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    })),
                },
                alias: "source_name".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::PresenceGated {
                presence_variable: "dependency".to_string(),
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "dependency_service".to_string(),
                    property: "risk".to_string(),
                })),
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_relationship_endpoint_identity_projections() {
    let plan = compile_cypher(
        "MATCH (source:Service)-[dependency:DEPENDS_ON]->(target:Service) \
             RETURN id(startNode(dependency)) AS source_id, \
                    elementId(endNode(dependency)) AS target_element_id, \
                    labels(startNode(dependency)) AS source_labels, \
                    keys(endNode(dependency)) AS target_keys \
             ORDER BY id(startNode(dependency))",
    )
    .expect("relationship endpoint identity projections should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Key {
                variable: "source".to_string(),
                alias: "source_id".to_string(),
            },
            Projection::ElementId {
                variable: "target".to_string(),
                alias: "target_element_id".to_string(),
            },
            Projection::NodeLabels {
                variable: "source".to_string(),
                label: "Service".to_string(),
                alias: "source_labels".to_string(),
            },
            Projection::PropertyKeys {
                variable: "target".to_string(),
                alias: "target_keys".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Key {
                variable: "source".to_string(),
            },
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_relationship_endpoint_identity_aggregates() {
    let plan = compile_cypher(
        "MATCH (source:Service)-[dependency:DEPENDS_ON]->(target:Service) \
             RETURN count(endNode(dependency)) AS targets",
    )
    .expect("relationship endpoint identity aggregate should compile");

    assert_eq!(
        plan.projections,
        vec![Projection::Aggregate {
            function: AggregateFunction::Count,
            target: AggregateTarget::VariableKey {
                variable: "target".to_string(),
            },
            distinct: false,
            alias: "targets".to_string(),
        }]
    );
}

#[test]
fn compiles_relationship_endpoint_identity_aggregates_on_optional_relationships() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH (service)-[dependency:DEPENDS_ON]->(dependency_service:Service) \
             RETURN count(endNode(dependency)) AS dependencies, \
                    count(DISTINCT startNode(dependency)) AS sources",
    )
    .expect("optional relationship endpoint identity aggregates should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Aggregate {
                function: AggregateFunction::Count,
                target: AggregateTarget::PresenceGatedVariableKey {
                    variable: "dependency_service".to_string(),
                    presence_variable: "dependency".to_string(),
                },
                distinct: false,
                alias: "dependencies".to_string(),
            },
            Projection::Aggregate {
                function: AggregateFunction::Count,
                target: AggregateTarget::PresenceGatedVariableKey {
                    variable: "service".to_string(),
                    presence_variable: "dependency".to_string(),
                },
                distinct: true,
                alias: "sources".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_relationship_endpoint_identity_predicates() {
    let plan = compile_cypher(
        "MATCH (source:Service)-[dependency:DEPENDS_ON]->(target:Service) \
             WHERE startNode(dependency) IS NOT NULL \
               AND endNode(dependency):Service \
               AND 'Service' IN labels(startNode(dependency)) \
               AND 'name' IN keys(endNode(dependency)) \
             RETURN target.name AS target",
    )
    .expect("relationship endpoint identity predicates should compile");

    assert!(matches!(
        plan.predicate,
        Some(PredicateExpression::And { .. })
    ));
}

#[test]
fn compiles_relationship_endpoint_identity_functions_on_optional_relationships() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH (service)-[dependency:DEPENDS_ON]->(dependency_service:Service) \
             RETURN id(endNode(dependency)) AS dependency_id, \
                    elementId(startNode(dependency)) AS source_element_id \
             ORDER BY id(endNode(dependency))",
    )
    .expect("relationship endpoint identity functions over optional relationships should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "dependency".to_string(),
                    expression: Box::new(ScalarExpression::Key {
                        variable: "dependency_service".to_string(),
                    }),
                },
                alias: "dependency_id".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "dependency".to_string(),
                    expression: Box::new(ScalarExpression::ElementId {
                        variable: "service".to_string(),
                    }),
                },
                alias: "source_element_id".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::PresenceGated {
                presence_variable: "dependency".to_string(),
                expression: Box::new(ScalarExpression::Key {
                    variable: "dependency_service".to_string(),
                }),
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_relationship_endpoint_metadata_functions_on_optional_relationships() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH (service)-[dependency:DEPENDS_ON]->(dependency_service:Service) \
             RETURN labels(endNode(dependency)) AS dependency_labels, \
                    keys(startNode(dependency)) AS source_keys \
             ORDER BY labels(endNode(dependency)), keys(startNode(dependency))",
    )
    .expect("relationship endpoint metadata functions over optional relationships should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "dependency".to_string(),
                    expression: Box::new(ScalarExpression::NodeLabels {
                        variable: "dependency_service".to_string(),
                        label: "Service".to_string(),
                    }),
                },
                alias: "dependency_labels".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "dependency".to_string(),
                    expression: Box::new(ScalarExpression::PropertyKeys {
                        variable: "service".to_string(),
                    }),
                },
                alias: "source_keys".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::PresenceGated {
                    presence_variable: "dependency".to_string(),
                    expression: Box::new(ScalarExpression::NodeLabels {
                        variable: "dependency_service".to_string(),
                        label: "Service".to_string(),
                    }),
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::PresenceGated {
                    presence_variable: "dependency".to_string(),
                    expression: Box::new(ScalarExpression::PropertyKeys {
                        variable: "service".to_string(),
                    }),
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_same_label_undirected_relationship_endpoint_properties() {
    let plan = compile_cypher(
        "MATCH (left:Service)-[dependency:DEPENDS_ON]-(right:Service) \
             RETURN startNode(dependency).name AS source, endNode(dependency).name AS target \
             ORDER BY startNode(dependency).name, endNode(dependency).name",
    )
    .expect("same-label undirected endpoint properties should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::UndirectedEndpointProperty {
                    relationship: "dependency".to_string(),
                    endpoint: UndirectedRelationshipEndpoint::Start,
                    property: "name".to_string(),
                },
                alias: "source".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::UndirectedEndpointProperty {
                    relationship: "dependency".to_string(),
                    endpoint: UndirectedRelationshipEndpoint::End,
                    property: "name".to_string(),
                },
                alias: "target".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::UndirectedEndpointProperty {
                    relationship: "dependency".to_string(),
                    endpoint: UndirectedRelationshipEndpoint::Start,
                    property: "name".to_string(),
                },),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::UndirectedEndpointProperty {
                    relationship: "dependency".to_string(),
                    endpoint: UndirectedRelationshipEndpoint::End,
                    property: "name".to_string(),
                },),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_same_label_undirected_relationship_endpoint_property_indexes() {
    let plan = compile_cypher(
        "MATCH (left:Service)-[dependency:DEPENDS_ON]-(right:Service) \
             RETURN startNode(dependency)['name'] AS source, \
                    properties(endNode(dependency))['tier'] AS target_tier \
             ORDER BY properties(startNode(dependency))['name'], endNode(dependency)['tier']",
    )
    .expect("same-label undirected endpoint property indexes should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::UndirectedEndpointProperty {
                    relationship: "dependency".to_string(),
                    endpoint: UndirectedRelationshipEndpoint::Start,
                    property: "name".to_string(),
                },
                alias: "source".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::UndirectedEndpointProperty {
                    relationship: "dependency".to_string(),
                    endpoint: UndirectedRelationshipEndpoint::End,
                    property: "tier".to_string(),
                },
                alias: "target_tier".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::UndirectedEndpointProperty {
                    relationship: "dependency".to_string(),
                    endpoint: UndirectedRelationshipEndpoint::Start,
                    property: "name".to_string(),
                },),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::UndirectedEndpointProperty {
                    relationship: "dependency".to_string(),
                    endpoint: UndirectedRelationshipEndpoint::End,
                    property: "tier".to_string(),
                },),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_same_label_undirected_relationship_endpoint_property_aggregates() {
    let plan = compile_cypher(
        "MATCH (left:Service)-[dependency:DEPENDS_ON]-(right:Service) \
             RETURN count(DISTINCT endNode(dependency).name) AS targets",
    )
    .expect("same-label undirected endpoint property aggregate should compile");

    assert_eq!(
        plan.projections,
        vec![Projection::Aggregate {
            function: AggregateFunction::Count,
            target: AggregateTarget::Expression(ScalarExpression::UndirectedEndpointProperty {
                relationship: "dependency".to_string(),
                endpoint: UndirectedRelationshipEndpoint::End,
                property: "name".to_string(),
            },),
            distinct: true,
            alias: "targets".to_string(),
        }]
    );
}

#[test]
fn compiles_same_label_undirected_relationship_endpoint_identity_and_metadata() {
    let plan = compile_cypher(
        "MATCH (left:Service)-[dependency:DEPENDS_ON]-(right:Service) \
             RETURN id(startNode(dependency)) AS source_id, \
                    elementId(endNode(dependency)) AS target_element_id, \
                    labels(startNode(dependency)) AS source_labels, \
                    keys(endNode(dependency)) AS target_keys \
             ORDER BY id(startNode(dependency)), elementId(endNode(dependency))",
    )
    .expect("same-label undirected endpoint identity and metadata should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::UndirectedEndpointKey {
                    relationship: "dependency".to_string(),
                    endpoint: UndirectedRelationshipEndpoint::Start,
                },
                alias: "source_id".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::UndirectedEndpointElementId {
                    relationship: "dependency".to_string(),
                    endpoint: UndirectedRelationshipEndpoint::End,
                },
                alias: "target_element_id".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::UndirectedEndpointLabels {
                    relationship: "dependency".to_string(),
                    endpoint: UndirectedRelationshipEndpoint::Start,
                    label: "Service".to_string(),
                },
                alias: "source_labels".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::UndirectedEndpointPropertyKeys {
                    relationship: "dependency".to_string(),
                    endpoint: UndirectedRelationshipEndpoint::End,
                },
                alias: "target_keys".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::UndirectedEndpointKey {
                    relationship: "dependency".to_string(),
                    endpoint: UndirectedRelationshipEndpoint::Start,
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Scalar(
                    ScalarExpression::UndirectedEndpointElementId {
                        relationship: "dependency".to_string(),
                        endpoint: UndirectedRelationshipEndpoint::End,
                    },
                ),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn rejects_relationship_endpoint_properties_on_node_variables() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             RETURN startNode(service).name AS source",
    )
    .expect_err("relationship endpoint functions should require relationship variables");

    assert!(
        error
            .to_string()
            .contains("startNode() argument 'service' is not a named relationship variable"),
        "{error}"
    );
}

#[test]
fn compiles_element_id_projections() {
    let plan = compile_cypher(
            "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN elementId(person) AS person_element_id, elementId(owns) AS ownership_element_id \
             ORDER BY ownership_element_id",
        )
        .expect("elementId() projections should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::ElementId {
                variable: "person".to_string(),
                alias: "person_element_id".to_string(),
            },
            Projection::ElementId {
                variable: "owns".to_string(),
                alias: "ownership_element_id".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("ownership_element_id".to_string()),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_labels_projection() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN labels(service) AS service_labels \
             ORDER BY service_labels",
    )
    .expect("labels() projection should compile");

    assert_eq!(
        plan.projections,
        vec![Projection::NodeLabels {
            variable: "service".to_string(),
            label: "Service".to_string(),
            alias: "service_labels".to_string(),
        }]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("service_labels".to_string()),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_order_by_id_and_type_functions() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN person.name AS owner \
             ORDER BY id(person), id(owns) DESC, type(owns)",
    )
    .expect("id() and type() order expressions should compile");

    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::Key {
                    variable: "person".to_string(),
                },
                direction: OrderDirection::Ascending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Key {
                    variable: "owns".to_string(),
                },
                direction: OrderDirection::Descending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::RelationshipType {
                    variable: "owns".to_string(),
                    relationship_type: "OWNS".to_string(),
                },
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_order_by_element_id_function() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN person.name AS owner \
             ORDER BY elementId(person), elementId(owns) DESC",
    )
    .expect("elementId() order expressions should compile");

    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::ElementId {
                    variable: "person".to_string(),
                },
                direction: OrderDirection::Ascending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::ElementId {
                    variable: "owns".to_string(),
                },
                direction: OrderDirection::Descending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_order_by_labels_function() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN service.name AS service \
             ORDER BY labels(service) DESC",
    )
    .expect("labels() order expression should compile");

    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::NodeLabels {
                variable: "service".to_string(),
                label: "Service".to_string(),
            },
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_order_by_keys_function() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN service.name AS service \
             ORDER BY keys(service) DESC, keys(owns)",
    )
    .expect("keys() order expressions should compile");

    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::PropertyKeys {
                    variable: "service".to_string(),
                },
                direction: OrderDirection::Descending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::PropertyKeys {
                    variable: "owns".to_string(),
                },
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn rejects_labels_on_relationship_variables() {
    let error = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN labels(owns) AS labels",
    )
    .expect_err("labels() should require a node variable");

    assert!(
        error.to_string().contains("labels() argument 'owns'"),
        "{error:?}"
    );
}

#[test]
fn compiles_keys_projection() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN keys(person) AS person_keys, keys(owns) AS ownership_keys",
    )
    .expect("keys() projections should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::PropertyKeys {
                variable: "person".to_string(),
                alias: "person_keys".to_string(),
            },
            Projection::PropertyKeys {
                variable: "owns".to_string(),
                alias: "ownership_keys".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_static_map_keys_as_list_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE service.name IN keys({name: service.name, tier: service.tier}) \
             RETURN keys({name: service.name, tier: service.tier}) AS map_keys, \
                    head(keys({first: 1, second: 2})) AS first_key \
             ORDER BY keys({zeta: 0, alpha: 1})",
    )
    .expect("literal map keys should compile as static list expressions");

    assert_eq!(
        plan.predicates,
        vec![PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            },
            operator: ComparisonOperator::In,
            rhs: PredicateRhs::List(vec![
                Literal::String("name".to_string()),
                Literal::String("tier".to_string()),
            ]),
        }]
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("name".to_string()),
                        Literal::String("tier".to_string()),
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "map_keys".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::String("first".to_string())),
                alias: "first_key".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::TypedLiteralList {
                literals: vec![
                    Literal::String("zeta".to_string()),
                    Literal::String("alpha".to_string()),
                ],
                element_type: LiteralListElementType::String,
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_property_key_membership_predicates() {
    let parameters = BTreeMap::from([(
        "relationship_key".to_string(),
        CypherParameterValue::Literal(Literal::String("since".to_string())),
    )]);
    let plan = compile_cypher_with_parameters(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE 'name' IN keys(person) AND $relationship_key IN keys(owns) \
             RETURN person.name AS owner",
        &parameters,
    )
    .expect("keys() membership predicates should compile");

    assert_eq!(plan.predicates, Vec::new());
    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::PropertyKeyMembership(
                PropertyKeyMembershipPredicate {
                    variable: "person".to_string(),
                    key: "name".to_string(),
                    presence_variable: None,
                },
            )),
            right: Box::new(PredicateExpression::PropertyKeyMembership(
                PropertyKeyMembershipPredicate {
                    variable: "owns".to_string(),
                    key: "since".to_string(),
                    presence_variable: None,
                },
            )),
        })
    );
}

#[test]
fn rejects_non_string_property_key_membership_predicates() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             WHERE 1 IN keys(service) \
             RETURN service.name",
    )
    .expect_err("keys() membership should require a string literal");

    assert!(
        error.to_string().contains("keys() membership predicates"),
        "{error:?}"
    );
}

#[test]
fn compiles_id_predicates() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE id(person) = 1 AND id(owns) IN [100, 200] \
             RETURN person.name AS owner",
    )
    .expect("id() predicates should compile");

    assert_eq!(plan.predicates, Vec::new());
    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::KeyComparison(KeyPredicate {
                variable: "person".to_string(),
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::Integer(1)),
            })),
            right: Box::new(PredicateExpression::KeyComparison(KeyPredicate {
                variable: "owns".to_string(),
                operator: ComparisonOperator::In,
                rhs: PredicateRhs::List(vec![Literal::Integer(100), Literal::Integer(200)]),
            })),
        })
    );
}

#[test]
fn compiles_element_id_predicates() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE elementId(person) = '1' AND elementId(owns) IN ['100', '200'] \
             RETURN person.name AS owner",
    )
    .expect("elementId() predicates should compile");

    assert_eq!(plan.predicates, Vec::new());
    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::ElementIdComparison(
                ElementIdPredicate {
                    variable: "person".to_string(),
                    operator: ComparisonOperator::Equal,
                    rhs: PredicateRhs::Literal(Literal::String("1".to_string())),
                },
            )),
            right: Box::new(PredicateExpression::ElementIdComparison(
                ElementIdPredicate {
                    variable: "owns".to_string(),
                    operator: ComparisonOperator::In,
                    rhs: PredicateRhs::List(vec![
                        Literal::String("100".to_string()),
                        Literal::String("200".to_string()),
                    ]),
                },
            )),
        })
    );
}

#[test]
fn compiles_type_predicates_as_boolean_constants() {
    let matching = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE type(owns) = 'OWNS' \
             RETURN person.name AS owner",
    )
    .expect("matching type() predicate should compile");
    let non_matching = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE type(owns) = 'DEPENDS_ON' \
             RETURN person.name AS owner",
    )
    .expect("non-matching type() predicate should compile");
    let string_matching = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE type(owns) STARTS WITH 'OW' \
                AND type(owns) ENDS WITH 'NS' \
                AND type(owns) CONTAINS 'WN' \
                AND type(owns) =~ '^OW.*' \
             RETURN person.name AS owner",
    )
    .expect("matching type() string predicates should compile");
    let string_non_matching = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE type(owns) STARTS WITH 'DEP' \
             RETURN person.name AS owner",
    )
    .expect("non-matching type() string predicate should compile");

    assert_eq!(matching.predicate, Some(PredicateExpression::Boolean(true)));
    assert_eq!(
        non_matching.predicate,
        Some(PredicateExpression::Boolean(false))
    );
    assert!(matches!(
        string_matching.predicate,
        Some(PredicateExpression::And { .. })
    ));
    assert_eq!(
        string_non_matching.predicate,
        Some(PredicateExpression::Boolean(false))
    );
}

#[test]
fn compiles_label_membership_predicates_as_boolean_constants() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE 'Service' IN labels(service) AND NOT ('Team' IN labels(service)) \
             RETURN service.name AS service",
    )
    .expect("labels() membership predicates should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::Boolean(true)),
            right: Box::new(PredicateExpression::Not {
                expression: Box::new(PredicateExpression::Boolean(false)),
            }),
        })
    );
}

#[test]
fn compiles_metadata_list_equality_predicates_as_boolean_constants() {
    let graph = star_test_graph();
    let label_match = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             WHERE labels(service) = ['Service'] \
             RETURN service.name AS service",
    )
    .expect("labels() list equality should compile");
    let label_mismatch = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             WHERE ['Team'] <> labels(service) \
             RETURN service.name AS service",
    )
    .expect("reversed labels() list inequality should compile");
    let keys_match = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE keys(service) = ['name', 'tier'] \
               AND ['since', 'source'] = keys(owns) \
             RETURN person.name AS owner",
    )
    .expect("keys() list equality should compile");
    let parameters = BTreeMap::from([
        (
            "service_labels".to_string(),
            CypherParameterValue::List(vec![Literal::String("Service".to_string())]),
        ),
        (
            "service_keys".to_string(),
            CypherParameterValue::List(vec![
                Literal::String("name".to_string()),
                Literal::String("tier".to_string()),
            ]),
        ),
    ]);
    let parameterized = compile_cypher_for_graph_with_parameters(
        &graph,
        "MATCH (service:Service) \
             WHERE labels(service) = $service_labels \
               AND keys(service) = $service_keys \
             RETURN service.name AS service",
        &parameters,
    )
    .expect("metadata list equality should support list parameters");

    assert_eq!(
        label_match.predicate,
        Some(PredicateExpression::Boolean(true))
    );
    assert_eq!(
        label_mismatch.predicate,
        Some(PredicateExpression::Boolean(true))
    );
    assert_eq!(
        keys_match.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::Boolean(true)),
            right: Box::new(PredicateExpression::Boolean(true)),
        })
    );
    assert_eq!(
        parameterized.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::Boolean(true)),
            right: Box::new(PredicateExpression::Boolean(true)),
        })
    );
}

#[test]
fn compiles_optional_metadata_list_equality_as_presence_gated_scalar_predicates() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN labels(person) = ['Person'] AS owner_has_person_label, \
                    keys(person) = ['name', 'team'] AS owner_has_person_keys",
    )
    .expect("optional metadata list equality scalar projections should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::Predicate(Box::new(
                    PredicateExpression::ScalarComparison(ScalarPredicate {
                        lhs: ScalarExpression::PresenceGated {
                            presence_variable: "person".to_string(),
                            expression: Box::new(
                                ScalarExpression::Literal(Literal::Boolean(true),)
                            ),
                        },
                        operator: ComparisonOperator::Equal,
                        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                            Literal::Boolean(true),
                        )),
                    }),
                )),
                alias: "owner_has_person_label".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Predicate(Box::new(
                    PredicateExpression::ScalarComparison(ScalarPredicate {
                        lhs: ScalarExpression::PresenceGated {
                            presence_variable: "person".to_string(),
                            expression: Box::new(
                                ScalarExpression::Literal(Literal::Boolean(true),)
                            ),
                        },
                        operator: ComparisonOperator::Equal,
                        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                            Literal::Boolean(true),
                        )),
                    }),
                )),
                alias: "owner_has_person_keys".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_optional_static_list_in_rhs_as_presence_gated_predicates() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN service.name IN keys(person) AS service_name_is_owner_key",
    )
    .expect("optional static list IN RHS should compile");

    assert!(matches!(
        plan.projections.as_slice(),
        [Projection::Expression {
            expression: ScalarExpression::Predicate(predicate),
            alias,
        }] if alias == "service_name_is_owner_key"
            && matches!(
                predicate.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::PresenceGated {
                        presence_variable,
                        expression,
                    },
                    operator: ComparisonOperator::Equal,
                    rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                        Literal::Boolean(true),
                    )),
                }) if presence_variable == "person"
                    && matches!(
                        expression.as_ref(),
                        ScalarExpression::Predicate(inner)
                            if matches!(
                                inner.as_ref(),
                                PredicateExpression::Comparison(PropertyPredicate {
                                    property: PropertyRef { variable, property },
                                    operator: ComparisonOperator::In,
                                    rhs: PredicateRhs::List(literals),
                                }) if variable == "service"
                                    && property == "name"
                                    && literals == &vec![
                                    Literal::String("name".to_string()),
                                    Literal::String("team".to_string()),
                                ]
                            )
                    )
            )
    ));

    let distinct_plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN COLLECT { \
               MATCH (service)-[:DEPENDS_ON]->(dependency:Service) \
               RETURN DISTINCT dependency.name \
             } AS dependency_names",
    )
    .expect("COLLECT subquery DISTINCT scalar projection should compile");

    assert!(matches!(
        distinct_plan.projections.as_slice(),
        [Projection::Expression {
            expression:
                ScalarExpression::CollectSubquery {
                    pattern,
                    target,
                    distinct,
                },
            alias,
        }] if alias == "dependency_names"
            && *distinct
            && matches!(pattern.as_ref(), CountSubqueryPattern::Relationships(_))
            && matches!(
                target.as_ref(),
                ScalarExpression::Property(PropertyRef { variable, property })
                    if variable == "dependency" && property == "name"
            )
    ));
}

#[test]
fn compiles_optional_static_list_concat_in_rhs_as_presence_gated_predicates() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN service.name IN (keys(person) + ['extra']) AS service_name_is_owner_key",
    )
    .expect("optional static list concatenation IN RHS should compile");

    assert!(matches!(
        plan.projections.as_slice(),
        [Projection::Expression {
            expression: ScalarExpression::Predicate(predicate),
            alias,
        }] if alias == "service_name_is_owner_key"
            && matches!(
                predicate.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::PresenceGated {
                        presence_variable,
                        expression,
                    },
                    operator: ComparisonOperator::Equal,
                    rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                        Literal::Boolean(true),
                    )),
                }) if presence_variable == "person"
                    && matches!(
                        expression.as_ref(),
                        ScalarExpression::Predicate(inner)
                            if matches!(
                                inner.as_ref(),
                                PredicateExpression::Comparison(PropertyPredicate {
                                    property: PropertyRef { variable, property },
                                    operator: ComparisonOperator::In,
                                    rhs: PredicateRhs::List(literals),
                                }) if variable == "service"
                                    && property == "name"
                                    && literals == &vec![
                                        Literal::String("name".to_string()),
                                        Literal::String("team".to_string()),
                                        Literal::String("extra".to_string()),
                                    ]
                            )
                    )
            )
    ));
}

#[test]
fn compiles_metadata_list_index_scalar_expressions() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE labels(service)[0] = 'Service' \
               AND keys(service)[-1] = 'tier' \
             RETURN labels(service)[0] AS service_label, \
                    keys(owns)[0] AS first_ownership_key, \
                    keys(service)[99] AS missing_key \
             ORDER BY keys(service)[1]",
    )
    .expect("metadata list indexes should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Literal(Literal::String("Service".to_string())),
                operator: ComparisonOperator::Equal,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
                    "Service".to_string()
                ),)),
            })),
            right: Box::new(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Literal(Literal::String("tier".to_string())),
                operator: ComparisonOperator::Equal,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
                    "tier".to_string()
                ),)),
            })),
        })
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::String("Service".to_string())),
                alias: "service_label".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::String("since".to_string())),
                alias: "first_ownership_key".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Null),
                alias: "missing_key".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Literal(Literal::String(
                "tier".to_string(),
            ))),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_optional_metadata_list_indexes_as_presence_gated_scalars() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN labels(person)[0] AS owner_label, \
                    keys(person)[-1] AS owner_last_key",
    )
    .expect("optional metadata list indexes should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "person".to_string(),
                    expression: Box::new(ScalarExpression::Literal(Literal::String(
                        "Person".to_string(),
                    ))),
                },
                alias: "owner_label".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "person".to_string(),
                    expression: Box::new(ScalarExpression::Literal(Literal::String(
                        "team".to_string(),
                    ))),
                },
                alias: "owner_last_key".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_metadata_list_slice_expressions() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE labels(service)[0..1] = ['Service'] \
               AND ['since'] = keys(owns)[..1] \
               AND isEmpty(labels(service)[1..]) \
             RETURN labels(service)[0..1] AS service_labels, \
                    keys(service)[1..] AS service_keys_tail, \
                    keys(owns)[1..][0] AS owns_second_key, \
                    size(keys(service)[-1..]) AS service_tail_size \
             ORDER BY keys(service)[0..1]",
    )
    .expect("metadata list slices should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::And {
                left: Box::new(PredicateExpression::Boolean(true)),
                right: Box::new(PredicateExpression::Boolean(true)),
            }),
            right: Box::new(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Literal(Literal::Boolean(true)),
                operator: ComparisonOperator::Equal,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Boolean(
                    true
                ),)),
            })),
        })
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::LiteralList {
                literals: vec![Literal::String("Service".to_string())],
                alias: "service_labels".to_string(),
            },
            Projection::LiteralList {
                literals: vec![Literal::String("tier".to_string())],
                alias: "service_keys_tail".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::String("source".to_string(),)),
                alias: "owns_second_key".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Integer(1)),
                alias: "service_tail_size".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::TypedLiteralList {
                literals: vec![Literal::String("name".to_string())],
                element_type: LiteralListElementType::String,
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_optional_metadata_list_slices_as_presence_gated_lists() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN labels(person)[0..1] AS owner_labels, \
                    keys(person)[..1] AS owner_first_key",
    )
    .expect("optional metadata list slices should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "person".to_string(),
                    expression: Box::new(ScalarExpression::TypedLiteralList {
                        literals: vec![Literal::String("Person".to_string())],
                        element_type: LiteralListElementType::String,
                    }),
                },
                alias: "owner_labels".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "person".to_string(),
                    expression: Box::new(ScalarExpression::TypedLiteralList {
                        literals: vec![Literal::String("name".to_string())],
                        element_type: LiteralListElementType::String,
                    }),
                },
                alias: "owner_first_key".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_empty_metadata_list_slices_as_typed_lists() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             RETURN labels(service)[1..] AS label_tail, \
                    keys(service)[8..] AS key_tail \
             ORDER BY keys(service)[8..]",
    )
    .expect("empty metadata list slices should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: Vec::new(),
                    element_type: LiteralListElementType::String,
                },
                alias: "label_tail".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: Vec::new(),
                    element_type: LiteralListElementType::String,
                },
                alias: "key_tail".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::TypedLiteralList {
                literals: Vec::new(),
                element_type: LiteralListElementType::String,
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_static_list_endpoint_functions() {
    let graph = star_test_graph();
    let parameters = BTreeMap::from([(
        "tiers".to_string(),
        CypherParameterValue::List(vec![
            Literal::String("prod".to_string()),
            Literal::String("dev".to_string()),
        ]),
    )]);
    let plan = compile_cypher_for_graph_with_parameters(
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE head(labels(service)) = 'Service' \
               AND last(keys(owns)) = 'source' \
               AND head($tiers) = 'prod' \
             RETURN head(keys(service)) AS first_service_key, \
                    last(keys(service)) AS last_service_key, \
                    head(labels(service)[1..]) AS missing_label, \
                    last(['prod', 'critical']) AS last_literal \
             ORDER BY last(keys(service))",
        &parameters,
    )
    .expect("static list endpoint functions should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::String("name".to_string())),
                alias: "first_service_key".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::String("tier".to_string())),
                alias: "last_service_key".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Null),
                alias: "missing_label".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::String("critical".to_string(),)),
                alias: "last_literal".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Literal(Literal::String(
                "tier".to_string()
            ))),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_static_list_tail_function() {
    let graph = star_test_graph();
    let parameters = BTreeMap::from([(
        "tiers".to_string(),
        CypherParameterValue::List(vec![
            Literal::String("prod".to_string()),
            Literal::String("dev".to_string()),
        ]),
    )]);
    let plan = compile_cypher_for_graph_with_parameters(
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE head(tail(keys(service))) = 'tier' \
               AND size(tail(labels(service))) = 0 \
               AND isEmpty(tail(labels(service))) \
             RETURN tail(keys(service)) AS service_key_tail, \
                    tail(labels(service)) AS label_tail, \
                    tail(['prod', 'critical']) AS literal_tail, \
                    tail($tiers) AS parameter_tail, \
                    head(tail($tiers)) AS parameter_tail_head, \
                    last(tail(keys(service))) AS service_tail_last, \
                    size(tail(keys(service))) AS service_tail_size \
             ORDER BY tail(keys(service))",
        &parameters,
    )
    .expect("static tail() list functions should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("tier".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "service_key_tail".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: Vec::new(),
                    element_type: LiteralListElementType::String,
                },
                alias: "label_tail".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("critical".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "literal_tail".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("dev".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "parameter_tail".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::String("dev".to_string())),
                alias: "parameter_tail_head".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::String("tier".to_string())),
                alias: "service_tail_last".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Integer(1)),
                alias: "service_tail_size".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::TypedLiteralList {
                literals: vec![Literal::String("tier".to_string())],
                element_type: LiteralListElementType::String,
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_static_list_reverse_function() {
    let graph = star_test_graph();
    let parameters = BTreeMap::from([(
        "tiers".to_string(),
        CypherParameterValue::List(vec![
            Literal::String("prod".to_string()),
            Literal::String("dev".to_string()),
            Literal::String("test".to_string()),
        ]),
    )]);
    let plan = compile_cypher_for_graph_with_parameters(
        &graph,
        "MATCH (service:Service) \
             WHERE head(reverse(keys(service))) = 'tier' \
               AND any(key IN reverse(keys(service)) WHERE key = 'name') \
             RETURN reverse(keys(service)) AS service_keys_reversed, \
                    reverse(labels(service) + keys(service)) AS metadata_reversed, \
                    head(reverse(keys(service))) AS first_reversed_key, \
                    tail(reverse($tiers)) AS reversed_parameter_tail, \
                    size(reverse(labels(service) + keys(service))) AS metadata_size \
             ORDER BY reverse(keys(service))",
        &parameters,
    )
    .expect("static reverse() list functions should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("tier".to_string()),
                        Literal::String("name".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "service_keys_reversed".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("tier".to_string()),
                        Literal::String("name".to_string()),
                        Literal::String("Service".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "metadata_reversed".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::String("tier".to_string())),
                alias: "first_reversed_key".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("dev".to_string()),
                        Literal::String("prod".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "reversed_parameter_tail".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Integer(3)),
                alias: "metadata_size".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::TypedLiteralList {
                literals: vec![
                    Literal::String("tier".to_string()),
                    Literal::String("name".to_string())
                ],
                element_type: LiteralListElementType::String,
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_optional_static_list_reverse_as_presence_gated_scalar() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN reverse(labels(person) + keys(person)) AS owner_metadata",
    )
    .expect("optional static reverse() list function should compile");

    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::PresenceGated {
                presence_variable: "person".to_string(),
                expression: Box::new(ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("team".to_string()),
                        Literal::String("name".to_string()),
                        Literal::String("Person".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                }),
            },
            alias: "owner_metadata".to_string(),
        }]
    );
}

#[test]
fn rejects_reverse_with_ambiguous_list_element_type() {
    for cypher in [
        "MATCH (service:Service) RETURN reverse([]) AS values",
        "MATCH (service:Service) RETURN reverse([null]) AS values",
        "MATCH (service:Service) RETURN reverse([1, 'prod']) AS values",
    ] {
        let error = compile_cypher(cypher).expect_err("ambiguous reverse() should be rejected");
        assert!(
            error
                .to_string()
                .contains("reverse() requires a list with a known non-null element type"),
            "unexpected error: {error}"
        );
    }
}

#[test]
fn compiles_static_list_indexes_and_slices_over_folded_lists() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             WHERE reverse(keys(service))[0] = 'tier' \
               AND size((labels(service) + keys(service))[1..]) = 2 \
             RETURN reverse(keys(service))[1] AS second_reversed_key, \
                    (labels(service) + keys(service))[1..] AS metadata_tail, \
                    tail(reverse(keys(service))[1..]) AS reversed_tail_tail \
             ORDER BY reverse(keys(service))[0]",
    )
    .expect("static list indexes and slices over folded lists should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::String("name".to_string())),
                alias: "second_reversed_key".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("name".to_string()),
                        Literal::String("tier".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "metadata_tail".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: Vec::new(),
                    element_type: LiteralListElementType::String,
                },
                alias: "reversed_tail_tail".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Literal(Literal::String(
                "tier".to_string()
            ))),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_optional_static_list_indexes_and_slices_as_presence_gated_scalars() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN reverse(labels(person) + keys(person))[0] AS owner_last_metadata, \
                    reverse(labels(person) + keys(person))[1..] AS owner_metadata_tail",
    )
    .expect("optional static list indexes and slices should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "person".to_string(),
                    expression: Box::new(ScalarExpression::Literal(Literal::String(
                        "team".to_string()
                    ))),
                },
                alias: "owner_last_metadata".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "person".to_string(),
                    expression: Box::new(ScalarExpression::TypedLiteralList {
                        literals: vec![
                            Literal::String("name".to_string()),
                            Literal::String("Person".to_string())
                        ],
                        element_type: LiteralListElementType::String,
                    }),
                },
                alias: "owner_metadata_tail".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_static_list_concatenation() {
    let graph = star_test_graph();
    let parameters = BTreeMap::from([(
        "prefixes".to_string(),
        CypherParameterValue::List(vec![Literal::String("prefix".to_string())]),
    )]);
    let plan = compile_cypher_for_graph_with_parameters(
        &graph,
        "MATCH (service:Service) \
             WHERE head($prefixes + tail(keys(service))) = 'prefix' \
               AND size(labels(service) + tail(labels(service))) = 1 \
               AND any(key IN ['active'] + tail(keys(service)) WHERE key = 'tier') \
               AND service.tier IN (['prod'] + ['dev']) \
             RETURN $prefixes + tail(keys(service)) AS keys_with_prefix, \
                    labels(service) + [] AS labels_copy, \
                    [null] + tail(keys(service)) AS nullable_keys, \
                    tail($prefixes + tail(keys(service))) AS concat_tail, \
                    size($prefixes + tail(keys(service))) AS concat_size \
             ORDER BY $prefixes + tail(keys(service))",
        &parameters,
    )
    .expect("static list concatenation should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("prefix".to_string()),
                        Literal::String("tier".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "keys_with_prefix".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("Service".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "labels_copy".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::Null, Literal::String("tier".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "nullable_keys".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("tier".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "concat_tail".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Integer(2)),
                alias: "concat_size".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::TypedLiteralList {
                literals: vec![
                    Literal::String("prefix".to_string()),
                    Literal::String("tier".to_string())
                ],
                element_type: LiteralListElementType::String,
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_static_list_scalar_concatenation() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE 'dev' IN (['prod'] + 'dev') \
               AND 'prod' IN ('prod' + ['dev']) \
             RETURN ['prod'] + 'dev' AS appended, \
                    'prod' + ['dev'] AS prepended, \
                    [] + 'prod' AS typed_from_append, \
                    'prod' + [] AS typed_from_prepend, \
                    ['prod'] + null AS nullable_appended, \
                    [tier IN 'prod' + ['dev'] | toUpper(tier)] AS mapped",
    )
    .expect("static list scalar concatenation should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::Boolean(true)),
            right: Box::new(PredicateExpression::Boolean(true)),
        })
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("prod".to_string()),
                        Literal::String("dev".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "appended".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("prod".to_string()),
                        Literal::String("dev".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "prepended".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("prod".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "typed_from_append".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("prod".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "typed_from_prepend".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("prod".to_string()), Literal::Null],
                    element_type: LiteralListElementType::String,
                },
                alias: "nullable_appended".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("PROD".to_string()),
                        Literal::String("DEV".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "mapped".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_static_list_cast_projections() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN toStringList([1, true, null]) AS strings, \
                    toIntegerList(['bad', 2, true, 3.7, false, null]) AS integers, \
                    toFloatList(['bad', 2, 2.5, '3.5', true, null]) AS floats, \
                    toBooleanList(['true', 'false', 'bad', 0, 2, true, 1.5, null]) AS booleans",
    )
    .expect("static list casts should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("1".to_string()),
                        Literal::String("true".to_string()),
                        Literal::Null,
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "strings".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Null,
                        Literal::Integer(2),
                        Literal::Integer(1),
                        Literal::Integer(3),
                        Literal::Integer(0),
                        Literal::Null,
                    ],
                    element_type: LiteralListElementType::Integer,
                },
                alias: "integers".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Null,
                        Literal::Float(OrderedFloat(2.0)),
                        Literal::Float(OrderedFloat(2.5)),
                        Literal::Float(OrderedFloat(3.5)),
                        Literal::Null,
                        Literal::Null,
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "floats".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Boolean(true),
                        Literal::Boolean(false),
                        Literal::Null,
                        Literal::Boolean(false),
                        Literal::Boolean(true),
                        Literal::Boolean(true),
                        Literal::Null,
                        Literal::Null,
                    ],
                    element_type: LiteralListElementType::Boolean,
                },
                alias: "booleans".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_static_list_cast_predicates_and_ordering() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE head(toIntegerList(['1', 'bad'])) = 1 \
               AND last(toBooleanList(['false', 2])) = true \
             RETURN service.name AS service \
             ORDER BY toStringList([2, true])",
    )
    .expect("static list casts should compile in predicates and ordering");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Literal(Literal::Integer(1)),
                operator: ComparisonOperator::Equal,
                rhs: ScalarPredicateRhs::Expression(
                    ScalarExpression::Literal(Literal::Integer(1),)
                ),
            })),
            right: Box::new(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Literal(Literal::Boolean(true)),
                operator: ComparisonOperator::Equal,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Boolean(
                    true
                ),)),
            })),
        })
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::TypedLiteralList {
                literals: vec![
                    Literal::String("2".to_string()),
                    Literal::String("true".to_string()),
                ],
                element_type: LiteralListElementType::String,
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_static_list_comprehensions() {
    let graph = star_test_graph();
    let parameters = BTreeMap::from([(
        "selected_keys".to_string(),
        CypherParameterValue::List(vec![
            Literal::String("tier".to_string()),
            Literal::String("missing".to_string()),
            Literal::String("name".to_string()),
        ]),
    )]);
    let plan = compile_cypher_for_graph_with_parameters(
        &graph,
        "MATCH (service:Service) \
             RETURN [k IN keys(service)] AS service_keys_copy, \
                    [l IN labels(service)] AS service_labels, \
                    [k IN ['name', 'tier', null]] AS literal_keys_copy, \
                    [k IN $selected_keys] AS selected_keys_copy \
             ORDER BY [k IN keys(service)]",
        &parameters,
    )
    .expect("static list comprehensions should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("name".to_string()),
                        Literal::String("tier".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "service_keys_copy".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("Service".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "service_labels".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("name".to_string()),
                        Literal::String("tier".to_string()),
                        Literal::Null
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "literal_keys_copy".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("tier".to_string()),
                        Literal::String("missing".to_string()),
                        Literal::String("name".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "selected_keys_copy".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::TypedLiteralList {
                literals: vec![
                    Literal::String("name".to_string()),
                    Literal::String("tier".to_string())
                ],
                element_type: LiteralListElementType::String,
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_static_filter_and_extract_list_functions() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             RETURN filter(key IN keys(service) WHERE key <> 'name') AS service_keys_without_name, \
                    extract(key IN keys(service) | toUpper(key)) AS service_key_tokens, \
                    extract(x IN filter(x IN [1, 2, 3] WHERE x > 1) | x + 1) AS shifted \
             ORDER BY filter(key IN keys(service) WHERE key = 'tier')",
    )
    .expect("static filter()/extract() list functions should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("tier".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "service_keys_without_name".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("NAME".to_string()),
                        Literal::String("TIER".to_string()),
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "service_key_tokens".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::Integer(3), Literal::Integer(4)],
                    element_type: LiteralListElementType::Integer,
                },
                alias: "shifted".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::TypedLiteralList {
                literals: vec![Literal::String("tier".to_string())],
                element_type: LiteralListElementType::String,
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn rejects_dynamic_static_filter_and_extract_list_functions() {
    let dynamic_filter_collection = compile_cypher(
        "MATCH (service:Service) \
             RETURN filter(x IN service.name WHERE x <> 'a') AS values",
    )
    .expect_err("dynamic filter() collection should be rejected");
    assert!(
        dynamic_filter_collection
            .to_string()
            .contains("filter() requires a literal list"),
        "{dynamic_filter_collection}"
    );

    let dynamic_extract_map = compile_cypher(
        "MATCH (service:Service) \
             RETURN extract(x IN [1, 2] | service.name) AS values",
    )
    .expect_err("dynamic extract() map should be rejected");
    assert!(
        dynamic_extract_map
            .to_string()
            .contains("static list comprehension map expressions"),
        "{dynamic_extract_map}"
    );
}

#[test]
fn compiles_filtered_static_list_comprehensions() {
    let graph = star_test_graph();
    let parameters = BTreeMap::from([(
        "selected_keys".to_string(),
        CypherParameterValue::List(vec![
            Literal::String("tier".to_string()),
            Literal::String("missing".to_string()),
            Literal::String("name".to_string()),
            Literal::Null,
        ]),
    )]);
    let plan = compile_cypher_for_graph_with_parameters(
        &graph,
        "MATCH (service:Service) \
             RETURN [k IN keys(service) WHERE k <> 'tier'] AS service_keys_without_tier, \
                    [k IN ['name', 'tier', null] WHERE k IS NOT NULL] AS non_null_literal_keys, \
                    [k IN $selected_keys WHERE k IN ['name', 'tier']] AS selected_known_keys \
             ORDER BY [k IN keys(service) WHERE k = 'name']",
        &parameters,
    )
    .expect("filtered static list comprehensions should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("name".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "service_keys_without_tier".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("name".to_string()),
                        Literal::String("tier".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "non_null_literal_keys".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("tier".to_string()),
                        Literal::String("name".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "selected_known_keys".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::TypedLiteralList {
                literals: vec![Literal::String("name".to_string())],
                element_type: LiteralListElementType::String,
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_static_list_comprehensions_over_case_and_coalesce_sources() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
            &graph,
            "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN [k IN coalesce(keys(person), ['fallback', 'owner']) WHERE k <> 'owner' | toUpper(k)] AS owner_key_tokens, \
                    [k IN CASE WHEN person IS NULL THEN ['fallback'] ELSE keys(person) END WHERE k STARTS WITH 't' | k] AS team_keys \
             ORDER BY [k IN coalesce(keys(person), ['fallback']) | k]",
        )
        .expect("static list comprehensions over CASE/coalesce sources should compile");

    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Coalesce {
                    expressions: coalesced_expressions,
                },
                alias: coalesced_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Case {
                    alternatives: case_alternatives,
                    else_expression: case_else,
                },
                alias: case_alias,
            },
        ] if coalesced_alias == "owner_key_tokens"
            && matches!(
                coalesced_expressions.as_slice(),
                [
                    ScalarExpression::PresenceGated {
                        presence_variable,
                        expression,
                    },
                    ScalarExpression::TypedLiteralList {
                        literals: fallback_literals,
                        element_type: fallback_type,
                    },
                ] if presence_variable == "person"
                    && matches!(
                        expression.as_ref(),
                        ScalarExpression::TypedLiteralList { literals, element_type }
                            if literals.as_slice() == [
                                Literal::String("NAME".to_string()),
                                Literal::String("TEAM".to_string()),
                            ]
                            && *element_type == LiteralListElementType::String
                    )
                    && fallback_literals.as_slice() == [Literal::String("FALLBACK".to_string())]
                    && *fallback_type == LiteralListElementType::String
            )
            && case_alias == "team_keys"
            && matches!(
                case_alternatives.as_slice(),
                [ScalarCaseAlternative {
                    when: PredicateExpression::Presence(PresencePredicate {
                        variable,
                        operator: ComparisonOperator::Equal,
                    }),
                    then: ScalarExpression::TypedLiteralList { literals, element_type },
                }] if variable == "person"
                    && literals.is_empty()
                    && *element_type == LiteralListElementType::String
            )
            && matches!(
                case_else.as_deref(),
                Some(ScalarExpression::PresenceGated {
                    presence_variable,
                    expression,
                }) if presence_variable == "person"
                    && matches!(
                        expression.as_ref(),
                        ScalarExpression::TypedLiteralList { literals, element_type }
                            if literals.as_slice() == [Literal::String("team".to_string())]
                                && *element_type == LiteralListElementType::String
                    )
            )
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Coalesce { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_static_list_comprehensions_over_sliced_case_and_coalesce_sources() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
            &graph,
            "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN [k IN coalesce(keys(person), ['fallback', 'owner'])[0..1] | toUpper(k)] AS first_owner_key, \
                    [k IN (CASE WHEN person IS NULL THEN ['fallback', 'owner'] ELSE keys(person) END)[1..2] | k] AS second_owner_key \
             ORDER BY [k IN coalesce(keys(person), ['fallback'])[0..1] | k]",
        )
        .expect("static list comprehensions over sliced CASE/coalesce sources should compile");

    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Coalesce { expressions },
                alias: coalesce_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Case { .. },
                alias: case_alias,
            },
        ] if coalesce_alias == "first_owner_key"
            && case_alias == "second_owner_key"
            && matches!(
                expressions.as_slice(),
                [
                    ScalarExpression::PresenceGated { expression, .. },
                    ScalarExpression::TypedLiteralList { literals, element_type },
                ] if matches!(
                    expression.as_ref(),
                    ScalarExpression::TypedLiteralList {
                        literals: matched_literals,
                        element_type: matched_type,
                    } if matched_literals.as_slice() == [Literal::String("NAME".to_string())]
                        && *matched_type == LiteralListElementType::String
                )
                && literals.as_slice() == [Literal::String("FALLBACK".to_string())]
                && *element_type == LiteralListElementType::String
            )
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Coalesce { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_static_list_comprehensions_as_in_rhs() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
            &graph,
            "MATCH (service:Service) \
             WHERE service.tier IN [tier IN ['prod', 'dev'] WHERE tier <> 'dev' | tier] \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN 'TEAM' IN [k IN coalesce(keys(person), ['fallback']) | toUpper(k)] AS owner_has_team_key, \
                    'team' IN [k IN CASE WHEN person IS NULL THEN ['fallback'] ELSE keys(person) END | k] AS case_has_team_key",
        )
        .expect("static list comprehensions should compile as IN RHS values");

    assert!(matches!(
        plan.predicates.as_slice(),
        [PropertyPredicate {
            property: PropertyRef { variable, property },
            operator: ComparisonOperator::In,
            rhs: PredicateRhs::List(literals),
        }] if variable == "service"
            && property == "tier"
            && literals == &vec![Literal::String("prod".to_string())]
    ));
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Predicate(coalesced_predicate),
                alias: coalesced_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Predicate(case_predicate),
                alias: case_alias,
            },
        ] if coalesced_alias == "owner_has_team_key"
            && case_alias == "case_has_team_key"
            && is_case_boolean_scalar_predicate(coalesced_predicate.as_ref())
            && is_case_boolean_scalar_predicate(case_predicate.as_ref())
    ));
}

#[test]
fn compiles_static_list_comprehension_comparisons() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
            &graph,
            "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN [k IN coalesce(keys(person), ['fallback']) | k] = ['name', 'team'] AS coalesced_matches_keys, \
                    ['fallback'] = [k IN coalesce(keys(person), ['fallback']) | k] AS coalesced_is_fallback, \
                    [k IN CASE WHEN person IS NULL THEN ['fallback'] ELSE keys(person) END | k] <> [] AS case_non_empty \
             ORDER BY [k IN CASE WHEN person IS NULL THEN ['fallback'] ELSE keys(person) END | k] > ['fallback']",
        )
        .expect("static list comprehension comparisons should compile");

    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Predicate(coalesced_matches),
                alias: coalesced_matches_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Predicate(coalesced_fallback),
                alias: coalesced_fallback_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Predicate(case_non_empty),
                alias: case_non_empty_alias,
            },
        ] if coalesced_matches_alias == "coalesced_matches_keys"
            && coalesced_fallback_alias == "coalesced_is_fallback"
            && case_non_empty_alias == "case_non_empty"
            && is_case_boolean_scalar_predicate(coalesced_matches.as_ref())
            && is_case_boolean_scalar_predicate(coalesced_fallback.as_ref())
            && is_case_boolean_scalar_predicate(case_non_empty.as_ref())
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Predicate(_)),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_sliced_conditional_list_comprehension_predicates() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
            &graph,
            "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN [k IN coalesce(keys(person), ['fallback', 'owner'])[0..1] | k] = ['name'] AS coalesced_first_key_matches, \
                    'team' IN [k IN (CASE WHEN person IS NULL THEN ['fallback', 'owner'] ELSE keys(person) END)[1..2] | k] AS case_second_key_has_team",
        )
        .expect("sliced conditional list comprehension predicates should compile");

    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Predicate(coalesced_predicate),
                alias: coalesced_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Predicate(case_predicate),
                alias: case_alias,
            },
        ] if coalesced_alias == "coalesced_first_key_matches"
            && case_alias == "case_second_key_has_team"
            && is_case_boolean_scalar_predicate(coalesced_predicate.as_ref())
            && is_case_boolean_scalar_predicate(case_predicate.as_ref())
    ));
}

#[test]
fn compiles_static_list_comprehension_expression_filters() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             RETURN [k IN keys(service) WHERE toUpper(k) STARTS WITH 'T'] AS upper_t_keys, \
                    [x IN ['1', '2', 'bad'] WHERE toIntegerOrNull(x) >= 2] AS numeric_strings, \
                    [x IN [1, 2, 3] WHERE x + 1 >= 3] AS arithmetic_values, \
                    [x IN [1.2, 2.8] WHERE floor(x) = 2.0] AS floored_values, \
                    [x IN ['', 'a', null] WHERE isEmpty(x)] AS empty_strings, \
                    [x IN ['', 'a', null] WHERE isEmpty(x) = false] AS non_empty_strings",
    )
    .expect("static list comprehension expression filters should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("tier".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "upper_t_keys".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("2".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "numeric_strings".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::Integer(2), Literal::Integer(3)],
                    element_type: LiteralListElementType::Integer,
                },
                alias: "arithmetic_values".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::Float(OrderedFloat(2.8))],
                    element_type: LiteralListElementType::Float,
                },
                alias: "floored_values".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String(String::new())],
                    element_type: LiteralListElementType::String,
                },
                alias: "empty_strings".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("a".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "non_empty_strings".to_string(),
            },
        ]
    );
}

#[test]
fn rejects_dynamic_static_list_comprehension_sources() {
    let error = compile_cypher_for_graph(
        &star_test_graph(),
        "MATCH (service:Service) RETURN [c IN service.name] AS characters",
    )
    .expect_err("dynamic list comprehension sources should be rejected");

    assert!(
        error
            .to_string()
            .contains("static list comprehensions require"),
        "{error}"
    );
}

#[test]
fn compiles_mapped_static_list_comprehensions() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             RETURN [k IN keys(service) | toUpper(k)] AS upper_keys, \
                    [k IN [' name ', null, 'tier'] | trim(k)] AS trimmed_keys, \
                    [k IN ['service-id'] | replace(k, '-', '_')] AS replaced_keys, \
                    [k IN keys(service) | left(k, 2)] AS key_prefixes, \
                    [k IN ['service-name', null] | substring(k, 8, 4)] AS key_suffixes, \
                    [k IN ['ops'] | right(k, 2)] AS right_suffixes, \
                    [k IN ['abc'] | reverse(k)] AS reversed_literals, \
                    [k IN [1, 2] | toString(k)] AS number_strings \
             ORDER BY [k IN keys(service) WHERE k <> 'tier' | upper(k)]",
    )
    .expect("mapped static list comprehensions should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("NAME".to_string()),
                        Literal::String("TIER".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "upper_keys".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("name".to_string()),
                        Literal::Null,
                        Literal::String("tier".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "trimmed_keys".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("service_id".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "replaced_keys".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("na".to_string()),
                        Literal::String("ti".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "key_prefixes".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("name".to_string()), Literal::Null],
                    element_type: LiteralListElementType::String,
                },
                alias: "key_suffixes".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("ps".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "right_suffixes".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("cba".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "reversed_literals".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("1".to_string()),
                        Literal::String("2".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "number_strings".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::TypedLiteralList {
                literals: vec![Literal::String("NAME".to_string())],
                element_type: LiteralListElementType::String,
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_static_list_comprehension_null_maps() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             RETURN [k IN ['name', null] | coalesce(k, 'missing')] AS coalesced_keys, \
                    [k IN keys(service) | nullIf(k, 'tier')] AS nullified_tier, \
                    [k IN ['fallback'] | coalesce(null, k)] AS coalesced_second_arg, \
                    [k IN ['id'] | nullIf('id', k)] AS nullified_second_arg",
    )
    .expect("static list comprehension null maps should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("name".to_string()),
                        Literal::String("missing".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "coalesced_keys".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("name".to_string()), Literal::Null],
                    element_type: LiteralListElementType::String,
                },
                alias: "nullified_tier".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("fallback".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "coalesced_second_arg".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::Null],
                    element_type: LiteralListElementType::String,
                },
                alias: "nullified_second_arg".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_static_list_comprehension_length_maps() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             RETURN [k IN keys(service) | size(k)] AS key_lengths, \
                    [k IN ['ops', null] | char_length(k)] AS literal_lengths, \
                    [k IN ['deploy'] | character_length(k)] AS gql_literal_lengths",
    )
    .expect("static list comprehension length maps should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::Integer(4), Literal::Integer(4)],
                    element_type: LiteralListElementType::Integer,
                },
                alias: "key_lengths".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::Integer(3), Literal::Null],
                    element_type: LiteralListElementType::Integer,
                },
                alias: "literal_lengths".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::Integer(6)],
                    element_type: LiteralListElementType::Integer,
                },
                alias: "gql_literal_lengths".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_static_list_comprehension_strict_cast_maps() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             RETURN [x IN ['1', '2', null] | toInteger(x)] AS ints, \
                    [x IN ['1.5', '2.25', null] | toFloat(x)] AS floats, \
                    [x IN ['true', 'FALSE', null] | toBoolean(x)] AS booleans, \
                    [x IN [1, 2, null] | toString(x)] AS strings",
    )
    .expect("static list comprehension strict cast maps should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::Integer(1), Literal::Integer(2), Literal::Null],
                    element_type: LiteralListElementType::Integer,
                },
                alias: "ints".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Float(OrderedFloat(1.5)),
                        Literal::Float(OrderedFloat(2.25)),
                        Literal::Null
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "floats".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Boolean(true),
                        Literal::Boolean(false),
                        Literal::Null
                    ],
                    element_type: LiteralListElementType::Boolean,
                },
                alias: "booleans".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("1".to_string()),
                        Literal::String("2".to_string()),
                        Literal::Null
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "strings".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_static_list_comprehension_nullable_cast_maps() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             RETURN [x IN ['bad', '3', null] | toIntegerOrNull(x)] AS ints, \
                    [x IN ['bad', '2.5', null] | toFloatOrNull(x)] AS floats, \
                    [x IN ['maybe', 'true', null] | toBooleanOrNull(x)] AS booleans, \
                    [x IN [1, 2, null] | toStringOrNull(x)] AS strings",
    )
    .expect("static list comprehension nullable cast maps should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::Null, Literal::Integer(3), Literal::Null],
                    element_type: LiteralListElementType::Integer,
                },
                alias: "ints".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Null,
                        Literal::Float(OrderedFloat(2.5)),
                        Literal::Null
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "floats".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::Null, Literal::Boolean(true), Literal::Null],
                    element_type: LiteralListElementType::Boolean,
                },
                alias: "booleans".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("1".to_string()),
                        Literal::String("2".to_string()),
                        Literal::Null
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "strings".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_static_list_comprehension_numeric_function_maps() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             RETURN [x IN [1, 3, 6] | abs(x - 3)] AS absolute_ints, \
                    [x IN [1.5, null, 5.5] | abs(x - 3.0)] AS absolute_floats, \
                    [x IN [4, 9] | sqrt(x)] AS roots, \
                    [x IN [1.0, 3.0, 6.5, null] | sign(x - 3.0)] AS signs, \
                    [x IN [2, 3, null] | pow(x, 3)] AS powers, \
                    [x IN [2, 3] | power(x, 2)] AS squares, \
                    [x IN [1.0, 2.0, null] | isNaN(x)] AS nan_checks",
    )
    .expect("static list comprehension numeric function maps should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Integer(2),
                        Literal::Integer(0),
                        Literal::Integer(3)
                    ],
                    element_type: LiteralListElementType::Integer,
                },
                alias: "absolute_ints".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Float(OrderedFloat(1.5)),
                        Literal::Null,
                        Literal::Float(OrderedFloat(2.5))
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "absolute_floats".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Float(OrderedFloat(2.0)),
                        Literal::Float(OrderedFloat(3.0))
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "roots".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Integer(-1),
                        Literal::Integer(0),
                        Literal::Integer(1),
                        Literal::Null
                    ],
                    element_type: LiteralListElementType::Integer,
                },
                alias: "signs".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Float(OrderedFloat(8.0)),
                        Literal::Float(OrderedFloat(27.0)),
                        Literal::Null
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "powers".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Float(OrderedFloat(4.0)),
                        Literal::Float(OrderedFloat(9.0))
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "squares".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Boolean(false),
                        Literal::Boolean(false),
                        Literal::Null
                    ],
                    element_type: LiteralListElementType::Boolean,
                },
                alias: "nan_checks".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_static_list_comprehension_log_exp_constant_maps() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             RETURN [x IN [0, 1, null] | round(exp(x), 0)] AS exponentials, \
                    [x IN [1.0, 2.718281828459045, null] | round(log(x), 0)] AS natural_logs, \
                    [x IN [1, 100, null] | log10(x)] AS base10_logs, \
                    [x IN [1, 2] | round(pi(), x)] AS rounded_pi, \
                    [x IN [1, 2] | round(e(), x)] AS rounded_e",
    )
    .expect("static list comprehension log/exp/constant maps should compile");

    let rounded_pi_one =
        round_static_float(std::f64::consts::PI, 1, "test").expect("pi should round");
    let rounded_pi_two =
        round_static_float(std::f64::consts::PI, 2, "test").expect("pi should round");
    let rounded_e_one = round_static_float(std::f64::consts::E, 1, "test").expect("e should round");
    let rounded_e_two = round_static_float(std::f64::consts::E, 2, "test").expect("e should round");
    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Float(OrderedFloat(1.0)),
                        Literal::Float(OrderedFloat(3.0)),
                        Literal::Null
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "exponentials".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Float(OrderedFloat(0.0)),
                        Literal::Float(OrderedFloat(1.0)),
                        Literal::Null
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "natural_logs".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Float(OrderedFloat(0.0)),
                        Literal::Float(OrderedFloat(2.0)),
                        Literal::Null
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "base10_logs".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Float(OrderedFloat(rounded_pi_one)),
                        Literal::Float(OrderedFloat(rounded_pi_two))
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "rounded_pi".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Float(OrderedFloat(rounded_e_one)),
                        Literal::Float(OrderedFloat(rounded_e_two))
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "rounded_e".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_static_list_comprehension_unary_trigonometric_function_maps() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             RETURN [x IN [0.0, 1.5707963267948966] | round(sin(x), 0)] AS sines, \
                    [x IN [0.0, 1.5707963267948966] | round(cos(x), 0)] AS cosines, \
                    [x IN [0.0, 0.7853981633974483] | round(tan(x), 0)] AS tangents, \
                    [x IN [0.7853981633974483] | round(cot(x), 0)] AS cotangents, \
                    [x IN [1.0] | round(asin(x), 0)] AS arcsines, \
                    [x IN [1.0] | round(acos(x), 0)] AS arccosines, \
                    [x IN [1.0] | round(atan(x), 0)] AS arctangents",
    )
    .expect("static list comprehension trigonometric maps should compile");

    assert_eq!(
        plan.projections,
        vec![
            typed_float_list_projection("sines", vec![0.0, 1.0]),
            typed_float_list_projection("cosines", vec![1.0, 0.0]),
            typed_float_list_projection("tangents", vec![0.0, 1.0]),
            typed_float_list_projection("cotangents", vec![1.0]),
            typed_float_list_projection("arcsines", vec![2.0]),
            typed_float_list_projection("arccosines", vec![0.0]),
            typed_float_list_projection("arctangents", vec![1.0]),
        ]
    );
}

#[test]
fn compiles_static_list_comprehension_angle_function_maps() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             RETURN [x IN [1.0] | round(atan2(x, x), 2)] AS arctangent_pairs, \
                    [x IN [3.141592653589793] | round(degrees(x), 0)] AS degree_values, \
                    [x IN [180.0] | round(radians(x), 2)] AS radian_values, \
                    [x IN [0.0] | haversin(x)] AS haversines",
    )
    .expect("static list comprehension angle maps should compile");

    let atan2_rounded =
        round_static_float(1.0_f64.atan2(1.0), 2, "test").expect("atan2 should round");
    let radians_rounded =
        round_static_float(180.0_f64.to_radians(), 2, "test").expect("radians should round");
    assert_eq!(
        plan.projections,
        vec![
            typed_float_list_projection("arctangent_pairs", vec![atan2_rounded]),
            typed_float_list_projection("degree_values", vec![180.0]),
            typed_float_list_projection("radian_values", vec![radians_rounded]),
            typed_float_list_projection("haversines", vec![0.0]),
        ]
    );
}

#[test]
fn compiles_static_list_comprehension_rounding_function_maps() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             RETURN [x IN [1.2, 2.8, null] | ceiling(x)] AS ceilings, \
                    [x IN [1.2, 2.8, null] | floor(x)] AS floors, \
                    [x IN [1.24, 1.25, 1.26] | round(x, 1)] AS rounded_tenths, \
                    [x IN [1.4, 1.5, 1.6] | round(x)] AS rounded_wholes",
    )
    .expect("static list comprehension rounding function maps should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Float(OrderedFloat(2.0)),
                        Literal::Float(OrderedFloat(3.0)),
                        Literal::Null
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "ceilings".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Float(OrderedFloat(1.0)),
                        Literal::Float(OrderedFloat(2.0)),
                        Literal::Null
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "floors".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Float(OrderedFloat(1.2)),
                        Literal::Float(OrderedFloat(1.3)),
                        Literal::Float(OrderedFloat(1.3))
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "rounded_tenths".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Float(OrderedFloat(1.0)),
                        Literal::Float(OrderedFloat(2.0)),
                        Literal::Float(OrderedFloat(2.0))
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "rounded_wholes".to_string(),
            },
        ]
    );
}

#[test]
fn rejects_static_list_comprehension_null_maps_with_invalid_arguments() {
    let error = compile_cypher_for_graph(
        &star_test_graph(),
        "MATCH (service:Service) RETURN [k IN keys(service) | coalesce(k)] AS values",
    )
    .expect_err("coalesce() should require at least two arguments");

    assert!(
        error.to_string().contains(
            "coalesce() in static list comprehension maps requires at least two arguments"
        ),
        "{error}"
    );

    let error = compile_cypher_for_graph(
        &star_test_graph(),
        "MATCH (service:Service) RETURN [k IN keys(service) | nullIf(k)] AS values",
    )
    .expect_err("nullIf() should require exactly two arguments");

    assert!(
        error
            .to_string()
            .contains("nullIf() in static list comprehension maps requires exactly two arguments"),
        "{error}"
    );
}

#[test]
fn rejects_static_list_comprehension_numeric_function_maps_with_invalid_arguments() {
    let error = compile_cypher_for_graph(
        &star_test_graph(),
        "MATCH (service:Service) RETURN [x IN [1] | sqrt(x - 2)] AS roots",
    )
    .expect_err("sqrt() should reject negative static values");

    assert!(
        error.to_string().contains(
            "sqrt() in static list comprehension maps requires non-negative numeric arguments"
        ),
        "{error}"
    );

    let error = compile_cypher_for_graph(
        &star_test_graph(),
        "MATCH (service:Service) RETURN [x IN ['not-numeric'] | abs(x)] AS values",
    )
    .expect_err("abs() should reject non-numeric static values");

    assert!(
        error
            .to_string()
            .contains("static numeric map expressions require numeric operands"),
        "{error}"
    );

    let error = compile_cypher_for_graph(
        &star_test_graph(),
        "MATCH (service:Service) RETURN [x IN ['bad'] | toInteger(x)] AS values",
    )
    .expect_err("toInteger() should reject invalid strict casts");

    assert!(
        error
            .to_string()
            .contains("toInteger() in static list comprehension maps cannot cast value to integer"),
        "{error}"
    );

    let error = compile_cypher_for_graph(
        &star_test_graph(),
        "MATCH (service:Service) RETURN [x IN [1.2] | round(x, '1')] AS values",
    )
    .expect_err("round() should reject non-integer precision");

    assert!(
        error.to_string().contains(
            "round() in static list comprehension maps requires integer precision arguments"
        ),
        "{error}"
    );

    let error = compile_cypher_for_graph(
        &star_test_graph(),
        "MATCH (service:Service) RETURN [x IN [2] | pow(x)] AS values",
    )
    .expect_err("pow() should require two static map arguments");

    assert!(
        error
            .to_string()
            .contains("pow() in static list comprehension maps requires exactly two arguments"),
        "{error}"
    );

    let error = compile_cypher_for_graph(
        &star_test_graph(),
        "MATCH (service:Service) RETURN [x IN [2] | pi(x)] AS values",
    )
    .expect_err("pi() should reject static map arguments");

    assert!(
        error
            .to_string()
            .contains("pi() in static list comprehension maps requires exactly zero arguments"),
        "{error}"
    );

    let error = compile_cypher_for_graph(
        &star_test_graph(),
        "MATCH (service:Service) RETURN [x IN [2] | atan2(x)] AS values",
    )
    .expect_err("atan2() should require two static map arguments");

    assert!(
        error
            .to_string()
            .contains("atan2() in static list comprehension maps requires exactly two arguments"),
        "{error}"
    );
}

#[test]
fn rejects_static_list_comprehension_string_maps_with_invalid_arguments() {
    let error = compile_cypher_for_graph(
        &star_test_graph(),
        "MATCH (service:Service) RETURN [k IN keys(service) | left(k, 'x')] AS key_prefixes",
    )
    .expect_err("left() count should be integer");

    assert!(
        error
            .to_string()
            .contains("left() in static list comprehension maps requires integer arguments"),
        "{error}"
    );

    let error = compile_cypher_for_graph(
        &star_test_graph(),
        "MATCH (service:Service) RETURN [k IN keys(service) | substring(k, -1, 2)] AS key_prefixes",
    )
    .expect_err("substring() start should be non-negative");

    assert!(
        error.to_string().contains(
            "substring() in static list comprehension maps requires non-negative integer arguments"
        ),
        "{error}"
    );
}

#[test]
fn compiles_static_list_comprehension_string_filters() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
            &graph,
            "MATCH (service:Service) \
             RETURN [k IN keys(service) WHERE k STARTS WITH 't'] AS starts_with_t, \
                    [k IN keys(service) WHERE k ENDS WITH 'e'] AS ends_with_e, \
                    [k IN ['billing', 'deployments', 'experiments'] WHERE k CONTAINS 'ing'] AS contains_ing, \
                    [k IN keys(service) WHERE k =~ '^t.*'] AS regex_t, \
                    [k IN keys(service) WHERE k > 'risk'] AS after_risk, \
                    [k IN keys(service) WHERE k <= 'name'] AS through_name",
        )
        .expect("static list comprehension string filters should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("tier".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "starts_with_t".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("name".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "ends_with_e".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("billing".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "contains_ing".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("tier".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "regex_t".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("tier".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "after_risk".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("name".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "through_name".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_static_collection_string_ordering_predicates() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             WHERE any(key IN keys(service) WHERE key > 'team') \
             RETURN all(key IN keys(service) WHERE key >= 'name') AS keys_after_name",
    )
    .expect("static collection string ordering predicates should compile");

    assert_eq!(plan.predicate, Some(PredicateExpression::Boolean(true)));
    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::Predicate(Box::new(PredicateExpression::Boolean(true))),
            alias: "keys_after_name".to_string(),
        }]
    );
}

#[test]
fn compiles_numeric_and_boolean_static_list_comprehension_maps() {
    let graph = star_test_graph();
    let parameters = BTreeMap::from([(
        "weights".to_string(),
        CypherParameterValue::List(vec![
            Literal::Integer(2),
            Literal::Integer(4),
            Literal::Null,
        ]),
    )]);
    let plan = compile_cypher_for_graph_with_parameters(
        &graph,
        "MATCH (service:Service) \
             RETURN [x IN [1, 2, 3] | x + 1] AS incremented, \
                    [x IN [1.5, 2.5] | x * 2] AS doubled, \
                    [x IN $weights | x / 2] AS halved_weights, \
                    [k IN keys(service) | k STARTS WITH 't'] AS t_flags, \
                    [x IN ['', 'a', null] | isEmpty(x)] AS empty_flags",
        &parameters,
    )
    .expect("numeric and boolean static list comprehension maps should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Integer(2),
                        Literal::Integer(3),
                        Literal::Integer(4)
                    ],
                    element_type: LiteralListElementType::Integer,
                },
                alias: "incremented".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Float(OrderedFloat(3.0)),
                        Literal::Float(OrderedFloat(5.0))
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "doubled".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Float(OrderedFloat(1.0)),
                        Literal::Float(OrderedFloat(2.0)),
                        Literal::Null
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "halved_weights".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::Boolean(false), Literal::Boolean(true)],
                    element_type: LiteralListElementType::Boolean,
                },
                alias: "t_flags".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Boolean(true),
                        Literal::Boolean(false),
                        Literal::Null
                    ],
                    element_type: LiteralListElementType::Boolean,
                },
                alias: "empty_flags".to_string(),
            },
        ]
    );
}

#[test]
fn rejects_dynamic_mapped_static_list_comprehensions() {
    let error = compile_cypher_for_graph(
        &star_test_graph(),
        "MATCH (service:Service) RETURN [k IN keys(service) | service.name] AS key_names",
    )
    .expect_err("dynamic mapped static list comprehensions should be rejected");

    assert!(
        error
            .to_string()
            .contains("static list comprehension map expressions support"),
        "{error}"
    );
}

#[test]
fn compiles_optional_static_list_concatenation_as_presence_gated_scalar() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN labels(person) + keys(person) AS owner_metadata",
    )
    .expect("optional static list concatenation should compile");

    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::PresenceGated {
                presence_variable: "person".to_string(),
                expression: Box::new(ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("Person".to_string()),
                        Literal::String("name".to_string()),
                        Literal::String("team".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                }),
            },
            alias: "owner_metadata".to_string(),
        }]
    );
}

#[test]
fn rejects_incompatible_static_list_concatenation() {
    for (cypher, expected) in [
        (
            "MATCH (service:Service) RETURN ['a'] + [1] AS values",
            "static list concatenation requires compatible non-null element types",
        ),
        (
            "MATCH (service:Service) RETURN ['a'] + 1 AS values",
            "static list concatenation requires compatible non-null element types",
        ),
        (
            "MATCH (service:Service) RETURN ['a', 1] + [] AS values",
            "static list concatenation requires each operand to have a single non-null element type",
        ),
        (
            "MATCH (service:Service) RETURN [] + [null] AS values",
            "static list expressions require a known non-null element type",
        ),
    ] {
        let error = compile_cypher_for_graph(&star_test_graph(), cypher).expect_err(expected);
        assert!(
            error.to_string().contains(expected),
            "expected '{expected}' in error: {error}"
        );
    }
}

#[test]
fn compiles_static_list_comparison_predicates() {
    let graph = star_test_graph();
    let parameters = BTreeMap::from([(
        "tiers".to_string(),
        CypherParameterValue::List(vec![
            Literal::String("prod".to_string()),
            Literal::String("dev".to_string()),
        ]),
    )]);
    let plan = compile_cypher_for_graph_with_parameters(
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE tail(keys(service)) = ['tier'] \
               AND [] = tail(labels(service)) \
               AND tail($tiers) <> ['prod'] \
             RETURN service.name AS service",
        &parameters,
    )
    .expect("static list comparison predicates should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::And {
                left: Box::new(PredicateExpression::Boolean(true)),
                right: Box::new(PredicateExpression::Boolean(true)),
            }),
            right: Box::new(PredicateExpression::Boolean(true)),
        })
    );
}

#[test]
fn compiles_static_list_quantifier_predicates() {
    let graph = star_test_graph();
    let parameters = BTreeMap::from([(
        "selected_keys".to_string(),
        CypherParameterValue::List(vec![
            Literal::String("missing".to_string()),
            Literal::String("tier".to_string()),
        ]),
    )]);
    let plan = compile_cypher_for_graph_with_parameters(
        &graph,
        "MATCH (service:Service) \
             WHERE all(key IN keys(service) WHERE key <> 'deprecated') \
               AND any(key IN tail(keys(service)) WHERE key = 'tier') \
               AND none(label IN labels(service) WHERE label = 'Team') \
               AND single(key IN ['name', 'tier', 'risk'] WHERE key STARTS WITH 'r') \
               AND any(key IN $selected_keys WHERE key IN keys(service)) \
             RETURN all(key IN keys(service) WHERE key <> 'deprecated') AS all_declared, \
                    any(label IN labels(service) WHERE label = 'Service') AS has_label",
        &parameters,
    )
    .expect("static list collection predicates should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::And {
                left: Box::new(PredicateExpression::And {
                    left: Box::new(PredicateExpression::And {
                        left: Box::new(PredicateExpression::Boolean(true)),
                        right: Box::new(PredicateExpression::Boolean(true)),
                    }),
                    right: Box::new(PredicateExpression::Boolean(true)),
                }),
                right: Box::new(PredicateExpression::Boolean(true)),
            }),
            right: Box::new(PredicateExpression::Boolean(true)),
        })
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::Predicate(Box::new(PredicateExpression::Boolean(
                    true
                ),)),
                alias: "all_declared".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Predicate(Box::new(PredicateExpression::Boolean(
                    true
                ),)),
                alias: "has_label".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_optional_static_list_quantifiers_as_presence_gated_predicates() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN all(key IN keys(person) WHERE key <> 'deprecated') AS owner_keys_declared",
    )
    .expect("optional static list collection predicate should compile");

    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::Predicate(Box::new(
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::PresenceGated {
                        presence_variable: "person".to_string(),
                        expression: Box::new(ScalarExpression::Literal(Literal::Boolean(true))),
                    },
                    operator: ComparisonOperator::Equal,
                    rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                        Literal::Boolean(true),
                    )),
                }),
            )),
            alias: "owner_keys_declared".to_string(),
        }]
    );
}

#[test]
fn compiles_static_list_case_and_coalesce_quantifier_predicates() {
    let query = compile_cypher_query_for_graph(
            &star_test_graph(),
            "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN any(key IN CASE WHEN person IS NULL THEN ['fallback'] ELSE keys(person) END WHERE key = 'team') AS case_has_team_key, \
                    all(key IN coalesce(keys(person), ['fallback']) WHERE key <> 'deprecated') AS coalesced_all_declared, \
                    none(key IN CASE WHEN service.tier = 'prod' THEN [] ELSE null END WHERE key = 'x') AS empty_none \
             ORDER BY any(key IN CASE WHEN person IS NULL THEN ['fallback'] ELSE keys(person) END WHERE key = 'team')",
        )
        .expect("static list CASE/coalesce collection predicates should compile");

    let GraphQuery::Plan(plan) = query else {
        panic!("expected single graph plan");
    };
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Predicate(case_predicate),
                alias: case_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Predicate(coalesce_predicate),
                alias: coalesce_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Predicate(empty_predicate),
                alias: empty_alias,
            },
        ] if case_alias == "case_has_team_key"
            && coalesce_alias == "coalesced_all_declared"
            && empty_alias == "empty_none"
            && matches!(
                case_predicate.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::Case { .. },
                    operator: ComparisonOperator::Equal,
                    rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                        Literal::Boolean(true),
                    )),
                })
            )
            && matches!(
                coalesce_predicate.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::Case { .. },
                    operator: ComparisonOperator::Equal,
                    rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                        Literal::Boolean(true),
                    )),
                })
            )
            && matches!(
                empty_predicate.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::Case { .. },
                    operator: ComparisonOperator::Equal,
                    rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                        Literal::Boolean(true),
                    )),
                })
            )
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Predicate(_)),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_static_list_case_and_coalesce_slice_quantifier_predicates() {
    let query = compile_cypher_query_for_graph(
            &star_test_graph(),
            "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN any(key IN (CASE WHEN person IS NULL THEN ['fallback', 'owner'] ELSE keys(person) END)[1..2] WHERE key = 'team') AS case_slice_has_team, \
                    all(key IN coalesce(keys(person), ['fallback', 'owner'])[0..1] WHERE key <> 'deprecated') AS coalesced_slice_all_declared, \
                    none(key IN (CASE WHEN service.tier = 'prod' THEN [] ELSE ['not-prod'] END)[0..1] WHERE key = 'prod') AS tier_slice_none_prod, \
                    single(key IN coalesce(keys(person), ['fallback', 'owner'])[0..1] WHERE key STARTS WITH 'f') AS coalesced_slice_single_fallback \
             ORDER BY any(key IN (CASE WHEN person IS NULL THEN ['fallback', 'owner'] ELSE keys(person) END)[1..2] WHERE key = 'team')",
        )
        .expect("sliced static list CASE/coalesce collection predicates should compile");

    let GraphQuery::Plan(plan) = query else {
        panic!("expected single graph plan");
    };
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Predicate(case_predicate),
                alias: case_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Predicate(all_predicate),
                alias: all_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Predicate(none_predicate),
                alias: none_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Predicate(single_predicate),
                alias: single_alias,
            },
        ] if case_alias == "case_slice_has_team"
            && all_alias == "coalesced_slice_all_declared"
            && none_alias == "tier_slice_none_prod"
            && single_alias == "coalesced_slice_single_fallback"
            && is_case_boolean_scalar_predicate(case_predicate.as_ref())
            && is_case_boolean_scalar_predicate(all_predicate.as_ref())
            && is_case_boolean_scalar_predicate(none_predicate.as_ref())
            && is_case_boolean_scalar_predicate(single_predicate.as_ref())
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Predicate(_)),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_static_list_quantifiers_without_where_and_preserves_unknown() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN all(flag IN [true, true]) AS all_true, \
                    any(flag IN [false, null]) AS any_unknown, \
                    none(flag IN [false]) AS none_true, \
                    single(flag IN [true, null]) AS single_unknown",
    )
    .expect("boolean list collection predicates should compile");

    assert_eq!(
        plan.projections
            .first()
            .expect("expected all_true projection"),
        &Projection::Expression {
            expression: ScalarExpression::Predicate(Box::new(PredicateExpression::Boolean(true))),
            alias: "all_true".to_string(),
        }
    );
    assert!(matches!(
        plan.projections
            .get(1)
            .expect("expected any_unknown projection"),
        Projection::Expression {
            expression: ScalarExpression::Predicate(predicate),
            alias,
        } if alias == "any_unknown"
            && matches!(
                predicate.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::Literal(Literal::Null),
                    operator: ComparisonOperator::Equal,
                    rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                        Literal::Boolean(true)
                    )),
                })
            )
    ));
    assert_eq!(
        plan.projections
            .get(2)
            .expect("expected none_true projection"),
        &Projection::Expression {
            expression: ScalarExpression::Predicate(Box::new(PredicateExpression::Boolean(true))),
            alias: "none_true".to_string(),
        }
    );
    assert!(matches!(
        plan.projections
            .get(3)
            .expect("expected single_unknown projection"),
        Projection::Expression {
            expression: ScalarExpression::Predicate(predicate),
            alias,
        } if alias == "single_unknown"
            && matches!(
                predicate.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::Literal(Literal::Null),
                    operator: ComparisonOperator::Equal,
                    rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                        Literal::Boolean(true)
                    )),
                })
            )
    ));
}

#[test]
fn rejects_dynamic_list_quantifier_collections() {
    let graph = star_test_graph();
    let error = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             WHERE any(key IN service.name WHERE key = 'billing-api') \
             RETURN service.name AS service",
    )
    .expect_err("dynamic collection predicates should be rejected");

    assert!(
            error
                .to_string()
                .contains("collection predicates require a literal list, list parameter, static split(...), range(...), tail(...), or static labels()/keys() metadata list"),
            "unexpected error: {error}"
        );
}

#[test]
fn compiles_optional_static_list_comparisons_as_presence_gated_predicates() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN tail(keys(person)) = ['team'] AS owner_key_tail_matches",
    )
    .expect("optional static list comparison should compile");

    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::Predicate(Box::new(
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::PresenceGated {
                        presence_variable: "person".to_string(),
                        expression: Box::new(ScalarExpression::Literal(Literal::Boolean(true,))),
                    },
                    operator: ComparisonOperator::Equal,
                    rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                        Literal::Boolean(true),
                    )),
                }),
            )),
            alias: "owner_key_tail_matches".to_string(),
        }]
    );
}

#[test]
fn rejects_tail_with_ambiguous_list_element_type() {
    for cypher in [
        "MATCH (service:Service) RETURN tail([]) AS values",
        "MATCH (service:Service) RETURN tail([null]) AS values",
        "MATCH (service:Service) RETURN tail([1, 'prod']) AS values",
    ] {
        let error = compile_cypher(cypher).expect_err("ambiguous tail() should be rejected");
        assert!(
            error
                .to_string()
                .contains("tail() requires a list with a known non-null element type"),
            "unexpected error: {error}"
        );
    }
}

#[test]
fn compiles_optional_static_list_endpoint_functions_as_presence_gated_scalars() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN head(labels(person)) AS owner_label, \
                    last(keys(person)) AS owner_last_key",
    )
    .expect("optional list endpoint functions should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "person".to_string(),
                    expression: Box::new(ScalarExpression::Literal(Literal::String(
                        "Person".to_string(),
                    ))),
                },
                alias: "owner_label".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "person".to_string(),
                    expression: Box::new(ScalarExpression::Literal(Literal::String(
                        "team".to_string(),
                    ))),
                },
                alias: "owner_last_key".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_metadata_list_size_scalar_expressions() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE size(labels(service)) = 1 \
               AND size(keys(owns)) = 2 \
             RETURN size(labels(service)) AS service_label_count, \
                    size(keys(service)) AS service_key_count \
             ORDER BY size(keys(owns))",
    )
    .expect("metadata list sizes should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Literal(Literal::Integer(1)),
                operator: ComparisonOperator::Equal,
                rhs: ScalarPredicateRhs::Expression(
                    ScalarExpression::Literal(Literal::Integer(1),)
                ),
            })),
            right: Box::new(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Literal(Literal::Integer(2)),
                operator: ComparisonOperator::Equal,
                rhs: ScalarPredicateRhs::Expression(
                    ScalarExpression::Literal(Literal::Integer(2),)
                ),
            })),
        })
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Integer(1)),
                alias: "service_label_count".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Integer(2)),
                alias: "service_key_count".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Literal(Literal::Integer(2,))),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_optional_metadata_list_sizes_as_presence_gated_scalars() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN size(labels(person)) AS owner_label_count, \
                    size(keys(person)) AS owner_key_count",
    )
    .expect("optional metadata list sizes should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "person".to_string(),
                    expression: Box::new(ScalarExpression::Literal(Literal::Integer(1))),
                },
                alias: "owner_label_count".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "person".to_string(),
                    expression: Box::new(ScalarExpression::Literal(Literal::Integer(2))),
                },
                alias: "owner_key_count".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_ordered_metadata_list_predicates() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             WHERE labels(service) > ['Account'] \
             RETURN service.name AS service",
    )
    .expect("ordered metadata list predicates should compile");

    assert_eq!(plan.predicate, Some(PredicateExpression::Boolean(true)));
}

#[test]
fn compiles_ordered_static_list_predicates_with_literal_left_side() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             WHERE ['Team'] > labels(service) \
               AND labels(service) <= ['Service', 'z'] \
               AND [1, 2] < [1, 3] \
             RETURN service.name AS service",
    )
    .expect("ordered static list predicates should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::And {
                left: Box::new(PredicateExpression::Boolean(true)),
                right: Box::new(PredicateExpression::Boolean(true)),
            }),
            right: Box::new(PredicateExpression::Boolean(true)),
        })
    );
}

#[test]
fn compiles_null_ordered_static_list_predicates_as_unknown() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             RETURN [null] < [null] AS unknown_order",
    )
    .expect("null ordered static list predicates should compile to unknown");

    assert!(matches!(
        plan.projections.as_slice(),
        [Projection::Expression {
            expression: ScalarExpression::Predicate(predicate),
            alias,
        }] if alias == "unknown_order"
            && matches!(
                predicate.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::Literal(Literal::Null),
                    operator: ComparisonOperator::Equal,
                    rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                        Literal::Boolean(true)
                    )),
                })
            )
    ));
}

#[test]
fn rejects_boolean_ordered_static_list_predicates() {
    let graph = star_test_graph();
    let error = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             WHERE [true] < [false] \
             RETURN service.name AS service",
    )
    .expect_err("boolean ordered static list predicates should be rejected");

    assert!(
        error
            .to_string()
            .contains("ordered static list predicates require string or numeric list elements"),
        "{error:?}"
    );
}

#[test]
fn rejects_cross_family_ordered_static_list_predicates() {
    let graph = star_test_graph();
    let error = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             WHERE labels(service) < [1] \
             RETURN service.name AS service",
    )
    .expect_err("cross-family ordered static list predicates should be rejected");

    assert!(
        error.to_string().contains("same orderable element family"),
        "{error:?}"
    );
}

#[test]
fn compiles_parameterized_label_membership_predicates() {
    let parameters = BTreeMap::from([(
        "label".to_string(),
        CypherParameterValue::Literal(Literal::String("Service".to_string())),
    )]);
    let plan = compile_cypher_with_parameters(
        "MATCH (service:Service) \
             WHERE $label IN labels(service) \
             RETURN service.name AS service",
        &parameters,
    )
    .expect("parameterized labels() membership should compile");

    assert_eq!(plan.predicate, Some(PredicateExpression::Boolean(true)));
}

#[test]
fn rejects_non_string_label_membership_predicates() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             WHERE 1 IN labels(service) \
             RETURN service.name AS service",
    )
    .expect_err("label membership should require a string literal");

    assert!(
        error.to_string().contains("label membership predicates"),
        "{error:?}"
    );
}

#[test]
fn compiles_node_label_predicates_as_boolean_constants() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE service:Service AND NOT (service:Team) \
             RETURN service.name AS service",
    )
    .expect("node label predicates should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::Boolean(true)),
            right: Box::new(PredicateExpression::Not {
                expression: Box::new(PredicateExpression::Boolean(false)),
            }),
        })
    );
}

#[test]
fn compiles_compound_node_label_predicates_as_boolean_constants() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE service:Service|Team \
             RETURN service.name AS service",
    )
    .expect("compound node label predicates should compile");

    assert_eq!(plan.predicate, Some(PredicateExpression::Boolean(true)));
}

#[test]
fn compiles_relationship_type_predicates_as_boolean_constants() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE owns:OWNS AND NOT (owns:DEPENDS_ON) \
             RETURN service.name AS service",
    )
    .expect("relationship type predicates should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::Boolean(true)),
            right: Box::new(PredicateExpression::Not {
                expression: Box::new(PredicateExpression::Boolean(false)),
            }),
        })
    );
}

#[test]
fn compiles_boolean_scalar_projections() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN service.risk > 0.8 AS high_risk, \
                    service.tier IS NULL AS missing_tier, \
                    service.name =~ '^billing.*' AS billing_service, \
                    service:Service AS is_service, \
                    owns:OWNS AS is_ownership, \
                    'tier' IN keys(service) AS has_tier \
             ORDER BY service.risk > 0.8 DESC",
    )
    .expect("boolean scalar projections should compile");

    assert_eq!(plan.projections.len(), 6);
    assert!(plan.projections.iter().all(|projection| {
        matches!(
            projection,
            Projection::Expression {
                expression: ScalarExpression::Predicate(_),
                ..
            }
        )
    }));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Predicate(_)),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_exists_subqueries_as_boolean_scalar_projections() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN EXISTS { MATCH (service)-[:DEPENDS_ON]->(:Service) } AS has_dependency \
             ORDER BY has_dependency DESC",
    )
    .expect("EXISTS subquery scalar projection should compile");

    assert!(matches!(
        plan.projections.as_slice(),
        [Projection::Expression {
            expression: ScalarExpression::Predicate(predicate),
            alias,
        }] if alias == "has_dependency"
            && matches!(predicate.as_ref(), PredicateExpression::ExistsPattern(_))
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::ProjectionAlias(alias),
            direction: OrderDirection::Descending,
            nulls: None,
        }] if alias == "has_dependency"
    ));
}

#[test]
fn compiles_compact_exists_pattern_where_predicates() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE EXISTS { (service)-[:DEPENDS_ON]->(target:Service) WHERE target.tier = 'dev' } \
             RETURN service.name AS service",
    )
    .expect("compact EXISTS pattern WHERE should compile");

    let Some(PredicateExpression::ExistsPattern(pattern)) = plan.predicate else {
        panic!("expected compact EXISTS pattern WHERE to compile as an EXISTS predicate");
    };
    assert!(pattern.predicates.iter().any(|predicate| {
        predicate.property.variable == "target"
            && predicate.property.property == "tier"
            && predicate.operator == ComparisonOperator::Equal
            && predicate.rhs == PredicateRhs::Literal(Literal::String("dev".to_string()))
    }));
}

#[test]
fn compiles_compact_count_pattern_where_predicates() {
    let plan = compile_cypher(
            "MATCH (service:Service) \
             RETURN COUNT { (service)-[:DEPENDS_ON]->(target:Service) WHERE target.tier = 'dev' } AS dev_dependencies",
        )
        .expect("compact COUNT pattern WHERE should compile");

    assert!(matches!(
        plan.projections.as_slice(),
        [Projection::Expression {
            expression: ScalarExpression::CountSubquery {
                pattern,
                distinct_target: None,
            },
            alias,
        }] if alias == "dev_dependencies"
            && matches!(pattern.as_ref(), CountSubqueryPattern::Relationships(pattern)
                if pattern.predicates.iter().any(|predicate| {
                    predicate.property.variable == "target"
                        && predicate.property.property == "tier"
                        && predicate.operator == ComparisonOperator::Equal
                        && predicate.rhs == PredicateRhs::Literal(Literal::String("dev".to_string()))
                }))
    ));
}

#[test]
fn compiles_compact_count_named_path_patterns() {
    let plan = compile_cypher(
            "MATCH (service:Service) \
             RETURN COUNT { dependency_path = (service)-[:DEPENDS_ON]->(:Service) } AS dependency_paths",
        )
        .expect("compact COUNT named path pattern should compile");

    assert!(matches!(
        plan.projections.as_slice(),
        [Projection::Expression {
            expression: ScalarExpression::CountSubquery {
                pattern,
                distinct_target: None,
            },
            alias,
        }] if alias == "dependency_paths"
            && matches!(pattern.as_ref(), CountSubqueryPattern::Relationships(_))
    ));
}

#[test]
fn compiles_collect_subquery_scalar_projections() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN COLLECT { \
               MATCH (service)-[:DEPENDS_ON]->(dependency:Service) \
               RETURN dependency.name \
             } AS dependency_names",
    )
    .expect("COLLECT subquery scalar projection should compile");

    assert!(matches!(
        plan.projections.as_slice(),
        [Projection::Expression {
            expression:
                ScalarExpression::CollectSubquery {
                    pattern,
                    target,
                    distinct,
                },
            alias,
        }] if alias == "dependency_names"
            && !*distinct
            && matches!(pattern.as_ref(), CountSubqueryPattern::Relationships(_))
            && matches!(
                target.as_ref(),
                ScalarExpression::Property(PropertyRef { variable, property })
                    if variable == "dependency" && property == "name"
            )
    ));
}

#[test]
fn compiles_collect_subquery_size_as_count_subquery() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN size(COLLECT { \
               MATCH (service)-[:DEPENDS_ON]->(dependency:Service) \
               RETURN dependency.name \
             }) AS dependency_count",
    )
    .expect("COLLECT subquery size should compile through count lowering");

    assert!(matches!(
        plan.projections.as_slice(),
        [Projection::Expression {
            expression:
                ScalarExpression::CountSubquery {
                    pattern,
                    distinct_target: None,
                },
            alias,
        }] if alias == "dependency_count"
            && matches!(pattern.as_ref(), CountSubqueryPattern::Relationships(pattern)
                if pattern.relationships.len() == 1)
    ));
}

#[test]
fn compiles_distinct_collect_subquery_size_as_distinct_count_subquery() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN size(COLLECT { \
               MATCH (service)-[:DEPENDS_ON]->(dependency:Service) \
               RETURN DISTINCT dependency.team \
             }) AS dependency_teams",
    )
    .expect("DISTINCT COLLECT subquery size should compile through distinct count lowering");

    assert!(matches!(
        plan.projections.as_slice(),
        [Projection::Expression {
            expression:
                ScalarExpression::CountSubquery {
                    pattern,
                    distinct_target: Some(target),
                },
            alias,
        }] if alias == "dependency_teams"
            && matches!(pattern.as_ref(), CountSubqueryPattern::Relationships(pattern)
                if pattern.relationships.len() == 1)
            && matches!(
                target.as_ref(),
                ScalarExpression::Property(PropertyRef { variable, property })
                    if variable == "dependency" && property == "team"
            )
    ));
}

#[test]
fn compiles_collect_subquery_is_empty_as_count_predicate() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE isEmpty(COLLECT { \
               MATCH (service)-[:DEPENDS_ON]->(dependency:Service) \
               WHERE dependency.tier = 'prod' \
               RETURN dependency.name \
             }) \
             RETURN service.name AS service",
    )
    .expect("COLLECT subquery isEmpty should compile through count lowering");

    assert!(matches!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::CountSubquery {
                pattern,
                distinct_target: None,
            },
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(0))),
        })) if matches!(pattern.as_ref(), CountSubqueryPattern::Relationships(pattern)
            if pattern.relationships.len() == 1
                && pattern.predicates.iter().any(|predicate| {
                    predicate.property.variable == "dependency"
                        && predicate.property.property == "tier"
                        && predicate.operator == ComparisonOperator::Equal
                        && predicate.rhs == PredicateRhs::Literal(Literal::String("prod".to_string()))
                }))
    ));
}

#[test]
fn compiles_pattern_comprehension_scalar_projections() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN [(service)-[dependency:DEPENDS_ON]->(target:Service) \
                       WHERE dependency.strength > 0.5 | target.name] AS dependency_names",
    )
    .expect("pattern comprehension projection should compile");

    assert!(matches!(
        plan.projections.as_slice(),
        [Projection::Expression {
            expression:
                ScalarExpression::CollectSubquery {
                    pattern,
                    target,
                    distinct,
                },
            alias,
        }] if alias == "dependency_names"
            && !*distinct
            && matches!(pattern.as_ref(), CountSubqueryPattern::Relationships(pattern)
                if pattern.relationships.len() == 1
                    && pattern.predicates.iter().any(|predicate| {
                        predicate.property.variable == "dependency"
                            && predicate.property.property == "strength"
                            && predicate.operator == ComparisonOperator::GreaterThan
                            && predicate.rhs == PredicateRhs::Literal(Literal::Float(OrderedFloat(0.5)))
                    }))
            && matches!(
                target.as_ref(),
                ScalarExpression::Property(PropertyRef { variable, property })
                    if variable == "target" && property == "name"
            )
    ));
}

#[test]
fn compiles_pattern_comprehension_path_variable_maps() {
    let plan = compile_cypher(
            "MATCH (service:Service) \
             RETURN [dependency_path = (service)-[:DEPENDS_ON]->(target:Service) | length(dependency_path)] AS dependency_lengths",
        )
        .expect("pattern comprehension path variable maps should compile");

    assert!(matches!(
        plan.projections.as_slice(),
        [Projection::Expression {
            expression:
                ScalarExpression::CollectSubquery {
                    pattern,
                    target,
                    distinct: false,
                },
            alias,
        }] if alias == "dependency_lengths"
            && matches!(pattern.as_ref(), CountSubqueryPattern::Relationships(pattern)
                if pattern.relationships.len() == 1)
            && matches!(target.as_ref(), ScalarExpression::Literal(Literal::Integer(1)))
    ));
}

#[test]
fn compiles_pattern_comprehension_size_as_count_subquery() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN size([(service)-[:DEPENDS_ON]->(target:Service) | target]) AS dependency_count",
    )
    .expect("pattern comprehension size should compile through count lowering");

    assert!(matches!(
        plan.projections.as_slice(),
        [Projection::Expression {
            expression:
                ScalarExpression::CountSubquery {
                    pattern,
                    distinct_target: None,
                },
            alias,
        }] if alias == "dependency_count"
            && matches!(pattern.as_ref(), CountSubqueryPattern::Relationships(pattern)
                if pattern.relationships.len() == 1)
    ));
}

#[test]
fn compiles_pattern_comprehension_is_empty_as_count_predicate() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE isEmpty([(service)-[:DEPENDS_ON]->(target:Service) \
                            WHERE target.tier = 'prod' | target]) \
             RETURN service.name AS service",
    )
    .expect("pattern comprehension isEmpty should compile through count lowering");

    assert!(matches!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::CountSubquery {
                pattern,
                distinct_target: None,
            },
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(0))),
        })) if matches!(pattern.as_ref(), CountSubqueryPattern::Relationships(pattern)
            if pattern.relationships.len() == 1
                && pattern.predicates.iter().any(|predicate| {
                    predicate.property.variable == "target"
                        && predicate.property.property == "tier"
                        && predicate.operator == ComparisonOperator::Equal
                        && predicate.rhs == PredicateRhs::Literal(Literal::String("prod".to_string()))
                }))
    ));
}

#[test]
fn rejects_pattern_comprehension_graph_object_maps() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             RETURN [(service)-[:DEPENDS_ON]->(target:Service) | target] AS dependencies",
    )
    .expect_err("pattern comprehension graph-object maps should remain rejected");

    assert!(
        error.to_string().contains("scalar alias"),
        "expected scalar alias rejection, got {error}"
    );
}

#[test]
fn rejects_collect_subqueries_without_single_scalar_return() {
    for (cypher, expected) in [
        (
            "MATCH (service:Service) \
                 RETURN COLLECT { MATCH (service)-[:DEPENDS_ON]->(dependency:Service) RETURN * } AS dependencies",
            "COLLECT subqueries require exactly one scalar RETURN projection",
        ),
        (
            "MATCH (service:Service) \
                 RETURN COLLECT { MATCH (service)-[:DEPENDS_ON]->(dependency:Service) RETURN dependency.name ORDER BY dependency.name } AS dependencies",
            "RETURN ORDER BY, SKIP, or LIMIT inside COLLECT subqueries requires scoped row-source planning",
        ),
        (
            "MATCH (service:Service) \
                 RETURN COLLECT { MATCH (service)-[:DEPENDS_ON]->(dependency:Service) RETURN count(*) } AS dependencies",
            "aggregate projections inside COLLECT subqueries require scoped aggregation planning",
        ),
    ] {
        let error = compile_cypher(cypher).expect_err("unsupported COLLECT shape should fail");
        assert!(
            error.to_string().contains(expected),
            "expected {expected:?}, got {error}"
        );
    }
}

#[test]
fn compiles_scoped_exists_where_boolean_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE EXISTS { \
               MATCH (service)-[:DEPENDS_ON]->(target:Service) \
               WHERE target.tier = 'dev' OR lower(target.name) CONTAINS 'api' \
             } \
             RETURN service.name AS service",
    )
    .expect("scoped EXISTS WHERE boolean expressions should compile");

    let Some(PredicateExpression::ExistsPattern(pattern)) = plan.predicate else {
        panic!("expected EXISTS subquery to compile as an EXISTS predicate");
    };
    assert!(matches!(
        pattern.predicate.as_deref(),
        Some(PredicateExpression::Or { left, right })
            if matches!(left.as_ref(), PredicateExpression::Comparison(_))
                && matches!(right.as_ref(), PredicateExpression::ScalarComparison(_))
    ));
}

#[test]
fn compiles_nested_scoped_exists_predicates() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE EXISTS { \
               MATCH (service)-[:DEPENDS_ON]->(target:Service) \
               WHERE EXISTS { MATCH (target)-[:DEPENDS_ON]->(:Service) } \
             } \
             RETURN service.name AS service",
    )
    .expect("nested scoped EXISTS predicates should compile");

    let Some(PredicateExpression::ExistsPattern(pattern)) = plan.predicate else {
        panic!("expected outer EXISTS predicate");
    };
    let Some(PredicateExpression::ExistsPattern(_)) = pattern.predicate.as_deref() else {
        panic!("expected nested EXISTS predicate");
    };
}

#[test]
fn compiles_nested_scoped_count_predicates() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE EXISTS { \
               MATCH (service)-[:DEPENDS_ON]->(target:Service) \
               WHERE COUNT { MATCH (target)-[:DEPENDS_ON]->(:Service) } > 0 \
             } \
             RETURN service.name AS service",
    )
    .expect("nested scoped COUNT predicates should compile");

    let Some(PredicateExpression::ExistsPattern(pattern)) = plan.predicate else {
        panic!("expected outer EXISTS predicate");
    };
    assert!(matches!(
        pattern.predicate.as_deref(),
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::CountSubquery { .. },
            ..
        }))
    ));
}

#[test]
fn compiles_noop_returns_inside_scoped_exists_and_count_subqueries() {
    let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE EXISTS { MATCH (service)-[:DEPENDS_ON]->(target:Service) RETURN DISTINCT target.name } \
             RETURN COUNT { MATCH (service)-[:DEPENDS_ON]->(target:Service) RETURN target.name, 1 } AS dependencies",
        )
        .expect("row-preserving scoped subquery RETURN clauses should compile");

    assert!(matches!(
        plan.predicate,
        Some(PredicateExpression::ExistsPattern(_))
    ));
    assert!(matches!(
        plan.projections.as_slice(),
        [Projection::Expression {
            expression: ScalarExpression::CountSubquery { .. },
            alias,
        }] if alias == "dependencies"
    ));
}

#[test]
fn compiles_distinct_return_inside_count_subqueries_as_count_target() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN COUNT { \
               MATCH (service)-[:DEPENDS_ON]->(target:Service) \
               RETURN DISTINCT target.team \
             } AS dependency_teams",
    )
    .expect("COUNT subquery DISTINCT scalar projection should compile");

    assert!(matches!(
        plan.projections.as_slice(),
        [Projection::Expression {
            expression:
                ScalarExpression::CountSubquery {
                    pattern,
                    distinct_target: Some(target),
                },
            alias,
        }] if alias == "dependency_teams"
            && matches!(pattern.as_ref(), CountSubqueryPattern::Relationships(_))
            && matches!(
                target.as_ref(),
                ScalarExpression::Property(PropertyRef { variable, property })
                    if variable == "target" && property == "team"
            )
    ));
}

#[test]
fn rejects_cardinality_changing_or_graph_expression_scoped_subquery_returns() {
    for (cypher, expected) in [
        (
            "MATCH (service:Service) \
                 RETURN COUNT { MATCH (service)-[:DEPENDS_ON]->(:Service) RETURN DISTINCT 1, 2 } AS dependencies",
            "RETURN DISTINCT inside COUNT subqueries currently supports exactly one scalar projection",
        ),
        (
            "MATCH (service:Service) \
                 WHERE EXISTS { MATCH (service)-[:DEPENDS_ON]->(target:Service) RETURN target } \
                 RETURN service.name AS service",
            "RETURN inside EXISTS subqueries currently supports only row-preserving scalar or literal projections or RETURN *",
        ),
        (
            "MATCH (service:Service) \
                 RETURN COUNT { MATCH (service)-[:DEPENDS_ON]->(:Service) RETURN 1 LIMIT 1 } AS dependencies",
            "RETURN ORDER BY, SKIP, or LIMIT inside COUNT subqueries requires scoped row-source planning",
        ),
    ] {
        let error =
            compile_cypher(cypher).expect_err("unsupported scoped subquery RETURN should fail");
        assert!(
            error.to_string().contains(expected),
            "expected {expected:?}, got {error}"
        );
    }
}

#[test]
fn compiles_projected_correlated_subquery_order_expressions_as_aliases() {
    for (cypher, expected_alias) in [
        (
            "MATCH (service:Service) \
                 RETURN EXISTS { MATCH (service)-[:DEPENDS_ON]->(:Service) } AS has_dependency \
                 ORDER BY EXISTS { MATCH (service)-[:DEPENDS_ON]->(:Service) } DESC",
            "has_dependency",
        ),
        (
            "MATCH (service:Service) \
                 RETURN COUNT { MATCH (service)-[:DEPENDS_ON]->(:Service) } AS dependency_count \
                 ORDER BY COUNT { MATCH (service)-[:DEPENDS_ON]->(:Service) } DESC",
            "dependency_count",
        ),
    ] {
        let plan = compile_cypher(cypher)
            .expect("projected correlated subquery ORDER BY expression should compile");

        assert!(matches!(
            plan.order_by.as_slice(),
            [OrderKey {
                expression: OrderExpression::ProjectionAlias(alias),
                direction: OrderDirection::Descending,
                nulls: None,
            }] if alias == expected_alias
        ));
    }
}

#[test]
fn compiles_hidden_direct_correlated_subquery_order_expressions() {
    for cypher in [
        "MATCH (service:Service) \
             RETURN service.name AS service \
             ORDER BY COUNT { MATCH (service)-[:DEPENDS_ON]->(:Service) } DESC",
        "MATCH (service:Service) \
             RETURN service.name AS service \
             ORDER BY EXISTS { MATCH (service)-[:DEPENDS_ON]->(:Service) } DESC",
    ] {
        let plan = compile_cypher(cypher)
            .expect("hidden direct correlated subquery ORDER BY expression should compile");

        assert!(matches!(
            plan.order_by.as_slice(),
            [OrderKey {
                expression: OrderExpression::Scalar(_),
                direction: OrderDirection::Descending,
                nulls: None,
            }]
        ));
    }
}

#[test]
fn compiles_compound_hidden_order_by_precomputable_correlated_subqueries() {
    for cypher in [
        "MATCH (service:Service) \
             RETURN service.name AS service \
             ORDER BY COUNT { MATCH (service)-[:DEPENDS_ON]->(:Service) } + 1 DESC",
        "MATCH (service:Service) \
             RETURN service.name AS service \
             ORDER BY EXISTS { MATCH (service)-[:DEPENDS_ON]->(:Service) } OR service.active DESC",
        "MATCH (service:Service) \
             RETURN service.name AS service \
             ORDER BY CASE \
               WHEN EXISTS { MATCH (service)-[:DEPENDS_ON]->(:Service) } THEN 0 \
               ELSE 1 \
             END ASC",
    ] {
        let plan = compile_cypher(cypher)
            .expect("compound hidden precomputable subquery ordering should compile");

        assert!(matches!(
            plan.order_by.as_slice(),
            [OrderKey {
                expression: OrderExpression::Scalar(_),
                ..
            }]
        ));
    }
}

#[test]
fn compiles_hidden_order_by_uncorrelated_node_count_subqueries() {
    for cypher in [
        "MATCH (service:Service) \
             RETURN service.name AS service \
             ORDER BY COUNT { MATCH (other:Service) } DESC, service",
        "MATCH (service:Service) \
             RETURN service.name AS service \
             ORDER BY COUNT { MATCH (other:Service) WHERE other.tier = 'prod' } + 1 DESC",
        "MATCH (service:Service) \
             RETURN service.name AS service \
             ORDER BY COUNT { MATCH (other:Service) RETURN DISTINCT other.tier } DESC",
    ] {
        let plan = compile_cypher(cypher)
            .expect("hidden uncorrelated node-count subquery ordering should compile");

        assert!(matches!(
            plan.order_by.first(),
            Some(OrderKey {
                expression: OrderExpression::Scalar(_),
                ..
            })
        ));
    }
}

#[test]
fn compiles_hidden_order_by_correlated_node_count_subqueries() {
    for cypher in [
        "MATCH (service:Service) \
             RETURN service.name AS service \
             ORDER BY COUNT { MATCH (other:Service) WHERE other.tier = service.tier } DESC",
        "MATCH (service:Service) \
             RETURN service.name AS service \
             ORDER BY COUNT { MATCH (other:Service) WHERE other.tier = service.tier } + 1 DESC",
        "MATCH (service:Service) \
             RETURN service.name AS service \
             ORDER BY COUNT { MATCH (other:Service) WHERE other.tier = service.tier RETURN DISTINCT other.team } DESC",
    ] {
        let plan = compile_cypher(cypher)
            .expect("hidden correlated node-count subquery ordering should compile");

        assert!(matches!(
            plan.order_by.first(),
            Some(OrderKey {
                expression: OrderExpression::Scalar(_),
                ..
            })
        ));
    }
}

#[test]
fn compiles_hidden_order_by_correlated_node_exists_subqueries() {
    for cypher in [
        "MATCH (service:Service) \
             RETURN service.name AS service \
             ORDER BY EXISTS { MATCH (other:Service) WHERE other.tier = service.tier } DESC",
        "MATCH (service:Service) \
             RETURN service.name AS service \
             ORDER BY CASE \
               WHEN EXISTS { MATCH (other:Service) WHERE other.tier = service.tier } THEN 0 \
               ELSE 1 \
             END ASC",
    ] {
        let plan = compile_cypher(cypher)
            .expect("hidden correlated node-exists subquery ordering should compile");

        assert!(matches!(
            plan.order_by.first(),
            Some(OrderKey {
                expression: OrderExpression::Scalar(_),
                ..
            })
        ));
    }
}

#[test]
fn compiles_is_empty_metadata_as_boolean_scalar_projections() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN isEmpty(labels(service)) AS service_labels_empty, \
                    isEmpty(keys(service)) AS service_keys_empty, \
                    isEmpty(keys(owns)) AS ownership_keys_empty \
             ORDER BY isEmpty(keys(owns))",
    )
    .expect("isEmpty metadata scalar projections should compile");

    assert_eq!(plan.projections.len(), 3);
    assert!(plan.projections.iter().all(|projection| {
        matches!(
            projection,
            Projection::Expression {
                expression: ScalarExpression::Predicate(_),
                ..
            }
        )
    }));
    let Projection::Expression {
        expression: ScalarExpression::Predicate(predicate),
        alias,
    } = plan
        .projections
        .first()
        .expect("expected labels isEmpty projection")
    else {
        panic!("expected labels isEmpty predicate projection");
    };
    assert_eq!(alias, "service_labels_empty");
    assert!(matches!(
        predicate.as_ref(),
        PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Literal(Literal::Boolean(false)),
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Boolean(true))),
        })
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Predicate(_)),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_is_empty_metadata_on_optional_relationship_endpoints() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[owns:OWNS]->(service) \
             RETURN isEmpty(labels(startNode(owns))) AS owner_labels_empty, \
                    isEmpty(keys(startNode(owns))) AS owner_keys_empty \
             ORDER BY isEmpty(labels(startNode(owns)))",
    )
    .expect("optional endpoint isEmpty metadata should compile");

    let Projection::Expression {
        expression: ScalarExpression::Predicate(predicate),
        alias,
    } = plan
        .projections
        .first()
        .expect("expected optional endpoint labels isEmpty projection")
    else {
        panic!("expected optional endpoint labels isEmpty projection");
    };
    assert_eq!(alias, "owner_labels_empty");
    assert!(matches!(
        predicate.as_ref(),
        PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::PresenceGated {
                presence_variable,
                expression,
            },
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Boolean(true))),
        }) if presence_variable == "owns"
            && matches!(expression.as_ref(), ScalarExpression::Literal(Literal::Boolean(false)))
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Predicate(_)),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn rejects_is_empty_keys_without_graph_declaration() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             RETURN isEmpty(keys(service)) AS service_keys_empty",
    )
    .expect_err("keys metadata emptiness should require graph declaration");

    assert!(
        error.to_string().contains("requires a graph declaration"),
        "{error}"
    );
}

#[test]
fn rejects_dynamic_node_label_predicates() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             WHERE service:$(label) \
             RETURN service.name AS service",
    )
    .expect_err("dynamic node label predicates should be rejected");

    assert!(
        error
            .to_string()
            .contains("dynamic label expressions require a string literal"),
        "{error:?}"
    );
}

#[test]
fn compiles_parameterized_dynamic_node_label_patterns() {
    let parameters = BTreeMap::from([(
        "label".to_string(),
        CypherParameterValue::Literal(Literal::String("Service".to_string())),
    )]);
    let plan = compile_cypher_with_parameters(
        "MATCH (service:$($label)) \
             RETURN service.name AS service",
        &parameters,
    )
    .expect("parameterized dynamic node label pattern should compile");

    assert_eq!(
        plan.nodes,
        vec![NodePattern {
            variable: "service".to_string(),
            label: "Service".to_string(),
        }]
    );
}

#[test]
fn compiles_parameterized_dynamic_relationship_type_patterns() {
    let parameters = BTreeMap::from([(
        "type".to_string(),
        CypherParameterValue::Literal(Literal::String("OWNS".to_string())),
    )]);
    let plan = compile_cypher_with_parameters(
        "MATCH (person:Person)-[owns:$($type)]->(service:Service) \
             RETURN service.name AS service",
        &parameters,
    )
    .expect("parameterized dynamic relationship type pattern should compile");

    assert_eq!(
        plan.relationships,
        vec![RelationshipPattern {
            variable: Some("owns".to_string()),
            relationship_type: "OWNS".to_string(),
            left: "person".to_string(),
            direction: Direction::Outgoing,
            right: "service".to_string(),
        }]
    );
}

#[test]
fn compiles_parameterized_dynamic_node_label_predicates() {
    let parameters = BTreeMap::from([
        (
            "label".to_string(),
            CypherParameterValue::Literal(Literal::String("Service".to_string())),
        ),
        (
            "other".to_string(),
            CypherParameterValue::Literal(Literal::String("Team".to_string())),
        ),
    ]);
    let plan = compile_cypher_with_parameters(
        "MATCH (service:Service) \
             WHERE service:$($label) AND NOT service:$($other) \
             RETURN service.name AS service",
        &parameters,
    )
    .expect("parameterized dynamic node label predicate should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::Boolean(true)),
            right: Box::new(PredicateExpression::Not {
                expression: Box::new(PredicateExpression::Boolean(false)),
            }),
        })
    );
}

#[test]
fn compiles_parameterized_dynamic_relationship_type_predicates() {
    let parameters = BTreeMap::from([(
        "type".to_string(),
        CypherParameterValue::Literal(Literal::String("OWNS".to_string())),
    )]);
    let plan = compile_cypher_with_parameters(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE owns:$($type) \
             RETURN service.name AS service",
        &parameters,
    )
    .expect("parameterized dynamic relationship type predicate should compile");

    assert_eq!(plan.predicate, Some(PredicateExpression::Boolean(true)));
}

#[test]
fn compiles_parameterized_dynamic_label_predicate_list_parameters() {
    let parameters = BTreeMap::from([
        (
            "labels".to_string(),
            CypherParameterValue::List(vec![
                Literal::String("Team".to_string()),
                Literal::String("Service".to_string()),
            ]),
        ),
        (
            "excluded".to_string(),
            CypherParameterValue::List(vec![Literal::String("Team".to_string())]),
        ),
    ]);
    let plan = compile_cypher_with_parameters(
        "MATCH (service:Service) \
             WHERE service:$($labels) AND NOT service:$($excluded) \
             RETURN service.name AS service",
        &parameters,
    )
    .expect("dynamic label predicate list parameters should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::Boolean(true)),
            right: Box::new(PredicateExpression::Not {
                expression: Box::new(PredicateExpression::Boolean(false)),
            }),
        })
    );
}

#[test]
fn compiles_parameterized_dynamic_label_pattern_list_parameters() {
    let parameters = BTreeMap::from([(
        "labels".to_string(),
        CypherParameterValue::List(vec![
            Literal::String("Team".to_string()),
            Literal::String("Service".to_string()),
        ]),
    )]);
    let query = compile_cypher_query_with_parameters(
        "MATCH (entity:$($labels)) \
             RETURN entity.name AS name",
        &parameters,
    )
    .expect("dynamic label pattern list parameters should compile");

    let GraphQuery::Union(union) = query else {
        panic!("dynamic label list parameters should expand into a union query");
    };
    assert_eq!(
        union.first.nodes.first().map(|node| node.label.as_str()),
        Some("Team")
    );
    assert_eq!(union.branches.len(), 1);
    assert_eq!(
        union
            .branches
            .first()
            .and_then(|branch| branch.plan.nodes.first())
            .map(|node| node.label.as_str()),
        Some("Service")
    );
}

#[test]
fn compiles_literal_dynamic_label_pattern_lists() {
    let query = compile_cypher_query(
        "MATCH (entity:$(['Team', 'Service'])) \
             RETURN entity.name AS name",
    )
    .expect("literal dynamic label pattern lists should compile");

    let GraphQuery::Union(union) = query else {
        panic!("dynamic label literal lists should expand into a union query");
    };
    assert_eq!(
        union.first.nodes.first().map(|node| node.label.as_str()),
        Some("Team")
    );
    assert_eq!(union.branches.len(), 1);
    assert_eq!(
        union
            .branches
            .first()
            .and_then(|branch| branch.plan.nodes.first())
            .map(|node| node.label.as_str()),
        Some("Service")
    );
}

#[test]
fn compiles_literal_dynamic_label_predicate_lists() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE service:$(['Team', 'Service']) AND NOT service:$(['Team']) \
             RETURN service.name AS service",
    )
    .expect("dynamic label predicate literal lists should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::Boolean(true)),
            right: Box::new(PredicateExpression::Not {
                expression: Box::new(PredicateExpression::Boolean(false)),
            }),
        })
    );
}

#[test]
fn compiles_literal_dynamic_relationship_type_lists() {
    let query = compile_cypher_query(
        "MATCH (source:Service)-[:DEPENDS_ON|$(['OWNS', 'DEPENDS_ON'])]->(target:Service) \
             RETURN target.name AS target",
    )
    .expect("dynamic relationship type literal lists should compile and deduplicate");

    let GraphQuery::Union(union) = query else {
        panic!("dynamic relationship type literal lists should expand into a union query");
    };
    assert_eq!(
        union
            .first
            .relationships
            .first()
            .map(|relationship| relationship.relationship_type.as_str()),
        Some("DEPENDS_ON")
    );
    assert_eq!(union.branches.len(), 1);
    assert_eq!(
        union
            .branches
            .first()
            .and_then(|branch| branch.plan.relationships.first())
            .map(|relationship| relationship.relationship_type.as_str()),
        Some("OWNS")
    );
}

#[test]
fn compiles_folded_dynamic_label_pattern_lists() {
    let query = compile_cypher_query(
        "MATCH (entity:$(split('Team,Service', ','))) \
             RETURN entity.name AS name",
    )
    .expect("folded dynamic label pattern lists should compile");

    let GraphQuery::Union(union) = query else {
        panic!("folded dynamic label lists should expand into a union query");
    };
    assert_eq!(
        union.first.nodes.first().map(|node| node.label.as_str()),
        Some("Team")
    );
    assert_eq!(union.branches.len(), 1);
    assert_eq!(
        union
            .branches
            .first()
            .and_then(|branch| branch.plan.nodes.first())
            .map(|node| node.label.as_str()),
        Some("Service")
    );
}

#[test]
fn compiles_folded_dynamic_label_predicate_lists() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE service:$(tail(['Team'] + split('Service', ','))) \
             RETURN service.name AS service",
    )
    .expect("folded dynamic label predicate lists should compile");

    assert_eq!(plan.predicate, Some(PredicateExpression::Boolean(true)));
}

#[test]
fn compiles_folded_dynamic_relationship_type_lists() {
    let query = compile_cypher_query(
            "MATCH (source:Service)-[:DEPENDS_ON|$(tail(['IGNORE'] + split('OWNS,DEPENDS_ON', ',')))]->(target:Service) \
             RETURN target.name AS target",
        )
        .expect("folded dynamic relationship type lists should compile and deduplicate");

    let GraphQuery::Union(union) = query else {
        panic!("folded dynamic relationship type lists should expand into a union query");
    };
    assert_eq!(
        union
            .first
            .relationships
            .first()
            .map(|relationship| relationship.relationship_type.as_str()),
        Some("DEPENDS_ON")
    );
    assert_eq!(union.branches.len(), 1);
    assert_eq!(
        union
            .branches
            .first()
            .and_then(|branch| branch.plan.relationships.first())
            .map(|relationship| relationship.relationship_type.as_str()),
        Some("OWNS")
    );
}

#[test]
fn compiles_static_case_dynamic_label_pattern_lists() {
    let query = compile_cypher_query(
        "MATCH (entity:$(CASE WHEN true THEN split('Team,Service', ',') ELSE ['Person'] END)) \
             RETURN entity.name AS name",
    )
    .expect("static CASE dynamic label pattern lists should compile");

    let GraphQuery::Union(union) = query else {
        panic!("static CASE dynamic label lists should expand into a union query");
    };
    assert_eq!(
        union.first.nodes.first().map(|node| node.label.as_str()),
        Some("Team")
    );
    assert_eq!(union.branches.len(), 1);
    assert_eq!(
        union
            .branches
            .first()
            .and_then(|branch| branch.plan.nodes.first())
            .map(|node| node.label.as_str()),
        Some("Service")
    );
}

#[test]
fn compiles_static_case_dynamic_label_predicate_lists() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE service:$(CASE WHEN false THEN ['Team'] ELSE ['Service'] END) \
             RETURN service.name AS service",
    )
    .expect("static CASE dynamic label predicate lists should compile");

    assert_eq!(plan.predicate, Some(PredicateExpression::Boolean(true)));
}

#[test]
fn compiles_static_case_dynamic_relationship_type_lists() {
    let query = compile_cypher_query(
            "MATCH (source:Service)-[:DEPENDS_ON|$(CASE WHEN true THEN split('OWNS,DEPENDS_ON', ',') ELSE ['ALERTS'] END)]->(target:Service) \
             RETURN target.name AS target",
        )
        .expect("static CASE dynamic relationship type lists should compile and deduplicate");

    let GraphQuery::Union(union) = query else {
        panic!("static CASE dynamic relationship type lists should expand into a union query");
    };
    assert_eq!(
        union
            .first
            .relationships
            .first()
            .map(|relationship| relationship.relationship_type.as_str()),
        Some("DEPENDS_ON")
    );
    assert_eq!(union.branches.len(), 1);
    assert_eq!(
        union
            .branches
            .first()
            .and_then(|branch| branch.plan.relationships.first())
            .map(|relationship| relationship.relationship_type.as_str()),
        Some("OWNS")
    );
}

#[test]
fn compiles_parameterized_dynamic_node_label_pattern_alternatives() {
    let parameters = BTreeMap::from([(
        "label".to_string(),
        CypherParameterValue::Literal(Literal::String("Service".to_string())),
    )]);
    let query = compile_cypher_query_with_parameters(
        "MATCH (service:Team|$($label)) \
             RETURN service.name AS service",
        &parameters,
    )
    .expect("parameterized dynamic label alternatives should compile");

    let GraphQuery::Union(union) = query else {
        panic!("dynamic label alternatives should expand into a union query");
    };
    assert_eq!(
        union.first.nodes.first().map(|node| node.label.as_str()),
        Some("Team")
    );
    assert_eq!(union.branches.len(), 1);
    assert_eq!(
        union
            .branches
            .first()
            .and_then(|branch| branch.plan.nodes.first())
            .map(|node| node.label.as_str()),
        Some("Service")
    );
    assert!(union.branches.iter().all(|branch| branch.all));
}

#[test]
fn compiles_parameterized_dynamic_relationship_type_alternatives() {
    let parameters = BTreeMap::from([(
        "type".to_string(),
        CypherParameterValue::Literal(Literal::String("OWNS".to_string())),
    )]);
    let query = compile_cypher_query_with_parameters(
        "MATCH (source:Service)-[:DEPENDS_ON|$($type)]->(target:Service) \
             RETURN target.name AS target",
        &parameters,
    )
    .expect("parameterized dynamic relationship type alternatives should compile");

    let GraphQuery::Union(union) = query else {
        panic!("dynamic relationship alternatives should expand into a union query");
    };
    assert_eq!(
        union
            .first
            .relationships
            .first()
            .map(|relationship| relationship.relationship_type.as_str()),
        Some("DEPENDS_ON")
    );
    assert_eq!(union.branches.len(), 1);
    assert_eq!(
        union
            .branches
            .first()
            .and_then(|branch| branch.plan.relationships.first())
            .map(|relationship| relationship.relationship_type.as_str()),
        Some("OWNS")
    );
}

#[test]
fn compiles_parameterized_dynamic_label_alternative_list_parameters() {
    let parameters = BTreeMap::from([(
        "labels".to_string(),
        CypherParameterValue::List(vec![
            Literal::String("Service".to_string()),
            Literal::String("Team".to_string()),
        ]),
    )]);
    let query = compile_cypher_query_with_parameters(
        "MATCH (service:Team|$($labels)) \
             RETURN service.name AS service",
        &parameters,
    )
    .expect("dynamic label alternative list parameters should compile and deduplicate");

    let GraphQuery::Union(union) = query else {
        panic!("dynamic label alternatives should expand into a union query");
    };
    assert_eq!(
        union.first.nodes.first().map(|node| node.label.as_str()),
        Some("Team")
    );
    assert_eq!(union.branches.len(), 1);
    assert_eq!(
        union
            .branches
            .first()
            .and_then(|branch| branch.plan.nodes.first())
            .map(|node| node.label.as_str()),
        Some("Service")
    );
}

#[test]
fn compiles_parameterized_dynamic_relationship_type_alternative_list_parameters() {
    let parameters = BTreeMap::from([(
        "types".to_string(),
        CypherParameterValue::List(vec![
            Literal::String("OWNS".to_string()),
            Literal::String("DEPENDS_ON".to_string()),
        ]),
    )]);
    let query = compile_cypher_query_with_parameters(
        "MATCH (source:Service)-[:DEPENDS_ON|$($types)]->(target:Service) \
             RETURN target.name AS target",
        &parameters,
    )
    .expect("dynamic relationship type list parameters should compile and deduplicate");

    let GraphQuery::Union(union) = query else {
        panic!("dynamic relationship type alternatives should expand into a union query");
    };
    assert_eq!(
        union
            .first
            .relationships
            .first()
            .map(|relationship| relationship.relationship_type.as_str()),
        Some("DEPENDS_ON")
    );
    assert_eq!(union.branches.len(), 1);
    assert_eq!(
        union
            .branches
            .first()
            .and_then(|branch| branch.plan.relationships.first())
            .map(|relationship| relationship.relationship_type.as_str()),
        Some("OWNS")
    );
}

#[test]
fn rejects_dynamic_label_list_parameters_without_string_values() {
    let parameters = BTreeMap::from([(
        "labels".to_string(),
        CypherParameterValue::List(vec![
            Literal::String("Service".to_string()),
            Literal::Integer(1),
        ]),
    )]);
    let error = compile_cypher_query_with_parameters(
        "MATCH (service:$($labels)) \
             RETURN service.name AS service",
        &parameters,
    )
    .expect_err("dynamic label list parameters with non-string values should be rejected");

    assert!(
        error
            .to_string()
            .contains("dynamic label list parameters require only strings"),
        "{error:?}"
    );
}

#[test]
fn rejects_literal_dynamic_label_lists_without_string_values() {
    let error = compile_cypher_query(
        "MATCH (service:$(['Service', 1])) \
             RETURN service.name AS service",
    )
    .expect_err("dynamic label literal lists with non-string values should be rejected");

    assert!(
        error
            .to_string()
            .contains("dynamic label literal lists require only strings"),
        "{error:?}"
    );
}

#[test]
fn rejects_empty_literal_dynamic_label_lists() {
    let error = compile_cypher_query(
        "MATCH (service:$([])) \
             RETURN service.name AS service",
    )
    .expect_err("empty dynamic label literal lists should be rejected");

    assert!(
        error
            .to_string()
            .contains("dynamic label literal lists require at least one string"),
        "{error:?}"
    );
}

#[test]
fn rejects_folded_dynamic_label_lists_without_string_values() {
    let error = compile_cypher_query(
        "MATCH (service:$(range(1, 2))) \
             RETURN service.name AS service",
    )
    .expect_err("dynamic label folded lists with non-string values should be rejected");

    assert!(
        error
            .to_string()
            .contains("dynamic label list expressions require only strings"),
        "{error:?}"
    );
}

#[test]
fn rejects_row_dependent_static_case_dynamic_label_lists() {
    let error = compile_cypher_query(
            "MATCH (service:Service) \
             WHERE service:$(CASE WHEN service.name = 'billing' THEN ['Service'] ELSE ['Team'] END) \
             RETURN service.name AS service",
        )
        .expect_err("dynamic label CASE predicates with row dependencies should be rejected");

    assert!(
        error
            .to_string()
            .contains("dynamic label CASE expressions require statically foldable WHEN predicates"),
        "{error:?}"
    );
}

#[test]
fn rejects_empty_dynamic_label_list_parameters() {
    let parameters =
        BTreeMap::from([("labels".to_string(), CypherParameterValue::List(Vec::new()))]);
    let error = compile_cypher_query_with_parameters(
        "MATCH (service:$($labels)) \
             RETURN service.name AS service",
        &parameters,
    )
    .expect_err("empty dynamic label list parameters should be rejected");

    assert!(
        error.to_string().contains("require at least one string"),
        "{error:?}"
    );
}

#[test]
fn compiles_reverse_relationship_direction() {
    let plan = compile_cypher(
        "MATCH (service:Service)<-[ownership:OWNS]-(person:Person) \
             RETURN ownership.source AS source",
    )
    .expect("query should compile");

    assert_eq!(
        plan.relationships,
        vec![RelationshipPattern {
            variable: Some("ownership".to_string()),
            relationship_type: "OWNS".to_string(),
            left: "service".to_string(),
            direction: Direction::Incoming,
            right: "person".to_string(),
        }]
    );
    assert_eq!(
        plan.projections,
        vec![Projection::Property {
            property: PropertyRef {
                variable: "ownership".to_string(),
                property: "source".to_string(),
            },
            alias: Some("source".to_string()),
        }]
    );
}

#[test]
fn compiles_connected_comma_separated_patterns_with_reused_nodes() {
    let plan = compile_cypher(
        "MATCH (source:Service)-[:DEPENDS_ON]->(middle:Service), \
                   (middle)-[:DEPENDS_ON]->(target:Service), \
                   (source)-[:DEPENDS_ON]->(target) \
             RETURN source.name AS source, middle.name AS middle, target.name AS target",
    )
    .expect("query should compile");

    assert_eq!(
        plan.nodes,
        vec![
            NodePattern {
                variable: "source".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "middle".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "target".to_string(),
                label: "Service".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.relationships,
        vec![
            RelationshipPattern {
                variable: None,
                relationship_type: "DEPENDS_ON".to_string(),
                left: "source".to_string(),
                direction: Direction::Outgoing,
                right: "middle".to_string(),
            },
            RelationshipPattern {
                variable: None,
                relationship_type: "DEPENDS_ON".to_string(),
                left: "middle".to_string(),
                direction: Direction::Outgoing,
                right: "target".to_string(),
            },
            RelationshipPattern {
                variable: None,
                relationship_type: "DEPENDS_ON".to_string(),
                left: "source".to_string(),
                direction: Direction::Outgoing,
                right: "target".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_repeated_node_property_maps_as_additional_predicates() {
    let plan = compile_cypher(
        "MATCH (service:Service {tier: 'prod'}), (service {team: 'platform'}) \
             RETURN service.name",
    )
    .expect("query should compile");

    assert_eq!(
        plan.nodes,
        vec![NodePattern {
            variable: "service".to_string(),
            label: "Service".to_string(),
        }]
    );
    assert_eq!(
        plan.predicates,
        vec![
            PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
            },
            PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "team".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("platform".to_string())),
            },
        ]
    );
}

#[test]
fn compiles_property_to_property_predicates() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[:OWNS]->(service:Service) \
             WHERE person.team = service.team \
             RETURN service.name",
    )
    .expect("query should compile");

    assert_eq!(
        plan.predicates,
        vec![PropertyPredicate {
            property: PropertyRef {
                variable: "person".to_string(),
                property: "team".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Property(PropertyRef {
                variable: "service".to_string(),
                property: "team".to_string(),
            }),
        }]
    );
}

#[test]
fn compiles_literal_left_comparisons_by_inverting_operator() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE 'prod' = service.tier AND 10 < service.id \
             RETURN service.name",
    )
    .expect("query should compile");

    assert_eq!(
        plan.predicates,
        vec![
            PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
            },
            PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "id".to_string(),
                },
                operator: ComparisonOperator::GreaterThan,
                rhs: PredicateRhs::Literal(Literal::Integer(10)),
            },
        ]
    );
}

#[test]
fn compiles_literal_projections() {
    let parameters = BTreeMap::from([(
        "kind".to_string(),
        CypherParameterValue::Literal(Literal::String("service".to_string())),
    )]);
    let plan = compile_cypher_with_parameters(
        "MATCH (service:Service) \
             RETURN $kind AS kind, 1 AS version, true AS enabled, null AS missing, -1.5 AS score \
             ORDER BY 'constant'",
        &parameters,
    )
    .expect("literal projections should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Literal {
                literal: Literal::String("service".to_string()),
                alias: "kind".to_string(),
            },
            Projection::Literal {
                literal: Literal::Integer(1),
                alias: "version".to_string(),
            },
            Projection::Literal {
                literal: Literal::Boolean(true),
                alias: "enabled".to_string(),
            },
            Projection::Literal {
                literal: Literal::Null,
                alias: "missing".to_string(),
            },
            Projection::Literal {
                literal: Literal::Float(OrderedFloat(-1.5)),
                alias: "score".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Literal(Literal::String("constant".to_string())),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_static_literal_map_value_lookups() {
    let parameters = BTreeMap::from([(
        "kind".to_string(),
        CypherParameterValue::Literal(Literal::String("service".to_string())),
    )]);
    let plan = compile_cypher_with_parameters(
        "MATCH (service:Service) \
             WHERE ({tier: 'prod'}).tier = service.tier \
             RETURN ({kind: $kind}).kind AS kind, \
                    {rank: 1}['rank'] AS rank, \
                    {known: true}.missing AS missing \
             ORDER BY {sort: 'constant'}['sort']",
        &parameters,
    )
    .expect("static literal map value lookups should fold to scalar literals");

    assert_eq!(
        plan.predicates,
        vec![PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
        }]
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Literal {
                literal: Literal::String("service".to_string()),
                alias: "kind".to_string(),
            },
            Projection::Literal {
                literal: Literal::Integer(1),
                alias: "rank".to_string(),
            },
            Projection::Literal {
                literal: Literal::Null,
                alias: "missing".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Literal(Literal::String("constant".to_string())),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_static_map_value_lookups_over_graph_scalars() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE {tier: service.tier}['tier'] = 'prod' \
             RETURN {name: service.name}['name'] AS name, \
                    ({tier_upper: toUpper(service.tier)}).tier_upper AS tier_upper \
             ORDER BY ({sort: service.name}).sort",
    )
    .expect("static map value lookups should compile selected graph scalar expressions");

    assert_eq!(
        plan.predicates,
        vec![PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
        }]
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("name".to_string()),
            },
            Projection::Expression {
                expression: ScalarExpression::ToUpper {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    })),
                },
                alias: "tier_upper".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_literal_list_projections() {
    let parameters = BTreeMap::from([(
        "selected_tiers".to_string(),
        CypherParameterValue::List(vec![Literal::String("prod".to_string()), Literal::Null]),
    )]);
    let plan = compile_cypher_with_parameters(
        "MATCH (service:Service) \
             RETURN ['prod', 'dev'] AS tiers, $selected_tiers AS selected_tiers",
        &parameters,
    )
    .expect("literal list projections should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::LiteralList {
                literals: vec![
                    Literal::String("prod".to_string()),
                    Literal::String("dev".to_string()),
                ],
                alias: "tiers".to_string(),
            },
            Projection::LiteralList {
                literals: vec![Literal::String("prod".to_string()), Literal::Null,],
                alias: "selected_tiers".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_static_range_list_projections() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN range(1, 3) AS forward, range(3, 1, -1) AS backward, range(3, 1) AS empty",
    )
    .expect("static range list projections should compile");

    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: forward,
                    element_type: LiteralListElementType::Integer,
                },
                alias: forward_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: backward,
                    element_type: LiteralListElementType::Integer,
                },
                alias: backward_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: empty,
                    element_type: LiteralListElementType::Integer,
                },
                alias: empty_alias,
            },
        ] if forward_alias == "forward"
            && forward == &vec![Literal::Integer(1), Literal::Integer(2), Literal::Integer(3)]
            && backward_alias == "backward"
            && backward == &vec![Literal::Integer(3), Literal::Integer(2), Literal::Integer(1)]
            && empty_alias == "empty"
            && empty.is_empty()
    ));
}

#[test]
fn compiles_static_range_indexes_slices_and_comprehensions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE 2 IN range(1, 3) \
             RETURN range(1, 5, 2)[1] AS middle, \
                    range(1, 5, 2)[1..] AS tail, \
                    [x IN range(1, 3) | x * 10] AS scaled",
    )
    .expect("static range list expressions should compose with folded list operations");

    assert_eq!(plan.predicate, Some(PredicateExpression::Boolean(true)));
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Integer(3)),
                alias,
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: tail,
                    element_type: LiteralListElementType::Integer,
                },
                alias: tail_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: scaled,
                    element_type: LiteralListElementType::Integer,
                },
                alias: scaled_alias,
            },
        ] if alias == "middle"
            && tail_alias == "tail"
            && tail == &vec![Literal::Integer(3), Literal::Integer(5)]
            && scaled_alias == "scaled"
            && scaled == &vec![Literal::Integer(10), Literal::Integer(20), Literal::Integer(30)]
    ));
}

#[test]
fn compiles_static_list_expressions_as_direct_order_keys() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN service.name AS name \
             ORDER BY range(1, 2), split('prod,dev', ','), toStringList([1, 2]) DESC",
    )
    .expect("folded static list expressions should compile as direct order keys");

    assert!(matches!(
        plan.order_by.as_slice(),
        [
            OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::TypedLiteralList {
                    literals: range,
                    element_type: LiteralListElementType::Integer,
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::TypedLiteralList {
                    literals: split,
                    element_type: LiteralListElementType::String,
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::TypedLiteralList {
                    literals: cast,
                    element_type: LiteralListElementType::String,
                }),
                direction: OrderDirection::Descending,
                nulls: None,
            },
        ] if range == &vec![Literal::Integer(1), Literal::Integer(2)]
            && split == &vec![
                Literal::String("prod".to_string()),
                Literal::String("dev".to_string()),
            ]
            && cast == &vec![
                Literal::String("1".to_string()),
                Literal::String("2".to_string()),
            ]
    ));
}

#[test]
fn rejects_static_range_with_zero_step() {
    let error = compile_cypher(
        "UNWIND range(1, 3, 0) AS ordinal \
             MATCH (service:Service) \
             RETURN ordinal AS ordinal",
    )
    .expect_err("zero-step static range should be rejected");

    assert!(
        error.to_string().contains("step must not be zero"),
        "{error}"
    );
}

#[test]
fn compiles_static_split_list_projections() {
    let parameters = BTreeMap::from([(
        "tiers".to_string(),
        CypherParameterValue::Literal(Literal::String("prod|dev".to_string())),
    )]);
    let plan = compile_cypher_with_parameters(
        "MATCH (service:Service) \
             RETURN split('prod,dev', ',') AS literal_tiers, \
                    split($tiers, '|') AS parameter_tiers",
        &parameters,
    )
    .expect("static split list projections should compile");

    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: literal_tiers,
                    element_type: LiteralListElementType::String,
                },
                alias: literal_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: parameter_tiers,
                    element_type: LiteralListElementType::String,
                },
                alias: parameter_alias,
            },
        ] if literal_alias == "literal_tiers"
            && literal_tiers == &vec![
                Literal::String("prod".to_string()),
                Literal::String("dev".to_string()),
            ]
            && parameter_alias == "parameter_tiers"
            && parameter_tiers == &vec![
                Literal::String("prod".to_string()),
                Literal::String("dev".to_string()),
            ]
    ));
}

#[test]
fn compiles_static_split_indexes_slices_and_comprehensions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE 'prod' IN split('dev,prod', ',') \
             RETURN split('prod:dev:stage', ':')[1] AS middle, \
                    split('prod:dev:stage', ':')[1..] AS tail, \
                    [tier IN split('prod,dev', ',') | toUpper(tier)] AS upper_tiers",
    )
    .expect("static split list expressions should compose with folded list operations");

    assert_eq!(plan.predicate, Some(PredicateExpression::Boolean(true)));
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::String(middle)),
                alias: middle_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: tail,
                    element_type: LiteralListElementType::String,
                },
                alias: tail_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: upper_tiers,
                    element_type: LiteralListElementType::String,
                },
                alias: upper_alias,
            },
        ] if middle_alias == "middle"
            && middle == "dev"
            && tail_alias == "tail"
            && tail == &vec![
                Literal::String("dev".to_string()),
                Literal::String("stage".to_string()),
            ]
            && upper_alias == "upper_tiers"
            && upper_tiers == &vec![
                Literal::String("PROD".to_string()),
                Literal::String("DEV".to_string()),
            ]
    ));
}

#[test]
fn compiles_static_reduce_scalar_expressions() {
    let parameters = BTreeMap::from([
        (
            "seed".to_string(),
            CypherParameterValue::Literal(Literal::Integer(1)),
        ),
        (
            "weights".to_string(),
            CypherParameterValue::List(vec![Literal::Integer(2), Literal::Integer(4)]),
        ),
    ]);
    let plan = compile_cypher_with_parameters(
            "MATCH (service:Service) \
             WHERE reduce(total = 0, x IN range(1, 3) | total + x) = 6 \
             RETURN reduce(total = $seed, x IN $weights | total + x) AS weighted, \
                    reduce(found = false, key IN ['name', 'tier'] | found OR key = 'tier') AS has_tier \
             ORDER BY reduce(total = 0, x IN [3, 1] | total + x)",
            &parameters,
        )
        .expect("static reduce scalar expressions should compile");

    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Integer(7)),
                alias: weighted_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Boolean(true)),
                alias: has_tier_alias,
            },
        ] if weighted_alias == "weighted" && has_tier_alias == "has_tier"
    ));
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Literal(Literal::Integer(4))),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn rejects_static_reduce_unsupported_shapes() {
    let dynamic_collection = compile_cypher(
        "MATCH (service:Service) \
             RETURN reduce(total = 0, x IN service.name | total + x) AS total",
    )
    .expect_err("dynamic reduce collection should be rejected");
    assert!(
        dynamic_collection
            .to_string()
            .contains("reduce() requires a literal list"),
        "{dynamic_collection}"
    );

    let dynamic_initial = compile_cypher(
        "MATCH (service:Service) \
             RETURN reduce(total = service.risk, x IN [1, 2] | total + x) AS total",
    )
    .expect_err("dynamic reduce initial accumulator should be rejected");
    assert!(
        dynamic_initial.to_string().contains("initial accumulator"),
        "{dynamic_initial}"
    );

    let reused_variable = compile_cypher(
        "MATCH (service:Service) \
             RETURN reduce(total = 0, total IN [1, 2] | total + total) AS total",
    )
    .expect_err("reduce should reject reused accumulator and item variables");
    assert!(
        reused_variable.to_string().contains("must be distinct"),
        "{reused_variable}"
    );
}

#[test]
fn rejects_static_split_with_empty_or_dynamic_arguments() {
    let empty_delimiter = compile_cypher(
        "MATCH (service:Service) \
             RETURN split('prod,dev', '') AS tiers",
    )
    .expect_err("empty split delimiter should be rejected");
    assert!(
        empty_delimiter.to_string().contains("non-empty delimiter"),
        "{empty_delimiter}"
    );

    let dynamic_source = compile_cypher(
        "MATCH (service:Service) \
             RETURN split(service.name, '-') AS name_parts",
    )
    .expect_err("dynamic split source should be rejected");
    assert!(
        dynamic_source
            .to_string()
            .contains("string literals or scalar string parameters"),
        "{dynamic_source}"
    );
}

#[test]
fn compiles_coalesce_projection() {
    let parameters = BTreeMap::from([(
        "fallback".to_string(),
        CypherParameterValue::Literal(Literal::String("unassigned".to_string())),
    )]);
    let plan = compile_cypher_with_parameters(
        "MATCH (service:Service) \
             RETURN coalesce(service.team, service.tier, $fallback) AS owner_team",
        &parameters,
    )
    .expect("coalesce projection should compile");

    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::Coalesce {
                expressions: vec![
                    ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "team".to_string(),
                    }),
                    ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    }),
                    ScalarExpression::Literal(Literal::String("unassigned".to_string())),
                ],
            },
            alias: "owner_team".to_string(),
        }]
    );
}

#[test]
fn compiles_static_list_coalesce_projection_and_ordering() {
    let query = compile_cypher_query_for_graph(
        &star_test_graph(),
        "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN coalesce(keys(person), []) AS owner_keys, \
                    coalesce(null, labels(service)) AS service_labels \
             ORDER BY coalesce(keys(person), ['missing'])",
    )
    .expect("static list coalesce should compile with graph metadata");

    let GraphQuery::Plan(plan) = query else {
        panic!("expected single graph plan");
    };
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Coalesce { expressions: owner_key_args },
                alias: owner_key_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Coalesce { expressions: service_label_args },
                alias: service_label_alias,
            },
        ] if owner_key_alias == "owner_keys"
            && owner_key_args.len() == 2
            && matches!(
                owner_key_args.as_slice(),
                [
                    ScalarExpression::PresenceGated {
                        presence_variable,
                        expression,
                    },
                    ScalarExpression::TypedLiteralList {
                        literals,
                        element_type: LiteralListElementType::String,
                    },
                ] if presence_variable == "person"
                    && matches!(
                        expression.as_ref(),
                        ScalarExpression::TypedLiteralList {
                            literals,
                            element_type: LiteralListElementType::String,
                        } if literals == &vec![
                            Literal::String("name".to_string()),
                            Literal::String("team".to_string()),
                        ]
                    )
                    && literals.is_empty()
            )
            && service_label_alias == "service_labels"
            && matches!(
                service_label_args.as_slice(),
                [
                    ScalarExpression::Literal(Literal::Null),
                    ScalarExpression::TypedLiteralList {
                        literals,
                        element_type: LiteralListElementType::String,
                    },
                ] if literals == &vec![Literal::String("Service".to_string())]
            )
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Coalesce { expressions }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }] if expressions.len() == 2
    ));
}

#[test]
fn compiles_static_list_coalesce_size_and_is_empty() {
    let query = compile_cypher_query_for_graph(
        &star_test_graph(),
        "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN size(coalesce(keys(person), [])) AS owner_key_count, \
                    isEmpty(coalesce(keys(person), [])) AS owner_keys_empty, \
                    size(coalesce([], [])) AS empty_count, \
                    isEmpty(coalesce([], [])) AS empty_is_empty \
             ORDER BY size(coalesce(keys(person), []))",
    )
    .expect("static list coalesce size/isEmpty should compile with graph metadata");

    let GraphQuery::Plan(plan) = query else {
        panic!("expected single graph plan");
    };
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Coalesce { expressions: size_args },
                alias: size_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Predicate(predicate),
                alias: empty_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Coalesce { expressions: empty_size_args },
                alias: empty_size_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Predicate(empty_predicate),
                alias: empty_predicate_alias,
            },
        ] if size_alias == "owner_key_count"
            && size_args.len() == 2
            && matches!(
                predicate.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::Coalesce { expressions },
                    operator: ComparisonOperator::Equal,
                    rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                        Literal::Boolean(true)
                    )),
                }) if expressions.len() == 2
            )
            && empty_alias == "owner_keys_empty"
            && empty_size_alias == "empty_count"
            && matches!(
                empty_size_args.as_slice(),
                [
                    ScalarExpression::Literal(Literal::Integer(0)),
                    ScalarExpression::Literal(Literal::Integer(0)),
                ]
            )
            && empty_predicate_alias == "empty_is_empty"
            && matches!(
                empty_predicate.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::Coalesce { expressions },
                    operator: ComparisonOperator::Equal,
                    rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                        Literal::Boolean(true)
                    )),
                }) if matches!(
                    expressions.as_slice(),
                    [
                        ScalarExpression::Literal(Literal::Boolean(true)),
                        ScalarExpression::Literal(Literal::Boolean(true)),
                    ]
                )
            )
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Coalesce { expressions }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }] if expressions.len() == 2
    ));
}

#[test]
fn compiles_null_if_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE nullIf(service.tier, 'dev') IS NULL \
             RETURN nullIf(service.tier, 'prod') AS normalized_tier \
             ORDER BY nullIf(service.team, service.tier)",
    )
    .expect("nullIf scalar expressions should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::NullIf {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                })),
                value: Box::new(ScalarExpression::Literal(Literal::String(
                    "dev".to_string()
                ))),
            },
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Null)),
        }))
    );
    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::NullIf {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                })),
                value: Box::new(ScalarExpression::Literal(Literal::String(
                    "prod".to_string()
                ))),
            },
            alias: "normalized_tier".to_string(),
        }]
    );
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::NullIf { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn rejects_null_if_with_unsupported_arity() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             RETURN nullIf(service.tier) AS normalized_tier",
    )
    .expect_err("nullIf() requires exactly two arguments");

    assert!(
        error
            .to_string()
            .contains("nullIf() requires exactly two arguments"),
        "{error}"
    );
}

#[test]
fn compiles_order_by_coalesce_expression() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN service.name AS service \
             ORDER BY coalesce(service.tier, 'unassigned') DESC",
    )
    .expect("coalesce order expression should compile");

    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Coalesce {
                expressions: vec![
                    ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    }),
                    ScalarExpression::Literal(Literal::String("unassigned".to_string())),
                ],
            }),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_to_string_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE toString(service.risk) STARTS WITH '0.9' \
             RETURN toString(service.risk) AS risk_text \
             ORDER BY toString(service.risk)",
    )
    .expect("toString scalar expressions should compile");

    let expected_expression = ScalarExpression::ToString {
        expression: Box::new(ScalarExpression::Property(PropertyRef {
            variable: "service".to_string(),
            property: "risk".to_string(),
        })),
    };
    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: expected_expression.clone(),
            operator: ComparisonOperator::StartsWith,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
                "0.9".to_string()
            ))),
        }))
    );
    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: expected_expression.clone(),
            alias: "risk_text".to_string(),
        }]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(expected_expression),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_string_case_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE toLower(service.name) CONTAINS 'api' \
             RETURN toUpper(service.tier) AS tier_upper \
             ORDER BY toLower(service.name)",
    )
    .expect("string case scalar expressions should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::ToLower {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                })),
            },
            operator: ComparisonOperator::Contains,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
                "api".to_string()
            ))),
        }))
    );
    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::ToUpper {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                })),
            },
            alias: "tier_upper".to_string(),
        }]
    );
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::ToLower { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_trim_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE trim(service.tier) = 'prod' \
             RETURN lTrim(service.name) AS left_trimmed \
             ORDER BY rTrim(service.name)",
    )
    .expect("trim scalar expressions should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Trim {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                })),
            },
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
                "prod".to_string()
            ))),
        }))
    );
    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::LTrim {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                })),
            },
            alias: "left_trimmed".to_string(),
        }]
    );
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::RTrim { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_gql_string_function_aliases() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE lower(service.name) CONTAINS 'api' \
             RETURN upper(service.tier) AS tier_upper, \
                    btrim(service.name) AS name_trimmed \
             ORDER BY btrim(service.name)",
    )
    .expect("GQL string function aliases should compile");

    assert!(matches!(
        &plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::ToLower { .. },
            operator: ComparisonOperator::Contains,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(value))),
        })) if value == "api"
    ));
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::ToUpper { .. },
                alias: tier_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Trim { .. },
                alias: trim_alias,
            },
        ] if tier_alias == "tier_upper" && trim_alias == "name_trimmed"
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Trim { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn rejects_gql_string_aliases_with_unsupported_arity() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             RETURN btrim(service.name, '-') AS name_trimmed",
    )
    .expect_err("btrim() should require one argument");

    assert!(
        error
            .to_string()
            .contains("btrim() requires exactly one argument"),
        "{error}"
    );
}

#[test]
fn compiles_replace_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE replace(service.name, '-', '') = 'billingapi' \
             RETURN replace(service.team, 'platform', 'core') AS normalized_team \
             ORDER BY replace(service.name, '-', '')",
    )
    .expect("replace scalar expressions should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Replace {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                })),
                search: Box::new(ScalarExpression::Literal(Literal::String("-".to_string()))),
                replacement: Box::new(ScalarExpression::Literal(Literal::String(String::new()))),
            },
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
                "billingapi".to_string()
            ))),
        }))
    );
    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::Replace {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "team".to_string(),
                })),
                search: Box::new(ScalarExpression::Literal(Literal::String(
                    "platform".to_string()
                ))),
                replacement: Box::new(ScalarExpression::Literal(Literal::String(
                    "core".to_string()
                ))),
            },
            alias: "normalized_team".to_string(),
        }]
    );
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Replace { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_character_length_and_substring_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE size(service.name) > 10 \
             RETURN substring(service.name, 0, 7) AS prefix, \
                    char_length(service.tier) AS tier_length \
             ORDER BY character_length(service.name)",
    )
    .expect("string length and substring scalar expressions should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::CharacterLength {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                })),
            },
            operator: ComparisonOperator::GreaterThan,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(10))),
        }))
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::Substring {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    })),
                    start: Box::new(ScalarExpression::Literal(Literal::Integer(0))),
                    length: Some(Box::new(ScalarExpression::Literal(Literal::Integer(7)))),
                },
                alias: "prefix".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::CharacterLength {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    })),
                },
                alias: "tier_length".to_string(),
            },
        ]
    );
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::CharacterLength { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_is_empty_string_predicates() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE isEmpty(trim(service.tier)) OR NOT isEmpty(service.name) \
             RETURN service.name",
    )
    .expect("isEmpty predicates should compile");

    assert!(plan.predicates.is_empty());
    assert!(matches!(
        &plan.predicate,
        Some(PredicateExpression::Or { left, right })
            if matches!(
                left.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::CharacterLength { expression },
                    operator: ComparisonOperator::Equal,
                    rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(0))),
                }) if matches!(expression.as_ref(), ScalarExpression::Trim { .. })
            ) && matches!(
                right.as_ref(),
                PredicateExpression::Not { expression }
                    if matches!(
                        expression.as_ref(),
                        PredicateExpression::ScalarComparison(ScalarPredicate {
                            lhs: ScalarExpression::CharacterLength { expression },
                            operator: ComparisonOperator::Equal,
                            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(0))),
                        }) if matches!(
                            expression.as_ref(),
                            ScalarExpression::Property(PropertyRef { property, .. }) if property == "name"
                        )
                    )
            )
    ));
}

#[test]
fn rejects_is_empty_with_unsupported_arity() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             WHERE isEmpty(service.name, service.tier) \
             RETURN service.name",
    )
    .expect_err("isEmpty() requires one argument");

    assert!(
        error
            .to_string()
            .contains("isEmpty() supports exactly one scalar string argument"),
        "{error}"
    );
}

#[test]
fn rejects_substring_with_unsupported_arity() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             RETURN substring(service.name) AS prefix",
    )
    .expect_err("substring() requires a start argument");

    assert!(
        error
            .to_string()
            .contains("substring() requires exactly two or three arguments"),
        "{error}"
    );
}

#[test]
fn compiles_left_right_and_reverse_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE left(service.name, 7) = 'billing' \
             RETURN right(service.name, 3) AS suffix, \
                    reverse(service.tier) AS reversed_tier \
             ORDER BY reverse(service.name)",
    )
    .expect("left, right, and reverse scalar expressions should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Left {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                })),
                count: Box::new(ScalarExpression::Literal(Literal::Integer(7))),
            },
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
                "billing".to_string()
            ))),
        }))
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::Right {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    })),
                    count: Box::new(ScalarExpression::Literal(Literal::Integer(3))),
                },
                alias: "suffix".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Reverse {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    })),
                },
                alias: "reversed_tier".to_string(),
            },
        ]
    );
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Reverse { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_indices_lpad_and_rpad_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE lpad(service.name, 13, '*') = '**billing-api' \
             RETURN indices(service.name, 'i') AS name_indices, \
                    rpad(service.tier, 8, '-') AS padded_tier \
             ORDER BY indices(service.name, 'i')",
    )
    .expect("indices, lpad, and rpad scalar expressions should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::LPad {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                })),
                length: Box::new(ScalarExpression::Literal(Literal::Integer(13))),
                fill: Box::new(ScalarExpression::Literal(Literal::String("*".to_string()))),
            },
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
                "**billing-api".to_string()
            ))),
        }))
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::StringIndices {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    })),
                    pattern: Box::new(ScalarExpression::Literal(Literal::String("i".to_string()))),
                },
                alias: "name_indices".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::RPad {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    })),
                    length: Box::new(ScalarExpression::Literal(Literal::Integer(8))),
                    fill: Box::new(ScalarExpression::Literal(Literal::String("-".to_string()))),
                },
                alias: "padded_tier".to_string(),
            },
        ]
    );
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::StringIndices { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_string_predicate_function_scalar_projections() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN contains(service.name, 'api') AS has_api, \
                    startsWith(service.name, 'bill') AS starts_bill, \
                    endsWith(service.name, 'api') AS ends_api \
             ORDER BY contains(service.name, 'api') DESC",
    )
    .expect("string predicate function scalar projections should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::StringContains {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    })),
                    pattern: Box::new(ScalarExpression::Literal(Literal::String(
                        "api".to_string()
                    ))),
                },
                alias: "has_api".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::StringStartsWith {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    })),
                    pattern: Box::new(ScalarExpression::Literal(Literal::String(
                        "bill".to_string()
                    ))),
                },
                alias: "starts_bill".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::StringEndsWith {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    })),
                    pattern: Box::new(ScalarExpression::Literal(Literal::String(
                        "api".to_string()
                    ))),
                },
                alias: "ends_api".to_string(),
            },
        ]
    );
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::StringContains { .. }),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_string_predicate_functions_as_boolean_predicates() {
    let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE contains(service.name, 'api') \
             RETURN CASE WHEN startsWith(service.name, 'bill') THEN 'billing' ELSE 'other' END AS bucket \
             ORDER BY endsWith(service.name, 'api') DESC",
        )
        .expect("string predicate functions should compile as boolean predicates");

    assert!(matches!(
        &plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::StringContains {
                expression,
                pattern,
            },
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Boolean(
                true
            ))),
        })) if matches!(
            expression.as_ref(),
            ScalarExpression::Property(PropertyRef { property, .. }) if property == "name"
        ) && matches!(
            pattern.as_ref(),
            ScalarExpression::Literal(Literal::String(pattern)) if pattern == "api"
        )
    ));
    assert!(matches!(
        &plan.projections[..],
        [Projection::Expression {
            expression: ScalarExpression::Case {
                alternatives,
                else_expression: Some(_),
            },
            alias,
        }] if alias == "bucket"
            && matches!(
                alternatives.as_slice(),
                [ScalarCaseAlternative {
                    when: PredicateExpression::ScalarComparison(ScalarPredicate {
                        lhs: ScalarExpression::StringStartsWith { .. },
                        operator: ComparisonOperator::Equal,
                        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                            Literal::Boolean(true)
                        )),
                    }),
                    then: ScalarExpression::Literal(Literal::String(bucket)),
                }] if bucket == "billing"
            )
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::StringEndsWith { .. }),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    ));
}

#[test]
fn rejects_contains_with_unsupported_arity() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             RETURN contains(service.name) AS has_api",
    )
    .expect_err("contains() requires a substring argument");

    assert!(
        error
            .to_string()
            .contains("contains() requires exactly two arguments"),
        "{error}"
    );
}

#[test]
fn rejects_left_with_unsupported_arity() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             RETURN left(service.name) AS prefix",
    )
    .expect_err("left() requires a count argument");

    assert!(
        error
            .to_string()
            .contains("left() requires exactly two arguments"),
        "{error}"
    );
}

#[test]
fn rejects_lpad_with_unsupported_arity() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             RETURN lpad(service.name, 10) AS padded",
    )
    .expect_err("lpad() requires a fill argument");

    assert!(
        error
            .to_string()
            .contains("lpad() requires exactly three arguments"),
        "{error}"
    );
}

#[test]
fn compiles_numeric_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE abs(service.risk - 1.0) < 0.2 \
             RETURN ceil(service.risk) AS risk_ceiling, \
                    floor(service.risk) AS risk_floor, \
                    round(service.risk, 1) AS risk_rounded \
             ORDER BY round(service.risk)",
    )
    .expect("numeric scalar functions should compile");

    assert!(matches!(
        &plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Abs { expression },
            operator: ComparisonOperator::LessThan,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Float(_))),
        })) if matches!(
            expression.as_ref(),
            ScalarExpression::Arithmetic {
                operator: ArithmeticOperator::Subtract,
                ..
            }
        )
    ));
    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::Ceil {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    })),
                },
                alias: "risk_ceiling".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Floor {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    })),
                },
                alias: "risk_floor".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Round {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    })),
                    places: Some(Box::new(ScalarExpression::Literal(Literal::Integer(1)))),
                },
                alias: "risk_rounded".to_string(),
            },
        ]
    );
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Round { places: None, .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_more_numeric_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE sqrt(service.risk) < 1.0 AND sign(service.risk - 0.5) = 1 \
             RETURN sqrt(service.risk) AS risk_root, \
                    sign(service.risk - 0.5) AS risk_sign, \
                    exp(service.risk) AS risk_exp, \
                    log(service.risk) AS risk_log, \
                    log10(service.risk) AS risk_log10 \
             ORDER BY log(service.risk)",
    )
    .expect("additional numeric scalar functions should compile");

    assert!(matches!(
        &plan.predicate,
        Some(PredicateExpression::And { left, right })
            if matches!(
                left.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::Sqrt { .. },
                    operator: ComparisonOperator::LessThan,
                    ..
                })
            ) && matches!(
                right.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::Sign { expression },
                    operator: ComparisonOperator::Equal,
                    rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(1))),
                }) if matches!(
                    expression.as_ref(),
                    ScalarExpression::Arithmetic {
                        operator: ArithmeticOperator::Subtract,
                        ..
                    }
                )
            )
    ));
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Sqrt { .. },
                alias
            },
            Projection::Expression {
                expression: ScalarExpression::Sign { .. },
                ..
            },
            Projection::Expression {
                expression: ScalarExpression::Exp { .. },
                ..
            },
            Projection::Expression {
                expression: ScalarExpression::Log { .. },
                ..
            },
            Projection::Expression {
                expression: ScalarExpression::Log10 { .. },
                ..
            },
        ] if alias == "risk_root"
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Log { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_is_nan_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE isNaN(service.risk) = false \
             RETURN isNaN(service.risk) AS risk_is_nan, \
                    isnan(toFloat(service.risk)) AS coerced_risk_is_nan \
             ORDER BY isNaN(service.risk)",
    )
    .expect("isNaN scalar function should compile");

    assert!(matches!(
        &plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::IsNaN { expression },
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Boolean(false))),
        })) if matches!(
            expression.as_ref(),
            ScalarExpression::Property(PropertyRef { property, .. }) if property == "risk"
        )
    ));
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::IsNaN { .. },
                alias: risk_is_nan,
            },
            Projection::Expression {
                expression: ScalarExpression::IsNaN { expression },
                alias: coerced_risk_is_nan,
            },
        ] if risk_is_nan == "risk_is_nan"
            && coerced_risk_is_nan == "coerced_risk_is_nan"
            && matches!(expression.as_ref(), ScalarExpression::ToFloat { .. })
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::IsNaN { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_gql_numeric_scalar_aliases() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN ceiling(service.risk) AS risk_ceiling, \
                    ln(service.risk) AS risk_ln \
             ORDER BY ln(service.risk)",
    )
    .expect("GQL numeric scalar aliases should compile");

    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Ceil { .. },
                alias: risk_ceiling,
            },
            Projection::Expression {
                expression: ScalarExpression::Log { .. },
                alias: risk_ln,
            },
        ] if risk_ceiling == "risk_ceiling" && risk_ln == "risk_ln"
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Log { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn rejects_round_with_unsupported_arity() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             RETURN round(service.risk, 1, 2) AS rounded",
    )
    .expect_err("round() supports only optional places");

    assert!(
        error
            .to_string()
            .contains("round() requires exactly one or two arguments"),
        "{error}"
    );
}

#[test]
fn rejects_more_numeric_scalars_with_unsupported_arity() {
    for cypher in [
        "MATCH (service:Service) RETURN sqrt() AS value",
        "MATCH (service:Service) RETURN sign(service.risk, 1) AS value",
        "MATCH (service:Service) RETURN exp(service.risk, 1) AS value",
        "MATCH (service:Service) RETURN log(service.risk, 10) AS value",
        "MATCH (service:Service) RETURN log10(service.risk, 10) AS value",
        "MATCH (service:Service) RETURN isNaN() AS value",
        "MATCH (service:Service) RETURN isNaN(service.risk, 1) AS value",
    ] {
        let error = compile_cypher(cypher).expect_err("wrong arity should be rejected");
        assert!(
            error.to_string().contains("requires exactly one argument"),
            "{error}"
        );
    }
}

#[test]
fn compiles_trigonometric_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE sin(service.risk) >= 0 AND atan2(service.risk, 1.0) < 1.0 \
             RETURN sin(service.risk) AS risk_sin, \
                    cos(service.risk) AS risk_cos, \
                    tan(service.risk) AS risk_tan, \
                    cot(service.risk) AS risk_cot, \
                    asin(0.5) AS half_asin, \
                    acos(1.0) AS one_acos, \
                    atan(service.risk) AS risk_atan, \
                    atan2(service.risk, 1.0) AS risk_atan2, \
                    degrees(service.risk) AS risk_degrees, \
                    radians(180.0) AS pi_radians \
             ORDER BY radians(degrees(service.risk))",
    )
    .expect("trigonometric scalar functions should compile");

    assert!(matches!(
        &plan.predicate,
        Some(PredicateExpression::And { left, right })
            if matches!(
                left.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::Sin { .. },
                    operator: ComparisonOperator::GreaterThanOrEqual,
                    ..
                })
            ) && matches!(
                right.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::Atan2 { .. },
                    operator: ComparisonOperator::LessThan,
                    ..
                })
            )
    ));
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Sin { .. },
                alias
            },
            Projection::Expression {
                expression: ScalarExpression::Cos { .. },
                ..
            },
            Projection::Expression {
                expression: ScalarExpression::Tan { .. },
                ..
            },
            Projection::Expression {
                expression: ScalarExpression::Cot { .. },
                ..
            },
            Projection::Expression {
                expression: ScalarExpression::Asin { .. },
                ..
            },
            Projection::Expression {
                expression: ScalarExpression::Acos { .. },
                ..
            },
            Projection::Expression {
                expression: ScalarExpression::Atan { .. },
                ..
            },
            Projection::Expression {
                expression: ScalarExpression::Atan2 { .. },
                ..
            },
            Projection::Expression {
                expression: ScalarExpression::Degrees { .. },
                ..
            },
            Projection::Expression {
                expression: ScalarExpression::Radians { .. },
                ..
            },
        ] if alias == "risk_sin"
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Radians { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn rejects_trigonometric_scalars_with_unsupported_arity() {
    for cypher in [
        "MATCH (service:Service) RETURN sin() AS value",
        "MATCH (service:Service) RETURN cos(service.risk, 1) AS value",
        "MATCH (service:Service) RETURN tan() AS value",
        "MATCH (service:Service) RETURN cot(service.risk, 1) AS value",
        "MATCH (service:Service) RETURN asin() AS value",
        "MATCH (service:Service) RETURN acos(service.risk, 1) AS value",
        "MATCH (service:Service) RETURN atan() AS value",
        "MATCH (service:Service) RETURN degrees(service.risk, 1) AS value",
        "MATCH (service:Service) RETURN radians() AS value",
    ] {
        let error = compile_cypher(cypher).expect_err("wrong arity should be rejected");
        assert!(
            error.to_string().contains("requires exactly one argument"),
            "{error}"
        );
    }

    for cypher in [
        "MATCH (service:Service) RETURN atan2(service.risk) AS value",
        "MATCH (service:Service) RETURN atan2(service.risk, 1, 2) AS value",
        "MATCH (service:Service) RETURN pow(service.risk) AS value",
        "MATCH (service:Service) RETURN power(service.risk, 2, 3) AS value",
    ] {
        let error = compile_cypher(cypher).expect_err("wrong arity should be rejected");
        assert!(
            error.to_string().contains("requires exactly two arguments"),
            "{error}"
        );
    }
}

#[test]
fn compiles_power_scalar_function_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE pow(service.risk, 2) > 0.5 \
             RETURN power(service.risk, 2) AS risk_squared \
             ORDER BY pow(service.risk, 2)",
    )
    .expect("power scalar functions should compile");

    let expected = ScalarExpression::Arithmetic {
        operator: ArithmeticOperator::Power,
        left: Box::new(ScalarExpression::Property(PropertyRef {
            variable: "service".to_string(),
            property: "risk".to_string(),
        })),
        right: Box::new(ScalarExpression::Literal(Literal::Integer(2))),
    };
    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: expected.clone(),
            operator: ComparisonOperator::GreaterThan,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Float(
                ordered_float::OrderedFloat(0.5),
            ))),
        }))
    );
    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: expected.clone(),
            alias: "risk_squared".to_string(),
        }]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(expected),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_math_constant_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE service.risk < pi() \
             RETURN pi() AS pi_value, e() AS e_value, sin(pi()) AS zeroish \
             ORDER BY e()",
    )
    .expect("math constants should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "risk".to_string(),
            }),
            operator: ComparisonOperator::LessThan,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Float(
                ordered_float::OrderedFloat(std::f64::consts::PI),
            ))),
        }))
    );
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Float(pi)),
                alias
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Float(e)),
                ..
            },
            Projection::Expression {
                expression: ScalarExpression::Sin { expression },
                ..
            },
        ] if *pi == ordered_float::OrderedFloat(std::f64::consts::PI)
            && *e == ordered_float::OrderedFloat(std::f64::consts::E)
            && alias == "pi_value"
            && matches!(expression.as_ref(), ScalarExpression::Literal(Literal::Float(value))
                if *value == ordered_float::OrderedFloat(std::f64::consts::PI))
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Literal(Literal::Float(e))),
            direction: OrderDirection::Ascending,
            nulls: None,
        }] if *e == ordered_float::OrderedFloat(std::f64::consts::E)
    ));
}

#[test]
fn rejects_math_constants_with_arguments() {
    for cypher in [
        "MATCH (service:Service) RETURN pi(1) AS value",
        "MATCH (service:Service) RETURN e(service.risk) AS value",
    ] {
        let error = compile_cypher(cypher).expect_err("math constants take no arguments");
        assert!(
            error
                .to_string()
                .contains("requires exactly zero arguments"),
            "{error}"
        );
    }
}

#[test]
fn compiles_haversin_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE haversin(service.risk) < 0.1 \
             RETURN haversin(0.0) AS zero_haversin \
             ORDER BY haversin(service.risk)",
    )
    .expect("haversin() should compile");

    assert!(matches!(
        &plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Arithmetic {
                operator: ArithmeticOperator::Divide,
                left,
                right,
            },
            operator: ComparisonOperator::LessThan,
            ..
        })) if matches!(
            left.as_ref(),
            ScalarExpression::Arithmetic {
                operator: ArithmeticOperator::Subtract,
                ..
            }
        ) && matches!(
            right.as_ref(),
            ScalarExpression::Literal(Literal::Integer(2))
        )
    ));
    assert!(matches!(
        plan.projections.as_slice(),
        [Projection::Expression {
            expression: ScalarExpression::Arithmetic {
                operator: ArithmeticOperator::Divide,
                ..
            },
            alias
        }] if alias == "zero_haversin"
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Arithmetic {
                operator: ArithmeticOperator::Divide,
                ..
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn rejects_haversin_with_unsupported_arity() {
    for cypher in [
        "MATCH (service:Service) RETURN haversin() AS value",
        "MATCH (service:Service) RETURN haversin(service.risk, 1) AS value",
    ] {
        let error = compile_cypher(cypher).expect_err("wrong arity should be rejected");
        assert!(
            error.to_string().contains("requires exactly one argument"),
            "{error}"
        );
    }
}

#[test]
fn compiles_scalar_cast_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE toInteger(service.id) = 10 \
             RETURN toFloat(service.risk) AS risk_float, \
                    toBoolean(service.active) AS active_bool \
             ORDER BY toInteger(service.id)",
    )
    .expect("scalar cast expressions should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::ToInteger {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "id".to_string(),
                })),
            },
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(10))),
        }))
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::ToFloat {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    })),
                },
                alias: "risk_float".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::ToBoolean {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "active".to_string(),
                    })),
                },
                alias: "active_bool".to_string(),
            },
        ]
    );
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::ToInteger { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_nullable_scalar_cast_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE toIntegerOrNull(service.id) = 10 \
             RETURN toStringOrNull(service.id) AS id_text, \
                    toFloatOrNull(service.risk) AS risk_float, \
                    toBooleanOrNull(service.active) AS active_bool \
             ORDER BY toIntegerOrNull(service.id)",
    )
    .expect("nullable scalar cast expressions should compile");

    assert!(matches!(
        &plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::ToIntegerOrNull { .. },
            operator: ComparisonOperator::Equal,
            ..
        }))
    ));
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::ToStringOrNull { .. },
                alias
            },
            Projection::Expression {
                expression: ScalarExpression::ToFloatOrNull { .. },
                ..
            },
            Projection::Expression {
                expression: ScalarExpression::ToBooleanOrNull { .. },
                ..
            },
        ] if alias == "id_text"
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::ToIntegerOrNull { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn rejects_nullable_scalar_casts_with_unsupported_arity() {
    for cypher in [
        "MATCH (service:Service) RETURN toStringOrNull() AS value",
        "MATCH (service:Service) RETURN toIntegerOrNull(service.id, 10) AS value",
        "MATCH (service:Service) RETURN toFloatOrNull() AS value",
        "MATCH (service:Service) RETURN toBooleanOrNull(service.active, false) AS value",
    ] {
        let error = compile_cypher(cypher).expect_err("wrong arity should be rejected");
        assert!(
            error.to_string().contains("requires exactly one argument"),
            "{error}"
        );
    }
}

#[test]
fn compiles_arithmetic_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE service.risk * 100 >= 50 \
             RETURN service.risk * 100 + 1 AS risk_points, \
                    service.risk ^ 2 AS risk_squared \
             ORDER BY service.id % 20",
    )
    .expect("arithmetic scalar expressions should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Arithmetic {
                operator: ArithmeticOperator::Multiply,
                left: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                })),
                right: Box::new(ScalarExpression::Literal(Literal::Integer(100))),
            },
            operator: ComparisonOperator::GreaterThanOrEqual,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(50))),
        }))
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::Arithmetic {
                    operator: ArithmeticOperator::Add,
                    left: Box::new(ScalarExpression::Arithmetic {
                        operator: ArithmeticOperator::Multiply,
                        left: Box::new(ScalarExpression::Property(PropertyRef {
                            variable: "service".to_string(),
                            property: "risk".to_string(),
                        })),
                        right: Box::new(ScalarExpression::Literal(Literal::Integer(100))),
                    }),
                    right: Box::new(ScalarExpression::Literal(Literal::Integer(1))),
                },
                alias: "risk_points".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Arithmetic {
                    operator: ArithmeticOperator::Power,
                    left: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    })),
                    right: Box::new(ScalarExpression::Literal(Literal::Integer(2))),
                },
                alias: "risk_squared".to_string(),
            },
        ]
    );
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Arithmetic {
                operator: ArithmeticOperator::Modulo,
                ..
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_unary_negation_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE -service.risk < -0.8 \
             RETURN -service.risk AS inverse_risk, \
                    -(service.risk * 100) AS inverse_points \
             ORDER BY -service.risk",
    )
    .expect("unary negation scalar expressions should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Negate {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                })),
            },
            operator: ComparisonOperator::LessThan,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Float(
                OrderedFloat(-0.8)
            ))),
        }))
    );
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Negate { expression },
                alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Negate {
                    expression: nested
                },
                alias: nested_alias,
            },
        ] if alias == "inverse_risk"
            && matches!(expression.as_ref(), ScalarExpression::Property(_))
            && nested_alias == "inverse_points"
            && matches!(nested.as_ref(), ScalarExpression::Arithmetic { .. })
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Negate { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_searched_case_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN CASE \
                      WHEN service.risk >= 0.75 THEN 'high' \
                      WHEN service.active AND service.tier = 'prod' THEN 'watch' \
                      ELSE 'normal' \
                    END AS risk_band \
             ORDER BY CASE WHEN service.active THEN 0 ELSE 1 END",
    )
    .expect("searched CASE scalar expressions should compile");

    let [
        Projection::Expression {
            expression:
                ScalarExpression::Case {
                    alternatives,
                    else_expression,
                },
            alias,
        },
    ] = plan.projections.as_slice()
    else {
        panic!("expected CASE expression projection");
    };
    assert_eq!(alias, "risk_band");
    let [high_alternative, watch_alternative] = alternatives.as_slice() else {
        panic!("expected two CASE alternatives");
    };
    assert!(matches!(
        &high_alternative.when,
        PredicateExpression::Comparison(PropertyPredicate {
            property: PropertyRef { variable, property },
            operator: ComparisonOperator::GreaterThanOrEqual,
            rhs: PredicateRhs::Literal(Literal::Float(_)),
        }) if variable == "service" && property == "risk"
    ));
    assert_eq!(
        high_alternative.then,
        ScalarExpression::Literal(Literal::String("high".to_string()))
    );
    assert!(matches!(
        &watch_alternative.when,
        PredicateExpression::And { .. }
    ));
    assert_eq!(
        else_expression.as_deref(),
        Some(&ScalarExpression::Literal(Literal::String(
            "normal".to_string()
        )))
    );
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Case { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_static_list_case_projection_and_ordering() {
    let query = compile_cypher_query_for_graph(
            &star_test_graph(),
            "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN CASE WHEN person IS NULL THEN [] ELSE keys(person) END AS owner_keys, \
                    CASE WHEN person IS NOT NULL THEN labels(person) ELSE ['missing'] END AS owner_labels, \
                    CASE WHEN person IS NULL THEN [] ELSE coalesce(keys(person), []) END AS coalesced_keys \
             ORDER BY CASE WHEN person IS NULL THEN [] ELSE keys(person) END",
        )
        .expect("static list CASE should compile with graph metadata");

    let GraphQuery::Plan(plan) = query else {
        panic!("expected single graph plan");
    };
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Case {
                    alternatives,
                    else_expression,
                },
                alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Case { .. },
                alias: label_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Case { .. },
                alias: coalesced_alias,
            },
        ] if alias == "owner_keys"
            && matches!(
                alternatives.as_slice(),
                [ScalarCaseAlternative {
                    then: ScalarExpression::TypedLiteralList {
                        literals,
                        element_type: LiteralListElementType::String,
                    },
                    ..
                }] if literals.is_empty()
            )
            && matches!(
                else_expression.as_deref(),
                Some(ScalarExpression::PresenceGated {
                    presence_variable,
                    expression,
                }) if presence_variable == "person"
                    && matches!(
                        expression.as_ref(),
                        ScalarExpression::TypedLiteralList {
                            literals,
                            element_type: LiteralListElementType::String,
                        } if literals == &vec![
                            Literal::String("name".to_string()),
                            Literal::String("team".to_string()),
                        ]
                    )
            )
            && label_alias == "owner_labels"
            && coalesced_alias == "coalesced_keys"
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Case { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_static_list_case_size_and_is_empty() {
    let query = compile_cypher_query_for_graph(
            &star_test_graph(),
            "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN size(CASE WHEN person IS NULL THEN [] ELSE keys(person) END) AS owner_key_count, \
                    isEmpty(CASE WHEN person IS NULL THEN [] ELSE keys(person) END) AS owner_keys_empty, \
                    size(CASE WHEN service.tier = 'prod' THEN [] ELSE null END) AS empty_count, \
                    isEmpty(CASE WHEN service.tier = 'prod' THEN [] ELSE null END) AS empty_is_empty \
             ORDER BY size(CASE WHEN person IS NULL THEN [] ELSE keys(person) END)",
        )
        .expect("static list CASE size/isEmpty should compile with graph metadata");

    let GraphQuery::Plan(plan) = query else {
        panic!("expected single graph plan");
    };
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Case { alternatives, .. },
                alias: count_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Predicate(predicate),
                alias: empty_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Case {
                    alternatives: untyped_count_alternatives,
                    else_expression,
                },
                alias: untyped_count_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Predicate(untyped_empty_predicate),
                alias: untyped_empty_alias,
            },
        ] if count_alias == "owner_key_count"
            && matches!(
                alternatives.as_slice(),
                [ScalarCaseAlternative {
                    then: ScalarExpression::Literal(Literal::Integer(0)),
                    ..
                }]
            )
            && empty_alias == "owner_keys_empty"
            && matches!(
                predicate.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::Case { .. },
                    operator: ComparisonOperator::Equal,
                    rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                        Literal::Boolean(true)
                    )),
                })
            )
            && untyped_count_alias == "empty_count"
            && matches!(
                untyped_count_alternatives.as_slice(),
                [ScalarCaseAlternative {
                    then: ScalarExpression::Literal(Literal::Integer(0)),
                    ..
                }]
            )
            && matches!(
                else_expression.as_deref(),
                Some(ScalarExpression::Literal(Literal::Null))
            )
            && untyped_empty_alias == "empty_is_empty"
            && matches!(
                untyped_empty_predicate.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::Case { .. },
                    operator: ComparisonOperator::Equal,
                    rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                        Literal::Boolean(true)
                    )),
                })
            )
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Case { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_static_list_case_endpoint_functions() {
    let query = compile_cypher_query_for_graph(
            &star_test_graph(),
            "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN head(CASE WHEN person IS NULL THEN [] ELSE keys(person) END) AS first_owner_key, \
                    last(CASE WHEN person IS NULL THEN [] ELSE keys(person) END) AS last_owner_key, \
                    head(coalesce(keys(person), [])) AS coalesced_first_key, \
                    last(CASE WHEN service.tier = 'prod' THEN [] ELSE null END) AS empty_last \
             ORDER BY last(CASE WHEN person IS NULL THEN [] ELSE keys(person) END)",
        )
        .expect("static list CASE head/last should compile with graph metadata");

    let GraphQuery::Plan(plan) = query else {
        panic!("expected single graph plan");
    };
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Case { alternatives, .. },
                alias: first_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Case { .. },
                alias: last_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Coalesce { expressions },
                alias: coalesced_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Case {
                    alternatives: empty_alternatives,
                    else_expression,
                },
                alias: empty_alias,
            },
        ] if first_alias == "first_owner_key"
            && matches!(
                alternatives.as_slice(),
                [ScalarCaseAlternative {
                    then: ScalarExpression::Literal(Literal::Null),
                    ..
                }]
            )
            && last_alias == "last_owner_key"
            && coalesced_alias == "coalesced_first_key"
            && expressions.len() == 2
            && empty_alias == "empty_last"
            && matches!(
                empty_alternatives.as_slice(),
                [ScalarCaseAlternative {
                    then: ScalarExpression::Literal(Literal::Null),
                    ..
                }]
            )
            && matches!(
                else_expression.as_deref(),
                Some(ScalarExpression::Literal(Literal::Null))
            )
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Case { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_static_list_case_and_coalesce_indexes() {
    let query = compile_cypher_query_for_graph(
            &star_test_graph(),
            "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN (CASE WHEN person IS NULL THEN ['fallback'] ELSE keys(person) END)[0] AS case_first_key, \
                    coalesce(keys(person), ['fallback'])[0] AS coalesced_first_key, \
                    (CASE WHEN service.tier = 'prod' THEN [] ELSE null END)[0] AS empty_first_key \
             ORDER BY (CASE WHEN person IS NULL THEN ['fallback'] ELSE keys(person) END)[-1]",
        )
        .expect("static list CASE/coalesce indexes should compile with graph metadata");

    let GraphQuery::Plan(plan) = query else {
        panic!("expected single graph plan");
    };
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Case { alternatives, .. },
                alias: case_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Case {
                    alternatives: coalesce_alternatives,
                    ..
                },
                alias: coalesce_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Case {
                    alternatives: empty_alternatives,
                    else_expression,
                },
                alias: empty_alias,
            },
        ] if case_alias == "case_first_key"
            && matches!(
                alternatives.as_slice(),
                [ScalarCaseAlternative {
                    then: ScalarExpression::Literal(Literal::String(value)),
                    ..
                }] if value == "fallback"
            )
            && coalesce_alias == "coalesced_first_key"
            && matches!(
                coalesce_alternatives.as_slice(),
                [ScalarCaseAlternative {
                    when: PredicateExpression::Presence(PresencePredicate {
                        variable,
                        operator: ComparisonOperator::NotEqual,
                    }),
                    then: ScalarExpression::Literal(Literal::String(value)),
                }] if variable == "person" && value == "name"
            )
            && empty_alias == "empty_first_key"
            && matches!(
                empty_alternatives.as_slice(),
                [ScalarCaseAlternative {
                    then: ScalarExpression::Literal(Literal::Null),
                    ..
                }]
            )
            && matches!(
                else_expression.as_deref(),
                Some(ScalarExpression::Literal(Literal::Null))
            )
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Case { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_static_list_case_and_coalesce_slices() {
    let query = compile_cypher_query_for_graph(
            &star_test_graph(),
            "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN (CASE WHEN person IS NULL THEN ['fallback', 'owner'] ELSE keys(person) END)[0..1] AS case_key_window, \
                    coalesce(keys(person), ['fallback', 'owner'])[0..1] AS coalesced_key_window, \
                    (CASE WHEN service.tier = 'prod' THEN [] ELSE ['not-prod'] END)[0..1] AS tier_window \
             ORDER BY coalesce(keys(person), ['fallback', 'owner'])[0..1]",
        )
        .expect("static list CASE/coalesce slices should compile with graph metadata");

    let GraphQuery::Plan(plan) = query else {
        panic!("expected single graph plan");
    };
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Case { alternatives, .. },
                alias: case_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Case {
                    alternatives: coalesce_alternatives,
                    ..
                },
                alias: coalesce_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Case {
                    alternatives: tier_alternatives,
                    else_expression,
                },
                alias: tier_alias,
            },
        ] if case_alias == "case_key_window"
            && matches!(
                alternatives.as_slice(),
                [ScalarCaseAlternative {
                    then: ScalarExpression::TypedLiteralList { literals, element_type },
                    ..
                }] if literals.as_slice() == [Literal::String("fallback".to_string())]
                    && *element_type == LiteralListElementType::String
            )
            && coalesce_alias == "coalesced_key_window"
            && matches!(
                coalesce_alternatives.as_slice(),
                [ScalarCaseAlternative {
                    when: PredicateExpression::Presence(PresencePredicate {
                        variable,
                        operator: ComparisonOperator::NotEqual,
                    }),
                    then: ScalarExpression::TypedLiteralList { literals, element_type },
                }] if variable == "person"
                    && literals.as_slice() == [Literal::String("name".to_string())]
                    && *element_type == LiteralListElementType::String
            )
            && tier_alias == "tier_window"
            && matches!(
                tier_alternatives.as_slice(),
                [ScalarCaseAlternative {
                    then: ScalarExpression::TypedLiteralList { literals, element_type },
                    ..
                }] if literals.is_empty()
                    && *element_type == LiteralListElementType::String
            )
            && matches!(
                else_expression.as_deref(),
                Some(ScalarExpression::TypedLiteralList {
                    literals,
                    element_type,
                }) if literals.as_slice() == [Literal::String("not-prod".to_string())]
                    && *element_type == LiteralListElementType::String
            )
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Case { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_static_list_case_and_coalesce_slice_reducers() {
    let query = compile_cypher_query_for_graph(
            &star_test_graph(),
            "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN size((CASE WHEN person IS NULL THEN ['fallback', 'owner'] ELSE keys(person) END)[0..1]) AS case_window_size, \
                    isEmpty(coalesce(keys(person), ['fallback'])[2..]) AS coalesced_tail_empty, \
                    size((CASE WHEN service.tier = 'prod' THEN [] ELSE null END)[0..1]) AS empty_window_size, \
                    isEmpty((CASE WHEN service.tier = 'prod' THEN [] ELSE null END)[0..1]) AS empty_window_is_empty \
             ORDER BY size(coalesce(keys(person), ['fallback'])[0..1])",
        )
        .expect("static list CASE/coalesce slice reducers should compile with graph metadata");

    let GraphQuery::Plan(plan) = query else {
        panic!("expected single graph plan");
    };
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Case { alternatives, .. },
                alias: case_size_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Predicate(coalesced_empty),
                alias: coalesced_empty_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Case {
                    alternatives: empty_size_alternatives,
                    else_expression,
                },
                alias: empty_size_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Predicate(empty_predicate),
                alias: empty_alias,
            },
        ] if case_size_alias == "case_window_size"
            && matches!(
                alternatives.as_slice(),
                [ScalarCaseAlternative {
                    then: ScalarExpression::Literal(Literal::Integer(1)),
                    ..
                }]
            )
            && coalesced_empty_alias == "coalesced_tail_empty"
            && matches!(
                coalesced_empty.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::Case { .. },
                    operator: ComparisonOperator::Equal,
                    rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                        Literal::Boolean(true),
                    )),
                })
            )
            && empty_size_alias == "empty_window_size"
            && matches!(
                empty_size_alternatives.as_slice(),
                [ScalarCaseAlternative {
                    then: ScalarExpression::Literal(Literal::Integer(0)),
                    ..
                }]
            )
            && matches!(
                else_expression.as_deref(),
                Some(ScalarExpression::Literal(Literal::Null))
            )
            && empty_alias == "empty_window_is_empty"
            && matches!(
                empty_predicate.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::Case { .. },
                    operator: ComparisonOperator::Equal,
                    rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                        Literal::Boolean(true),
                    )),
                })
            )
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Case { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_static_list_case_and_coalesce_slice_indexes_and_endpoints() {
    let query = compile_cypher_query_for_graph(
            &star_test_graph(),
            "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN ((CASE WHEN person IS NULL THEN ['fallback', 'owner'] ELSE keys(person) END)[0..1])[0] AS case_slice_first, \
                    (coalesce(keys(person), ['fallback', 'owner'])[0..1])[0] AS coalesced_slice_first, \
                    head((CASE WHEN service.tier = 'prod' THEN [] ELSE ['not-prod'] END)[0..1]) AS tier_head, \
                    last(coalesce(keys(person), ['fallback', 'owner'])[0..1]) AS coalesced_slice_last \
             ORDER BY ((CASE WHEN person IS NULL THEN ['fallback', 'owner'] ELSE keys(person) END)[0..1])[0]",
        )
        .expect("static list CASE/coalesce slice indexes and endpoints should compile");

    let GraphQuery::Plan(plan) = query else {
        panic!("expected single graph plan");
    };
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Case { alternatives, .. },
                alias: case_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Case {
                    alternatives: coalesced_alternatives,
                    ..
                },
                alias: coalesced_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Case {
                    alternatives: tier_alternatives,
                    else_expression,
                },
                alias: tier_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Case { .. },
                alias: last_alias,
            },
        ] if case_alias == "case_slice_first"
            && matches!(
                alternatives.as_slice(),
                [ScalarCaseAlternative {
                    then: ScalarExpression::Literal(Literal::String(value)),
                    ..
                }] if value == "fallback"
            )
            && coalesced_alias == "coalesced_slice_first"
            && matches!(
                coalesced_alternatives.as_slice(),
                [ScalarCaseAlternative {
                    when: PredicateExpression::Presence(PresencePredicate {
                        variable,
                        operator: ComparisonOperator::NotEqual,
                    }),
                    then: ScalarExpression::Literal(Literal::String(value)),
                }] if variable == "person" && value == "name"
            )
            && tier_alias == "tier_head"
            && matches!(
                tier_alternatives.as_slice(),
                [ScalarCaseAlternative {
                    then: ScalarExpression::Literal(Literal::Null),
                    ..
                }]
            )
            && matches!(
                else_expression.as_deref(),
                Some(ScalarExpression::Literal(Literal::String(value))) if value == "not-prod"
            )
            && last_alias == "coalesced_slice_last"
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Case { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

fn is_case_boolean_scalar_predicate(predicate: &PredicateExpression) -> bool {
    matches!(
        predicate,
        PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Case { .. },
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Boolean(true),)),
        })
    )
}

#[test]
fn compiles_static_list_case_and_coalesce_slice_comparisons() {
    let query = compile_cypher_query_for_graph(
            &star_test_graph(),
            "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             WHERE (CASE WHEN person IS NULL THEN ['fallback', 'owner'] ELSE keys(person) END)[0..1] = ['name'] \
                OR ['fallback'] = coalesce(keys(person), ['fallback', 'owner'])[0..1] \
             RETURN (CASE WHEN person IS NULL THEN ['fallback', 'owner'] ELSE keys(person) END)[0..1] = ['name'] AS case_slice_matches, \
                    ['fallback'] = coalesce(keys(person), ['fallback', 'owner'])[0..1] AS coalesced_slice_fallback, \
                    (CASE WHEN service.tier = 'prod' THEN [] ELSE ['not-prod'] END)[0..1] <> [] AS tier_window_non_empty \
             ORDER BY coalesce(keys(person), ['fallback', 'owner'])[0..1] > ['fallback']",
        )
        .expect("static list CASE/coalesce slice comparisons should compile");

    let GraphQuery::Plan(plan) = query else {
        panic!("expected single graph plan");
    };
    assert!(plan.predicate.is_none());
    let optional_predicate = &plan
        .optional_matches
        .first()
        .expect("optional match scope")
        .predicate;
    assert!(
        matches!(
            optional_predicate,
            Some(PredicateExpression::Or { left, right })
                if is_case_boolean_scalar_predicate(left.as_ref())
                    && is_case_boolean_scalar_predicate(right.as_ref())
        ),
        "{optional_predicate:#?}"
    );
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Predicate(case_predicate),
                alias: case_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Predicate(coalesced_predicate),
                alias: coalesced_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Predicate(tier_predicate),
                alias: tier_alias,
            },
        ] if case_alias == "case_slice_matches"
            && coalesced_alias == "coalesced_slice_fallback"
            && tier_alias == "tier_window_non_empty"
            && is_case_boolean_scalar_predicate(case_predicate.as_ref())
            && is_case_boolean_scalar_predicate(coalesced_predicate.as_ref())
            && is_case_boolean_scalar_predicate(tier_predicate.as_ref())
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Predicate(_)),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_static_list_case_and_coalesce_in_rhs_predicates() {
    let query = compile_cypher_query_for_graph(
            &star_test_graph(),
            "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             WHERE service.tier IN coalesce(keys(person), ['prod']) \
             RETURN 'team' IN CASE WHEN person IS NULL THEN ['fallback'] ELSE keys(person) END AS case_has_team_key, \
                    service.tier IN coalesce(keys(person), ['prod']) AS coalesced_tier_membership \
             ORDER BY 'team' IN CASE WHEN person IS NULL THEN ['fallback'] ELSE keys(person) END",
        )
        .expect("static list CASE/coalesce IN right-hand sides should compile");

    let GraphQuery::Plan(plan) = query else {
        panic!("expected single graph plan");
    };
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Predicate(case_predicate),
                alias: case_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Predicate(coalesce_predicate),
                alias: coalesce_alias,
            },
        ] if case_alias == "case_has_team_key"
            && coalesce_alias == "coalesced_tier_membership"
            && matches!(
                case_predicate.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::Case { alternatives, .. },
                    operator: ComparisonOperator::Equal,
                    rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                        Literal::Boolean(true),
                    )),
                }) if matches!(
                    alternatives.as_slice(),
                    [
                        ScalarCaseAlternative {
                            then: ScalarExpression::Predicate(_),
                            ..
                        },
                    ]
                )
            )
            && matches!(
                coalesce_predicate.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::Case { alternatives, .. },
                    operator: ComparisonOperator::Equal,
                    rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                        Literal::Boolean(true),
                    )),
                }) if matches!(
                    alternatives.as_slice(),
                    [
                        ScalarCaseAlternative {
                            when: PredicateExpression::Presence(PresencePredicate {
                                variable,
                                operator: ComparisonOperator::NotEqual,
                            }),
                            then: ScalarExpression::Predicate(_),
                        },
                    ] if variable == "person"
                )
            )
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Predicate(_)),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_static_list_case_and_coalesce_slice_in_rhs_predicates() {
    let query = compile_cypher_query_for_graph(
            &star_test_graph(),
            "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             WHERE 'name' IN coalesce(keys(person), ['fallback', 'owner'])[0..1] \
                OR service.name IN coalesce(keys(person), ['legacy-sync', 'fallback'])[0..1] \
             RETURN 'team' IN (CASE WHEN person IS NULL THEN ['fallback', 'owner'] ELSE keys(person) END)[1..2] AS case_slice_has_team, \
                    'fallback' IN coalesce(keys(person), ['fallback', 'owner'])[0..1] AS coalesced_slice_has_fallback \
             ORDER BY 'fallback' IN coalesce(keys(person), ['fallback', 'owner'])[0..1]",
        )
        .expect("sliced static list CASE/coalesce IN right-hand sides should compile");

    let GraphQuery::Plan(plan) = query else {
        panic!("expected single graph plan");
    };
    let optional_predicate = &plan
        .optional_matches
        .first()
        .expect("optional match scope")
        .predicate;
    assert!(
        matches!(
            optional_predicate,
            Some(PredicateExpression::Or { left, right })
                if is_case_boolean_scalar_predicate(left.as_ref())
                    && is_case_boolean_scalar_predicate(right.as_ref())
        ),
        "{optional_predicate:#?}"
    );
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Predicate(case_predicate),
                alias: case_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Predicate(coalesced_predicate),
                alias: coalesced_alias,
            },
        ] if case_alias == "case_slice_has_team"
            && coalesced_alias == "coalesced_slice_has_fallback"
            && is_case_boolean_scalar_predicate(case_predicate.as_ref())
            && is_case_boolean_scalar_predicate(coalesced_predicate.as_ref())
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Predicate(_)),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn rejects_invalid_static_list_case_results() {
    let scalar_mix = compile_cypher_query_for_graph(
            &star_test_graph(),
            "MATCH (service:Service) \
             RETURN CASE WHEN service.tier = 'prod' THEN keys(service) ELSE 'missing' END AS keys_or_missing",
        )
        .expect_err("scalar/list CASE result mixes should be rejected");
    assert!(
        scalar_mix
            .to_string()
            .contains("every non-null branch to be a static list"),
        "{scalar_mix}"
    );

    let mixed_types = compile_cypher_query_for_graph(
        &star_test_graph(),
        "MATCH (service:Service) \
             RETURN CASE WHEN service.tier = 'prod' THEN [1] ELSE ['missing'] END AS mixed",
    )
    .expect_err("mixed list element types should be rejected");
    assert!(
        mixed_types
            .to_string()
            .contains("compatible non-null list element types"),
        "{mixed_types}"
    );

    let untyped_empty = compile_cypher_query_for_graph(
        &star_test_graph(),
        "MATCH (service:Service) \
             RETURN CASE WHEN service.tier = 'prod' THEN [] ELSE null END AS untyped",
    )
    .expect_err("all-empty/all-null list CASE should be rejected");
    assert!(
        untyped_empty
            .to_string()
            .contains("at least one non-null list element type"),
        "{untyped_empty}"
    );
}

#[test]
fn compiles_graph_null_checks_inside_searched_case_predicates() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[owns:OWNS]->(service) \
             RETURN CASE \
                      WHEN person IS NULL THEN 'unowned' \
                      WHEN id(owns) IS NOT NULL THEN person.name \
                      ELSE 'unknown' \
                    END AS ownership_state \
             ORDER BY CASE WHEN person IS NOT NULL THEN 0 ELSE 1 END",
    )
    .expect("CASE graph null checks should compile");

    let [
        Projection::Expression {
            expression:
                ScalarExpression::Case {
                    alternatives,
                    else_expression,
                },
            alias,
        },
    ] = plan.projections.as_slice()
    else {
        panic!("expected CASE expression projection");
    };
    assert_eq!(alias, "ownership_state");
    let [unowned, owned] = alternatives.as_slice() else {
        panic!("expected two CASE alternatives");
    };
    assert_eq!(
        unowned.when,
        PredicateExpression::Presence(PresencePredicate {
            variable: "person".to_string(),
            operator: ComparisonOperator::Equal,
        })
    );
    assert!(matches!(
        &owned.when,
        PredicateExpression::KeyComparison(KeyPredicate {
            variable,
            operator: ComparisonOperator::NotEqual,
            rhs: PredicateRhs::Literal(Literal::Null),
        }) if variable == "owns"
    ));
    assert_eq!(
        else_expression.as_deref(),
        Some(&ScalarExpression::Literal(Literal::String(
            "unknown".to_string()
        )))
    );
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Case {
                alternatives,
                ..
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }] if matches!(
            alternatives.as_slice(),
            [ScalarCaseAlternative {
                when: PredicateExpression::Presence(PresencePredicate {
                    variable,
                    operator: ComparisonOperator::NotEqual,
                }),
                ..
            }] if variable == "person"
        )
    ));
}

#[test]
fn compiles_graph_metadata_predicates_inside_searched_case_predicates() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             MATCH (person:Person)-[owns:OWNS]->(service) \
             RETURN CASE \
                      WHEN type(owns) = 'OWNS' THEN 'relationship' \
                      WHEN service:Service \
                        AND 'Service' IN labels(service) \
                        AND labels(service) = ['Service'] \
                        AND 'source' IN keys(owns) \
                        AND keys(owns) = ['since', 'source'] THEN 'metadata' \
                      ELSE 'unknown' \
                    END AS state \
             ORDER BY CASE WHEN type(owns) IN ['OWNS'] THEN 0 ELSE 1 END",
    )
    .expect("CASE graph metadata predicates should compile");

    let [
        Projection::Expression {
            expression: ScalarExpression::Case { alternatives, .. },
            ..
        },
    ] = plan.projections.as_slice()
    else {
        panic!("expected CASE expression projection");
    };
    let [relationship, metadata] = alternatives.as_slice() else {
        panic!("expected two CASE alternatives");
    };
    assert_eq!(relationship.when, PredicateExpression::Boolean(true));
    assert!(matches!(metadata.when, PredicateExpression::And { .. }));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Case {
                alternatives,
                ..
            }),
            ..
        }] if matches!(
            alternatives.as_slice(),
            [ScalarCaseAlternative {
                when: PredicateExpression::Boolean(true),
                ..
            }]
        )
    ));
}

#[test]
fn compiles_xor_inside_searched_case_predicates() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN CASE \
                      WHEN service.tier = 'prod' XOR service.name CONTAINS 'billing' THEN 'xor' \
                      ELSE 'other' \
                    END AS marker",
    )
    .expect("searched CASE XOR predicates should compile");

    let [
        Projection::Expression {
            expression: ScalarExpression::Case { alternatives, .. },
            ..
        },
    ] = plan.projections.as_slice()
    else {
        panic!("expected CASE expression projection");
    };
    assert!(matches!(
        alternatives.as_slice(),
        [ScalarCaseAlternative {
            when: PredicateExpression::Xor { .. },
            ..
        }]
    ));
}

#[test]
fn compiles_is_empty_inside_searched_case_predicates() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN CASE \
                      WHEN isEmpty(trim(service.tier)) THEN 'empty' \
                      ELSE 'present' \
                    END AS tier_state",
    )
    .expect("searched CASE isEmpty predicates should compile");

    let [
        Projection::Expression {
            expression: ScalarExpression::Case { alternatives, .. },
            ..
        },
    ] = plan.projections.as_slice()
    else {
        panic!("expected CASE expression projection");
    };
    assert!(matches!(
        alternatives.as_slice(),
        [ScalarCaseAlternative {
            when: PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::CharacterLength { expression },
                operator: ComparisonOperator::Equal,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(0))),
            }),
            ..
        }] if matches!(expression.as_ref(), ScalarExpression::Trim { .. })
    ));
}

#[test]
fn compiles_generic_case_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN CASE service.tier WHEN 'prod' THEN 'production' ELSE 'other' END AS tier",
    )
    .expect("generic CASE scalar expressions should compile");

    let [
        Projection::Expression {
            expression:
                ScalarExpression::Case {
                    alternatives,
                    else_expression,
                },
            alias,
        },
    ] = plan.projections.as_slice()
    else {
        panic!("expected CASE expression projection");
    };
    assert_eq!(alias, "tier");
    let [production_alternative] = alternatives.as_slice() else {
        panic!("expected one CASE alternative");
    };
    assert_eq!(
        production_alternative.when,
        PredicateExpression::Comparison(PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
        })
    );
    assert_eq!(
        production_alternative.then,
        ScalarExpression::Literal(Literal::String("production".to_string()))
    );
    assert_eq!(
        else_expression.as_deref(),
        Some(&ScalarExpression::Literal(Literal::String(
            "other".to_string()
        )))
    );
}

#[test]
fn compiles_scalar_null_predicates() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE coalesce(service.tier, null) IS NOT NULL \
             RETURN service.name AS service",
    )
    .expect("scalar null predicate should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Coalesce {
                expressions: vec![
                    ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    }),
                    ScalarExpression::Literal(Literal::Null),
                ],
            },
            operator: ComparisonOperator::NotEqual,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Null)),
        }))
    );
}

#[test]
fn compiles_coalesce_predicates() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE coalesce(service.tier, 'unassigned') = 'prod' \
             RETURN service.name AS service",
    )
    .expect("coalesce predicate should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Coalesce {
                expressions: vec![
                    ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    }),
                    ScalarExpression::Literal(Literal::String("unassigned".to_string())),
                ],
            },
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
                "prod".to_string()
            ))),
        }))
    );
}

#[test]
fn compiles_reversed_coalesce_predicates() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE 'prod' = coalesce(service.tier, 'unassigned') \
             RETURN service.name AS service",
    )
    .expect("reversed coalesce predicate should compile");

    assert!(matches!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            operator: ComparisonOperator::Equal,
            ..
        }))
    ));
}

#[test]
fn compiles_coalesce_in_predicates() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE coalesce(service.tier, 'unassigned') IN ['prod', 'dev'] \
             RETURN service.name AS service",
    )
    .expect("coalesce IN predicate should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Coalesce {
                expressions: vec![
                    ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    }),
                    ScalarExpression::Literal(Literal::String("unassigned".to_string())),
                ],
            },
            operator: ComparisonOperator::In,
            rhs: ScalarPredicateRhs::List(vec![
                Literal::String("prod".to_string()),
                Literal::String("dev".to_string()),
            ]),
        }))
    );
}

#[test]
fn compiles_relationship_type_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN coalesce(type(owns), 'missing') AS rel_type, \
                    CASE WHEN service.tier = 'prod' THEN type(owns) ELSE 'other' END AS rel_bucket \
             ORDER BY coalesce(type(owns), 'missing')",
    )
    .expect("relationship type scalar expressions should compile");

    let [
        Projection::Expression {
            expression:
                ScalarExpression::Coalesce {
                    expressions: coalesce_expressions,
                },
            alias: coalesce_alias,
        },
        Projection::Expression {
            expression:
                ScalarExpression::Case {
                    alternatives,
                    else_expression,
                },
            alias: case_alias,
        },
    ] = plan.projections.as_slice()
    else {
        panic!("expected scalar relationship type projections");
    };
    assert_eq!(coalesce_alias, "rel_type");
    assert_eq!(
        coalesce_expressions,
        &vec![
            ScalarExpression::RelationshipType {
                variable: "owns".to_string(),
                relationship_type: "OWNS".to_string(),
            },
            ScalarExpression::Literal(Literal::String("missing".to_string())),
        ]
    );
    assert_eq!(case_alias, "rel_bucket");
    let [alternative] = alternatives.as_slice() else {
        panic!("expected one CASE alternative");
    };
    assert_eq!(
        alternative.then,
        ScalarExpression::RelationshipType {
            variable: "owns".to_string(),
            relationship_type: "OWNS".to_string(),
        }
    );
    assert_eq!(
        else_expression.as_deref(),
        Some(&ScalarExpression::Literal(Literal::String(
            "other".to_string()
        )))
    );
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Coalesce { expressions }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }] if matches!(
            expressions.as_slice(),
            [
                ScalarExpression::RelationshipType {
                    variable,
                    relationship_type,
                },
                ScalarExpression::Literal(Literal::String(fallback)),
            ] if variable == "owns" && relationship_type == "OWNS" && fallback == "missing"
        )
    ));
}

#[test]
fn compiles_identity_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN id(service) + 1 AS next_service_id, \
                    coalesce(elementId(owns), 'missing') AS ownership_element_id, \
                    CASE WHEN service.tier = 'prod' THEN id(person) ELSE 0 END AS owner_id \
             ORDER BY toString(id(service)), coalesce(elementId(owns), 'missing')",
    )
    .expect("identity scalar expressions should compile");

    let [
        Projection::Expression {
            expression:
                ScalarExpression::Arithmetic {
                    operator: ArithmeticOperator::Add,
                    left,
                    right,
                },
            alias: next_alias,
        },
        Projection::Expression {
            expression:
                ScalarExpression::Coalesce {
                    expressions: coalesce_expressions,
                },
            alias: element_alias,
        },
        Projection::Expression {
            expression:
                ScalarExpression::Case {
                    alternatives,
                    else_expression,
                },
            alias: case_alias,
        },
    ] = plan.projections.as_slice()
    else {
        panic!("expected identity scalar projections");
    };
    assert_eq!(next_alias, "next_service_id");
    assert_eq!(
        left.as_ref(),
        &ScalarExpression::Key {
            variable: "service".to_string(),
        }
    );
    assert_eq!(
        right.as_ref(),
        &ScalarExpression::Literal(Literal::Integer(1))
    );
    assert_eq!(element_alias, "ownership_element_id");
    assert_eq!(
        coalesce_expressions,
        &vec![
            ScalarExpression::ElementId {
                variable: "owns".to_string(),
            },
            ScalarExpression::Literal(Literal::String("missing".to_string())),
        ]
    );
    assert_eq!(case_alias, "owner_id");
    let [alternative] = alternatives.as_slice() else {
        panic!("expected one CASE alternative");
    };
    assert_eq!(
        alternative.then,
        ScalarExpression::Key {
            variable: "person".to_string(),
        }
    );
    assert_eq!(
        else_expression.as_deref(),
        Some(&ScalarExpression::Literal(Literal::Integer(0)))
    );
    assert!(matches!(
        plan.order_by.as_slice(),
        [
            OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::ToString { expression }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::Coalesce { expressions }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ] if matches!(expression.as_ref(), ScalarExpression::Key { variable } if variable == "service")
            && matches!(expressions.as_slice(), [
                ScalarExpression::ElementId { variable },
                ScalarExpression::Literal(Literal::String(fallback)),
            ] if variable == "owns" && fallback == "missing")
    ));
}

#[test]
fn rejects_identity_scalar_expressions_on_unbound_variables() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             RETURN coalesce(id(owner), 0) AS owner_id",
    )
    .expect_err("id() over an unbound variable should be rejected");

    assert!(
        error
            .to_string()
            .contains("id() argument 'owner' is not a bound graph variable"),
        "{error:?}"
    );
}

#[test]
fn rejects_relationship_type_scalar_expressions_on_nodes() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             RETURN coalesce(type(service), 'missing') AS relationship_type",
    )
    .expect_err("type() over a node variable should be rejected");

    assert!(
        error
            .to_string()
            .contains("type() argument 'service' is not a named relationship variable"),
        "{error:?}"
    );
}

#[test]
fn rejects_invalid_coalesce_projections() {
    for (cypher, expected) in [
        (
            "MATCH (service:Service) RETURN coalesce(service.team) AS owner_team",
            "at least two arguments",
        ),
        (
            "MATCH (service:Service) RETURN coalesce(labels(service), 'unknown') AS owner_team",
            "list-valued coalesce() requires every non-null argument to be a static list",
        ),
    ] {
        let error = compile_cypher(cypher).expect_err("query should be rejected");
        assert!(
            error.to_string().contains(expected),
            "unexpected error: {error}"
        );
    }
}

#[test]
fn rejects_ambiguous_literal_list_projections() {
    for (cypher, expected) in [
        (
            "MATCH (service:Service) RETURN [] AS values",
            "at least one element",
        ),
        (
            "MATCH (service:Service) RETURN [null] AS values",
            "at least one non-null element",
        ),
        (
            "MATCH (service:Service) RETURN [1, 'prod'] AS values",
            "all non-null elements to have the same type",
        ),
    ] {
        let error = compile_cypher(cypher).expect_err("query should be rejected");
        assert!(
            error.to_string().contains(expected),
            "unexpected error: {error}"
        );
    }
}

#[test]
fn compiles_float_literals() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE service.risk >= 0.75 AND -1.5 < service.margin \
             RETURN service.name",
    )
    .expect("query should compile");

    assert_eq!(
        plan.predicates,
        vec![
            PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                },
                operator: ComparisonOperator::GreaterThanOrEqual,
                rhs: PredicateRhs::Literal(Literal::Float(OrderedFloat(0.75_f64))),
            },
            PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "margin".to_string(),
                },
                operator: ComparisonOperator::GreaterThan,
                rhs: PredicateRhs::Literal(Literal::Float(OrderedFloat(-1.5_f64))),
            },
        ]
    );
}

#[test]
fn compiles_chained_comparisons_as_conjunctions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE 10 <= service.id < 30 \
             RETURN service.name",
    )
    .expect("query should compile");

    assert_eq!(
        plan.predicates,
        vec![
            PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "id".to_string(),
                },
                operator: ComparisonOperator::GreaterThanOrEqual,
                rhs: PredicateRhs::Literal(Literal::Integer(10)),
            },
            PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "id".to_string(),
                },
                operator: ComparisonOperator::LessThan,
                rhs: PredicateRhs::Literal(Literal::Integer(30)),
            },
        ]
    );
}

#[test]
fn compiles_in_predicates_with_literal_lists() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE service.tier IN ['prod', null, 'dev'] \
             RETURN service.name",
    )
    .expect("query should compile");

    assert_eq!(
        plan.predicates,
        vec![PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            },
            operator: ComparisonOperator::In,
            rhs: PredicateRhs::List(vec![
                Literal::String("prod".to_string()),
                Literal::Null,
                Literal::String("dev".to_string()),
            ]),
        }]
    );
}

#[test]
fn compiles_bound_cypher_parameters() {
    let parameters = BTreeMap::from([
        (
            "tier".to_string(),
            CypherParameterValue::Literal(Literal::String("prod".to_string())),
        ),
        (
            "ids".to_string(),
            CypherParameterValue::List(vec![Literal::Integer(10), Literal::Integer(40)]),
        ),
        (
            "limit".to_string(),
            CypherParameterValue::Literal(Literal::Integer(2)),
        ),
    ]);
    let plan = compile_cypher_with_parameters(
        "MATCH (service:Service {tier: $tier}) \
             WHERE service.id IN $ids \
             RETURN service.name \
             LIMIT $limit",
        &parameters,
    )
    .expect("parameterized query should compile");

    assert_eq!(
        plan.predicates,
        vec![
            PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
            },
            PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "id".to_string(),
                },
                operator: ComparisonOperator::In,
                rhs: PredicateRhs::List(vec![Literal::Integer(10), Literal::Integer(40)]),
            },
        ]
    );
    assert_eq!(plan.limit, Some(2));
}

#[test]
fn compiles_string_predicates() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE service.name STARTS WITH 'bill' \
                AND service.name ENDS WITH 'api' \
                AND service.name CONTAINS 'ing' \
             RETURN service.name",
    )
    .expect("query should compile");

    assert_eq!(
        plan.predicates
            .iter()
            .map(|predicate| predicate.operator)
            .collect::<Vec<_>>(),
        vec![
            ComparisonOperator::StartsWith,
            ComparisonOperator::EndsWith,
            ComparisonOperator::Contains,
        ]
    );
}

#[test]
fn compiles_dynamic_string_predicate_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE service.name STARTS WITH left(service.name, 4) \
                AND service.name ENDS WITH right(service.name, 3) \
                AND service.name CONTAINS substring(service.name, 1, 3) \
             RETURN service.name",
    )
    .expect("dynamic string predicates should compile");

    assert!(plan.predicates.is_empty());
    assert!(matches!(
        &plan.predicate,
        Some(PredicateExpression::And { left, right })
            if matches!(
                left.as_ref(),
                PredicateExpression::And { left, right }
                    if matches!(
                        left.as_ref(),
                        PredicateExpression::ScalarComparison(ScalarPredicate {
                            lhs: ScalarExpression::Property(PropertyRef { property, .. }),
                            operator: ComparisonOperator::StartsWith,
                            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Left { .. }),
                        }) if property == "name"
                    ) && matches!(
                        right.as_ref(),
                        PredicateExpression::ScalarComparison(ScalarPredicate {
                            operator: ComparisonOperator::EndsWith,
                            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Right { .. }),
                            ..
                        })
                    )
            ) && matches!(
                right.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    operator: ComparisonOperator::Contains,
                    rhs: ScalarPredicateRhs::Expression(ScalarExpression::Substring { .. }),
                    ..
                })
            )
    ));
}

#[test]
fn compiles_or_predicates_as_boolean_expression_tree() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE service.tier = 'prod' OR service.tier IS NULL \
             RETURN service.name",
    )
    .expect("query should compile");

    assert!(plan.predicates.is_empty());
    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::Or {
            left: Box::new(PredicateExpression::Comparison(PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
            })),
            right: Box::new(PredicateExpression::Comparison(PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::Null),
            })),
        })
    );
}

#[test]
fn compiles_not_predicates_as_boolean_expression_tree() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE NOT (service.tier = 'prod') \
             RETURN service.name",
    )
    .expect("query should compile");

    assert!(plan.predicates.is_empty());
    assert!(matches!(
        plan.predicate,
        Some(PredicateExpression::Not { .. })
    ));
}

#[test]
fn compiles_bare_boolean_property_predicates() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE service.active \
             RETURN service.name",
    )
    .expect("bare boolean property query should compile");

    assert_eq!(
        plan.predicates,
        vec![PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "active".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Literal(Literal::Boolean(true)),
        }]
    );

    let negated = compile_cypher(
        "MATCH (service:Service) \
             WHERE NOT service.active \
             RETURN service.name",
    )
    .expect("negated bare boolean property query should compile");
    assert!(matches!(
        negated.predicate,
        Some(PredicateExpression::Not { .. })
    ));
}

#[test]
fn compiles_constant_boolean_predicates() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE true \
             RETURN service.name",
    )
    .expect("constant true predicate query should compile");

    assert!(plan.predicates.is_empty());
    assert_eq!(plan.predicate, Some(PredicateExpression::Boolean(true)));

    let combined = compile_cypher(
        "MATCH (service:Service) \
             WHERE service.active OR false \
             RETURN service.name",
    )
    .expect("constant false predicate expression query should compile");
    assert!(matches!(
        combined.predicate,
        Some(PredicateExpression::Or { .. })
    ));
}

#[test]
fn compiles_literal_only_predicates() {
    for (cypher, expected) in [
        (
            "MATCH (service:Service) WHERE 1 = 1 RETURN service.name",
            true,
        ),
        (
            "MATCH (service:Service) WHERE 5 > 3 RETURN service.name",
            true,
        ),
        (
            "MATCH (service:Service) WHERE 1 = 1.0 RETURN service.name",
            true,
        ),
        (
            "MATCH (service:Service) WHERE (1 + 2) * 3 = 9 RETURN service.name",
            true,
        ),
        (
            "MATCH (service:Service) WHERE toLower('PROD') = 'prod' RETURN service.name",
            true,
        ),
        (
            "MATCH (service:Service) WHERE trim(' prod ') = 'prod' RETURN service.name",
            true,
        ),
        (
            "MATCH (service:Service) WHERE size('abc') = 3 RETURN service.name",
            true,
        ),
        (
            "MATCH (service:Service) WHERE coalesce(null, 'prod') = 'prod' RETURN service.name",
            true,
        ),
        (
            "MATCH (service:Service) WHERE null IS NULL RETURN service.name",
            true,
        ),
        (
            "MATCH (service:Service) WHERE trim(' prod ') IS NOT NULL RETURN service.name",
            true,
        ),
        (
            "MATCH (service:Service) WHERE nullIf('prod', 'prod') IS NULL RETURN service.name",
            true,
        ),
        (
            "MATCH (service:Service) WHERE 'prod' IN ['dev', 'prod', null] RETURN service.name",
            true,
        ),
        (
            "MATCH (service:Service) WHERE toLower('PROD') IN ['dev', 'prod'] RETURN service.name",
            true,
        ),
        (
            "MATCH (service:Service) WHERE 'stage' IN ['dev', 'prod'] RETURN service.name",
            false,
        ),
        (
            "MATCH (service:Service) WHERE 'prod' IN ['dev', null] RETURN service.name",
            false,
        ),
        (
            "MATCH (service:Service) WHERE replace('billing-api', '-', '') = 'billing-api' RETURN service.name",
            false,
        ),
        (
            "MATCH (service:Service) WHERE nullIf('prod', 'prod') IS NOT NULL RETURN service.name",
            false,
        ),
    ] {
        let plan = compile_cypher(cypher).expect("literal-only predicate should compile");
        assert_eq!(plan.predicate, Some(PredicateExpression::Boolean(expected)));
    }

    let parameters = BTreeMap::from([(
        "enabled".to_string(),
        CypherParameterValue::Literal(Literal::Boolean(true)),
    )]);
    let plan = compile_cypher_with_parameters(
        "MATCH (service:Service) WHERE $enabled = true RETURN service.name",
        &parameters,
    )
    .expect("parameterized literal-only predicate should compile");
    assert_eq!(plan.predicate, Some(PredicateExpression::Boolean(true)));
}

#[test]
fn rejects_unsafe_literal_only_predicates() {
    for (cypher, expected) in [
        (
            "MATCH (service:Service) WHERE null = null RETURN service.name",
            "literal-only null comparisons",
        ),
        (
            "MATCH (service:Service) WHERE null IN ['prod'] RETURN service.name",
            "null left-hand side",
        ),
    ] {
        let error = compile_cypher(cypher).expect_err("query should be rejected");
        assert!(
            error.to_string().contains(expected),
            "unexpected error: {error}"
        );
    }
}

#[test]
fn preserves_parenthesized_boolean_precedence() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE service.team = 'platform' AND (service.tier = 'prod' OR service.tier IS NULL) \
             RETURN service.name",
    )
    .expect("query should compile");

    assert!(plan.predicates.is_empty());
    assert!(matches!(
        plan.predicate,
        Some(PredicateExpression::And { .. })
    ));
}

#[test]
fn combines_inline_property_maps_with_boolean_where_tree() {
    let plan = compile_cypher(
        "MATCH (service:Service {team: 'platform'}) \
             WHERE service.tier = 'prod' OR service.tier IS NULL \
             RETURN service.name",
    )
    .expect("query should compile");

    assert_eq!(plan.predicates.len(), 1);
    assert!(matches!(
        plan.predicate,
        Some(PredicateExpression::Or { .. })
    ));
}

#[test]
fn compiles_count_star_projection() {
    let plan = compile_cypher("MATCH (service:Service) RETURN count(*) AS services")
        .expect("query should compile");

    assert_eq!(
        plan.projections,
        vec![Projection::CountAll {
            alias: "services".to_string(),
        }]
    );
}

#[test]
fn compiles_return_star_with_graph_declaration() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[ownership:OWNS]->(service:Service) \
             RETURN * ORDER BY service.name",
    )
    .expect("RETURN * should expand using graph metadata");

    assert_eq!(
        plan.projection_output_names(),
        vec![
            "person.__id",
            "person.__labels",
            "person.name",
            "person.team",
            "service.__id",
            "service.__labels",
            "service.name",
            "service.tier",
            "ownership.__id",
            "ownership.__type",
            "ownership.since",
            "ownership.source",
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_return_star_with_explicit_projections() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[ownership:OWNS]->(service:Service) \
             RETURN *, service.tier AS tier \
             ORDER BY tier",
    )
    .expect("RETURN *, explicit projections should compile");

    assert_eq!(
        plan.projection_output_names(),
        vec![
            "person.__id",
            "person.__labels",
            "person.name",
            "person.team",
            "service.__id",
            "service.__labels",
            "service.name",
            "service.tier",
            "ownership.__id",
            "ownership.__type",
            "ownership.since",
            "ownership.source",
            "tier",
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_return_node_variable_with_graph_declaration() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) RETURN service ORDER BY service.name",
    )
    .expect("node graph variable return should expand using graph metadata");

    assert_eq!(
        plan.projection_output_names(),
        vec![
            "service.__id",
            "service.__labels",
            "service.name",
            "service.tier",
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_return_graph_variable_alias_prefix() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(&graph, "MATCH (service:Service) RETURN service AS svc")
        .expect("graph variable aliases should prefix expanded columns");

    assert_eq!(
        plan.projection_output_names(),
        vec!["svc.__id", "svc.__labels", "svc.name", "svc.tier"]
    );
}

#[test]
fn compiles_return_relationship_variable_with_graph_declaration() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (:Person)-[ownership:OWNS]->(:Service) RETURN ownership",
    )
    .expect("relationship graph variable return should expand using graph metadata");

    assert_eq!(
        plan.projection_output_names(),
        vec![
            "ownership.__id",
            "ownership.__type",
            "ownership.since",
            "ownership.source",
        ]
    );
}

#[test]
fn compiles_return_relationship_endpoint_graph_values_with_graph_declaration() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[ownership:OWNS]->(service:Service) \
             RETURN startNode(ownership) AS owner, endNode(ownership) AS owned",
    )
    .expect("relationship endpoint graph values should expand using graph metadata");

    assert_eq!(
        plan.projection_output_names(),
        vec![
            "owner.__id",
            "owner.__labels",
            "owner.name",
            "owner.team",
            "owned.__id",
            "owned.__labels",
            "owned.name",
            "owned.tier",
        ]
    );
    assert_eq!(
        plan.projections.first(),
        Some(&Projection::Expression {
            expression: ScalarExpression::Key {
                variable: "person".to_string(),
            },
            alias: "owner.__id".to_string(),
        })
    );
}

#[test]
fn rejects_return_graph_variable_without_graph_declaration() {
    let error = compile_cypher("MATCH (service:Service) RETURN service")
        .expect_err("declaration-free compiler cannot expand graph variables");

    assert!(
        error
            .to_string()
            .contains("graph-variable expansion requires a graph declaration"),
        "{error}"
    );
}

#[test]
fn rejects_return_path_variable_as_graph_variable() {
    let graph = star_test_graph();
    let error = compile_cypher_for_graph(
        &graph,
        "MATCH path = (person:Person)-[ownership:OWNS]->(service:Service) RETURN path",
    )
    .expect_err("path graph variable returns should be rejected");

    assert!(
        error
            .to_string()
            .contains("path variable 'path' cannot be used as a graph value"),
        "{error}"
    );
}

#[test]
fn compiles_return_star_over_keyless_relationships() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (team:Team)-[ownership:OWNS]->(service:Service) RETURN *",
    )
    .expect("RETURN * should handle keyless relationship mappings");

    assert_eq!(
        plan.projection_output_names(),
        vec![
            "team.__id",
            "team.__labels",
            "team.name",
            "service.__id",
            "service.__labels",
            "service.name",
            "service.tier",
            "ownership.__type",
            "ownership.source",
        ]
    );
}

#[test]
fn rejects_return_star_without_graph_declaration() {
    let error = compile_cypher("MATCH (service:Service) RETURN *")
        .expect_err("declaration-free compiler cannot expand RETURN *");

    assert!(
        error
            .to_string()
            .contains("graph-variable expansion requires a graph declaration"),
        "{error}"
    );
}

#[test]
fn rejects_return_star_over_path_variables() {
    let graph = star_test_graph();
    let error = compile_cypher_for_graph(
        &graph,
        "MATCH path = (person:Person)-[ownership:OWNS]->(service:Service) RETURN *",
    )
    .expect_err("RETURN * should reject unmaterialized path values");

    assert!(
        error
            .to_string()
            .contains("RETURN * cannot carry path variables"),
        "{error}"
    );
}

#[test]
fn return_star_respects_transparent_with_scope() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[ownership:OWNS]->(service:Service) \
             WITH service \
             RETURN *",
    )
    .expect("RETURN * should only expand visible variables after WITH");

    assert_eq!(
        plan.projection_output_names(),
        vec![
            "service.__id",
            "service.__labels",
            "service.name",
            "service.tier",
        ]
    );
}

#[test]
fn compiles_return_distinct() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[:OWNS]->(service:Service) \
             RETURN DISTINCT service.tier AS tier ORDER BY tier",
    )
    .expect("query should compile");

    assert!(plan.distinct);
    assert_eq!(plan.projections.len(), 1);
    assert_eq!(plan.order_by.len(), 1);
}

#[test]
fn compiles_skip_and_limit() {
    let plan = compile_cypher(
        "MATCH (service:Service) RETURN service.name AS service ORDER BY service SKIP 1 LIMIT 2",
    )
    .expect("query should compile");

    assert_eq!(plan.skip, Some(1));
    assert_eq!(plan.limit, Some(2));
}

#[test]
fn compiles_static_skip_and_limit_expressions() {
    let parameters = BTreeMap::from([(
        "limit".to_string(),
        CypherParameterValue::Literal(Literal::Integer(2)),
    )]);
    let plan = compile_cypher_with_parameters(
        "MATCH (service:Service) \
             RETURN service.name AS service \
             ORDER BY service \
             SKIP (1 + 1) \
             LIMIT coalesce($limit, 3)",
        &parameters,
    )
    .expect("static row modifier expressions should compile");

    assert_eq!(plan.skip, Some(2));
    assert_eq!(plan.limit, Some(2));
}

#[test]
fn rejects_negative_skip() {
    let error = compile_cypher("MATCH (service:Service) RETURN service.name SKIP -1")
        .expect_err("negative SKIP should fail");

    assert!(
        error.to_string().contains("UNSUPPORTED_CYPHER"),
        "{error:?}"
    );
}

#[test]
fn compiles_inline_node_property_maps_as_predicates() {
    let plan =
        compile_cypher("MATCH (service:Service {tier: 'prod', active: true}) RETURN service.name")
            .expect("query should compile");

    assert_eq!(
        plan.predicates,
        vec![
            PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
            },
            PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "active".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::Boolean(true)),
            },
        ]
    );
}

#[test]
fn compiles_named_inline_relationship_property_maps_as_predicates() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[ownership:OWNS {source: 'catalog'}]->(service:Service) \
             RETURN service.name",
    )
    .expect("query should compile");

    assert_eq!(
        plan.relationships,
        vec![RelationshipPattern {
            variable: Some("ownership".to_string()),
            relationship_type: "OWNS".to_string(),
            left: "person".to_string(),
            direction: Direction::Outgoing,
            right: "service".to_string(),
        }]
    );
    assert_eq!(
        plan.predicates,
        vec![PropertyPredicate {
            property: PropertyRef {
                variable: "ownership".to_string(),
                property: "source".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Literal(Literal::String("catalog".to_string())),
        }]
    );
}

#[test]
fn compiles_parameterized_inline_property_maps_as_predicates() {
    let parameters = BTreeMap::from([
        (
            "source".to_string(),
            CypherParameterValue::Literal(Literal::String("catalog".to_string())),
        ),
        (
            "active".to_string(),
            CypherParameterValue::Literal(Literal::Boolean(true)),
        ),
    ]);
    let plan = compile_cypher_with_parameters(
            "MATCH (person:Person)-[ownership:OWNS {source: $source}]->(service:Service {active: $active}) \
             RETURN service.name",
            &parameters,
        )
        .expect("parameterized inline property maps should compile");

    assert_eq!(
        plan.predicates,
        vec![
            PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "active".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::Boolean(true)),
            },
            PropertyPredicate {
                property: PropertyRef {
                    variable: "ownership".to_string(),
                    property: "source".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("catalog".to_string())),
            },
        ]
    );
}

#[test]
fn compiles_inline_property_maps_with_scalar_alias_values() {
    let plan = compile_cypher(
        "MATCH (source:Service) \
             WITH source.name AS source_name \
             MATCH (matched:Service {name: source_name}) \
             RETURN matched.name",
    )
    .expect("inline node property map should accept property-backed scalar alias values");

    let predicate = plan
        .predicates
        .first()
        .expect("inline property predicate should exist");
    assert_eq!(predicate.property.variable, "matched");
    assert_eq!(predicate.property.property, "name");
    let PredicateRhs::Property(property) = &predicate.rhs else {
        panic!("expected property-backed scalar alias RHS, got {predicate:?}");
    };
    assert!(property.variable.starts_with("__coral_hidden_source"));
    assert_eq!(property.property, "name");
}

#[test]
fn compiles_inline_relationship_property_maps_with_scalar_alias_values() {
    let plan = compile_cypher(
        "MATCH (team:Team)-[ownership:OWNS]->(service:Service) \
             WITH service, ownership.source AS source_filter \
             MATCH (service)-[dependency:DEPENDS_ON {source: source_filter}]->(target:Service) \
             RETURN target.name",
    )
    .expect("inline relationship property map should accept property-backed scalar aliases");

    let predicate = plan
        .predicates
        .iter()
        .find(|predicate| predicate.property.variable == "dependency")
        .expect("dependency inline property predicate should exist");
    assert_eq!(predicate.property.property, "source");
    let PredicateRhs::Property(property) = &predicate.rhs else {
        panic!("expected property-backed scalar alias RHS, got {predicate:?}");
    };
    assert!(property.variable.starts_with("__coral_hidden_ownership"));
    assert_eq!(property.property, "source");
}

#[test]
fn compiles_inline_node_property_maps_with_property_expression_values() {
    let cypher = "MATCH (source:Service) \
             MATCH (matched:Service {team: source.team}) \
             RETURN matched.name";
    let plan = compile_cypher(cypher)
        .expect("inline node property map should accept property expression values");

    let predicate = plan
        .predicates
        .first()
        .expect("inline property predicate should exist");
    assert_eq!(predicate.property.variable, "matched");
    assert_eq!(predicate.property.property, "team");
    assert_eq!(
        predicate.rhs,
        PredicateRhs::Property(PropertyRef {
            variable: "source".to_string(),
            property: "team".to_string(),
        })
    );
}

#[test]
fn compiles_inline_relationship_property_maps_with_property_expression_values() {
    let plan = compile_cypher(
        "MATCH (team:Team)-[ownership:OWNS]->(service:Service) \
             MATCH (service)-[dependency:DEPENDS_ON {source: ownership.source}]->(target:Service) \
             RETURN target.name",
    )
    .expect("inline relationship property map should accept property expression values");

    let predicate = plan
        .predicates
        .iter()
        .find(|predicate| predicate.property.variable == "dependency")
        .expect("dependency inline property predicate should exist");
    assert_eq!(predicate.property.property, "source");
    assert_eq!(
        predicate.rhs,
        PredicateRhs::Property(PropertyRef {
            variable: "ownership".to_string(),
            property: "source".to_string(),
        })
    );
}

#[test]
fn compiles_inline_property_maps_with_identity_expression_values() {
    let plan = compile_cypher(
        "MATCH (source:Service) \
             MATCH (same_key:Service {name: id(source)}) \
             MATCH (same_element:Service {name: elementId(source)}) \
             RETURN same_key.name, same_element.name",
    )
    .expect("inline property maps should accept id and elementId expression values");

    assert_eq!(
        plan.predicates,
        vec![
            PropertyPredicate {
                property: PropertyRef {
                    variable: "same_key".to_string(),
                    property: "name".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Key {
                    variable: "source".to_string(),
                },
            },
            PropertyPredicate {
                property: PropertyRef {
                    variable: "same_element".to_string(),
                    property: "name".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::ElementId {
                    variable: "source".to_string(),
                },
            },
        ]
    );
}

#[test]
fn rejects_inline_property_maps_with_expression_scalar_alias_values() {
    let error = compile_cypher(
        "MATCH (source:Service) \
             WITH toUpper(source.name) AS source_name \
             MATCH (matched:Service {name: source_name}) \
             RETURN matched.name",
    )
    .expect_err("inline property maps should reject expression-backed scalar aliases");

    assert!(
        error
            .to_string()
            .contains("inline property maps can only use WITH scalar aliases"),
        "{error}"
    );
}

#[test]
fn compiles_anonymous_inline_relationship_property_maps_with_internal_variable() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[:OWNS {source: 'catalog'}]->(service:Service) \
             RETURN service.name",
    )
    .expect("query should compile");
    let relationship = plan
        .relationships
        .first()
        .expect("query should contain a relationship");
    let internal_variable = relationship
        .variable
        .as_ref()
        .expect("anonymous property map relationship should get an internal variable");

    assert!(
        internal_variable.starts_with("__coral_rel_"),
        "{internal_variable}"
    );
    assert_eq!(
        plan.predicates,
        vec![PropertyPredicate {
            property: PropertyRef {
                variable: internal_variable.clone(),
                property: "source".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Literal(Literal::String("catalog".to_string())),
        }]
    );
}

#[test]
fn compiles_order_by_property_projection_aliases() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN service.name AS service_name \
             ORDER BY service_name DESC",
    )
    .expect("query should compile");

    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            }),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_is_null_predicates() {
    let plan =
        compile_cypher("MATCH (service:Service) WHERE service.tier IS NULL RETURN service.name")
            .expect("query should compile");

    assert_eq!(
        plan.predicates,
        vec![PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Literal(Literal::Null),
        }]
    );
}

#[test]
fn compiles_graph_variable_null_predicates() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE person IS NOT NULL AND owns IS NULL \
             RETURN person.name AS owner",
    )
    .expect("graph variable null predicates should compile");

    assert!(plan.predicates.is_empty());
    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::Presence(PresencePredicate {
                variable: "person".to_string(),
                operator: ComparisonOperator::NotEqual,
            })),
            right: Box::new(PredicateExpression::Presence(PresencePredicate {
                variable: "owns".to_string(),
                operator: ComparisonOperator::Equal,
            })),
        })
    );
}

#[test]
fn compiles_exists_property_predicates() {
    let plan =
        compile_cypher("MATCH (service:Service) WHERE exists(service.tier) RETURN service.name")
            .expect("exists property query should compile");

    assert_eq!(
        plan.predicates,
        vec![PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            },
            operator: ComparisonOperator::NotEqual,
            rhs: PredicateRhs::Literal(Literal::Null),
        }]
    );

    let negated = compile_cypher(
        "MATCH (service:Service) WHERE NOT exists(service.tier) RETURN service.name",
    )
    .expect("negated exists property query should compile");
    assert!(matches!(
        negated.predicate,
        Some(PredicateExpression::Not { .. })
    ));
}

#[test]
fn rejects_exists_without_single_property_argument() {
    assert_unsupported("MATCH (service:Service) WHERE exists() RETURN service.name");
    assert_unsupported("MATCH (service:Service) WHERE exists(1) RETURN service.name");
    assert_unsupported("MATCH (service:Service) WHERE exists(service) RETURN service.name");
}

#[test]
fn compiles_anchored_optional_match() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN service.name AS service, person.name AS owner",
    )
    .expect("anchored OPTIONAL MATCH should compile");

    assert_eq!(plan.optional_relationships, vec![0]);
    assert_eq!(
        plan.optional_matches,
        vec![OptionalMatchScope {
            node_indices: vec![1],
            relationship_indices: vec![0],
            predicate: None,
        }]
    );
    assert_eq!(
        plan.nodes,
        vec![
            NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "person".to_string(),
                label: "Person".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "OWNS".to_string(),
            left: "person".to_string(),
            direction: Direction::Outgoing,
            right: "service".to_string(),
        }]
    );
}

#[test]
fn compiles_match_after_optional_when_it_uses_only_mandatory_bindings() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH (service)-[:DEPENDS_ON]->(dependency:Service) \
             MATCH (owner:Person)-[:OWNS]->(service) \
             RETURN service.name AS service, dependency.name AS dependency, owner.name AS owner",
    )
    .expect("MATCH after OPTIONAL MATCH should compile when it avoids optional bindings");

    assert_eq!(plan.optional_relationships, vec![0]);
    assert_eq!(
        plan.optional_matches,
        vec![OptionalMatchScope {
            node_indices: vec![1],
            relationship_indices: vec![0],
            predicate: None,
        }]
    );
    assert_eq!(
        plan.relationships,
        vec![
            RelationshipPattern {
                variable: None,
                relationship_type: "DEPENDS_ON".to_string(),
                left: "service".to_string(),
                direction: Direction::Outgoing,
                right: "dependency".to_string(),
            },
            RelationshipPattern {
                variable: None,
                relationship_type: "OWNS".to_string(),
                left: "owner".to_string(),
                direction: Direction::Outgoing,
                right: "service".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_match_after_optional_when_it_uses_optional_bindings() {
    let dependent_match = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH (service)-[:DEPENDS_ON]->(target:Service) \
             MATCH (target)-[:DEPENDS_ON]->(next:Service) \
             RETURN service.name AS service, target.name AS target, next.name AS next",
    )
    .expect("MATCH after OPTIONAL MATCH should compile when it depends on optional bindings");

    assert_eq!(dependent_match.optional_relationships, vec![0]);
    assert_eq!(
        dependent_match.optional_matches,
        vec![OptionalMatchScope {
            node_indices: vec![1],
            relationship_indices: vec![0],
            predicate: None,
        }]
    );
    assert_eq!(
        dependent_match.relationships,
        vec![
            RelationshipPattern {
                variable: None,
                relationship_type: "DEPENDS_ON".to_string(),
                left: "service".to_string(),
                direction: Direction::Outgoing,
                right: "target".to_string(),
            },
            RelationshipPattern {
                variable: None,
                relationship_type: "DEPENDS_ON".to_string(),
                left: "target".to_string(),
                direction: Direction::Outgoing,
                right: "next".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_optional_fixed_length_relationship_ranges() {
    for cypher in [
        "MATCH (source:Service) OPTIONAL MATCH (source)-[:DEPENDS_ON*2]->(target:Service) RETURN target.name",
        "MATCH (source:Service) OPTIONAL MATCH (source)-[:DEPENDS_ON*2..2]->(target:Service) RETURN target.name",
        "MATCH (source:Service) OPTIONAL MATCH (source)-[:DEPENDS_ON]->{2}(target:Service) RETURN target.name",
    ] {
        let plan = compile_cypher(cypher)
            .expect("exact positive OPTIONAL MATCH relationship range should compile");

        assert_eq!(plan.optional_relationships, vec![0, 1]);
        assert_eq!(
            plan.optional_matches,
            vec![OptionalMatchScope {
                node_indices: vec![1, 2],
                relationship_indices: vec![0, 1],
                predicate: None,
            }]
        );
    }
}

#[test]
fn compiles_optional_zero_hop_relationship_ranges_for_same_label_endpoints() {
    let plan = compile_cypher(
        "MATCH (source:Service) \
             OPTIONAL MATCH (source)-[:DEPENDS_ON*0]->(target:Service) \
             RETURN source.name AS source, target.name AS target",
    )
    .expect("same-label optional zero-hop relationship range should compile");

    assert!(plan.optional_relationships.is_empty());
    assert!(plan.optional_matches.is_empty());
    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::KeyComparison(KeyPredicate {
            variable: "source".to_string(),
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Key {
                variable: "target".to_string(),
            },
        }))
    );
}

#[test]
fn compiles_optional_zero_hop_relationship_ranges_with_bound_endpoints_as_row_preserving() {
    let same_label = compile_cypher(
        "MATCH (source:Service), (target:Service) \
             OPTIONAL MATCH (source)-[:DEPENDS_ON*0]->(target) \
             RETURN source.name AS source, target.name AS target",
    )
    .expect("optional zero-hop with bound same-label endpoints should compile");

    assert!(
        same_label.predicate.is_none(),
        "already-bound optional zero-hop endpoints must not filter rows: {:?}",
        same_label.predicate
    );

    let cross_label = compile_cypher(
        "MATCH (source:Service), (person:Person) \
             OPTIONAL MATCH (source)-[:DEPENDS_ON*0]->(person) \
             RETURN source.name AS source, person.name AS person",
    )
    .expect("optional zero-hop with bound cross-label endpoints should compile");

    assert!(
        cross_label.predicate.is_none(),
        "already-bound optional zero-hop cross-label endpoints must not filter rows: {:?}",
        cross_label.predicate
    );
}

#[test]
fn rejects_optional_zero_hop_relationship_ranges_requiring_nullable_bindings() {
    assert_unsupported(
        "MATCH (person:Person) \
             OPTIONAL MATCH (person)-[:OWNS*0]->(service:Service) \
             RETURN service.name",
    );
}

#[test]
fn compiles_optional_match_local_predicates() {
    let plan = compile_cypher(
            "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person {active: true})-[owns:OWNS {source: 'pagerduty'}]->(service) \
             WHERE person.team = service.team AND id(owns) > 10 \
             RETURN service.name AS service, person.name AS owner",
        )
        .expect("OPTIONAL MATCH predicates should compile");

    assert_eq!(plan.optional_relationships, vec![0]);
    assert_eq!(plan.predicates, Vec::new());
    assert_eq!(plan.optional_matches.len(), 1);
    let optional_match = plan
        .optional_matches
        .first()
        .expect("optional match scope should be present");
    assert_eq!(optional_match.relationship_indices, vec![0]);
    assert!(matches!(
        &optional_match.predicate,
        Some(PredicateExpression::And { .. })
    ));
}

#[test]
fn compiles_undirected_optional_match_local_predicates() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH (service)-[dependency_edge:DEPENDS_ON]-(dependency:Service) \
             WHERE dependency.tier = 'dev' \
             RETURN service.name AS service, dependency.name AS dependency",
    )
    .expect("undirected OPTIONAL MATCH predicate should compile");

    assert_eq!(plan.optional_relationships, vec![0]);
    assert_eq!(plan.predicates, Vec::new());
    assert_eq!(plan.optional_matches.len(), 1);
    let optional_match = plan
        .optional_matches
        .first()
        .expect("optional match scope should be present");
    assert_eq!(optional_match.relationship_indices, vec![0]);
    assert!(optional_match.predicate.is_some());
    let relationship = plan
        .relationships
        .first()
        .expect("optional relationship should be present");
    assert_eq!(relationship.direction, Direction::Undirected);
}

#[test]
fn compiles_relationship_endpoint_properties_on_optional_relationships() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH (service)-[dependency:DEPENDS_ON]->(dependency_service:Service) \
             RETURN service.name AS service, endNode(dependency).name AS dependency \
             ORDER BY endNode(dependency).name",
    )
    .expect("relationship endpoint properties over optional relationships should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("service".to_string()),
            },
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "dependency".to_string(),
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "dependency_service".to_string(),
                        property: "name".to_string(),
                    })),
                },
                alias: "dependency".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::PresenceGated {
                presence_variable: "dependency".to_string(),
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "dependency_service".to_string(),
                    property: "name".to_string(),
                })),
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn rejects_unsupported_optional_match_shapes() {
    assert_unsupported("OPTIONAL MATCH (service:Service) RETURN service.name");
}

#[test]
fn rejects_non_transparent_with_boundaries() {
    assert_unsupported("MATCH (service:Service) WITH DISTINCT service RETURN service.name");
    assert_unsupported("MATCH (service:Service) WITH *, service.name AS name RETURN *");
    assert_unsupported(
        "MATCH (service:Service) WITH service LIMIT 1 MATCH (service)-[:DEPENDS_ON]->(target:Service) RETURN target.name",
    );
    assert_unsupported(
        "MATCH (person:Person)-[:OWNS]->(service:Service) WITH service RETURN person.name",
    );
    assert_unsupported(
        "MATCH (service:Service) WITH service, count(*) AS services MATCH (service)-[:DEPENDS_ON]->(target:Service) RETURN services, target.name",
    );
    assert_unsupported(
        "MATCH (service:Service) WITH service.name AS target MATCH (target:Service) RETURN target.name",
    );
    assert_unsupported(
        "MATCH (service:Service) WITH service, service.name AS name, service.tier AS name MATCH (service)-[:DEPENDS_ON]->(target:Service) RETURN name, target.name",
    );
    assert_unsupported(
        "MATCH (service:Service) WITH *, service AS copy MATCH (service)-[:DEPENDS_ON]->(target:Service) RETURN copy.name",
    );
    assert_unsupported(
        "MATCH (service:Service) WITH *, service.name AS service MATCH (service)-[:DEPENDS_ON]->(target:Service) RETURN service",
    );
    assert_unsupported(
        "MATCH (service:Service) WITH *, count(*) AS services MATCH (service)-[:DEPENDS_ON]->(target:Service) RETURN services, target.name",
    );
    assert_unsupported(
        "MATCH path = (person:Person)-[:OWNS]->(service:Service) WITH *, path AS p MATCH (service)-[:DEPENDS_ON]->(target:Service) RETURN p",
    );
}

#[test]
fn rejects_terminal_with_projection_boundaries_requiring_staging() {
    assert_unsupported("MATCH (service:Service) WITH service.name RETURN service.name");
    assert_unsupported("MATCH (service:Service) WITH service AS renamed RETURN renamed");
    assert_unsupported("MATCH (service:Service) WITH service.name AS service RETURN missing");
    assert_unsupported(
        "MATCH (service:Service) WITH service.name AS value, service.tier AS value RETURN value",
    );
    assert_unsupported(
        "MATCH (service:Service) WITH service.name AS name, service.tier AS tier RETURN name, name",
    );
    assert_unsupported(
        "MATCH (service:Service) WITH service.name AS name, service.tier AS tier RETURN name",
    );
    assert_unsupported(
        "MATCH (service:Service) WITH service.name AS service MATCH (service)-[:DEPENDS_ON]->(target:Service) RETURN service, target.name",
    );
    assert_unsupported(
        "MATCH (service:Service) WITH service.name AS service RETURN service ORDER BY service.name",
    );
    assert_unsupported("MATCH (service:Service) WITH service.name AS service RETURN *, service");
    assert_unsupported(
        "MATCH (service:Service) WITH service.name AS service ORDER BY service RETURN service ORDER BY service",
    );
}

#[test]
fn rejects_write_queries() {
    assert_unsupported("CREATE (service:Service) RETURN service");
}

#[test]
fn rejects_variable_length_relationships() {
    for cypher in [
        "MATCH (a:Service)-[:DEPENDS_ON*]->(b:Service) RETURN a.name",
        "MATCH (a:Service)-[:DEPENDS_ON*..3]->(b:Service) RETURN a.name",
        "MATCH (a:Service)-[:DEPENDS_ON]->{1,}(b:Service) RETURN a.name",
        "MATCH (a:Service)-[:DEPENDS_ON*9..9]->(b:Service) RETURN a.name",
        "MATCH (a:Service) OPTIONAL MATCH (a)-[:DEPENDS_ON*1..2]->(b:Service) RETURN a.name",
        "MATCH (a:Service)-[:DEPENDS_ON*2]->(b:Person) RETURN a.name",
        "MATCH (a:Service)-[r:DEPENDS_ON*0]->(b:Service) RETURN a.name",
        "MATCH (a:Service)-[r:DEPENDS_ON]->{0,1}(b:Service) RETURN a.name",
        "MATCH (a:Service)-[r:DEPENDS_ON*2]->(b:Service) RETURN a.name",
    ] {
        assert_unsupported(cypher);
    }
}

#[test]
fn compiles_cross_label_fixed_relationship_ranges_from_graph_declaration() {
    let graph = route_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH path = (person:Person)-[:ROUTES*2]->(incident:Incident) \
             RETURN person.name AS person, incident.title AS incident, length(path) AS hops",
    )
    .expect("cross-label fixed-hop path should compile from declaration metadata");

    let service = plan
        .nodes
        .iter()
        .find(|node| node.label == "Service")
        .expect("intermediate Service node should be inferred");
    assert_eq!(plan.relationships.len(), 2);
    let first_relationship = plan.relationships.first().expect("first relationship");
    let second_relationship = plan.relationships.get(1).expect("second relationship");
    assert_eq!(
        first_relationship,
        &RelationshipPattern {
            variable: None,
            relationship_type: "ROUTES".to_string(),
            left: "person".to_string(),
            direction: Direction::Outgoing,
            right: service.variable.clone(),
        }
    );
    assert_eq!(
        second_relationship,
        &RelationshipPattern {
            variable: None,
            relationship_type: "ROUTES".to_string(),
            left: service.variable.clone(),
            direction: Direction::Outgoing,
            right: "incident".to_string(),
        }
    );
    assert_eq!(path_length_projection_literal(&plan), Some(2));
}

#[test]
fn compiles_incoming_cross_label_fixed_relationship_ranges_from_graph_declaration() {
    let graph = route_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH path = (incident:Incident)<-[:ROUTES*2]-(person:Person) \
             RETURN person.name AS person, incident.title AS incident, length(path) AS hops",
    )
    .expect("incoming cross-label fixed-hop path should infer reverse labels");

    let service = plan
        .nodes
        .iter()
        .find(|node| node.label == "Service")
        .expect("intermediate Service node should be inferred");
    assert_eq!(plan.relationships.len(), 2);
    let first_relationship = plan.relationships.first().expect("first relationship");
    let second_relationship = plan.relationships.get(1).expect("second relationship");
    assert_eq!(first_relationship.left, "incident");
    assert_eq!(first_relationship.right, service.variable);
    assert_eq!(first_relationship.direction, Direction::Incoming);
    assert_eq!(second_relationship.left, first_relationship.right);
    assert_eq!(second_relationship.right, "person");
    assert_eq!(second_relationship.direction, Direction::Incoming);
}

#[test]
fn rejects_ambiguous_cross_label_fixed_relationship_ranges() {
    let error = compile_cypher_for_graph(
        &route_test_graph(),
        "MATCH (person:Person)-[:ESCALATES_TO*2]->(incident:Incident) \
             RETURN person.name AS person, incident.title AS incident",
    )
    .expect_err("ambiguous intermediate labels should be rejected");

    assert!(
        error
            .to_string()
            .contains("found at least 2 possible 2-hop"),
        "{error}"
    );
    assert!(
        error
            .to_string()
            .contains("use explicit intermediate nodes to disambiguate"),
        "{error}"
    );
}

#[test]
fn caps_fixed_length_label_sequence_collection_after_ambiguity_detected() {
    let sequences = fixed_length_label_sequences(
        &fanout_test_graph(),
        "FANS_OUT",
        Direction::Outgoing,
        "Person",
        "Incident",
        2,
    );

    assert_eq!(sequences.len(), MAX_FIXED_LABEL_SEQUENCE_RESULTS);
}

#[test]
fn rejects_unmapped_cross_label_fixed_relationship_ranges() {
    let error = compile_cypher_for_graph(
        &route_test_graph(),
        "MATCH (team:Team)-[:ROUTES*2]->(incident:Incident) \
             RETURN team.name AS team, incident.title AS incident",
    )
    .expect_err("unmapped fixed-hop label paths should be rejected");

    assert!(error.to_string().contains("could not infer"), "{error}");
}

#[test]
fn compiles_bounded_cross_label_relationship_ranges_from_graph_declaration() {
    let query = compile_cypher_query_for_graph(
        &route_test_graph(),
        "MATCH path = (person:Person)-[:ROUTES*0..2]->(incident:Incident) \
             RETURN person.name AS person, incident.title AS incident, length(path) AS hops",
    )
    .expect("bounded cross-label path should prune impossible lengths and compile");

    let GraphQuery::Plan(plan) = query else {
        panic!("only the feasible two-hop branch should remain");
    };
    assert_eq!(plan.relationships.len(), 2);
    assert_eq!(path_length_projection_literal(&plan), Some(2));
}

#[test]
fn compiles_parameterized_dynamic_bounded_cross_label_ranges_from_graph_declaration() {
    let parameters = BTreeMap::from([
        (
            "from_label".to_string(),
            CypherParameterValue::Literal(Literal::String("Person".to_string())),
        ),
        (
            "relationship_type".to_string(),
            CypherParameterValue::Literal(Literal::String("ROUTES".to_string())),
        ),
        (
            "to_label".to_string(),
            CypherParameterValue::Literal(Literal::String("Incident".to_string())),
        ),
    ]);
    let query = compile_cypher_query_for_graph_with_parameters(
            &route_test_graph(),
            "MATCH path = (person:$($from_label))-[:$($relationship_type)*0..2]->(incident:$($to_label)) \
             RETURN person.name AS person, incident.title AS incident, length(path) AS hops",
            &parameters,
        )
        .expect("parameterized dynamic bounded cross-label path should prune and compile");

    let GraphQuery::Plan(plan) = query else {
        panic!("only the feasible two-hop branch should remain");
    };
    assert_eq!(plan.relationships.len(), 2);
    assert_eq!(path_length_projection_literal(&plan), Some(2));
    assert_eq!(
        plan.nodes
            .iter()
            .map(|node| node.label.as_str())
            .collect::<Vec<_>>(),
        vec!["Person", "Incident", "Service"]
    );
    assert!(
        plan.relationships
            .iter()
            .all(|relationship| relationship.relationship_type == "ROUTES")
    );
}

#[test]
fn compiles_bounded_cross_label_gql_quantifiers_from_graph_declaration() {
    let query = compile_cypher_query_for_graph(
        &route_test_graph(),
        "MATCH path = (person:Person)-[:ROUTES]->{0,2}(incident:Incident) \
             RETURN person.name AS person, incident.title AS incident, length(path) AS hops",
    )
    .expect("bounded cross-label GQL quantifier should prune impossible lengths and compile");

    let GraphQuery::Plan(plan) = query else {
        panic!("only the feasible two-hop branch should remain");
    };
    assert_eq!(plan.relationships.len(), 2);
    assert_eq!(path_length_projection_literal(&plan), Some(2));
}

#[test]
fn compiles_incoming_bounded_cross_label_relationship_ranges_from_graph_declaration() {
    let query = compile_cypher_query_for_graph(
        &route_test_graph(),
        "MATCH path = (incident:Incident)<-[:ROUTES*0..2]-(person:Person) \
             RETURN person.name AS person, incident.title AS incident, length(path) AS hops",
    )
    .expect("incoming bounded cross-label path should prune impossible lengths and compile");

    let GraphQuery::Plan(plan) = query else {
        panic!("only the feasible two-hop branch should remain");
    };
    assert_eq!(plan.relationships.len(), 2);
    assert!(
        plan.relationships
            .iter()
            .all(|relationship| relationship.direction == Direction::Incoming)
    );
    assert_eq!(path_length_projection_literal(&plan), Some(2));
}

#[test]
fn compiles_undirected_bounded_cross_label_relationship_ranges_from_graph_declaration() {
    let query = compile_cypher_query_for_graph(
        &route_test_graph(),
        "MATCH path = (person:Person)-[:ROUTES*0..2]-(incident:Incident) \
             RETURN person.name AS person, incident.title AS incident, length(path) AS hops",
    )
    .expect("undirected bounded cross-label path should prune impossible lengths and compile");

    let GraphQuery::Plan(plan) = query else {
        panic!("only the feasible two-hop branch should remain");
    };
    assert_eq!(plan.relationships.len(), 2);
    assert!(
        plan.relationships
            .iter()
            .all(|relationship| relationship.direction == Direction::Undirected)
    );
    assert_eq!(path_length_projection_literal(&plan), Some(2));
}

#[test]
fn rejects_ambiguous_bounded_cross_label_relationship_ranges() {
    let error = compile_cypher_query_for_graph(
        &route_test_graph(),
        "MATCH (person:Person)-[:ESCALATES_TO*0..2]->(incident:Incident) \
             RETURN person.name AS person, incident.title AS incident",
    )
    .expect_err("ambiguous bounded intermediate labels should be rejected");

    assert!(
        error
            .to_string()
            .contains("found at least 2 possible 2-hop 'ESCALATES_TO' label paths"),
        "{error}"
    );
}

#[test]
fn compiles_bounded_cross_label_ranges_with_no_feasible_schema_paths_as_empty_plans() {
    for cypher in [
        "MATCH path = (team:Team)-[:ROUTES*1..2]->(incident:Incident) \
             RETURN team.name AS team, incident.title AS incident, length(path) AS hops",
        "MATCH path = (team:Team)-[:ROUTES*1..2]->(other:Team) \
             RETURN team.name AS team, other.name AS other, length(path) AS hops",
    ] {
        let query = compile_cypher_query_for_graph(&route_test_graph(), cypher)
            .expect("all-pruned bounded ranges should compile as empty plans");

        let GraphQuery::Plan(plan) = query else {
            panic!("all-pruned bounded range should compile as one empty plan");
        };
        assert_eq!(path_length_projection_literal(&plan), Some(0));
        assert!(
            predicate_contains_boolean_false(plan.predicate.as_ref()),
            "{:#?}",
            plan.predicate
        );
    }
}

#[test]
fn compiles_exact_zero_relationship_ranges_as_same_node_identity() {
    for cypher in [
        "MATCH (a:Service)-[:DEPENDS_ON*0]->(b:Service) RETURN a.name",
        "MATCH (a:Service)-[:DEPENDS_ON*0..0]->(b:Service) RETURN a.name",
        "MATCH (a:Service)-[:DEPENDS_ON]->{0}(b:Service) RETURN a.name",
        "MATCH (a:Service)-[:DEPENDS_ON]->{0,0}(b:Service) RETURN a.name",
    ] {
        let plan = compile_cypher(cypher).expect("exact zero-hop relationship should compile");

        assert_eq!(
            plan.nodes,
            vec![
                NodePattern {
                    variable: "a".to_string(),
                    label: "Service".to_string(),
                },
                NodePattern {
                    variable: "b".to_string(),
                    label: "Service".to_string(),
                },
            ]
        );
        assert!(plan.relationships.is_empty());
        assert_eq!(
            plan.predicate,
            Some(PredicateExpression::KeyComparison(KeyPredicate {
                variable: "a".to_string(),
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Key {
                    variable: "b".to_string(),
                },
            }))
        );
    }
}

#[test]
fn compiles_exact_zero_cross_label_relationship_ranges_as_false() {
    let plan = compile_cypher(
        "MATCH (a:Service)-[:DEPENDS_ON*0]->(b:Person) \
             RETURN a.name AS source, b.name AS target",
    )
    .expect("cross-label exact zero-hop relationship should compile as an empty match");

    assert!(plan.relationships.is_empty());
    assert_eq!(plan.predicate, Some(PredicateExpression::Boolean(false)));
}

#[test]
fn compiles_bounded_variable_length_relationship_ranges_as_union_all() {
    let query = compile_cypher_query(
        "MATCH path = (a:Service)-[:DEPENDS_ON*1..3]->(b:Service) \
             RETURN a.name AS source, b.name AS target, length(path) AS hops \
             ORDER BY source, target, hops",
    )
    .expect("bounded relationship range should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected bounded relationship range to expand into a union query");
    };
    assert_eq!(union.branches.len(), 2);
    let first_branch = union.branches.first().expect("first range branch");
    let second_branch = union.branches.get(1).expect("second range branch");
    assert_eq!(union.first.relationships.len(), 1);
    assert_eq!(first_branch.plan.relationships.len(), 2);
    assert_eq!(second_branch.plan.relationships.len(), 3);
    assert!(union.branches.iter().all(|branch| branch.all));
    assert_eq!(path_length_projection_literal(&union.first), Some(1));
    assert_eq!(path_length_projection_literal(&first_branch.plan), Some(2));
    assert_eq!(path_length_projection_literal(&second_branch.plan), Some(3));
    assert_eq!(union.order_by.len(), 3);
}

#[test]
fn compiles_zero_hop_bounded_variable_length_relationship_ranges_as_union_all() {
    let query = compile_cypher_query(
        "MATCH path = (a:Service)-[:DEPENDS_ON*0..2]->(b:Service) \
             RETURN a.name AS source, b.name AS target, length(path) AS hops \
             ORDER BY source, target, hops",
    )
    .expect("zero-hop bounded relationship range should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected zero-hop bounded relationship range to expand into a union query");
    };
    assert_eq!(union.branches.len(), 2);
    let first_branch = union.branches.first().expect("first range branch");
    let second_branch = union.branches.get(1).expect("second range branch");
    assert!(union.first.relationships.is_empty());
    assert_eq!(first_branch.plan.relationships.len(), 1);
    assert_eq!(second_branch.plan.relationships.len(), 2);
    assert_eq!(
        union.first.predicate,
        Some(PredicateExpression::KeyComparison(KeyPredicate {
            variable: "a".to_string(),
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Key {
                variable: "b".to_string(),
            },
        }))
    );
    assert!(union.branches.iter().all(|branch| branch.all));
    assert_eq!(path_length_projection_literal(&union.first), Some(0));
    assert_eq!(path_length_projection_literal(&first_branch.plan), Some(1));
    assert_eq!(path_length_projection_literal(&second_branch.plan), Some(2));
    assert_eq!(union.order_by.len(), 3);
}

#[test]
fn compiles_bounded_gql_relationship_quantifiers_as_union_all() {
    let query = compile_cypher_query(
        "MATCH (a:Service)-[:DEPENDS_ON]->{1,2}(b:Service) \
             RETURN a.name AS source, b.name AS target",
    )
    .expect("bounded GQL relationship quantifier should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected bounded relationship quantifier to expand into a union query");
    };
    assert_eq!(union.first.relationships.len(), 1);
    assert_eq!(union.branches.len(), 1);
    assert_eq!(
        union
            .branches
            .first()
            .expect("first range branch")
            .plan
            .relationships
            .len(),
        2
    );
}

#[test]
fn compiles_unquantified_parenthesized_path_patterns() {
    let plan = compile_cypher(
        "MATCH ((a:Service)-[:DEPENDS_ON]->(b:Service)) \
             RETURN a.name AS source, b.name AS target",
    )
    .expect("unquantified parenthesized path pattern should compile");

    assert_eq!(
        plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "DEPENDS_ON".to_string(),
            left: "a".to_string(),
            direction: Direction::Outgoing,
            right: "b".to_string(),
        }]
    );

    let path_plan = compile_cypher(
        "MATCH dependency_path = ((a:Service)-[:DEPENDS_ON]->(b:Service)) \
             RETURN length(dependency_path) AS hops",
    )
    .expect("path variable over parenthesized path pattern should compile");
    assert_eq!(path_length_projection_literal(&path_plan), Some(1));

    let optional_plan = compile_cypher(
        "MATCH (a:Service) \
             OPTIONAL MATCH ((a)-[:DEPENDS_ON]->(b:Service)) \
             RETURN a.name AS source, b.name AS target",
    )
    .expect("anchored optional parenthesized path pattern should compile");
    assert_eq!(optional_plan.optional_relationships, vec![0]);

    let ranged_query = compile_cypher_query(
        "MATCH ((a:Service)-[:DEPENDS_ON*1..2]->(b:Service)) \
             RETURN a.name AS source, b.name AS target",
    )
    .expect("bounded range inside parenthesized path should compile");
    assert!(matches!(ranged_query, GraphQuery::Union(_)));

    let alternative_query = compile_cypher_query(
        "MATCH ((a:Service)-[:DEPENDS_ON|CALLS]->(b:Service)) \
             RETURN a.name AS source, b.name AS target",
    )
    .expect("relationship type alternatives inside parenthesized path should compile");
    assert!(matches!(alternative_query, GraphQuery::Union(_)));
}

#[test]
fn compiles_exact_one_quantified_parenthesized_path_patterns() {
    let plan = compile_cypher(
        "MATCH ((a:Service)-[:DEPENDS_ON]->(b:Service)){1} \
             RETURN a.name AS source, b.name AS target",
    )
    .expect("exact-one quantified parenthesized path pattern should compile");

    assert_eq!(
        plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "DEPENDS_ON".to_string(),
            left: "a".to_string(),
            direction: Direction::Outgoing,
            right: "b".to_string(),
        }]
    );

    let path_plan = compile_cypher(
        "MATCH dependency_path = ((a:Service)-[:DEPENDS_ON]->(b:Service)){1,1} \
             RETURN length(dependency_path) AS hops",
    )
    .expect("path variable over exact-one quantified parenthesized path should compile");
    assert_eq!(path_length_projection_literal(&path_plan), Some(1));

    let optional_plan = compile_cypher(
        "MATCH (a:Service) \
             OPTIONAL MATCH ((a)-[:DEPENDS_ON]->(b:Service)){1} \
             RETURN a.name AS source, b.name AS target",
    )
    .expect("anchored optional exact-one quantified parenthesized path should compile");
    assert_eq!(optional_plan.optional_relationships, vec![0]);
}

#[test]
fn rejects_quantified_parenthesized_path_patterns() {
    let error = compile_cypher(
        "MATCH ((a:Service)-[:DEPENDS_ON]->(b:Service)){1,2} \
             RETURN a.name AS source",
    )
    .expect_err("quantified parenthesized path patterns should remain rejected");

    assert!(
        error
            .to_string()
            .contains("quantified path patterns are not supported yet"),
        "{error}"
    );
}

#[test]
fn compiles_exact_one_relationship_ranges_as_single_hop() {
    for cypher in [
        "MATCH (a:Service)-[:DEPENDS_ON*1]->(b:Service) RETURN a.name",
        "MATCH (a:Service)-[:DEPENDS_ON*1..1]->(b:Service) RETURN a.name",
        "MATCH (a:Service)-[:DEPENDS_ON]->{1}(b:Service) RETURN a.name",
        "MATCH (a:Service)-[:DEPENDS_ON]->{1,1}(b:Service) RETURN a.name",
    ] {
        let plan = compile_cypher(cypher).expect("exact-one relationship should compile");

        assert_eq!(
            plan.relationships,
            vec![RelationshipPattern {
                variable: None,
                relationship_type: "DEPENDS_ON".to_string(),
                left: "a".to_string(),
                direction: Direction::Outgoing,
                right: "b".to_string(),
            }]
        );
    }
}

#[test]
fn compiles_exact_fixed_relationship_ranges_as_repeated_hops() {
    for cypher in [
        "MATCH (a:Service)-[:DEPENDS_ON*2]->(b:Service) RETURN a.name",
        "MATCH (a:Service)-[:DEPENDS_ON*2..2]->(b:Service) RETURN a.name",
        "MATCH (a:Service)-[:DEPENDS_ON]->{2}(b:Service) RETURN a.name",
        "MATCH (a:Service)-[:DEPENDS_ON]->{2,2}(b:Service) RETURN a.name",
    ] {
        let plan = compile_cypher(cypher).expect("exact fixed relationship should compile");

        assert_eq!(
            plan.nodes,
            vec![
                NodePattern {
                    variable: "a".to_string(),
                    label: "Service".to_string(),
                },
                NodePattern {
                    variable: "b".to_string(),
                    label: "Service".to_string(),
                },
                NodePattern {
                    variable: "__coral_node_0_1".to_string(),
                    label: "Service".to_string(),
                },
            ]
        );
        assert_eq!(
            plan.relationships,
            vec![
                RelationshipPattern {
                    variable: None,
                    relationship_type: "DEPENDS_ON".to_string(),
                    left: "a".to_string(),
                    direction: Direction::Outgoing,
                    right: "__coral_node_0_1".to_string(),
                },
                RelationshipPattern {
                    variable: None,
                    relationship_type: "DEPENDS_ON".to_string(),
                    left: "__coral_node_0_1".to_string(),
                    direction: Direction::Outgoing,
                    right: "b".to_string(),
                },
            ]
        );
    }
}

#[test]
fn compiles_exact_fixed_relationship_range_property_maps_per_hop() {
    let plan = compile_cypher(
        "MATCH (a:Service)-[:DEPENDS_ON*2 {source: 'catalog'}]->(b:Service) RETURN a.name",
    )
    .expect("exact fixed relationship property map should compile");

    assert_eq!(
        plan.relationships,
        vec![
            RelationshipPattern {
                variable: Some("__coral_rel_0".to_string()),
                relationship_type: "DEPENDS_ON".to_string(),
                left: "a".to_string(),
                direction: Direction::Outgoing,
                right: "__coral_node_0_1".to_string(),
            },
            RelationshipPattern {
                variable: Some("__coral_rel_1".to_string()),
                relationship_type: "DEPENDS_ON".to_string(),
                left: "__coral_node_0_1".to_string(),
                direction: Direction::Outgoing,
                right: "b".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.predicates,
        vec![
            PropertyPredicate {
                property: PropertyRef {
                    variable: "__coral_rel_0".to_string(),
                    property: "source".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("catalog".to_string())),
            },
            PropertyPredicate {
                property: PropertyRef {
                    variable: "__coral_rel_1".to_string(),
                    property: "source".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("catalog".to_string())),
            },
        ]
    );
}

#[test]
fn compiles_undirected_relationships() {
    let plan = compile_cypher("MATCH (a:Service)-[:DEPENDS_ON]-(b:Service) RETURN a.name")
        .expect("undirected relationship should compile");

    assert_eq!(
        plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "DEPENDS_ON".to_string(),
            left: "a".to_string(),
            direction: Direction::Undirected,
            right: "b".to_string(),
        }]
    );
}

#[test]
fn compiles_xor_predicates() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE service.tier = 'prod' XOR service.tier IS NULL \
             RETURN service.name",
    )
    .expect("XOR predicate should compile");

    assert!(matches!(
        plan.predicate,
        Some(PredicateExpression::Xor { .. })
    ));
}

#[test]
fn compiles_terminal_with_xor_where_alias_predicates() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[:OWNS]->(service:Service) \
             WITH person.name AS owner, service.tier AS tier \
             WHERE owner STARTS WITH 'Ada' XOR tier = 'prod' \
             RETURN owner, tier",
    )
    .expect("terminal WITH XOR WHERE should compile");

    assert!(matches!(
        plan.post_projection_predicate,
        Some(ProjectionPredicateExpression::Xor { .. })
    ));
}

#[test]
fn rejects_missing_cypher_parameters() {
    let error =
        compile_cypher("MATCH (service:Service) WHERE service.tier IN $tiers RETURN service.name")
            .expect_err("missing parameter should fail");

    assert!(
        error.to_string().contains("MISSING_PARAMETER"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_cypher_parameter_kind_mismatches() {
    let scalar_for_list = BTreeMap::from([(
        "tiers".to_string(),
        CypherParameterValue::Literal(Literal::String("prod".to_string())),
    )]);
    let error = compile_cypher_with_parameters(
        "MATCH (service:Service) WHERE service.tier IN $tiers RETURN service.name",
        &scalar_for_list,
    )
    .expect_err("scalar parameter should not bind as IN list");
    assert!(
        error.to_string().contains("IN parameter right-hand sides"),
        "unexpected error: {error}"
    );

    let list_for_scalar = BTreeMap::from([(
        "tier".to_string(),
        CypherParameterValue::List(vec![Literal::String("prod".to_string())]),
    )]);
    let error = compile_cypher_with_parameters(
        "MATCH (service:Service) WHERE service.tier = $tier RETURN service.name",
        &list_for_scalar,
    )
    .expect_err("list parameter should not bind as scalar literal");
    assert!(
        error
            .to_string()
            .contains("list parameters can only be used"),
        "unexpected error: {error}"
    );

    let ambiguous_list_projection = BTreeMap::from([(
        "value".to_string(),
        CypherParameterValue::List(vec![Literal::Null]),
    )]);
    let error = compile_cypher_with_parameters(
        "MATCH (service:Service) RETURN $value AS value",
        &ambiguous_list_projection,
    )
    .expect_err("ambiguous list parameter projection should fail");
    assert!(
        error.to_string().contains("at least one non-null element"),
        "unexpected error: {error}"
    );
}

#[test]
fn compiles_regex_predicates() {
    let plan = compile_cypher(
        "MATCH (service:Service) WHERE service.name =~ '^billing.*' RETURN service.name",
    )
    .expect("regex predicate should compile");

    assert_eq!(
        plan.predicates,
        vec![PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            },
            operator: ComparisonOperator::RegexMatch,
            rhs: PredicateRhs::Literal(Literal::String("^billing.*".to_string())),
        }]
    );
}

#[test]
fn compiles_dynamic_regex_predicate_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) WHERE service.name =~ left(service.name, 4) RETURN service.name",
    )
    .expect("dynamic regex predicate should compile");

    assert!(plan.predicates.is_empty());
    assert!(matches!(
        &plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Property(PropertyRef { property, .. }),
            operator: ComparisonOperator::RegexMatch,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Left { .. }),
        })) if property == "name"
    ));
}

#[test]
fn rejects_invalid_literal_regex_predicates() {
    assert_unsupported("MATCH (service:Service) WHERE 'billing-api' =~ '[' RETURN service.name");
}

#[test]
fn rejects_comparisons_without_supported_operands() {
    assert_unsupported("MATCH (service:Service) WHERE service = service RETURN service.name");
}

#[test]
fn compiles_grouped_count_projection() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN service.tier AS tier, count(*) AS services \
             ORDER BY tier",
    )
    .expect("grouped count query should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                alias: Some("tier".to_string()),
            },
            Projection::CountAll {
                alias: "services".to_string(),
            },
        ]
    );
    assert_eq!(plan.order_by.len(), 1);
}

#[test]
fn rejects_reserved_internal_variable_prefix() {
    assert_unsupported("MATCH (__coral_rel_0:Service) RETURN __coral_rel_0.name");
}

#[test]
fn rejects_unlabeled_first_node_binding() {
    assert_unsupported("MATCH (source)-[:DEPENDS_ON]->(target:Service) RETURN target.name");
}

#[test]
fn graph_aware_cypher_infers_unlabeled_outgoing_endpoint_labels() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[:OWNS]->(service) RETURN service.name",
    )
    .expect("graph declaration should infer the unlabeled outgoing endpoint");

    assert_eq!(
        plan.nodes,
        vec![
            NodePattern {
                variable: "person".to_string(),
                label: "Person".to_string(),
            },
            NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "OWNS".to_string(),
            left: "person".to_string(),
            direction: Direction::Outgoing,
            right: "service".to_string(),
        }]
    );
}

#[test]
fn graph_aware_cypher_infers_unlabeled_incoming_endpoint_labels() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service)<-[:OWNS]-(person:Person) RETURN service.name",
    )
    .expect("graph declaration should infer the unlabeled incoming endpoint");

    assert_eq!(
        plan.nodes,
        vec![
            NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "person".to_string(),
                label: "Person".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "OWNS".to_string(),
            left: "service".to_string(),
            direction: Direction::Incoming,
            right: "person".to_string(),
        }]
    );
}

#[test]
fn graph_aware_cypher_infers_unlabeled_exact_one_endpoint_labels() {
    let graph = star_test_graph();
    for cypher in [
        "MATCH (person:Person)-[:OWNS*1]->(service) RETURN service.name",
        "MATCH (person:Person)-[:OWNS*1..1]->(service) RETURN service.name",
        "MATCH (person:Person)-[:OWNS]->{1}(service) RETURN service.name",
    ] {
        let plan = compile_cypher_for_graph(&graph, cypher)
            .expect("graph declaration should infer exact-one endpoint labels");
        assert!(
            plan.nodes
                .iter()
                .any(|node| { node.variable == "service" && node.label == "Service" }),
            "service endpoint label was not inferred for {cypher}: {:?}",
            plan.nodes
        );
    }
}

#[test]
fn graph_aware_cypher_infers_unlabeled_fixed_length_endpoint_labels() {
    let graph = route_test_graph();
    for cypher in [
        "MATCH (person:Person)-[:ROUTES*2]->(incident) RETURN incident.title",
        "MATCH (incident)<-[:ROUTES*2]-(person:Person) RETURN incident.title",
        "MATCH (person:Person)-[:ROUTES]->{2}(incident) RETURN incident.title",
    ] {
        let plan = compile_cypher_for_graph(&graph, cypher)
            .expect("graph declaration should infer fixed-length endpoint labels");
        assert!(
            plan.nodes
                .iter()
                .any(|node| { node.variable == "incident" && node.label == "Incident" }),
            "incident endpoint label was not inferred for {cypher}: {:?}",
            plan.nodes
        );
    }
}

#[test]
fn graph_aware_cypher_infers_unlabeled_zero_hop_endpoint_labels() {
    let graph = route_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[:ROUTES*0]->(same) RETURN same.name",
    )
    .expect("graph declaration should infer zero-hop endpoint labels");

    assert!(
        plan.nodes
            .iter()
            .any(|node| { node.variable == "same" && node.label == "Person" }),
        "zero-hop endpoint label was not inferred: {:?}",
        plan.nodes
    );
}

#[test]
fn graph_aware_cypher_infers_anonymous_outgoing_endpoint_labels() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[:OWNS]->() RETURN person.name",
    )
    .expect("graph declaration should infer the anonymous outgoing endpoint");

    let anonymous = plan
        .nodes
        .iter()
        .find(|node| node.variable.starts_with("__coral_node_"))
        .expect("anonymous endpoint should be bound internally");
    assert_eq!(anonymous.label, "Service");
    assert_eq!(
        plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "OWNS".to_string(),
            left: "person".to_string(),
            direction: Direction::Outgoing,
            right: anonymous.variable.clone(),
        }]
    );
}

#[test]
fn graph_aware_cypher_infers_anonymous_incoming_endpoint_labels() {
    let graph = route_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH ()-[:ROUTES]->(service:Service) RETURN service.name",
    )
    .expect("graph declaration should infer the anonymous incoming endpoint");

    let anonymous = plan
        .nodes
        .iter()
        .find(|node| node.variable.starts_with("__coral_node_"))
        .expect("anonymous endpoint should be bound internally");
    assert_eq!(anonymous.label, "Person");
    assert_eq!(
        plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "ROUTES".to_string(),
            left: anonymous.variable.clone(),
            direction: Direction::Outgoing,
            right: "service".to_string(),
        }]
    );
}

#[test]
fn graph_aware_cypher_infers_anonymous_fixed_and_zero_hop_endpoint_labels() {
    let graph = route_test_graph();
    let fixed = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[:ROUTES*2]->() RETURN person.name",
    )
    .expect("graph declaration should infer the anonymous fixed-hop endpoint");
    assert!(
        fixed
            .nodes
            .iter()
            .any(|node| { node.variable.starts_with("__coral_node_") && node.label == "Incident" }),
        "fixed-hop anonymous endpoint label was not inferred: {:?}",
        fixed.nodes
    );

    let zero = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[:ROUTES*0]->() RETURN person.name",
    )
    .expect("graph declaration should infer the anonymous zero-hop endpoint");
    assert!(
        zero.nodes
            .iter()
            .any(|node| { node.variable.starts_with("__coral_node_") && node.label == "Person" }),
        "zero-hop anonymous endpoint label was not inferred: {:?}",
        zero.nodes
    );
}

#[test]
fn graph_aware_cypher_preserves_fixed_length_intermediate_ambiguity() {
    let graph = route_test_graph();
    let error = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[:ESCALATES_TO*2]->(incident) RETURN incident.title",
    )
    .expect_err("ambiguous fixed-length intermediate labels should still fail");

    assert!(
        error
            .to_string()
            .contains("found at least 2 possible 2-hop"),
        "{error}"
    );
}

#[test]
fn graph_aware_cypher_compile_rejects_unknown_declared_properties() {
    let graph = star_test_graph();
    let error = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) RETURN service.missing AS value",
    )
    .expect_err("graph-aware Cypher compile should validate declared properties");

    assert!(error.to_string().contains("UNKNOWN_PROPERTY"), "{error}");
}

#[test]
fn graph_aware_cypher_query_compile_validates_union_branches() {
    let graph = star_test_graph();
    let error = compile_cypher_query_for_graph(
        &graph,
        "MATCH (service:Service) RETURN service.name AS value \
             UNION ALL \
             MATCH (service:Service) RETURN service.missing AS value",
    )
    .expect_err("graph-aware Cypher query compile should validate union branches");

    assert!(error.to_string().contains("UNKNOWN_PROPERTY"), "{error}");
}

#[test]
fn graph_aware_cypher_rejects_ambiguous_unlabeled_endpoint_labels() {
    let graph = star_test_graph();
    let error = compile_cypher_for_graph(
        &graph,
        "MATCH (owner)-[:OWNS]->(service:Service) RETURN service.name",
    )
    .expect_err("ambiguous unlabeled endpoint labels should be rejected");

    assert!(
        error.to_string().contains("could not infer a unique label"),
        "{error:?}"
    );
}

#[test]
fn graph_aware_cypher_rejects_ambiguous_anonymous_endpoint_labels() {
    let graph = star_test_graph();
    let error = compile_cypher_for_graph(
        &graph,
        "MATCH ()-[:OWNS]->(service:Service) RETURN service.name",
    )
    .expect_err("ambiguous anonymous endpoint labels should be rejected");

    assert!(
        error
            .to_string()
            .contains("anonymous node at path position 0"),
        "{error:?}"
    );
}

#[test]
fn graph_aware_cypher_infers_untyped_relationship_types() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-->(service:Service) RETURN service.name",
    )
    .expect("graph declaration should infer an untyped relationship");

    assert_eq!(
        plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "OWNS".to_string(),
            left: "person".to_string(),
            direction: Direction::Outgoing,
            right: "service".to_string(),
        }]
    );
}

#[test]
fn graph_aware_cypher_infers_untyped_relationship_endpoint_labels() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-->(service) RETURN service.name",
    )
    .expect("graph declaration should infer the untyped relationship endpoint");

    assert!(
        plan.nodes
            .iter()
            .any(|node| node.variable == "service" && node.label == "Service"),
        "service endpoint label was not inferred: {:?}",
        plan.nodes
    );
    assert_eq!(
        plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "OWNS".to_string(),
            left: "person".to_string(),
            direction: Direction::Outgoing,
            right: "service".to_string(),
        }]
    );
}

#[test]
fn graph_aware_cypher_infers_untyped_anonymous_endpoint_labels() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(&graph, "MATCH (person:Person)-->() RETURN person.name")
        .expect("graph declaration should infer the untyped anonymous endpoint");

    let anonymous = plan
        .nodes
        .iter()
        .find(|node| node.variable.starts_with("__coral_node_"))
        .expect("anonymous endpoint should be bound internally");
    assert_eq!(anonymous.label, "Service");
    assert_eq!(
        plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "OWNS".to_string(),
            left: "person".to_string(),
            direction: Direction::Outgoing,
            right: anonymous.variable.clone(),
        }]
    );
}

#[test]
fn graph_aware_cypher_infers_untyped_relationship_variables() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[ownership]->(service:Service) RETURN type(ownership)",
    )
    .expect("graph declaration should infer an untyped relationship variable");

    assert_eq!(
        plan.relationships,
        vec![RelationshipPattern {
            variable: Some("ownership".to_string()),
            relationship_type: "OWNS".to_string(),
            left: "person".to_string(),
            direction: Direction::Outgoing,
            right: "service".to_string(),
        }]
    );
}

#[test]
fn graph_aware_cypher_rejects_ambiguous_untyped_endpoint_labels() {
    let graph = route_test_graph();
    let error = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-->(target) RETURN person.name",
    )
    .expect_err("ambiguous untyped endpoint label should be rejected");

    assert!(
            error.to_string().contains(
                "could not infer a unique label for node variable 'target' from untyped relationship mappings"
            ),
            "{error:?}"
        );
}

#[test]
fn graph_aware_cypher_rejects_ambiguous_untyped_relationship_types() {
    let graph = route_test_graph();
    let error = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-->(service:Service) RETURN service.name",
    )
    .expect_err("ambiguous untyped relationship should be rejected");

    assert!(
        error
            .to_string()
            .contains("could not infer a unique relationship type"),
        "{error:?}"
    );
}

#[test]
fn graph_aware_cypher_rejects_unmapped_untyped_relationship_types() {
    let graph = route_test_graph();
    let error = compile_cypher_for_graph(
        &graph,
        "MATCH (incident:Incident)-->(person:Person) RETURN incident.title",
    )
    .expect_err("unmapped untyped relationship should be rejected");

    assert!(
        error
            .to_string()
            .contains("could not infer a relationship type"),
        "{error:?}"
    );
}

#[test]
fn graph_aware_cypher_rejects_untyped_relationship_ranges() {
    let graph = route_test_graph();
    let error = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[*2]->(incident:Incident) RETURN incident.title",
    )
    .expect_err("untyped relationship ranges should be rejected");

    assert!(
        error
            .to_string()
            .contains("untyped relationship ranges require an explicit relationship type"),
        "{error:?}"
    );
}

#[test]
fn rejects_untyped_relationships_without_graph_declaration() {
    let error = compile_cypher("MATCH (person:Person)-->(service:Service) RETURN service.name")
        .expect_err("declaration-free untyped relationships should be rejected");

    assert!(
        error.to_string().contains("relationship type is required"),
        "{error:?}"
    );
}

#[test]
fn compiles_anonymous_labeled_node_patterns() {
    let plan = compile_cypher(
        "MATCH (:Service {tier: 'prod'})-[:DEPENDS_ON]->(target:Service) \
             RETURN target.name",
    )
    .expect("anonymous labeled node pattern should compile");

    assert_eq!(plan.nodes.len(), 2);
    let anonymous_node = plan.nodes.first().expect("anonymous node should exist");
    let target_node = plan.nodes.get(1).expect("target node should exist");
    let relationship = plan
        .relationships
        .first()
        .expect("relationship should exist");
    let anonymous_variable = &anonymous_node.variable;
    assert!(anonymous_variable.starts_with("__coral_node_"));
    assert_eq!(anonymous_node.label, "Service");
    assert_eq!(target_node.variable, "target");
    assert_eq!(relationship.left, anonymous_variable.as_str());
    assert_eq!(relationship.right, "target");
    assert_eq!(
        plan.predicates,
        vec![PropertyPredicate {
            property: PropertyRef {
                variable: anonymous_variable.clone(),
                property: "tier".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
        }]
    );
}

#[test]
fn compiles_static_label_expression_patterns() {
    let plan = compile_cypher(
            "MATCH (person:Person&!(Team|Service))-[owns:OWNS&!(DEPENDS_ON|ALERTS)]->(service:Service&!Team) \
             RETURN person.name AS owner, service.name AS service",
        )
        .expect("static label expression patterns should compile");

    assert_eq!(
        plan.nodes,
        vec![
            NodePattern {
                variable: "person".to_string(),
                label: "Person".to_string(),
            },
            NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.relationships,
        vec![RelationshipPattern {
            variable: Some("owns".to_string()),
            relationship_type: "OWNS".to_string(),
            left: "person".to_string(),
            direction: Direction::Outgoing,
            right: "service".to_string(),
        }]
    );
}

#[test]
fn rejects_contradictory_compound_label_exclusion_patterns() {
    let error = compile_cypher(
        "MATCH (service:Service&!(Service|Team)) \
             RETURN service.name AS service",
    )
    .expect_err("contradictory compound label exclusion should be rejected");

    assert!(
        error
            .to_string()
            .contains("contradictory label expressions"),
        "{error:?}"
    );
}

#[test]
fn rejects_unlabeled_anonymous_node_patterns() {
    assert_unsupported("MATCH ()-[:DEPENDS_ON]->(target:Service) RETURN target.name");
}

#[test]
fn rejects_conflicting_labels_for_reused_node_variables() {
    assert_unsupported(
        "MATCH (source:Service)-[:DEPENDS_ON]->(target:Service), \
                   (source:Person)-[:OWNS]->(target) \
             RETURN target.name",
    );
}

#[test]
fn rejects_ambiguous_label_expression_patterns() {
    assert_unsupported("MATCH (service:Service|Team) RETURN service.name");
    assert_unsupported("MATCH (service:Service&Team) RETURN service.name");
    assert_unsupported("MATCH (service:Service&!Service) RETURN service.name");
    assert_unsupported("MATCH (service:!Team) RETURN service.name");
    assert_unsupported(
        "MATCH (person:Person)-[:OWNS|DEPENDS_ON]->(service:Service) RETURN service.name",
    );
}

#[test]
fn compiles_count_property_projection() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN service.tier AS tier, count(service.name) AS named_services \
             ORDER BY named_services DESC",
    )
    .expect("count property query should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                alias: Some("tier".to_string()),
            },
            Projection::Aggregate {
                function: super::AggregateFunction::Count,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                }),
                distinct: false,
                alias: "named_services".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("named_services".to_string()),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_count_distinct_property_projection() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN count(DISTINCT service.tier) AS tier_count",
    )
    .expect("count distinct property query should compile");

    assert_eq!(
        plan.projections,
        vec![Projection::Aggregate {
            function: super::AggregateFunction::Count,
            target: AggregateTarget::Property(PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            }),
            distinct: true,
            alias: "tier_count".to_string(),
        }]
    );
}

#[test]
fn compiles_collect_property_projection() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[:OWNS]->(service:Service) \
             RETURN person.team AS team, collect(DISTINCT service.name) AS services \
             ORDER BY services",
    )
    .expect("collect property query should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "person".to_string(),
                    property: "team".to_string(),
                },
                alias: Some("team".to_string()),
            },
            Projection::Aggregate {
                function: super::AggregateFunction::Collect,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                }),
                distinct: true,
                alias: "services".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("services".to_string()),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_properties_function_aggregate_targets() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN properties(service).tier AS tier, \
                    count(properties(service).name) AS services \
             ORDER BY tier",
    )
    .expect("properties(variable).property aggregate targets should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                alias: Some("tier".to_string()),
            },
            Projection::Aggregate {
                function: super::AggregateFunction::Count,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                }),
                distinct: false,
                alias: "services".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_numeric_aggregate_projections() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN service.tier AS tier, \
                    sum(service.risk) AS total_risk, \
                    avg(service.risk) AS average_risk, \
                    min(service.risk) AS lowest_risk, \
                    min(DISTINCT service.risk) AS distinct_lowest_risk, \
                    max(DISTINCT service.risk) AS highest_risk \
             ORDER BY average_risk DESC",
    )
    .expect("numeric aggregate query should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                alias: Some("tier".to_string()),
            },
            Projection::Aggregate {
                function: super::AggregateFunction::Sum,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: false,
                alias: "total_risk".to_string(),
            },
            Projection::Aggregate {
                function: super::AggregateFunction::Avg,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: false,
                alias: "average_risk".to_string(),
            },
            Projection::Aggregate {
                function: super::AggregateFunction::Min,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: false,
                alias: "lowest_risk".to_string(),
            },
            Projection::Aggregate {
                function: super::AggregateFunction::Min,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: true,
                alias: "distinct_lowest_risk".to_string(),
            },
            Projection::Aggregate {
                function: super::AggregateFunction::Max,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: true,
                alias: "highest_risk".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("average_risk".to_string()),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_statistical_aggregate_projections() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN stDev(service.risk) AS sample_risk, \
                    stDevP(service.risk) AS population_risk",
    )
    .expect("statistical aggregate query should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Aggregate {
                function: super::AggregateFunction::StdDev,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: false,
                alias: "sample_risk".to_string(),
            },
            Projection::Aggregate {
                function: super::AggregateFunction::StdDevP,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: false,
                alias: "population_risk".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_percentile_cont_aggregate_projections_and_ordering() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN service.tier AS tier, \
                    percentileCont(service.risk, 0.75) AS p75_risk \
             ORDER BY percentileCont(service.risk, 0.5) DESC, tier",
    )
    .expect("percentileCont aggregate query should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                alias: Some("tier".to_string()),
            },
            Projection::Aggregate {
                function: super::AggregateFunction::PercentileCont {
                    percentile: ordered_float::OrderedFloat(0.75),
                },
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: false,
                alias: "p75_risk".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::Aggregate {
                    function: AggregateFunction::PercentileCont {
                        percentile: ordered_float::OrderedFloat(0.5),
                    },
                    target: AggregateTarget::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    }),
                    distinct: false,
                },
                direction: OrderDirection::Descending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn rejects_distinct_percentile_cont_aggregates() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             RETURN percentileCont(DISTINCT service.risk, 0.75) AS p75_risk",
    )
    .expect_err("distinct percentileCont should be rejected before SQL lowering");

    assert!(error.to_string().contains("percentileCont(DISTINCT"));
    assert!(error.to_string().contains("DataFusion 53"));
}

#[test]
fn compiles_gql_aggregate_function_aliases() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN collect_list(service.tier) AS tiers, \
                    stdev_samp(service.risk) AS sample_risk, \
                    stdev_pop(service.risk) AS population_risk",
    )
    .expect("GQL aggregate aliases should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Aggregate {
                function: super::AggregateFunction::Collect,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                }),
                distinct: false,
                alias: "tiers".to_string(),
            },
            Projection::Aggregate {
                function: super::AggregateFunction::StdDev,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: false,
                alias: "sample_risk".to_string(),
            },
            Projection::Aggregate {
                function: super::AggregateFunction::StdDevP,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: false,
                alias: "population_risk".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_distinct_standard_deviation_aggregate_projections() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN stDevP(DISTINCT service.risk) AS population_risk",
    )
    .expect("distinct standard-deviation aggregate should compile");

    assert_eq!(
        plan.projections,
        vec![Projection::Aggregate {
            function: super::AggregateFunction::StdDevP,
            target: AggregateTarget::Property(PropertyRef {
                variable: "service".to_string(),
                property: "risk".to_string(),
            }),
            distinct: true,
            alias: "population_risk".to_string(),
        }]
    );
}

#[test]
fn compiles_median_aggregate_projections() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN median(service.risk) AS median_risk, \
                    median(DISTINCT service.risk) AS distinct_median_risk",
    )
    .expect("median aggregate query should compile");

    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Aggregate {
                target: AggregateTarget::Property(PropertyRef {
                    variable,
                    property,
                }),
                distinct: false,
                alias,
                ..
            },
            Projection::Aggregate {
                distinct: true,
                alias: distinct_alias,
                ..
            },
        ] if variable == "service"
            && property == "risk"
            && alias == "median_risk"
            && distinct_alias == "distinct_median_risk"
    ));
}

#[test]
fn compiles_count_node_projection() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN count(service) AS services, count(DISTINCT service) AS distinct_services \
             ORDER BY services DESC",
    )
    .expect("count node query should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Aggregate {
                function: super::AggregateFunction::Count,
                target: AggregateTarget::VariableKey {
                    variable: "service".to_string(),
                },
                distinct: false,
                alias: "services".to_string(),
            },
            Projection::Aggregate {
                function: super::AggregateFunction::Count,
                target: AggregateTarget::VariableKey {
                    variable: "service".to_string(),
                },
                distinct: true,
                alias: "distinct_services".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("services".to_string()),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_collect_graph_variable_projection() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN collect(service) AS service_ids, \
                    collect(DISTINCT service) AS distinct_service_ids, \
                    collect(owns) AS ownership_ids",
    )
    .expect("collect graph variable query should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Aggregate {
                function: super::AggregateFunction::Collect,
                target: AggregateTarget::VariableKey {
                    variable: "service".to_string(),
                },
                distinct: false,
                alias: "service_ids".to_string(),
            },
            Projection::Aggregate {
                function: super::AggregateFunction::Collect,
                target: AggregateTarget::VariableKey {
                    variable: "service".to_string(),
                },
                distinct: true,
                alias: "distinct_service_ids".to_string(),
            },
            Projection::Aggregate {
                function: super::AggregateFunction::Collect,
                target: AggregateTarget::VariableKey {
                    variable: "owns".to_string(),
                },
                distinct: false,
                alias: "ownership_ids".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_aggregate_scalar_expression_targets() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN collect(coalesce(service.tier, 'unknown')) AS tiers, \
                    count(coalesce(service.tier, 'unknown')) AS tier_count, \
                    sum(service.risk + 1) AS adjusted_risk, \
                    collect(({tier: service.tier}).tier) AS selected_tiers, \
                    sum(({risk: service.risk + 1}).risk) AS selected_adjusted_risk, \
                    count(({kind: 'service'}).kind) AS literal_kind_count",
    )
    .expect("aggregate expression target query should compile");

    assert_eq!(plan.projections.len(), 6);
    assert!(matches!(
        plan.projections
            .first()
            .expect("collect projection should be present"),
        Projection::Aggregate {
            function: super::AggregateFunction::Collect,
            target: AggregateTarget::Expression(ScalarExpression::Coalesce { .. }),
            alias,
            ..
        } if alias == "tiers"
    ));
    assert!(matches!(
        plan.projections
            .get(1)
            .expect("count projection should be present"),
        Projection::Aggregate {
            function: super::AggregateFunction::Count,
            target: AggregateTarget::Expression(ScalarExpression::Coalesce { .. }),
            alias,
            ..
        } if alias == "tier_count"
    ));
    assert!(matches!(
        plan.projections
            .get(2)
            .expect("sum projection should be present"),
        Projection::Aggregate {
            function: super::AggregateFunction::Sum,
            target: AggregateTarget::Expression(ScalarExpression::Arithmetic { .. }),
            alias,
            ..
        } if alias == "adjusted_risk"
    ));
    assert!(matches!(
        plan.projections
            .get(3)
            .expect("selected property collect projection should be present"),
        Projection::Aggregate {
            function: super::AggregateFunction::Collect,
            target: AggregateTarget::Property(PropertyRef { property, .. }),
            alias,
            ..
        } if property == "tier" && alias == "selected_tiers"
    ));
    assert!(matches!(
        plan.projections
            .get(4)
            .expect("selected expression sum projection should be present"),
        Projection::Aggregate {
            function: super::AggregateFunction::Sum,
            target: AggregateTarget::Expression(ScalarExpression::Arithmetic { .. }),
            alias,
            ..
        } if alias == "selected_adjusted_risk"
    ));
    assert!(matches!(
        plan.projections
            .get(5)
            .expect("selected literal count projection should be present"),
        Projection::Aggregate {
            function: super::AggregateFunction::Count,
            target: AggregateTarget::Expression(ScalarExpression::Literal(
                Literal::String(value)
            )),
            alias,
            ..
        } if value == "service" && alias == "literal_kind_count"
    ));
}

#[test]
fn compiles_predicate_aggregate_targets() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN collect(service.risk > 0.8) AS high_risk_flags, \
                    count(service.tier IS NULL) AS tier_null_checks",
    )
    .expect("predicate aggregate target query should compile");

    assert_eq!(plan.projections.len(), 2);
    assert!(matches!(
        plan.projections
            .first()
            .expect("collect projection should be present"),
        Projection::Aggregate {
            function: super::AggregateFunction::Collect,
            target: AggregateTarget::Expression(ScalarExpression::Predicate(_)),
            alias,
            ..
        } if alias == "high_risk_flags"
    ));
    assert!(matches!(
        plan.projections
            .get(1)
            .expect("count projection should be present"),
        Projection::Aggregate {
            function: super::AggregateFunction::Count,
            target: AggregateTarget::Expression(ScalarExpression::Predicate(_)),
            alias,
            ..
        } if alias == "tier_null_checks"
    ));
}

#[test]
fn rejects_order_by_unknown_aliases() {
    assert_unsupported("MATCH (service:Service) RETURN service.name AS name ORDER BY missing");
}

#[test]
fn rejects_unsupported_return_functions() {
    assert_unsupported("MATCH (service:Service) RETURN id(missing)");
    assert_unsupported("MATCH (service:Service) RETURN type(service)");
}

#[test]
fn compiles_order_by_aggregate_aliases() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN count(*) AS services \
             ORDER BY services DESC",
    )
    .expect("aggregate alias ordering should compile");

    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("services".to_string()),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_order_by_count_star_expression() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN service.tier AS tier, count(*) AS services \
             ORDER BY count(*) DESC, tier",
    )
    .expect("count(*) order expression should compile");

    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::ProjectionAlias("services".to_string()),
                direction: OrderDirection::Descending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_order_by_aggregate_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN count(service) AS services, avg(service.risk) AS average_risk \
             ORDER BY count(service) DESC, avg(service.risk)",
    )
    .expect("aggregate order expressions should compile");

    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::ProjectionAlias("services".to_string()),
                direction: OrderDirection::Descending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::ProjectionAlias("average_risk".to_string()),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_unprojected_order_by_aggregate_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN service.tier AS tier \
             ORDER BY count(*) DESC, avg(service.risk), tier",
    )
    .expect("hidden aggregate order expressions should compile");

    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::CountAll,
                direction: OrderDirection::Descending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Aggregate {
                    function: AggregateFunction::Avg,
                    target: AggregateTarget::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    }),
                    distinct: false,
                },
                direction: OrderDirection::Ascending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

fn assert_unsupported(cypher: &str) {
    let error = compile_cypher(cypher).expect_err("query should be rejected");
    assert!(
        error.to_string().contains("UNSUPPORTED_CYPHER"),
        "unexpected error: {error}"
    );
}

fn path_length_projection_literal(plan: &GraphPlan) -> Option<i64> {
    plan.projections.iter().find_map(|projection| {
        let Projection::Expression {
            expression: ScalarExpression::Literal(Literal::Integer(length)),
            alias,
        } = projection
        else {
            return None;
        };
        (alias == "hops").then_some(*length)
    })
}

fn predicate_contains_boolean_false(predicate: Option<&PredicateExpression>) -> bool {
    match predicate {
        Some(PredicateExpression::Boolean(false)) => true,
        Some(
            PredicateExpression::And { left, right }
            | PredicateExpression::Or { left, right }
            | PredicateExpression::Xor { left, right },
        ) => {
            predicate_contains_boolean_false(Some(left))
                || predicate_contains_boolean_false(Some(right))
        }
        Some(PredicateExpression::Not { expression }) => {
            predicate_contains_boolean_false(Some(expression))
        }
        Some(
            PredicateExpression::Boolean(true)
            | PredicateExpression::Comparison(_)
            | PredicateExpression::KeyComparison(_)
            | PredicateExpression::ElementIdComparison(_)
            | PredicateExpression::Presence(_)
            | PredicateExpression::PropertyKeyMembership(_)
            | PredicateExpression::ExistsPattern(_)
            | PredicateExpression::ScalarComparison(_),
        )
        | None => false,
    }
}
