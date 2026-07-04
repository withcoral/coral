use super::*;
use crate::virtual_graph::ir::{
    AggregateFunction, AggregateTarget, ComparisonOperator, CountSubqueryPattern, Direction,
    ExistsPatternPredicate, GraphPlan, GraphQuery, GraphStage, GraphStageExport, GraphStagedQuery,
    KeyPredicate, Literal, NodePattern, OptionalMatchScope, OrderDirection, OrderExpression,
    OrderKey, PredicateExpression, PredicateRhs, Projection, ProjectionPredicate,
    ProjectionPredicateExpression, ProjectionPredicateRhs, PropertyPredicate, PropertyRef,
    RelationshipPattern, ScalarExpression, ScalarPredicate, ScalarPredicateRhs, TemporalExpr,
};

const GRAPH: &str = r"
version: 1
name: ownership
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
      risk: risk_score
relationships:
  - type: OWNS
    table: { schema: ops, name: ownerships }
    key: ownership_id
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
    properties:
      since: since
  - type: DEPENDS_ON
    table: { schema: ops, name: service_dependencies }
    from: { label: Service, key: from_service_id }
    to: { label: Service, key: to_service_id }
    properties:
      criticality: criticality
";

const STAGED_GRAPH: &str = r"
version: 1
name: staged
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
";

fn date_expression(year: i64, month: i64, day: i64) -> ScalarExpression {
    ScalarExpression::Temporal(TemporalExpr::MakeDate {
        year: Box::new(ScalarExpression::Literal(Literal::Integer(year))),
        month: Box::new(ScalarExpression::Literal(Literal::Integer(month))),
        day: Box::new(ScalarExpression::Literal(Literal::Integer(day))),
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

#[test]
fn lower_graph_plan_renders_forward_relationship_sql() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = ownership_plan(Direction::Outgoing);

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("plan should lower to SQL");

    assert_eq!(
        translation.sql(),
        "SELECT \"n0\".\"full_name\" AS \"owner\", \"n1\".\"service_name\" AS \"service\" \
             FROM \"ops\".\"people\" AS \"n0\" \
             JOIN \"ops\".\"ownerships\" AS \"r0\" ON \"r0\".\"person_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n1\" ON \"r0\".\"service_id\" = \"n1\".\"id\" \
             WHERE \"n1\".\"tier\" = 'prod' ORDER BY \"n0\".\"full_name\" ASC LIMIT 25"
    );
}

#[test]
fn lower_graph_query_renders_staged_with_order_limit_cte() {
    let graph = Declaration::from_yaml(STAGED_GRAPH).expect("graph should parse");
    let query = GraphQuery::Staged(GraphStagedQuery {
        stages: vec![GraphStage {
            plan: GraphPlan {
                nodes: vec![NodePattern {
                    variable: "a".to_string(),
                    label: "Person".to_string(),
                }],
                relationships: Vec::new(),
                optional_relationships: Vec::new(),
                optional_matches: Vec::new(),
                distinct: false,
                projections: vec![Projection::Key {
                    variable: "a".to_string(),
                    alias: "a_id".to_string(),
                }],
                predicates: Vec::new(),
                predicate: None,
                post_projection_predicate: None,
                order_by: vec![OrderKey {
                    expression: OrderExpression::Property(PropertyRef {
                        variable: "a".to_string(),
                        property: "age".to_string(),
                    }),
                    direction: OrderDirection::Ascending,
                    nulls: None,
                }],
                skip: None,
                limit: Some(2),
            },
            exports: vec![GraphStageExport::NodeKey {
                variable: "a".to_string(),
                column: "a_id".to_string(),
            }],
        }],
        final_plan: GraphPlan {
            nodes: vec![
                NodePattern {
                    variable: "a".to_string(),
                    label: "Person".to_string(),
                },
                NodePattern {
                    variable: "b".to_string(),
                    label: "Person".to_string(),
                },
            ],
            relationships: vec![RelationshipPattern {
                variable: None,
                relationship_type: "KNOWS".to_string(),
                left: "a".to_string(),
                direction: Direction::Outgoing,
                right: "b".to_string(),
            }],
            optional_relationships: Vec::new(),
            optional_matches: Vec::new(),
            distinct: false,
            projections: vec![
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
            ],
            predicates: Vec::new(),
            predicate: None,
            post_projection_predicate: None,
            order_by: Vec::new(),
            skip: None,
            limit: None,
        },
    });

    let translation = graph
        .lower_graph_query(&query)
        .expect("staged graph query should lower");

    assert_eq!(
        translation.sql(),
        "WITH \"stage0\" AS (SELECT \"n0\".\"id\" AS \"a_id\" \
             FROM \"ops\".\"people\" AS \"n0\" ORDER BY \"n0\".\"age\" ASC LIMIT 2) \
             SELECT \"n0\".\"full_name\" AS \"a\", \"n1\".\"full_name\" AS \"b\" \
             FROM \"stage0\" AS \"stage0\" \
             JOIN \"ops\".\"people\" AS \"n0\" ON \"n0\".\"id\" = \"stage0\".\"a_id\" \
             JOIN \"ops\".\"knows\" AS \"r0\" ON \"r0\".\"person_id\" = \"stage0\".\"a_id\" \
             JOIN \"ops\".\"people\" AS \"n1\" ON \"r0\".\"friend_id\" = \"n1\".\"id\""
    );
}

#[test]
fn lower_graph_query_renders_staged_incoming_final_match() {
    let graph = Declaration::from_yaml(STAGED_GRAPH).expect("graph should parse");
    let query = staged_order_limit_query(RelationshipPattern {
        variable: None,
        relationship_type: "KNOWS".to_string(),
        left: "b".to_string(),
        direction: Direction::Outgoing,
        right: "a".to_string(),
    });

    let translation = graph
        .lower_graph_query(&query)
        .expect("staged incoming graph query should lower");

    assert_eq!(
        translation.sql(),
        "WITH \"stage0\" AS (SELECT \"n0\".\"id\" AS \"a_id\" \
             FROM \"ops\".\"people\" AS \"n0\" ORDER BY \"n0\".\"age\" ASC LIMIT 2) \
             SELECT \"n0\".\"full_name\" AS \"a\", \"n1\".\"full_name\" AS \"b\" \
             FROM \"stage0\" AS \"stage0\" \
             JOIN \"ops\".\"people\" AS \"n0\" ON \"n0\".\"id\" = \"stage0\".\"a_id\" \
             JOIN \"ops\".\"knows\" AS \"r0\" ON \"r0\".\"friend_id\" = \"stage0\".\"a_id\" \
             JOIN \"ops\".\"people\" AS \"n1\" ON \"r0\".\"person_id\" = \"n1\".\"id\""
    );
}

#[test]
fn lower_graph_query_renders_staged_undirected_final_match() {
    let graph = Declaration::from_yaml(STAGED_GRAPH).expect("graph should parse");
    let query = staged_order_limit_query(RelationshipPattern {
        variable: None,
        relationship_type: "KNOWS".to_string(),
        left: "a".to_string(),
        direction: Direction::Undirected,
        right: "b".to_string(),
    });

    let translation = graph
        .lower_graph_query(&query)
        .expect("staged undirected graph query should lower");

    assert_eq!(
        translation.sql(),
        "WITH \"stage0\" AS (SELECT \"n0\".\"id\" AS \"a_id\" \
             FROM \"ops\".\"people\" AS \"n0\" ORDER BY \"n0\".\"age\" ASC LIMIT 2) \
             SELECT \"n0\".\"full_name\" AS \"a\", \"n1\".\"full_name\" AS \"b\" \
             FROM \"stage0\" AS \"stage0\" \
             JOIN \"ops\".\"people\" AS \"n0\" ON \"n0\".\"id\" = \"stage0\".\"a_id\" \
             JOIN \"ops\".\"knows\" AS \"r0\" ON (\"r0\".\"person_id\" = \"stage0\".\"a_id\" OR \"r0\".\"friend_id\" = \"stage0\".\"a_id\") \
             JOIN \"ops\".\"people\" AS \"n1\" ON ((\"r0\".\"person_id\" = \"stage0\".\"a_id\" AND \"r0\".\"friend_id\" = \"n1\".\"id\") OR (\"r0\".\"friend_id\" = \"stage0\".\"a_id\" AND \"r0\".\"person_id\" = \"n1\".\"id\"))"
    );
}

fn staged_order_limit_query(final_relationship: RelationshipPattern) -> GraphQuery {
    GraphQuery::Staged(GraphStagedQuery {
        stages: vec![GraphStage {
            plan: GraphPlan {
                nodes: vec![NodePattern {
                    variable: "a".to_string(),
                    label: "Person".to_string(),
                }],
                relationships: Vec::new(),
                optional_relationships: Vec::new(),
                optional_matches: Vec::new(),
                distinct: false,
                projections: vec![Projection::Key {
                    variable: "a".to_string(),
                    alias: "a_id".to_string(),
                }],
                predicates: Vec::new(),
                predicate: None,
                post_projection_predicate: None,
                order_by: vec![OrderKey {
                    expression: OrderExpression::Property(PropertyRef {
                        variable: "a".to_string(),
                        property: "age".to_string(),
                    }),
                    direction: OrderDirection::Ascending,
                    nulls: None,
                }],
                skip: None,
                limit: Some(2),
            },
            exports: vec![GraphStageExport::NodeKey {
                variable: "a".to_string(),
                column: "a_id".to_string(),
            }],
        }],
        final_plan: GraphPlan {
            nodes: vec![
                NodePattern {
                    variable: "a".to_string(),
                    label: "Person".to_string(),
                },
                NodePattern {
                    variable: "b".to_string(),
                    label: "Person".to_string(),
                },
            ],
            relationships: vec![final_relationship],
            optional_relationships: Vec::new(),
            optional_matches: Vec::new(),
            distinct: false,
            projections: vec![
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
            ],
            predicates: Vec::new(),
            predicate: None,
            post_projection_predicate: None,
            order_by: Vec::new(),
            skip: None,
            limit: None,
        },
    })
}

#[test]
fn lower_graph_query_renders_staged_aggregate_cte() {
    let graph = Declaration::from_yaml(STAGED_GRAPH).expect("graph should parse");
    let query = staged_aggregate_query();

    let translation = graph
        .lower_graph_query(&query)
        .expect("staged aggregate graph query should lower");

    assert_eq!(
        translation.sql(),
        "WITH \"stage0\" AS (SELECT \"n0\".\"id\" AS \"a_id\", COUNT(\"n1\".\"id\") AS \"deg\" \
             FROM \"ops\".\"people\" AS \"n0\" \
             JOIN \"ops\".\"knows\" AS \"r0\" ON \"r0\".\"person_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"people\" AS \"n1\" ON \"r0\".\"friend_id\" = \"n1\".\"id\" \
             GROUP BY \"n0\".\"id\") \
             SELECT \"n0\".\"full_name\" AS \"name\", \"stage0\".\"deg\" AS \"deg\" \
             FROM \"stage0\" AS \"stage0\" \
             JOIN \"ops\".\"people\" AS \"n0\" ON \"n0\".\"id\" = \"stage0\".\"a_id\" \
             JOIN \"ops\".\"knows\" AS \"r0\" ON \"r0\".\"person_id\" = \"stage0\".\"a_id\" \
             JOIN \"ops\".\"people\" AS \"n1\" ON \"r0\".\"friend_id\" = \"n1\".\"id\""
    );
}

fn staged_aggregate_query() -> GraphQuery {
    GraphQuery::Staged(GraphStagedQuery {
        stages: vec![GraphStage {
            plan: staged_aggregate_stage_plan(),
            exports: vec![
                GraphStageExport::NodeKey {
                    variable: "a".to_string(),
                    column: "a_id".to_string(),
                },
                GraphStageExport::AggregateValue {
                    alias: "deg".to_string(),
                    column: "deg".to_string(),
                },
            ],
        }],
        final_plan: staged_aggregate_final_plan(),
    })
}

fn staged_aggregate_stage_plan() -> GraphPlan {
    GraphPlan {
        nodes: vec![
            NodePattern {
                variable: "a".to_string(),
                label: "Person".to_string(),
            },
            NodePattern {
                variable: "b".to_string(),
                label: "Person".to_string(),
            },
        ],
        relationships: vec![RelationshipPattern {
            variable: None,
            relationship_type: "KNOWS".to_string(),
            left: "a".to_string(),
            direction: Direction::Outgoing,
            right: "b".to_string(),
        }],
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![
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
        ],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    }
}

fn staged_aggregate_final_plan() -> GraphPlan {
    GraphPlan {
        nodes: vec![
            NodePattern {
                variable: "a".to_string(),
                label: "Person".to_string(),
            },
            NodePattern {
                variable: "c".to_string(),
                label: "Person".to_string(),
            },
        ],
        relationships: vec![RelationshipPattern {
            variable: None,
            relationship_type: "KNOWS".to_string(),
            left: "a".to_string(),
            direction: Direction::Outgoing,
            right: "c".to_string(),
        }],
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![
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
        ],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    }
}

#[test]
fn lower_graph_plan_renders_disconnected_components_as_cross_joins() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![
            NodePattern {
                variable: "source".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "target".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "person".to_string(),
                label: "Person".to_string(),
            },
        ],
        relationships: vec![RelationshipPattern {
            variable: None,
            relationship_type: "DEPENDS_ON".to_string(),
            left: "source".to_string(),
            direction: Direction::Outgoing,
            right: "target".to_string(),
        }],
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![
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
            Projection::Property {
                property: PropertyRef {
                    variable: "person".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("person".to_string()),
            },
        ],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("disconnected mandatory components should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"n0\".\"service_name\" AS \"source\", \"n1\".\"service_name\" AS \"target\", \"n2\".\"full_name\" AS \"person\" \
             FROM \"ops\".\"services\" AS \"n0\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r0\" ON \"r0\".\"from_service_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n1\" ON \"r0\".\"to_service_id\" = \"n1\".\"id\" \
             CROSS JOIN \"ops\".\"people\" AS \"n2\""
    );
}

#[test]
fn lower_graph_plan_renders_reverse_relationship_sql() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![
            NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "person".to_string(),
                label: "Person".to_string(),
            },
        ],
        relationships: vec![RelationshipPattern {
            variable: None,
            relationship_type: "OWNS".to_string(),
            left: "service".to_string(),
            direction: Direction::Incoming,
            right: "person".to_string(),
        }],
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![Projection::Property {
            property: PropertyRef {
                variable: "person".to_string(),
                property: "name".to_string(),
            },
            alias: Some("owner".to_string()),
        }],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("reverse relationship should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"n1\".\"full_name\" AS \"owner\" FROM \"ops\".\"services\" AS \"n0\" \
             JOIN \"ops\".\"ownerships\" AS \"r0\" ON \"r0\".\"service_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"people\" AS \"n1\" ON \"r0\".\"person_id\" = \"n1\".\"id\""
    );
}

#[test]
fn lower_graph_plan_renders_optional_relationship_sql() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![
            NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "person".to_string(),
                label: "Person".to_string(),
            },
        ],
        relationships: vec![RelationshipPattern {
            variable: None,
            relationship_type: "OWNS".to_string(),
            left: "person".to_string(),
            direction: Direction::Outgoing,
            right: "service".to_string(),
        }],
        optional_relationships: vec![0],
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("service".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "person".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("owner".to_string()),
            },
        ],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("optional relationship should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"n0\".\"service_name\" AS \"service\", \"n1\".\"full_name\" AS \"owner\" \
             FROM \"ops\".\"services\" AS \"n0\" \
             LEFT JOIN \"ops\".\"ownerships\" AS \"r0\" ON \"r0\".\"service_id\" = \"n0\".\"id\" \
             LEFT JOIN \"ops\".\"people\" AS \"n1\" ON \"r0\".\"person_id\" = \"n1\".\"id\""
    );
}

#[test]
fn lower_graph_plan_renders_optional_relationship_from_disconnected_component() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![
            NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "person".to_string(),
                label: "Person".to_string(),
            },
            NodePattern {
                variable: "owned".to_string(),
                label: "Service".to_string(),
            },
        ],
        relationships: vec![RelationshipPattern {
            variable: None,
            relationship_type: "OWNS".to_string(),
            left: "person".to_string(),
            direction: Direction::Outgoing,
            right: "owned".to_string(),
        }],
        optional_relationships: vec![0],
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("service".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "person".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("person".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "owned".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("owned".to_string()),
            },
        ],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("optional relationship from disconnected component should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"n0\".\"service_name\" AS \"service\", \"n1\".\"full_name\" AS \"person\", \"n2\".\"service_name\" AS \"owned\" \
             FROM \"ops\".\"services\" AS \"n0\" \
             CROSS JOIN \"ops\".\"people\" AS \"n1\" \
             LEFT JOIN \"ops\".\"ownerships\" AS \"r0\" ON \"r0\".\"person_id\" = \"n1\".\"id\" \
             LEFT JOIN \"ops\".\"services\" AS \"n2\" ON \"r0\".\"service_id\" = \"n2\".\"id\""
    );
}

#[test]
fn lower_graph_plan_renders_multihop_optional_scope_as_grouped_left_join() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![
            NodePattern {
                variable: "service".to_string(),
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
        ],
        relationships: vec![
            RelationshipPattern {
                variable: None,
                relationship_type: "DEPENDS_ON".to_string(),
                left: "service".to_string(),
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
        ],
        optional_relationships: vec![0, 1],
        optional_matches: vec![OptionalMatchScope {
            node_indices: vec![1, 2],
            relationship_indices: vec![0, 1],
            predicate: None,
        }],
        distinct: false,
        projections: vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("service".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "middle".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("middle".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "target".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("target".to_string()),
            },
        ],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("multi-hop optional scope should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"n0\".\"service_name\" AS \"service\", \"n1\".\"service_name\" AS \"middle\", \"n2\".\"service_name\" AS \"target\" \
             FROM \"ops\".\"services\" AS \"n0\" \
             LEFT JOIN (\"ops\".\"service_dependencies\" AS \"r0\" \
             JOIN \"ops\".\"services\" AS \"n1\" ON \"r0\".\"to_service_id\" = \"n1\".\"id\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r1\" ON \"r1\".\"from_service_id\" = \"n1\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n2\" ON \"r1\".\"to_service_id\" = \"n2\".\"id\") \
             ON \"r0\".\"from_service_id\" = \"n0\".\"id\""
    );
}

#[test]
fn lower_graph_plan_renders_multihop_optional_scope_between_bound_endpoints_as_grouped_left_join() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![
            NodePattern {
                variable: "source".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "target".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "middle".to_string(),
                label: "Service".to_string(),
            },
        ],
        relationships: vec![
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
        ],
        optional_relationships: vec![0, 1],
        optional_matches: vec![OptionalMatchScope {
            node_indices: vec![2],
            relationship_indices: vec![0, 1],
            predicate: None,
        }],
        distinct: false,
        projections: vec![
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
            Projection::Property {
                property: PropertyRef {
                    variable: "middle".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("middle".to_string()),
            },
        ],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("bound-endpoint multi-hop optional scope should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"n0\".\"service_name\" AS \"source\", \"n1\".\"service_name\" AS \"target\", \"n2\".\"service_name\" AS \"middle\" \
             FROM \"ops\".\"services\" AS \"n0\" \
             CROSS JOIN \"ops\".\"services\" AS \"n1\" \
             LEFT JOIN (\"ops\".\"service_dependencies\" AS \"r0\" \
             JOIN \"ops\".\"services\" AS \"n2\" ON \"r0\".\"to_service_id\" = \"n2\".\"id\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r1\" ON \"r1\".\"from_service_id\" = \"n2\".\"id\") \
             ON (\"r0\".\"from_service_id\" = \"n0\".\"id\") AND (\"r1\".\"to_service_id\" = \"n1\".\"id\")"
    );
}

#[test]
fn lower_graph_plan_renders_optional_predicates_inside_join_scope() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![
            NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "person".to_string(),
                label: "Person".to_string(),
            },
        ],
        relationships: vec![RelationshipPattern {
            variable: Some("owns".to_string()),
            relationship_type: "OWNS".to_string(),
            left: "person".to_string(),
            direction: Direction::Outgoing,
            right: "service".to_string(),
        }],
        optional_relationships: vec![0],
        optional_matches: vec![OptionalMatchScope {
            node_indices: vec![1],
            relationship_indices: vec![0],
            predicate: Some(PredicateExpression::Comparison(PropertyPredicate {
                property: PropertyRef {
                    variable: "person".to_string(),
                    property: "team".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("platform".to_string())),
            })),
        }],
        distinct: false,
        projections: vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("service".to_string()),
            },
            Projection::Key {
                variable: "owns".to_string(),
                alias: "ownership_id".to_string(),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "person".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("owner".to_string()),
            },
        ],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("optional predicate should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"n0\".\"service_name\" AS \"service\", \"r0\".\"ownership_id\" AS \"ownership_id\", \"n1\".\"full_name\" AS \"owner\" \
             FROM \"ops\".\"services\" AS \"n0\" \
             LEFT JOIN (\"ops\".\"ownerships\" AS \"r0\" JOIN \"ops\".\"people\" AS \"n1\" ON \"r0\".\"person_id\" = \"n1\".\"id\") \
             ON (\"r0\".\"service_id\" = \"n0\".\"id\") AND (\"n1\".\"team\" = 'platform')"
    );
}

#[test]
fn lower_graph_plan_renders_undirected_optional_predicates_inside_join_scope() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![
            NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "dependency".to_string(),
                label: "Service".to_string(),
            },
        ],
        relationships: vec![RelationshipPattern {
            variable: Some("dependency_edge".to_string()),
            relationship_type: "DEPENDS_ON".to_string(),
            left: "service".to_string(),
            direction: Direction::Undirected,
            right: "dependency".to_string(),
        }],
        optional_relationships: vec![0],
        optional_matches: vec![OptionalMatchScope {
            node_indices: vec![1],
            relationship_indices: vec![0],
            predicate: Some(PredicateExpression::Comparison(PropertyPredicate {
                property: PropertyRef {
                    variable: "dependency".to_string(),
                    property: "tier".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("dev".to_string())),
            })),
        }],
        distinct: false,
        projections: vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("service".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "dependency".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("dependency".to_string()),
            },
        ],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("undirected optional predicate should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"n0\".\"service_name\" AS \"service\", \"n1\".\"service_name\" AS \"dependency\" \
             FROM \"ops\".\"services\" AS \"n0\" \
             LEFT JOIN (\"ops\".\"service_dependencies\" AS \"r0\" JOIN \"ops\".\"services\" AS \"n1\" ON (\"r0\".\"to_service_id\" = \"n1\".\"id\" OR \"r0\".\"from_service_id\" = \"n1\".\"id\")) \
             ON (((\"r0\".\"from_service_id\" = \"n0\".\"id\" AND \"r0\".\"to_service_id\" = \"n1\".\"id\") OR (\"r0\".\"to_service_id\" = \"n0\".\"id\" AND \"r0\".\"from_service_id\" = \"n1\".\"id\"))) AND (\"n1\".\"tier\" = 'dev')"
    );
}

#[test]
fn lower_graph_plan_renders_undirected_distinct_label_relationship_sql() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![
            NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "person".to_string(),
                label: "Person".to_string(),
            },
        ],
        relationships: vec![RelationshipPattern {
            variable: None,
            relationship_type: "OWNS".to_string(),
            left: "service".to_string(),
            direction: Direction::Undirected,
            right: "person".to_string(),
        }],
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![Projection::Property {
            property: PropertyRef {
                variable: "person".to_string(),
                property: "name".to_string(),
            },
            alias: Some("owner".to_string()),
        }],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("undirected relationship should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"n1\".\"full_name\" AS \"owner\" FROM \"ops\".\"services\" AS \"n0\" \
             JOIN \"ops\".\"ownerships\" AS \"r0\" ON \"r0\".\"service_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"people\" AS \"n1\" ON \"r0\".\"person_id\" = \"n1\".\"id\""
    );
}

#[test]
fn lower_graph_plan_renders_undirected_same_label_relationship_sql() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![
            NodePattern {
                variable: "source".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "neighbor".to_string(),
                label: "Service".to_string(),
            },
        ],
        relationships: vec![RelationshipPattern {
            variable: None,
            relationship_type: "DEPENDS_ON".to_string(),
            left: "source".to_string(),
            direction: Direction::Undirected,
            right: "neighbor".to_string(),
        }],
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![Projection::Property {
            property: PropertyRef {
                variable: "neighbor".to_string(),
                property: "name".to_string(),
            },
            alias: Some("neighbor".to_string()),
        }],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("undirected same-label relationship should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"n1\".\"service_name\" AS \"neighbor\" FROM \"ops\".\"services\" AS \"n0\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r0\" ON (\"r0\".\"from_service_id\" = \"n0\".\"id\" OR \"r0\".\"to_service_id\" = \"n0\".\"id\") \
             JOIN \"ops\".\"services\" AS \"n1\" ON ((\"r0\".\"from_service_id\" = \"n0\".\"id\" AND \"r0\".\"to_service_id\" = \"n1\".\"id\") OR (\"r0\".\"to_service_id\" = \"n0\".\"id\" AND \"r0\".\"from_service_id\" = \"n1\".\"id\"))"
    );
}

#[test]
fn lower_graph_plan_renders_identity_and_static_function_projections() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.relationships
        .get_mut(0)
        .expect("ownership plan should include one relationship")
        .variable = Some("owns".to_string());
    plan.projections = vec![
        Projection::Key {
            variable: "person".to_string(),
            alias: "person_id".to_string(),
        },
        Projection::NodeLabels {
            variable: "person".to_string(),
            label: "Person".to_string(),
            alias: "person_labels".to_string(),
        },
        Projection::PropertyKeys {
            variable: "person".to_string(),
            alias: "person_keys".to_string(),
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
        Projection::PropertyKeys {
            variable: "owns".to_string(),
            alias: "relationship_keys".to_string(),
        },
    ];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("identity and static function projections should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"n0\".\"id\" AS \"person_id\", CASE WHEN \"n0\".\"id\" IS NULL THEN NULL ELSE make_array('Person') END AS \"person_labels\", CASE WHEN \"n0\".\"id\" IS NULL THEN NULL ELSE make_array('name', 'team') END AS \"person_keys\", \"r0\".\"ownership_id\" AS \"ownership_id\", CASE WHEN \"r0\".\"ownership_id\" IS NULL THEN NULL ELSE 'OWNS' END AS \"relationship_type\", CASE WHEN \"r0\".\"ownership_id\" IS NULL THEN NULL ELSE make_array('since') END AS \"relationship_keys\" \
             FROM \"ops\".\"people\" AS \"n0\" \
             JOIN \"ops\".\"ownerships\" AS \"r0\" ON \"r0\".\"person_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n1\" ON \"r0\".\"service_id\" = \"n1\".\"id\" \
             WHERE \"n1\".\"tier\" = 'prod' ORDER BY \"n0\".\"full_name\" ASC LIMIT 25"
    );
}

#[test]
fn lower_graph_plan_renders_property_keys_ordering() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::PropertyKeys {
            variable: "service".to_string(),
        },
        direction: OrderDirection::Descending,
        nulls: None,
    }];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("property key ordering should lower");

    assert!(
        translation.sql().contains(
            "ORDER BY CASE WHEN \"n1\".\"id\" IS NULL THEN NULL ELSE \
                 make_array('name', 'risk', 'tier') END DESC"
        ),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_scalar_post_projection_predicates_as_where() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.post_projection_predicate = Some(ProjectionPredicateExpression::Comparison(
        ProjectionPredicate {
            alias: "owner".to_string(),
            operator: ComparisonOperator::StartsWith,
            rhs: ProjectionPredicateRhs::Literal(Literal::String("Ada".to_string())),
        },
    ));

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("post-projection predicate should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"n0\".\"full_name\" AS \"owner\", \"n1\".\"service_name\" AS \"service\" \
             FROM \"ops\".\"people\" AS \"n0\" \
             JOIN \"ops\".\"ownerships\" AS \"r0\" ON \"r0\".\"person_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n1\" ON \"r0\".\"service_id\" = \"n1\".\"id\" \
             WHERE \"n1\".\"tier\" = 'prod' AND \"n0\".\"full_name\" LIKE 'Ada%' ESCAPE '\\' \
             ORDER BY \"n0\".\"full_name\" ASC LIMIT 25"
    );
}

#[test]
fn lower_graph_plan_renders_xor_post_projection_predicates() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.post_projection_predicate = Some(ProjectionPredicateExpression::Xor {
        left: Box::new(ProjectionPredicateExpression::Comparison(
            ProjectionPredicate {
                alias: "owner".to_string(),
                operator: ComparisonOperator::StartsWith,
                rhs: ProjectionPredicateRhs::Literal(Literal::String("Ada".to_string())),
            },
        )),
        right: Box::new(ProjectionPredicateExpression::Comparison(
            ProjectionPredicate {
                alias: "service".to_string(),
                operator: ComparisonOperator::Contains,
                rhs: ProjectionPredicateRhs::Literal(Literal::String("api".to_string())),
            },
        )),
    });

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("post-projection XOR predicate should lower");

    assert!(
            translation.sql().contains(
                "WHERE \"n1\".\"tier\" = 'prod' AND ((\"n0\".\"full_name\" LIKE 'Ada%' ESCAPE '\\' AND NOT (\"n1\".\"service_name\" LIKE '%api%' ESCAPE '\\')) OR (NOT (\"n0\".\"full_name\" LIKE 'Ada%' ESCAPE '\\') AND \"n1\".\"service_name\" LIKE '%api%' ESCAPE '\\'))"
            ),
            "{}",
            translation.sql()
        );
}

#[test]
fn lower_graph_plan_renders_aggregate_post_projection_predicates_as_having() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.projections = vec![
        Projection::Property {
            property: PropertyRef {
                variable: "person".to_string(),
                property: "team".to_string(),
            },
            alias: Some("team".to_string()),
        },
        Projection::CountAll {
            alias: "service_count".to_string(),
        },
    ];
    plan.post_projection_predicate = Some(ProjectionPredicateExpression::Comparison(
        ProjectionPredicate {
            alias: "service_count".to_string(),
            operator: ComparisonOperator::GreaterThan,
            rhs: ProjectionPredicateRhs::Literal(Literal::Integer(1)),
        },
    ));
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::ProjectionAlias("service_count".to_string()),
        direction: OrderDirection::Descending,
        nulls: None,
    }];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("aggregate post-projection predicate should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"n0\".\"team\" AS \"team\", COUNT(*) AS \"service_count\" \
             FROM \"ops\".\"people\" AS \"n0\" \
             JOIN \"ops\".\"ownerships\" AS \"r0\" ON \"r0\".\"person_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n1\" ON \"r0\".\"service_id\" = \"n1\".\"id\" \
             WHERE \"n1\".\"tier\" = 'prod' GROUP BY \"n0\".\"team\" \
             HAVING COUNT(*) > 1 ORDER BY \"service_count\" DESC LIMIT 25"
    );
}

#[test]
fn lower_graph_plan_renders_relationship_between_joined_nodes() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![
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
        ],
        relationships: vec![
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
        ],
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "source".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("source".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "middle".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("middle".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "target".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("target".to_string()),
            },
        ],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("closed service dependency path should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"n0\".\"service_name\" AS \"source\", \"n1\".\"service_name\" AS \"middle\", \"n2\".\"service_name\" AS \"target\" \
             FROM \"ops\".\"services\" AS \"n0\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r0\" ON \"r0\".\"from_service_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n1\" ON \"r0\".\"to_service_id\" = \"n1\".\"id\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r1\" ON \"r1\".\"from_service_id\" = \"n1\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n2\" ON \"r1\".\"to_service_id\" = \"n2\".\"id\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r2\" ON \"r2\".\"from_service_id\" = \"n0\".\"id\" AND \"r2\".\"to_service_id\" = \"n2\".\"id\""
    );
}

#[test]
fn lower_graph_plan_reorders_connected_relationships() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![
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
        ],
        relationships: vec![
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
                right: "middle".to_string(),
            },
        ],
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![
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
        ],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("connected out-of-order relationship plan should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"n0\".\"service_name\" AS \"source\", \"n2\".\"service_name\" AS \"target\" \
             FROM \"ops\".\"services\" AS \"n0\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r1\" ON \"r1\".\"from_service_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n1\" ON \"r1\".\"to_service_id\" = \"n1\".\"id\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r0\" ON \"r0\".\"from_service_id\" = \"n1\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n2\" ON \"r0\".\"to_service_id\" = \"n2\".\"id\""
    );
}

#[test]
fn lower_graph_plan_quotes_identifiers_and_literals() {
    let graph = Declaration::from_yaml(
        r#"
version: 1
name: quoting
nodes:
  - label: Weird
    table: { schema: weird-schema, name: table"name }
    key: id"key
    properties:
      display: display"name
relationships: []
"#,
    )
    .expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![NodePattern {
            variable: "weird".to_string(),
            label: "Weird".to_string(),
        }],
        relationships: Vec::new(),
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![Projection::Property {
            property: PropertyRef {
                variable: "weird".to_string(),
                property: "display".to_string(),
            },
            alias: Some("value".to_string()),
        }],
        predicates: vec![PropertyPredicate {
            property: PropertyRef {
                variable: "weird".to_string(),
                property: "display".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Literal(Literal::String("Ada's laptop".to_string())),
        }],
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("quoted plan should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"n0\".\"display\"\"name\" AS \"value\" \
             FROM \"weird-schema\".\"table\"\"name\" AS \"n0\" \
             WHERE \"n0\".\"display\"\"name\" = 'Ada''s laptop'"
    );
}

#[test]
fn lower_graph_plan_renders_key_predicates() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
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
            variable: Some("owns".to_string()),
            relationship_type: "OWNS".to_string(),
            left: "person".to_string(),
            direction: Direction::Outgoing,
            right: "service".to_string(),
        }],
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![Projection::Property {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            },
            alias: Some("service".to_string()),
        }],
        predicates: Vec::new(),
        predicate: Some(PredicateExpression::And {
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
        }),
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("key predicates should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"n1\".\"service_name\" AS \"service\" FROM \"ops\".\"people\" AS \"n0\" \
             JOIN \"ops\".\"ownerships\" AS \"r0\" ON \"r0\".\"person_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n1\" ON \"r0\".\"service_id\" = \"n1\".\"id\" \
             WHERE (\"n0\".\"id\" = 1 AND \"r0\".\"ownership_id\" IN (100, 200))"
    );
}

#[test]
fn lower_graph_plan_renders_element_id_projection_predicate_and_ordering() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.relationships
        .first_mut()
        .expect("ownership plan should contain a relationship")
        .variable = Some("owns".to_string());
    plan.projections = vec![
        Projection::ElementId {
            variable: "person".to_string(),
            alias: "person_element_id".to_string(),
        },
        Projection::ElementId {
            variable: "owns".to_string(),
            alias: "ownership_element_id".to_string(),
        },
    ];
    plan.predicates.clear();
    plan.predicate = Some(PredicateExpression::ElementIdComparison(
        ElementIdPredicate {
            variable: "person".to_string(),
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Literal(Literal::String("1".to_string())),
        },
    ));
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::ElementId {
            variable: "owns".to_string(),
        },
        direction: OrderDirection::Descending,
        nulls: None,
    }];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("elementId() projection and predicate should lower");

    assert!(
        translation
            .sql()
            .contains("CAST(\"n0\".\"id\" AS VARCHAR) AS \"person_element_id\""),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("CAST(\"r0\".\"ownership_id\" AS VARCHAR) AS \"ownership_element_id\""),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("WHERE CAST(\"n0\".\"id\" AS VARCHAR) = '1'"),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("ORDER BY CAST(\"r0\".\"ownership_id\" AS VARCHAR) DESC"),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_key_rhs_predicates() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![
            NodePattern {
                variable: "source".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "target".to_string(),
                label: "Service".to_string(),
            },
        ],
        relationships: vec![RelationshipPattern {
            variable: None,
            relationship_type: "DEPENDS_ON".to_string(),
            left: "source".to_string(),
            direction: Direction::Outgoing,
            right: "target".to_string(),
        }],
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![Projection::Property {
            property: PropertyRef {
                variable: "source".to_string(),
                property: "name".to_string(),
            },
            alias: Some("source".to_string()),
        }],
        predicates: Vec::new(),
        predicate: Some(PredicateExpression::KeyComparison(KeyPredicate {
            variable: "source".to_string(),
            operator: ComparisonOperator::NotEqual,
            rhs: PredicateRhs::Key {
                variable: "target".to_string(),
            },
        })),
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("key RHS predicate should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"n0\".\"service_name\" AS \"source\" FROM \"ops\".\"services\" AS \"n0\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r0\" ON \"r0\".\"from_service_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n1\" ON \"r0\".\"to_service_id\" = \"n1\".\"id\" \
             WHERE \"n0\".\"id\" <> \"n1\".\"id\""
    );
}

#[test]
fn lower_graph_plan_renders_null_predicates() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![NodePattern {
            variable: "service".to_string(),
            label: "Service".to_string(),
        }],
        relationships: Vec::new(),
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![Projection::Property {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            },
            alias: Some("service".to_string()),
        }],
        predicates: vec![
            PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::Null),
            },
            PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                operator: ComparisonOperator::NotEqual,
                rhs: PredicateRhs::Literal(Literal::Null),
            },
        ],
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("null predicate plan should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"n0\".\"service_name\" AS \"service\" FROM \"ops\".\"services\" AS \"n0\" \
             WHERE \"n0\".\"tier\" IS NULL AND \"n0\".\"service_name\" IS NOT NULL"
    );
}

#[test]
fn lower_graph_plan_renders_property_rhs_predicates() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates = vec![PropertyPredicate {
        property: PropertyRef {
            variable: "person".to_string(),
            property: "team".to_string(),
        },
        operator: ComparisonOperator::Equal,
        rhs: PredicateRhs::Property(PropertyRef {
            variable: "service".to_string(),
            property: "tier".to_string(),
        }),
    }];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("property comparison should lower");

    assert!(
        translation
            .sql()
            .contains("WHERE \"n0\".\"team\" = \"n1\".\"tier\""),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_in_predicates() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates = vec![PropertyPredicate {
        property: PropertyRef {
            variable: "service".to_string(),
            property: "tier".to_string(),
        },
        operator: ComparisonOperator::In,
        rhs: PredicateRhs::List(vec![
            Literal::String("prod".to_string()),
            Literal::String("dev".to_string()),
        ]),
    }];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("IN predicate should lower");

    assert!(
        translation
            .sql()
            .contains("WHERE \"n1\".\"tier\" IN ('prod', 'dev')"),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_float_predicates() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates = vec![
        PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "risk".to_string(),
            },
            operator: ComparisonOperator::GreaterThanOrEqual,
            rhs: PredicateRhs::Literal(Literal::Float(ordered_float::OrderedFloat(0.75_f64))),
        },
        PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "risk".to_string(),
            },
            operator: ComparisonOperator::In,
            rhs: PredicateRhs::List(vec![
                Literal::Float(ordered_float::OrderedFloat(0.5_f64)),
                Literal::Float(ordered_float::OrderedFloat(0.75_f64)),
            ]),
        },
    ];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("float predicates should lower");

    assert!(
        translation.sql().contains(
            "WHERE \"n1\".\"risk_score\" >= 0.75 AND \"n1\".\"risk_score\" IN (0.5, 0.75)"
        ),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_empty_in_lists_as_false() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates = vec![PropertyPredicate {
        property: PropertyRef {
            variable: "service".to_string(),
            property: "tier".to_string(),
        },
        operator: ComparisonOperator::In,
        rhs: PredicateRhs::List(Vec::new()),
    }];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("empty IN predicate should lower");

    assert!(
        translation.sql().contains("WHERE FALSE"),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_string_predicates_as_escaped_like() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates = vec![
        PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            },
            operator: ComparisonOperator::StartsWith,
            rhs: PredicateRhs::Literal(Literal::String("bill_%".to_string())),
        },
        PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            },
            operator: ComparisonOperator::Contains,
            rhs: PredicateRhs::Literal(Literal::String("api".to_string())),
        },
    ];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("string predicates should lower");

    assert!(
            translation.sql().contains(
                "WHERE \"n1\".\"service_name\" LIKE 'bill\\_\\%%' ESCAPE '\\' AND \"n1\".\"service_name\" LIKE '%api%' ESCAPE '\\'"
            ),
            "{}",
            translation.sql()
        );
}

#[test]
fn lower_graph_plan_renders_dynamic_string_predicates_as_functions() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: service_name_expression(),
        operator: ComparisonOperator::StartsWith,
        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Left {
            expression: Box::new(service_name_expression()),
            count: Box::new(ScalarExpression::Literal(Literal::Integer(4))),
        }),
    }));

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("dynamic string predicate should lower");

    assert!(
        translation.sql().contains(
            "WHERE starts_with(\"n1\".\"service_name\", left(\"n1\".\"service_name\", 4))"
        ),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_regex_predicates() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates = vec![PropertyPredicate {
        property: PropertyRef {
            variable: "service".to_string(),
            property: "name".to_string(),
        },
        operator: ComparisonOperator::RegexMatch,
        rhs: PredicateRhs::Literal(Literal::String("^bill.*".to_string())),
    }];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("regex predicate should lower");

    assert!(
        translation
            .sql()
            .contains("WHERE regexp_like(\"n1\".\"service_name\", '^bill.*')"),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_dynamic_regex_predicates() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: service_name_expression(),
        operator: ComparisonOperator::RegexMatch,
        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Left {
            expression: Box::new(service_name_expression()),
            count: Box::new(ScalarExpression::Literal(Literal::Integer(4))),
        }),
    }));

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("dynamic regex predicate should lower");

    assert!(
        translation.sql().contains(
            "WHERE regexp_like(\"n1\".\"service_name\", left(\"n1\".\"service_name\", 4))"
        ),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_scalar_predicates() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
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
    }));

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("scalar predicate should lower");

    assert!(
        translation
            .sql()
            .contains("WHERE COALESCE(\"n1\".\"tier\", 'unassigned') IN ('prod', 'dev')"),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_relationship_type_scalar_expressions() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.relationships
        .first_mut()
        .expect("ownership plan should have a relationship")
        .variable = Some("owns".to_string());
    plan.predicates.clear();
    plan.projections = vec![Projection::Expression {
        expression: ScalarExpression::Coalesce {
            expressions: vec![
                ScalarExpression::RelationshipType {
                    variable: "owns".to_string(),
                    relationship_type: "OWNS".to_string(),
                },
                ScalarExpression::Literal(Literal::String("missing".to_string())),
            ],
        },
        alias: "relationship_type".to_string(),
    }];
    plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: ScalarExpression::RelationshipType {
            variable: "owns".to_string(),
            relationship_type: "OWNS".to_string(),
        },
        operator: ComparisonOperator::Equal,
        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
            "OWNS".to_string(),
        ))),
    }));
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::Scalar(ScalarExpression::RelationshipType {
            variable: "owns".to_string(),
            relationship_type: "OWNS".to_string(),
        }),
        direction: OrderDirection::Ascending,
        nulls: None,
    }];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("relationship type scalar expression should lower");

    assert!(
            translation.sql().contains(
                "COALESCE(CASE WHEN \"r0\".\"ownership_id\" IS NULL THEN NULL ELSE 'OWNS' END, 'missing') AS \"relationship_type\""
            ),
            "{}",
            translation.sql()
        );
    assert!(
        translation.sql().contains(
            "WHERE CASE WHEN \"r0\".\"ownership_id\" IS NULL THEN NULL ELSE 'OWNS' END = 'OWNS'"
        ),
        "{}",
        translation.sql()
    );
    assert!(
        translation.sql().contains(
            "ORDER BY CASE WHEN \"r0\".\"ownership_id\" IS NULL THEN NULL ELSE 'OWNS' END ASC"
        ),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_identity_scalar_expressions() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.relationships
        .first_mut()
        .expect("ownership plan should have a relationship")
        .variable = Some("owns".to_string());
    plan.predicates.clear();
    plan.projections = vec![
        Projection::Expression {
            expression: ScalarExpression::Arithmetic {
                operator: ArithmeticOperator::Add,
                left: Box::new(ScalarExpression::Key {
                    variable: "service".to_string(),
                }),
                right: Box::new(ScalarExpression::Literal(Literal::Integer(1))),
            },
            alias: "next_service_id".to_string(),
        },
        Projection::Expression {
            expression: ScalarExpression::Coalesce {
                expressions: vec![
                    ScalarExpression::ElementId {
                        variable: "owns".to_string(),
                    },
                    ScalarExpression::Literal(Literal::String("missing".to_string())),
                ],
            },
            alias: "ownership_element_id".to_string(),
        },
    ];
    plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: ScalarExpression::ElementId {
            variable: "owns".to_string(),
        },
        operator: ComparisonOperator::StartsWith,
        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
            "1".to_string(),
        ))),
    }));
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::Scalar(ScalarExpression::ToString {
            expression: Box::new(ScalarExpression::Key {
                variable: "service".to_string(),
            }),
        }),
        direction: OrderDirection::Ascending,
        nulls: None,
    }];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("identity scalar expressions should lower");

    assert!(
            translation.sql().contains(
                "SELECT (\"n1\".\"id\" + 1) AS \"next_service_id\", COALESCE(CAST(\"r0\".\"ownership_id\" AS VARCHAR), 'missing') AS \"ownership_element_id\""
            ),
            "{}",
            translation.sql()
        );
    assert!(
        translation
            .sql()
            .contains("WHERE CAST(\"r0\".\"ownership_id\" AS VARCHAR) LIKE '1%' ESCAPE '\\'"),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("ORDER BY CAST(\"n1\".\"id\" AS VARCHAR) ASC"),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_character_length_and_substring_expressions() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.projections = vec![
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
    ];
    plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: ScalarExpression::CharacterLength {
            expression: Box::new(ScalarExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            })),
        },
        operator: ComparisonOperator::GreaterThan,
        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(10))),
    }));
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::Scalar(ScalarExpression::Substring {
            expression: Box::new(ScalarExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            })),
            start: Box::new(ScalarExpression::Literal(Literal::Integer(0))),
            length: None,
        }),
        direction: OrderDirection::Ascending,
        nulls: None,
    }];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("string scalar expressions should lower");

    assert!(
        translation.sql().contains(
            "SELECT SUBSTRING(\"n1\".\"service_name\" FROM (0 + 1) FOR 7) AS \"prefix\", \
                 character_length(\"n1\".\"tier\") AS \"tier_length\""
        ),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("WHERE character_length(\"n1\".\"service_name\") > 10"),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("ORDER BY SUBSTRING(\"n1\".\"service_name\" FROM (0 + 1)) ASC"),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_temporal_date_expressions() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.projections = vec![
        Projection::Expression {
            expression: date_expression(1984, 10, 11),
            alias: "d".to_string(),
        },
        Projection::Expression {
            expression: ScalarExpression::ToString {
                expression: Box::new(date_expression(1984, 10, 11)),
            },
            alias: "text".to_string(),
        },
    ];
    plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: date_expression(1984, 10, 11),
        operator: ComparisonOperator::LessThan,
        rhs: ScalarPredicateRhs::Expression(date_expression(1985, 1, 1)),
    }));
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::Scalar(date_expression(1984, 10, 11)),
        direction: OrderDirection::Ascending,
        nulls: None,
    }];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("date scalar expressions should lower");

    assert!(
        translation.sql().contains(
            "SELECT make_date(1984, 10, 11) AS \"d\", CAST(make_date(1984, 10, 11) AS VARCHAR) AS \"text\""
        ),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("WHERE make_date(1984, 10, 11) < make_date(1985, 1, 1)"),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("ORDER BY make_date(1984, 10, 11) ASC"),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_temporal_localdatetime_expressions() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.projections = vec![
        Projection::Expression {
            expression: localdatetime_expression(2020, 1, 15, 12, 34, 56, 0, 0, 0),
            alias: "d".to_string(),
        },
        Projection::Expression {
            expression: ScalarExpression::ToString {
                expression: Box::new(localdatetime_expression(2020, 1, 15, 12, 34, 56, 0, 0, 0)),
            },
            alias: "text".to_string(),
        },
    ];
    plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: localdatetime_expression(2020, 1, 15, 12, 0, 0, 0, 0, 0),
        operator: ComparisonOperator::LessThan,
        rhs: ScalarPredicateRhs::Expression(localdatetime_expression(
            2020, 1, 16, 0, 0, 0, 0, 0, 0,
        )),
    }));
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::Scalar(localdatetime_expression(
            2020, 1, 15, 12, 34, 56, 0, 0, 0,
        )),
        direction: OrderDirection::Ascending,
        nulls: None,
    }];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("localdatetime scalar expressions should lower");

    assert!(
        translation.sql().contains(
            "SELECT CAST('2020-01-15T12:34:56' AS TIMESTAMP) AS \"d\", \
             CAST(CAST('2020-01-15T12:34:56' AS TIMESTAMP) AS VARCHAR) AS \"text\""
        ),
        "{}",
        translation.sql()
    );
    assert!(
        translation.sql().contains(
            "WHERE CAST('2020-01-15T12:00:00' AS TIMESTAMP) < CAST('2020-01-16T00:00:00' AS TIMESTAMP)"
        ),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("ORDER BY CAST('2020-01-15T12:34:56' AS TIMESTAMP) ASC"),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_null_if_expressions() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.projections = vec![Projection::Expression {
        expression: ScalarExpression::NullIf {
            expression: Box::new(ScalarExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            })),
            value: Box::new(ScalarExpression::Literal(Literal::String(
                "prod".to_string(),
            ))),
        },
        alias: "normalized_tier".to_string(),
    }];
    plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: ScalarExpression::NullIf {
            expression: Box::new(ScalarExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            })),
            value: Box::new(ScalarExpression::Literal(Literal::String(
                "dev".to_string(),
            ))),
        },
        operator: ComparisonOperator::Equal,
        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Null)),
    }));
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::Scalar(ScalarExpression::NullIf {
            expression: Box::new(ScalarExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            })),
            value: Box::new(ScalarExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            })),
        }),
        direction: OrderDirection::Ascending,
        nulls: None,
    }];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("nullIf scalar expressions should lower");

    assert!(
        translation
            .sql()
            .contains("NULLIF(\"n1\".\"tier\", 'prod') AS \"normalized_tier\""),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("WHERE NULLIF(\"n1\".\"tier\", 'dev') IS NULL"),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("ORDER BY NULLIF(\"n1\".\"service_name\", \"n1\".\"tier\") ASC"),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_left_right_and_reverse_expressions() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.projections = vec![
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
    ];
    plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: ScalarExpression::Left {
            expression: Box::new(ScalarExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            })),
            count: Box::new(ScalarExpression::Literal(Literal::Integer(7))),
        },
        operator: ComparisonOperator::Equal,
        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
            "billing".to_string(),
        ))),
    }));
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::Scalar(ScalarExpression::Reverse {
            expression: Box::new(ScalarExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            })),
        }),
        direction: OrderDirection::Ascending,
        nulls: None,
    }];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("left, right, and reverse expressions should lower");

    assert!(
            translation
                .sql()
                .contains("SELECT right(\"n1\".\"service_name\", 3) AS \"suffix\", reverse(\"n1\".\"tier\") AS \"reversed_tier\""),
            "{}",
            translation.sql()
        );
    assert!(
        translation
            .sql()
            .contains("WHERE left(\"n1\".\"service_name\", 7) = 'billing'"),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("ORDER BY reverse(\"n1\".\"service_name\") ASC"),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_string_predicate_function_expressions() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.projections = vec![
        expression_projection(
            "has_api",
            ScalarExpression::StringContains {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                })),
                pattern: Box::new(ScalarExpression::Literal(Literal::String(
                    "api".to_string(),
                ))),
            },
        ),
        expression_projection(
            "starts_bill",
            ScalarExpression::StringStartsWith {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                })),
                pattern: Box::new(ScalarExpression::Literal(Literal::String(
                    "bill".to_string(),
                ))),
            },
        ),
        expression_projection(
            "ends_api",
            ScalarExpression::StringEndsWith {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                })),
                pattern: Box::new(ScalarExpression::Literal(Literal::String(
                    "api".to_string(),
                ))),
            },
        ),
    ];
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::Scalar(ScalarExpression::StringContains {
            expression: Box::new(ScalarExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            })),
            pattern: Box::new(ScalarExpression::Literal(Literal::String(
                "api".to_string(),
            ))),
        }),
        direction: OrderDirection::Descending,
        nulls: None,
    }];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("string predicate function expressions should lower");

    assert!(
            translation.sql().contains(
                "SELECT contains(\"n1\".\"service_name\", 'api') AS \"has_api\", starts_with(\"n1\".\"service_name\", 'bill') AS \"starts_bill\", ends_with(\"n1\".\"service_name\", 'api') AS \"ends_api\""
            ),
            "{}",
            translation.sql()
        );
    assert!(
        translation
            .sql()
            .contains("ORDER BY contains(\"n1\".\"service_name\", 'api') DESC"),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_nullable_scalar_cast_expressions() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.projections = vec![
        expression_projection(
            "id_text",
            ScalarExpression::ToStringOrNull {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "id".to_string(),
                })),
            },
        ),
        expression_projection(
            "risk_float",
            ScalarExpression::ToFloatOrNull {
                expression: Box::new(service_risk_expression()),
            },
        ),
        expression_projection(
            "active_bool",
            ScalarExpression::ToBooleanOrNull {
                expression: Box::new(ScalarExpression::Literal(Literal::String(
                    "true".to_string(),
                ))),
            },
        ),
    ];
    plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: ScalarExpression::ToIntegerOrNull {
            expression: Box::new(ScalarExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "id".to_string(),
            })),
        },
        operator: ComparisonOperator::GreaterThan,
        rhs: ScalarPredicateRhs::Expression(integer_literal(0)),
    }));
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::Scalar(ScalarExpression::ToIntegerOrNull {
            expression: Box::new(ScalarExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "id".to_string(),
            })),
        }),
        direction: OrderDirection::Ascending,
        nulls: None,
    }];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("nullable scalar casts should lower");

    for expected in [
        "TRY_CAST(\"n1\".\"id\" AS VARCHAR) AS \"id_text\"",
        "TRY_CAST(\"n1\".\"risk_score\" AS DOUBLE) AS \"risk_float\"",
        "TRY_CAST('true' AS BOOLEAN) AS \"active_bool\"",
        "WHERE TRY_CAST(\"n1\".\"id\" AS BIGINT) > 0",
        "ORDER BY TRY_CAST(\"n1\".\"id\" AS BIGINT) ASC",
    ] {
        assert!(
            translation.sql().contains(expected),
            "{}",
            translation.sql()
        );
    }
}

#[test]
fn lower_graph_plan_renders_numeric_scalar_expressions() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.projections = vec![
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
    ];
    plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: ScalarExpression::Abs {
            expression: Box::new(ScalarExpression::Arithmetic {
                operator: ArithmeticOperator::Subtract,
                left: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                })),
                right: Box::new(ScalarExpression::Literal(Literal::Integer(1))),
            }),
        },
        operator: ComparisonOperator::LessThan,
        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(1))),
    }));
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::Scalar(ScalarExpression::Round {
            expression: Box::new(ScalarExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "risk".to_string(),
            })),
            places: None,
        }),
        direction: OrderDirection::Ascending,
        nulls: None,
    }];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("numeric scalar expressions should lower");

    assert!(
            translation
                .sql()
                .contains("SELECT ceil(\"n1\".\"risk_score\") AS \"risk_ceiling\", floor(\"n1\".\"risk_score\") AS \"risk_floor\", round(\"n1\".\"risk_score\", 1) AS \"risk_rounded\""),
            "{}",
            translation.sql()
        );
    assert!(
        translation
            .sql()
            .contains("WHERE abs((\"n1\".\"risk_score\" - 1)) < 1"),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("ORDER BY round(\"n1\".\"risk_score\") ASC"),
        "{}",
        translation.sql()
    );
}

#[test]
fn render_literal_preserves_whole_float_type() {
    assert_eq!(
        render_literal(&Literal::Float(ordered_float::OrderedFloat(3.0))),
        "3.0"
    );
    assert_eq!(
        render_literal(&Literal::Float(ordered_float::OrderedFloat(0.5))),
        "0.5"
    );
}

#[test]
fn lower_graph_plan_preserves_whole_float_literals_in_numeric_expressions() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.projections = vec![expression_projection(
        "risk_thirds",
        ScalarExpression::Round {
            expression: Box::new(ScalarExpression::Arithmetic {
                operator: ArithmeticOperator::Divide,
                left: Box::new(service_risk_expression()),
                right: Box::new(float_literal(3.0)),
            }),
            places: None,
        },
    )];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("whole float literals should lower as floats");

    assert!(
        translation
            .sql()
            .contains("round((\"n1\".\"risk_score\" / 3.0)) AS \"risk_thirds\""),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_more_numeric_scalar_expressions() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.projections = vec![
        Projection::Expression {
            expression: ScalarExpression::Sqrt {
                expression: Box::new(service_risk_expression()),
            },
            alias: "risk_root".to_string(),
        },
        Projection::Expression {
            expression: ScalarExpression::Sign {
                expression: Box::new(ScalarExpression::Arithmetic {
                    operator: ArithmeticOperator::Subtract,
                    left: Box::new(service_risk_expression()),
                    right: Box::new(ScalarExpression::Literal(Literal::Float(
                        ordered_float::OrderedFloat(0.5),
                    ))),
                }),
            },
            alias: "risk_sign".to_string(),
        },
        Projection::Expression {
            expression: ScalarExpression::Exp {
                expression: Box::new(service_risk_expression()),
            },
            alias: "risk_exp".to_string(),
        },
        Projection::Expression {
            expression: ScalarExpression::Log10 {
                expression: Box::new(service_risk_expression()),
            },
            alias: "risk_log10".to_string(),
        },
    ];
    plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: ScalarExpression::Log {
            expression: Box::new(service_risk_expression()),
        },
        operator: ComparisonOperator::LessThan,
        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(0))),
    }));
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::Scalar(ScalarExpression::Sqrt {
            expression: Box::new(service_risk_expression()),
        }),
        direction: OrderDirection::Ascending,
        nulls: None,
    }];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("additional numeric scalar expressions should lower");

    assert!(
        translation
            .sql()
            .contains("sqrt(\"n1\".\"risk_score\") AS \"risk_root\""),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("signum((\"n1\".\"risk_score\" - 0.5)) AS \"risk_sign\""),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("exp(\"n1\".\"risk_score\") AS \"risk_exp\""),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("log10(\"n1\".\"risk_score\") AS \"risk_log10\""),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("WHERE ln(\"n1\".\"risk_score\") < 0"),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("ORDER BY sqrt(\"n1\".\"risk_score\") ASC"),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_is_nan_scalar_expressions() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.projections = vec![expression_projection(
        "risk_is_nan",
        ScalarExpression::IsNaN {
            expression: Box::new(service_risk_expression()),
        },
    )];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("isNaN scalar expression should lower");

    assert!(
        translation
            .sql()
            .contains("isnan(\"n1\".\"risk_score\") AS \"risk_is_nan\""),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_unary_trigonometric_scalar_expressions() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.projections = vec![
        expression_projection(
            "risk_sin",
            ScalarExpression::Sin {
                expression: Box::new(service_risk_expression()),
            },
        ),
        expression_projection(
            "risk_cos",
            ScalarExpression::Cos {
                expression: Box::new(service_risk_expression()),
            },
        ),
        expression_projection(
            "risk_tan",
            ScalarExpression::Tan {
                expression: Box::new(service_risk_expression()),
            },
        ),
        expression_projection(
            "risk_cot",
            ScalarExpression::Cot {
                expression: Box::new(service_risk_expression()),
            },
        ),
        expression_projection(
            "half_asin",
            ScalarExpression::Asin {
                expression: Box::new(float_literal(0.5)),
            },
        ),
        expression_projection(
            "one_acos",
            ScalarExpression::Acos {
                expression: Box::new(float_literal(1.0)),
            },
        ),
        expression_projection(
            "risk_atan",
            ScalarExpression::Atan {
                expression: Box::new(service_risk_expression()),
            },
        ),
    ];
    plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: ScalarExpression::Sin {
            expression: Box::new(service_risk_expression()),
        },
        operator: ComparisonOperator::GreaterThanOrEqual,
        rhs: ScalarPredicateRhs::Expression(integer_literal(0)),
    }));

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("unary trigonometric scalar expressions should lower");

    for expected in [
        "sin(\"n1\".\"risk_score\") AS \"risk_sin\"",
        "cos(\"n1\".\"risk_score\") AS \"risk_cos\"",
        "tan(\"n1\".\"risk_score\") AS \"risk_tan\"",
        "cot(\"n1\".\"risk_score\") AS \"risk_cot\"",
        "asin(0.5) AS \"half_asin\"",
        "acos(1.0) AS \"one_acos\"",
        "atan(\"n1\".\"risk_score\") AS \"risk_atan\"",
        "WHERE sin(\"n1\".\"risk_score\") >= 0",
    ] {
        assert!(
            translation.sql().contains(expected),
            "{}",
            translation.sql()
        );
    }
}

#[test]
fn lower_graph_plan_renders_atan2_and_angle_conversion_scalar_expressions() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.projections = vec![
        expression_projection(
            "risk_atan2",
            ScalarExpression::Atan2 {
                y: Box::new(service_risk_expression()),
                x: Box::new(integer_literal(1)),
            },
        ),
        expression_projection(
            "risk_degrees",
            ScalarExpression::Degrees {
                expression: Box::new(service_risk_expression()),
            },
        ),
        expression_projection(
            "pi_radians",
            ScalarExpression::Radians {
                expression: Box::new(float_literal(180.0)),
            },
        ),
    ];
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::Scalar(ScalarExpression::Atan2 {
            y: Box::new(service_risk_expression()),
            x: Box::new(integer_literal(1)),
        }),
        direction: OrderDirection::Ascending,
        nulls: None,
    }];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("angle conversion scalar expressions should lower");

    for expected in [
        "atan2(\"n1\".\"risk_score\", 1) AS \"risk_atan2\"",
        "degrees(\"n1\".\"risk_score\") AS \"risk_degrees\"",
        "radians(180.0) AS \"pi_radians\"",
        "ORDER BY atan2(\"n1\".\"risk_score\", 1) ASC",
    ] {
        assert!(
            translation.sql().contains(expected),
            "{}",
            translation.sql()
        );
    }
}

#[test]
fn lower_graph_plan_renders_unary_negation_scalar_expressions() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.projections = vec![
        Projection::Expression {
            expression: ScalarExpression::Negate {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                })),
            },
            alias: "inverse_risk".to_string(),
        },
        Projection::Expression {
            expression: ScalarExpression::Negate {
                expression: Box::new(ScalarExpression::Arithmetic {
                    operator: ArithmeticOperator::Multiply,
                    left: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    })),
                    right: Box::new(ScalarExpression::Literal(Literal::Integer(100))),
                }),
            },
            alias: "inverse_points".to_string(),
        },
    ];
    plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: ScalarExpression::Negate {
            expression: Box::new(ScalarExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "risk".to_string(),
            })),
        },
        operator: ComparisonOperator::LessThan,
        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(0))),
    }));
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::Scalar(ScalarExpression::Negate {
            expression: Box::new(ScalarExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "risk".to_string(),
            })),
        }),
        direction: OrderDirection::Ascending,
        nulls: None,
    }];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("unary negation scalar expressions should lower");

    assert!(
        translation.sql().contains(
            "SELECT -(\"n1\".\"risk_score\") AS \"inverse_risk\", \
                 -((\"n1\".\"risk_score\" * 100)) AS \"inverse_points\""
        ),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("WHERE -(\"n1\".\"risk_score\") < 0"),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("ORDER BY -(\"n1\".\"risk_score\") ASC"),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_power_arithmetic_expressions() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.projections = vec![Projection::Expression {
        expression: ScalarExpression::Arithmetic {
            operator: ArithmeticOperator::Power,
            left: Box::new(ScalarExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "risk".to_string(),
            })),
            right: Box::new(ScalarExpression::Literal(Literal::Integer(2))),
        },
        alias: "risk_squared".to_string(),
    }];
    plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: ScalarExpression::Arithmetic {
            operator: ArithmeticOperator::Power,
            left: Box::new(ScalarExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "risk".to_string(),
            })),
            right: Box::new(ScalarExpression::Literal(Literal::Integer(2))),
        },
        operator: ComparisonOperator::GreaterThan,
        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Float(
            ordered_float::OrderedFloat(0.5),
        ))),
    }));
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::Scalar(ScalarExpression::Arithmetic {
            operator: ArithmeticOperator::Power,
            left: Box::new(ScalarExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "risk".to_string(),
            })),
            right: Box::new(ScalarExpression::Literal(Literal::Integer(2))),
        }),
        direction: OrderDirection::Descending,
        nulls: None,
    }];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("power arithmetic expressions should lower");

    assert!(
        translation
            .sql()
            .contains("SELECT power(\"n1\".\"risk_score\", 2) AS \"risk_squared\""),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("WHERE power(\"n1\".\"risk_score\", 2) > 0.5"),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("ORDER BY power(\"n1\".\"risk_score\", 2) DESC"),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_or_predicate_expressions() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.predicate = Some(PredicateExpression::Or {
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
    });

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("OR predicate expression should lower");

    assert!(
        translation
            .sql()
            .contains("WHERE (\"n1\".\"tier\" = 'prod' OR \"n1\".\"tier\" IS NULL)"),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_xor_predicate_expressions() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.predicate = Some(PredicateExpression::Xor {
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
    });

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("XOR predicate expression should lower");

    assert!(
            translation.sql().contains(
                "WHERE ((\"n1\".\"tier\" = 'prod' AND NOT (\"n1\".\"tier\" IS NULL)) OR (NOT (\"n1\".\"tier\" = 'prod') AND \"n1\".\"tier\" IS NULL))"
            ),
            "{}",
            translation.sql()
        );
}

#[test]
fn lower_graph_plan_renders_not_predicate_expressions() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.predicate = Some(PredicateExpression::Not {
        expression: Box::new(PredicateExpression::Comparison(PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
        })),
    });

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("NOT predicate expression should lower");

    assert!(
        translation
            .sql()
            .contains("WHERE NOT (\"n1\".\"tier\" = 'prod')"),
        "{}",
        translation.sql()
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "The test builds a complete graph plan fixture inline for SQL assertion readability"
)]
fn lower_graph_plan_renders_exists_pattern_predicates_as_correlated_subqueries() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = GraphPlan {
        nodes: vec![NodePattern {
            variable: "service".to_string(),
            label: "Service".to_string(),
        }],
        relationships: Vec::new(),
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![Projection::Property {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            },
            alias: Some("service".to_string()),
        }],
        predicates: Vec::new(),
        predicate: Some(PredicateExpression::ExistsPattern(ExistsPatternPredicate {
            nodes: vec![NodePattern {
                variable: "dependency".to_string(),
                label: "Service".to_string(),
            }],
            relationships: vec![RelationshipPattern {
                variable: Some("dependency_edge".to_string()),
                relationship_type: "DEPENDS_ON".to_string(),
                left: "service".to_string(),
                direction: Direction::Outgoing,
                right: "dependency".to_string(),
            }],
            predicates: vec![
                PropertyPredicate {
                    property: PropertyRef {
                        variable: "dependency".to_string(),
                        property: "tier".to_string(),
                    },
                    operator: ComparisonOperator::Equal,
                    rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
                },
                PropertyPredicate {
                    property: PropertyRef {
                        variable: "dependency_edge".to_string(),
                        property: "criticality".to_string(),
                    },
                    operator: ComparisonOperator::Equal,
                    rhs: PredicateRhs::Literal(Literal::String("runtime".to_string())),
                },
            ],
            predicate: None,
        })),
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("EXISTS predicate expression should lower");

    assert!(
        translation.sql().contains(
            "EXISTS (SELECT 1 FROM \"ops\".\"service_dependencies\" AS \"__coral_exists_r0\" \
                 JOIN \"ops\".\"services\" AS \"__coral_exists_n0\" ON TRUE WHERE"
        ),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("\"__coral_exists_r0\".\"from_service_id\" = \"n0\".\"id\""),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("\"__coral_exists_r0\".\"to_service_id\" = \"__coral_exists_n0\".\"id\""),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("\"__coral_exists_n0\".\"tier\" = 'prod'"),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("\"__coral_exists_r0\".\"criticality\" = 'runtime'"),
        "{}",
        translation.sql()
    );

    plan.predicate = Some(PredicateExpression::Not {
        expression: Box::new(plan.predicate.take().expect("predicate")),
    });
    let negated = graph
        .lower_graph_plan(&plan)
        .expect("negated EXISTS predicate expression should lower");
    assert!(negated.sql().contains("WHERE NOT (EXISTS (SELECT 1"));
}

#[test]
fn lower_graph_plan_renders_boolean_constant_predicates() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.predicate = Some(PredicateExpression::Or {
        left: Box::new(PredicateExpression::Comparison(PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
        })),
        right: Box::new(PredicateExpression::Boolean(false)),
    });

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("constant boolean predicate expression should lower");

    assert!(
        translation
            .sql()
            .contains("WHERE (\"n1\".\"tier\" = 'prod' OR FALSE)"),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_combines_conjunctive_vector_and_predicate_expression() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicate = Some(PredicateExpression::Or {
        left: Box::new(PredicateExpression::Comparison(PropertyPredicate {
            property: PropertyRef {
                variable: "person".to_string(),
                property: "team".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Literal(Literal::String("platform".to_string())),
        })),
        right: Box::new(PredicateExpression::Comparison(PropertyPredicate {
            property: PropertyRef {
                variable: "person".to_string(),
                property: "team".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Literal(Literal::String("infra".to_string())),
        })),
    });

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("conjunctive vector plus predicate expression should lower");

    assert!(
            translation.sql().contains(
                "WHERE \"n1\".\"tier\" = 'prod' AND (\"n0\".\"team\" = 'platform' OR \"n0\".\"team\" = 'infra')"
            ),
            "{}",
            translation.sql()
        );
}

#[test]
fn lower_graph_plan_renders_distinct_projection() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.distinct = true;
    plan.projections = vec![Projection::Property {
        property: PropertyRef {
            variable: "service".to_string(),
            property: "tier".to_string(),
        },
        alias: Some("tier".to_string()),
    }];
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::Property(PropertyRef {
            variable: "service".to_string(),
            property: "tier".to_string(),
        }),
        direction: OrderDirection::Ascending,
        nulls: None,
    }];
    plan.limit = None;

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("distinct plan should lower");

    assert!(
        translation
            .sql()
            .starts_with("SELECT DISTINCT \"n1\".\"tier\" AS \"tier\""),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_offset() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.skip = Some(5);

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("offset plan should lower");

    assert!(
        translation.sql().ends_with(" LIMIT 25 OFFSET 5"),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_grouped_count_projection() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.projections.push(Projection::CountAll {
        alias: "ownership_count".to_string(),
    });

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("grouped aggregate projection should lower");

    assert!(
            translation.sql().contains(
                " GROUP BY \"n0\".\"full_name\", \"n1\".\"service_name\" ORDER BY \"n0\".\"full_name\" ASC"
            ),
            "{}",
            translation.sql()
        );
}

#[test]
fn lower_graph_plan_orders_by_count_alias() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.projections.push(Projection::CountAll {
        alias: "ownership_count".to_string(),
    });
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::ProjectionAlias("ownership_count".to_string()),
        direction: OrderDirection::Descending,
        nulls: None,
    }];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("aggregate alias ordering should lower");

    assert!(
        translation
            .sql()
            .contains(" ORDER BY \"ownership_count\" DESC"),
        "{}",
        translation.sql()
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "The test keeps the correlated node-count plan inline so the SQL shape under test is explicit"
)]
fn lower_graph_plan_precomputes_hidden_correlated_node_count_ordering() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = GraphPlan {
        nodes: vec![NodePattern {
            variable: "service".to_string(),
            label: "Service".to_string(),
        }],
        relationships: Vec::new(),
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![Projection::Property {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            },
            alias: Some("service".to_string()),
        }],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::CountSubquery {
                pattern: Box::new(CountSubqueryPattern::Nodes {
                    nodes: vec![NodePattern {
                        variable: "other".to_string(),
                        label: "Service".to_string(),
                    }],
                    predicates: vec![
                        PropertyPredicate {
                            property: PropertyRef {
                                variable: "other".to_string(),
                                property: "tier".to_string(),
                            },
                            operator: ComparisonOperator::Equal,
                            rhs: PredicateRhs::Property(PropertyRef {
                                variable: "service".to_string(),
                                property: "tier".to_string(),
                            }),
                        },
                        PropertyPredicate {
                            property: PropertyRef {
                                variable: "other".to_string(),
                                property: "name".to_string(),
                            },
                            operator: ComparisonOperator::NotEqual,
                            rhs: PredicateRhs::Literal(Literal::String("legacy".to_string())),
                        },
                    ],
                    predicate: None,
                }),
                distinct_target: None,
            }),
            direction: OrderDirection::Descending,
            nulls: None,
        }],
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("correlated node count ordering should lower");

    assert!(
        translation.sql().contains(
            "LEFT JOIN (SELECT \"__coral_count_n0\".\"tier\" AS \"__coral_outer_key\", \
                 COUNT(*) AS \"__coral_value\" FROM \"ops\".\"services\" AS \"__coral_count_n0\" \
                 WHERE \"__coral_count_n0\".\"service_name\" <> 'legacy' \
                 GROUP BY \"__coral_count_n0\".\"tier\") AS \"__coral_scalar_subquery_0\" \
                 ON \"__coral_scalar_subquery_0\".\"__coral_outer_key\" = \"n0\".\"tier\""
        ),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("ORDER BY COALESCE(\"__coral_scalar_subquery_0\".\"__coral_value\", 0) DESC"),
        "{}",
        translation.sql()
    );

    let order_expression = &mut plan.order_by.first_mut().expect("order key").expression;
    let CountSubqueryPattern::Nodes { predicates, .. } = (match order_expression {
        OrderExpression::Scalar(ScalarExpression::CountSubquery { pattern, .. }) => {
            pattern.as_mut()
        }
        _ => panic!("expected count subquery order expression"),
    }) else {
        panic!("expected node count subquery");
    };
    predicates.push(PropertyPredicate {
        property: PropertyRef {
            variable: "other".to_string(),
            property: "name".to_string(),
        },
        operator: ComparisonOperator::Equal,
        rhs: PredicateRhs::Property(PropertyRef {
            variable: "service".to_string(),
            property: "name".to_string(),
        }),
    });
    let error = graph
        .lower_graph_plan(&plan)
        .expect_err("multiple correlated node-count keys should remain rejected");
    assert!(
        error
            .to_string()
            .contains("requires a precomputable single-anchor relationship or node pattern"),
        "{error}"
    );
}

#[test]
fn lower_graph_plan_precomputes_hidden_correlated_node_exists_ordering() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = GraphPlan {
        nodes: vec![NodePattern {
            variable: "service".to_string(),
            label: "Service".to_string(),
        }],
        relationships: Vec::new(),
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![Projection::Property {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            },
            alias: Some("service".to_string()),
        }],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Predicate(Box::new(
                PredicateExpression::ExistsPattern(ExistsPatternPredicate {
                    nodes: vec![NodePattern {
                        variable: "other".to_string(),
                        label: "Service".to_string(),
                    }],
                    relationships: Vec::new(),
                    predicates: vec![PropertyPredicate {
                        property: PropertyRef {
                            variable: "other".to_string(),
                            property: "tier".to_string(),
                        },
                        operator: ComparisonOperator::Equal,
                        rhs: PredicateRhs::Property(PropertyRef {
                            variable: "service".to_string(),
                            property: "tier".to_string(),
                        }),
                    }],
                    predicate: None,
                }),
            ))),
            direction: OrderDirection::Descending,
            nulls: None,
        }],
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("correlated node exists ordering should lower");

    assert!(
            translation.sql().contains(
                "LEFT JOIN (SELECT \"__coral_exists_n0\".\"tier\" AS \"__coral_outer_key\", \
                 COUNT(*) > 0 AS \"__coral_value\" FROM \"ops\".\"services\" AS \"__coral_exists_n0\" \
                 GROUP BY \"__coral_exists_n0\".\"tier\") AS \"__coral_scalar_subquery_0\" \
                 ON \"__coral_scalar_subquery_0\".\"__coral_outer_key\" = \"n0\".\"tier\""
            ),
            "{}",
            translation.sql()
        );
    assert!(
        translation.sql().contains(
            "ORDER BY COALESCE(\"__coral_scalar_subquery_0\".\"__coral_value\", FALSE) DESC"
        ),
        "{}",
        translation.sql()
    );

    let order_expression = &mut plan.order_by.first_mut().expect("order key").expression;
    let exists_predicate = match order_expression {
        OrderExpression::Scalar(ScalarExpression::Predicate(predicate)) => {
            match predicate.as_mut() {
                PredicateExpression::ExistsPattern(predicate) => predicate,
                _ => panic!("expected exists predicate order expression"),
            }
        }
        _ => panic!("expected exists predicate order expression"),
    };
    exists_predicate.predicates.push(PropertyPredicate {
        property: PropertyRef {
            variable: "other".to_string(),
            property: "name".to_string(),
        },
        operator: ComparisonOperator::Equal,
        rhs: PredicateRhs::Property(PropertyRef {
            variable: "service".to_string(),
            property: "name".to_string(),
        }),
    });
    let error = graph
        .lower_graph_plan(&plan)
        .expect_err("multiple correlated node-exists keys should remain rejected");
    assert!(
        error
            .to_string()
            .contains("requires a precomputable single-anchor relationship or node pattern"),
        "{error}"
    );
}

#[test]
fn lower_graph_plan_renders_count_property_projection() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.projections.push(Projection::Aggregate {
        function: AggregateFunction::Count,
        target: AggregateTarget::Property(PropertyRef {
            variable: "service".to_string(),
            property: "tier".to_string(),
        }),
        distinct: true,
        alias: "tier_count".to_string(),
    });

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("count property projection should lower");

    assert!(
        translation
            .sql()
            .contains("COUNT(DISTINCT \"n1\".\"tier\") AS \"tier_count\""),
        "{}",
        translation.sql()
    );
    assert!(
        translation.sql().contains(" GROUP BY "),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_collect_property_projection() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.projections.push(Projection::Aggregate {
        function: AggregateFunction::Collect,
        target: AggregateTarget::Property(PropertyRef {
            variable: "service".to_string(),
            property: "name".to_string(),
        }),
        distinct: true,
        alias: "services".to_string(),
    });

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("collect property projection should lower");

    assert!(
            translation
                .sql()
                .contains("COALESCE(ARRAY_AGG(DISTINCT \"n1\".\"service_name\") FILTER (WHERE (\"n1\".\"service_name\") IS NOT NULL), make_array()) AS \"services\""),
            "{}",
            translation.sql()
        );
    assert!(
        translation.sql().contains(" GROUP BY "),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_numeric_aggregate_projections() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.projections.push(Projection::Aggregate {
        function: AggregateFunction::Sum,
        target: AggregateTarget::Property(PropertyRef {
            variable: "service".to_string(),
            property: "risk".to_string(),
        }),
        distinct: false,
        alias: "total_risk".to_string(),
    });
    plan.projections.push(Projection::Aggregate {
        function: AggregateFunction::Avg,
        target: AggregateTarget::Property(PropertyRef {
            variable: "service".to_string(),
            property: "risk".to_string(),
        }),
        distinct: false,
        alias: "average_risk".to_string(),
    });
    plan.projections.push(Projection::Aggregate {
        function: AggregateFunction::Min,
        target: AggregateTarget::Property(PropertyRef {
            variable: "service".to_string(),
            property: "risk".to_string(),
        }),
        distinct: false,
        alias: "lowest_risk".to_string(),
    });
    plan.projections.push(Projection::Aggregate {
        function: AggregateFunction::Max,
        target: AggregateTarget::Property(PropertyRef {
            variable: "service".to_string(),
            property: "risk".to_string(),
        }),
        distinct: true,
        alias: "highest_risk".to_string(),
    });

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("numeric aggregate projections should lower");

    assert!(
        translation.sql().contains(
            "SUM(\"n1\".\"risk_score\") AS \"total_risk\", \
                 AVG(\"n1\".\"risk_score\") AS \"average_risk\", \
                 MIN(\"n1\".\"risk_score\") AS \"lowest_risk\", \
                 MAX(DISTINCT \"n1\".\"risk_score\") AS \"highest_risk\""
        ),
        "{}",
        translation.sql()
    );
    assert!(
        translation.sql().contains(" GROUP BY "),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_statistical_aggregate_projections() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.projections.push(Projection::Aggregate {
        function: AggregateFunction::Median,
        target: AggregateTarget::Property(PropertyRef {
            variable: "service".to_string(),
            property: "risk".to_string(),
        }),
        distinct: false,
        alias: "median_risk".to_string(),
    });
    plan.projections.push(Projection::Aggregate {
        function: AggregateFunction::PercentileCont {
            percentile: ordered_float::OrderedFloat(0.75),
        },
        target: AggregateTarget::Property(PropertyRef {
            variable: "service".to_string(),
            property: "risk".to_string(),
        }),
        distinct: false,
        alias: "p75_risk".to_string(),
    });
    plan.projections.push(Projection::Aggregate {
        function: AggregateFunction::StdDev,
        target: AggregateTarget::Property(PropertyRef {
            variable: "service".to_string(),
            property: "risk".to_string(),
        }),
        distinct: false,
        alias: "sample_risk".to_string(),
    });
    plan.projections.push(Projection::Aggregate {
        function: AggregateFunction::StdDevP,
        target: AggregateTarget::Property(PropertyRef {
            variable: "service".to_string(),
            property: "risk".to_string(),
        }),
        distinct: false,
        alias: "population_risk".to_string(),
    });
    plan.projections.push(Projection::Aggregate {
        function: AggregateFunction::Median,
        target: AggregateTarget::Property(PropertyRef {
            variable: "service".to_string(),
            property: "risk".to_string(),
        }),
        distinct: true,
        alias: "distinct_median_risk".to_string(),
    });
    plan.projections.push(Projection::Aggregate {
        function: AggregateFunction::StdDev,
        target: AggregateTarget::Property(PropertyRef {
            variable: "service".to_string(),
            property: "risk".to_string(),
        }),
        distinct: true,
        alias: "distinct_sample_risk".to_string(),
    });
    plan.projections.push(Projection::Aggregate {
        function: AggregateFunction::StdDevP,
        target: AggregateTarget::Property(PropertyRef {
            variable: "service".to_string(),
            property: "risk".to_string(),
        }),
        distinct: true,
        alias: "distinct_population_risk".to_string(),
    });

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("statistical aggregate projections should lower");

    assert!(
            translation.sql().contains(
                "MEDIAN(CAST(\"n1\".\"risk_score\" AS DOUBLE)) AS \"median_risk\", \
                 PERCENTILE_CONT(\"n1\".\"risk_score\", 0.75) AS \"p75_risk\", \
                 STDDEV_SAMP(\"n1\".\"risk_score\") AS \"sample_risk\", \
                 STDDEV_POP(\"n1\".\"risk_score\") AS \"population_risk\", \
                 MEDIAN(DISTINCT CAST(\"n1\".\"risk_score\" AS DOUBLE)) AS \"distinct_median_risk\", \
                 SQRT(VAR_SAMP(DISTINCT \"n1\".\"risk_score\")) AS \"distinct_sample_risk\", \
                 SQRT(VAR_POP(DISTINCT \"n1\".\"risk_score\")) AS \"distinct_population_risk\""
            ),
            "{}",
            translation.sql()
        );
}

#[test]
fn lower_graph_plan_renders_count_node_projection() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.projections = vec![Projection::Aggregate {
        function: AggregateFunction::Count,
        target: AggregateTarget::VariableKey {
            variable: "service".to_string(),
        },
        distinct: true,
        alias: "service_count".to_string(),
    }];
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::ProjectionAlias("service_count".to_string()),
        direction: OrderDirection::Descending,
        nulls: None,
    }];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("count node projection should lower");

    assert!(
        translation
            .sql()
            .contains("COUNT(DISTINCT \"n1\".\"id\") AS \"service_count\""),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains(" ORDER BY \"service_count\" DESC"),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_count_keyed_relationship_projection() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.relationships
        .first_mut()
        .expect("ownership plan should include a relationship")
        .variable = Some("owns".to_string());
    plan.projections = vec![Projection::Aggregate {
        function: AggregateFunction::Count,
        target: AggregateTarget::VariableKey {
            variable: "owns".to_string(),
        },
        distinct: true,
        alias: "ownership_count".to_string(),
    }];
    plan.order_by.clear();

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("count keyed relationship projection should lower");

    assert!(
        translation
            .sql()
            .contains("COUNT(DISTINCT \"r0\".\"ownership_id\") AS \"ownership_count\""),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_count_keyless_relationship_projection() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![
            NodePattern {
                variable: "source".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "target".to_string(),
                label: "Service".to_string(),
            },
        ],
        relationships: vec![RelationshipPattern {
            variable: Some("dependency".to_string()),
            relationship_type: "DEPENDS_ON".to_string(),
            left: "source".to_string(),
            direction: Direction::Outgoing,
            right: "target".to_string(),
        }],
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![Projection::Aggregate {
            function: AggregateFunction::Count,
            target: AggregateTarget::VariableKey {
                variable: "dependency".to_string(),
            },
            distinct: false,
            alias: "dependency_count".to_string(),
        }],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("count keyless relationship projection should lower");

    assert!(
        translation
            .sql()
            .contains("COUNT(\"r0\".\"from_service_id\") AS \"dependency_count\""),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_presence_predicate_for_keyless_relationship() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![
            NodePattern {
                variable: "source".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "target".to_string(),
                label: "Service".to_string(),
            },
        ],
        relationships: vec![RelationshipPattern {
            variable: Some("dependency".to_string()),
            relationship_type: "DEPENDS_ON".to_string(),
            left: "source".to_string(),
            direction: Direction::Outgoing,
            right: "target".to_string(),
        }],
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![Projection::Property {
            property: PropertyRef {
                variable: "source".to_string(),
                property: "name".to_string(),
            },
            alias: Some("source".to_string()),
        }],
        predicates: Vec::new(),
        predicate: Some(PredicateExpression::Presence(PresencePredicate {
            variable: "dependency".to_string(),
            operator: ComparisonOperator::NotEqual,
        })),
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("keyless relationship presence predicate should lower");

    assert!(
        translation
            .sql()
            .contains("\"r0\".\"from_service_id\" IS NOT NULL"),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_rejects_count_with_property_ordering() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.projections = vec![Projection::CountAll {
        alias: "ownership_count".to_string(),
    }];

    let error = graph
        .lower_graph_plan(&plan)
        .expect_err("count with property ordering should fail");

    assert!(
        error.to_string().contains("UNSUPPORTED_AGGREGATION"),
        "{error:?}"
    );
}

#[test]
fn lower_graph_plan_rejects_ordered_null_comparisons() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    let predicate = plan
        .predicates
        .get_mut(0)
        .expect("ownership fixture should include a predicate");
    predicate.operator = ComparisonOperator::GreaterThan;
    predicate.rhs = PredicateRhs::Literal(Literal::Null);

    let error = graph
        .lower_graph_plan(&plan)
        .expect_err("ordered null comparison should fail");

    assert!(
        error.to_string().contains("INVALID_NULL_COMPARISON"),
        "{error:?}"
    );
}

#[test]
fn lower_graph_plan_rejects_endpoint_mismatch() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    let service_node = plan
        .nodes
        .get_mut(1)
        .expect("ownership fixture should include a service node");
    service_node.label = "Person".to_string();

    let error = graph
        .lower_graph_plan(&plan)
        .expect_err("endpoint mismatch should fail");

    assert!(
        error.to_string().contains("RELATIONSHIP_ENDPOINT_MISMATCH"),
        "{error:?}"
    );
}

fn ownership_plan(direction: Direction) -> GraphPlan {
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
            direction,
            right: "service".to_string(),
        }],
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![
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
        ],
        predicates: vec![PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
        }],
        predicate: None,
        post_projection_predicate: None,
        order_by: vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "person".to_string(),
                property: "name".to_string(),
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }],
        skip: None,
        limit: Some(25),
    }
}

fn service_risk_expression() -> ScalarExpression {
    ScalarExpression::Property(PropertyRef {
        variable: "service".to_string(),
        property: "risk".to_string(),
    })
}

fn service_name_expression() -> ScalarExpression {
    ScalarExpression::Property(PropertyRef {
        variable: "service".to_string(),
        property: "name".to_string(),
    })
}

fn integer_literal(value: i64) -> ScalarExpression {
    ScalarExpression::Literal(Literal::Integer(value))
}

fn float_literal(value: f64) -> ScalarExpression {
    ScalarExpression::Literal(Literal::Float(ordered_float::OrderedFloat(value)))
}

fn expression_projection(alias: &str, expression: ScalarExpression) -> Projection {
    Projection::Expression {
        expression,
        alias: alias.to_string(),
    }
}

fn date_from_string_expression(text: &str) -> ScalarExpression {
    ScalarExpression::Temporal(TemporalExpr::DateFromString {
        text: Box::new(ScalarExpression::Literal(Literal::String(text.to_string()))),
    })
}

fn localdatetime_from_string_expression(text: &str) -> ScalarExpression {
    ScalarExpression::Temporal(TemporalExpr::LocalDateTimeFromString {
        text: Box::new(ScalarExpression::Literal(Literal::String(text.to_string()))),
    })
}

#[test]
fn renders_date_from_string_projection() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.projections = vec![
        expression_projection("d", date_from_string_expression("2015-07-21")),
        expression_projection(
            "text",
            ScalarExpression::ToString {
                expression: Box::new(date_from_string_expression("2015-07-21")),
            },
        ),
    ];
    plan.order_by.clear();
    plan.limit = None;

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("date string projection should lower");

    assert!(
        translation.sql().contains(
            "SELECT CAST('2015-07-21' AS DATE) AS \"d\", \
             CAST(CAST('2015-07-21' AS DATE) AS VARCHAR) AS \"text\""
        ),
        "{}",
        translation.sql()
    );
}

#[test]
fn renders_date_from_string_comparison() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: date_from_string_expression("2015-07-21"),
        operator: ComparisonOperator::LessThan,
        rhs: ScalarPredicateRhs::Expression(date_from_string_expression("2016-01-01")),
    }));
    plan.order_by.clear();
    plan.limit = None;

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("date string comparison should lower");

    assert!(
        translation
            .sql()
            .contains("WHERE CAST('2015-07-21' AS DATE) < CAST('2016-01-01' AS DATE)"),
        "{}",
        translation.sql()
    );
}

#[test]
fn renders_date_from_string_order() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::Scalar(date_from_string_expression("2015-07-21")),
        direction: OrderDirection::Ascending,
        nulls: None,
    }];
    plan.limit = None;

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("date string ordering should lower");

    assert!(
        translation
            .sql()
            .contains("ORDER BY CAST('2015-07-21' AS DATE) ASC"),
        "{}",
        translation.sql()
    );
}

#[test]
fn renders_localdatetime_from_string_projection() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.projections = vec![
        expression_projection(
            "d",
            localdatetime_from_string_expression("2020-01-15T12:34:56"),
        ),
        expression_projection(
            "text",
            ScalarExpression::ToString {
                expression: Box::new(localdatetime_from_string_expression("2020-01-15T12:34:56")),
            },
        ),
    ];
    plan.order_by.clear();
    plan.limit = None;

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("localdatetime string projection should lower");

    assert!(
        translation.sql().contains(
            "SELECT CAST('2020-01-15T12:34:56' AS TIMESTAMP) AS \"d\", \
             CAST(CAST('2020-01-15T12:34:56' AS TIMESTAMP) AS VARCHAR) AS \"text\""
        ),
        "{}",
        translation.sql()
    );
}

#[test]
fn renders_localdatetime_from_string_comparison() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: localdatetime_from_string_expression("2020-01-15T12:00:00"),
        operator: ComparisonOperator::LessThan,
        rhs: ScalarPredicateRhs::Expression(localdatetime_from_string_expression(
            "2020-01-16T00:00:00",
        )),
    }));
    plan.order_by.clear();
    plan.limit = None;

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("localdatetime string comparison should lower");

    assert!(
        translation.sql().contains(
            "WHERE CAST('2020-01-15T12:00:00' AS TIMESTAMP) < CAST('2020-01-16T00:00:00' AS TIMESTAMP)"
        ),
        "{}",
        translation.sql()
    );
}

#[test]
fn renders_localdatetime_from_string_order() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::Scalar(localdatetime_from_string_expression(
            "2020-01-15T12:34:56",
        )),
        direction: OrderDirection::Ascending,
        nulls: None,
    }];
    plan.limit = None;

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("localdatetime string ordering should lower");

    assert!(
        translation
            .sql()
            .contains("ORDER BY CAST('2020-01-15T12:34:56' AS TIMESTAMP) ASC"),
        "{}",
        translation.sql()
    );
}
