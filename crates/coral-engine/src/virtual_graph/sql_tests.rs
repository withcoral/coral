use super::*;
use crate::virtual_graph::ir::{
    AggregateFunction, AggregateTarget, ComparisonOperator, CountSubqueryPattern, Direction,
    ExistsPatternPredicate, GraphPlan, GraphQuery, GraphStage, GraphStageExport, GraphStagedQuery,
    GraphStagedUnwind, GraphStagedUnwindBinding, GraphStagedUnwindQuery, GraphUnion,
    GraphUnionBranch, GraphUnwind, GraphUnwindInput, GraphUnwindInputProjection,
    GraphUnwindPipeline, GraphUnwindProjection, KeyPredicate, Literal, LiteralListElementType,
    NodePattern, OptionalMatchScope, OrderDirection, OrderExpression, OrderKey,
    PredicateExpression, PredicateRhs, Projection, ProjectionPredicate,
    ProjectionPredicateExpression, ProjectionPredicateRhs, PropertyPredicate, PropertyRef,
    RelationshipPattern, ScalarExpression, ScalarPredicate, ScalarPredicateRhs,
    TemporalComponentUnit, TemporalDurationUnit, TemporalExpr, ZonedDateTimeAccessor,
};
use crate::{CatalogInfo, ColumnInfo, TableInfo};

#[path = "sql_tests/joins.rs"]
mod joins;
#[path = "sql_tests/predicates.rs"]
mod predicates;
#[path = "sql_tests/render.rs"]
mod render;
#[path = "sql_tests/scalar.rs"]
mod scalar;

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
    key: id
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

#[expect(
    clippy::too_many_arguments,
    reason = "Test helper mirrors openCypher datetime fields."
)]
fn zoneddatetime_expression(
    year: i64,
    month: i64,
    day: i64,
    hour: i64,
    minute: i64,
    second: i64,
    millisecond: i64,
    microsecond: i64,
    nanosecond: i64,
    timezone: &str,
) -> ScalarExpression {
    ScalarExpression::Temporal(TemporalExpr::MakeZonedDateTime {
        year: Box::new(ScalarExpression::Literal(Literal::Integer(year))),
        month: Box::new(ScalarExpression::Literal(Literal::Integer(month))),
        day: Box::new(ScalarExpression::Literal(Literal::Integer(day))),
        hour: Box::new(ScalarExpression::Literal(Literal::Integer(hour))),
        minute: Box::new(ScalarExpression::Literal(Literal::Integer(minute))),
        second: Box::new(ScalarExpression::Literal(Literal::Integer(second))),
        millisecond: Box::new(ScalarExpression::Literal(Literal::Integer(millisecond))),
        microsecond: Box::new(ScalarExpression::Literal(Literal::Integer(microsecond))),
        nanosecond: Box::new(ScalarExpression::Literal(Literal::Integer(nanosecond))),
        timezone: timezone.to_string(),
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

fn duration_expression(months: i64, days: i64, seconds: i64, nanos: i64) -> ScalarExpression {
    ScalarExpression::Temporal(TemporalExpr::MakeDuration {
        months,
        days,
        seconds,
        nanos,
    })
}

fn duration_in_units_expression(
    unit: TemporalDurationUnit,
    start: ScalarExpression,
    end: ScalarExpression,
) -> ScalarExpression {
    ScalarExpression::Temporal(TemporalExpr::DurationInUnits {
        unit,
        start: Box::new(start),
        end: Box::new(end),
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

fn zoneddatetime_accessor_expression(
    expression: ScalarExpression,
    accessor: ZonedDateTimeAccessor,
    timezone: Option<&str>,
) -> ScalarExpression {
    ScalarExpression::Temporal(TemporalExpr::ZonedDateTimeAccessor {
        expression: Box::new(expression),
        accessor,
        timezone: timezone.map(str::to_string),
    })
}

fn staged_relationship_key_optional_query() -> GraphQuery {
    GraphQuery::Staged(GraphStagedQuery {
        stages: vec![staged_relationship_key_stage()],
        final_plan: staged_relationship_key_optional_final_plan(),
    })
}

fn staged_node_relationship_key_optional_miss_query() -> GraphQuery {
    let mut stage = staged_relationship_key_stage();
    stage.plan.projections.push(Projection::Key {
        variable: "a".to_string(),
        alias: "a1_id".to_string(),
    });
    stage.plan.order_by.push(OrderKey {
        expression: OrderExpression::Key {
            variable: "r".to_string(),
        },
        direction: OrderDirection::Ascending,
        nulls: None,
    });
    stage.exports.push(GraphStageExport::NodeKey {
        variable: "a1".to_string(),
        column: "a1_id".to_string(),
    });

    GraphQuery::Staged(GraphStagedQuery {
        stages: vec![stage],
        final_plan: GraphPlan {
            nodes: vec![
                NodePattern {
                    variable: "a1".to_string(),
                    label: "Person".to_string(),
                },
                NodePattern {
                    variable: "b2".to_string(),
                    label: "Person".to_string(),
                },
            ],
            relationships: vec![RelationshipPattern {
                variable: Some("r".to_string()),
                relationship_type: "KNOWS".to_string(),
                left: "b2".to_string(),
                direction: Direction::Outgoing,
                right: "a1".to_string(),
            }],
            optional_relationships: vec![0],
            optional_matches: vec![OptionalMatchScope {
                node_indices: vec![1],
                relationship_indices: vec![0],
                predicate: None,
            }],
            distinct: false,
            projections: vec![
                Projection::Property {
                    property: PropertyRef {
                        variable: "a1".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("a".to_string()),
                },
                Projection::Key {
                    variable: "r".to_string(),
                    alias: "r".to_string(),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "b2".to_string(),
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

fn staged_relationship_key_stage() -> GraphStage {
    GraphStage {
        plan: GraphPlan {
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
                variable: Some("r".to_string()),
                relationship_type: "KNOWS".to_string(),
                left: "a".to_string(),
                direction: Direction::Outgoing,
                right: "b".to_string(),
            }],
            optional_relationships: Vec::new(),
            optional_matches: Vec::new(),
            distinct: false,
            projections: vec![Projection::Key {
                variable: "r".to_string(),
                alias: "r_id".to_string(),
            }],
            predicates: Vec::new(),
            predicate: None,
            post_projection_predicate: None,
            order_by: Vec::new(),
            skip: None,
            limit: Some(1),
        },
        exports: vec![GraphStageExport::RelationshipKey {
            variable: "r".to_string(),
            column: "r_id".to_string(),
        }],
    }
}

fn staged_relationship_key_optional_final_plan() -> GraphPlan {
    GraphPlan {
        nodes: vec![
            NodePattern {
                variable: "a2".to_string(),
                label: "Person".to_string(),
            },
            NodePattern {
                variable: "b2".to_string(),
                label: "Person".to_string(),
            },
        ],
        relationships: vec![RelationshipPattern {
            variable: Some("r".to_string()),
            relationship_type: "KNOWS".to_string(),
            left: "a2".to_string(),
            direction: Direction::Outgoing,
            right: "b2".to_string(),
        }],
        optional_relationships: vec![0],
        optional_matches: vec![OptionalMatchScope {
            node_indices: vec![0, 1],
            relationship_indices: vec![0],
            predicate: None,
        }],
        distinct: false,
        projections: vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "a2".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("a".to_string()),
            },
            Projection::Key {
                variable: "r".to_string(),
                alias: "r".to_string(),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "b2".to_string(),
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
    }
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

fn staged_collect_unwind_query() -> GraphQuery {
    let mut stage_plan = staged_aggregate_stage_plan();
    *stage_plan
        .projections
        .get_mut(1)
        .expect("staged aggregate fixture should have aggregate projection") =
        Projection::Aggregate {
            function: AggregateFunction::Collect,
            target: AggregateTarget::VariableKey {
                variable: "b".to_string(),
            },
            distinct: false,
            alias: "bees".to_string(),
        };

    let mut final_plan = staged_aggregate_final_plan();
    final_plan
        .nodes
        .get_mut(1)
        .expect("staged aggregate fixture should have final target node")
        .variable = "b2".to_string();
    final_plan
        .relationships
        .get_mut(0)
        .expect("staged aggregate fixture should have final relationship")
        .right = "b2".to_string();
    final_plan.projections = vec![
        Projection::Property {
            property: PropertyRef {
                variable: "a".to_string(),
                property: "name".to_string(),
            },
            alias: Some("a".to_string()),
        },
        Projection::Property {
            property: PropertyRef {
                variable: "b2".to_string(),
                property: "name".to_string(),
            },
            alias: Some("b".to_string()),
        },
    ];

    GraphQuery::StagedUnwind(Box::new(GraphStagedUnwindQuery {
        stage: GraphStage {
            plan: stage_plan,
            exports: vec![
                GraphStageExport::NodeKey {
                    variable: "a".to_string(),
                    column: "a_id".to_string(),
                },
                GraphStageExport::AggregateValue {
                    alias: "bees".to_string(),
                    column: "bees".to_string(),
                },
            ],
        },
        unwind: GraphStagedUnwind {
            source_alias: "bees".to_string(),
            variable: "b2".to_string(),
            binding: GraphStagedUnwindBinding::NodeKey {
                label: "Person".to_string(),
            },
        },
        final_plan,
    }))
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

fn staged_aggregate_relationship_key_query() -> GraphQuery {
    GraphQuery::Staged(GraphStagedQuery {
        stages: vec![GraphStage {
            plan: GraphPlan {
                projections: vec![
                    Projection::Key {
                        variable: "a".to_string(),
                        alias: "a_id".to_string(),
                    },
                    Projection::Key {
                        variable: "r".to_string(),
                        alias: "r_id".to_string(),
                    },
                    Projection::Key {
                        variable: "b".to_string(),
                        alias: "b_id".to_string(),
                    },
                    Projection::CountAll {
                        alias: "c".to_string(),
                    },
                ],
                ..staged_aggregate_relationship_key_base_plan()
            },
            exports: vec![
                GraphStageExport::NodeKey {
                    variable: "a".to_string(),
                    column: "a_id".to_string(),
                },
                GraphStageExport::RelationshipKey {
                    variable: "r".to_string(),
                    column: "r_id".to_string(),
                },
                GraphStageExport::NodeKey {
                    variable: "b".to_string(),
                    column: "b_id".to_string(),
                },
                GraphStageExport::AggregateValue {
                    alias: "c".to_string(),
                    column: "c".to_string(),
                },
            ],
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
                variable: Some("r".to_string()),
                relationship_type: "KNOWS".to_string(),
                left: "a".to_string(),
                direction: Direction::Outgoing,
                right: "b".to_string(),
            }],
            projections: vec![
                Projection::Key {
                    variable: "r".to_string(),
                    alias: "rel.__id".to_string(),
                },
                Projection::RelationshipType {
                    variable: "r".to_string(),
                    relationship_type: "KNOWS".to_string(),
                    alias: "rel.__type".to_string(),
                },
            ],
            ..GraphPlan::default()
        },
    })
}

fn staged_aggregate_relationship_key_base_plan() -> GraphPlan {
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
            variable: Some("r".to_string()),
            relationship_type: "KNOWS".to_string(),
            left: "a".to_string(),
            direction: Direction::Outgoing,
            right: "b".to_string(),
        }],
        ..GraphPlan::default()
    }
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

fn zoneddatetime_from_string_expression(text: &str, timezone: &str) -> ScalarExpression {
    ScalarExpression::Temporal(TemporalExpr::ZonedDateTimeFromString {
        text: Box::new(ScalarExpression::Literal(Literal::String(text.to_string()))),
        timezone: timezone.to_string(),
    })
}

fn localtime_from_string_expression(text: &str) -> ScalarExpression {
    ScalarExpression::Temporal(TemporalExpr::LocalTimeFromString {
        text: Box::new(ScalarExpression::Literal(Literal::String(text.to_string()))),
    })
}

fn typed_ownership_catalog() -> CatalogInfo {
    typed_ownership_catalog_with_since_type("Utf8")
}

fn typed_ownership_catalog_with_since_type(since_type: &str) -> CatalogInfo {
    CatalogInfo {
        tables: vec![
            typed_table(
                "ops",
                "people",
                &[("id", "Int64"), ("full_name", "Utf8"), ("team", "Utf8")],
            ),
            typed_table(
                "ops",
                "services",
                &[
                    ("id", "Int64"),
                    ("service_name", "Utf8"),
                    ("tier", "Utf8"),
                    ("risk_score", "Float64"),
                ],
            ),
            typed_table(
                "ops",
                "ownerships",
                &[
                    ("ownership_id", "Int64"),
                    ("person_id", "Int64"),
                    ("service_id", "Int64"),
                    ("since", since_type),
                ],
            ),
            typed_table(
                "ops",
                "service_dependencies",
                &[
                    ("from_service_id", "Int64"),
                    ("to_service_id", "Int64"),
                    ("criticality", "Utf8"),
                ],
            ),
        ],
        table_functions: Vec::new(),
    }
}

fn typed_table(schema: &str, name: &str, columns: &[(&str, &str)]) -> TableInfo {
    TableInfo {
        schema_name: schema.to_string(),
        table_name: name.to_string(),
        description: String::new(),
        guide: String::new(),
        columns: columns
            .iter()
            .enumerate()
            .map(|(position, (column, data_type))| ColumnInfo {
                name: (*column).to_string(),
                data_type: (*data_type).to_string(),
                nullable: true,
                is_virtual: false,
                is_required_filter: false,
                description: String::new(),
                ordinal_position: u32::try_from(position).unwrap_or(u32::MAX),
            })
            .collect(),
        required_filters: Vec::new(),
    }
}
