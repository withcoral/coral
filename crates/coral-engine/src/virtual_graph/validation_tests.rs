use super::*;
use crate::virtual_graph::ir::{
    AggregateFunction, AggregateTarget, ArithmeticOperator, Direction, KeyPredicate, NodePattern,
    OptionalMatchScope, OrderDirection, OrderExpression, OrderKey, PredicateExpression,
    PredicateRhs, Projection, PropertyPredicate, PropertyRef, RelationshipPattern,
    ScalarExpression, ScalarPredicate, ScalarPredicateRhs, TemporalExpr,
};
use crate::{ColumnInfo, TableInfo};

const GRAPH: &str = r"
version: 1
name: ownership
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
      tier: tier
relationships:
  - type: OWNS
    table: { schema: ops, name: ownerships }
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
    properties:
      since: since
";

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

#[test]
fn validate_graph_plan_resolves_bindings_and_relationships() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = ownership_plan();

    let validated = graph
        .validate_graph_plan(&plan)
        .expect("plan should validate");

    assert_eq!(validated.binding("person").unwrap().alias(), "n0");
    assert_eq!(validated.binding("owns").unwrap().alias(), "r0");
    assert_eq!(
        validated
            .relationship_mapping(0)
            .expect("relationship mapping")
            .relationship_type,
        "OWNS"
    );
}

#[test]
fn validate_graph_plan_selects_relationship_type_overload_by_endpoint_labels() {
    let graph = Declaration::from_yaml(
        r"
version: 1
name: overloaded-ownership
nodes:
  - label: Person
    table: { schema: ops, name: people }
    key: id
    properties:
      name: full_name
  - label: Team
    table: { schema: ops, name: teams }
    key: id
    properties:
      name: team_name
  - label: Service
    table: { schema: ops, name: services }
    key: id
    properties:
      name: service_name
relationships:
  - type: OWNS
    table: { schema: ops, name: person_ownerships }
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
  - type: OWNS
    table: { schema: ops, name: team_ownerships }
    from: { label: Team, key: team_id }
    to: { label: Service, key: service_id }
",
    )
    .expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![
            NodePattern {
                variable: "team".to_string(),
                label: "Team".to_string(),
            },
            NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            },
        ],
        relationships: vec![RelationshipPattern {
            variable: Some("owns".to_string()),
            relationship_type: "OWNS".to_string(),
            left: "team".to_string(),
            direction: Direction::Outgoing,
            right: "service".to_string(),
        }],
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![Projection::Property {
            property: PropertyRef {
                variable: "team".to_string(),
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

    let validated = graph
        .validate_graph_plan(&plan)
        .expect("plan should validate");

    assert_eq!(
        validated
            .relationship_mapping(0)
            .expect("relationship mapping")
            .table
            .name,
        "team_ownerships"
    );
}

#[test]
fn validate_graph_plan_rejects_ambiguous_undirected_relationship_overloads() {
    let graph = Declaration::from_yaml(
        r"
version: 1
name: inverse-ownership
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
relationships:
  - type: OWNS
    table: { schema: ops, name: person_ownerships }
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
  - type: OWNS
    table: { schema: ops, name: service_owner_edges }
    from: { label: Service, key: service_id }
    to: { label: Person, key: person_id }
",
    )
    .expect("graph should parse");
    let mut plan = ownership_plan();
    plan.relationships
        .first_mut()
        .expect("ownership plan should have a relationship")
        .direction = Direction::Undirected;

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("undirected inverse overloads should be ambiguous");

    assert!(
        error.to_string().contains("AMBIGUOUS_RELATIONSHIP_MAPPING"),
        "{error:?}"
    );
}

#[test]
fn validate_graph_plan_accepts_undirected_reversed_relationship_labels() {
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

    graph
        .validate_graph_plan(&plan)
        .expect("undirected relationship should validate in either endpoint order");
}

#[test]
fn validate_graph_plan_rejects_unknown_properties_before_lowering() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::Property(PropertyRef {
            variable: "service".to_string(),
            property: "missing".to_string(),
        }),
        direction: OrderDirection::Ascending,
        nulls: None,
    }];

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("unknown property should fail validation");

    assert!(error.to_string().contains("UNKNOWN_PROPERTY"), "{error:?}");
}

#[test]
fn validate_graph_plan_rejects_unknown_aggregate_properties_before_lowering() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections.push(Projection::Aggregate {
        function: AggregateFunction::Count,
        target: AggregateTarget::Property(PropertyRef {
            variable: "service".to_string(),
            property: "missing".to_string(),
        }),
        distinct: false,
        alias: "missing_count".to_string(),
    });

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("unknown aggregate property should fail validation");

    assert!(error.to_string().contains("UNKNOWN_PROPERTY"), "{error:?}");
}

#[test]
fn validate_graph_plan_accepts_catalog_typed_numeric_aggregate_targets() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections = vec![Projection::Aggregate {
        function: AggregateFunction::Sum,
        target: AggregateTarget::Property(PropertyRef {
            variable: "service".to_string(),
            property: "id".to_string(),
        }),
        distinct: false,
        alias: "service_id_sum".to_string(),
    }];

    graph
        .validate_graph_plan_against_catalog(&plan, &typed_ownership_catalog())
        .expect("numeric aggregate target should validate against catalog types");
}

#[test]
fn validate_graph_plan_rejects_catalog_typed_non_numeric_aggregate_targets() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections = vec![Projection::Aggregate {
        function: AggregateFunction::Sum,
        target: AggregateTarget::Property(PropertyRef {
            variable: "person".to_string(),
            property: "name".to_string(),
        }),
        distinct: false,
        alias: "bad_sum".to_string(),
    }];

    let error = graph
        .validate_graph_plan_against_catalog(&plan, &typed_ownership_catalog())
        .expect_err("string aggregate target should fail catalog-aware validation");

    assert!(
        error.to_string().contains("INVALID_AGGREGATE_TARGET"),
        "{error:?}"
    );
    assert!(error.to_string().contains("numeric"), "{error:?}");
}

#[test]
fn validate_graph_plan_accepts_keyless_relationship_count_targets() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections = vec![Projection::Aggregate {
        function: AggregateFunction::Count,
        target: AggregateTarget::VariableKey {
            variable: "owns".to_string(),
        },
        distinct: false,
        alias: "ownership_count".to_string(),
    }];

    graph
        .validate_graph_plan(&plan)
        .expect("keyless relationship count target should validate");
}

#[test]
fn validate_graph_plan_rejects_keyless_relationship_collect_targets() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections = vec![Projection::Aggregate {
        function: AggregateFunction::Collect,
        target: AggregateTarget::VariableKey {
            variable: "owns".to_string(),
        },
        distinct: false,
        alias: "ownerships".to_string(),
    }];

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("keyless relationship collect target should fail validation");

    assert!(
        error.to_string().contains("INVALID_AGGREGATE_TARGET"),
        "{error:?}"
    );
    assert!(error.to_string().contains("declare a key"), "{error:?}");
}

#[test]
fn validate_graph_plan_rejects_keyless_relationship_element_id_projections() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections = vec![Projection::ElementId {
        variable: "owns".to_string(),
        alias: "ownership_element_id".to_string(),
    }];

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("keyless relationship element id should fail validation");

    assert!(
        error.to_string().contains("INVALID_ELEMENT_ID_PROJECTION"),
        "{error:?}"
    );
}

#[test]
fn validate_graph_plan_rejects_non_string_element_id_predicate_literals() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.predicate = Some(PredicateExpression::ElementIdComparison(
        ElementIdPredicate {
            variable: "person".to_string(),
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Literal(Literal::Integer(1)),
        },
    ));

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("non-string element id literal should fail validation");

    assert!(
        error.to_string().contains("INVALID_PREDICATE_OPERAND"),
        "{error:?}"
    );
}

#[test]
fn validate_graph_plan_rejects_distinct_keyless_relationship_count_targets() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections = vec![Projection::Aggregate {
        function: AggregateFunction::Count,
        target: AggregateTarget::VariableKey {
            variable: "owns".to_string(),
        },
        distinct: true,
        alias: "ownership_count".to_string(),
    }];

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("distinct keyless relationship aggregate target should fail validation");

    assert!(
        error.to_string().contains("INVALID_AGGREGATE_TARGET"),
        "{error:?}"
    );
}

#[test]
fn validate_graph_plan_rejects_hidden_distinct_keyless_relationship_count_ordering() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::Aggregate {
            function: AggregateFunction::Count,
            target: AggregateTarget::VariableKey {
                variable: "owns".to_string(),
            },
            distinct: true,
        },
        direction: OrderDirection::Descending,
        nulls: None,
    }];

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("hidden distinct keyless relationship aggregate target should fail");

    assert!(
        error.to_string().contains("INVALID_AGGREGATE_TARGET"),
        "{error:?}"
    );
    assert!(
        error.to_string().contains("order_by[0].aggregate.target"),
        "{error:?}"
    );
}

#[test]
fn validate_graph_plan_accepts_distinct_standard_deviation_aggregates() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections = vec![Projection::Aggregate {
        function: AggregateFunction::StdDevP,
        target: AggregateTarget::Property(PropertyRef {
            variable: "service".to_string(),
            property: "tier".to_string(),
        }),
        distinct: true,
        alias: "population_risk".to_string(),
    }];

    graph
        .validate_graph_plan(&plan)
        .expect("distinct standard deviation aggregate should validate");
}

#[test]
fn validate_graph_plan_rejects_distinct_percentile_cont_aggregates() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections = vec![Projection::Aggregate {
        function: AggregateFunction::PercentileCont {
            percentile: ordered_float::OrderedFloat(0.75),
        },
        target: AggregateTarget::Property(PropertyRef {
            variable: "service".to_string(),
            property: "tier".to_string(),
        }),
        distinct: true,
        alias: "p75_tier".to_string(),
    }];

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("distinct percentile-continuous aggregate should fail validation");

    assert!(
        error.to_string().contains("INVALID_AGGREGATE_TARGET"),
        "{error:?}"
    );
    assert!(
        error.to_string().contains("percentileCont(DISTINCT ...)"),
        "{error:?}"
    );
}

#[test]
fn validate_graph_plan_rejects_hidden_distinct_percentile_cont_ordering() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::Aggregate {
            function: AggregateFunction::PercentileCont {
                percentile: ordered_float::OrderedFloat(0.75),
            },
            target: AggregateTarget::Property(PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            }),
            distinct: true,
        },
        direction: OrderDirection::Descending,
        nulls: None,
    }];

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("hidden distinct percentile-continuous aggregate should fail validation");

    assert!(
        error.to_string().contains("INVALID_AGGREGATE_TARGET"),
        "{error:?}"
    );
    assert!(
        error.to_string().contains("order_by[0].aggregate.distinct"),
        "{error:?}"
    );
}

#[test]
fn validate_graph_plan_accepts_keyed_relationship_aggregate_targets() {
    let keyed_graph = GRAPH.replace(
        "table: { schema: ops, name: ownerships }\n    from:",
        "table: { schema: ops, name: ownerships }\n    key: ownership_id\n    from:",
    );
    let graph = Declaration::from_yaml(&keyed_graph).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections = vec![Projection::Aggregate {
        function: AggregateFunction::Count,
        target: AggregateTarget::VariableKey {
            variable: "owns".to_string(),
        },
        distinct: true,
        alias: "ownership_count".to_string(),
    }];

    graph
        .validate_graph_plan(&plan)
        .expect("keyed relationship aggregate target should validate");
}

#[test]
fn validate_graph_plan_rejects_ambiguous_literal_list_projections() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");

    for literals in [
        Vec::new(),
        vec![Literal::Null],
        vec![Literal::Integer(1), Literal::String("prod".to_string())],
    ] {
        let mut plan = ownership_plan();
        plan.projections = vec![Projection::LiteralList {
            literals,
            alias: "values".to_string(),
        }];

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("ambiguous literal list projection should fail validation");

        assert!(
            error
                .to_string()
                .contains("INVALID_LITERAL_LIST_PROJECTION"),
            "{error:?}"
        );
    }
}

#[test]
fn validate_graph_plan_accepts_empty_typed_literal_list_expressions() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections = vec![Projection::Expression {
        expression: ScalarExpression::TypedLiteralList {
            literals: Vec::new(),
            element_type: LiteralListElementType::String,
        },
        alias: "values".to_string(),
    }];

    graph
        .validate_graph_plan(&plan)
        .expect("typed empty list expression should validate");
}

#[test]
fn validate_graph_plan_rejects_typed_literal_list_type_mismatches() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections = vec![Projection::Expression {
        expression: ScalarExpression::TypedLiteralList {
            literals: vec![Literal::Integer(1)],
            element_type: LiteralListElementType::String,
        },
        alias: "values".to_string(),
    }];

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("typed literal list mismatch should fail validation");

    assert!(
        error.to_string().contains("INVALID_TYPED_LITERAL_LIST"),
        "{error:?}"
    );
}

#[test]
fn validate_graph_plan_accepts_scalar_expression_projections() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections = vec![Projection::Expression {
        expression: ScalarExpression::Coalesce {
            expressions: vec![
                ScalarExpression::Property(PropertyRef {
                    variable: "person".to_string(),
                    property: "name".to_string(),
                }),
                ScalarExpression::Literal(Literal::String("unknown".to_string())),
            ],
        },
        alias: "owner_name".to_string(),
    }];

    graph
        .validate_graph_plan(&plan)
        .expect("scalar expression projection should validate");
}

#[test]
fn validate_graph_plan_accepts_relationship_type_scalar_expression_projections() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
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

    graph
        .validate_graph_plan(&plan)
        .expect("relationship type scalar expression projection should validate");
}

#[test]
fn validate_graph_plan_accepts_identity_scalar_expression_projections() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections = vec![
        Projection::Expression {
            expression: ScalarExpression::Coalesce {
                expressions: vec![
                    ScalarExpression::Key {
                        variable: "person".to_string(),
                    },
                    ScalarExpression::Literal(Literal::Integer(0)),
                ],
            },
            alias: "person_id".to_string(),
        },
        Projection::Expression {
            expression: ScalarExpression::ToString {
                expression: Box::new(ScalarExpression::ElementId {
                    variable: "person".to_string(),
                }),
            },
            alias: "person_element_id".to_string(),
        },
    ];

    graph
        .validate_graph_plan(&plan)
        .expect("identity scalar expression projections should validate");
}

#[test]
fn validate_graph_plan_accepts_temporal_date_scalar_expressions() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections = vec![
        Projection::Expression {
            expression: date_expression(1984, 10, 11),
            alias: "date_value".to_string(),
        },
        Projection::Expression {
            expression: date_from_string_expression("2015-07-21"),
            alias: "date_string_value".to_string(),
        },
        Projection::Expression {
            expression: ScalarExpression::ToString {
                expression: Box::new(date_from_string_expression("2015-07-21")),
            },
            alias: "date_text".to_string(),
        },
    ];
    plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: date_expression(1984, 10, 11),
        operator: ComparisonOperator::LessThan,
        rhs: ScalarPredicateRhs::Expression(date_expression(1985, 1, 1)),
    }));

    graph
        .validate_graph_plan(&plan)
        .expect("date scalar expressions should validate");
}

#[test]
fn validate_graph_plan_rejects_date_string_constructor_with_non_string_text() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections = vec![Projection::Expression {
        expression: ScalarExpression::Temporal(TemporalExpr::DateFromString {
            text: Box::new(ScalarExpression::Literal(Literal::Integer(1984))),
        }),
        alias: "date_value".to_string(),
    }];

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("date string constructor text should require a string");

    assert!(
        error
            .to_string()
            .contains("date string constructor requires a string scalar expression, got integer"),
        "{error}"
    );
}

#[test]
fn validate_graph_plan_rejects_temporal_date_as_numeric() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections = vec![Projection::Expression {
        expression: ScalarExpression::Arithmetic {
            operator: ArithmeticOperator::Add,
            left: Box::new(date_expression(1984, 10, 11)),
            right: Box::new(ScalarExpression::Literal(Literal::Integer(1))),
        },
        alias: "shifted".to_string(),
    }];

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("date arithmetic should fail validation");

    assert!(
        error
            .to_string()
            .contains("arithmetic requires a numeric scalar expression, got date"),
        "{error}"
    );
}

#[test]
fn validate_graph_plan_rejects_temporal_date_string_comparison() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: date_expression(1984, 10, 11),
        operator: ComparisonOperator::LessThan,
        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
            "1985-01-01".to_string(),
        ))),
    }));

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("date/string comparison should fail validation");

    assert!(
        error.to_string().contains(
            "scalar predicate operands require compatible scalar types, got date and string"
        ),
        "{error}"
    );
}

#[test]
fn validate_graph_plan_accepts_temporal_localdatetime_scalar_expressions() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections = vec![
        Projection::Expression {
            expression: localdatetime_expression(2020, 1, 15, 12, 34, 56, 0, 0, 0),
            alias: "localdatetime_value".to_string(),
        },
        Projection::Expression {
            expression: localdatetime_from_string_expression("2020-01-15T12:34:56"),
            alias: "localdatetime_string_value".to_string(),
        },
        Projection::Expression {
            expression: ScalarExpression::ToString {
                expression: Box::new(localdatetime_from_string_expression("2020-01-15T12:34:56")),
            },
            alias: "localdatetime_text".to_string(),
        },
    ];
    plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: localdatetime_expression(2020, 1, 15, 12, 0, 0, 0, 0, 0),
        operator: ComparisonOperator::LessThan,
        rhs: ScalarPredicateRhs::Expression(localdatetime_expression(
            2020, 1, 16, 0, 0, 0, 0, 0, 0,
        )),
    }));

    graph
        .validate_graph_plan(&plan)
        .expect("localdatetime scalar expressions should validate");
}

#[test]
fn validate_graph_plan_rejects_localdatetime_string_constructor_with_non_string_text() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections = vec![Projection::Expression {
        expression: ScalarExpression::Temporal(TemporalExpr::LocalDateTimeFromString {
            text: Box::new(ScalarExpression::Literal(Literal::Integer(2020))),
        }),
        alias: "localdatetime_value".to_string(),
    }];

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("localdatetime string constructor text should require a string");

    assert!(
        error.to_string().contains(
            "localdatetime string constructor requires a string scalar expression, got integer"
        ),
        "{error}"
    );
}

#[test]
fn validate_graph_plan_rejects_temporal_localdatetime_as_numeric() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections = vec![Projection::Expression {
        expression: ScalarExpression::Arithmetic {
            operator: ArithmeticOperator::Add,
            left: Box::new(localdatetime_expression(2020, 1, 15, 12, 0, 0, 0, 0, 0)),
            right: Box::new(ScalarExpression::Literal(Literal::Integer(1))),
        },
        alias: "shifted".to_string(),
    }];

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("localdatetime arithmetic should fail validation");

    assert!(
        error
            .to_string()
            .contains("arithmetic requires a numeric scalar expression, got localdatetime"),
        "{error}"
    );
}

#[test]
fn validate_graph_plan_rejects_temporal_date_localdatetime_comparison() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: localdatetime_expression(2020, 1, 15, 12, 0, 0, 0, 0, 0),
        operator: ComparisonOperator::LessThan,
        rhs: ScalarPredicateRhs::Expression(date_expression(2020, 1, 15)),
    }));

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("localdatetime/date comparison should fail validation");

    assert!(
        error.to_string().contains(
            "scalar predicate operands require compatible scalar types, got localdatetime and date"
        ),
        "{error}"
    );
}

#[test]
fn validate_graph_plan_rejects_catalog_typed_coalesce_mismatches() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections = vec![Projection::Expression {
        expression: ScalarExpression::Coalesce {
            expressions: vec![
                ScalarExpression::Key {
                    variable: "service".to_string(),
                },
                ScalarExpression::Literal(Literal::String("unknown".to_string())),
            ],
        },
        alias: "service_id".to_string(),
    }];

    let error = graph
        .validate_graph_plan_against_catalog(&plan, &typed_ownership_catalog())
        .expect_err("catalog-typed coalesce mismatch should fail validation");

    assert!(
        error.to_string().contains("INVALID_SCALAR_TYPE"),
        "{error:?}"
    );
    assert!(error.to_string().contains("coalesce"), "{error:?}");
}

#[test]
fn validate_graph_plan_rejects_catalog_typed_case_branch_mismatches() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections = vec![Projection::Expression {
        expression: ScalarExpression::Case {
            alternatives: vec![ScalarCaseAlternative {
                when: PredicateExpression::Boolean(true),
                then: ScalarExpression::RelationshipType {
                    variable: "owns".to_string(),
                    relationship_type: "OWNS".to_string(),
                },
            }],
            else_expression: Some(Box::new(ScalarExpression::Literal(Literal::Integer(1)))),
        },
        alias: "kind".to_string(),
    }];

    let error = graph
        .validate_graph_plan_against_catalog(&plan, &typed_ownership_catalog())
        .expect_err("catalog-typed CASE mismatch should fail validation");

    assert!(
        error.to_string().contains("INVALID_SCALAR_TYPE"),
        "{error:?}"
    );
    assert!(error.to_string().contains("CASE"), "{error:?}");
}

#[test]
fn validate_graph_plan_rejects_catalog_typed_string_functions_over_numeric_values() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections = vec![Projection::Expression {
        expression: ScalarExpression::ToLower {
            expression: Box::new(ScalarExpression::Key {
                variable: "service".to_string(),
            }),
        },
        alias: "lower_id".to_string(),
    }];

    let error = graph
        .validate_graph_plan_against_catalog(&plan, &typed_ownership_catalog())
        .expect_err("catalog-typed string function mismatch should fail validation");

    assert!(
        error.to_string().contains("INVALID_SCALAR_TYPE"),
        "{error:?}"
    );
    assert!(error.to_string().contains("string"), "{error:?}");
}

#[test]
fn validate_graph_plan_rejects_catalog_typed_numeric_functions_over_string_values() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections = vec![Projection::Expression {
        expression: ScalarExpression::Abs {
            expression: Box::new(ScalarExpression::Property(PropertyRef {
                variable: "person".to_string(),
                property: "name".to_string(),
            })),
        },
        alias: "abs_name".to_string(),
    }];

    let error = graph
        .validate_graph_plan_against_catalog(&plan, &typed_ownership_catalog())
        .expect_err("catalog-typed numeric function mismatch should fail validation");

    assert!(
        error.to_string().contains("INVALID_SCALAR_TYPE"),
        "{error:?}"
    );
    assert!(error.to_string().contains("numeric"), "{error:?}");
}

#[test]
fn validate_graph_plan_rejects_catalog_typed_scalar_predicate_mismatches() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: ScalarExpression::Key {
            variable: "service".to_string(),
        },
        operator: ComparisonOperator::Equal,
        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
            "10".to_string(),
        ))),
    }));

    let error = graph
        .validate_graph_plan_against_catalog(&plan, &typed_ownership_catalog())
        .expect_err("catalog-typed scalar predicate mismatch should fail validation");

    assert!(
        error.to_string().contains("INVALID_SCALAR_TYPE"),
        "{error:?}"
    );
    assert!(error.to_string().contains("predicate"), "{error:?}");
}

#[test]
fn validate_graph_plan_rejects_catalog_typed_property_predicate_mismatches() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.predicate = Some(PredicateExpression::Comparison(PropertyPredicate {
        property: PropertyRef {
            variable: "service".to_string(),
            property: "name".to_string(),
        },
        operator: ComparisonOperator::Equal,
        rhs: PredicateRhs::Literal(Literal::Integer(10)),
    }));

    let error = graph
        .validate_graph_plan_against_catalog(&plan, &typed_ownership_catalog())
        .expect_err("catalog-typed property predicate mismatch should fail validation");

    assert!(
        error.to_string().contains("INVALID_SCALAR_TYPE"),
        "{error:?}"
    );
    assert!(error.to_string().contains("predicate"), "{error:?}");
}

#[test]
fn validate_graph_plan_rejects_keyless_relationship_element_id_scalar_expressions() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections = vec![Projection::Expression {
        expression: ScalarExpression::ElementId {
            variable: "owns".to_string(),
        },
        alias: "ownership_element_id".to_string(),
    }];

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("keyless relationship elementId scalar should fail validation");

    assert!(
        error.to_string().contains("INVALID_ELEMENT_ID_PROJECTION"),
        "{error:?}"
    );
}

#[test]
fn validate_graph_plan_rejects_invalid_relationship_type_scalar_expression_projections() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections = vec![Projection::Expression {
        expression: ScalarExpression::RelationshipType {
            variable: "person".to_string(),
            relationship_type: "OWNS".to_string(),
        },
        alias: "relationship_type".to_string(),
    }];

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("relationship type scalar over a node should fail validation");

    assert!(
        error.to_string().contains("INVALID_TYPE_PROJECTION"),
        "{error:?}"
    );
}

#[test]
fn validate_graph_plan_rejects_unknown_properties_in_scalar_expression_projections() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections = vec![Projection::Expression {
        expression: ScalarExpression::Coalesce {
            expressions: vec![
                ScalarExpression::Property(PropertyRef {
                    variable: "person".to_string(),
                    property: "missing".to_string(),
                }),
                ScalarExpression::Literal(Literal::String("unknown".to_string())),
            ],
        },
        alias: "owner_name".to_string(),
    }];

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("unknown scalar expression property should fail validation");

    assert!(error.to_string().contains("UNKNOWN_PROPERTY"), "{error:?}");
}

#[test]
fn validate_graph_plan_rejects_unknown_properties_in_scalar_predicates() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: ScalarExpression::Coalesce {
            expressions: vec![
                ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "missing".to_string(),
                }),
                ScalarExpression::Literal(Literal::String("unknown".to_string())),
            ],
        },
        operator: ComparisonOperator::Equal,
        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
            "prod".to_string(),
        ))),
    }));

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("unknown scalar predicate property should fail validation");

    assert!(error.to_string().contains("UNKNOWN_PROPERTY"), "{error:?}");
}

#[test]
fn validate_graph_plan_rejects_keyless_relationship_key_projections() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections = vec![Projection::Key {
        variable: "owns".to_string(),
        alias: "ownership_id".to_string(),
    }];

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("keyless relationship id projection should fail validation");

    assert!(
        error.to_string().contains("INVALID_KEY_PROJECTION"),
        "{error:?}"
    );
}

#[test]
fn validate_graph_plan_accepts_node_labels_projections() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections = vec![Projection::NodeLabels {
        variable: "person".to_string(),
        label: "Person".to_string(),
        alias: "labels".to_string(),
    }];

    graph
        .validate_graph_plan(&plan)
        .expect("node labels projection should validate");
}

#[test]
fn validate_graph_plan_rejects_labels_projection_on_relationships() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections = vec![Projection::NodeLabels {
        variable: "owns".to_string(),
        label: "OWNS".to_string(),
        alias: "labels".to_string(),
    }];

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("relationship labels projection should fail validation");

    assert!(
        error.to_string().contains("INVALID_LABELS_PROJECTION"),
        "{error:?}"
    );
}

#[test]
fn validate_graph_plan_accepts_property_keys_projections() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections = vec![
        Projection::PropertyKeys {
            variable: "person".to_string(),
            alias: "person_keys".to_string(),
        },
        Projection::PropertyKeys {
            variable: "owns".to_string(),
            alias: "ownership_keys".to_string(),
        },
    ];

    graph
        .validate_graph_plan(&plan)
        .expect("property keys projections should validate");
}

#[test]
fn validate_graph_plan_accepts_property_key_membership_predicates() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.predicate = Some(PredicateExpression::PropertyKeyMembership(
        PropertyKeyMembershipPredicate {
            variable: "person".to_string(),
            key: "name".to_string(),
            presence_variable: None,
        },
    ));

    graph
        .validate_graph_plan(&plan)
        .expect("property key membership predicate should validate");
}

#[test]
fn validate_graph_plan_rejects_property_key_membership_on_unknown_variables() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.predicate = Some(PredicateExpression::PropertyKeyMembership(
        PropertyKeyMembershipPredicate {
            variable: "unknown".to_string(),
            key: "name".to_string(),
            presence_variable: None,
        },
    ));

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("unknown property key membership variable should fail validation");

    assert!(error.to_string().contains("UNKNOWN_VARIABLE"), "{error:?}");
}

#[test]
fn validate_graph_plan_rejects_keyless_relationship_key_predicates() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.predicate = Some(PredicateExpression::KeyComparison(KeyPredicate {
        variable: "owns".to_string(),
        operator: ComparisonOperator::Equal,
        rhs: PredicateRhs::Literal(Literal::Integer(100)),
    }));

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("keyless relationship id predicate should fail validation");

    assert!(
        error.to_string().contains("INVALID_KEY_PROJECTION"),
        "{error:?}"
    );
}

#[test]
fn validate_graph_plan_accepts_global_predicates_on_optional_bindings() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.optional_relationships = vec![0];
    plan.predicates = vec![PropertyPredicate {
        property: PropertyRef {
            variable: "service".to_string(),
            property: "tier".to_string(),
        },
        operator: ComparisonOperator::Equal,
        rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
    }];

    graph
        .validate_graph_plan(&plan)
        .expect("global optional binding predicate should validate");
}

#[test]
fn validate_graph_plan_accepts_global_scalar_predicates_on_optional_bindings() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.optional_relationships = vec![0];
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
        operator: ComparisonOperator::Equal,
        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
            "prod".to_string(),
        ))),
    }));

    graph
        .validate_graph_plan(&plan)
        .expect("global optional scalar predicate should validate");
}

#[test]
fn validate_graph_plan_accepts_optional_match_scoped_predicates() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.optional_relationships = vec![0];
    plan.optional_matches = vec![OptionalMatchScope {
        node_indices: Vec::new(),
        relationship_indices: vec![0],
        predicate: Some(PredicateExpression::Comparison(PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
        })),
    }];

    graph
        .validate_graph_plan(&plan)
        .expect("scoped optional predicate should validate");
}

#[test]
fn validate_graph_plan_accepts_multihop_optional_match_scope() {
    let graph = Declaration::from_yaml(
        r"
version: 1
name: dependencies
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
",
    )
    .expect("graph should parse");
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
        ],
        optional_relationships: vec![0, 1],
        optional_matches: vec![OptionalMatchScope {
            node_indices: vec![1, 2],
            relationship_indices: vec![0, 1],
            predicate: None,
        }],
        distinct: false,
        projections: vec![Projection::Property {
            property: PropertyRef {
                variable: "target".to_string(),
                property: "name".to_string(),
            },
            alias: Some("target".to_string()),
        }],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    graph
        .validate_graph_plan(&plan)
        .expect("multi-hop optional scope should validate");
}

#[test]
fn validate_graph_plan_accepts_multihop_optional_match_between_bound_endpoints() {
    let graph = Declaration::from_yaml(
        r"
version: 1
name: dependencies
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
",
    )
    .expect("graph should parse");
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
        projections: vec![Projection::Property {
            property: PropertyRef {
                variable: "middle".to_string(),
                property: "name".to_string(),
            },
            alias: Some("middle".to_string()),
        }],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    graph
        .validate_graph_plan(&plan)
        .expect("bound-endpoint multi-hop optional scope should validate");
}

#[test]
fn validate_graph_plan_rejects_multi_relationship_optional_match_scopes() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    let mut second_relationship = plan
        .relationships
        .first()
        .expect("ownership plan should contain a relationship")
        .clone();
    second_relationship.variable = Some("second_owns".to_string());
    plan.relationships.push(second_relationship);
    plan.optional_relationships = vec![0, 1];
    plan.optional_matches = vec![OptionalMatchScope {
        node_indices: Vec::new(),
        relationship_indices: vec![0, 1],
        predicate: None,
    }];

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("multi-relationship optional scope should fail validation");

    assert!(
        error
            .to_string()
            .contains("UNSUPPORTED_OPTIONAL_MATCH_SCOPE"),
        "{error:?}"
    );
}

#[test]
fn validate_graph_plan_rejects_optional_match_node_indices_outside_scope() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.nodes.push(NodePattern {
        variable: "unrelated".to_string(),
        label: "Service".to_string(),
    });
    plan.optional_relationships = vec![0];
    plan.optional_matches = vec![OptionalMatchScope {
        node_indices: vec![2],
        relationship_indices: vec![0],
        predicate: None,
    }];

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("out-of-scope optional node index should fail validation");

    assert!(
        error.to_string().contains("INVALID_OPTIONAL_MATCH_SCOPE"),
        "{error:?}"
    );
}

#[test]
fn validate_graph_plan_rejects_optional_match_predicates_outside_scope() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.nodes.push(NodePattern {
        variable: "other".to_string(),
        label: "Person".to_string(),
    });
    plan.optional_relationships = vec![0];
    plan.optional_matches = vec![OptionalMatchScope {
        node_indices: Vec::new(),
        relationship_indices: vec![0],
        predicate: Some(PredicateExpression::Comparison(PropertyPredicate {
            property: PropertyRef {
                variable: "other".to_string(),
                property: "name".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Literal(Literal::String("platform".to_string())),
        })),
    }];

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("out-of-scope optional predicate should fail validation");

    assert!(
        error.to_string().contains("UNSUPPORTED_OPTIONAL_PREDICATE"),
        "{error:?}"
    );
}

#[test]
fn validate_graph_plan_rejects_non_count_node_aggregate_targets() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections = vec![Projection::Aggregate {
        function: AggregateFunction::Sum,
        target: AggregateTarget::VariableKey {
            variable: "service".to_string(),
        },
        distinct: false,
        alias: "service_sum".to_string(),
    }];

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("non-count node aggregate target should fail validation");

    assert!(
        error.to_string().contains("INVALID_AGGREGATE_TARGET"),
        "{error:?}"
    );
}

#[test]
fn validate_graph_plan_accepts_collect_node_aggregate_targets() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections = vec![Projection::Aggregate {
        function: AggregateFunction::Collect,
        target: AggregateTarget::VariableKey {
            variable: "service".to_string(),
        },
        distinct: false,
        alias: "services".to_string(),
    }];

    graph
        .validate_graph_plan(&plan)
        .expect("collect node aggregate target should validate");
}

#[test]
fn validate_graph_plan_rejects_unknown_post_projection_aliases() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.post_projection_predicate = Some(ProjectionPredicateExpression::Comparison(
        ProjectionPredicate {
            alias: "missing".to_string(),
            operator: ComparisonOperator::Equal,
            rhs: ProjectionPredicateRhs::Literal(Literal::String("Ada".to_string())),
        },
    ));

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("unknown projected alias should fail validation");

    assert!(
        error.to_string().contains("UNKNOWN_PROJECTION_ALIAS"),
        "{error:?}"
    );
}

#[test]
fn validate_graph_plan_rejects_non_boolean_bare_post_projection_alias_predicates() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.projections = vec![Projection::Literal {
        literal: Literal::String("Ada Lovelace".to_string()),
        alias: "owner".to_string(),
    }];
    plan.post_projection_predicate = Some(ProjectionPredicateExpression::Comparison(
        ProjectionPredicate {
            alias: "owner".to_string(),
            operator: ComparisonOperator::Equal,
            rhs: ProjectionPredicateRhs::Literal(Literal::Boolean(true)),
        },
    ));

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("non-boolean projected alias should not be usable as a bare predicate");

    assert!(
        error.to_string().contains("INVALID_SCALAR_TYPE"),
        "{error:?}"
    );
    assert!(error.to_string().contains("boolean"), "{error:?}");
}

#[test]
fn validate_graph_plan_rejects_unknown_rhs_properties_before_lowering() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.predicates = vec![PropertyPredicate {
        property: PropertyRef {
            variable: "person".to_string(),
            property: "name".to_string(),
        },
        operator: ComparisonOperator::Equal,
        rhs: PredicateRhs::Property(PropertyRef {
            variable: "service".to_string(),
            property: "missing".to_string(),
        }),
    }];

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("unknown RHS property should fail validation");

    assert!(error.to_string().contains("UNKNOWN_PROPERTY"), "{error:?}");
}

#[test]
fn validate_graph_plan_rejects_unknown_properties_inside_predicate_expression() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
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
                property: "missing".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Literal(Literal::String("dev".to_string())),
        })),
    });

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("unknown property inside predicate tree should fail validation");

    assert!(error.to_string().contains("UNKNOWN_PROPERTY"), "{error:?}");
}

#[test]
fn validate_graph_plan_rejects_invalid_null_comparisons_inside_predicate_expression() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.predicate = Some(PredicateExpression::Not {
        expression: Box::new(PredicateExpression::Comparison(PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            },
            operator: ComparisonOperator::GreaterThan,
            rhs: PredicateRhs::Literal(Literal::Null),
        })),
    });

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("ordered null comparison inside predicate tree should fail validation");

    assert!(
        error.to_string().contains("INVALID_NULL_COMPARISON"),
        "{error:?}"
    );
}

#[test]
fn validate_graph_plan_accepts_null_values_in_in_lists() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.predicates = vec![PropertyPredicate {
        property: PropertyRef {
            variable: "service".to_string(),
            property: "tier".to_string(),
        },
        operator: ComparisonOperator::In,
        rhs: PredicateRhs::List(vec![Literal::String("prod".to_string()), Literal::Null]),
    }];

    graph
        .validate_graph_plan(&plan)
        .expect("null values in IN lists should validate");
}

#[test]
fn validate_graph_plan_rejects_list_rhs_without_in_operator() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.predicates = vec![PropertyPredicate {
        property: PropertyRef {
            variable: "service".to_string(),
            property: "tier".to_string(),
        },
        operator: ComparisonOperator::Equal,
        rhs: PredicateRhs::List(vec![Literal::String("prod".to_string())]),
    }];

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("literal list without IN should fail validation");

    assert!(
        error.to_string().contains("INVALID_PREDICATE_OPERAND"),
        "{error:?}"
    );
}

#[test]
fn validate_graph_plan_accepts_presence_predicates_for_keyless_relationships() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.predicate = Some(PredicateExpression::Presence(PresencePredicate {
        variable: "owns".to_string(),
        operator: ComparisonOperator::Equal,
    }));

    graph
        .validate_graph_plan(&plan)
        .expect("presence predicates should validate for keyless relationships");
}

#[test]
fn validate_graph_plan_rejects_invalid_presence_predicate_operator() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.predicate = Some(PredicateExpression::Presence(PresencePredicate {
        variable: "owns".to_string(),
        operator: ComparisonOperator::GreaterThan,
    }));

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("invalid presence predicate operator should fail validation");

    assert!(
        error.to_string().contains("INVALID_PRESENCE_PREDICATE"),
        "{error:?}"
    );
}

#[test]
fn validate_graph_plan_rejects_non_string_rhs_for_string_predicates() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.predicates = vec![PropertyPredicate {
        property: PropertyRef {
            variable: "service".to_string(),
            property: "name".to_string(),
        },
        operator: ComparisonOperator::StartsWith,
        rhs: PredicateRhs::Literal(Literal::Integer(10)),
    }];

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("non-string RHS for string predicate should fail validation");

    assert!(
        error.to_string().contains("INVALID_PREDICATE_OPERAND"),
        "{error:?}"
    );
}

#[test]
fn validate_graph_plan_rejects_property_rhs_for_string_predicates() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.predicates = vec![PropertyPredicate {
        property: PropertyRef {
            variable: "service".to_string(),
            property: "name".to_string(),
        },
        operator: ComparisonOperator::Contains,
        rhs: PredicateRhs::Property(PropertyRef {
            variable: "person".to_string(),
            property: "name".to_string(),
        }),
    }];

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("property RHS for string predicate should fail validation");

    assert!(
        error.to_string().contains("INVALID_PREDICATE_OPERAND"),
        "{error:?}"
    );
}

#[test]
fn validate_graph_plan_rejects_non_string_rhs_for_regex_predicates() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.predicates = vec![PropertyPredicate {
        property: PropertyRef {
            variable: "service".to_string(),
            property: "name".to_string(),
        },
        operator: ComparisonOperator::RegexMatch,
        rhs: PredicateRhs::Literal(Literal::Integer(10)),
    }];

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("non-string RHS for regex predicate should fail validation");

    assert!(
        error.to_string().contains("INVALID_PREDICATE_OPERAND"),
        "{error:?}"
    );
}

#[test]
fn validate_graph_plan_rejects_distinct_ordering_by_unprojected_properties() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.distinct = true;
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::Property(PropertyRef {
            variable: "service".to_string(),
            property: "name".to_string(),
        }),
        direction: OrderDirection::Ascending,
        nulls: None,
    }];

    let error = graph
        .validate_graph_plan(&plan)
        .expect_err("DISTINCT should not order by unprojected properties");

    assert!(
        error.to_string().contains("UNSUPPORTED_DISTINCT_ORDERING"),
        "{error:?}"
    );
}

#[test]
fn validate_graph_plan_accepts_out_of_order_connected_relationships() {
    let graph = Declaration::from_yaml(
        r"
version: 1
name: dependencies
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
",
    )
    .expect("graph should parse");
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
        projections: vec![Projection::Property {
            property: PropertyRef {
                variable: "target".to_string(),
                property: "name".to_string(),
            },
            alias: Some("target".to_string()),
        }],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    graph
        .validate_graph_plan(&plan)
        .expect("connected relationships should validate independent of order");
}

#[test]
fn validate_graph_plan_accepts_disconnected_mandatory_patterns() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan();
    plan.nodes.push(NodePattern {
        variable: "orphan".to_string(),
        label: "Service".to_string(),
    });

    graph
        .validate_graph_plan(&plan)
        .expect("disconnected mandatory nodes should validate for CROSS JOIN lowering");
}

#[test]
fn validate_graph_plan_accepts_optional_match_from_disconnected_component() {
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
            variable: Some("owns".to_string()),
            relationship_type: "OWNS".to_string(),
            left: "person".to_string(),
            direction: Direction::Outgoing,
            right: "owned".to_string(),
        }],
        optional_relationships: vec![0],
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![Projection::Property {
            property: PropertyRef {
                variable: "owned".to_string(),
                property: "name".to_string(),
            },
            alias: Some("owned".to_string()),
        }],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    graph
        .validate_graph_plan(&plan)
        .expect("optional match should anchor to disconnected mandatory component");
}

fn typed_ownership_catalog() -> CatalogInfo {
    CatalogInfo {
        tables: vec![
            typed_table("ops", "people", &[("id", "Int64"), ("full_name", "Utf8")]),
            typed_table(
                "ops",
                "services",
                &[("id", "Int64"), ("service_name", "Utf8"), ("tier", "Utf8")],
            ),
            typed_table(
                "ops",
                "ownerships",
                &[
                    ("person_id", "Int64"),
                    ("service_id", "Int64"),
                    ("since", "Utf8"),
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

fn ownership_plan() -> GraphPlan {
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
    }
}
