use super::*;

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
            .contains("ORDER BY TRY_CAST(\"n1\".\"id\" AS VARCHAR) ASC"),
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
            "SELECT make_date(1984, 10, 11) AS \"d\", TRY_CAST(make_date(1984, 10, 11) AS VARCHAR) AS \"text\""
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
             TRY_CAST(CAST('2020-01-15T12:34:56' AS TIMESTAMP) AS VARCHAR) AS \"text\""
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
fn lower_graph_plan_renders_temporal_zoneddatetime_expressions() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.projections = vec![
        Projection::Expression {
            expression: zoneddatetime_from_string_expression("2020-06-01T09:00:00+01:00", "+01:00"),
            alias: "offset_datetime".to_string(),
        },
        Projection::Expression {
            expression: zoneddatetime_expression(
                1984,
                10,
                11,
                12,
                31,
                14,
                0,
                0,
                645_876_123,
                "Europe/Stockholm",
            ),
            alias: "named_datetime".to_string(),
        },
        Projection::Expression {
            expression: ScalarExpression::ToString {
                expression: Box::new(zoneddatetime_from_string_expression(
                    "1984-10-11T12:31:14.645876123",
                    "Europe/Stockholm",
                )),
            },
            alias: "text".to_string(),
        },
    ];
    plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: zoneddatetime_from_string_expression("2020-06-01T09:00:00+01:00", "+01:00"),
        operator: ComparisonOperator::LessThan,
        rhs: ScalarPredicateRhs::Expression(zoneddatetime_from_string_expression(
            "2020-06-01T08:30:00Z",
            "+00:00",
        )),
    }));
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::Scalar(zoneddatetime_from_string_expression(
            "2020-06-01T09:00:00",
            "Europe/London",
        )),
        direction: OrderDirection::Ascending,
        nulls: None,
    }];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("zoneddatetime scalar expressions should lower");

    assert!(
        translation.sql().contains(
            "SELECT coral_zoneddatetime_to_iso(arrow_cast('2020-06-01T09:00:00+01:00', 'Timestamp(ns, Some(\"+01:00\"))'), '+01:00') AS \"offset_datetime\", \
             coral_zoneddatetime_to_iso(arrow_cast('1984-10-11T12:31:14.645876123', 'Timestamp(ns, Some(\"Europe/Stockholm\"))'), 'Europe/Stockholm') AS \"named_datetime\", \
             coral_zoneddatetime_to_iso(arrow_cast('1984-10-11T12:31:14.645876123', 'Timestamp(ns, Some(\"Europe/Stockholm\"))'), 'Europe/Stockholm') AS \"text\""
        ),
        "{}",
        translation.sql()
    );
    assert!(
        translation.sql().contains(
            "WHERE arrow_cast('2020-06-01T09:00:00+01:00', 'Timestamp(ns, Some(\"+01:00\"))') < arrow_cast('2020-06-01T08:30:00Z', 'Timestamp(ns, Some(\"+00:00\"))')"
        ),
        "{}",
        translation.sql()
    );
    assert!(
        translation.sql().contains(
            "ORDER BY arrow_cast('2020-06-01T09:00:00', 'Timestamp(ns, Some(\"Europe/London\"))') ASC"
        ),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_formats_stored_zoneddatetime_projection_from_catalog_type() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.relationships
        .get_mut(0)
        .expect("ownership plan should have a relationship")
        .variable = Some("owns".to_string());
    plan.projections = vec![
        Projection::Property {
            property: PropertyRef {
                variable: "owns".to_string(),
                property: "since".to_string(),
            },
            alias: Some("since".to_string()),
        },
        Projection::Expression {
            expression: ScalarExpression::ToString {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "owns".to_string(),
                    property: "since".to_string(),
                })),
            },
            alias: "since_text".to_string(),
        },
    ];

    let translation = graph
        .lower_graph_plan_against_catalog(
            &plan,
            &typed_ownership_catalog_with_since_type("Timestamp(ns, Some(\"Europe/London\"))"),
        )
        .expect("stored zoned timestamp projection should lower");

    assert!(
        translation.sql().contains(
            "SELECT coral_zoneddatetime_to_iso(\"r0\".\"since\", 'Europe/London') AS \"since\", \
             coral_zoneddatetime_to_iso(\"r0\".\"since\", 'Europe/London') AS \"since_text\""
        ),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_temporal_localtime_expressions() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.projections = vec![
        Projection::Expression {
            expression: localtime_expression(12, 34, 56, 0, 0, 0),
            alias: "t".to_string(),
        },
        Projection::Expression {
            expression: ScalarExpression::ToString {
                expression: Box::new(localtime_expression(12, 34, 56, 0, 0, 0)),
            },
            alias: "text".to_string(),
        },
    ];
    plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: localtime_expression(12, 0, 0, 0, 0, 0),
        operator: ComparisonOperator::LessThan,
        rhs: ScalarPredicateRhs::Expression(localtime_expression(13, 0, 0, 0, 0, 0)),
    }));
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::Scalar(localtime_expression(12, 34, 56, 0, 0, 0)),
        direction: OrderDirection::Ascending,
        nulls: None,
    }];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("localtime scalar expressions should lower");

    assert!(
        translation.sql().contains(
            "SELECT CAST('12:34:56' AS TIME) AS \"t\", \
             TRY_CAST(CAST('12:34:56' AS TIME) AS VARCHAR) AS \"text\""
        ),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("WHERE CAST('12:00:00' AS TIME) < CAST('13:00:00' AS TIME)"),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("ORDER BY CAST('12:34:56' AS TIME) ASC"),
        "{}",
        translation.sql()
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "Temporal arithmetic SQL coverage keeps related date, datetime, time, and zoned duration cases together."
)]
fn lower_graph_plan_renders_temporal_duration_arithmetic_expressions() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.projections = vec![
        Projection::Expression {
            expression: ScalarExpression::Arithmetic {
                operator: ArithmeticOperator::Add,
                left: Box::new(date_from_string_expression("2020-01-31")),
                right: Box::new(duration_expression(1, 0, 0, 0)),
            },
            alias: "date_shift".to_string(),
        },
        Projection::Expression {
            expression: ScalarExpression::Arithmetic {
                operator: ArithmeticOperator::Add,
                left: Box::new(localdatetime_from_string_expression("2020-01-01T00:00:00")),
                right: Box::new(duration_expression(0, 0, 5_400, 0)),
            },
            alias: "datetime_shift".to_string(),
        },
        Projection::Expression {
            expression: ScalarExpression::Arithmetic {
                operator: ArithmeticOperator::Add,
                left: Box::new(localtime_from_string_expression("12:00:00")),
                right: Box::new(duration_expression(0, 0, 5_400, 0)),
            },
            alias: "time_shift".to_string(),
        },
        Projection::Expression {
            expression: ScalarExpression::Arithmetic {
                operator: ArithmeticOperator::Add,
                left: Box::new(date_from_string_expression("2020-01-01")),
                right: Box::new(ScalarExpression::Arithmetic {
                    operator: ArithmeticOperator::Multiply,
                    left: Box::new(duration_expression(0, 1, 0, 0)),
                    right: Box::new(ScalarExpression::Literal(Literal::Integer(2))),
                }),
            },
            alias: "scaled_date_shift".to_string(),
        },
        Projection::Expression {
            expression: ScalarExpression::Arithmetic {
                operator: ArithmeticOperator::Add,
                left: Box::new(zoneddatetime_from_string_expression(
                    "2020-03-29T00:30:00",
                    "Europe/London",
                )),
                right: Box::new(duration_expression(0, 0, 3_600, 0)),
            },
            alias: "zoned_shift".to_string(),
        },
        Projection::Expression {
            expression: ScalarExpression::Arithmetic {
                operator: ArithmeticOperator::Subtract,
                left: Box::new(zoneddatetime_from_string_expression(
                    "2020-06-01T13:00:00+01:00",
                    "Europe/London",
                )),
                right: Box::new(zoneddatetime_from_string_expression(
                    "2020-06-01T12:00:00Z",
                    "+00:00",
                )),
            },
            alias: "zoned_delta".to_string(),
        },
    ];
    plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: ScalarExpression::Arithmetic {
            operator: ArithmeticOperator::Subtract,
            left: Box::new(date_from_string_expression("2020-03-15")),
            right: Box::new(duration_expression(1, 0, 0, 0)),
        },
        operator: ComparisonOperator::GreaterThan,
        rhs: ScalarPredicateRhs::Expression(date_from_string_expression("2020-02-01")),
    }));
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::Scalar(ScalarExpression::Arithmetic {
            operator: ArithmeticOperator::Add,
            left: Box::new(date_from_string_expression("2020-01-01")),
            right: Box::new(duration_expression(0, 1, 0, 0)),
        }),
        direction: OrderDirection::Ascending,
        nulls: None,
    }];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("temporal duration arithmetic should lower");

    assert!(
        translation.sql().contains(
             "SELECT (CAST('2020-01-31' AS DATE) + CAST('1 months 0 days 0 seconds' AS INTERVAL)) AS \"date_shift\", \
             (CAST('2020-01-01T00:00:00' AS TIMESTAMP) + CAST('0 months 0 days 5400 seconds' AS INTERVAL)) AS \"datetime_shift\", \
             CAST((CAST(concat('1970-01-01T', CAST(CAST('12:00:00' AS TIME) AS VARCHAR)) AS TIMESTAMP) + CAST('0 months 0 days 5400 seconds' AS INTERVAL)) AS TIME) AS \"time_shift\", \
             (CAST('2020-01-01' AS DATE) + CAST('0 months 2 days 0 seconds' AS INTERVAL)) AS \"scaled_date_shift\", \
             coral_zoneddatetime_to_iso((arrow_cast('2020-03-29T00:30:00', 'Timestamp(ns, Some(\"Europe/London\"))') + CAST('0 months 0 days 3600 seconds' AS INTERVAL)), 'Europe/London') AS \"zoned_shift\""
        ),
        "{}",
        translation.sql()
    );
    assert!(
        translation.sql().contains(
            "coral_duration_to_iso(CASE WHEN arrow_cast('2020-06-01T13:00:00+01:00', 'Timestamp(ns, Some(\"Europe/London\"))') IS NULL OR arrow_cast('2020-06-01T12:00:00Z', 'Timestamp(ns, Some(\"+00:00\"))') IS NULL THEN CAST(NULL AS INTERVAL) ELSE CAST(concat('0 months 0 days ', coalesce(CAST(date_part('epoch', (arrow_cast('2020-06-01T13:00:00+01:00', 'Timestamp(ns, Some(\"Europe/London\"))') - arrow_cast('2020-06-01T12:00:00Z', 'Timestamp(ns, Some(\"+00:00\"))'))) AS VARCHAR), '0'), ' seconds') AS INTERVAL) END) AS \"zoned_delta\""
        ),
        "{}",
        translation.sql()
    );
    assert!(
        translation.sql().contains(
            "WHERE (CAST('2020-03-15' AS DATE) - CAST('1 months 0 days 0 seconds' AS INTERVAL)) > CAST('2020-02-01' AS DATE)"
        ),
        "{}",
        translation.sql()
    );
    assert!(
        translation.sql().contains(
            "ORDER BY (CAST('2020-01-01' AS DATE) + CAST('0 months 1 days 0 seconds' AS INTERVAL)) ASC"
        ),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_duration_results_as_iso_strings() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.projections = vec![
        Projection::Expression {
            expression: duration_expression(149, 14, 58_390, 1),
            alias: "bare".to_string(),
        },
        Projection::Expression {
            expression: ScalarExpression::ToString {
                expression: Box::new(duration_expression(0, 0, -61, 999_000_000)),
            },
            alias: "text".to_string(),
        },
    ];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("duration projections should lower through ISO formatter");

    assert!(
        translation.sql().contains(
            "SELECT coral_duration_to_iso(CAST('149 months 14 days 58390.000000001 seconds' AS INTERVAL)) AS \"bare\", \
             coral_duration_to_iso(CAST('0 months 0 days -60.001 seconds' AS INTERVAL)) AS \"text\""
        ),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_temporal_duration_unit_total_functions() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.projections = vec![
        Projection::Expression {
            expression: duration_in_units_expression(
                TemporalDurationUnit::Between,
                date_from_string_expression("1984-10-11"),
                date_from_string_expression("2015-06-24"),
            ),
            alias: "between_duration".to_string(),
        },
        Projection::Expression {
            expression: duration_in_units_expression(
                TemporalDurationUnit::Months,
                date_from_string_expression("1984-10-11"),
                date_from_string_expression("2015-06-24"),
            ),
            alias: "months_duration".to_string(),
        },
        Projection::Expression {
            expression: duration_in_units_expression(
                TemporalDurationUnit::Seconds,
                localdatetime_from_string_expression("2020-01-01T00:00:00"),
                localdatetime_from_string_expression("2020-03-01T12:00:00"),
            ),
            alias: "seconds_duration".to_string(),
        },
        Projection::Expression {
            expression: duration_in_units_expression(
                TemporalDurationUnit::Days,
                date_from_string_expression("1984-10-11"),
                date_from_string_expression("2015-06-24"),
            ),
            alias: "days_duration".to_string(),
        },
        Projection::Expression {
            expression: duration_in_units_expression(
                TemporalDurationUnit::Days,
                localdatetime_from_string_expression("2015-07-21T21:40:32.142"),
                date_from_string_expression("2015-06-24"),
            ),
            alias: "negative_partial_days_duration".to_string(),
        },
        Projection::Expression {
            expression: ScalarExpression::ToStringOrNull {
                expression: Box::new(duration_in_units_expression(
                    TemporalDurationUnit::Seconds,
                    ScalarExpression::Literal(Literal::Null),
                    ScalarExpression::Literal(Literal::Null),
                )),
            },
            alias: "null_duration".to_string(),
        },
    ];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("duration unit-total functions should lower through ISO formatter");

    assert!(
        translation.sql().contains(
            "coral_duration_to_iso(coral_duration_between(CAST('1984-10-11' AS DATE), CAST('2015-06-24' AS DATE))) AS \"between_duration\""
        ),
        "{}",
        translation.sql()
    );
    assert!(
        translation.sql().contains(
            "coral_duration_to_iso(coral_duration_in_months(CAST('1984-10-11' AS DATE), CAST('2015-06-24' AS DATE))) AS \"months_duration\""
        ),
        "{}",
        translation.sql()
    );
    assert!(
        translation.sql().contains(
            "coral_duration_to_iso(CASE WHEN CAST('2020-01-01T00:00:00' AS TIMESTAMP) IS NULL OR CAST('2020-03-01T12:00:00' AS TIMESTAMP) IS NULL THEN CAST(NULL AS INTERVAL) ELSE CAST(concat('0 months 0 days ', coalesce(CAST(date_part('epoch', (CAST('2020-03-01T12:00:00' AS TIMESTAMP) - CAST('2020-01-01T00:00:00' AS TIMESTAMP))) AS VARCHAR), '0'), ' seconds') AS INTERVAL) END) AS \"seconds_duration\""
        ),
        "{}",
        translation.sql()
    );
    assert!(
        translation.sql().contains(
            "CAST(concat('0 months ', coalesce(CAST(trunc(date_part('epoch', (CAST(CAST('2015-06-24' AS DATE) AS TIMESTAMP) - CAST(CAST('1984-10-11' AS DATE) AS TIMESTAMP))) / 86400) AS VARCHAR), '0'), ' days 0 seconds') AS INTERVAL)"
        ),
        "{}",
        translation.sql()
    );
    assert!(
        translation.sql().contains(
            "CAST(concat('0 months ', coalesce(CAST(trunc(date_part('epoch', (CAST(CAST('2015-06-24' AS DATE) AS TIMESTAMP) - CAST('2015-07-21T21:40:32.142' AS TIMESTAMP))) / 86400) AS VARCHAR), '0'), ' days 0 seconds') AS INTERVAL)"
        ),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("coral_duration_to_iso(CASE WHEN CAST(NULL AS TIMESTAMP) IS NULL"),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_temporal_component_expressions() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.projections = vec![
        Projection::Expression {
            expression: temporal_component_expression(
                date_from_string_expression("2020-01-15"),
                TemporalComponentUnit::Year,
            ),
            alias: "year".to_string(),
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
                localdatetime_from_string_expression("2020-01-15T12:34:56.789123456"),
                TemporalComponentUnit::Millisecond,
            ),
            alias: "millisecond".to_string(),
        },
        Projection::Expression {
            expression: temporal_component_expression(
                localtime_from_string_expression("12:34:56.789123456"),
                TemporalComponentUnit::Microsecond,
            ),
            alias: "microsecond".to_string(),
        },
    ];
    plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: temporal_component_expression(
            localdatetime_from_string_expression("2020-01-15T12:34:56"),
            TemporalComponentUnit::Hour,
        ),
        operator: ComparisonOperator::GreaterThan,
        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(11))),
    }));
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::Scalar(temporal_component_expression(
            localtime_from_string_expression("12:34:56"),
            TemporalComponentUnit::Minute,
        )),
        direction: OrderDirection::Ascending,
        nulls: None,
    }];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("temporal component scalar expressions should lower");

    assert!(
        translation.sql().contains(
             "SELECT CAST(date_part('year', CAST('2020-01-15' AS DATE)) AS BIGINT) AS \"year\", \
             CAST(date_part('hour', CAST('2020-01-15T12:34:56' AS TIMESTAMP)) AS BIGINT) AS \"hour\", \
             (CAST(date_part('millisecond', CAST('2020-01-15T12:34:56.789123456' AS TIMESTAMP)) AS BIGINT) % 1000) AS \"millisecond\", \
             (CAST(date_part('microsecond', CAST('12:34:56.789123456' AS TIME)) AS BIGINT) % 1000000) AS \"microsecond\""
        ),
        "{}",
        translation.sql()
    );
    assert!(
        translation.sql().contains(
            "WHERE CAST(date_part('hour', CAST('2020-01-15T12:34:56' AS TIMESTAMP)) AS BIGINT) > 11"
        ),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("ORDER BY CAST(date_part('minute', CAST('12:34:56' AS TIME)) AS BIGINT) ASC"),
        "{}",
        translation.sql()
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "Zoned datetime accessor SQL coverage keeps the related accessor projections together."
)]
fn lower_graph_plan_renders_zoneddatetime_components_and_accessors() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    let london =
        || zoneddatetime_from_string_expression("2020-06-01T13:00:00+01:00", "Europe/London");
    plan.projections = vec![
        Projection::Expression {
            expression: temporal_component_expression(london(), TemporalComponentUnit::Hour),
            alias: "london_hour".to_string(),
        },
        Projection::Expression {
            expression: temporal_component_expression(
                zoneddatetime_from_string_expression(
                    "2020-06-01T23:30:00-04:00",
                    "America/New_York",
                ),
                TemporalComponentUnit::Day,
            ),
            alias: "new_york_day".to_string(),
        },
        Projection::Expression {
            expression: zoneddatetime_accessor_expression(
                london(),
                ZonedDateTimeAccessor::Timezone,
                Some("Europe/London"),
            ),
            alias: "timezone".to_string(),
        },
        Projection::Expression {
            expression: zoneddatetime_accessor_expression(
                london(),
                ZonedDateTimeAccessor::Offset,
                Some("Europe/London"),
            ),
            alias: "offset".to_string(),
        },
        Projection::Expression {
            expression: zoneddatetime_accessor_expression(
                london(),
                ZonedDateTimeAccessor::OffsetSeconds,
                Some("Europe/London"),
            ),
            alias: "offset_seconds".to_string(),
        },
        Projection::Expression {
            expression: zoneddatetime_accessor_expression(
                london(),
                ZonedDateTimeAccessor::OffsetMinutes,
                Some("Europe/London"),
            ),
            alias: "offset_minutes".to_string(),
        },
        Projection::Expression {
            expression: zoneddatetime_accessor_expression(
                london(),
                ZonedDateTimeAccessor::EpochSeconds,
                Some("Europe/London"),
            ),
            alias: "epoch_seconds".to_string(),
        },
        Projection::Expression {
            expression: zoneddatetime_accessor_expression(
                london(),
                ZonedDateTimeAccessor::EpochMillis,
                Some("Europe/London"),
            ),
            alias: "epoch_millis".to_string(),
        },
        Projection::Expression {
            expression: temporal_component_expression(
                ScalarExpression::Arithmetic {
                    operator: ArithmeticOperator::Add,
                    left: Box::new(zoneddatetime_from_string_expression(
                        "2020-03-29T00:30:00",
                        "Europe/London",
                    )),
                    right: Box::new(duration_expression(0, 0, 3_600, 0)),
                },
                TemporalComponentUnit::Hour,
            ),
            alias: "dst_hour".to_string(),
        },
    ];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("zoned datetime components and accessors should lower");
    let london_sql =
        "arrow_cast('2020-06-01T13:00:00+01:00', 'Timestamp(ns, Some(\"Europe/London\"))')";
    let london_offset = format!("right(TRY_CAST({london_sql} AS VARCHAR), 6)");

    for expected in [
        format!("CAST(date_part('hour', {london_sql}) AS BIGINT) AS \"london_hour\""),
        "CAST(date_part('day', arrow_cast('2020-06-01T23:30:00-04:00', 'Timestamp(ns, Some(\"America/New_York\"))')) AS BIGINT) AS \"new_york_day\"".to_string(),
        "'Europe/London' AS \"timezone\"".to_string(),
        format!("{london_offset} AS \"offset\""),
        format!(
            "CASE WHEN {london_offset} IS NULL THEN CAST(NULL AS BIGINT) ELSE ((CASE WHEN left({london_offset}, 1) = '-' THEN -1 ELSE 1 END) * ((CAST(SUBSTRING({london_offset} FROM 2 FOR 2) AS BIGINT) * 3600) + (CAST(SUBSTRING({london_offset} FROM 5 FOR 2) AS BIGINT) * 60))) END AS \"offset_seconds\""
        ),
        format!(
            "CASE WHEN {london_offset} IS NULL THEN CAST(NULL AS BIGINT) ELSE ((CASE WHEN left({london_offset}, 1) = '-' THEN -1 ELSE 1 END) * ((CAST(SUBSTRING({london_offset} FROM 2 FOR 2) AS BIGINT) * 60) + (CAST(SUBSTRING({london_offset} FROM 5 FOR 2) AS BIGINT) * 1))) END AS \"offset_minutes\""
        ),
        format!("CAST(trunc(date_part('epoch', {london_sql})) AS BIGINT) AS \"epoch_seconds\""),
        format!("CAST(trunc(date_part('epoch', {london_sql}) * 1000) AS BIGINT) AS \"epoch_millis\""),
        "CAST(date_part('hour', (arrow_cast('2020-03-29T00:30:00', 'Timestamp(ns, Some(\"Europe/London\"))') + CAST('0 months 0 days 3600 seconds' AS INTERVAL))) AS BIGINT) AS \"dst_hour\"".to_string(),
    ] {
        assert!(
            translation.sql().contains(&expected),
            "expected SQL fragment {expected:?} in {}",
            translation.sql()
        );
    }
}

#[test]
fn lower_graph_plan_renders_duration_component_expressions() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.projections = vec![
        Projection::Expression {
            expression: temporal_component_expression(
                duration_expression(14, 0, 0, 0),
                TemporalComponentUnit::Years,
            ),
            alias: "years".to_string(),
        },
        Projection::Expression {
            expression: temporal_component_expression(
                duration_expression(14, 0, 0, 0),
                TemporalComponentUnit::MonthsOfYear,
            ),
            alias: "monthsOfYear".to_string(),
        },
        Projection::Expression {
            expression: temporal_component_expression(
                duration_expression(0, 10, 0, 0),
                TemporalComponentUnit::DaysOfWeek,
            ),
            alias: "daysOfWeek".to_string(),
        },
        Projection::Expression {
            expression: temporal_component_expression(
                duration_expression(0, 0, 3_661, 111_111_111),
                TemporalComponentUnit::NanosecondsOfSecond,
            ),
            alias: "nanosecondsOfSecond".to_string(),
        },
    ];
    plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: temporal_component_expression(
            duration_expression(14, 0, 0, 0),
            TemporalComponentUnit::Months,
        ),
        operator: ComparisonOperator::Equal,
        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(14))),
    }));
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::Scalar(temporal_component_expression(
            duration_expression(0, 0, 3_661, 111_111_111),
            TemporalComponentUnit::SecondsOfMinute,
        )),
        direction: OrderDirection::Ascending,
        nulls: None,
    }];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("duration component scalar expressions should lower");

    assert!(
        translation.sql().contains(
            "SELECT coral_duration_part(CAST('14 months 0 days 0 seconds' AS INTERVAL), 'years') AS \"years\", \
             coral_duration_part(CAST('14 months 0 days 0 seconds' AS INTERVAL), 'monthsOfYear') AS \"monthsOfYear\", \
             coral_duration_part(CAST('0 months 10 days 0 seconds' AS INTERVAL), 'daysOfWeek') AS \"daysOfWeek\", \
             coral_duration_part(CAST('0 months 0 days 3661.111111111 seconds' AS INTERVAL), 'nanosecondsOfSecond') AS \"nanosecondsOfSecond\""
        ),
        "{}",
        translation.sql()
    );
    assert!(
        translation.sql().contains(
            "WHERE coral_duration_part(CAST('14 months 0 days 0 seconds' AS INTERVAL), 'months') = 14"
        ),
        "{}",
        translation.sql()
    );
    assert!(
        translation.sql().contains(
            "ORDER BY coral_duration_part(CAST('0 months 0 days 3661.111111111 seconds' AS INTERVAL), 'secondsOfMinute') ASC"
        ),
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
fn lower_graph_plan_renders_scalar_cast_expressions() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.projections = vec![
        expression_projection(
            "id_text",
            ScalarExpression::ToString {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "id".to_string(),
                })),
            },
        ),
        expression_projection(
            "risk_float",
            ScalarExpression::ToFloat {
                expression: Box::new(service_risk_expression()),
            },
        ),
        expression_projection(
            "active_bool",
            ScalarExpression::ToBoolean {
                expression: Box::new(ScalarExpression::Literal(Literal::String(
                    "true".to_string(),
                ))),
            },
        ),
    ];
    plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: ScalarExpression::ToInteger {
            expression: Box::new(ScalarExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "id".to_string(),
            })),
        },
        operator: ComparisonOperator::GreaterThan,
        rhs: ScalarPredicateRhs::Expression(integer_literal(0)),
    }));
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::Scalar(ScalarExpression::ToInteger {
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
        .expect("scalar casts should lower");

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
             TRY_CAST(CAST('2015-07-21' AS DATE) AS VARCHAR) AS \"text\""
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
             TRY_CAST(CAST('2020-01-15T12:34:56' AS TIMESTAMP) AS VARCHAR) AS \"text\""
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

#[test]
fn renders_localtime_from_string_projection() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.projections = vec![
        expression_projection("t", localtime_from_string_expression("12:34:56")),
        expression_projection(
            "text",
            ScalarExpression::ToString {
                expression: Box::new(localtime_from_string_expression("12:34:56")),
            },
        ),
    ];
    plan.order_by.clear();
    plan.limit = None;

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("localtime string projection should lower");

    assert!(
        translation.sql().contains(
            "SELECT CAST('12:34:56' AS TIME) AS \"t\", \
             TRY_CAST(CAST('12:34:56' AS TIME) AS VARCHAR) AS \"text\""
        ),
        "{}",
        translation.sql()
    );
}

#[test]
fn renders_localtime_from_string_comparison() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: localtime_from_string_expression("12:00:00"),
        operator: ComparisonOperator::LessThan,
        rhs: ScalarPredicateRhs::Expression(localtime_from_string_expression("13:00:00")),
    }));
    plan.order_by.clear();
    plan.limit = None;

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("localtime string comparison should lower");

    assert!(
        translation
            .sql()
            .contains("WHERE CAST('12:00:00' AS TIME) < CAST('13:00:00' AS TIME)"),
        "{}",
        translation.sql()
    );
}

#[test]
fn renders_localtime_from_string_order() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.predicates.clear();
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::Scalar(localtime_from_string_expression("12:34:56")),
        direction: OrderDirection::Ascending,
        nulls: None,
    }];
    plan.limit = None;

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("localtime string ordering should lower");

    assert!(
        translation
            .sql()
            .contains("ORDER BY CAST('12:34:56' AS TIME) ASC"),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_temporal_coercion_casts_from_catalog_type() {
    let cases = [
        (
            "Timestamp(Nanosecond, None)",
            "2020-06-01T09:00:00Z",
            "CAST('2020-06-01T09:00:00Z' AS TIMESTAMP)",
        ),
        ("Date32", "2020-06-01", "CAST('2020-06-01' AS DATE)"),
        ("Time64(Nanosecond)", "09:00:00", "CAST('09:00:00' AS TIME)"),
    ];
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");

    for (since_type, source, expected_cast) in cases {
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.relationships
            .get_mut(0)
            .expect("ownership plan should have a relationship")
            .variable = Some("owns".to_string());
        plan.predicates.clear();
        plan.predicate = Some(PredicateExpression::Comparison(PropertyPredicate {
            property: PropertyRef {
                variable: "owns".to_string(),
                property: "since".to_string(),
            },
            operator: ComparisonOperator::GreaterThan,
            rhs: PredicateRhs::TemporalCoercion {
                source: source.to_string(),
            },
        }));
        plan.order_by.clear();
        plan.limit = None;

        let translation = graph
            .lower_graph_plan_against_catalog(
                &plan,
                &typed_ownership_catalog_with_since_type(since_type),
            )
            .expect("temporal coercion should lower with catalog type");

        assert!(
            translation
                .sql()
                .contains(&format!("WHERE \"r0\".\"since\" > {expected_cast}")),
            "{}",
            translation.sql()
        );
    }
}

#[test]
fn lower_graph_plan_renders_temporal_coercion_list_casts_from_catalog_type() {
    let cases = [
        (
            "Timestamp(Nanosecond, None)",
            vec!["2020-06-01T09:00:00Z", "2020-06-02T09:00:00Z"],
            "CAST('2020-06-01T09:00:00Z' AS TIMESTAMP), CAST('2020-06-02T09:00:00Z' AS TIMESTAMP)",
        ),
        (
            "Date32",
            vec!["2020-06-01", "2020-06-02"],
            "CAST('2020-06-01' AS DATE), CAST('2020-06-02' AS DATE)",
        ),
        (
            "Time64(Nanosecond)",
            vec!["09:00:00", "10:00:00"],
            "CAST('09:00:00' AS TIME), CAST('10:00:00' AS TIME)",
        ),
    ];
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");

    for (since_type, sources, expected_casts) in cases {
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.relationships
            .get_mut(0)
            .expect("ownership plan should have a relationship")
            .variable = Some("owns".to_string());
        plan.predicates.clear();
        plan.predicate = Some(PredicateExpression::Comparison(PropertyPredicate {
            property: PropertyRef {
                variable: "owns".to_string(),
                property: "since".to_string(),
            },
            operator: ComparisonOperator::In,
            rhs: PredicateRhs::TemporalCoercionList(
                sources.into_iter().map(str::to_string).collect(),
            ),
        }));
        plan.order_by.clear();
        plan.limit = None;

        let translation = graph
            .lower_graph_plan_against_catalog(
                &plan,
                &typed_ownership_catalog_with_since_type(since_type),
            )
            .expect("temporal coercion list should lower with catalog type");

        assert!(
            translation
                .sql()
                .contains(&format!("WHERE \"r0\".\"since\" IN ({expected_casts})")),
            "{}",
            translation.sql()
        );
    }
}

#[test]
fn lower_graph_plan_renders_negated_temporal_coercion_list_with_casts() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.relationships
        .get_mut(0)
        .expect("ownership plan should have a relationship")
        .variable = Some("owns".to_string());
    plan.predicates.clear();
    plan.predicate = Some(PredicateExpression::Not {
        expression: Box::new(PredicateExpression::Comparison(PropertyPredicate {
            property: PropertyRef {
                variable: "owns".to_string(),
                property: "since".to_string(),
            },
            operator: ComparisonOperator::In,
            rhs: PredicateRhs::TemporalCoercionList(vec![
                "2020-06-01T09:00:00Z".to_string(),
                "2020-06-02T09:00:00Z".to_string(),
            ]),
        })),
    });
    plan.order_by.clear();
    plan.limit = None;

    let translation = graph
        .lower_graph_plan_against_catalog(
            &plan,
            &typed_ownership_catalog_with_since_type("Timestamp(Nanosecond, None)"),
        )
        .expect("negated temporal coercion list should lower with catalog type");

    assert!(
        translation.sql().contains(
            "WHERE NOT (\"r0\".\"since\" IN (CAST('2020-06-01T09:00:00Z' AS TIMESTAMP), CAST('2020-06-02T09:00:00Z' AS TIMESTAMP)))"
        ),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_non_temporal_coercion_list_like_string_list() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut coerced_plan = ownership_plan(Direction::Outgoing);
    coerced_plan.predicates = vec![PropertyPredicate {
        property: PropertyRef {
            variable: "service".to_string(),
            property: "tier".to_string(),
        },
        operator: ComparisonOperator::In,
        rhs: PredicateRhs::TemporalCoercionList(vec!["prod".to_string(), "dev".to_string()]),
    }];
    let mut literal_plan = ownership_plan(Direction::Outgoing);
    literal_plan.predicates = vec![PropertyPredicate {
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
    let catalog = typed_ownership_catalog();

    let coerced_sql = graph
        .lower_graph_plan_against_catalog(&coerced_plan, &catalog)
        .expect("temporal coercion list should lower for string property")
        .sql()
        .to_string();
    let literal_sql = graph
        .lower_graph_plan_against_catalog(&literal_plan, &catalog)
        .expect("literal string list should lower for string property")
        .sql()
        .to_string();

    assert_eq!(coerced_sql, literal_sql);
}

#[test]
fn lower_graph_plan_renders_non_temporal_coercion_like_string_literal() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut coerced_plan = ownership_plan(Direction::Outgoing);
    coerced_plan.predicates = vec![PropertyPredicate {
        property: PropertyRef {
            variable: "service".to_string(),
            property: "tier".to_string(),
        },
        operator: ComparisonOperator::Equal,
        rhs: PredicateRhs::TemporalCoercion {
            source: "prod".to_string(),
        },
    }];
    let mut literal_plan = ownership_plan(Direction::Outgoing);
    literal_plan.predicates = vec![PropertyPredicate {
        property: PropertyRef {
            variable: "service".to_string(),
            property: "tier".to_string(),
        },
        operator: ComparisonOperator::Equal,
        rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
    }];
    let catalog = typed_ownership_catalog();

    let coerced_sql = graph
        .lower_graph_plan_against_catalog(&coerced_plan, &catalog)
        .expect("temporal coercion should lower for string property")
        .sql()
        .to_string();
    let literal_sql = graph
        .lower_graph_plan_against_catalog(&literal_plan, &catalog)
        .expect("literal string should lower for string property")
        .sql()
        .to_string();

    assert_eq!(coerced_sql, literal_sql);
}
