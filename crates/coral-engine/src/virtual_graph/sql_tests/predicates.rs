use super::*;

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
