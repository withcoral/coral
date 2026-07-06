use super::*;

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
