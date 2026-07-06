use super::*;

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
