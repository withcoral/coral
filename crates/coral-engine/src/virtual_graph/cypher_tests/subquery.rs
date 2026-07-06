use super::*;

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
