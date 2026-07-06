use super::*;

#[test]
fn compiles_graphql_identity_filters_and_ordering() {
    let plan = compile_graphql(
        r#"
            query {
              Service(
                where: {
                  _id: { in: [10, 20, 40] }
                  _elementId: { notIn: ["40"] }
                }
                orderBy: [
                  { field: _id, direction: DESC }
                  { field: _elementId, direction: ASC }
                ]
              ) {
                name
              }
            }
            "#,
    )
    .expect("GraphQL identity filters and ordering should compile");

    assert!(plan.predicates.is_empty());
    assert!(matches!(
        plan.predicate,
        Some(PredicateExpression::And { .. })
    ));
    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::Key {
                    variable: "service".to_string(),
                },
                direction: OrderDirection::Descending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::ElementId {
                    variable: "service".to_string(),
                },
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn rejects_graphql_string_filters_on_raw_identity_fields() {
    let error = compile_graphql(
        r#"
            query {
              Service(where: { _id: { contains: "1" } }) {
                name
              }
            }
            "#,
    )
    .expect_err("GraphQL string filters on _id should fail");

    assert!(
        error
            .to_string()
            .contains("GraphQL _id filters do not support string predicates"),
        "{error}"
    );
}

#[test]
fn compiles_root_boolean_where_filters() {
    let plan = compile_graphql(
        r#"
            query {
              Service(
                where: {
                  or: [
                    { tier: { eq: "prod" } }
                    { risk: { gte: 0.9 } }
                  ]
                  not: { name: { contains: "legacy" } }
                }
              ) {
                name
              }
            }
            "#,
    )
    .expect("GraphQL boolean where filters should compile");

    assert!(plan.predicates.is_empty());
    assert!(matches!(
        plan.predicate,
        Some(PredicateExpression::And { .. })
    ));
}

#[test]
fn compiles_graphql_regex_and_xor_filters() {
    let plan = compile_graphql(
        r#"
            query {
              Service(
                where: {
                  xor: [
                    { name: { matches: "^billing.*" } }
                    { tier: { regex: "^dev$" } }
                  ]
                }
              ) {
                name
              }
            }
            "#,
    )
    .expect("GraphQL regex and xor filters should compile");

    assert!(plan.predicates.is_empty());
    let Some(PredicateExpression::Xor { left, right }) = plan.predicate.as_ref() else {
        panic!("expected GraphQL xor to compile as a non-conjunctive predicate");
    };
    assert!(matches!(
        left.as_ref(),
        PredicateExpression::Comparison(PropertyPredicate {
            operator: ComparisonOperator::RegexMatch,
            ..
        })
    ));
    assert!(matches!(
        right.as_ref(),
        PredicateExpression::Comparison(PropertyPredicate {
            operator: ComparisonOperator::RegexMatch,
            ..
        })
    ));
}

#[test]
fn compiles_graphql_filter_operator_aliases() {
    let plan = compile_graphql(
        r#"
            query {
              Service(
                where: {
                  tier: { equals: "prod" }
                  name: { notEquals: "legacy-sync", starts_with: "billing" }
                  risk: { greaterThanOrEqual: 0.5, lessThanOrEqual: 0.95 }
                }
              ) {
                name
              }
            }
            "#,
    )
    .expect("GraphQL filter operator aliases should compile");

    assert_eq!(plan.predicates.len(), 5);
    assert!(plan.predicates.iter().any(|predicate| {
        predicate.property.property == "tier"
            && predicate.operator == ComparisonOperator::Equal
            && predicate.rhs == temporal_rhs("prod")
    }));
    assert!(plan.predicates.iter().any(|predicate| {
        predicate.property.property == "name"
            && predicate.operator == ComparisonOperator::NotEqual
            && predicate.rhs == temporal_rhs("legacy-sync")
    }));
    assert!(plan.predicates.iter().any(|predicate| {
        predicate.property.property == "name"
            && predicate.operator == ComparisonOperator::StartsWith
            && predicate.rhs == PredicateRhs::Literal(Literal::String("billing".to_string()))
    }));
    assert!(plan.predicates.iter().any(|predicate| {
        predicate.property.property == "risk"
            && predicate.operator == ComparisonOperator::GreaterThanOrEqual
    }));
    assert!(plan.predicates.iter().any(|predicate| {
        predicate.property.property == "risk"
            && predicate.operator == ComparisonOperator::LessThanOrEqual
    }));
}

#[test]
fn compiles_graphql_shorthand_where_filters() {
    let plan = compile_graphql(
        r#"
            query {
              Service(
                where: {
                  tier: "prod"
                  risk: 0.5
                }
              ) {
                name
              }
            }
            "#,
    )
    .expect("GraphQL shorthand where filters should compile");

    assert_eq!(plan.predicates.len(), 2);
    assert!(plan.predicates.iter().any(|predicate| {
        predicate.property.property == "tier"
            && predicate.operator == ComparisonOperator::Equal
            && predicate.rhs == temporal_rhs("prod")
    }));
    assert!(plan.predicates.iter().any(|predicate| {
        predicate.property.property == "risk"
            && predicate.operator == ComparisonOperator::Equal
            && predicate.rhs == PredicateRhs::Literal(Literal::Float(OrderedFloat(0.5)))
    }));
}

#[test]
fn compiles_graphql_negated_filter_operators() {
    let plan = compile_graphql(
        r#"
            query {
              Service(
                where: {
                  tier: { isNotNull: true }
                  name: {
                    notIn: ["legacy-sync", "experiments"]
                    notContains: "legacy"
                    notRegex: "^internal"
                  }
                }
              ) {
                name
              }
            }
            "#,
    )
    .expect("GraphQL negated filter operators should compile");

    assert!(plan.predicates.is_empty());
    let expression = plan
        .predicate
        .as_ref()
        .expect("negated GraphQL filters should compile into the predicate tree");
    assert!(predicate_expression_contains_not(expression));
}

#[test]
fn graph_aware_graphql_rejects_unknown_filter_and_order_properties() {
    let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
    for (query, reason) in [
        (
            r#"
                query {
                  Service(where: { missingProperty: { eq: "x" } }) {
                    name
                  }
                }
                "#,
            "unknown filter property",
        ),
        (
            r"
                query {
                  Service(orderBy: [{ field: missingProperty, direction: ASC }]) {
                    name
                  }
                }
                ",
            "unknown orderBy property",
        ),
    ] {
        let error = compile_graphql_for_graph(&graph, query).expect_err(reason);
        assert!(
            error.to_string().contains("UNKNOWN_PROPERTY"),
            "unexpected error for {reason}: {error}"
        );
    }
}

#[test]
fn graph_aware_graphql_rejects_unknown_variable_filter_properties() {
    let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
    let variables = BTreeMap::from([(
        "filter".to_string(),
        variable_object([(
            "missingProperty",
            variable_object([(
                "eq",
                GraphqlVariableValue::Literal(Literal::String("x".to_string())),
            )]),
        )]),
    )]);
    let error = compile_graphql_for_graph_with_variables(
        &graph,
        r"
            query Services($filter: ServiceWhere!) {
              Service(where: $filter) {
                name
              }
            }
            ",
        &variables,
    )
    .expect_err("unknown object-variable filter property should fail");

    assert!(
        error.to_string().contains("UNKNOWN_PROPERTY"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_graphql_xor_with_wrong_arity() {
    for query in [
        r#"
            {
              Service(where: { xor: [{ name: { matches: "^billing" } }] }) {
                name
              }
            }
            "#,
        r#"
            {
              Service(
                where: {
                  xor: [
                    { name: { matches: "^billing" } }
                    { tier: { eq: "prod" } }
                    { risk: { gt: 0.5 } }
                  ]
                }
              ) {
                name
              }
            }
            "#,
    ] {
        let error = compile_graphql(query).expect_err("bad GraphQL xor arity should fail");

        assert!(
            error.to_string().contains("requires exactly two objects"),
            "{error}"
        );
    }
}

#[test]
fn rejects_invalid_graphql_regex_filters() {
    let error = compile_graphql(
        r#"
            {
              Service(where: { name: { matches: "[" } }) {
                name
              }
            }
            "#,
    )
    .expect_err("invalid GraphQL regex should fail");

    assert!(
        error.to_string().contains("invalid GraphQL regex literal"),
        "{error}"
    );
}

#[test]
fn rejects_non_string_graphql_regex_filters() {
    let error = compile_graphql(
        r"
            {
              Service(where: { name: { regex: 1 } }) {
                name
              }
            }
            ",
    )
    .expect_err("non-string GraphQL regex should fail");

    assert!(
        error
            .to_string()
            .contains("GraphQL regex filters require a string literal"),
        "{error}"
    );
}

#[test]
fn compiles_graphql_relationship_existence_filters() {
    let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
    let plan = compile_graphql_for_graph(
        &graph,
        r#"
            {
              Person(
                where: {
                  out_OWNS: {
                    where: { tier: { eq: "prod" } }
                    relationshipWhere: { source: { eq: "pagerduty" } }
                  }
                }
              ) {
                name
              }
            }
            "#,
    )
    .expect("GraphQL relationship existence filters should compile");

    assert_eq!(plan.nodes.len(), 1);
    assert!(plan.relationships.is_empty());
    let Some(PredicateExpression::ExistsPattern(pattern)) = plan.predicate else {
        panic!("expected relationship filter to compile as an EXISTS pattern");
    };
    assert_eq!(
        pattern.nodes,
        vec![NodePattern {
            variable: "graphql_exists_service".to_string(),
            label: "Service".to_string(),
        }]
    );
    assert_eq!(
        pattern.relationships,
        vec![RelationshipPattern {
            variable: Some("graphql_exists_relationship".to_string()),
            relationship_type: "OWNS".to_string(),
            left: "person".to_string(),
            direction: Direction::Outgoing,
            right: "graphql_exists_service".to_string(),
        }]
    );
    assert!(matches!(
        pattern.predicate.as_deref(),
        Some(PredicateExpression::And { .. })
    ));
}

#[test]
fn compiles_graphql_relationship_existence_filter_variables() {
    let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
    let variables = BTreeMap::from([(
        "ownsFilter".to_string(),
        variable_object([
            (
                "where",
                variable_object([(
                    "tier",
                    variable_object([(
                        "eq",
                        GraphqlVariableValue::Literal(Literal::String("prod".to_string())),
                    )]),
                )]),
            ),
            (
                "relationshipWhere",
                variable_object([(
                    "source",
                    variable_object([(
                        "eq",
                        GraphqlVariableValue::Literal(Literal::String("pagerduty".to_string())),
                    )]),
                )]),
            ),
        ]),
    )]);
    let plan = compile_graphql_for_graph_with_variables(
        &graph,
        r"
            query People($ownsFilter: PersonOutOWNSFilter!) {
              Person(where: { out_OWNS: $ownsFilter }) {
                name
              }
            }
            ",
        &variables,
    )
    .expect("GraphQL relationship existence filter variables should compile");

    assert!(plan.relationships.is_empty());
    assert!(matches!(
        plan.predicate,
        Some(PredicateExpression::ExistsPattern(_))
    ));
}

#[test]
fn compiles_prefix_named_graphql_where_properties_when_no_relationship_matches() {
    let graph = Declaration::from_yaml(
        r"
version: 1
name: prefix_property_test
nodes:
  - label: Service
    table: { schema: ops, name: services }
    key: id
    properties:
      name: service_name
      out_status: out_status
",
    )
    .expect("graph should parse");
    let plan = compile_graphql_for_graph(
        &graph,
        r#"
            {
              Service(where: { out_status: { eq: "green" } }) {
                name
              }
            }
            "#,
    )
    .expect("prefix-named properties should compile as property filters");

    assert!(plan.relationships.is_empty());
    assert!(matches!(
        plan.predicates.as_slice(),
        [PropertyPredicate {
            property: PropertyRef { variable, property },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::TemporalCoercion { source },
        }] if variable == "service" && property == "out_status" && source == "green"
    ));
}

#[test]
fn compiles_nested_relationship_identity_filters() {
    let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
    let plan = compile_graphql_for_graph(
        &graph,
        r#"
            query {
              Person {
                out_OWNS(
                  to: Service
                  relationshipWhere: {
                    _id: { eq: 200 }
                    _elementId: { eq: "200" }
                  }
                ) {
                  name
                }
              }
            }
            "#,
    )
    .expect("GraphQL relationship identity filters should compile");

    assert!(plan.predicates.is_empty());
    assert!(matches!(
        plan.predicate,
        Some(PredicateExpression::And { .. })
    ));
    assert!(matches!(
        plan.relationships.as_slice(),
        [RelationshipPattern {
            variable: Some(variable),
            ..
        }] if variable == "relationship0"
    ));
}

#[test]
fn compiles_nested_boolean_where_filters_with_declaration() {
    let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
    let plan = compile_graphql_for_graph(
        &graph,
        r#"
            {
              Person(where: { or: [{ team: { eq: "infra" } }, { team: { eq: "analytics" } }] }) {
                owner: name
                out_OWNS(
                  to: Service
                  where: { or: [{ tier: { eq: "prod" } }, { name: { contains: "experiments" } }] }
                  relationshipWhere: { not: { source: { isNull: true } } }
                ) {
                  service: name
                  _edge { source }
                }
              }
            }
            "#,
    )
    .expect("nested GraphQL boolean where filters should compile");

    assert!(plan.predicates.is_empty());
    assert!(matches!(
        plan.predicate,
        Some(PredicateExpression::And { .. })
    ));
    assert!(matches!(
        plan.relationships.as_slice(),
        [RelationshipPattern {
            variable: Some(variable),
            ..
        }] if variable == "relationship0"
    ));
}

#[test]
fn compiles_nested_graphql_regex_and_xor_filters_with_declaration() {
    let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
    let plan = compile_graphql_for_graph(
        &graph,
        r#"
            {
              Person(where: { name: { matches: "^Grace" } }) {
                owner: name
                out_OWNS(
                  to: Service
                  where: { name: { regex: "^(billing|deploy)" } }
                  relationshipWhere: {
                    xor: [
                      { source: { regex: "^pager" } }
                      { source: { isNull: true } }
                    ]
                  }
                ) {
                  service: name
                  _edge { source }
                }
              }
            }
            "#,
    )
    .expect("nested GraphQL regex and xor filters should compile");

    assert_eq!(plan.predicates.len(), 2);
    assert!(matches!(
        plan.predicate,
        Some(PredicateExpression::Xor { .. })
    ));
    assert!(matches!(
        plan.relationships.as_slice(),
        [RelationshipPattern {
            variable: Some(variable),
            ..
        }] if variable == "relationship0"
    ));
}

#[test]
fn rejects_empty_graphql_boolean_where_lists() {
    let error = compile_graphql(
        r"
            {
              Service(where: { or: [] }) { name }
            }
            ",
    )
    .expect_err("empty boolean filter list should fail");

    assert!(
        error.to_string().contains("require at least one object"),
        "{error:?}"
    );
}
