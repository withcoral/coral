use super::*;

#[test]
fn compiles_root_query_with_variables() {
    let variables = BTreeMap::from([
        (
            "tier".to_string(),
            GraphqlVariableValue::Literal(Literal::String("prod".to_string())),
        ),
        (
            "minRisk".to_string(),
            GraphqlVariableValue::Literal(Literal::Float(OrderedFloat(0.5))),
        ),
        (
            "names".to_string(),
            GraphqlVariableValue::List(vec![
                Literal::String("billing-api".to_string()),
                Literal::String("deployments".to_string()),
            ]),
        ),
        (
            "sortField".to_string(),
            GraphqlVariableValue::Literal(Literal::String("name".to_string())),
        ),
        (
            "sortDirection".to_string(),
            GraphqlVariableValue::Literal(Literal::String("DESCENDING".to_string())),
        ),
        (
            "rowLimit".to_string(),
            GraphqlVariableValue::Literal(Literal::Integer(10)),
        ),
        (
            "dedupe".to_string(),
            GraphqlVariableValue::Literal(Literal::Boolean(true)),
        ),
    ]);
    let plan = compile_graphql_with_variables(
        r"
            query Services(
              $tier: String!
              $minRisk: Float!
              $names: [String!]
              $sortField: ServiceOrderField!
              $sortDirection: SortDirection!
              $rowLimit: Int!
              $dedupe: Boolean!
            ) {
              Service(
                where: {
                  tier: { eq: $tier }
                  risk: { gte: $minRisk }
                  name: { in: $names }
                }
                orderBy: [{ field: $sortField, direction: $sortDirection }]
                limit: $rowLimit
                distinct: $dedupe
              ) {
                name
              }
            }
            ",
        &variables,
    )
    .expect("GraphQL variables should compile");

    assert_eq!(plan.predicates.len(), 3);
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
    assert!(plan.distinct);
    assert!(plan.predicates.iter().any(|predicate| {
        predicate.property.property == "name"
            && predicate.rhs == temporal_list_rhs(&["billing-api", "deployments"])
    }));
}

#[test]
fn compiles_graphql_in_lists_with_temporal_coercion_marker_guard() {
    let plan = compile_graphql(
        r#"
            query {
              Service(
                where: {
                  tier: { in: ["prod", "dev"] }
                  name: { in: ["billing-api", 1] }
                  risk: { in: [] }
                }
              ) {
                name
              }
            }
            "#,
    )
    .expect("GraphQL IN-list filters should compile");

    assert_eq!(plan.predicates.len(), 3);
    assert!(plan.predicates.iter().any(|predicate| {
        predicate.property.property == "tier"
            && predicate.rhs == temporal_list_rhs(&["prod", "dev"])
    }));
    assert!(plan.predicates.iter().any(|predicate| {
        predicate.property.property == "name"
            && predicate.rhs
                == PredicateRhs::List(vec![
                    Literal::String("billing-api".to_string()),
                    Literal::Integer(1),
                ])
    }));
    assert!(plan.predicates.iter().any(|predicate| {
        predicate.property.property == "risk"
            && matches!(&predicate.rhs, PredicateRhs::List(values) if values.is_empty())
    }));
}

#[test]
fn compiles_root_query_with_object_where_variable() {
    let variables = BTreeMap::from([(
        "filter".to_string(),
        variable_object([
            (
                "tier",
                variable_object([(
                    "eq",
                    GraphqlVariableValue::Literal(Literal::String("prod".to_string())),
                )]),
            ),
            (
                "risk",
                variable_object([(
                    "gte",
                    GraphqlVariableValue::Literal(Literal::Float(OrderedFloat(0.5))),
                )]),
            ),
            (
                "name",
                variable_object([(
                    "in",
                    GraphqlVariableValue::List(vec![
                        Literal::String("billing-api".to_string()),
                        Literal::String("deployments".to_string()),
                    ]),
                )]),
            ),
        ]),
    )]);
    let plan = compile_graphql_with_variables(
        r"
            query Services($filter: ServiceWhere!) {
              Service(where: $filter) { name }
            }
            ",
        &variables,
    )
    .expect("GraphQL object where variable should compile");

    assert_eq!(plan.predicates.len(), 3);
    assert!(plan.predicates.iter().any(|predicate| {
        predicate.property.property == "tier" && predicate.rhs == temporal_rhs("prod")
    }));
    assert!(plan.predicates.iter().any(|predicate| {
        predicate.property.property == "risk"
            && predicate.operator == ComparisonOperator::GreaterThanOrEqual
    }));
    assert!(plan.predicates.iter().any(|predicate| {
        predicate.property.property == "name"
            && predicate.rhs == temporal_list_rhs(&["billing-api", "deployments"])
    }));
}

#[test]
fn compiles_root_query_with_object_where_variable_operator_aliases() {
    let variables = BTreeMap::from([(
        "filter".to_string(),
        variable_object([
            (
                "tier",
                variable_object([(
                    "equals",
                    GraphqlVariableValue::Literal(Literal::String("prod".to_string())),
                )]),
            ),
            (
                "name",
                variable_object([(
                    "neq",
                    GraphqlVariableValue::Literal(Literal::String("legacy-sync".to_string())),
                )]),
            ),
            (
                "risk",
                variable_object([
                    (
                        "greaterThan",
                        GraphqlVariableValue::Literal(Literal::Float(OrderedFloat(0.25))),
                    ),
                    (
                        "lessThanOrEqual",
                        GraphqlVariableValue::Literal(Literal::Float(OrderedFloat(0.95))),
                    ),
                ]),
            ),
        ]),
    )]);
    let plan = compile_graphql_with_variables(
        r"
            query Services($filter: ServiceWhere!) {
              Service(where: $filter) { name }
            }
            ",
        &variables,
    )
    .expect("GraphQL object where variable operator aliases should compile");

    assert_eq!(plan.predicates.len(), 4);
    assert!(plan.predicates.iter().any(|predicate| {
        predicate.property.property == "tier" && predicate.operator == ComparisonOperator::Equal
    }));
    assert!(plan.predicates.iter().any(|predicate| {
        predicate.property.property == "name" && predicate.operator == ComparisonOperator::NotEqual
    }));
    assert!(plan.predicates.iter().any(|predicate| {
        predicate.property.property == "risk"
            && predicate.operator == ComparisonOperator::GreaterThan
    }));
    assert!(plan.predicates.iter().any(|predicate| {
        predicate.property.property == "risk"
            && predicate.operator == ComparisonOperator::LessThanOrEqual
    }));
}

#[test]
fn compiles_root_query_with_object_where_variable_negated_operators() {
    let variables = BTreeMap::from([(
        "filter".to_string(),
        variable_object([
            (
                "tier",
                variable_object([(
                    "isNotNull",
                    GraphqlVariableValue::Literal(Literal::Boolean(true)),
                )]),
            ),
            (
                "name",
                variable_object([
                    (
                        "notIn",
                        GraphqlVariableValue::List(vec![
                            Literal::String("legacy-sync".to_string()),
                            Literal::String("experiments".to_string()),
                        ]),
                    ),
                    (
                        "notStartsWith",
                        GraphqlVariableValue::Literal(Literal::String("internal".to_string())),
                    ),
                ]),
            ),
        ]),
    )]);
    let plan = compile_graphql_with_variables(
        r"
            query Services($filter: ServiceWhere!) {
              Service(where: $filter) { name }
            }
            ",
        &variables,
    )
    .expect("GraphQL object where variable negated operators should compile");

    assert!(plan.predicates.is_empty());
    let expression = plan
        .predicate
        .as_ref()
        .expect("negated GraphQL variable filters should compile into the predicate tree");
    assert!(predicate_expression_contains_not(expression));
    assert!(predicate_expression_contains_rhs(
        expression,
        "name",
        &temporal_list_rhs(&["legacy-sync", "experiments"])
    ));
}

#[test]
fn compiles_root_query_with_property_condition_variable() {
    let variables = BTreeMap::from([(
        "tierCondition".to_string(),
        variable_object([(
            "eq",
            GraphqlVariableValue::Literal(Literal::String("prod".to_string())),
        )]),
    )]);
    let plan = compile_graphql_with_variables(
        r"
            query Services($tierCondition: StringCondition!) {
              Service(where: { tier: $tierCondition }) { name }
            }
            ",
        &variables,
    )
    .expect("GraphQL property condition variable should compile");

    assert_eq!(
        plan.predicates,
        vec![PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: temporal_rhs("prod"),
        }]
    );
}

#[test]
fn compiles_root_query_with_scalar_shorthand_where_variable() {
    let variables = BTreeMap::from([(
        "tier".to_string(),
        GraphqlVariableValue::Literal(Literal::String("prod".to_string())),
    )]);
    let plan = compile_graphql_with_variables(
        r"
            query Services($tier: String!) {
              Service(where: { tier: $tier }) { name }
            }
            ",
        &variables,
    )
    .expect("GraphQL scalar shorthand where variable should compile");

    assert_eq!(
        plan.predicates,
        vec![PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: temporal_rhs("prod"),
        }]
    );
}

#[test]
fn compiles_root_query_with_object_where_variable_shorthand_values() {
    let variables = BTreeMap::from([(
        "filter".to_string(),
        variable_object([
            (
                "tier",
                GraphqlVariableValue::Literal(Literal::String("prod".to_string())),
            ),
            (
                "risk",
                GraphqlVariableValue::Literal(Literal::Float(OrderedFloat(0.5))),
            ),
        ]),
    )]);
    let plan = compile_graphql_with_variables(
        r"
            query Services($filter: ServiceWhere!) {
              Service(where: $filter) { name }
            }
            ",
        &variables,
    )
    .expect("GraphQL object where variable shorthand values should compile");

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
fn compiles_root_query_with_object_list_boolean_variable() {
    let variables = BTreeMap::from([(
        "filters".to_string(),
        GraphqlVariableValue::ObjectList(vec![
            variable_object_map([(
                "tier",
                variable_object([(
                    "eq",
                    GraphqlVariableValue::Literal(Literal::String("prod".to_string())),
                )]),
            )]),
            variable_object_map([(
                "name",
                variable_object([(
                    "contains",
                    GraphqlVariableValue::Literal(Literal::String("experiments".to_string())),
                )]),
            )]),
        ]),
    )]);
    let plan = compile_graphql_with_variables(
        r"
            query Services($filters: [ServiceWhere!]!) {
              Service(where: { or: $filters }) { name }
            }
            ",
        &variables,
    )
    .expect("GraphQL object-list boolean variable should compile");

    assert!(plan.predicates.is_empty());
    assert!(matches!(
        plan.predicate,
        Some(PredicateExpression::Or { .. })
    ));
}

#[test]
fn compiles_root_query_with_variable_defaults() {
    let plan = compile_graphql_with_variables(
        r#"
            query Services(
              $tier: String = "prod"
              $names: [String!] = ["billing-api", "deployments"]
              $sortField: ServiceOrderField = name
              $sortDirection: SortDirection = DESC
              $rowLimit: Int = 10
              $dedupe: Boolean = true
            ) {
              Service(
                where: {
                  tier: { eq: $tier }
                  name: { in: $names }
                }
                orderBy: [{ field: $sortField, direction: $sortDirection }]
                limit: $rowLimit
                distinct: $dedupe
              ) {
                name
              }
            }
            "#,
        &BTreeMap::new(),
    )
    .expect("GraphQL variable defaults should compile");

    assert_eq!(plan.predicates.len(), 2);
    assert!(plan.predicates.iter().any(|predicate| {
        predicate.property.property == "tier" && predicate.rhs == temporal_rhs("prod")
    }));
    assert!(plan.predicates.iter().any(|predicate| {
        predicate.property.property == "name"
            && predicate.rhs == temporal_list_rhs(&["billing-api", "deployments"])
    }));
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
    assert!(plan.distinct);
}

#[test]
fn runtime_graphql_variables_override_defaults() {
    let variables = BTreeMap::from([(
        "tier".to_string(),
        GraphqlVariableValue::Literal(Literal::String("dev".to_string())),
    )]);
    let plan = compile_graphql_with_variables(
        r#"
            query Services($tier: String = "prod") {
              Service(where: { tier: { eq: $tier } }) { name }
            }
            "#,
        &variables,
    )
    .expect("runtime variables should override defaults");

    assert_eq!(
        plan.predicates,
        vec![PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: temporal_rhs("dev"),
        }]
    );
}

#[test]
fn rejects_missing_graphql_variables() {
    let error = compile_graphql_with_variables(
        r"
            query Services($tier: String!) {
              Service(where: { tier: { eq: $tier } }) { name }
            }
            ",
        &BTreeMap::new(),
    )
    .expect_err("missing GraphQL variable should fail");

    assert!(
        error
            .to_string()
            .contains("GraphQL variable '$tier' was not provided"),
        "{error}"
    );
}

#[test]
fn rejects_undeclared_graphql_variables() {
    let variables = BTreeMap::from([(
        "tier".to_string(),
        GraphqlVariableValue::Literal(Literal::String("prod".to_string())),
    )]);
    let error = compile_graphql_with_variables(
        r"
            query Services {
              Service(where: { tier: { eq: $tier } }) { name }
            }
            ",
        &variables,
    )
    .expect_err("undeclared GraphQL variable should fail");

    assert!(
        error
            .to_string()
            .contains("GraphQL variable '$tier' is not declared"),
        "{error}"
    );
}

#[test]
fn rejects_graphql_variable_list_in_scalar_position() {
    let variables = BTreeMap::from([(
        "tier".to_string(),
        GraphqlVariableValue::List(vec![Literal::String("prod".to_string())]),
    )]);
    let error = compile_graphql_with_variables(
        r"
            query Services($tier: [String!]) {
              Service(where: { tier: { eq: $tier } }) { name }
            }
            ",
        &variables,
    )
    .expect_err("list variable in scalar position should fail");

    assert!(
        error
            .to_string()
            .contains("GraphQL variable '$tier' must be a scalar literal"),
        "{error}"
    );
}

#[test]
fn rejects_graphql_object_variable_in_scalar_position() {
    let variables = BTreeMap::from([(
        "tier".to_string(),
        variable_object([(
            "eq",
            GraphqlVariableValue::Literal(Literal::String("prod".to_string())),
        )]),
    )]);
    let error = compile_graphql_with_variables(
        r"
            query Services($tier: String!) {
              Service(where: { tier: { eq: $tier } }) { name }
            }
            ",
        &variables,
    )
    .expect_err("object variable in scalar position should fail");

    assert!(
        error
            .to_string()
            .contains("GraphQL variable '$tier' must be a scalar literal"),
        "{error}"
    );
}

#[test]
fn rejects_graphql_scalar_variable_in_object_position() {
    let variables = BTreeMap::from([(
        "filter".to_string(),
        GraphqlVariableValue::Literal(Literal::String("prod".to_string())),
    )]);
    let error = compile_graphql_with_variables(
        r"
            query Services($filter: ServiceWhere!) {
              Service(where: $filter) { name }
            }
            ",
        &variables,
    )
    .expect_err("scalar variable in object position should fail");

    assert!(
        error
            .to_string()
            .contains("GraphQL variable '$filter' must be an object"),
        "{error}"
    );
}

#[test]
fn rejects_graphql_list_variable_in_property_shorthand_position() {
    let variables = BTreeMap::from([(
        "condition".to_string(),
        GraphqlVariableValue::List(vec![Literal::String("prod".to_string())]),
    )]);
    let error = compile_graphql_with_variables(
        r"
            query Services($condition: [String!]!) {
              Service(where: { tier: $condition }) { name }
            }
            ",
        &variables,
    )
    .expect_err("list variable in property shorthand position should fail");

    assert!(
        error
            .to_string()
            .contains("must be a scalar literal or property condition object"),
        "{error}"
    );
}

#[test]
fn compiles_root_query_with_object_variable_defaults() {
    let plan = compile_graphql_with_variables(
        r#"
            query Services(
              $where: ServiceWhere = { tier: { eq: "prod" } }
              $order: ServiceOrder = { field: name, direction: DESC }
            ) {
              Service(where: $where, orderBy: $order) { name }
            }
            "#,
        &BTreeMap::new(),
    )
    .expect("GraphQL object variable defaults should compile");

    assert_eq!(
        plan.predicates,
        vec![PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: temporal_rhs("prod"),
        }]
    );
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
fn compiles_root_query_with_object_list_variable_defaults() {
    let plan = compile_graphql_with_variables(
        r#"
            query Services(
              $filters: [ServiceWhere!] = [
                { tier: { eq: "prod" } }
                { name: { contains: "experiments" } }
              ]
            ) {
              Service(where: { or: $filters }) { name }
            }
            "#,
        &BTreeMap::new(),
    )
    .expect("GraphQL object-list variable defaults should compile");

    assert!(plan.predicates.is_empty());
    assert!(matches!(
        plan.predicate,
        Some(PredicateExpression::Or { .. })
    ));
}

#[test]
fn rejects_graphql_mixed_object_scalar_default_lists() {
    let error = compile_graphql_with_variables(
        r#"
            query Services(
              $filters: [ServiceWhere!] = [
                { tier: { eq: "prod" } }
                "bad"
              ]
            ) {
              Service(where: { or: $filters }) { name }
            }
            "#,
        &BTreeMap::new(),
    )
    .expect_err("mixed object/scalar defaults should fail");

    assert!(
        error
            .to_string()
            .contains("cannot mix object and scalar values"),
        "{error}"
    );
}

#[test]
fn compiles_nested_relationship_query_with_object_variables() {
    let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
    let variables = BTreeMap::from([
        (
            "personFilter".to_string(),
            variable_object([(
                "team",
                variable_object([(
                    "eq",
                    GraphqlVariableValue::Literal(Literal::String("infra".to_string())),
                )]),
            )]),
        ),
        (
            "serviceFilter".to_string(),
            variable_object([(
                "tier",
                variable_object([(
                    "eq",
                    GraphqlVariableValue::Literal(Literal::String("prod".to_string())),
                )]),
            )]),
        ),
        (
            "ownershipFilter".to_string(),
            variable_object([(
                "source",
                variable_object([(
                    "eq",
                    GraphqlVariableValue::Literal(Literal::String("pagerduty".to_string())),
                )]),
            )]),
        ),
    ]);
    let plan = compile_graphql_for_graph_with_variables(
        &graph,
        r"
            query OwnedServices(
              $personFilter: PersonWhere!
              $serviceFilter: ServiceWhere!
              $ownershipFilter: OwnershipWhere!
            ) {
              Person(where: $personFilter) {
                owner: name
                out_OWNS(
                  to: Service
                  where: $serviceFilter
                  relationshipWhere: $ownershipFilter
                ) {
                  service: name
                  _edge { source }
                }
              }
            }
            ",
        &variables,
    )
    .expect("nested GraphQL object variables should compile");

    assert_eq!(plan.predicates.len(), 3);
    assert!(plan.predicates.iter().any(|predicate| {
        predicate.property.variable == "person" && predicate.property.property == "team"
    }));
    assert!(plan.predicates.iter().any(|predicate| {
        predicate.property.variable == "service1" && predicate.property.property == "tier"
    }));
    assert!(plan.predicates.iter().any(|predicate| {
        predicate.property.variable == "relationship0" && predicate.property.property == "source"
    }));
}
