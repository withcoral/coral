use super::super::graphql_schema_sdl_for_graph;
use super::*;

fn variable_object(
    entries: impl IntoIterator<Item = (&'static str, GraphqlVariableValue)>,
) -> GraphqlVariableValue {
    GraphqlVariableValue::Object(variable_object_map(entries))
}

fn variable_object_map(
    entries: impl IntoIterator<Item = (&'static str, GraphqlVariableValue)>,
) -> BTreeMap<String, GraphqlVariableValue> {
    entries
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}

fn predicate_expression_contains_not(expression: &PredicateExpression) -> bool {
    match expression {
        PredicateExpression::Not { .. } => true,
        PredicateExpression::And { left, right }
        | PredicateExpression::Or { left, right }
        | PredicateExpression::Xor { left, right } => {
            predicate_expression_contains_not(left) || predicate_expression_contains_not(right)
        }
        PredicateExpression::Boolean(_)
        | PredicateExpression::Comparison(_)
        | PredicateExpression::KeyComparison(_)
        | PredicateExpression::ElementIdComparison(_)
        | PredicateExpression::Presence(_)
        | PredicateExpression::PropertyKeyMembership(_)
        | PredicateExpression::ExistsPattern(_)
        | PredicateExpression::ScalarComparison(_) => false,
    }
}

#[test]
fn graphql_capability_surface_derives_aliases_from_engine_classifiers() {
    let surface = graphql_read_capability_surface();

    for spelling in GRAPHQL_SCALAR_FILTER_OPERATOR_SPELLINGS {
        assert!(
            classify_graphql_where_operator(spelling).is_some(),
            "scalar spelling should classify: {spelling}"
        );
    }
    for spelling in GRAPHQL_IDENTITY_FILTER_OPERATOR_SPELLINGS {
        assert!(
            classify_graphql_where_operator(spelling).is_some(),
            "identity spelling should classify: {spelling}"
        );
    }
    for spelling in GRAPHQL_BOOLEAN_OPERATOR_SPELLINGS {
        assert!(
            graphql_boolean_operator(spelling).is_some(),
            "boolean spelling should classify: {spelling}"
        );
    }

    assert_eq!(surface.scalar_operators.len(), 18);
    assert_eq!(surface.identity_operators.len(), 10);
    assert_eq!(surface.element_id_operators.len(), 18);
    assert_eq!(
        surface.aggregates.len(),
        GRAPHQL_PROPERTY_AGGREGATE_FIELDS.len() + 1
    );
    assert!(surface.aggregates.contains(&"_count"));
    assert_eq!(surface.boolean_combinators.len(), 4);
}

#[test]
fn graphql_capability_surface_lists_match_engine_behavior() {
    let surface = graphql_read_capability_surface();

    assert_eq!(
        surface.order_direction_aliases.get("ASCENDING").copied(),
        Some("ASC")
    );
    assert_eq!(
        surface.order_direction_aliases.get("DESCENDING").copied(),
        Some("DESC")
    );
    assert_eq!(
        compile_order_direction_name("ASCENDING", "test").ok(),
        Some(OrderDirection::Ascending)
    );
    assert_eq!(
        compile_order_direction_name("DESCENDING", "test").ok(),
        Some(OrderDirection::Descending)
    );
    assert_eq!(
        surface.null_order_aliases.get("NULLS_FIRST").copied(),
        Some("FIRST")
    );
    assert_eq!(
        surface.null_order_aliases.get("NULLS_LAST").copied(),
        Some("LAST")
    );
    assert_eq!(
        compile_null_order_name("NULLS_FIRST", "test").ok(),
        Some(NullOrder::First)
    );
    assert_eq!(
        compile_null_order_name("NULLS_LAST", "test").ok(),
        Some(NullOrder::Last)
    );
    assert_eq!(
        graphql_root_argument_slot("first"),
        Some(GraphqlRootArgumentSlot::Limit)
    );
    assert_eq!(
        graphql_root_argument_slot("skip"),
        Some(GraphqlRootArgumentSlot::Offset)
    );
    assert_eq!(surface.rejection_paths.len(), 15);
    assert!(surface.rejection_paths.iter().all(|path| {
        !path.id.is_empty() && !path.stable_substring.is_empty() && path.source_line > 0
    }));
}

#[test]
fn compiles_root_node_query() {
    let plan = compile_graphql(
        r#"
            query {
              Service(
                where: { tier: { eq: "prod" }, risk: { gte: 0.5 } }
                orderBy: [{ field: name, direction: ASCENDING }]
                limit: 10
                offset: 2
              ) {
                serviceName: name
                tier
              }
            }
            "#,
    )
    .expect("GraphQL query should compile");

    assert_eq!(
        plan.nodes,
        vec![NodePattern {
            variable: "service".to_string(),
            label: "Service".to_string(),
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
                alias: Some("serviceName".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                alias: Some("tier".to_string()),
            },
        ]
    );
    assert_eq!(plan.predicates.len(), 2);
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
    assert_eq!(plan.limit, Some(10));
    assert_eq!(plan.skip, Some(2));
}

#[test]
fn compiles_root_first_argument_as_limit() {
    let plan = compile_graphql(
        r"
            query {
              Service(first: 3, skip: 1) {
                name
              }
            }
            ",
    )
    .expect("GraphQL first argument should compile as a limit");

    assert_eq!(plan.limit, Some(3));
    assert_eq!(plan.skip, Some(1));
}

#[test]
fn rejects_ambiguous_graphql_root_arguments() {
    let cases = [
        (
            r"
                query {
                  Service(limit: 1, first: 2) {
                    name
                  }
                }
                ",
            "GraphQL root argument 'first' conflicts with earlier 'limit' argument",
        ),
        (
            r"
                query {
                  Service(offset: 1, skip: 2) {
                    name
                  }
                }
                ",
            "GraphQL root argument 'skip' conflicts with earlier 'offset' argument",
        ),
        (
            r#"
                query {
                  Service(
                    where: { tier: { eq: "prod" } }
                    where: { name: { eq: "billing-api" } }
                  ) {
                    name
                  }
                }
                "#,
            "GraphQL root argument 'where' is specified more than once",
        ),
        (
            r"
                query {
                  Service(
                    orderBy: [{ field: name }]
                    orderBy: [{ field: tier }]
                  ) {
                    name
                  }
                }
                ",
            "GraphQL root argument 'orderBy' is specified more than once",
        ),
        (
            r"
                query {
                  Service(distinct: true, distinct: false) {
                    name
                  }
                }
                ",
            "GraphQL root argument 'distinct' is specified more than once",
        ),
    ];

    for (query, expected) in cases {
        let error =
            compile_graphql(query).expect_err("ambiguous GraphQL root arguments should fail");
        assert!(
            error.to_string().contains(expected),
            "expected {expected:?}, got {error}"
        );
    }
}

#[test]
fn compiles_root_alias_typename_and_directives() {
    let variables = BTreeMap::from([
        (
            "withRisk".to_string(),
            GraphqlVariableValue::Literal(Literal::Boolean(true)),
        ),
        (
            "skipTier".to_string(),
            GraphqlVariableValue::Literal(Literal::Boolean(true)),
        ),
    ]);
    let plan = compile_graphql_with_variables(
        r"
            query Services($withRisk: Boolean!, $skipTier: Boolean!) {
              services: Service {
                __typename
                name
                risk @include(if: $withRisk)
                tier @skip(if: $skipTier)
              }
            }
            ",
        &variables,
    )
    .expect("GraphQL root aliases, typename, and directives should compile");

    assert_eq!(
        plan.nodes,
        vec![NodePattern {
            variable: "service".to_string(),
            label: "Service".to_string(),
        }]
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Literal {
                literal: Literal::String("Service".to_string()),
                alias: "__typename".to_string(),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("name".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                },
                alias: Some("risk".to_string()),
            },
        ]
    );
}

#[test]
fn compiles_graphql_node_identity_fields() {
    let plan = compile_graphql(
        r"
            query {
              Service {
                nodeId: _id
                element: _elementId
                name
              }
            }
            ",
    )
    .expect("GraphQL node identity fields should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Key {
                variable: "service".to_string(),
                alias: "nodeId".to_string(),
            },
            Projection::ElementId {
                variable: "service".to_string(),
                alias: "element".to_string(),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("name".to_string()),
            },
        ]
    );
}

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
fn compiles_graphql_shorthand_order_by_fields() {
    let plan = compile_graphql(
        r"
            query {
              Service(
                orderBy: [
                  { risk: DESC }
                  { name: ASCENDING }
                ]
              ) {
                name
              }
            }
            ",
    )
    .expect("GraphQL shorthand orderBy fields should compile");

    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                direction: OrderDirection::Descending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_graphql_order_by_null_placement() {
    let plan = compile_graphql(
        r"
            query {
              Service(
                orderBy: [
                  { field: tier, direction: ASC, nulls: LAST }
                  { field: name, direction: DESC, nulls: FIRST }
                ]
              ) {
                name
              }
            }
            ",
    )
    .expect("GraphQL orderBy null placement should compile");

    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                }),
                direction: OrderDirection::Ascending,
                nulls: Some(NullOrder::Last),
            },
            OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                }),
                direction: OrderDirection::Descending,
                nulls: Some(NullOrder::First),
            },
        ]
    );
}

#[test]
fn compiles_graphql_flat_aggregate_fields() {
    let plan = compile_graphql(
        r"
            query {
              Service {
                tier
                services: _count
                namedServices: _count(field: name)
                tiers: _countDistinct(field: tier)
                totalRisk: _sum(field: risk)
                averageRisk: _avg(field: risk)
                minRisk: _min(field: risk)
                maxRisk: _max(field: risk)
              }
            }
            ",
    )
    .expect("GraphQL flat aggregate fields should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                alias: Some("tier".to_string()),
            },
            Projection::CountAll {
                alias: "services".to_string(),
            },
            Projection::Aggregate {
                function: AggregateFunction::Count,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                }),
                distinct: false,
                alias: "namedServices".to_string(),
            },
            Projection::Aggregate {
                function: AggregateFunction::Count,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                }),
                distinct: true,
                alias: "tiers".to_string(),
            },
            Projection::Aggregate {
                function: AggregateFunction::Sum,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: false,
                alias: "totalRisk".to_string(),
            },
            Projection::Aggregate {
                function: AggregateFunction::Avg,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: false,
                alias: "averageRisk".to_string(),
            },
            Projection::Aggregate {
                function: AggregateFunction::Min,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: false,
                alias: "minRisk".to_string(),
            },
            Projection::Aggregate {
                function: AggregateFunction::Max,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: false,
                alias: "maxRisk".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_graphql_statistical_aggregate_fields() {
    let plan = compile_graphql(
        r"
            query {
              Service {
                sampleRisk: _stDev(field: risk)
                populationRisk: _stDevP(field: risk)
                distinctTotalRisk: _sumDistinct(field: risk)
                distinctAverageRisk: _avgDistinct(field: risk)
                medianRisk: _median(field: risk)
                distinctMedianRisk: _medianDistinct(field: risk)
                distinctMinRisk: _minDistinct(field: risk)
                distinctMaxRisk: _maxDistinct(field: risk)
              }
            }
            ",
    )
    .expect("GraphQL statistical aggregate fields should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Aggregate {
                function: AggregateFunction::StdDev,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: false,
                alias: "sampleRisk".to_string(),
            },
            Projection::Aggregate {
                function: AggregateFunction::StdDevP,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: false,
                alias: "populationRisk".to_string(),
            },
            Projection::Aggregate {
                function: AggregateFunction::Sum,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: true,
                alias: "distinctTotalRisk".to_string(),
            },
            Projection::Aggregate {
                function: AggregateFunction::Avg,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: true,
                alias: "distinctAverageRisk".to_string(),
            },
            Projection::Aggregate {
                function: AggregateFunction::Median,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: false,
                alias: "medianRisk".to_string(),
            },
            Projection::Aggregate {
                function: AggregateFunction::Median,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: true,
                alias: "distinctMedianRisk".to_string(),
            },
            Projection::Aggregate {
                function: AggregateFunction::Min,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: true,
                alias: "distinctMinRisk".to_string(),
            },
            Projection::Aggregate {
                function: AggregateFunction::Max,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: true,
                alias: "distinctMaxRisk".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_graphql_percentile_cont_with_variable_argument() {
    let variables = BTreeMap::from([(
        "percentile".to_string(),
        GraphqlVariableValue::Literal(Literal::Float(OrderedFloat(0.9))),
    )]);
    let plan = compile_graphql_with_variables(
        r"
            query Percentile($percentile: Float!) {
              Service {
                p90Risk: _percentileCont(percentile: $percentile, field: risk)
              }
            }
            ",
        &variables,
    )
    .expect("GraphQL percentile aggregate variable should compile");

    assert_eq!(
        plan.projections,
        vec![Projection::Aggregate {
            function: AggregateFunction::PercentileCont {
                percentile: OrderedFloat(0.9),
            },
            target: AggregateTarget::Property(PropertyRef {
                variable: "service".to_string(),
                property: "risk".to_string(),
            }),
            distinct: false,
            alias: "p90Risk".to_string(),
        }]
    );
}

#[test]
fn compiles_graphql_collect_aggregate_fields() {
    let plan = compile_graphql(
        r"
            query {
              Service {
                serviceNames: _collect(field: name)
                uniqueTiers: _collectDistinct(field: tier)
              }
            }
            ",
    )
    .expect("GraphQL collect aggregate fields should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Aggregate {
                function: AggregateFunction::Collect,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                }),
                distinct: false,
                alias: "serviceNames".to_string(),
            },
            Projection::Aggregate {
                function: AggregateFunction::Collect,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                }),
                distinct: true,
                alias: "uniqueTiers".to_string(),
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
            && predicate.rhs == PredicateRhs::Literal(Literal::String("prod".to_string()))
    }));
    assert!(plan.predicates.iter().any(|predicate| {
        predicate.property.property == "name"
            && predicate.operator == ComparisonOperator::NotEqual
            && predicate.rhs == PredicateRhs::Literal(Literal::String("legacy-sync".to_string()))
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
            && predicate.rhs == PredicateRhs::Literal(Literal::String("prod".to_string()))
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
fn rejects_invalid_graphql_flat_aggregate_arguments() {
    for query in [
        r"
            query {
              Service {
                _sum
              }
            }
            ",
        r"
            query {
              Service {
                _avg(property: risk)
              }
            }
            ",
        r"
            query {
              Service {
                _countDistinct {
                  value
                }
              }
            }
            ",
        r"
            query {
              Service {
                _percentileCont(field: risk)
              }
            }
            ",
        r"
            query {
              Service {
                _percentileCont(field: risk, percentile: 2.0)
              }
            }
            ",
    ] {
        let error =
            compile_graphql(query).expect_err("invalid GraphQL flat aggregate field should fail");

        assert!(
            error.to_string().contains("GraphQL aggregate")
                || error.to_string().contains("GraphQL percentile aggregate")
                || error.to_string().contains("unsupported GraphQL aggregate"),
            "{error}"
        );
    }
}

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
            && matches!(
                    &predicate.rhs,
            PredicateRhs::List(values) if values.len() == 2
                )
    }));
}

#[test]
fn compiles_named_operation_from_multi_operation_document() {
    let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
    let plan = compile_graphql_for_graph_with_operation_name(
        &graph,
        r"
            query Services {
              Service { name }
            }

            query People {
              Person { name }
            }
            ",
        "People",
    )
    .expect("named operation should compile from multi-operation document");

    assert_eq!(
        plan.nodes,
        vec![NodePattern {
            variable: "person".to_string(),
            label: "Person".to_string(),
        }]
    );
    assert!(plan.projections.iter().any(|projection| {
        matches!(
            projection,
            Projection::Property {
                property: PropertyRef { variable, property },
                alias: Some(alias),
            } if variable == "person" && property == "name" && alias == "name"
        )
    }));
}

#[test]
fn named_operation_selection_ignores_unselected_operation_variables() {
    let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
    let plan = compile_graphql_for_graph_with_variables_and_operation_name(
        &graph,
        r"
            query Services {
              Service { name }
            }

            query RequiresVariable($missing: String!) {
              Service(where: { tier: { eq: $missing } }) { name }
            }
            ",
        &BTreeMap::new(),
        "Services",
    )
    .expect("unselected operation variables should not be required");

    assert_eq!(
        plan.nodes
            .first()
            .expect("selected operation should bind a node")
            .label,
        "Service"
    );
    assert!(plan.predicates.is_empty());
}

#[test]
fn rejects_multi_operation_graphql_without_operation_name() {
    let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
    let error = compile_graphql_for_graph(
        &graph,
        r"
            query Services { Service { name } }
            query People { Person { name } }
            ",
    )
    .expect_err("multi-operation document should require operationName");

    assert!(
        error.to_string().contains("require an operationName"),
        "{error}"
    );
}

#[test]
fn rejects_missing_or_duplicate_graphql_operation_names() {
    let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
    let missing = compile_graphql_for_graph_with_operation_name(
        &graph,
        "query Services { Service { name } }",
        "People",
    )
    .expect_err("missing operation name should fail");
    assert!(
        missing
            .to_string()
            .contains("GraphQL operation 'People' was not found"),
        "{missing}"
    );

    let duplicate = compile_graphql_for_graph_with_operation_name(
        &graph,
        r"
            query Services { Service { name } }
            query Services { Service { tier } }
            ",
        "Services",
    )
    .expect_err("duplicate operation names should fail");
    assert!(
        duplicate.to_string().contains("defined more than once"),
        "{duplicate}"
    );
}

#[test]
fn rejects_selected_non_query_graphql_operation() {
    let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
    let error = compile_graphql_for_graph_with_operation_name(
        &graph,
        r"
            query Services { Service { name } }
            mutation MutateService { updateService { id } }
            ",
        "MutateService",
    )
    .expect_err("selected mutation should remain unsupported");

    assert!(
        error
            .to_string()
            .contains("GraphQL mutations and subscriptions are not supported"),
        "{error}"
    );
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
        predicate.property.property == "tier"
            && predicate.rhs == PredicateRhs::Literal(Literal::String("prod".to_string()))
    }));
    assert!(plan.predicates.iter().any(|predicate| {
        predicate.property.property == "risk"
            && predicate.operator == ComparisonOperator::GreaterThanOrEqual
    }));
    assert!(plan.predicates.iter().any(|predicate| {
        predicate.property.property == "name"
            && matches!(
                &predicate.rhs,
                PredicateRhs::List(values) if values.len() == 2
            )
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
            rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
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
            rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
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
            && predicate.rhs == PredicateRhs::Literal(Literal::String("prod".to_string()))
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
fn compiles_root_query_with_order_by_object_variable() {
    let variables = BTreeMap::from([(
        "order".to_string(),
        variable_object([
            (
                "field",
                GraphqlVariableValue::Literal(Literal::String("name".to_string())),
            ),
            (
                "direction",
                GraphqlVariableValue::Literal(Literal::String("DESC".to_string())),
            ),
        ]),
    )]);
    let plan = compile_graphql_with_variables(
        r"
            query Services($order: ServiceOrder!) {
              Service(orderBy: $order) { name }
            }
            ",
        &variables,
    )
    .expect("GraphQL orderBy object variable should compile");

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
fn compiles_root_query_with_order_by_null_placement_variable() {
    let variables = BTreeMap::from([(
        "order".to_string(),
        variable_object([
            (
                "field",
                GraphqlVariableValue::Literal(Literal::String("tier".to_string())),
            ),
            (
                "direction",
                GraphqlVariableValue::Literal(Literal::String("ASC".to_string())),
            ),
            (
                "nulls",
                GraphqlVariableValue::Literal(Literal::String("NULLS_LAST".to_string())),
            ),
        ]),
    )]);
    let plan = compile_graphql_with_variables(
        r"
            query Services($order: ServiceOrder!) {
              Service(orderBy: $order) { name }
            }
            ",
        &variables,
    )
    .expect("GraphQL orderBy null placement variable should compile");

    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            }),
            direction: OrderDirection::Ascending,
            nulls: Some(NullOrder::Last),
        }]
    );
}

#[test]
fn compiles_root_query_with_shorthand_order_by_object_variable() {
    let variables = BTreeMap::from([(
        "order".to_string(),
        variable_object([(
            "name",
            GraphqlVariableValue::Literal(Literal::String("DESC".to_string())),
        )]),
    )]);
    let plan = compile_graphql_with_variables(
        r"
            query Services($order: ServiceOrder!) {
              Service(orderBy: $order) { name }
            }
            ",
        &variables,
    )
    .expect("GraphQL shorthand orderBy object variable should compile");

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
fn compiles_root_query_with_order_by_object_list_variable() {
    let variables = BTreeMap::from([(
        "orders".to_string(),
        GraphqlVariableValue::ObjectList(vec![
            variable_object_map([
                (
                    "field",
                    GraphqlVariableValue::Literal(Literal::String("tier".to_string())),
                ),
                (
                    "direction",
                    GraphqlVariableValue::Literal(Literal::String("ASC".to_string())),
                ),
            ]),
            variable_object_map([
                (
                    "field",
                    GraphqlVariableValue::Literal(Literal::String("name".to_string())),
                ),
                (
                    "direction",
                    GraphqlVariableValue::Literal(Literal::String("DESC".to_string())),
                ),
            ]),
        ]),
    )]);
    let plan = compile_graphql_with_variables(
        r"
            query Services($orders: [ServiceOrder!]!) {
              Service(orderBy: $orders) { name }
            }
            ",
        &variables,
    )
    .expect("GraphQL orderBy object-list variable should compile");

    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                }),
                direction: OrderDirection::Descending,
                nulls: None,
            },
        ]
    );
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
        predicate.property.property == "tier"
            && predicate.rhs == PredicateRhs::Literal(Literal::String("prod".to_string()))
    }));
    assert!(plan.predicates.iter().any(|predicate| {
        predicate.property.property == "name"
            && matches!(
                &predicate.rhs,
                PredicateRhs::List(values)
                    if values
                        == &vec![
                            Literal::String("billing-api".to_string()),
                            Literal::String("deployments".to_string()),
                        ]
            )
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
            rhs: PredicateRhs::Literal(Literal::String("dev".to_string())),
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
fn rejects_graphql_scalar_variable_in_order_by_position() {
    let variables = BTreeMap::from([(
        "order".to_string(),
        GraphqlVariableValue::Literal(Literal::String("name".to_string())),
    )]);
    let error = compile_graphql_with_variables(
        r"
            query Services($order: ServiceOrder!) {
              Service(orderBy: $order) { name }
            }
            ",
        &variables,
    )
    .expect_err("scalar variable in orderBy position should fail");

    assert!(
        error
            .to_string()
            .contains("must be an orderBy object or list of objects"),
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
            rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
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
fn compiles_empty_order_by_list_variable_default_as_no_order_keys() {
    let plan = compile_graphql_with_variables(
        r"
            query Services($orders: [ServiceOrder!] = []) {
              Service(orderBy: $orders) { name }
            }
            ",
        &BTreeMap::new(),
    )
    .expect("empty GraphQL orderBy defaults should compile as no-op ordering");

    assert!(plan.order_by.is_empty());
}

#[test]
fn compiles_empty_order_by_list_variable_as_no_order_keys() {
    let variables =
        BTreeMap::from([("orders".to_string(), GraphqlVariableValue::List(Vec::new()))]);
    let plan = compile_graphql_with_variables(
        r"
            query Services($orders: [ServiceOrder!]!) {
              Service(orderBy: $orders) { name }
            }
            ",
        &variables,
    )
    .expect("empty GraphQL orderBy variables should compile as no-op ordering");

    assert!(plan.order_by.is_empty());
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
fn compiles_graphql_named_and_inline_fragments() {
    let plan = compile_graphql(
        r"
            query {
              Service {
                __typename
                ...ServiceFields
                ... on Service {
                  __typename
                  serviceTier: tier
                }
              }
            }

            fragment ServiceFields on Service {
              __typename
              serviceName: name
            }
            ",
    )
    .expect("GraphQL named and inline fragments should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Literal {
                literal: Literal::String("Service".to_string()),
                alias: "__typename".to_string(),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("serviceName".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                alias: Some("serviceTier".to_string()),
            },
        ]
    );
}

#[test]
fn compiles_query_operation_include_skip_directives() {
    let included_variables = BTreeMap::from([(
        "runQuery".to_string(),
        GraphqlVariableValue::Literal(Literal::Boolean(true)),
    )]);
    let included = compile_graphql_with_variables(
        r"
            query Services($runQuery: Boolean!) @include(if: $runQuery) {
              Service {
                name
              }
            }
            ",
        &included_variables,
    )
    .expect("included GraphQL query operation directive should compile");

    assert_eq!(included.predicate, None);
    assert_eq!(
        included.projections,
        vec![Projection::Property {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            },
            alias: Some("name".to_string()),
        }]
    );

    let skipped_variables = BTreeMap::from([(
        "skipQuery".to_string(),
        GraphqlVariableValue::Literal(Literal::Boolean(true)),
    )]);
    let skipped = compile_graphql_with_variables(
        r"
            query Services($skipQuery: Boolean!) @skip(if: $skipQuery) {
              Service {
                name
              }
            }
            ",
        &skipped_variables,
    )
    .expect("skipped GraphQL query operation directive should compile");

    assert_eq!(skipped.predicate, Some(PredicateExpression::Boolean(false)));
    assert_eq!(skipped.projections, included.projections);
}

#[test]
fn compiles_graphql_root_typename_metadata() {
    let plan = compile_graphql(
        r"
            query {
              queryType: __typename
              Service {
                service: name
              }
            }
            ",
    )
    .expect("GraphQL root __typename metadata should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("service".to_string()),
            },
            Projection::Literal {
                literal: Literal::String("Query".to_string()),
                alias: "queryType".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_graphql_fragment_definition_directives() {
    let variables = BTreeMap::from([
        (
            "includeFields".to_string(),
            GraphqlVariableValue::Literal(Literal::Boolean(true)),
        ),
        (
            "skipRisk".to_string(),
            GraphqlVariableValue::Literal(Literal::Boolean(true)),
        ),
    ]);
    let plan = compile_graphql_with_variables(
        r"
            query Services($includeFields: Boolean!, $skipRisk: Boolean!) {
              Service {
                ...ServiceFields
                ...RiskFields
              }
            }

            fragment ServiceFields on Service @include(if: $includeFields) {
              serviceName: name
            }

            fragment RiskFields on Service @skip(if: $skipRisk) {
              risk
            }
            ",
        &variables,
    )
    .expect("GraphQL fragment definition directives should compile");

    assert_eq!(
        plan.projections,
        vec![Projection::Property {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            },
            alias: Some("serviceName".to_string()),
        }]
    );
}

#[test]
fn merges_duplicate_graphql_relationship_fields_across_fragments() {
    let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
    let plan = compile_graphql_for_graph(
        &graph,
        r"
            query {
              Person {
                owner: name
                out_OWNS(to: Service) {
                  service: name
                  ...OwnedServiceFields
                }
                ...PersonRelationshipFields
              }
            }

            fragment PersonRelationshipFields on Person {
              out_OWNS(to: Service) {
                tier
              }
            }

            fragment OwnedServiceFields on Service {
              service: name
              risk
            }
            ",
    )
    .expect("duplicate GraphQL relationship fields should merge");

    assert_eq!(plan.nodes.len(), 2, "{plan:?}");
    assert_eq!(plan.relationships.len(), 1, "{plan:?}");
    let service_variable = plan
        .relationships
        .first()
        .expect("relationship should exist")
        .right
        .clone();
    assert_eq!(
        plan.projections
            .iter()
            .filter(|projection| {
                matches!(
                    projection,
                    Projection::Property {
                        property: PropertyRef { variable, property },
                        alias: Some(alias),
                    } if variable == &service_variable
                        && property == "name"
                        && alias == "service"
                )
            })
            .count(),
        1,
        "{:?}",
        plan.projections
    );
    for property in ["tier", "risk"] {
        assert!(
            plan.projections.iter().any(|projection| {
                matches!(
                    projection,
                    Projection::Property {
                        property: PropertyRef { variable, property: projected_property },
                        ..
                    } if variable == &service_variable && projected_property == property
                )
            }),
            "missing merged service property {property}: {:?}",
            plan.projections
        );
    }
}

#[test]
fn rejects_conflicting_duplicate_graphql_relationship_fields() {
    let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
    let error = compile_graphql_for_graph(
        &graph,
        r#"
            query {
              Person {
                out_OWNS(to: Service, where: { tier: { eq: "prod" } }) {
                  name
                }
                out_OWNS(to: Service, where: { tier: { eq: "dev" } }) {
                  name
                }
              }
            }
            "#,
    )
    .expect_err("conflicting duplicate GraphQL relationship fields should fail");

    assert!(
        error
            .to_string()
            .contains("relationship response field 'out_OWNS' selects conflicting traversals"),
        "{error}"
    );
}

#[test]
fn compiles_graphql_root_fragments() {
    let variables = BTreeMap::from([(
        "includeService".to_string(),
        GraphqlVariableValue::Literal(Literal::Boolean(true)),
    )]);
    let plan = compile_graphql_with_variables(
        r"
            query Services($includeService: Boolean!) {
              ...RootServices
              ... on Query {
                skipped: Team @skip(if: true) {
                  name
                }
              }
            }

            fragment RootServices on Query @include(if: $includeService) {
              services: Service(
                orderBy: [{ field: name, direction: ASC }]
                limit: 2
              ) {
                service: name
                tier
              }
            }
            ",
        &variables,
    )
    .expect("GraphQL root fragments should compile");

    assert_eq!(
        plan.nodes,
        vec![NodePattern {
            variable: "service".to_string(),
            label: "Service".to_string(),
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
                alias: Some("service".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                alias: Some("tier".to_string()),
            },
        ]
    );
    assert_eq!(plan.limit, Some(2));
}

#[test]
fn merges_duplicate_graphql_root_fields_across_fragments() {
    let plan = compile_graphql(
        r#"
            query {
              Service(where: { tier: { eq: "prod" } }) {
                service: name
              }
              ...ServiceRootDetails
            }

            fragment ServiceRootDetails on Query {
              Service(where: { tier: { eq: "prod" } }) {
                tier
              }
            }
            "#,
    )
    .expect("duplicate GraphQL root fields should merge");

    assert_eq!(
        plan.nodes,
        vec![NodePattern {
            variable: "service".to_string(),
            label: "Service".to_string(),
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
                alias: Some("service".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                alias: Some("tier".to_string()),
            },
        ]
    );
}

#[test]
fn rejects_conflicting_duplicate_graphql_root_fields() {
    let error = compile_graphql(
        r#"
            query {
              Service(where: { tier: { eq: "prod" } }) {
                name
              }
              Service(where: { tier: { eq: "dev" } }) {
                name
              }
            }
            "#,
    )
    .expect_err("conflicting duplicate GraphQL root fields should fail");

    assert!(
        error
            .to_string()
            .contains("root response field 'Service' selects conflicting root fields"),
        "{error}"
    );
}

#[test]
fn compiles_declaration_aware_root_field_aliases() {
    let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
    let plan = compile_graphql_for_graph(
        &graph,
        r"
            query {
              services {
                __typename
                service: name
              }
            }
            ",
    )
    .expect("GraphQL declaration-aware root field alias should compile");

    assert_eq!(
        plan.nodes,
        vec![NodePattern {
            variable: "service".to_string(),
            label: "Service".to_string(),
        }]
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Literal {
                literal: Literal::String("Service".to_string()),
                alias: "__typename".to_string(),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("service".to_string()),
            },
        ]
    );
}

#[test]
fn rejects_ambiguous_declaration_aware_root_field_aliases() {
    let graph = Declaration::from_yaml(
        r"
version: 1
name: ambiguous_roots
nodes:
  - label: User
    table: { schema: ops, name: users }
    key: id
    properties:
      name: name
  - label: user
    table: { schema: ops, name: lower_users }
    key: id
    properties:
      name: name
",
    )
    .expect("graph should parse");

    let error = compile_graphql_for_graph(
        &graph,
        r"
            query {
              users {
                name
              }
            }
            ",
    )
    .expect_err("ambiguous declaration-aware root field alias should fail");

    assert!(
        error
            .to_string()
            .contains("GraphQL root field 'users' is ambiguous"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_unknown_declaration_aware_root_fields() {
    let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
    let error = compile_graphql_for_graph(
        &graph,
        r"
            query {
              Incident {
                name
              }
            }
            ",
    )
    .expect_err("unknown graph-backed root field should fail");

    assert!(
        error
            .to_string()
            .contains("unknown GraphQL root node field 'Incident'"),
        "unexpected error: {error}"
    );
}

#[test]
fn graph_aware_graphql_rejects_unknown_selected_properties() {
    let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
    let error = compile_graphql_for_graph(
        &graph,
        r"
            query {
              Service {
                missingProperty
              }
            }
            ",
    )
    .expect_err("unknown graph-backed selected property should fail");

    assert!(
        error.to_string().contains("UNKNOWN_PROPERTY"),
        "unexpected error: {error}"
    );
}

#[test]
fn declaration_free_graphql_keeps_open_property_selection() {
    let plan = compile_graphql(
        r"
            query {
              Service {
                missingProperty
              }
            }
            ",
    )
    .expect("declaration-free GraphQL should keep open property names");

    assert_eq!(
        plan.projections,
        vec![Projection::Property {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "missingProperty".to_string(),
            },
            alias: Some("missingProperty".to_string()),
        }]
    );
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
fn graph_aware_graphql_validates_only_selected_operation_properties() {
    let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
    compile_graphql_for_graph_with_operation_name(
        &graph,
        r"
            query Good {
              Service {
                name
              }
            }

            query Bad {
              Service {
                missingProperty
              }
            }
            ",
        "Good",
    )
    .expect("unselected invalid operation should not be validated");

    let error = compile_graphql_for_graph_with_operation_name(
        &graph,
        r"
            query Good {
              Service {
                name
              }
            }

            query Bad {
              Service {
                missingProperty
              }
            }
            ",
        "Bad",
    )
    .expect_err("selected invalid operation should fail validation");

    assert!(
        error.to_string().contains("UNKNOWN_PROPERTY"),
        "unexpected error: {error}"
    );
}

#[test]
fn declaration_free_graphql_keeps_unknown_root_labels() {
    let plan = compile_graphql(
        r"
            query {
              Incident {
                title
              }
            }
            ",
    )
    .expect("declaration-free GraphQL should keep root labels open");

    assert_eq!(
        plan.nodes,
        vec![NodePattern {
            variable: "incident".to_string(),
            label: "Incident".to_string(),
        }]
    );
}

#[test]
fn compiles_graphql_edge_fields_inside_fragments() {
    let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
    let plan = compile_graphql_for_graph(
        &graph,
        r"
            {
              Person {
                owner: name
                out_OWNS(to: Service) {
                  service: name
                  ...OwnershipEdge
                }
              }
            }

            fragment OwnershipEdge on Service @include(if: true) {
              _edge { source }
            }
            ",
    )
    .expect("GraphQL _edge inside fragments should compile");

    assert!(matches!(
        plan.relationships.as_slice(),
        [RelationshipPattern {
            variable: Some(variable),
            ..
        }] if variable == "relationship0"
    ));
    assert!(plan.projections.iter().any(|projection| {
        matches!(
            projection,
            Projection::Property {
                property: PropertyRef { variable, property },
                alias: Some(alias),
            } if variable == "relationship0"
                && property == "source"
                && alias == "relationship0_source"
        )
    }));
}

#[test]
fn skipped_graphql_fragment_definition_directives_do_not_require_edge_variables() {
    let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
    let plan = compile_graphql_for_graph(
        &graph,
        r"
            {
              Person {
                out_OWNS(to: Service) {
                  service: name
                  ...SkippedEdgeFields
                }
              }
            }

            fragment SkippedEdgeFields on Service @skip(if: true) {
              _edge { source }
            }
            ",
    )
    .expect("skipped GraphQL fragment definition directives should compile");

    assert!(matches!(
        plan.relationships.as_slice(),
        [RelationshipPattern { variable: None, .. }]
    ));
    assert!(plan.projections.iter().all(|projection| {
        !matches!(
            projection,
            Projection::Property {
                property: PropertyRef { variable, property },
                ..
            } if variable == "relationship0" && property == "source"
        )
    }));
}

#[test]
fn compiles_graphql_edge_identity_fields() {
    let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
    let plan = compile_graphql_for_graph(
        &graph,
        r"
            {
              Person {
                out_OWNS(to: Service) {
                  name
                  _edge {
                    edgeId: _id
                    edgeElement: _elementId
                  }
                }
              }
            }
            ",
    )
    .expect("GraphQL edge identity fields should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service1".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("service1_name".to_string()),
            },
            Projection::Key {
                variable: "relationship0".to_string(),
                alias: "edgeId".to_string(),
            },
            Projection::ElementId {
                variable: "relationship0".to_string(),
                alias: "edgeElement".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_graphql_fragments_inside_edge_selections() {
    let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
    let plan = compile_graphql_for_graph(
        &graph,
        r"
            {
              Person {
                owner: name
                out_OWNS(to: Service) {
                  service: name
                  _edge {
                    ...OwnershipEdge
                    ... on OWNS {
                      ownershipSourceInline: source
                    }
                  }
                }
              }
            }

            fragment OwnershipEdge on OWNS {
              edgeKind: __typename
              ownershipSource: source
            }
            ",
    )
    .expect("GraphQL fragments inside _edge should compile");

    assert!(matches!(
        plan.relationships.as_slice(),
        [RelationshipPattern {
            variable: Some(variable),
            ..
        }] if variable == "relationship0"
    ));
    assert!(plan.projections.iter().any(|projection| {
        matches!(
            projection,
            Projection::Literal {
                literal: Literal::String(kind),
                alias,
            } if kind == "OWNS" && alias == "edgeKind"
        )
    }));
    for alias in ["ownershipSource", "ownershipSourceInline"] {
        assert!(
            plan.projections.iter().any(|projection| {
                matches!(
                    projection,
                    Projection::Property {
                        property: PropertyRef { variable, .. },
                        alias: Some(projected_alias),
                    } if variable == "relationship0" && projected_alias == alias
                )
            }),
            "missing edge projection alias {alias}: {:?}",
            plan.projections
        );
    }
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
fn rejects_conflicting_graphql_response_aliases() {
    let error = compile_graphql(
        r"
            {
              Service {
                value: name
                value: tier
              }
            }
            ",
    )
    .expect_err("conflicting GraphQL response aliases should fail");

    assert!(
        error
            .to_string()
            .contains("response alias 'value' selects conflicting fields"),
        "{error}"
    );
}

#[test]
fn rejects_graphql_fragment_cycles() {
    let error = compile_graphql(
        r"
            query {
              Service { ...A }
            }

            fragment A on Service { ...B }
            fragment B on Service { ...A }
            ",
    )
    .expect_err("fragment cycles should fail");

    assert!(error.to_string().contains("forms a cycle"), "{error}");
}

#[test]
fn rejects_graphql_fragment_type_mismatches() {
    let error = compile_graphql(
        r"
            query {
              Service { ...PersonFields }
            }

            fragment PersonFields on Person { name }
            ",
    )
    .expect_err("fragment type mismatches should fail");

    assert!(
        error
            .to_string()
            .contains("must match graph label 'Service'"),
        "{error}"
    );
}

#[test]
fn rejects_graphql_root_fragment_type_mismatches() {
    let error = compile_graphql(
        r"
            query {
              ...NotQuery
            }

            fragment NotQuery on Service {
              Service { name }
            }
            ",
    )
    .expect_err("root fragment type mismatches should fail");

    assert!(error.to_string().contains("must be Query"), "{error}");
}

#[test]
fn rejects_conflicting_graphql_root_typename_alias() {
    let error = compile_graphql(
        r"
            query {
              __typename
              Service {
                __typename
                name
              }
            }
            ",
    )
    .expect_err("root and node __typename cannot share one flattened alias");

    assert!(
        error
            .to_string()
            .contains("response alias '__typename' selects conflicting fields"),
        "{error}"
    );
}

#[test]
fn rejects_unknown_graphql_directives() {
    let error = compile_graphql(
        r"
            {
              Service { name @defer }
            }
            ",
    )
    .expect_err("unknown GraphQL directives should fail");

    assert!(
        error
            .to_string()
            .contains("unsupported GraphQL directive '@defer'"),
        "{error}"
    );
}

#[test]
fn rejects_unknown_graphql_fragment_definition_directives() {
    let error = compile_graphql(
        r"
            {
              Service { ...ServiceFields }
            }

            fragment ServiceFields on Service @defer {
              name
            }
            ",
    )
    .expect_err("unknown GraphQL fragment definition directives should fail");

    assert!(
        error
            .to_string()
            .contains("unsupported GraphQL directive '@defer'"),
        "{error}"
    );
}

#[test]
fn rejects_invalid_graphql_directives() {
    for (query, message) in [
        (
            r"
                {
                  Service { name @include(unless: true) }
                }
                ",
            "requires an 'if' argument",
        ),
        (
            r"
                {
                  Service { name @include(if: true) @include(if: false) }
                }
                ",
            "directive '@include' is repeated",
        ),
        (
            r"
                query Services @include(if: true) @include(if: false) {
                  Service { name }
                }
                ",
            "directive '@include' is repeated",
        ),
        (
            r"
                query Services($includeName: String!) {
                  Service { name @include(if: $includeName) }
                }
                ",
            "must be a boolean",
        ),
    ] {
        let variables = BTreeMap::from([(
            "includeName".to_string(),
            GraphqlVariableValue::Literal(Literal::String("yes".to_string())),
        )]);
        let error = compile_graphql_with_variables(query, &variables)
            .expect_err("invalid GraphQL directive should fail");

        assert!(error.to_string().contains(message), "{error}");
    }
}

#[test]
fn rejects_invalid_graphql_fragment_definition_directives() {
    for (query, message) in [
        (
            r"
                {
                  Service { ...ServiceFields }
                }

                fragment ServiceFields on Service @include(unless: true) {
                  name
                }
                ",
            "requires an 'if' argument",
        ),
        (
            r"
                {
                  Service { ...ServiceFields }
                }

                fragment ServiceFields on Service @include(if: true) @include(if: false) {
                  name
                }
                ",
            "directive '@include' is repeated",
        ),
    ] {
        let error =
            compile_graphql(query).expect_err("invalid fragment definition directive should fail");

        assert!(error.to_string().contains(message), "{error}");
    }
}

#[test]
fn compiles_nested_outgoing_relationship_query_with_declaration() {
    let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
    let plan = compile_graphql_for_graph(
        &graph,
        r#"
            {
              Person(where: { team: { eq: "infra" } }) {
                owner: name
                out_OWNS(
                  to: Service
                  relationshipWhere: { source: { eq: "pagerduty" } }
                  where: { tier: { eq: "prod" } }
                ) {
                  service: name
                  risk
                  _edge {
                    ownershipKind: __typename
                    ownershipSource: source
                  }
                }
              }
            }
            "#,
    )
    .expect("nested GraphQL query should compile");

    assert_eq!(
        plan.nodes,
        vec![
            NodePattern {
                variable: "person".to_string(),
                label: "Person".to_string(),
            },
            NodePattern {
                variable: "service1".to_string(),
                label: "Service".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.relationships,
        vec![RelationshipPattern {
            variable: Some("relationship0".to_string()),
            relationship_type: "OWNS".to_string(),
            left: "person".to_string(),
            direction: Direction::Outgoing,
            right: "service1".to_string(),
        }]
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "person".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("owner".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "service1".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("service".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "service1".to_string(),
                    property: "risk".to_string(),
                },
                alias: Some("service1_risk".to_string()),
            },
            Projection::Literal {
                literal: Literal::String("OWNS".to_string()),
                alias: "ownershipKind".to_string(),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "relationship0".to_string(),
                    property: "source".to_string(),
                },
                alias: Some("ownershipSource".to_string()),
            },
        ]
    );
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

#[test]
fn compiles_nested_relationship_query_with_inferred_endpoint_label() {
    let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
    let plan = compile_graphql_for_graph(
        &graph,
        r#"
            {
              Person {
                owner: name
                out_OWNS(where: { tier: { eq: "prod" } }) {
                  service: name
                }
              }
            }
            "#,
    )
    .expect("unambiguous GraphQL relationship endpoint labels should infer from declaration");

    assert_eq!(
        plan.nodes,
        vec![
            NodePattern {
                variable: "person".to_string(),
                label: "Person".to_string(),
            },
            NodePattern {
                variable: "service1".to_string(),
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
            right: "service1".to_string(),
        }]
    );
    assert!(plan.predicates.iter().any(|predicate| {
        predicate.property.variable == "service1" && predicate.property.property == "tier"
    }));
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
            rhs: PredicateRhs::Literal(Literal::String(value)),
        }] if variable == "service" && property == "out_status" && value == "green"
    ));
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
fn compiles_nested_incoming_relationship_query_with_declaration() {
    let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
    let plan = compile_graphql_for_graph(
        &graph,
        r"
            {
              Service {
                service: name
                owners: in_OWNS(from: Person) {
                  owner: name
                  team
                  _edge {
                    source
                  }
                }
              }
            }
            ",
    )
    .expect("incoming nested GraphQL query should compile");

    assert_eq!(
        plan.relationships,
        vec![RelationshipPattern {
            variable: Some("owners_edge".to_string()),
            relationship_type: "OWNS".to_string(),
            left: "service".to_string(),
            direction: Direction::Incoming,
            right: "owners".to_string(),
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
                alias: Some("service".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "owners".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("owner".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "owners".to_string(),
                    property: "team".to_string(),
                },
                alias: Some("owners_team".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "owners_edge".to_string(),
                    property: "source".to_string(),
                },
                alias: Some("owners_edge_source".to_string()),
            },
        ]
    );
}

#[test]
fn rejects_nested_graphql_selection() {
    let error = compile_graphql(
        r"
            {
              Service {
                name
                out_DEPENDS_ON { name }
              }
            }
            ",
    )
    .expect_err("nested selections should be rejected for first GraphQL slice");

    assert!(
        error.to_string().contains("relationship nesting"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_unknown_graphql_order_by_keys() {
    let error = compile_graphql(
        r"
            {
              Service(orderBy: { field: name, direction: ASC, collation: CASE_INSENSITIVE }) {
                name
              }
            }
            ",
    )
    .expect_err("unknown orderBy keys should be rejected");

    assert!(
        error
            .to_string()
            .contains("unsupported GraphQL orderBy key"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_unknown_graphql_order_by_null_placement() {
    let error = compile_graphql(
        r"
            {
              Service(orderBy: { field: name, direction: ASC, nulls: MIDDLE }) {
                name
              }
            }
            ",
    )
    .expect_err("unknown orderBy null placement should be rejected");

    assert!(
        error
            .to_string()
            .contains("GraphQL orderBy nulls must be FIRST, LAST"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_multi_field_graphql_shorthand_order_by_objects() {
    let error = compile_graphql(
        r"
            {
              Service(orderBy: { risk: DESC, name: ASC }) {
                name
              }
            }
            ",
    )
    .expect_err("multi-field shorthand orderBy object should fail");

    assert!(
        error
            .to_string()
            .contains("shorthand orderBy entries must contain exactly one field"),
        "unexpected error: {error}"
    );
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

#[test]
fn rejects_duplicate_nested_graphql_relationship_arguments() {
    let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
    let cases = [
        (
            r"
                {
                  Person {
                    out_OWNS(
                      to: Service
                      to: Person
                    ) {
                      name
                    }
                  }
                }
                ",
            "GraphQL relationship argument 'to' is specified more than once",
        ),
        (
            r#"
                {
                  Person {
                    out_OWNS(
                      where: { tier: { eq: "prod" } }
                      where: { name: { eq: "billing-api" } }
                    ) {
                      name
                    }
                  }
                }
                "#,
            "GraphQL relationship argument 'where' is specified more than once",
        ),
        (
            r#"
                {
                  Person {
                    out_OWNS(
                      relationshipWhere: { source: { eq: "pagerduty" } }
                      relationshipWhere: { source: { eq: "catalog" } }
                    ) {
                      name
                    }
                  }
                }
                "#,
            "GraphQL relationship argument 'relationshipWhere' is specified more than once",
        ),
    ];

    for (query, expected) in cases {
        let error = compile_graphql_for_graph(&graph, query)
            .expect_err("duplicate GraphQL relationship arguments should fail");
        assert!(
            error.to_string().contains(expected),
            "expected {expected:?}, got {error}"
        );
    }
}

#[test]
fn rejects_wrong_nested_graphql_relationship_endpoint_argument_before_inference() {
    let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
    let error = compile_graphql_for_graph(
        &graph,
        r"
            {
              Person {
                out_OWNS(from: Person) {
                  name
                }
              }
            }
            ",
    )
    .expect_err("wrong GraphQL relationship endpoint argument should fail");

    assert!(
        error
            .to_string()
            .contains("GraphQL relationship field 'out_OWNS' requires 'to' instead of 'from'"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_nested_graphql_relationship_endpoint_mismatches() {
    let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
    let error = compile_graphql_for_graph(
        &graph,
        r"
            {
              Person {
                out_OWNS(to: Person) { name }
              }
            }
            ",
    )
    .expect_err("endpoint mismatch should be rejected");

    assert!(
        error.to_string().contains("has no mapping"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_ambiguous_inferred_nested_graphql_relationship_endpoints() {
    let graph = Declaration::from_yaml(
        r"
version: 1
name: ambiguous_relationship_endpoint
nodes:
  - label: Person
    table: { schema: ops, name: people }
    key: id
  - label: Service
    table: { schema: ops, name: services }
    key: id
  - label: Team
    table: { schema: ops, name: teams }
    key: id
relationships:
  - type: OWNS
    table: { schema: ops, name: person_service_ownerships }
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
  - type: OWNS
    table: { schema: ops, name: person_team_ownerships }
    from: { label: Person, key: person_id }
    to: { label: Team, key: team_id }
",
    )
    .expect("graph should parse");
    let error = compile_graphql_for_graph(
        &graph,
        r"
            {
              Person {
                out_OWNS { _id }
              }
            }
            ",
    )
    .expect_err("ambiguous inferred endpoint should be rejected");

    assert!(
        error
            .to_string()
            .contains("maps graph label 'Person' to multiple endpoint labels"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_unknown_inferred_nested_graphql_relationship_types() {
    let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
    let error = compile_graphql_for_graph(
        &graph,
        r"
            {
              Person {
                out_MANAGES { _id }
              }
            }
            ",
    )
    .expect_err("unknown inferred relationship type should be rejected");

    assert!(
        error
            .to_string()
            .contains("unknown GraphQL relationship type 'MANAGES'"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_nested_graphql_relationship_row_modifiers() {
    let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
    let error = compile_graphql_for_graph(
        &graph,
        r"
            {
              Person {
                out_OWNS(to: Service, first: 2) { name }
              }
            }
            ",
    )
    .expect_err("nested GraphQL relationship first argument should be rejected");

    assert!(
        error
            .to_string()
            .contains("nested relationship fields do not support row modifiers"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_graphql_edge_fragment_type_mismatches() {
    let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
    let error = compile_graphql_for_graph(
        &graph,
        r"
            {
              Person {
                out_OWNS(to: Service) {
                  service: name
                  _edge { ...DependencyEdge }
                }
              }
            }

            fragment DependencyEdge on DEPENDS_ON {
              source
            }
            ",
    )
    .expect_err("edge fragment type mismatch should be rejected");

    assert!(
        error.to_string().contains("edge fragment type condition"),
        "unexpected error: {error}"
    );
}

#[test]
fn generates_graphql_schema_sdl_for_declaration() {
    let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");

    let sdl = graphql_schema_sdl_for_graph(&graph).expect("schema SDL should generate");

    graphql_parser::schema::parse_schema::<String>(&sdl)
        .expect("generated SDL should parse as GraphQL schema");
    assert!(sdl.contains("scalar CoralGraphValue"));
    assert!(sdl.contains("  ASCENDING"));
    assert!(sdl.contains("  DESCENDING"));
    assert!(sdl.contains("enum CoralGraphNullOrder {\n  FIRST\n  LAST\n}"));
    assert!(sdl.contains(
            "Person(where: PersonWhere, orderBy: [PersonOrderBy!], limit: Int, first: Int, offset: Int, skip: Int, distinct: Boolean): [Person!]!"
        ));
    assert!(sdl.contains(
            "service(where: ServiceWhere, orderBy: [ServiceOrderBy!], limit: Int, first: Int, offset: Int, skip: Int, distinct: Boolean): [Service!]!"
        ));
    assert!(sdl.contains(
            "Services(where: ServiceWhere, orderBy: [ServiceOrderBy!], limit: Int, first: Int, offset: Int, skip: Int, distinct: Boolean): [Service!]!"
        ));
    assert!(sdl.contains(
            "services(where: ServiceWhere, orderBy: [ServiceOrderBy!], limit: Int, first: Int, offset: Int, skip: Int, distinct: Boolean): [Service!]!"
        ));
    assert!(sdl.contains(
            "input PersonOrderBy {\n  field: PersonOrderField!\n  direction: CoralGraphOrderDirection = ASC\n  nulls: CoralGraphNullOrder\n  _elementId: CoralGraphOrderDirection\n  _id: CoralGraphOrderDirection\n  id: CoralGraphOrderDirection"
        ));
    assert!(sdl.contains("  name: CoralGraphOrderDirection"));
    assert!(sdl.contains("  team: CoralGraphOrderDirection"));
    assert!(sdl.contains("input PersonWhere {"));
    assert!(sdl.contains("  _id: CoralGraphIdentityFilter"));
    assert!(sdl.contains("  _elementId: CoralGraphElementIdFilter"));
    assert!(sdl.contains("  out_OWNS: PersonOutOWNSFilter"));
    assert!(sdl.contains("  _and: [PersonWhere!]"));
    assert!(sdl.contains("  _not: PersonWhere"));
    assert!(sdl.contains("  AND: [PersonWhere!]"));
    assert!(sdl.contains("  NOT: PersonWhere"));
    assert!(sdl.contains("enum PersonOrderField {"));
    assert!(sdl.contains("  team"));
    assert!(sdl.contains("enum PersonAggregateField {"));
    assert!(sdl.contains("  _count(field: PersonAggregateField): Int"));
    assert!(sdl.contains("  _countDistinct(field: PersonAggregateField!): Int"));
    assert!(sdl.contains("  _collect(field: PersonAggregateField!): [CoralGraphValue!]"));
    assert!(sdl.contains("  _collectDistinct(field: PersonAggregateField!): [CoralGraphValue!]"));
    assert!(sdl.contains("  _avg(field: PersonAggregateField!): CoralGraphValue"));
    assert!(sdl.contains("  _sumDistinct(field: PersonAggregateField!): CoralGraphValue"));
    assert!(sdl.contains("  _avgDistinct(field: PersonAggregateField!): CoralGraphValue"));
    assert!(sdl.contains("  _median(field: PersonAggregateField!): CoralGraphValue"));
    assert!(sdl.contains("  _medianDistinct(field: PersonAggregateField!): CoralGraphValue"));
    assert!(sdl.contains(
        "  _percentileCont(field: PersonAggregateField!, percentile: Float!): CoralGraphValue"
    ));
    assert!(sdl.contains("  _stDev(field: PersonAggregateField!): CoralGraphValue"));
    assert!(sdl.contains("  _stDevP(field: PersonAggregateField!): CoralGraphValue"));
    assert!(sdl.contains("  _minDistinct(field: PersonAggregateField!): CoralGraphValue"));
    assert!(sdl.contains("  _maxDistinct(field: PersonAggregateField!): CoralGraphValue"));
    assert!(sdl.contains(
            "out_OWNS(to: PersonOutOWNSToLabel = Service, where: ServiceWhere, relationshipWhere: OWNSRelationshipWhere): [Service!]!"
        ));
    assert!(sdl.contains(
            "input PersonOutOWNSFilter {\n  to: PersonOutOWNSToLabel = Service\n  where: ServiceWhere\n  relationshipWhere: OWNSRelationshipWhere\n}"
        ));
    assert!(sdl.contains("enum PersonOutOWNSToLabel {\n  Service\n}"));
    assert!(sdl.contains("type OWNS {"));
    assert!(sdl.contains("  source: CoralGraphValue"));
}

#[test]
fn graphql_schema_sdl_skips_reserved_shorthand_order_by_fields() {
    let graph = Declaration::from_yaml(
        r"
version: 1
name: order_reserved
nodes:
  - label: Service
    table: { schema: ops, name: services }
    key: id
    properties:
      field: field_column
      direction: direction_column
      nulls: nulls_column
      name: service_name
",
    )
    .expect("graph should parse");

    let sdl = graphql_schema_sdl_for_graph(&graph).expect("schema SDL should generate");
    graphql_parser::schema::parse_schema::<String>(&sdl)
        .expect("generated SDL should parse as GraphQL schema");

    let (_, order_input) = sdl
        .split_once("input ServiceOrderBy {")
        .expect("ServiceOrderBy input should exist");
    let (order_input, _) = order_input
        .split_once("}\n\n")
        .expect("ServiceOrderBy input should terminate");

    assert_eq!(order_input.matches("  field:").count(), 1);
    assert_eq!(order_input.matches("  direction:").count(), 1);
    assert_eq!(order_input.matches("  nulls:").count(), 1);
    assert!(order_input.contains("  _id: CoralGraphOrderDirection"));
    assert!(order_input.contains("  _elementId: CoralGraphOrderDirection"));
    assert!(order_input.contains("  id: CoralGraphOrderDirection"));
    assert!(order_input.contains("  name: CoralGraphOrderDirection"));
}

#[test]
fn rejects_graphql_schema_sdl_for_invalid_graphql_names() {
    let graph = Declaration::from_yaml(
        r"
version: 1
name: invalid_graphql
nodes:
  - label: Service-Account
    table: { schema: ops, name: services }
    key: id
",
    )
    .expect("graph should parse");

    let error = graphql_schema_sdl_for_graph(&graph)
        .expect_err("invalid GraphQL type names should be rejected");

    assert!(
        error.to_string().contains("not a valid GraphQL name"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_graphql_schema_sdl_for_reserved_property_names() {
    let graph = Declaration::from_yaml(
        r"
version: 1
name: reserved_property
nodes:
  - label: Service
    table: { schema: ops, name: services }
    key: id
    properties:
      _id: source_id
",
    )
    .expect("graph should parse");

    let error = graphql_schema_sdl_for_graph(&graph)
        .expect_err("reserved GraphQL property names should be rejected");

    assert!(
        error.to_string().contains("reserved GraphQL virtual field"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_graphql_schema_sdl_for_reserved_aggregate_property_names() {
    let graph = Declaration::from_yaml(
        r"
version: 1
name: reserved_aggregate_property
nodes:
  - label: Service
    table: { schema: ops, name: services }
    key: id
    properties:
      _median: risk_score
",
    )
    .expect("graph should parse");

    let error = graphql_schema_sdl_for_graph(&graph)
        .expect_err("reserved GraphQL aggregate property names should be rejected");

    assert!(
        error.to_string().contains("reserved GraphQL virtual field"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_graphql_schema_sdl_for_ambiguous_relationship_fields() {
    let graph = Declaration::from_yaml(
        r"
version: 1
name: ambiguous_relationship_field
nodes:
  - label: Person
    table: { schema: ops, name: people }
    key: id
  - label: Service
    table: { schema: ops, name: services }
    key: id
  - label: Team
    table: { schema: ops, name: teams }
    key: id
relationships:
  - type: OWNS
    table: { schema: ops, name: person_service_ownerships }
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
  - type: OWNS
    table: { schema: ops, name: person_team_ownerships }
    from: { label: Person, key: person_id }
    to: { label: Team, key: team_id }
",
    )
    .expect("graph should parse");

    let error = graphql_schema_sdl_for_graph(&graph)
        .expect_err("duplicate GraphQL relationship fields should be rejected");

    assert!(
        error
            .to_string()
            .contains("GraphQL field 'out_OWNS' would be generated more than once"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_graphql_schema_sdl_for_ambiguous_root_aliases() {
    let graph = Declaration::from_yaml(
        r"
version: 1
name: ambiguous_roots
nodes:
  - label: User
    table: { schema: ops, name: users }
    key: id
    properties:
      name: name
  - label: user
    table: { schema: ops, name: lower_users }
    key: id
    properties:
      name: name
",
    )
    .expect("graph should parse");

    let error = graphql_schema_sdl_for_graph(&graph)
        .expect_err("ambiguous root aliases should fail SDL generation");

    assert!(
        error
            .to_string()
            .contains("GraphQL query root field 'user' would be generated more than once"),
        "unexpected error: {error}"
    );
}

const TEST_GRAPH: &str = r"
version: 1
name: test
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
      source: source
";
