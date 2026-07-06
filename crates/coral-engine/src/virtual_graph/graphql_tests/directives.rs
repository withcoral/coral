use super::*;

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
