use super::*;

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
