use super::*;

#[test]
fn compiles_static_label_alternatives_with_optional_endpoint_property_aggregates() {
    let query = compile_cypher_query(
        "MATCH (owner:Person|Team) \
             OPTIONAL MATCH (owner)-[ownership:OWNS]->(service:Service) \
             RETURN owner.name AS owner, \
                    count(endNode(ownership).name) AS named_services, \
                    sum(endNode(ownership).risk) AS total_risk \
             ORDER BY owner",
    )
    .expect("optional endpoint property aggregates should compile as outer union aggregates");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert_eq!(
        union.first.projection_output_names(),
        vec![
            "owner".to_string(),
            "__coral_agg_1".to_string(),
            "__coral_agg_2".to_string(),
        ]
    );
    assert!(matches!(
        union.first.projections.get(1),
        Some(Projection::Expression {
            expression: ScalarExpression::PresenceGated {
                presence_variable,
                expression,
            },
            alias,
        }) if presence_variable == "ownership"
            && matches!(
                expression.as_ref(),
                ScalarExpression::Property(PropertyRef { variable, property })
                    if variable == "service" && property == "name"
            )
            && alias == "__coral_agg_1"
    ));
    assert_eq!(
        union.outer_projection,
        Some(GraphUnionOuterProjection {
            items: vec![
                GraphUnionOuterProjectionItem::Column {
                    name: "owner".to_string(),
                },
                GraphUnionOuterProjectionItem::Aggregate {
                    function: AggregateFunction::Count,
                    source: "__coral_agg_1".to_string(),
                    distinct: false,
                    alias: "named_services".to_string(),
                },
                GraphUnionOuterProjectionItem::Aggregate {
                    function: AggregateFunction::Sum,
                    source: "__coral_agg_2".to_string(),
                    distinct: false,
                    alias: "total_risk".to_string(),
                },
            ],
            group_by: vec!["owner".to_string()],
        })
    );
}

#[test]
fn compiles_static_label_alternatives_with_optional_endpoint_identity_aggregates() {
    let query = compile_cypher_query(
        "MATCH (owner:Person|Team) \
             OPTIONAL MATCH (owner)-[ownership:OWNS]->(service:Service) \
             RETURN owner.name AS owner, \
                    count(endNode(ownership)) AS services, \
                    count(DISTINCT endNode(ownership)) AS distinct_services, \
                    collect(endNode(ownership)) AS service_ids \
             ORDER BY owner",
    )
    .expect("optional endpoint identity aggregates should compile as outer union aggregates");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    for index in 1..=3 {
        let expected_alias = format!("__coral_agg_{index}");
        assert!(matches!(
            union.first.projections.get(index),
            Some(Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable,
                    expression,
                },
                alias,
            }) if presence_variable == "ownership"
                && matches!(
                    expression.as_ref(),
                    ScalarExpression::Key { variable } if variable == "service"
                )
                && alias == &expected_alias
        ));
    }
    assert_eq!(
        union.outer_projection,
        Some(GraphUnionOuterProjection {
            items: vec![
                GraphUnionOuterProjectionItem::Column {
                    name: "owner".to_string(),
                },
                GraphUnionOuterProjectionItem::Aggregate {
                    function: AggregateFunction::Count,
                    source: "__coral_agg_1".to_string(),
                    distinct: false,
                    alias: "services".to_string(),
                },
                GraphUnionOuterProjectionItem::Aggregate {
                    function: AggregateFunction::Count,
                    source: "__coral_agg_2".to_string(),
                    distinct: true,
                    alias: "distinct_services".to_string(),
                },
                GraphUnionOuterProjectionItem::Aggregate {
                    function: AggregateFunction::Collect,
                    source: "__coral_agg_3".to_string(),
                    distinct: false,
                    alias: "service_ids".to_string(),
                },
            ],
            group_by: vec!["owner".to_string()],
        })
    );
}

#[test]
fn compiles_optional_fixed_hop_path_value_returns_with_presence_gate() {
    let plan = compile_cypher(
        "MATCH (person:Person), (service:Service) \
             OPTIONAL MATCH path = (person)-[:OWNS]->(service) \
             RETURN path",
    )
    .expect("optional fixed-hop path values should compile");

    let Projection::Expression { expression, alias } = plan
        .projections
        .first()
        .expect("path return should produce one projection")
    else {
        panic!("path return should compile as an expression projection");
    };
    assert_eq!(alias, "path");
    let ScalarExpression::PresenceGated {
        presence_variable,
        expression,
    } = expression
    else {
        panic!("optional path value should be presence gated");
    };
    assert!(presence_variable.starts_with("__coral_rel_"));
    assert_eq!(
        expression.as_ref(),
        &ScalarExpression::PathValue {
            node_variables: vec!["person".to_string(), "service".to_string()],
            relationship_variables: vec![presence_variable.clone()],
        }
    );
}

#[test]
fn compiles_size_over_named_optional_path_variable() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH path = (service)-[dependency:DEPENDS_ON]->(target:Service) \
             RETURN service.name AS service, size(path) AS path_length \
             ORDER BY size(path)",
    )
    .expect("size(path) should preserve optional path length nullability");

    let expected = ScalarExpression::PresenceGated {
        presence_variable: "dependency".to_string(),
        expression: Box::new(ScalarExpression::Literal(Literal::Integer(1))),
    };
    assert_eq!(
        plan.projections.get(1),
        Some(&Projection::Expression {
            expression: expected.clone(),
            alias: "path_length".to_string(),
        })
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(expected),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_size_over_optional_path_element_lists() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH path = (service)-[dependency:DEPENDS_ON]->(target:Service) \
             RETURN service.name AS service, \
                    size(nodes(path)) AS node_count, \
                    size(relationships(path)) AS relationship_count \
             ORDER BY size(nodes(path))",
    )
    .expect("optional path element-list sizes should preserve nullability");

    let expected_node_count = ScalarExpression::PresenceGated {
        presence_variable: "dependency".to_string(),
        expression: Box::new(ScalarExpression::Literal(Literal::Integer(2))),
    };
    let expected_relationship_count = ScalarExpression::PresenceGated {
        presence_variable: "dependency".to_string(),
        expression: Box::new(ScalarExpression::Literal(Literal::Integer(1))),
    };
    assert_eq!(
        plan.projections.get(1),
        Some(&Projection::Expression {
            expression: expected_node_count.clone(),
            alias: "node_count".to_string(),
        })
    );
    assert_eq!(
        plan.projections.get(2),
        Some(&Projection::Expression {
            expression: expected_relationship_count,
            alias: "relationship_count".to_string(),
        })
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(expected_node_count),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_length_over_named_optional_path_variable() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH path = (service)-[dependency:DEPENDS_ON]->(target:Service) \
             RETURN service.name AS service, length(path) AS path_length \
             ORDER BY length(path)",
    )
    .expect("named optional path length should compile");

    let expected = ScalarExpression::PresenceGated {
        presence_variable: "dependency".to_string(),
        expression: Box::new(ScalarExpression::Literal(Literal::Integer(1))),
    };
    assert_eq!(
        plan.projections.get(1),
        Some(&Projection::Expression {
            expression: expected.clone(),
            alias: "path_length".to_string(),
        })
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(expected),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_length_over_anonymous_optional_path_variable() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH path = (service)-[:DEPENDS_ON]->(target:Service) \
             RETURN service.name AS service, length(path) AS path_length \
             ORDER BY length(path)",
    )
    .expect("anonymous optional path length should compile with an internal presence variable");

    let presence_variable = plan
        .relationships
        .first()
        .expect("anonymous optional relationship should compile")
        .variable
        .as_ref()
        .expect("anonymous optional relationship should receive an internal variable")
        .clone();
    assert!(presence_variable.starts_with("__coral_rel_"));
    let expected = ScalarExpression::PresenceGated {
        presence_variable,
        expression: Box::new(ScalarExpression::Literal(Literal::Integer(1))),
    };
    assert_eq!(
        plan.projections.get(1),
        Some(&Projection::Expression {
            expression: expected.clone(),
            alias: "path_length".to_string(),
        })
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(expected),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_length_over_optional_zero_hop_path_to_new_endpoint() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH path = (service)-[:DEPENDS_ON*0]->(self:Service) \
             RETURN service.name AS service, self.name AS self, length(path) AS path_length \
             ORDER BY size(path)",
    )
    .expect("deterministic optional zero-hop path length should compile");

    assert!(plan.relationships.is_empty());
    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::KeyComparison(KeyPredicate {
            variable: "service".to_string(),
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Key {
                variable: "self".to_string(),
            },
        }))
    );
    assert_eq!(
        plan.projections.get(2),
        Some(&Projection::Expression {
            expression: ScalarExpression::Literal(Literal::Integer(0)),
            alias: "path_length".to_string(),
        })
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Literal(Literal::Integer(0)),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_length_over_optional_zero_hop_path_to_same_bound_endpoint() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH path = (service)-[:DEPENDS_ON*0]->(service) \
             RETURN service.name AS service, length(path) AS path_length",
    )
    .expect("same-bound optional zero-hop path length should compile");

    assert!(plan.relationships.is_empty());
    assert_eq!(
        plan.projections.get(1),
        Some(&Projection::Expression {
            expression: ScalarExpression::Literal(Literal::Integer(0)),
            alias: "path_length".to_string(),
        })
    );
}

#[test]
fn compiles_optional_zero_hop_path_length_between_distinct_bound_endpoints() {
    let plan = compile_cypher(
        "MATCH (source:Service), (target:Service) \
             OPTIONAL MATCH path = (source)-[:DEPENDS_ON*0]->(target) \
             RETURN length(path) AS path_length, size(path) AS path_size \
             ORDER BY length(path)",
    )
    .expect("bound endpoint zero-hop path length should compile as equality-gated metadata");

    let expected = ScalarExpression::Case {
        alternatives: vec![ScalarCaseAlternative {
            when: PredicateExpression::KeyComparison(KeyPredicate {
                variable: "source".to_string(),
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Key {
                    variable: "target".to_string(),
                },
            }),
            then: ScalarExpression::Literal(Literal::Integer(0)),
        }],
        else_expression: Some(Box::new(ScalarExpression::Literal(Literal::Null))),
    };
    assert_eq!(
        plan.projections.first(),
        Some(&Projection::Expression {
            expression: expected.clone(),
            alias: "path_length".to_string(),
        })
    );
    assert_eq!(
        plan.projections.get(1),
        Some(&Projection::Expression {
            expression: expected.clone(),
            alias: "path_size".to_string(),
        })
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(expected),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_optional_zero_hop_path_length_between_bound_cross_label_endpoints() {
    let plan = compile_cypher(
        "MATCH (source:Service), (person:Person) \
             OPTIONAL MATCH path = (source)-[:DEPENDS_ON*0]->(person) \
             RETURN length(path) AS path_length",
    )
    .expect("bound cross-label zero-hop path length should compile as null metadata");

    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::Case {
                alternatives: vec![ScalarCaseAlternative {
                    when: PredicateExpression::Boolean(false),
                    then: ScalarExpression::Literal(Literal::Integer(0)),
                }],
                else_expression: Some(Box::new(ScalarExpression::Literal(Literal::Null))),
            },
            alias: "path_length".to_string(),
        }]
    );
}

#[test]
fn compiles_optional_zero_hop_local_predicates_into_path_presence_gate() {
    let plan = compile_cypher(
        "MATCH (source:Service), (target:Service) \
             OPTIONAL MATCH path = (source)-[:DEPENDS_ON*0]->(target) \
             WHERE source.tier = 'prod' \
             RETURN length(path) AS path_length",
    )
    .expect("bound endpoint zero-hop local predicate should gate path metadata");

    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::Case {
                alternatives: vec![ScalarCaseAlternative {
                    when: PredicateExpression::And {
                        left: Box::new(PredicateExpression::KeyComparison(KeyPredicate {
                            variable: "source".to_string(),
                            operator: ComparisonOperator::Equal,
                            rhs: PredicateRhs::Key {
                                variable: "target".to_string(),
                            },
                        })),
                        right: Box::new(PredicateExpression::Comparison(PropertyPredicate {
                            property: PropertyRef {
                                variable: "source".to_string(),
                                property: "tier".to_string(),
                            },
                            operator: ComparisonOperator::Equal,
                            rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
                        })),
                    },
                    then: ScalarExpression::Literal(Literal::Integer(0)),
                }],
                else_expression: Some(Box::new(ScalarExpression::Literal(Literal::Null))),
            },
            alias: "path_length".to_string(),
        }]
    );
}

#[test]
fn rejects_optional_zero_hop_local_predicates_with_introduced_endpoint() {
    let error = compile_cypher(
        "MATCH (source:Service) \
             OPTIONAL MATCH path = (source)-[:DEPENDS_ON*0]->(self:Service) \
             WHERE self.tier = 'prod' \
             RETURN length(path) AS path_length",
    )
    .expect_err("introduced zero-hop endpoint would require nullable node binding");

    assert!(
        error
            .to_string()
            .contains("nullable zero-hop endpoint binding"),
        "{error}"
    );
}

#[test]
fn anonymous_optional_path_presence_bindings_stay_hidden_from_with_scope() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH path = (service)-[:DEPENDS_ON]->(target:Service) \
             WITH service \
             RETURN service.name AS service",
    )
    .expect("transparent WITH should not require generated optional path bindings");

    assert!(
        plan.relationships
            .iter()
            .filter_map(|relationship| relationship.variable.as_deref())
            .any(|variable| variable.starts_with("__coral_rel_")),
        "anonymous optional path should still have an internal presence binding"
    );
    assert_eq!(
        plan.projections,
        vec![Projection::Property {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            },
            alias: Some("service".to_string()),
        }]
    );
}

#[test]
fn compiles_multihop_optional_match_scope() {
    let plan = compile_cypher(
            "MATCH (service:Service) \
             OPTIONAL MATCH (service)-[:DEPENDS_ON]->(middle:Service)-[:DEPENDS_ON]->(target:Service) \
             RETURN service.name AS service, middle.name AS middle, target.name AS target",
        )
        .expect("multi-hop optional match should compile");

    assert_eq!(plan.relationships.len(), 2);
    assert_eq!(plan.optional_relationships, vec![0, 1]);
    assert_eq!(plan.optional_matches.len(), 1);
    assert_eq!(
        plan.optional_matches
            .first()
            .expect("optional match scope")
            .relationship_indices,
        vec![0, 1]
    );
}

#[test]
fn compiles_multihop_optional_match_between_bound_endpoints_scope() {
    let plan = compile_cypher(
        "MATCH (source:Service), (target:Service) \
             OPTIONAL MATCH (source)-[:DEPENDS_ON]->(middle:Service)-[:DEPENDS_ON]->(target) \
             RETURN source.name AS source, target.name AS target, middle.name AS middle",
    )
    .expect("bound-endpoint multi-hop optional match should compile");

    assert_eq!(
        plan.nodes
            .iter()
            .map(|node| node.variable.as_str())
            .collect::<Vec<_>>(),
        vec!["source", "target", "middle"]
    );
    assert_eq!(plan.relationships.len(), 2);
    assert_eq!(plan.optional_relationships, vec![0, 1]);
    assert_eq!(
        plan.optional_matches,
        vec![OptionalMatchScope {
            node_indices: vec![2],
            relationship_indices: vec![0, 1],
            predicate: None,
        }]
    );
}

#[test]
fn compiles_relationship_endpoint_property_aggregates_on_optional_relationships() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH (service)-[dependency:DEPENDS_ON]->(dependency_service:Service) \
             RETURN count(endNode(dependency).name) AS named_dependencies, \
                    sum(endNode(dependency).risk) AS dependency_risk",
    )
    .expect("optional relationship endpoint property aggregates should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Aggregate {
                function: AggregateFunction::Count,
                target: AggregateTarget::PresenceGatedProperty {
                    property: PropertyRef {
                        variable: "dependency_service".to_string(),
                        property: "name".to_string(),
                    },
                    presence_variable: "dependency".to_string(),
                },
                distinct: false,
                alias: "named_dependencies".to_string(),
            },
            Projection::Aggregate {
                function: AggregateFunction::Sum,
                target: AggregateTarget::PresenceGatedProperty {
                    property: PropertyRef {
                        variable: "dependency_service".to_string(),
                        property: "risk".to_string(),
                    },
                    presence_variable: "dependency".to_string(),
                },
                distinct: false,
                alias: "dependency_risk".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_collect_over_optional_relationship_endpoint_properties() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH (service)-[dependency:DEPENDS_ON]->(dependency_service:Service) \
             RETURN collect(endNode(dependency).name) AS dependencies",
    )
    .expect("optional endpoint collect should compile to a presence-gated property aggregate");

    assert_eq!(
        plan.projections,
        vec![Projection::Aggregate {
            function: AggregateFunction::Collect,
            target: AggregateTarget::PresenceGatedProperty {
                property: PropertyRef {
                    variable: "dependency_service".to_string(),
                    property: "name".to_string(),
                },
                presence_variable: "dependency".to_string(),
            },
            distinct: false,
            alias: "dependencies".to_string(),
        }]
    );
}

#[test]
fn compiles_optional_relationship_endpoint_property_indexes() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH (service)-[dependency:DEPENDS_ON]->(dependency_service:Service) \
             RETURN endNode(dependency)['name'] AS dependency_name, \
                    properties(startNode(dependency))['name'] AS source_name \
             ORDER BY properties(endNode(dependency))['risk']",
    )
    .expect("optional endpoint property indexes should compile as presence-gated properties");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "dependency".to_string(),
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "dependency_service".to_string(),
                        property: "name".to_string(),
                    })),
                },
                alias: "dependency_name".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "dependency".to_string(),
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    })),
                },
                alias: "source_name".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::PresenceGated {
                presence_variable: "dependency".to_string(),
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "dependency_service".to_string(),
                    property: "risk".to_string(),
                })),
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_relationship_endpoint_identity_aggregates_on_optional_relationships() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH (service)-[dependency:DEPENDS_ON]->(dependency_service:Service) \
             RETURN count(endNode(dependency)) AS dependencies, \
                    count(DISTINCT startNode(dependency)) AS sources",
    )
    .expect("optional relationship endpoint identity aggregates should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Aggregate {
                function: AggregateFunction::Count,
                target: AggregateTarget::PresenceGatedVariableKey {
                    variable: "dependency_service".to_string(),
                    presence_variable: "dependency".to_string(),
                },
                distinct: false,
                alias: "dependencies".to_string(),
            },
            Projection::Aggregate {
                function: AggregateFunction::Count,
                target: AggregateTarget::PresenceGatedVariableKey {
                    variable: "service".to_string(),
                    presence_variable: "dependency".to_string(),
                },
                distinct: true,
                alias: "sources".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_relationship_endpoint_identity_functions_on_optional_relationships() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH (service)-[dependency:DEPENDS_ON]->(dependency_service:Service) \
             RETURN id(endNode(dependency)) AS dependency_id, \
                    elementId(startNode(dependency)) AS source_element_id \
             ORDER BY id(endNode(dependency))",
    )
    .expect("relationship endpoint identity functions over optional relationships should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "dependency".to_string(),
                    expression: Box::new(ScalarExpression::Key {
                        variable: "dependency_service".to_string(),
                    }),
                },
                alias: "dependency_id".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "dependency".to_string(),
                    expression: Box::new(ScalarExpression::ElementId {
                        variable: "service".to_string(),
                    }),
                },
                alias: "source_element_id".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::PresenceGated {
                presence_variable: "dependency".to_string(),
                expression: Box::new(ScalarExpression::Key {
                    variable: "dependency_service".to_string(),
                }),
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_relationship_endpoint_metadata_functions_on_optional_relationships() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH (service)-[dependency:DEPENDS_ON]->(dependency_service:Service) \
             RETURN labels(endNode(dependency)) AS dependency_labels, \
                    keys(startNode(dependency)) AS source_keys \
             ORDER BY labels(endNode(dependency)), keys(startNode(dependency))",
    )
    .expect("relationship endpoint metadata functions over optional relationships should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "dependency".to_string(),
                    expression: Box::new(ScalarExpression::NodeLabels {
                        variable: "dependency_service".to_string(),
                        label: "Service".to_string(),
                    }),
                },
                alias: "dependency_labels".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "dependency".to_string(),
                    expression: Box::new(ScalarExpression::PropertyKeys {
                        variable: "service".to_string(),
                    }),
                },
                alias: "source_keys".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::PresenceGated {
                    presence_variable: "dependency".to_string(),
                    expression: Box::new(ScalarExpression::NodeLabels {
                        variable: "dependency_service".to_string(),
                        label: "Service".to_string(),
                    }),
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::PresenceGated {
                    presence_variable: "dependency".to_string(),
                    expression: Box::new(ScalarExpression::PropertyKeys {
                        variable: "service".to_string(),
                    }),
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_optional_metadata_list_equality_as_presence_gated_scalar_predicates() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN labels(person) = ['Person'] AS owner_has_person_label, \
                    keys(person) = ['name', 'team'] AS owner_has_person_keys",
    )
    .expect("optional metadata list equality scalar projections should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::Predicate(Box::new(
                    PredicateExpression::ScalarComparison(ScalarPredicate {
                        lhs: ScalarExpression::PresenceGated {
                            presence_variable: "person".to_string(),
                            expression: Box::new(
                                ScalarExpression::Literal(Literal::Boolean(true),)
                            ),
                        },
                        operator: ComparisonOperator::Equal,
                        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                            Literal::Boolean(true),
                        )),
                    }),
                )),
                alias: "owner_has_person_label".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Predicate(Box::new(
                    PredicateExpression::ScalarComparison(ScalarPredicate {
                        lhs: ScalarExpression::PresenceGated {
                            presence_variable: "person".to_string(),
                            expression: Box::new(
                                ScalarExpression::Literal(Literal::Boolean(true),)
                            ),
                        },
                        operator: ComparisonOperator::Equal,
                        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                            Literal::Boolean(true),
                        )),
                    }),
                )),
                alias: "owner_has_person_keys".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_optional_static_list_in_rhs_as_presence_gated_predicates() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN service.name IN keys(person) AS service_name_is_owner_key",
    )
    .expect("optional static list IN RHS should compile");

    assert!(matches!(
        plan.projections.as_slice(),
        [Projection::Expression {
            expression: ScalarExpression::Predicate(predicate),
            alias,
        }] if alias == "service_name_is_owner_key"
            && matches!(
                predicate.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::PresenceGated {
                        presence_variable,
                        expression,
                    },
                    operator: ComparisonOperator::Equal,
                    rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                        Literal::Boolean(true),
                    )),
                }) if presence_variable == "person"
                    && matches!(
                        expression.as_ref(),
                        ScalarExpression::Predicate(inner)
                            if matches!(
                                inner.as_ref(),
                                PredicateExpression::Comparison(PropertyPredicate {
                                    property: PropertyRef { variable, property },
                                    operator: ComparisonOperator::In,
                                    rhs: PredicateRhs::List(literals),
                                }) if variable == "service"
                                    && property == "name"
                                    && literals == &vec![
                                    Literal::String("name".to_string()),
                                    Literal::String("team".to_string()),
                                ]
                            )
                    )
            )
    ));

    let distinct_plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN COLLECT { \
               MATCH (service)-[:DEPENDS_ON]->(dependency:Service) \
               RETURN DISTINCT dependency.name \
             } AS dependency_names",
    )
    .expect("COLLECT subquery DISTINCT scalar projection should compile");

    assert!(matches!(
        distinct_plan.projections.as_slice(),
        [Projection::Expression {
            expression:
                ScalarExpression::CollectSubquery {
                    pattern,
                    target,
                    distinct,
                },
            alias,
        }] if alias == "dependency_names"
            && *distinct
            && matches!(pattern.as_ref(), CountSubqueryPattern::Relationships(_))
            && matches!(
                target.as_ref(),
                ScalarExpression::Property(PropertyRef { variable, property })
                    if variable == "dependency" && property == "name"
            )
    ));
}

#[test]
fn compiles_optional_static_list_concat_in_rhs_as_presence_gated_predicates() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN service.name IN (keys(person) + ['extra']) AS service_name_is_owner_key",
    )
    .expect("optional static list concatenation IN RHS should compile");

    assert!(matches!(
        plan.projections.as_slice(),
        [Projection::Expression {
            expression: ScalarExpression::Predicate(predicate),
            alias,
        }] if alias == "service_name_is_owner_key"
            && matches!(
                predicate.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::PresenceGated {
                        presence_variable,
                        expression,
                    },
                    operator: ComparisonOperator::Equal,
                    rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                        Literal::Boolean(true),
                    )),
                }) if presence_variable == "person"
                    && matches!(
                        expression.as_ref(),
                        ScalarExpression::Predicate(inner)
                            if matches!(
                                inner.as_ref(),
                                PredicateExpression::Comparison(PropertyPredicate {
                                    property: PropertyRef { variable, property },
                                    operator: ComparisonOperator::In,
                                    rhs: PredicateRhs::List(literals),
                                }) if variable == "service"
                                    && property == "name"
                                    && literals == &vec![
                                        Literal::String("name".to_string()),
                                        Literal::String("team".to_string()),
                                        Literal::String("extra".to_string()),
                                    ]
                            )
                    )
            )
    ));
}

#[test]
fn compiles_optional_metadata_list_indexes_as_presence_gated_scalars() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN labels(person)[0] AS owner_label, \
                    keys(person)[-1] AS owner_last_key",
    )
    .expect("optional metadata list indexes should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "person".to_string(),
                    expression: Box::new(ScalarExpression::Literal(Literal::String(
                        "Person".to_string(),
                    ))),
                },
                alias: "owner_label".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "person".to_string(),
                    expression: Box::new(ScalarExpression::Literal(Literal::String(
                        "team".to_string(),
                    ))),
                },
                alias: "owner_last_key".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_optional_metadata_list_slices_as_presence_gated_lists() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN labels(person)[0..1] AS owner_labels, \
                    keys(person)[..1] AS owner_first_key",
    )
    .expect("optional metadata list slices should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "person".to_string(),
                    expression: Box::new(ScalarExpression::TypedLiteralList {
                        literals: vec![Literal::String("Person".to_string())],
                        element_type: LiteralListElementType::String,
                    }),
                },
                alias: "owner_labels".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "person".to_string(),
                    expression: Box::new(ScalarExpression::TypedLiteralList {
                        literals: vec![Literal::String("name".to_string())],
                        element_type: LiteralListElementType::String,
                    }),
                },
                alias: "owner_first_key".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_optional_static_list_reverse_as_presence_gated_scalar() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN reverse(labels(person) + keys(person)) AS owner_metadata",
    )
    .expect("optional static reverse() list function should compile");

    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::PresenceGated {
                presence_variable: "person".to_string(),
                expression: Box::new(ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("team".to_string()),
                        Literal::String("name".to_string()),
                        Literal::String("Person".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                }),
            },
            alias: "owner_metadata".to_string(),
        }]
    );
}

#[test]
fn compiles_optional_static_list_indexes_and_slices_as_presence_gated_scalars() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN reverse(labels(person) + keys(person))[0] AS owner_last_metadata, \
                    reverse(labels(person) + keys(person))[1..] AS owner_metadata_tail",
    )
    .expect("optional static list indexes and slices should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "person".to_string(),
                    expression: Box::new(ScalarExpression::Literal(Literal::String(
                        "team".to_string()
                    ))),
                },
                alias: "owner_last_metadata".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "person".to_string(),
                    expression: Box::new(ScalarExpression::TypedLiteralList {
                        literals: vec![
                            Literal::String("name".to_string()),
                            Literal::String("Person".to_string())
                        ],
                        element_type: LiteralListElementType::String,
                    }),
                },
                alias: "owner_metadata_tail".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_optional_static_list_concatenation_as_presence_gated_scalar() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN labels(person) + keys(person) AS owner_metadata",
    )
    .expect("optional static list concatenation should compile");

    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::PresenceGated {
                presence_variable: "person".to_string(),
                expression: Box::new(ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("Person".to_string()),
                        Literal::String("name".to_string()),
                        Literal::String("team".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                }),
            },
            alias: "owner_metadata".to_string(),
        }]
    );
}

#[test]
fn compiles_optional_static_list_quantifiers_as_presence_gated_predicates() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN all(key IN keys(person) WHERE key <> 'deprecated') AS owner_keys_declared",
    )
    .expect("optional static list collection predicate should compile");

    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::Predicate(Box::new(
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::PresenceGated {
                        presence_variable: "person".to_string(),
                        expression: Box::new(ScalarExpression::Literal(Literal::Boolean(true))),
                    },
                    operator: ComparisonOperator::Equal,
                    rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                        Literal::Boolean(true),
                    )),
                }),
            )),
            alias: "owner_keys_declared".to_string(),
        }]
    );
}

#[test]
fn compiles_optional_static_list_comparisons_as_presence_gated_predicates() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN tail(keys(person)) = ['team'] AS owner_key_tail_matches",
    )
    .expect("optional static list comparison should compile");

    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::Predicate(Box::new(
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::PresenceGated {
                        presence_variable: "person".to_string(),
                        expression: Box::new(ScalarExpression::Literal(Literal::Boolean(true,))),
                    },
                    operator: ComparisonOperator::Equal,
                    rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                        Literal::Boolean(true),
                    )),
                }),
            )),
            alias: "owner_key_tail_matches".to_string(),
        }]
    );
}

#[test]
fn compiles_optional_static_list_endpoint_functions_as_presence_gated_scalars() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN head(labels(person)) AS owner_label, \
                    last(keys(person)) AS owner_last_key",
    )
    .expect("optional list endpoint functions should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "person".to_string(),
                    expression: Box::new(ScalarExpression::Literal(Literal::String(
                        "Person".to_string(),
                    ))),
                },
                alias: "owner_label".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "person".to_string(),
                    expression: Box::new(ScalarExpression::Literal(Literal::String(
                        "team".to_string(),
                    ))),
                },
                alias: "owner_last_key".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_optional_metadata_list_sizes_as_presence_gated_scalars() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN size(labels(person)) AS owner_label_count, \
                    size(keys(person)) AS owner_key_count",
    )
    .expect("optional metadata list sizes should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "person".to_string(),
                    expression: Box::new(ScalarExpression::Literal(Literal::Integer(1))),
                },
                alias: "owner_label_count".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "person".to_string(),
                    expression: Box::new(ScalarExpression::Literal(Literal::Integer(2))),
                },
                alias: "owner_key_count".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_is_empty_metadata_on_optional_relationship_endpoints() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[owns:OWNS]->(service) \
             RETURN isEmpty(labels(startNode(owns))) AS owner_labels_empty, \
                    isEmpty(keys(startNode(owns))) AS owner_keys_empty \
             ORDER BY isEmpty(labels(startNode(owns)))",
    )
    .expect("optional endpoint isEmpty metadata should compile");

    let Projection::Expression {
        expression: ScalarExpression::Predicate(predicate),
        alias,
    } = plan
        .projections
        .first()
        .expect("expected optional endpoint labels isEmpty projection")
    else {
        panic!("expected optional endpoint labels isEmpty projection");
    };
    assert_eq!(alias, "owner_labels_empty");
    assert!(matches!(
        predicate.as_ref(),
        PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::PresenceGated {
                presence_variable,
                expression,
            },
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Boolean(true))),
        }) if presence_variable == "owns"
            && matches!(expression.as_ref(), ScalarExpression::Literal(Literal::Boolean(false)))
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
fn compiles_anchored_optional_match() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN service.name AS service, person.name AS owner",
    )
    .expect("anchored OPTIONAL MATCH should compile");

    assert_eq!(plan.optional_relationships, vec![0]);
    assert_eq!(
        plan.optional_matches,
        vec![OptionalMatchScope {
            node_indices: vec![1],
            relationship_indices: vec![0],
            predicate: None,
        }]
    );
    assert_eq!(
        plan.nodes,
        vec![
            NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "person".to_string(),
                label: "Person".to_string(),
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
}

#[test]
fn compiles_leading_node_only_optional_match() {
    let plan = compile_cypher("OPTIONAL MATCH (person:Person) RETURN person.name AS name")
        .expect("leading node-only OPTIONAL MATCH should compile");

    assert_eq!(plan.optional_relationships, Vec::<usize>::new());
    assert_eq!(
        plan.optional_matches,
        vec![OptionalMatchScope {
            node_indices: vec![0],
            relationship_indices: Vec::new(),
            predicate: None,
        }]
    );
    assert_eq!(
        plan.nodes,
        vec![NodePattern {
            variable: "person".to_string(),
            label: "Person".to_string(),
        }]
    );
}

#[test]
fn compiles_leading_relationship_optional_match() {
    let plan = compile_cypher(
        "OPTIONAL MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN owns.since AS since",
    )
    .expect("leading relationship OPTIONAL MATCH should compile");

    assert_eq!(plan.optional_relationships, vec![0]);
    assert_eq!(
        plan.optional_matches,
        vec![OptionalMatchScope {
            node_indices: vec![0, 1],
            relationship_indices: vec![0],
            predicate: None,
        }]
    );
    assert_eq!(
        plan.relationships,
        vec![RelationshipPattern {
            variable: Some("owns".to_string()),
            relationship_type: "OWNS".to_string(),
            left: "person".to_string(),
            direction: Direction::Outgoing,
            right: "service".to_string(),
        }]
    );
}

#[test]
fn compiles_leading_unlabeled_node_only_optional_match_from_single_label_graph() {
    let graph = single_label_person_knows_test_graph();
    let plan =
        compile_cypher_for_graph(&graph, "OPTIONAL MATCH (person) RETURN person.name AS name")
            .expect("leading unlabeled OPTIONAL MATCH should compile from graph metadata");

    assert_eq!(plan.optional_relationships, Vec::<usize>::new());
    assert_eq!(
        plan.optional_matches,
        vec![OptionalMatchScope {
            node_indices: vec![0],
            relationship_indices: Vec::new(),
            predicate: None,
        }]
    );
    assert_eq!(
        plan.nodes,
        vec![NodePattern {
            variable: "person".to_string(),
            label: "Person".to_string(),
        }]
    );
}

#[test]
fn compiles_leading_unlabeled_node_only_optional_match_from_multi_label_graph_as_null_preserving_union()
 {
    let graph = star_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "OPTIONAL MATCH (entity) RETURN entity.name AS name",
    )
    .expect("leading unlabeled OPTIONAL MATCH should compile as label alternatives");

    let GraphQuery::Union(union) = query else {
        panic!("expected multi-label unlabeled optional match to expand into a union query");
    };
    assert!(union.preserve_empty_result_with_null_row);
    assert_eq!(union.branches.len(), 2);
    for plan in
        std::iter::once(&union.first).chain(union.branches.iter().map(|branch| &branch.plan))
    {
        assert_eq!(plan.relationships, Vec::<RelationshipPattern>::new());
        assert_eq!(plan.optional_relationships, Vec::<usize>::new());
        assert_eq!(
            plan.optional_matches,
            vec![OptionalMatchScope {
                node_indices: vec![0],
                relationship_indices: Vec::new(),
                predicate: None,
            }]
        );
    }
}

#[test]
fn compiles_leading_untyped_relationship_optional_match_as_null_preserving_union() {
    let graph = star_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "OPTIONAL MATCH (owner)-[owns]->(service) RETURN owns.source AS source",
    )
    .expect("leading untyped relationship OPTIONAL MATCH should compile as mapping alternatives");

    let GraphQuery::Union(union) = query else {
        panic!("expected untyped relationship optional match to expand into a union query");
    };
    assert!(union.preserve_empty_result_with_null_row);
    assert_eq!(union.branches.len(), 1);

    let plans = std::iter::once(&union.first)
        .chain(union.branches.iter().map(|branch| &branch.plan))
        .collect::<Vec<_>>();
    assert_eq!(
        plans
            .iter()
            .map(|plan| {
                (
                    plan.nodes
                        .iter()
                        .map(|node| node.label.as_str())
                        .collect::<Vec<_>>(),
                    plan.relationships
                        .iter()
                        .map(|relationship| relationship.relationship_type.as_str())
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>(),
        vec![
            (vec!["Person", "Service"], vec!["OWNS"]),
            (vec!["Team", "Service"], vec!["OWNS"]),
        ]
    );
    for plan in plans {
        assert_eq!(plan.optional_relationships, vec![0]);
        assert_eq!(
            plan.optional_matches,
            vec![OptionalMatchScope {
                node_indices: vec![0, 1],
                relationship_indices: vec![0],
                predicate: None,
            }]
        );
    }
}

#[test]
fn compiles_consecutive_leading_node_only_optional_matches() {
    let plan = compile_cypher(
        "OPTIONAL MATCH (person:Person) \
             OPTIONAL MATCH (service:Service) \
             RETURN person.name AS person, service.name AS service",
    )
    .expect("consecutive leading node-only OPTIONAL MATCH clauses should compile");

    assert_eq!(plan.optional_relationships, Vec::<usize>::new());
    assert_eq!(
        plan.optional_matches,
        vec![
            OptionalMatchScope {
                node_indices: vec![0],
                relationship_indices: Vec::new(),
                predicate: None,
            },
            OptionalMatchScope {
                node_indices: vec![1],
                relationship_indices: Vec::new(),
                predicate: None,
            },
        ]
    );
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
}

#[test]
fn compiles_consecutive_leading_static_label_optional_matches_with_hidden_identity_dedupe() {
    let query = compile_cypher_query(
        "OPTIONAL MATCH (left:Person|Team) \
             OPTIONAL MATCH (right:Person|Team) \
             RETURN left.name AS l, right.name AS r",
    )
    .expect("consecutive leading static label OPTIONAL MATCH clauses should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected consecutive static label optionals to expand into a union query");
    };

    assert!(!union.preserve_empty_result_with_null_row);
    assert!(union.branches.iter().all(|branch| !branch.all));
    assert_eq!(
        union
            .outer_projection
            .as_ref()
            .expect("hidden identity columns should be stripped by an outer projection")
            .output_names(),
        vec!["l".to_string(), "r".to_string()]
    );
    let projection_names = union.first.projection_output_names();
    assert_eq!(projection_names.len(), 4);
    assert_eq!(
        projection_names.get(..2),
        Some(&["l".to_string(), "r".to_string()][..])
    );
    assert!(
        projection_names
            .get(2..)
            .expect("hidden identity projection suffix")
            .iter()
            .all(|name| name.starts_with("__coral_static_optional_identity_"))
    );
}

#[test]
fn compiles_consecutive_leading_relationship_optional_matches() {
    let plan = compile_cypher(
        "OPTIONAL MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             OPTIONAL MATCH (source:Service)-[depends:DEPENDS_ON]->(target:Service) \
             RETURN owns.since AS since, depends.source AS source",
    )
    .expect("consecutive leading relationship OPTIONAL MATCH clauses should compile");

    assert_eq!(plan.optional_relationships, vec![0, 1]);
    assert_eq!(
        plan.optional_matches,
        vec![
            OptionalMatchScope {
                node_indices: vec![0, 1],
                relationship_indices: vec![0],
                predicate: None,
            },
            OptionalMatchScope {
                node_indices: vec![2, 3],
                relationship_indices: vec![1],
                predicate: None,
            },
        ]
    );
    assert_eq!(
        plan.relationships,
        vec![
            RelationshipPattern {
                variable: Some("owns".to_string()),
                relationship_type: "OWNS".to_string(),
                left: "person".to_string(),
                direction: Direction::Outgoing,
                right: "service".to_string(),
            },
            RelationshipPattern {
                variable: Some("depends".to_string()),
                relationship_type: "DEPENDS_ON".to_string(),
                left: "source".to_string(),
                direction: Direction::Outgoing,
                right: "target".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_match_after_leading_optional_when_it_uses_optional_binding() {
    let plan = compile_cypher(
        "OPTIONAL MATCH (person:Person) \
             WITH person \
             MATCH (person)-[:KNOWS]->(friend:Person) \
             RETURN person.name AS person, friend.name AS friend",
    )
    .expect("MATCH after leading OPTIONAL MATCH should compile");

    assert_eq!(plan.optional_relationships, Vec::<usize>::new());
    assert_eq!(
        plan.optional_matches,
        vec![OptionalMatchScope {
            node_indices: vec![0],
            relationship_indices: Vec::new(),
            predicate: None,
        }]
    );
    assert_eq!(
        plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "KNOWS".to_string(),
            left: "person".to_string(),
            direction: Direction::Outgoing,
            right: "friend".to_string(),
        }]
    );
}

#[test]
fn compiles_match_after_optional_when_it_uses_only_mandatory_bindings() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH (service)-[:DEPENDS_ON]->(dependency:Service) \
             MATCH (owner:Person)-[:OWNS]->(service) \
             RETURN service.name AS service, dependency.name AS dependency, owner.name AS owner",
    )
    .expect("MATCH after OPTIONAL MATCH should compile when it avoids optional bindings");

    assert_eq!(plan.optional_relationships, vec![0]);
    assert_eq!(
        plan.optional_matches,
        vec![OptionalMatchScope {
            node_indices: vec![1],
            relationship_indices: vec![0],
            predicate: None,
        }]
    );
    assert_eq!(
        plan.relationships,
        vec![
            RelationshipPattern {
                variable: None,
                relationship_type: "DEPENDS_ON".to_string(),
                left: "service".to_string(),
                direction: Direction::Outgoing,
                right: "dependency".to_string(),
            },
            RelationshipPattern {
                variable: None,
                relationship_type: "OWNS".to_string(),
                left: "owner".to_string(),
                direction: Direction::Outgoing,
                right: "service".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_match_after_optional_when_it_uses_optional_bindings() {
    let dependent_match = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH (service)-[:DEPENDS_ON]->(target:Service) \
             MATCH (target)-[:DEPENDS_ON]->(next:Service) \
             RETURN service.name AS service, target.name AS target, next.name AS next",
    )
    .expect("MATCH after OPTIONAL MATCH should compile when it depends on optional bindings");

    assert_eq!(dependent_match.optional_relationships, vec![0]);
    assert_eq!(
        dependent_match.optional_matches,
        vec![OptionalMatchScope {
            node_indices: vec![1],
            relationship_indices: vec![0],
            predicate: None,
        }]
    );
    assert_eq!(
        dependent_match.relationships,
        vec![
            RelationshipPattern {
                variable: None,
                relationship_type: "DEPENDS_ON".to_string(),
                left: "service".to_string(),
                direction: Direction::Outgoing,
                right: "target".to_string(),
            },
            RelationshipPattern {
                variable: None,
                relationship_type: "DEPENDS_ON".to_string(),
                left: "target".to_string(),
                direction: Direction::Outgoing,
                right: "next".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_optional_fixed_length_relationship_ranges() {
    for cypher in [
        "MATCH (source:Service) OPTIONAL MATCH (source)-[:DEPENDS_ON*2]->(target:Service) RETURN target.name",
        "MATCH (source:Service) OPTIONAL MATCH (source)-[:DEPENDS_ON*2..2]->(target:Service) RETURN target.name",
        "MATCH (source:Service) OPTIONAL MATCH (source)-[:DEPENDS_ON]->{2}(target:Service) RETURN target.name",
    ] {
        let plan = compile_cypher(cypher)
            .expect("exact positive OPTIONAL MATCH relationship range should compile");

        assert_eq!(plan.optional_relationships, vec![0, 1]);
        assert_eq!(
            plan.optional_matches,
            vec![OptionalMatchScope {
                node_indices: vec![1, 2],
                relationship_indices: vec![0, 1],
                predicate: None,
            }]
        );
    }
}

#[test]
fn compiles_optional_zero_hop_relationship_ranges_for_same_label_endpoints() {
    let plan = compile_cypher(
        "MATCH (source:Service) \
             OPTIONAL MATCH (source)-[:DEPENDS_ON*0]->(target:Service) \
             RETURN source.name AS source, target.name AS target",
    )
    .expect("same-label optional zero-hop relationship range should compile");

    assert!(plan.optional_relationships.is_empty());
    assert!(plan.optional_matches.is_empty());
    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::KeyComparison(KeyPredicate {
            variable: "source".to_string(),
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Key {
                variable: "target".to_string(),
            },
        }))
    );
}

#[test]
fn compiles_optional_zero_hop_relationship_ranges_with_bound_endpoints_as_row_preserving() {
    let same_label = compile_cypher(
        "MATCH (source:Service), (target:Service) \
             OPTIONAL MATCH (source)-[:DEPENDS_ON*0]->(target) \
             RETURN source.name AS source, target.name AS target",
    )
    .expect("optional zero-hop with bound same-label endpoints should compile");

    assert!(
        same_label.predicate.is_none(),
        "already-bound optional zero-hop endpoints must not filter rows: {:?}",
        same_label.predicate
    );

    let cross_label = compile_cypher(
        "MATCH (source:Service), (person:Person) \
             OPTIONAL MATCH (source)-[:DEPENDS_ON*0]->(person) \
             RETURN source.name AS source, person.name AS person",
    )
    .expect("optional zero-hop with bound cross-label endpoints should compile");

    assert!(
        cross_label.predicate.is_none(),
        "already-bound optional zero-hop cross-label endpoints must not filter rows: {:?}",
        cross_label.predicate
    );
}

#[test]
fn rejects_optional_zero_hop_relationship_ranges_requiring_nullable_bindings() {
    assert_unsupported(
        "MATCH (person:Person) \
             OPTIONAL MATCH (person)-[:OWNS*0]->(service:Service) \
             RETURN service.name",
    );
}

#[test]
fn compiles_optional_match_local_predicates() {
    let plan = compile_cypher(
            "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person {active: true})-[owns:OWNS {source: 'pagerduty'}]->(service) \
             WHERE person.team = service.team AND id(owns) > 10 \
             RETURN service.name AS service, person.name AS owner",
        )
        .expect("OPTIONAL MATCH predicates should compile");

    assert_eq!(plan.optional_relationships, vec![0]);
    assert_eq!(plan.predicates, Vec::new());
    assert_eq!(plan.optional_matches.len(), 1);
    let optional_match = plan
        .optional_matches
        .first()
        .expect("optional match scope should be present");
    assert_eq!(optional_match.relationship_indices, vec![0]);
    assert!(matches!(
        &optional_match.predicate,
        Some(PredicateExpression::And { .. })
    ));
}

#[test]
fn compiles_undirected_optional_match_local_predicates() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH (service)-[dependency_edge:DEPENDS_ON]-(dependency:Service) \
             WHERE dependency.tier = 'dev' \
             RETURN service.name AS service, dependency.name AS dependency",
    )
    .expect("undirected OPTIONAL MATCH predicate should compile");

    assert_eq!(plan.optional_relationships, vec![0]);
    assert_eq!(plan.predicates, Vec::new());
    assert_eq!(plan.optional_matches.len(), 1);
    let optional_match = plan
        .optional_matches
        .first()
        .expect("optional match scope should be present");
    assert_eq!(optional_match.relationship_indices, vec![0]);
    assert!(optional_match.predicate.is_some());
    let relationship = plan
        .relationships
        .first()
        .expect("optional relationship should be present");
    assert_eq!(relationship.direction, Direction::Undirected);
}

#[test]
fn compiles_relationship_endpoint_properties_on_optional_relationships() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             OPTIONAL MATCH (service)-[dependency:DEPENDS_ON]->(dependency_service:Service) \
             RETURN service.name AS service, endNode(dependency).name AS dependency \
             ORDER BY endNode(dependency).name",
    )
    .expect("relationship endpoint properties over optional relationships should compile");

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
            Projection::Expression {
                expression: ScalarExpression::PresenceGated {
                    presence_variable: "dependency".to_string(),
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "dependency_service".to_string(),
                        property: "name".to_string(),
                    })),
                },
                alias: "dependency".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::PresenceGated {
                presence_variable: "dependency".to_string(),
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "dependency_service".to_string(),
                    property: "name".to_string(),
                })),
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn rejects_unsupported_optional_match_shapes() {
    assert_unsupported("OPTIONAL MATCH (service:Service), (person:Person) RETURN service.name");
    assert_unsupported(
        "OPTIONAL MATCH (person:Person)-[:OWNS]->(service:Service)-[:DEPENDS_ON]->(target:Service) RETURN target.name",
    );
}

#[test]
fn rejects_non_transparent_with_boundaries() {
    assert_unsupported("MATCH (service:Service) WITH DISTINCT service RETURN service.name");
    assert_unsupported("MATCH (service:Service) WITH *, service.name AS name RETURN *");
    assert_unsupported(
        "MATCH (service:Service) WITH service LIMIT 1 MATCH (service)-[:DEPENDS_ON]->(target:Service) RETURN target.name",
    );
    assert_unsupported(
        "MATCH (person:Person)-[:OWNS]->(service:Service) WITH service RETURN person.name",
    );
    assert_unsupported(
        "MATCH (service:Service) WITH service, count(*) AS services MATCH (service)-[:DEPENDS_ON]->(target:Service) RETURN services, target.name",
    );
    assert_unsupported(
        "MATCH (service:Service) WITH service.name AS target MATCH (target:Service) RETURN target.name",
    );
    assert_unsupported(
        "MATCH (service:Service) WITH service, service.name AS name, service.tier AS name MATCH (service)-[:DEPENDS_ON]->(target:Service) RETURN name, target.name",
    );
    assert_unsupported(
        "MATCH (service:Service) WITH *, service AS copy MATCH (service)-[:DEPENDS_ON]->(target:Service) RETURN copy.name",
    );
    assert_unsupported(
        "MATCH (service:Service) WITH *, service.name AS service MATCH (service)-[:DEPENDS_ON]->(target:Service) RETURN service",
    );
    assert_unsupported(
        "MATCH (service:Service) WITH *, count(*) AS services MATCH (service)-[:DEPENDS_ON]->(target:Service) RETURN services, target.name",
    );
    assert_unsupported(
        "MATCH path = (person:Person)-[:OWNS]->(service:Service) WITH *, path AS p MATCH (service)-[:DEPENDS_ON]->(target:Service) RETURN p",
    );
}

#[test]
fn rejects_terminal_with_projection_boundaries_requiring_staging() {
    assert_unsupported("MATCH (service:Service) WITH service.name RETURN service.name");
    assert_unsupported("MATCH (service:Service) WITH service AS renamed RETURN renamed");
    assert_unsupported("MATCH (service:Service) WITH service.name AS service RETURN missing");
    assert_unsupported(
        "MATCH (service:Service) WITH service.name AS value, service.tier AS value RETURN value",
    );
    assert_unsupported(
        "MATCH (service:Service) WITH service.name AS name, service.tier AS tier RETURN name, name",
    );
    assert_unsupported(
        "MATCH (service:Service) WITH service.name AS name, service.tier AS tier RETURN name",
    );
    assert_unsupported(
        "MATCH (service:Service) WITH service.name AS service MATCH (service)-[:DEPENDS_ON]->(target:Service) RETURN service, target.name",
    );
    assert_unsupported(
        "MATCH (service:Service) WITH service.name AS service RETURN service ORDER BY service.name",
    );
    assert_unsupported("MATCH (service:Service) WITH service.name AS service RETURN *, service");
    assert_unsupported(
        "MATCH (service:Service) WITH service.name AS service ORDER BY service RETURN service ORDER BY service",
    );
}
