use super::*;

#[test]
fn compiles_order_by_null_placement() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN service.name AS service, service.tier AS tier \
             ORDER BY service.tier ASC NULLS LAST, service.name DESC NULLS FIRST \
             LIMIT 5",
    )
    .expect("query should compile");

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
    assert_eq!(plan.limit, Some(5));
}

#[test]
fn compiles_union_query() {
    let query = compile_cypher_query(
        "MATCH (service:Service) \
             WHERE service.tier = 'prod' \
             RETURN service.name AS item \
             UNION \
             MATCH (person:Person) \
             WHERE person.team = 'platform' \
             RETURN person.name AS item",
    )
    .expect("UNION query should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected union query");
    };
    assert_eq!(projection_names(&union.first), vec!["item".to_string()]);
    assert_eq!(union.branches.len(), 1);
    let branch = union.branches.first().expect("union branch should exist");
    assert!(!branch.all);
    assert_eq!(projection_names(&branch.plan), vec!["item".to_string()]);
}

#[test]
fn compiles_union_all_query() {
    let query = compile_cypher_query(
        "MATCH (service:Service) RETURN service.tier AS tier \
             UNION ALL \
             MATCH (service:Service) RETURN service.tier AS tier",
    )
    .expect("UNION ALL query should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected union query");
    };
    assert_eq!(union.branches.len(), 1);
    let branch = union.branches.first().expect("union branch should exist");
    assert!(branch.all);
}

#[test]
fn compiles_transparent_with_pass_through() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WITH service \
             MATCH (service)-[:DEPENDS_ON]->(target:Service) \
             RETURN service.name AS source, target.name AS target \
             ORDER BY source, target",
    )
    .expect("transparent WITH query should compile");

    assert_eq!(
        plan.nodes,
        vec![
            NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "target".to_string(),
                label: "Service".to_string(),
            },
        ]
    );
    assert_eq!(plan.relationships.len(), 1);
    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("source".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "target".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("target".to_string()),
            },
        ]
    );
}

#[test]
fn compiles_transparent_with_variable_aliases() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WITH service AS s \
             WHERE s.tier = 'prod' \
             MATCH (s)-[:DEPENDS_ON]->(target:Service) \
             RETURN s.name AS source, target.name AS target",
    )
    .expect("transparent WITH aliases should compile");

    assert_eq!(
        plan.nodes,
        vec![
            NodePattern {
                variable: "s".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "target".to_string(),
                label: "Service".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "DEPENDS_ON".to_string(),
            left: "s".to_string(),
            direction: Direction::Outgoing,
            right: "target".to_string(),
        }]
    );
    assert_eq!(
        plan.predicates,
        vec![PropertyPredicate {
            property: PropertyRef {
                variable: "s".to_string(),
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
                    variable: "s".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("source".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "target".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("target".to_string()),
            },
        ]
    );
}

#[test]
fn compiles_nonterminal_with_scalar_aliases() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WITH service, service.name AS source_name \
             MATCH (service)-[:DEPENDS_ON]->(target:Service) \
             RETURN source_name, target.name AS target \
             ORDER BY source_name, target",
    )
    .expect("non-terminal WITH scalar aliases should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("source_name".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "target".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("target".to_string()),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "target".to_string(),
                    property: "name".to_string(),
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_nonterminal_with_scalar_aliases_from_dropped_graph_variables() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[:OWNS]->(service:Service) \
             WITH service, person.name AS owner \
             MATCH (service)-[:DEPENDS_ON]->(target:Service) \
             RETURN owner, target.name AS target",
    )
    .expect("WITH scalar aliases may preserve dropped graph-variable values");

    let owner = plan
        .projections
        .first()
        .expect("owner projection should exist");
    let Projection::Property {
        property,
        alias: Some(alias),
    } = owner
    else {
        panic!("expected owner property projection, got {owner:?}");
    };
    assert_eq!(alias, "owner");
    assert_eq!(property.property, "name");
    assert!(
        property.variable.starts_with("__coral_hidden_person"),
        "{property:?}"
    );
}

#[test]
fn compiles_nonterminal_with_star_scalar_aliases() {
    let plan = compile_cypher(
        "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             WITH *, service.name AS source_name, length(path) AS hops \
             WHERE source_name STARTS WITH 'billing' AND hops = 1 \
             MATCH (service)-[:DEPENDS_ON]->(target:Service) \
             RETURN source_name, hops, target.name AS target \
             ORDER BY hops, source_name, target",
    )
    .expect("WITH * scalar aliases should compile before later MATCH");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("source_name".to_string()),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Integer(1)),
                alias: "hops".to_string(),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "target".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("target".to_string()),
            },
        ]
    );
    assert_eq!(
        plan.order_by
            .iter()
            .map(|key| &key.expression)
            .collect::<Vec<_>>(),
        vec![
            &OrderExpression::ProjectionAlias("hops".to_string()),
            &OrderExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            }),
            &OrderExpression::Property(PropertyRef {
                variable: "target".to_string(),
                property: "name".to_string(),
            }),
        ]
    );
}

#[test]
fn compiles_transparent_with_relationship_variable_aliases() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WITH person AS p, owns AS rel, service AS s \
             RETURN p.name AS owner, type(rel) AS relationship_type, s.name AS service",
    )
    .expect("transparent WITH relationship aliases should compile");

    assert_eq!(
        plan.nodes,
        vec![
            NodePattern {
                variable: "p".to_string(),
                label: "Person".to_string(),
            },
            NodePattern {
                variable: "s".to_string(),
                label: "Service".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.relationships,
        vec![RelationshipPattern {
            variable: Some("rel".to_string()),
            relationship_type: "OWNS".to_string(),
            left: "p".to_string(),
            direction: Direction::Outgoing,
            right: "s".to_string(),
        }]
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "p".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("owner".to_string()),
            },
            Projection::RelationshipType {
                variable: "rel".to_string(),
                relationship_type: "OWNS".to_string(),
                alias: "relationship_type".to_string(),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "s".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("service".to_string()),
            },
        ]
    );
}

#[test]
fn compiles_transparent_with_dropped_variables() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[:OWNS]->(service:Service) \
             WITH service \
             MATCH (service)-[:DEPENDS_ON]->(target:Service) \
             RETURN service.name AS source, target.name AS target",
    )
    .expect("transparent WITH should allow dropping graph variables");

    assert_eq!(plan.nodes.len(), 3);
    assert!(
        plan.nodes
            .iter()
            .any(|node| node.variable.starts_with("__coral_hidden_person")),
        "{:?}",
        plan.nodes
    );
    assert!(
        plan.relationships
            .first()
            .is_some_and(|relationship| relationship.left.starts_with("__coral_hidden_person")),
        "{:?}",
        plan.relationships
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("source".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "target".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("target".to_string()),
            },
        ]
    );
}

#[test]
fn compiles_transparent_with_rebound_dropped_variable_name() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[:OWNS]->(service:Service) \
             WITH service \
             MATCH (person:Person)-[:OWNS]->(service) \
             RETURN person.name AS owner, service.name AS service",
    )
    .expect("dropped variable names should be reusable after transparent WITH");

    assert!(
        plan.nodes
            .iter()
            .any(|node| node.variable.starts_with("__coral_hidden_person")),
        "{:?}",
        plan.nodes
    );
    assert!(plan.nodes.iter().any(|node| node.variable == "person"));
    assert_eq!(plan.relationships.len(), 2);
}

#[test]
fn compiles_transparent_with_star_pass_through() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WITH * \
             MATCH (service)-[:DEPENDS_ON]->(target:Service) \
             RETURN service.name AS source, target.name AS target",
    )
    .expect("transparent WITH * query should compile");

    assert_eq!(plan.nodes.len(), 2);
    assert_eq!(plan.relationships.len(), 1);
    assert_eq!(plan.projections.len(), 2);
}

#[test]
fn compiles_transparent_with_before_return() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WITH service \
             RETURN service.name AS service \
             ORDER BY service",
    )
    .expect("transparent WITH before RETURN should compile");

    assert_eq!(plan.nodes.len(), 1);
    assert_eq!(plan.relationships.len(), 0);
    assert_eq!(plan.projections.len(), 1);
}

#[test]
fn compiles_multiple_match_clauses() {
    let plan = compile_cypher(
        "MATCH (person:Person) \
             WHERE person.team = 'platform' \
             MATCH (person)-[:OWNS]->(service:Service) \
             WHERE service.tier = 'prod' \
             RETURN person.name AS owner, service.name AS service",
    )
    .expect("multiple MATCH clauses should compile");

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
    assert_eq!(plan.relationships.len(), 1);
    assert_eq!(plan.predicates.len(), 2);
}

#[test]
fn compiles_terminal_with_projection_aliases() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[:OWNS]->(service:Service) \
             WITH person.name AS owner, count(service) AS services \
             RETURN owner, services \
             ORDER BY services DESC, owner \
             LIMIT 10",
    )
    .expect("terminal WITH projection query should compile");

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
            Projection::Aggregate {
                function: AggregateFunction::Count,
                target: AggregateTarget::VariableKey {
                    variable: "service".to_string(),
                },
                distinct: false,
                alias: "services".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::ProjectionAlias("services".to_string()),
                direction: OrderDirection::Descending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "person".to_string(),
                    property: "name".to_string(),
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
    assert_eq!(plan.limit, Some(10));
}

#[test]
fn compiles_terminal_with_final_return_aliases() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WITH service.tier AS tier, count(service) AS services \
             RETURN tier AS service_tier, services AS total_services \
             ORDER BY total_services DESC, service_tier",
    )
    .expect("terminal WITH final RETURN aliases should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                alias: Some("service_tier".to_string()),
            },
            Projection::Aggregate {
                function: AggregateFunction::Count,
                target: AggregateTarget::VariableKey {
                    variable: "service".to_string(),
                },
                distinct: false,
                alias: "total_services".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::ProjectionAlias("total_services".to_string()),
                direction: OrderDirection::Descending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_terminal_with_reordered_final_return_aliases() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WITH service.tier AS tier, count(service) AS services \
             RETURN services AS total_services, tier AS service_tier \
             ORDER BY total_services DESC, service_tier",
    )
    .expect("terminal WITH final RETURN aliases should reorder projections");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Aggregate {
                function: AggregateFunction::Count,
                target: AggregateTarget::VariableKey {
                    variable: "service".to_string(),
                },
                distinct: false,
                alias: "total_services".to_string(),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                alias: Some("service_tier".to_string()),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::ProjectionAlias("total_services".to_string()),
                direction: OrderDirection::Descending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_terminal_with_return_star_alias_passthrough() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WITH service.tier AS tier, count(service) AS services \
             RETURN * \
             ORDER BY services DESC, tier",
    )
    .expect("terminal WITH RETURN * should pass through scalar aliases");

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
            Projection::Aggregate {
                function: AggregateFunction::Count,
                target: AggregateTarget::VariableKey {
                    variable: "service".to_string(),
                },
                distinct: false,
                alias: "services".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::ProjectionAlias("services".to_string()),
                direction: OrderDirection::Descending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_terminal_with_star_and_explicit_projection_aliases() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WITH *, service.name AS name, service.tier AS tier \
             RETURN tier AS service_tier, name AS service_name \
             ORDER BY service_name",
    )
    .expect("terminal WITH * explicit projection aliases should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                alias: Some("service_tier".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("service_name".to_string()),
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
fn compiles_terminal_with_star_return_star_and_explicit_projection_aliases() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             WITH *, service.name AS name \
             RETURN * \
             ORDER BY name",
    )
    .expect("terminal WITH * RETURN * explicit aliases should compile");

    assert_eq!(
        plan.projection_output_names(),
        vec![
            "service.__id",
            "service.__labels",
            "service.name",
            "service.tier",
            "name",
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
fn rejects_terminal_with_star_return_star_with_aggregate_aliases() {
    let graph = star_test_graph();
    let error = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             WITH *, count(service) AS services \
             RETURN *",
    )
    .expect_err("terminal WITH * RETURN * aggregate aliases require grouping");

    assert!(
        error.to_string().contains("aggregate aliases"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_terminal_with_star_return_star_duplicate_aliases() {
    let graph = star_test_graph();
    let error = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             WITH *, service.name AS `service.name` \
             RETURN *",
    )
    .expect_err("terminal WITH * RETURN * duplicate aliases should fail");

    assert!(
        error.to_string().contains("duplicate output column"),
        "unexpected error: {error}"
    );
}

#[test]
fn compiles_terminal_with_distinct_property_projection() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WITH DISTINCT service.tier AS tier \
             RETURN tier \
             ORDER BY tier",
    )
    .expect("terminal WITH DISTINCT projection query should compile");

    assert!(plan.distinct);
    assert_eq!(
        plan.projections,
        vec![Projection::Property {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            },
            alias: Some("tier".to_string()),
        }]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_terminal_with_order_skip_limit() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WITH service.tier AS tier, count(service) AS services \
             ORDER BY services DESC, tier \
             SKIP 1 \
             LIMIT 5 \
             RETURN tier, services",
    )
    .expect("terminal WITH modifiers should compile");

    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::ProjectionAlias("services".to_string()),
                direction: OrderDirection::Descending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
    assert_eq!(plan.skip, Some(1));
    assert_eq!(plan.limit, Some(5));
}

#[test]
fn compiles_terminal_with_graph_variable_modifiers() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WITH service AS s \
             ORDER BY s.risk DESC \
             SKIP 1 \
             LIMIT 2 \
             RETURN s.name AS service, s.risk AS risk",
    )
    .expect("terminal WITH graph variable modifiers should compile");

    assert_eq!(
        plan.nodes,
        vec![NodePattern {
            variable: "s".to_string(),
            label: "Service".to_string(),
        }]
    );
    assert_eq!(plan.predicates, Vec::new());
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "s".to_string(),
                property: "risk".to_string(),
            }),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
    assert_eq!(plan.skip, Some(1));
    assert_eq!(plan.limit, Some(2));
    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "s".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("service".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "s".to_string(),
                    property: "risk".to_string(),
                },
                alias: Some("risk".to_string()),
            },
        ]
    );
}

#[test]
fn compiles_terminal_with_distinct_graph_variable_return() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (:Person)-[:OWNS]->(target:Service) \
             WITH DISTINCT target AS t \
             ORDER BY t.name \
             RETURN t",
    )
    .expect("terminal WITH DISTINCT graph variable return should compile");

    assert!(plan.distinct);
    assert_eq!(plan.nodes.len(), 2);
    assert!(
        plan.nodes
            .iter()
            .any(|node| { node.variable.starts_with("__coral_hidden_") && node.label == "Person" })
    );
    assert!(
        plan.nodes
            .iter()
            .any(|node| node.variable == "t" && node.label == "Service")
    );
    assert_eq!(
        plan.projection_output_names(),
        vec!["t.__id", "t.__labels", "t.name", "t.tier"]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "t".to_string(),
                property: "name".to_string(),
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn rejects_terminal_with_distinct_graph_variable_scalar_return() {
    let error = compile_cypher(
        "MATCH (:Service)-[:DEPENDS_ON]->(target:Service) \
             WITH DISTINCT target \
             RETURN target.name AS target",
    )
    .expect_err("scalar projection after graph-variable WITH DISTINCT should be rejected");

    assert!(
        error
            .to_string()
            .contains("scalar projections require staged query planning"),
        "{error}"
    );
}

#[test]
fn compiles_terminal_with_star_modifiers() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WITH * \
             ORDER BY service.risk DESC \
             LIMIT 1 \
             RETURN service.name AS service",
    )
    .expect("terminal WITH * modifiers should compile");

    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "risk".to_string(),
            }),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
    assert_eq!(plan.limit, Some(1));
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
fn compiles_literal_projections() {
    let parameters = BTreeMap::from([(
        "kind".to_string(),
        CypherParameterValue::Literal(Literal::String("service".to_string())),
    )]);
    let plan = compile_cypher_with_parameters(
        "MATCH (service:Service) \
             RETURN $kind AS kind, 1 AS version, true AS enabled, null AS missing, -1.5 AS score \
             ORDER BY 'constant'",
        &parameters,
    )
    .expect("literal projections should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Literal {
                literal: Literal::String("service".to_string()),
                alias: "kind".to_string(),
            },
            Projection::Literal {
                literal: Literal::Integer(1),
                alias: "version".to_string(),
            },
            Projection::Literal {
                literal: Literal::Boolean(true),
                alias: "enabled".to_string(),
            },
            Projection::Literal {
                literal: Literal::Null,
                alias: "missing".to_string(),
            },
            Projection::Literal {
                literal: Literal::Float(OrderedFloat(-1.5)),
                alias: "score".to_string(),
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
fn compiles_literal_list_projections() {
    let parameters = BTreeMap::from([(
        "selected_tiers".to_string(),
        CypherParameterValue::List(vec![Literal::String("prod".to_string()), Literal::Null]),
    )]);
    let plan = compile_cypher_with_parameters(
        "MATCH (service:Service) \
             RETURN ['prod', 'dev'] AS tiers, $selected_tiers AS selected_tiers",
        &parameters,
    )
    .expect("literal list projections should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::LiteralList {
                literals: vec![
                    Literal::String("prod".to_string()),
                    Literal::String("dev".to_string()),
                ],
                alias: "tiers".to_string(),
            },
            Projection::LiteralList {
                literals: vec![Literal::String("prod".to_string()), Literal::Null,],
                alias: "selected_tiers".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_static_range_list_projections() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN range(1, 3) AS forward, range(3, 1, -1) AS backward, range(3, 1) AS empty",
    )
    .expect("static range list projections should compile");

    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: forward,
                    element_type: LiteralListElementType::Integer,
                },
                alias: forward_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: backward,
                    element_type: LiteralListElementType::Integer,
                },
                alias: backward_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: empty,
                    element_type: LiteralListElementType::Integer,
                },
                alias: empty_alias,
            },
        ] if forward_alias == "forward"
            && forward == &vec![Literal::Integer(1), Literal::Integer(2), Literal::Integer(3)]
            && backward_alias == "backward"
            && backward == &vec![Literal::Integer(3), Literal::Integer(2), Literal::Integer(1)]
            && empty_alias == "empty"
            && empty.is_empty()
    ));
}

#[test]
fn compiles_static_list_expressions_as_direct_order_keys() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN service.name AS name \
             ORDER BY range(1, 2), split('prod,dev', ','), toStringList([1, 2]) DESC",
    )
    .expect("folded static list expressions should compile as direct order keys");

    assert!(matches!(
        plan.order_by.as_slice(),
        [
            OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::TypedLiteralList {
                    literals: range,
                    element_type: LiteralListElementType::Integer,
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::TypedLiteralList {
                    literals: split,
                    element_type: LiteralListElementType::String,
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::TypedLiteralList {
                    literals: cast,
                    element_type: LiteralListElementType::String,
                }),
                direction: OrderDirection::Descending,
                nulls: None,
            },
        ] if range == &vec![Literal::Integer(1), Literal::Integer(2)]
            && split == &vec![
                Literal::String("prod".to_string()),
                Literal::String("dev".to_string()),
            ]
            && cast == &vec![
                Literal::String("1".to_string()),
                Literal::String("2".to_string()),
            ]
    ));
}

#[test]
fn compiles_static_split_list_projections() {
    let parameters = BTreeMap::from([(
        "tiers".to_string(),
        CypherParameterValue::Literal(Literal::String("prod|dev".to_string())),
    )]);
    let plan = compile_cypher_with_parameters(
        "MATCH (service:Service) \
             RETURN split('prod,dev', ',') AS literal_tiers, \
                    split($tiers, '|') AS parameter_tiers",
        &parameters,
    )
    .expect("static split list projections should compile");

    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: literal_tiers,
                    element_type: LiteralListElementType::String,
                },
                alias: literal_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: parameter_tiers,
                    element_type: LiteralListElementType::String,
                },
                alias: parameter_alias,
            },
        ] if literal_alias == "literal_tiers"
            && literal_tiers == &vec![
                Literal::String("prod".to_string()),
                Literal::String("dev".to_string()),
            ]
            && parameter_alias == "parameter_tiers"
            && parameter_tiers == &vec![
                Literal::String("prod".to_string()),
                Literal::String("dev".to_string()),
            ]
    ));
}

#[test]
fn compiles_coalesce_projection() {
    let parameters = BTreeMap::from([(
        "fallback".to_string(),
        CypherParameterValue::Literal(Literal::String("unassigned".to_string())),
    )]);
    let plan = compile_cypher_with_parameters(
        "MATCH (service:Service) \
             RETURN coalesce(service.team, service.tier, $fallback) AS owner_team",
        &parameters,
    )
    .expect("coalesce projection should compile");

    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::Coalesce {
                expressions: vec![
                    ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "team".to_string(),
                    }),
                    ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    }),
                    ScalarExpression::Literal(Literal::String("unassigned".to_string())),
                ],
            },
            alias: "owner_team".to_string(),
        }]
    );
}

#[test]
fn compiles_static_list_coalesce_projection_and_ordering() {
    let query = compile_cypher_query_for_graph(
        &star_test_graph(),
        "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN coalesce(keys(person), []) AS owner_keys, \
                    coalesce(null, labels(service)) AS service_labels \
             ORDER BY coalesce(keys(person), ['missing'])",
    )
    .expect("static list coalesce should compile with graph metadata");

    let GraphQuery::Plan(plan) = query else {
        panic!("expected single graph plan");
    };
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Coalesce { expressions: owner_key_args },
                alias: owner_key_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Coalesce { expressions: service_label_args },
                alias: service_label_alias,
            },
        ] if owner_key_alias == "owner_keys"
            && owner_key_args.len() == 2
            && matches!(
                owner_key_args.as_slice(),
                [
                    ScalarExpression::PresenceGated {
                        presence_variable,
                        expression,
                    },
                    ScalarExpression::TypedLiteralList {
                        literals,
                        element_type: LiteralListElementType::String,
                    },
                ] if presence_variable == "person"
                    && matches!(
                        expression.as_ref(),
                        ScalarExpression::TypedLiteralList {
                            literals,
                            element_type: LiteralListElementType::String,
                        } if literals == &vec![
                            Literal::String("name".to_string()),
                            Literal::String("team".to_string()),
                        ]
                    )
                    && literals.is_empty()
            )
            && service_label_alias == "service_labels"
            && matches!(
                service_label_args.as_slice(),
                [
                    ScalarExpression::Literal(Literal::Null),
                    ScalarExpression::TypedLiteralList {
                        literals,
                        element_type: LiteralListElementType::String,
                    },
                ] if literals == &vec![Literal::String("Service".to_string())]
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
fn compiles_order_by_coalesce_expression() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN service.name AS service \
             ORDER BY coalesce(service.tier, 'unassigned') DESC",
    )
    .expect("coalesce order expression should compile");

    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Coalesce {
                expressions: vec![
                    ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    }),
                    ScalarExpression::Literal(Literal::String("unassigned".to_string())),
                ],
            }),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_static_list_case_projection_and_ordering() {
    let query = compile_cypher_query_for_graph(
            &star_test_graph(),
            "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN CASE WHEN person IS NULL THEN [] ELSE keys(person) END AS owner_keys, \
                    CASE WHEN person IS NOT NULL THEN labels(person) ELSE ['missing'] END AS owner_labels, \
                    CASE WHEN person IS NULL THEN [] ELSE coalesce(keys(person), []) END AS coalesced_keys \
             ORDER BY CASE WHEN person IS NULL THEN [] ELSE keys(person) END",
        )
        .expect("static list CASE should compile with graph metadata");

    let GraphQuery::Plan(plan) = query else {
        panic!("expected single graph plan");
    };
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Case {
                    alternatives,
                    else_expression,
                },
                alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Case { .. },
                alias: label_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Case { .. },
                alias: coalesced_alias,
            },
        ] if alias == "owner_keys"
            && matches!(
                alternatives.as_slice(),
                [ScalarCaseAlternative {
                    then: ScalarExpression::TypedLiteralList {
                        literals,
                        element_type: LiteralListElementType::String,
                    },
                    ..
                }] if literals.is_empty()
            )
            && matches!(
                else_expression.as_deref(),
                Some(ScalarExpression::PresenceGated {
                    presence_variable,
                    expression,
                }) if presence_variable == "person"
                    && matches!(
                        expression.as_ref(),
                        ScalarExpression::TypedLiteralList {
                            literals,
                            element_type: LiteralListElementType::String,
                        } if literals == &vec![
                            Literal::String("name".to_string()),
                            Literal::String("team".to_string()),
                        ]
                    )
            )
            && label_alias == "owner_labels"
            && coalesced_alias == "coalesced_keys"
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
fn rejects_ambiguous_literal_list_projections() {
    for (cypher, expected) in [
        (
            "MATCH (service:Service) RETURN [] AS values",
            "at least one element",
        ),
        (
            "MATCH (service:Service) RETURN [null] AS values",
            "at least one non-null element",
        ),
        (
            "MATCH (service:Service) RETURN [1, 'prod'] AS values",
            "all non-null elements to have the same type",
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
fn compiles_count_star_projection() {
    let plan = compile_cypher("MATCH (service:Service) RETURN count(*) AS services")
        .expect("query should compile");

    assert_eq!(
        plan.projections,
        vec![Projection::CountAll {
            alias: "services".to_string(),
        }]
    );
}

#[test]
fn compiles_return_star_with_graph_declaration() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[ownership:OWNS]->(service:Service) \
             RETURN * ORDER BY service.name",
    )
    .expect("RETURN * should expand using graph metadata");

    assert_eq!(
        plan.projection_output_names(),
        vec![
            "person.__id",
            "person.__labels",
            "person.name",
            "person.team",
            "service.__id",
            "service.__labels",
            "service.name",
            "service.tier",
            "ownership.__id",
            "ownership.__type",
            "ownership.since",
            "ownership.source",
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
fn compiles_return_star_with_explicit_projections() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[ownership:OWNS]->(service:Service) \
             RETURN *, service.tier AS tier \
             ORDER BY tier",
    )
    .expect("RETURN *, explicit projections should compile");

    assert_eq!(
        plan.projection_output_names(),
        vec![
            "person.__id",
            "person.__labels",
            "person.name",
            "person.team",
            "service.__id",
            "service.__labels",
            "service.name",
            "service.tier",
            "ownership.__id",
            "ownership.__type",
            "ownership.since",
            "ownership.source",
            "tier",
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_return_node_variable_with_graph_declaration() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) RETURN service ORDER BY service.name",
    )
    .expect("node graph variable return should expand using graph metadata");

    assert_eq!(
        plan.projection_output_names(),
        vec![
            "service.__id",
            "service.__labels",
            "service.name",
            "service.tier",
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
fn compiles_return_graph_variable_alias_prefix() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(&graph, "MATCH (service:Service) RETURN service AS svc")
        .expect("graph variable aliases should prefix expanded columns");

    assert_eq!(
        plan.projection_output_names(),
        vec!["svc.__id", "svc.__labels", "svc.name", "svc.tier"]
    );
}

#[test]
fn compiles_return_relationship_variable_with_graph_declaration() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (:Person)-[ownership:OWNS]->(:Service) RETURN ownership",
    )
    .expect("relationship graph variable return should expand using graph metadata");

    assert_eq!(
        plan.projection_output_names(),
        vec![
            "ownership.__id",
            "ownership.__type",
            "ownership.since",
            "ownership.source",
        ]
    );
}

#[test]
fn compiles_return_relationship_endpoint_graph_values_with_graph_declaration() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[ownership:OWNS]->(service:Service) \
             RETURN startNode(ownership) AS owner, endNode(ownership) AS owned",
    )
    .expect("relationship endpoint graph values should expand using graph metadata");

    assert_eq!(
        plan.projection_output_names(),
        vec![
            "owner.__id",
            "owner.__labels",
            "owner.name",
            "owner.team",
            "owned.__id",
            "owned.__labels",
            "owned.name",
            "owned.tier",
        ]
    );
    assert_eq!(
        plan.projections.first(),
        Some(&Projection::Expression {
            expression: ScalarExpression::Key {
                variable: "person".to_string(),
            },
            alias: "owner.__id".to_string(),
        })
    );
}

#[test]
fn rejects_return_graph_variable_without_graph_declaration() {
    let error = compile_cypher("MATCH (service:Service) RETURN service")
        .expect_err("declaration-free compiler cannot expand graph variables");

    assert!(
        error
            .to_string()
            .contains("graph-variable expansion requires a graph declaration"),
        "{error}"
    );
}

#[test]
fn compiles_return_star_over_keyless_relationships() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (team:Team)-[ownership:OWNS]->(service:Service) RETURN *",
    )
    .expect("RETURN * should handle keyless relationship mappings");

    assert_eq!(
        plan.projection_output_names(),
        vec![
            "team.__id",
            "team.__labels",
            "team.name",
            "service.__id",
            "service.__labels",
            "service.name",
            "service.tier",
            "ownership.__type",
            "ownership.source",
        ]
    );
}

#[test]
fn rejects_return_star_without_graph_declaration() {
    let error = compile_cypher("MATCH (service:Service) RETURN *")
        .expect_err("declaration-free compiler cannot expand RETURN *");

    assert!(
        error
            .to_string()
            .contains("graph-variable expansion requires a graph declaration"),
        "{error}"
    );
}

#[test]
fn return_star_respects_transparent_with_scope() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[ownership:OWNS]->(service:Service) \
             WITH service \
             RETURN *",
    )
    .expect("RETURN * should only expand visible variables after WITH");

    assert_eq!(
        plan.projection_output_names(),
        vec![
            "service.__id",
            "service.__labels",
            "service.name",
            "service.tier",
        ]
    );
}

#[test]
fn compiles_return_distinct() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[:OWNS]->(service:Service) \
             RETURN DISTINCT service.tier AS tier ORDER BY tier",
    )
    .expect("query should compile");

    assert!(plan.distinct);
    assert_eq!(plan.projections.len(), 1);
    assert_eq!(plan.order_by.len(), 1);
}

#[test]
fn compiles_skip_and_limit() {
    let plan = compile_cypher(
        "MATCH (service:Service) RETURN service.name AS service ORDER BY service SKIP 1 LIMIT 2",
    )
    .expect("query should compile");

    assert_eq!(plan.skip, Some(1));
    assert_eq!(plan.limit, Some(2));
}

#[test]
fn compiles_static_skip_and_limit_expressions() {
    let parameters = BTreeMap::from([(
        "limit".to_string(),
        CypherParameterValue::Literal(Literal::Integer(2)),
    )]);
    let plan = compile_cypher_with_parameters(
        "MATCH (service:Service) \
             RETURN service.name AS service \
             ORDER BY service \
             SKIP (1 + 1) \
             LIMIT coalesce($limit, 3)",
        &parameters,
    )
    .expect("static row modifier expressions should compile");

    assert_eq!(plan.skip, Some(2));
    assert_eq!(plan.limit, Some(2));
}

#[test]
fn rejects_negative_skip() {
    let error = compile_cypher("MATCH (service:Service) RETURN service.name SKIP -1")
        .expect_err("negative SKIP should fail");

    assert!(
        error.to_string().contains("UNSUPPORTED_CYPHER"),
        "{error:?}"
    );
}

#[test]
fn compiles_grouped_count_projection() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN service.tier AS tier, count(*) AS services \
             ORDER BY tier",
    )
    .expect("grouped count query should compile");

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
        ]
    );
    assert_eq!(plan.order_by.len(), 1);
}

#[test]
fn compiles_count_property_projection() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN service.tier AS tier, count(service.name) AS named_services \
             ORDER BY named_services DESC",
    )
    .expect("count property query should compile");

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
            Projection::Aggregate {
                function: super::AggregateFunction::Count,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                }),
                distinct: false,
                alias: "named_services".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("named_services".to_string()),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_count_distinct_property_projection() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN count(DISTINCT service.tier) AS tier_count",
    )
    .expect("count distinct property query should compile");

    assert_eq!(
        plan.projections,
        vec![Projection::Aggregate {
            function: super::AggregateFunction::Count,
            target: AggregateTarget::Property(PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            }),
            distinct: true,
            alias: "tier_count".to_string(),
        }]
    );
}

#[test]
fn compiles_collect_property_projection() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[:OWNS]->(service:Service) \
             RETURN person.team AS team, collect(DISTINCT service.name) AS services \
             ORDER BY services",
    )
    .expect("collect property query should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "person".to_string(),
                    property: "team".to_string(),
                },
                alias: Some("team".to_string()),
            },
            Projection::Aggregate {
                function: super::AggregateFunction::Collect,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                }),
                distinct: true,
                alias: "services".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("services".to_string()),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_count_node_projection() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN count(service) AS services, count(DISTINCT service) AS distinct_services \
             ORDER BY services DESC",
    )
    .expect("count node query should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Aggregate {
                function: super::AggregateFunction::Count,
                target: AggregateTarget::VariableKey {
                    variable: "service".to_string(),
                },
                distinct: false,
                alias: "services".to_string(),
            },
            Projection::Aggregate {
                function: super::AggregateFunction::Count,
                target: AggregateTarget::VariableKey {
                    variable: "service".to_string(),
                },
                distinct: true,
                alias: "distinct_services".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("services".to_string()),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_collect_graph_variable_projection() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN collect(service) AS service_ids, \
                    collect(DISTINCT service) AS distinct_service_ids, \
                    collect(owns) AS ownership_ids",
    )
    .expect("collect graph variable query should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Aggregate {
                function: super::AggregateFunction::Collect,
                target: AggregateTarget::VariableKey {
                    variable: "service".to_string(),
                },
                distinct: false,
                alias: "service_ids".to_string(),
            },
            Projection::Aggregate {
                function: super::AggregateFunction::Collect,
                target: AggregateTarget::VariableKey {
                    variable: "service".to_string(),
                },
                distinct: true,
                alias: "distinct_service_ids".to_string(),
            },
            Projection::Aggregate {
                function: super::AggregateFunction::Collect,
                target: AggregateTarget::VariableKey {
                    variable: "owns".to_string(),
                },
                distinct: false,
                alias: "ownership_ids".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_aggregate_scalar_expression_targets() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN collect(coalesce(service.tier, 'unknown')) AS tiers, \
                    count(coalesce(service.tier, 'unknown')) AS tier_count, \
                    sum(service.risk + 1) AS adjusted_risk, \
                    collect(({tier: service.tier}).tier) AS selected_tiers, \
                    sum(({risk: service.risk + 1}).risk) AS selected_adjusted_risk, \
                    count(({kind: 'service'}).kind) AS literal_kind_count",
    )
    .expect("aggregate expression target query should compile");

    assert_eq!(plan.projections.len(), 6);
    assert!(matches!(
        plan.projections
            .first()
            .expect("collect projection should be present"),
        Projection::Aggregate {
            function: super::AggregateFunction::Collect,
            target: AggregateTarget::Expression(ScalarExpression::Coalesce { .. }),
            alias,
            ..
        } if alias == "tiers"
    ));
    assert!(matches!(
        plan.projections
            .get(1)
            .expect("count projection should be present"),
        Projection::Aggregate {
            function: super::AggregateFunction::Count,
            target: AggregateTarget::Expression(ScalarExpression::Coalesce { .. }),
            alias,
            ..
        } if alias == "tier_count"
    ));
    assert!(matches!(
        plan.projections
            .get(2)
            .expect("sum projection should be present"),
        Projection::Aggregate {
            function: super::AggregateFunction::Sum,
            target: AggregateTarget::Expression(ScalarExpression::Arithmetic { .. }),
            alias,
            ..
        } if alias == "adjusted_risk"
    ));
    assert!(matches!(
        plan.projections
            .get(3)
            .expect("selected property collect projection should be present"),
        Projection::Aggregate {
            function: super::AggregateFunction::Collect,
            target: AggregateTarget::Property(PropertyRef { property, .. }),
            alias,
            ..
        } if property == "tier" && alias == "selected_tiers"
    ));
    assert!(matches!(
        plan.projections
            .get(4)
            .expect("selected expression sum projection should be present"),
        Projection::Aggregate {
            function: super::AggregateFunction::Sum,
            target: AggregateTarget::Expression(ScalarExpression::Arithmetic { .. }),
            alias,
            ..
        } if alias == "selected_adjusted_risk"
    ));
    assert!(matches!(
        plan.projections
            .get(5)
            .expect("selected literal count projection should be present"),
        Projection::Aggregate {
            function: super::AggregateFunction::Count,
            target: AggregateTarget::Expression(ScalarExpression::Literal(
                Literal::String(value)
            )),
            alias,
            ..
        } if value == "service" && alias == "literal_kind_count"
    ));
}

#[test]
fn rejects_order_by_unknown_aliases() {
    assert_unsupported("MATCH (service:Service) RETURN service.name AS name ORDER BY missing");
}

#[test]
fn compiles_order_by_aggregate_aliases() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN count(*) AS services \
             ORDER BY services DESC",
    )
    .expect("aggregate alias ordering should compile");

    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("services".to_string()),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_order_by_count_star_expression() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN service.tier AS tier, count(*) AS services \
             ORDER BY count(*) DESC, tier",
    )
    .expect("count(*) order expression should compile");

    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::ProjectionAlias("services".to_string()),
                direction: OrderDirection::Descending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_order_by_aggregate_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN count(service) AS services, avg(service.risk) AS average_risk \
             ORDER BY count(service) DESC, avg(service.risk)",
    )
    .expect("aggregate order expressions should compile");

    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::ProjectionAlias("services".to_string()),
                direction: OrderDirection::Descending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::ProjectionAlias("average_risk".to_string()),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_unprojected_order_by_aggregate_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN service.tier AS tier \
             ORDER BY count(*) DESC, avg(service.risk), tier",
    )
    .expect("hidden aggregate order expressions should compile");

    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::CountAll,
                direction: OrderDirection::Descending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Aggregate {
                    function: AggregateFunction::Avg,
                    target: AggregateTarget::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    }),
                    distinct: false,
                },
                direction: OrderDirection::Ascending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}
