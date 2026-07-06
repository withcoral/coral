use super::*;

#[test]
fn compiles_id_and_type_projections() {
    let plan = compile_cypher(
            "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN id(person) AS person_id, id(owns) AS ownership_id, type(owns) AS relationship_type \
             ORDER BY ownership_id",
        )
        .expect("id() and type() projections should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Key {
                variable: "person".to_string(),
                alias: "person_id".to_string(),
            },
            Projection::Key {
                variable: "owns".to_string(),
                alias: "ownership_id".to_string(),
            },
            Projection::RelationshipType {
                variable: "owns".to_string(),
                relationship_type: "OWNS".to_string(),
                alias: "relationship_type".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("ownership_id".to_string()),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_relationship_endpoint_property_projections() {
    let plan = compile_cypher(
        "MATCH (source:Service)-[dependency:DEPENDS_ON]->(target:Service) \
             RETURN startNode(dependency).name AS source, endNode(dependency).name AS target \
             ORDER BY endNode(dependency).name",
    )
    .expect("relationship endpoint property projections should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "source".to_string(),
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
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "target".to_string(),
                property: "name".to_string(),
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_properties_function_property_lookups() {
    let plan = compile_cypher(
        "MATCH (source:Service)-[dependency:DEPENDS_ON]->(target:Service) \
             WHERE properties(source).tier = properties(target).tier \
             RETURN properties(source).name AS source_name, \
                    properties(startNode(dependency)).name AS start_name, \
                    properties(endNode(dependency)).tier AS end_tier \
             ORDER BY properties(target).name",
    )
    .expect("properties(variable).property lookups should compile as graph properties");

    assert_eq!(
        plan.predicates,
        vec![PropertyPredicate {
            property: PropertyRef {
                variable: "source".to_string(),
                property: "tier".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Property(PropertyRef {
                variable: "target".to_string(),
                property: "tier".to_string(),
            }),
        }]
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "source".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("source_name".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "source".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("start_name".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "target".to_string(),
                    property: "tier".to_string(),
                },
                alias: Some("end_tier".to_string()),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "target".to_string(),
                property: "name".to_string(),
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_string_property_index_lookups() {
    let parameters = BTreeMap::from([
        (
            "tier_key".to_string(),
            CypherParameterValue::Literal(Literal::String("tier".to_string())),
        ),
        (
            "order_key".to_string(),
            CypherParameterValue::Literal(Literal::String("name".to_string())),
        ),
    ]);
    let plan = compile_cypher_with_parameters(
        "MATCH (source:Service)-[dependency:DEPENDS_ON]->(target:Service) \
             WHERE source['tier'] = properties(target)[$tier_key] \
             RETURN source['name'] AS source_name, \
                    properties(startNode(dependency))['name'] AS start_name, \
                    properties(endNode(dependency))['tier'] AS end_tier \
             ORDER BY target[$order_key]",
        &parameters,
    )
    .expect("string property index lookups should compile as graph properties");

    assert_eq!(
        plan.predicates,
        vec![PropertyPredicate {
            property: PropertyRef {
                variable: "source".to_string(),
                property: "tier".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Property(PropertyRef {
                variable: "target".to_string(),
                property: "tier".to_string(),
            }),
        }]
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "source".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("source_name".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "source".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("start_name".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "target".to_string(),
                    property: "tier".to_string(),
                },
                alias: Some("end_tier".to_string()),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "target".to_string(),
                property: "name".to_string(),
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn rejects_non_string_property_indexes() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             RETURN service[0] AS service_name",
    )
    .expect_err("property indexes should require static string keys");

    assert!(
        error
            .to_string()
            .contains("property index lookups require a string literal"),
        "{error}"
    );
}

#[test]
fn compiles_reversed_relationship_endpoint_property_projections() {
    let plan = compile_cypher(
        "MATCH (target:Service)<-[dependency:DEPENDS_ON]-(source:Service) \
             RETURN startNode(dependency).name AS source, endNode(dependency).name AS target",
    )
    .expect("reversed relationship endpoint property projections should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "source".to_string(),
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
fn compiles_relationship_endpoint_properties_in_predicates_and_scalars() {
    let plan = compile_cypher(
        "MATCH (source:Service)-[dependency:DEPENDS_ON]->(target:Service) \
             WHERE startNode(dependency).tier = 'prod' \
             RETURN lower(endNode(dependency).name) AS target \
             ORDER BY endNode(dependency).name",
    )
    .expect("relationship endpoint property scalar expressions should compile");

    assert_eq!(
        plan.predicates,
        vec![PropertyPredicate {
            property: PropertyRef {
                variable: "source".to_string(),
                property: "tier".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
        }]
    );
    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::ToLower {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "target".to_string(),
                    property: "name".to_string(),
                })),
            },
            alias: "target".to_string(),
        }]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "target".to_string(),
                property: "name".to_string(),
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_relationship_endpoint_property_aggregates() {
    let plan = compile_cypher(
        "MATCH (source:Service)-[dependency:DEPENDS_ON]->(target:Service) \
             RETURN count(startNode(dependency).name) AS sources",
    )
    .expect("relationship endpoint aggregate target should compile");

    assert_eq!(
        plan.projections,
        vec![Projection::Aggregate {
            function: AggregateFunction::Count,
            target: AggregateTarget::Property(PropertyRef {
                variable: "source".to_string(),
                property: "name".to_string(),
            }),
            distinct: false,
            alias: "sources".to_string(),
        }]
    );
}

#[test]
fn compiles_undirected_cross_label_relationship_endpoint_properties() {
    let graph = star_test_graph();
    for cypher in [
        "MATCH (person:Person)-[owns:OWNS]-(service:Service) \
             RETURN startNode(owns).name AS owner, endNode(owns).name AS service \
             ORDER BY startNode(owns).name",
        "MATCH (service:Service)-[owns:OWNS]-(person:Person) \
             RETURN startNode(owns).name AS owner, endNode(owns).name AS service \
             ORDER BY startNode(owns).name",
    ] {
        let plan = compile_cypher_for_graph(&graph, cypher)
            .expect("undirected cross-label endpoint properties should compile");

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
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("service".to_string()),
                },
            ]
        );
        assert_eq!(
            plan.order_by,
            vec![OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "person".to_string(),
                    property: "name".to_string(),
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            }]
        );
    }
}

#[test]
fn rejects_undirected_cross_label_endpoint_properties_without_graph_context() {
    let error = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]-(service:Service) \
             RETURN startNode(owns).name AS owner",
    )
    .expect_err("undirected cross-label endpoint functions require graph context");

    assert!(
        error.to_string().contains(
            "startNode() over cross-label undirected relationships requires a graph declaration"
        ),
        "{error}"
    );
}

#[test]
fn compiles_relationship_endpoint_identity_projections() {
    let plan = compile_cypher(
        "MATCH (source:Service)-[dependency:DEPENDS_ON]->(target:Service) \
             RETURN id(startNode(dependency)) AS source_id, \
                    elementId(endNode(dependency)) AS target_element_id, \
                    labels(startNode(dependency)) AS source_labels, \
                    keys(endNode(dependency)) AS target_keys \
             ORDER BY id(startNode(dependency))",
    )
    .expect("relationship endpoint identity projections should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Key {
                variable: "source".to_string(),
                alias: "source_id".to_string(),
            },
            Projection::ElementId {
                variable: "target".to_string(),
                alias: "target_element_id".to_string(),
            },
            Projection::NodeLabels {
                variable: "source".to_string(),
                label: "Service".to_string(),
                alias: "source_labels".to_string(),
            },
            Projection::PropertyKeys {
                variable: "target".to_string(),
                alias: "target_keys".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Key {
                variable: "source".to_string(),
            },
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_relationship_endpoint_identity_aggregates() {
    let plan = compile_cypher(
        "MATCH (source:Service)-[dependency:DEPENDS_ON]->(target:Service) \
             RETURN count(endNode(dependency)) AS targets",
    )
    .expect("relationship endpoint identity aggregate should compile");

    assert_eq!(
        plan.projections,
        vec![Projection::Aggregate {
            function: AggregateFunction::Count,
            target: AggregateTarget::VariableKey {
                variable: "target".to_string(),
            },
            distinct: false,
            alias: "targets".to_string(),
        }]
    );
}

#[test]
fn compiles_relationship_endpoint_identity_predicates() {
    let plan = compile_cypher(
        "MATCH (source:Service)-[dependency:DEPENDS_ON]->(target:Service) \
             WHERE startNode(dependency) IS NOT NULL \
               AND endNode(dependency):Service \
               AND 'Service' IN labels(startNode(dependency)) \
               AND 'name' IN keys(endNode(dependency)) \
             RETURN target.name AS target",
    )
    .expect("relationship endpoint identity predicates should compile");

    assert!(matches!(
        plan.predicate,
        Some(PredicateExpression::And { .. })
    ));
}

#[test]
fn compiles_same_label_undirected_relationship_endpoint_properties() {
    let plan = compile_cypher(
        "MATCH (left:Service)-[dependency:DEPENDS_ON]-(right:Service) \
             RETURN startNode(dependency).name AS source, endNode(dependency).name AS target \
             ORDER BY startNode(dependency).name, endNode(dependency).name",
    )
    .expect("same-label undirected endpoint properties should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::UndirectedEndpointProperty {
                    relationship: "dependency".to_string(),
                    endpoint: UndirectedRelationshipEndpoint::Start,
                    property: "name".to_string(),
                },
                alias: "source".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::UndirectedEndpointProperty {
                    relationship: "dependency".to_string(),
                    endpoint: UndirectedRelationshipEndpoint::End,
                    property: "name".to_string(),
                },
                alias: "target".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::UndirectedEndpointProperty {
                    relationship: "dependency".to_string(),
                    endpoint: UndirectedRelationshipEndpoint::Start,
                    property: "name".to_string(),
                },),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::UndirectedEndpointProperty {
                    relationship: "dependency".to_string(),
                    endpoint: UndirectedRelationshipEndpoint::End,
                    property: "name".to_string(),
                },),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_same_label_undirected_relationship_endpoint_property_indexes() {
    let plan = compile_cypher(
        "MATCH (left:Service)-[dependency:DEPENDS_ON]-(right:Service) \
             RETURN startNode(dependency)['name'] AS source, \
                    properties(endNode(dependency))['tier'] AS target_tier \
             ORDER BY properties(startNode(dependency))['name'], endNode(dependency)['tier']",
    )
    .expect("same-label undirected endpoint property indexes should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::UndirectedEndpointProperty {
                    relationship: "dependency".to_string(),
                    endpoint: UndirectedRelationshipEndpoint::Start,
                    property: "name".to_string(),
                },
                alias: "source".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::UndirectedEndpointProperty {
                    relationship: "dependency".to_string(),
                    endpoint: UndirectedRelationshipEndpoint::End,
                    property: "tier".to_string(),
                },
                alias: "target_tier".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::UndirectedEndpointProperty {
                    relationship: "dependency".to_string(),
                    endpoint: UndirectedRelationshipEndpoint::Start,
                    property: "name".to_string(),
                },),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::UndirectedEndpointProperty {
                    relationship: "dependency".to_string(),
                    endpoint: UndirectedRelationshipEndpoint::End,
                    property: "tier".to_string(),
                },),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_same_label_undirected_relationship_endpoint_property_aggregates() {
    let plan = compile_cypher(
        "MATCH (left:Service)-[dependency:DEPENDS_ON]-(right:Service) \
             RETURN count(DISTINCT endNode(dependency).name) AS targets",
    )
    .expect("same-label undirected endpoint property aggregate should compile");

    assert_eq!(
        plan.projections,
        vec![Projection::Aggregate {
            function: AggregateFunction::Count,
            target: AggregateTarget::Expression(ScalarExpression::UndirectedEndpointProperty {
                relationship: "dependency".to_string(),
                endpoint: UndirectedRelationshipEndpoint::End,
                property: "name".to_string(),
            },),
            distinct: true,
            alias: "targets".to_string(),
        }]
    );
}

#[test]
fn compiles_same_label_undirected_relationship_endpoint_identity_and_metadata() {
    let plan = compile_cypher(
        "MATCH (left:Service)-[dependency:DEPENDS_ON]-(right:Service) \
             RETURN id(startNode(dependency)) AS source_id, \
                    elementId(endNode(dependency)) AS target_element_id, \
                    labels(startNode(dependency)) AS source_labels, \
                    keys(endNode(dependency)) AS target_keys \
             ORDER BY id(startNode(dependency)), elementId(endNode(dependency))",
    )
    .expect("same-label undirected endpoint identity and metadata should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::UndirectedEndpointKey {
                    relationship: "dependency".to_string(),
                    endpoint: UndirectedRelationshipEndpoint::Start,
                },
                alias: "source_id".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::UndirectedEndpointElementId {
                    relationship: "dependency".to_string(),
                    endpoint: UndirectedRelationshipEndpoint::End,
                },
                alias: "target_element_id".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::UndirectedEndpointLabels {
                    relationship: "dependency".to_string(),
                    endpoint: UndirectedRelationshipEndpoint::Start,
                    label: "Service".to_string(),
                },
                alias: "source_labels".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::UndirectedEndpointPropertyKeys {
                    relationship: "dependency".to_string(),
                    endpoint: UndirectedRelationshipEndpoint::End,
                },
                alias: "target_keys".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::UndirectedEndpointKey {
                    relationship: "dependency".to_string(),
                    endpoint: UndirectedRelationshipEndpoint::Start,
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Scalar(
                    ScalarExpression::UndirectedEndpointElementId {
                        relationship: "dependency".to_string(),
                        endpoint: UndirectedRelationshipEndpoint::End,
                    },
                ),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn rejects_relationship_endpoint_properties_on_node_variables() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             RETURN startNode(service).name AS source",
    )
    .expect_err("relationship endpoint functions should require relationship variables");

    assert!(
        error
            .to_string()
            .contains("startNode() argument 'service' is not a named relationship variable"),
        "{error}"
    );
}

#[test]
fn compiles_element_id_projections() {
    let plan = compile_cypher(
            "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN elementId(person) AS person_element_id, elementId(owns) AS ownership_element_id \
             ORDER BY ownership_element_id",
        )
        .expect("elementId() projections should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::ElementId {
                variable: "person".to_string(),
                alias: "person_element_id".to_string(),
            },
            Projection::ElementId {
                variable: "owns".to_string(),
                alias: "ownership_element_id".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("ownership_element_id".to_string()),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_labels_projection() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN labels(service) AS service_labels \
             ORDER BY service_labels",
    )
    .expect("labels() projection should compile");

    assert_eq!(
        plan.projections,
        vec![Projection::NodeLabels {
            variable: "service".to_string(),
            label: "Service".to_string(),
            alias: "service_labels".to_string(),
        }]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("service_labels".to_string()),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_labels_null_projection() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN labels(null) AS null_labels",
    )
    .expect("labels(null) projection should compile");

    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::Literal(Literal::Null),
            alias: "null_labels".to_string(),
        }]
    );
}

#[test]
fn compiles_order_by_id_and_type_functions() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN person.name AS owner \
             ORDER BY id(person), id(owns) DESC, type(owns)",
    )
    .expect("id() and type() order expressions should compile");

    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::Key {
                    variable: "person".to_string(),
                },
                direction: OrderDirection::Ascending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Key {
                    variable: "owns".to_string(),
                },
                direction: OrderDirection::Descending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::RelationshipType {
                    variable: "owns".to_string(),
                    relationship_type: "OWNS".to_string(),
                },
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_order_by_element_id_function() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN person.name AS owner \
             ORDER BY elementId(person), elementId(owns) DESC",
    )
    .expect("elementId() order expressions should compile");

    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::ElementId {
                    variable: "person".to_string(),
                },
                direction: OrderDirection::Ascending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::ElementId {
                    variable: "owns".to_string(),
                },
                direction: OrderDirection::Descending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_order_by_labels_function() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN service.name AS service \
             ORDER BY labels(service) DESC",
    )
    .expect("labels() order expression should compile");

    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::NodeLabels {
                variable: "service".to_string(),
                label: "Service".to_string(),
            },
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_order_by_keys_function() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN service.name AS service \
             ORDER BY keys(service) DESC, keys(owns)",
    )
    .expect("keys() order expressions should compile");

    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::PropertyKeys {
                    variable: "service".to_string(),
                },
                direction: OrderDirection::Descending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::PropertyKeys {
                    variable: "owns".to_string(),
                },
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn rejects_labels_on_relationship_variables() {
    let error = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN labels(owns) AS labels",
    )
    .expect_err("labels() should require a node variable");

    assert!(
        error.to_string().contains("labels() argument 'owns'"),
        "{error:?}"
    );
}

#[test]
fn compiles_keys_projection() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN keys(person) AS person_keys, keys(owns) AS ownership_keys",
    )
    .expect("keys() projections should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::PropertyKeys {
                variable: "person".to_string(),
                alias: "person_keys".to_string(),
            },
            Projection::PropertyKeys {
                variable: "owns".to_string(),
                alias: "ownership_keys".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_static_map_keys_as_list_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE service.name IN keys({name: service.name, tier: service.tier}) \
             RETURN keys({name: service.name, tier: service.tier}) AS map_keys, \
                    head(keys({first: 1, second: 2})) AS first_key \
             ORDER BY keys({zeta: 0, alpha: 1})",
    )
    .expect("literal map keys should compile as static list expressions");

    assert_eq!(
        plan.predicates,
        vec![PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            },
            operator: ComparisonOperator::In,
            rhs: PredicateRhs::List(vec![
                Literal::String("name".to_string()),
                Literal::String("tier".to_string()),
            ]),
        }]
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("name".to_string()),
                        Literal::String("tier".to_string()),
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "map_keys".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::String("first".to_string())),
                alias: "first_key".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::TypedLiteralList {
                literals: vec![
                    Literal::String("zeta".to_string()),
                    Literal::String("alpha".to_string()),
                ],
                element_type: LiteralListElementType::String,
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_property_key_membership_predicates() {
    let parameters = BTreeMap::from([(
        "relationship_key".to_string(),
        CypherParameterValue::Literal(Literal::String("since".to_string())),
    )]);
    let plan = compile_cypher_with_parameters(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE 'name' IN keys(person) AND $relationship_key IN keys(owns) \
             RETURN person.name AS owner",
        &parameters,
    )
    .expect("keys() membership predicates should compile");

    assert_eq!(plan.predicates, Vec::new());
    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::PropertyKeyMembership(
                PropertyKeyMembershipPredicate {
                    variable: "person".to_string(),
                    key: "name".to_string(),
                    presence_variable: None,
                },
            )),
            right: Box::new(PredicateExpression::PropertyKeyMembership(
                PropertyKeyMembershipPredicate {
                    variable: "owns".to_string(),
                    key: "since".to_string(),
                    presence_variable: None,
                },
            )),
        })
    );
}

#[test]
fn rejects_non_string_property_key_membership_predicates() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             WHERE 1 IN keys(service) \
             RETURN service.name",
    )
    .expect_err("keys() membership should require a string literal");

    assert!(
        error.to_string().contains("keys() membership predicates"),
        "{error:?}"
    );
}

#[test]
fn compiles_id_predicates() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE id(person) = 1 AND id(owns) IN [100, 200] \
             RETURN person.name AS owner",
    )
    .expect("id() predicates should compile");

    assert_eq!(plan.predicates, Vec::new());
    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::KeyComparison(KeyPredicate {
                variable: "person".to_string(),
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::Integer(1)),
            })),
            right: Box::new(PredicateExpression::KeyComparison(KeyPredicate {
                variable: "owns".to_string(),
                operator: ComparisonOperator::In,
                rhs: PredicateRhs::List(vec![Literal::Integer(100), Literal::Integer(200)]),
            })),
        })
    );
}

#[test]
fn compiles_element_id_predicates() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE elementId(person) = '1' AND elementId(owns) IN ['100', '200'] \
             RETURN person.name AS owner",
    )
    .expect("elementId() predicates should compile");

    assert_eq!(plan.predicates, Vec::new());
    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::ElementIdComparison(
                ElementIdPredicate {
                    variable: "person".to_string(),
                    operator: ComparisonOperator::Equal,
                    rhs: PredicateRhs::Literal(Literal::String("1".to_string())),
                },
            )),
            right: Box::new(PredicateExpression::ElementIdComparison(
                ElementIdPredicate {
                    variable: "owns".to_string(),
                    operator: ComparisonOperator::In,
                    rhs: PredicateRhs::List(vec![
                        Literal::String("100".to_string()),
                        Literal::String("200".to_string()),
                    ]),
                },
            )),
        })
    );
}

#[test]
fn compiles_type_predicates_as_boolean_constants() {
    let matching = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE type(owns) = 'OWNS' \
             RETURN person.name AS owner",
    )
    .expect("matching type() predicate should compile");
    let non_matching = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE type(owns) = 'DEPENDS_ON' \
             RETURN person.name AS owner",
    )
    .expect("non-matching type() predicate should compile");
    let string_matching = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE type(owns) STARTS WITH 'OW' \
                AND type(owns) ENDS WITH 'NS' \
                AND type(owns) CONTAINS 'WN' \
                AND type(owns) =~ '^OW.*' \
             RETURN person.name AS owner",
    )
    .expect("matching type() string predicates should compile");
    let string_non_matching = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE type(owns) STARTS WITH 'DEP' \
             RETURN person.name AS owner",
    )
    .expect("non-matching type() string predicate should compile");

    assert_eq!(matching.predicate, Some(PredicateExpression::Boolean(true)));
    assert_eq!(
        non_matching.predicate,
        Some(PredicateExpression::Boolean(false))
    );
    assert!(matches!(
        string_matching.predicate,
        Some(PredicateExpression::And { .. })
    ));
    assert_eq!(
        string_non_matching.predicate,
        Some(PredicateExpression::Boolean(false))
    );
}

#[test]
fn compiles_label_membership_predicates_as_boolean_constants() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE 'Service' IN labels(service) AND NOT ('Team' IN labels(service)) \
             RETURN service.name AS service",
    )
    .expect("labels() membership predicates should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::Boolean(true)),
            right: Box::new(PredicateExpression::Not {
                expression: Box::new(PredicateExpression::Boolean(false)),
            }),
        })
    );
}

#[test]
fn compiles_metadata_list_equality_predicates_as_boolean_constants() {
    let graph = star_test_graph();
    let label_match = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             WHERE labels(service) = ['Service'] \
             RETURN service.name AS service",
    )
    .expect("labels() list equality should compile");
    let label_mismatch = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             WHERE ['Team'] <> labels(service) \
             RETURN service.name AS service",
    )
    .expect("reversed labels() list inequality should compile");
    let keys_match = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE keys(service) = ['name', 'tier'] \
               AND ['since', 'source'] = keys(owns) \
             RETURN person.name AS owner",
    )
    .expect("keys() list equality should compile");
    let parameters = BTreeMap::from([
        (
            "service_labels".to_string(),
            CypherParameterValue::List(vec![Literal::String("Service".to_string())]),
        ),
        (
            "service_keys".to_string(),
            CypherParameterValue::List(vec![
                Literal::String("name".to_string()),
                Literal::String("tier".to_string()),
            ]),
        ),
    ]);
    let parameterized = compile_cypher_for_graph_with_parameters(
        &graph,
        "MATCH (service:Service) \
             WHERE labels(service) = $service_labels \
               AND keys(service) = $service_keys \
             RETURN service.name AS service",
        &parameters,
    )
    .expect("metadata list equality should support list parameters");

    assert_eq!(
        label_match.predicate,
        Some(PredicateExpression::Boolean(true))
    );
    assert_eq!(
        label_mismatch.predicate,
        Some(PredicateExpression::Boolean(true))
    );
    assert_eq!(
        keys_match.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::Boolean(true)),
            right: Box::new(PredicateExpression::Boolean(true)),
        })
    );
    assert_eq!(
        parameterized.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::Boolean(true)),
            right: Box::new(PredicateExpression::Boolean(true)),
        })
    );
}

#[test]
fn compiles_metadata_list_index_scalar_expressions() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE labels(service)[0] = 'Service' \
               AND keys(service)[-1] = 'tier' \
             RETURN labels(service)[0] AS service_label, \
                    keys(owns)[0] AS first_ownership_key, \
                    keys(service)[99] AS missing_key \
             ORDER BY keys(service)[1]",
    )
    .expect("metadata list indexes should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Literal(Literal::String("Service".to_string())),
                operator: ComparisonOperator::Equal,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
                    "Service".to_string()
                ),)),
            })),
            right: Box::new(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Literal(Literal::String("tier".to_string())),
                operator: ComparisonOperator::Equal,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
                    "tier".to_string()
                ),)),
            })),
        })
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::String("Service".to_string())),
                alias: "service_label".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::String("since".to_string())),
                alias: "first_ownership_key".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Null),
                alias: "missing_key".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Literal(Literal::String(
                "tier".to_string(),
            ))),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_metadata_list_slice_expressions() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE labels(service)[0..1] = ['Service'] \
               AND ['since'] = keys(owns)[..1] \
               AND isEmpty(labels(service)[1..]) \
             RETURN labels(service)[0..1] AS service_labels, \
                    keys(service)[1..] AS service_keys_tail, \
                    keys(owns)[1..][0] AS owns_second_key, \
                    size(keys(service)[-1..]) AS service_tail_size \
             ORDER BY keys(service)[0..1]",
    )
    .expect("metadata list slices should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::And {
                left: Box::new(PredicateExpression::Boolean(true)),
                right: Box::new(PredicateExpression::Boolean(true)),
            }),
            right: Box::new(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Literal(Literal::Boolean(true)),
                operator: ComparisonOperator::Equal,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Boolean(
                    true
                ),)),
            })),
        })
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::LiteralList {
                literals: vec![Literal::String("Service".to_string())],
                alias: "service_labels".to_string(),
            },
            Projection::LiteralList {
                literals: vec![Literal::String("tier".to_string())],
                alias: "service_keys_tail".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::String("source".to_string(),)),
                alias: "owns_second_key".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Integer(1)),
                alias: "service_tail_size".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::TypedLiteralList {
                literals: vec![Literal::String("name".to_string())],
                element_type: LiteralListElementType::String,
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_empty_metadata_list_slices_as_typed_lists() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             RETURN labels(service)[1..] AS label_tail, \
                    keys(service)[8..] AS key_tail \
             ORDER BY keys(service)[8..]",
    )
    .expect("empty metadata list slices should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: Vec::new(),
                    element_type: LiteralListElementType::String,
                },
                alias: "label_tail".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: Vec::new(),
                    element_type: LiteralListElementType::String,
                },
                alias: "key_tail".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::TypedLiteralList {
                literals: Vec::new(),
                element_type: LiteralListElementType::String,
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_static_list_endpoint_functions() {
    let graph = star_test_graph();
    let parameters = BTreeMap::from([(
        "tiers".to_string(),
        CypherParameterValue::List(vec![
            Literal::String("prod".to_string()),
            Literal::String("dev".to_string()),
        ]),
    )]);
    let plan = compile_cypher_for_graph_with_parameters(
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE head(labels(service)) = 'Service' \
               AND last(keys(owns)) = 'source' \
               AND head($tiers) = 'prod' \
             RETURN head(keys(service)) AS first_service_key, \
                    last(keys(service)) AS last_service_key, \
                    head(labels(service)[1..]) AS missing_label, \
                    last(['prod', 'critical']) AS last_literal \
             ORDER BY last(keys(service))",
        &parameters,
    )
    .expect("static list endpoint functions should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::String("name".to_string())),
                alias: "first_service_key".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::String("tier".to_string())),
                alias: "last_service_key".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Null),
                alias: "missing_label".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::String("critical".to_string(),)),
                alias: "last_literal".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Literal(Literal::String(
                "tier".to_string()
            ))),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_static_list_tail_function() {
    let graph = star_test_graph();
    let parameters = BTreeMap::from([(
        "tiers".to_string(),
        CypherParameterValue::List(vec![
            Literal::String("prod".to_string()),
            Literal::String("dev".to_string()),
        ]),
    )]);
    let plan = compile_cypher_for_graph_with_parameters(
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE head(tail(keys(service))) = 'tier' \
               AND size(tail(labels(service))) = 0 \
               AND isEmpty(tail(labels(service))) \
             RETURN tail(keys(service)) AS service_key_tail, \
                    tail(labels(service)) AS label_tail, \
                    tail(['prod', 'critical']) AS literal_tail, \
                    tail($tiers) AS parameter_tail, \
                    head(tail($tiers)) AS parameter_tail_head, \
                    last(tail(keys(service))) AS service_tail_last, \
                    size(tail(keys(service))) AS service_tail_size \
             ORDER BY tail(keys(service))",
        &parameters,
    )
    .expect("static tail() list functions should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("tier".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "service_key_tail".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: Vec::new(),
                    element_type: LiteralListElementType::String,
                },
                alias: "label_tail".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("critical".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "literal_tail".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("dev".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "parameter_tail".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::String("dev".to_string())),
                alias: "parameter_tail_head".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::String("tier".to_string())),
                alias: "service_tail_last".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Integer(1)),
                alias: "service_tail_size".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::TypedLiteralList {
                literals: vec![Literal::String("tier".to_string())],
                element_type: LiteralListElementType::String,
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_static_list_reverse_function() {
    let graph = star_test_graph();
    let parameters = BTreeMap::from([(
        "tiers".to_string(),
        CypherParameterValue::List(vec![
            Literal::String("prod".to_string()),
            Literal::String("dev".to_string()),
            Literal::String("test".to_string()),
        ]),
    )]);
    let plan = compile_cypher_for_graph_with_parameters(
        &graph,
        "MATCH (service:Service) \
             WHERE head(reverse(keys(service))) = 'tier' \
               AND any(key IN reverse(keys(service)) WHERE key = 'name') \
             RETURN reverse(keys(service)) AS service_keys_reversed, \
                    reverse(labels(service) + keys(service)) AS metadata_reversed, \
                    head(reverse(keys(service))) AS first_reversed_key, \
                    tail(reverse($tiers)) AS reversed_parameter_tail, \
                    size(reverse(labels(service) + keys(service))) AS metadata_size \
             ORDER BY reverse(keys(service))",
        &parameters,
    )
    .expect("static reverse() list functions should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("tier".to_string()),
                        Literal::String("name".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "service_keys_reversed".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("tier".to_string()),
                        Literal::String("name".to_string()),
                        Literal::String("Service".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "metadata_reversed".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::String("tier".to_string())),
                alias: "first_reversed_key".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("dev".to_string()),
                        Literal::String("prod".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "reversed_parameter_tail".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Integer(3)),
                alias: "metadata_size".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::TypedLiteralList {
                literals: vec![
                    Literal::String("tier".to_string()),
                    Literal::String("name".to_string())
                ],
                element_type: LiteralListElementType::String,
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn rejects_reverse_with_ambiguous_list_element_type() {
    for cypher in [
        "MATCH (service:Service) RETURN reverse([]) AS values",
        "MATCH (service:Service) RETURN reverse([null]) AS values",
        "MATCH (service:Service) RETURN reverse([1, 'prod']) AS values",
    ] {
        let error = compile_cypher(cypher).expect_err("ambiguous reverse() should be rejected");
        assert!(
            error
                .to_string()
                .contains("reverse() requires a list with a known non-null element type"),
            "unexpected error: {error}"
        );
    }
}

#[test]
fn compiles_static_list_indexes_and_slices_over_folded_lists() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             WHERE reverse(keys(service))[0] = 'tier' \
               AND size((labels(service) + keys(service))[1..]) = 2 \
             RETURN reverse(keys(service))[1] AS second_reversed_key, \
                    (labels(service) + keys(service))[1..] AS metadata_tail, \
                    tail(reverse(keys(service))[1..]) AS reversed_tail_tail \
             ORDER BY reverse(keys(service))[0]",
    )
    .expect("static list indexes and slices over folded lists should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::String("name".to_string())),
                alias: "second_reversed_key".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("name".to_string()),
                        Literal::String("tier".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "metadata_tail".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: Vec::new(),
                    element_type: LiteralListElementType::String,
                },
                alias: "reversed_tail_tail".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Literal(Literal::String(
                "tier".to_string()
            ))),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_static_list_concatenation() {
    let graph = star_test_graph();
    let parameters = BTreeMap::from([(
        "prefixes".to_string(),
        CypherParameterValue::List(vec![Literal::String("prefix".to_string())]),
    )]);
    let plan = compile_cypher_for_graph_with_parameters(
        &graph,
        "MATCH (service:Service) \
             WHERE head($prefixes + tail(keys(service))) = 'prefix' \
               AND size(labels(service) + tail(labels(service))) = 1 \
               AND any(key IN ['active'] + tail(keys(service)) WHERE key = 'tier') \
               AND service.tier IN (['prod'] + ['dev']) \
             RETURN $prefixes + tail(keys(service)) AS keys_with_prefix, \
                    labels(service) + [] AS labels_copy, \
                    [null] + tail(keys(service)) AS nullable_keys, \
                    tail($prefixes + tail(keys(service))) AS concat_tail, \
                    size($prefixes + tail(keys(service))) AS concat_size \
             ORDER BY $prefixes + tail(keys(service))",
        &parameters,
    )
    .expect("static list concatenation should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("prefix".to_string()),
                        Literal::String("tier".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "keys_with_prefix".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("Service".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "labels_copy".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::Null, Literal::String("tier".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "nullable_keys".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("tier".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "concat_tail".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Integer(2)),
                alias: "concat_size".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::TypedLiteralList {
                literals: vec![
                    Literal::String("prefix".to_string()),
                    Literal::String("tier".to_string())
                ],
                element_type: LiteralListElementType::String,
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_static_list_scalar_concatenation() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE 'dev' IN (['prod'] + 'dev') \
               AND 'prod' IN ('prod' + ['dev']) \
             RETURN ['prod'] + 'dev' AS appended, \
                    'prod' + ['dev'] AS prepended, \
                    [] + 'prod' AS typed_from_append, \
                    'prod' + [] AS typed_from_prepend, \
                    ['prod'] + null AS nullable_appended, \
                    [tier IN 'prod' + ['dev'] | toUpper(tier)] AS mapped",
    )
    .expect("static list scalar concatenation should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::Boolean(true)),
            right: Box::new(PredicateExpression::Boolean(true)),
        })
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("prod".to_string()),
                        Literal::String("dev".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "appended".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("prod".to_string()),
                        Literal::String("dev".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "prepended".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("prod".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "typed_from_append".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("prod".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "typed_from_prepend".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("prod".to_string()), Literal::Null],
                    element_type: LiteralListElementType::String,
                },
                alias: "nullable_appended".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("PROD".to_string()),
                        Literal::String("DEV".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "mapped".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_static_list_cast_projections() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN toStringList([1, true, null]) AS strings, \
                    toIntegerList(['bad', 2, true, 3.7, false, null]) AS integers, \
                    toFloatList(['bad', 2, 2.5, '3.5', true, null]) AS floats, \
                    toBooleanList(['true', 'false', 'bad', 0, 2, true, 1.5, null]) AS booleans",
    )
    .expect("static list casts should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("1".to_string()),
                        Literal::String("true".to_string()),
                        Literal::Null,
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "strings".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Null,
                        Literal::Integer(2),
                        Literal::Integer(1),
                        Literal::Integer(3),
                        Literal::Integer(0),
                        Literal::Null,
                    ],
                    element_type: LiteralListElementType::Integer,
                },
                alias: "integers".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Null,
                        Literal::Float(OrderedFloat(2.0)),
                        Literal::Float(OrderedFloat(2.5)),
                        Literal::Float(OrderedFloat(3.5)),
                        Literal::Null,
                        Literal::Null,
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "floats".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Boolean(true),
                        Literal::Boolean(false),
                        Literal::Null,
                        Literal::Boolean(false),
                        Literal::Boolean(true),
                        Literal::Boolean(true),
                        Literal::Null,
                        Literal::Null,
                    ],
                    element_type: LiteralListElementType::Boolean,
                },
                alias: "booleans".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_static_list_cast_predicates_and_ordering() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE head(toIntegerList(['1', 'bad'])) = 1 \
               AND last(toBooleanList(['false', 2])) = true \
             RETURN service.name AS service \
             ORDER BY toStringList([2, true])",
    )
    .expect("static list casts should compile in predicates and ordering");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Literal(Literal::Integer(1)),
                operator: ComparisonOperator::Equal,
                rhs: ScalarPredicateRhs::Expression(
                    ScalarExpression::Literal(Literal::Integer(1),)
                ),
            })),
            right: Box::new(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Literal(Literal::Boolean(true)),
                operator: ComparisonOperator::Equal,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Boolean(
                    true
                ),)),
            })),
        })
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::TypedLiteralList {
                literals: vec![
                    Literal::String("2".to_string()),
                    Literal::String("true".to_string()),
                ],
                element_type: LiteralListElementType::String,
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_static_list_comprehensions() {
    let graph = star_test_graph();
    let parameters = BTreeMap::from([(
        "selected_keys".to_string(),
        CypherParameterValue::List(vec![
            Literal::String("tier".to_string()),
            Literal::String("missing".to_string()),
            Literal::String("name".to_string()),
        ]),
    )]);
    let plan = compile_cypher_for_graph_with_parameters(
        &graph,
        "MATCH (service:Service) \
             RETURN [k IN keys(service)] AS service_keys_copy, \
                    [l IN labels(service)] AS service_labels, \
                    [k IN ['name', 'tier', null]] AS literal_keys_copy, \
                    [k IN $selected_keys] AS selected_keys_copy \
             ORDER BY [k IN keys(service)]",
        &parameters,
    )
    .expect("static list comprehensions should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("name".to_string()),
                        Literal::String("tier".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "service_keys_copy".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("Service".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "service_labels".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("name".to_string()),
                        Literal::String("tier".to_string()),
                        Literal::Null
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "literal_keys_copy".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("tier".to_string()),
                        Literal::String("missing".to_string()),
                        Literal::String("name".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "selected_keys_copy".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::TypedLiteralList {
                literals: vec![
                    Literal::String("name".to_string()),
                    Literal::String("tier".to_string())
                ],
                element_type: LiteralListElementType::String,
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_static_filter_and_extract_list_functions() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             RETURN filter(key IN keys(service) WHERE key <> 'name') AS service_keys_without_name, \
                    extract(key IN keys(service) | toUpper(key)) AS service_key_tokens, \
                    extract(x IN filter(x IN [1, 2, 3] WHERE x > 1) | x + 1) AS shifted \
             ORDER BY filter(key IN keys(service) WHERE key = 'tier')",
    )
    .expect("static filter()/extract() list functions should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("tier".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "service_keys_without_name".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("NAME".to_string()),
                        Literal::String("TIER".to_string()),
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "service_key_tokens".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::Integer(3), Literal::Integer(4)],
                    element_type: LiteralListElementType::Integer,
                },
                alias: "shifted".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::TypedLiteralList {
                literals: vec![Literal::String("tier".to_string())],
                element_type: LiteralListElementType::String,
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn rejects_dynamic_static_filter_and_extract_list_functions() {
    let dynamic_filter_collection = compile_cypher(
        "MATCH (service:Service) \
             RETURN filter(x IN service.name WHERE x <> 'a') AS values",
    )
    .expect_err("dynamic filter() collection should be rejected");
    assert!(
        dynamic_filter_collection
            .to_string()
            .contains("filter() requires a literal list"),
        "{dynamic_filter_collection}"
    );

    let dynamic_extract_map = compile_cypher(
        "MATCH (service:Service) \
             RETURN extract(x IN [1, 2] | service.name) AS values",
    )
    .expect_err("dynamic extract() map should be rejected");
    assert!(
        dynamic_extract_map
            .to_string()
            .contains("static list comprehension map expressions"),
        "{dynamic_extract_map}"
    );
}

#[test]
fn compiles_filtered_static_list_comprehensions() {
    let graph = star_test_graph();
    let parameters = BTreeMap::from([(
        "selected_keys".to_string(),
        CypherParameterValue::List(vec![
            Literal::String("tier".to_string()),
            Literal::String("missing".to_string()),
            Literal::String("name".to_string()),
            Literal::Null,
        ]),
    )]);
    let plan = compile_cypher_for_graph_with_parameters(
        &graph,
        "MATCH (service:Service) \
             RETURN [k IN keys(service) WHERE k <> 'tier'] AS service_keys_without_tier, \
                    [k IN ['name', 'tier', null] WHERE k IS NOT NULL] AS non_null_literal_keys, \
                    [k IN $selected_keys WHERE k IN ['name', 'tier']] AS selected_known_keys \
             ORDER BY [k IN keys(service) WHERE k = 'name']",
        &parameters,
    )
    .expect("filtered static list comprehensions should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("name".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "service_keys_without_tier".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("name".to_string()),
                        Literal::String("tier".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "non_null_literal_keys".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("tier".to_string()),
                        Literal::String("name".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "selected_known_keys".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::TypedLiteralList {
                literals: vec![Literal::String("name".to_string())],
                element_type: LiteralListElementType::String,
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_static_list_comprehensions_over_case_and_coalesce_sources() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
            &graph,
            "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN [k IN coalesce(keys(person), ['fallback', 'owner']) WHERE k <> 'owner' | toUpper(k)] AS owner_key_tokens, \
                    [k IN CASE WHEN person IS NULL THEN ['fallback'] ELSE keys(person) END WHERE k STARTS WITH 't' | k] AS team_keys \
             ORDER BY [k IN coalesce(keys(person), ['fallback']) | k]",
        )
        .expect("static list comprehensions over CASE/coalesce sources should compile");

    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Coalesce {
                    expressions: coalesced_expressions,
                },
                alias: coalesced_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Case {
                    alternatives: case_alternatives,
                    else_expression: case_else,
                },
                alias: case_alias,
            },
        ] if coalesced_alias == "owner_key_tokens"
            && matches!(
                coalesced_expressions.as_slice(),
                [
                    ScalarExpression::PresenceGated {
                        presence_variable,
                        expression,
                    },
                    ScalarExpression::TypedLiteralList {
                        literals: fallback_literals,
                        element_type: fallback_type,
                    },
                ] if presence_variable == "person"
                    && matches!(
                        expression.as_ref(),
                        ScalarExpression::TypedLiteralList { literals, element_type }
                            if literals.as_slice() == [
                                Literal::String("NAME".to_string()),
                                Literal::String("TEAM".to_string()),
                            ]
                            && *element_type == LiteralListElementType::String
                    )
                    && fallback_literals.as_slice() == [Literal::String("FALLBACK".to_string())]
                    && *fallback_type == LiteralListElementType::String
            )
            && case_alias == "team_keys"
            && matches!(
                case_alternatives.as_slice(),
                [ScalarCaseAlternative {
                    when: PredicateExpression::Presence(PresencePredicate {
                        variable,
                        operator: ComparisonOperator::Equal,
                    }),
                    then: ScalarExpression::TypedLiteralList { literals, element_type },
                }] if variable == "person"
                    && literals.is_empty()
                    && *element_type == LiteralListElementType::String
            )
            && matches!(
                case_else.as_deref(),
                Some(ScalarExpression::PresenceGated {
                    presence_variable,
                    expression,
                }) if presence_variable == "person"
                    && matches!(
                        expression.as_ref(),
                        ScalarExpression::TypedLiteralList { literals, element_type }
                            if literals.as_slice() == [Literal::String("team".to_string())]
                                && *element_type == LiteralListElementType::String
                    )
            )
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Coalesce { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_static_list_comprehensions_over_sliced_case_and_coalesce_sources() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
            &graph,
            "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN [k IN coalesce(keys(person), ['fallback', 'owner'])[0..1] | toUpper(k)] AS first_owner_key, \
                    [k IN (CASE WHEN person IS NULL THEN ['fallback', 'owner'] ELSE keys(person) END)[1..2] | k] AS second_owner_key \
             ORDER BY [k IN coalesce(keys(person), ['fallback'])[0..1] | k]",
        )
        .expect("static list comprehensions over sliced CASE/coalesce sources should compile");

    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Coalesce { expressions },
                alias: coalesce_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Case { .. },
                alias: case_alias,
            },
        ] if coalesce_alias == "first_owner_key"
            && case_alias == "second_owner_key"
            && matches!(
                expressions.as_slice(),
                [
                    ScalarExpression::PresenceGated { expression, .. },
                    ScalarExpression::TypedLiteralList { literals, element_type },
                ] if matches!(
                    expression.as_ref(),
                    ScalarExpression::TypedLiteralList {
                        literals: matched_literals,
                        element_type: matched_type,
                    } if matched_literals.as_slice() == [Literal::String("NAME".to_string())]
                        && *matched_type == LiteralListElementType::String
                )
                && literals.as_slice() == [Literal::String("FALLBACK".to_string())]
                && *element_type == LiteralListElementType::String
            )
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Coalesce { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_static_list_comprehensions_as_in_rhs() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
            &graph,
            "MATCH (service:Service) \
             WHERE service.tier IN [tier IN ['prod', 'dev'] WHERE tier <> 'dev' | tier] \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN 'TEAM' IN [k IN coalesce(keys(person), ['fallback']) | toUpper(k)] AS owner_has_team_key, \
                    'team' IN [k IN CASE WHEN person IS NULL THEN ['fallback'] ELSE keys(person) END | k] AS case_has_team_key",
        )
        .expect("static list comprehensions should compile as IN RHS values");

    assert!(matches!(
        plan.predicates.as_slice(),
        [PropertyPredicate {
            property: PropertyRef { variable, property },
            operator: ComparisonOperator::In,
            rhs: PredicateRhs::List(literals),
        }] if variable == "service"
            && property == "tier"
            && literals == &vec![Literal::String("prod".to_string())]
    ));
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Predicate(coalesced_predicate),
                alias: coalesced_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Predicate(case_predicate),
                alias: case_alias,
            },
        ] if coalesced_alias == "owner_has_team_key"
            && case_alias == "case_has_team_key"
            && is_case_boolean_scalar_predicate(coalesced_predicate.as_ref())
            && is_case_boolean_scalar_predicate(case_predicate.as_ref())
    ));
}

#[test]
fn compiles_static_list_comprehension_comparisons() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
            &graph,
            "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN [k IN coalesce(keys(person), ['fallback']) | k] = ['name', 'team'] AS coalesced_matches_keys, \
                    ['fallback'] = [k IN coalesce(keys(person), ['fallback']) | k] AS coalesced_is_fallback, \
                    [k IN CASE WHEN person IS NULL THEN ['fallback'] ELSE keys(person) END | k] <> [] AS case_non_empty \
             ORDER BY [k IN CASE WHEN person IS NULL THEN ['fallback'] ELSE keys(person) END | k] > ['fallback']",
        )
        .expect("static list comprehension comparisons should compile");

    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Predicate(coalesced_matches),
                alias: coalesced_matches_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Predicate(coalesced_fallback),
                alias: coalesced_fallback_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Predicate(case_non_empty),
                alias: case_non_empty_alias,
            },
        ] if coalesced_matches_alias == "coalesced_matches_keys"
            && coalesced_fallback_alias == "coalesced_is_fallback"
            && case_non_empty_alias == "case_non_empty"
            && is_case_boolean_scalar_predicate(coalesced_matches.as_ref())
            && is_case_boolean_scalar_predicate(coalesced_fallback.as_ref())
            && is_case_boolean_scalar_predicate(case_non_empty.as_ref())
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
fn compiles_sliced_conditional_list_comprehension_predicates() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
            &graph,
            "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN [k IN coalesce(keys(person), ['fallback', 'owner'])[0..1] | k] = ['name'] AS coalesced_first_key_matches, \
                    'team' IN [k IN (CASE WHEN person IS NULL THEN ['fallback', 'owner'] ELSE keys(person) END)[1..2] | k] AS case_second_key_has_team",
        )
        .expect("sliced conditional list comprehension predicates should compile");

    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Predicate(coalesced_predicate),
                alias: coalesced_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Predicate(case_predicate),
                alias: case_alias,
            },
        ] if coalesced_alias == "coalesced_first_key_matches"
            && case_alias == "case_second_key_has_team"
            && is_case_boolean_scalar_predicate(coalesced_predicate.as_ref())
            && is_case_boolean_scalar_predicate(case_predicate.as_ref())
    ));
}

#[test]
fn compiles_static_list_comprehension_expression_filters() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             RETURN [k IN keys(service) WHERE toUpper(k) STARTS WITH 'T'] AS upper_t_keys, \
                    [x IN ['1', '2', 'bad'] WHERE toIntegerOrNull(x) >= 2] AS numeric_strings, \
                    [x IN [1, 2, 3] WHERE x + 1 >= 3] AS arithmetic_values, \
                    [x IN [1.2, 2.8] WHERE floor(x) = 2.0] AS floored_values, \
                    [x IN ['', 'a', null] WHERE isEmpty(x)] AS empty_strings, \
                    [x IN ['', 'a', null] WHERE isEmpty(x) = false] AS non_empty_strings",
    )
    .expect("static list comprehension expression filters should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("tier".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "upper_t_keys".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("2".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "numeric_strings".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::Integer(2), Literal::Integer(3)],
                    element_type: LiteralListElementType::Integer,
                },
                alias: "arithmetic_values".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::Float(OrderedFloat(2.8))],
                    element_type: LiteralListElementType::Float,
                },
                alias: "floored_values".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String(String::new())],
                    element_type: LiteralListElementType::String,
                },
                alias: "empty_strings".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("a".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "non_empty_strings".to_string(),
            },
        ]
    );
}

#[test]
fn rejects_dynamic_static_list_comprehension_sources() {
    let error = compile_cypher_for_graph(
        &star_test_graph(),
        "MATCH (service:Service) RETURN [c IN service.name] AS characters",
    )
    .expect_err("dynamic list comprehension sources should be rejected");

    assert!(
        error
            .to_string()
            .contains("static list comprehensions require"),
        "{error}"
    );
}

#[test]
fn compiles_mapped_static_list_comprehensions() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             RETURN [k IN keys(service) | toUpper(k)] AS upper_keys, \
                    [k IN [' name ', null, 'tier'] | trim(k)] AS trimmed_keys, \
                    [k IN ['service-id'] | replace(k, '-', '_')] AS replaced_keys, \
                    [k IN keys(service) | left(k, 2)] AS key_prefixes, \
                    [k IN ['service-name', null] | substring(k, 8, 4)] AS key_suffixes, \
                    [k IN ['ops'] | right(k, 2)] AS right_suffixes, \
                    [k IN ['abc'] | reverse(k)] AS reversed_literals, \
                    [k IN [1, 2] | toString(k)] AS number_strings \
             ORDER BY [k IN keys(service) WHERE k <> 'tier' | upper(k)]",
    )
    .expect("mapped static list comprehensions should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("NAME".to_string()),
                        Literal::String("TIER".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "upper_keys".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("name".to_string()),
                        Literal::Null,
                        Literal::String("tier".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "trimmed_keys".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("service_id".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "replaced_keys".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("na".to_string()),
                        Literal::String("ti".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "key_prefixes".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("name".to_string()), Literal::Null],
                    element_type: LiteralListElementType::String,
                },
                alias: "key_suffixes".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("ps".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "right_suffixes".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("cba".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "reversed_literals".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("1".to_string()),
                        Literal::String("2".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "number_strings".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::TypedLiteralList {
                literals: vec![Literal::String("NAME".to_string())],
                element_type: LiteralListElementType::String,
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_static_list_comprehension_null_maps() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             RETURN [k IN ['name', null] | coalesce(k, 'missing')] AS coalesced_keys, \
                    [k IN keys(service) | nullIf(k, 'tier')] AS nullified_tier, \
                    [k IN ['fallback'] | coalesce(null, k)] AS coalesced_second_arg, \
                    [k IN ['id'] | nullIf('id', k)] AS nullified_second_arg",
    )
    .expect("static list comprehension null maps should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("name".to_string()),
                        Literal::String("missing".to_string())
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "coalesced_keys".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("name".to_string()), Literal::Null],
                    element_type: LiteralListElementType::String,
                },
                alias: "nullified_tier".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("fallback".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "coalesced_second_arg".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::Null],
                    element_type: LiteralListElementType::String,
                },
                alias: "nullified_second_arg".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_static_list_comprehension_length_maps() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             RETURN [k IN keys(service) | size(k)] AS key_lengths, \
                    [k IN ['ops', null] | char_length(k)] AS literal_lengths, \
                    [k IN ['deploy'] | character_length(k)] AS gql_literal_lengths",
    )
    .expect("static list comprehension length maps should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::Integer(4), Literal::Integer(4)],
                    element_type: LiteralListElementType::Integer,
                },
                alias: "key_lengths".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::Integer(3), Literal::Null],
                    element_type: LiteralListElementType::Integer,
                },
                alias: "literal_lengths".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::Integer(6)],
                    element_type: LiteralListElementType::Integer,
                },
                alias: "gql_literal_lengths".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_static_list_comprehension_cast_maps() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             RETURN [x IN ['bad', '3', null] | toInteger(x)] AS ints, \
                    [x IN ['bad', '2.5', null] | toFloat(x)] AS floats, \
                    [x IN ['maybe', 'true', null] | toBoolean(x)] AS booleans, \
                    [x IN [1, 2, null] | toString(x)] AS strings",
    )
    .expect("static list comprehension cast maps should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::Null, Literal::Integer(3), Literal::Null],
                    element_type: LiteralListElementType::Integer,
                },
                alias: "ints".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Null,
                        Literal::Float(OrderedFloat(2.5)),
                        Literal::Null
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "floats".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::Null, Literal::Boolean(true), Literal::Null],
                    element_type: LiteralListElementType::Boolean,
                },
                alias: "booleans".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("1".to_string()),
                        Literal::String("2".to_string()),
                        Literal::Null
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "strings".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_static_list_comprehension_nullable_cast_maps() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             RETURN [x IN ['bad', '3', null] | toIntegerOrNull(x)] AS ints, \
                    [x IN ['bad', '2.5', null] | toFloatOrNull(x)] AS floats, \
                    [x IN ['maybe', 'true', null] | toBooleanOrNull(x)] AS booleans, \
                    [x IN [1, 2, null] | toStringOrNull(x)] AS strings",
    )
    .expect("static list comprehension nullable cast maps should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::Null, Literal::Integer(3), Literal::Null],
                    element_type: LiteralListElementType::Integer,
                },
                alias: "ints".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Null,
                        Literal::Float(OrderedFloat(2.5)),
                        Literal::Null
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "floats".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::Null, Literal::Boolean(true), Literal::Null],
                    element_type: LiteralListElementType::Boolean,
                },
                alias: "booleans".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::String("1".to_string()),
                        Literal::String("2".to_string()),
                        Literal::Null
                    ],
                    element_type: LiteralListElementType::String,
                },
                alias: "strings".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_static_list_comprehension_numeric_function_maps() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             RETURN [x IN [1, 3, 6] | abs(x - 3)] AS absolute_ints, \
                    [x IN [1.5, null, 5.5] | abs(x - 3.0)] AS absolute_floats, \
                    [x IN [4, 9] | sqrt(x)] AS roots, \
                    [x IN [1.0, 3.0, 6.5, null] | sign(x - 3.0)] AS signs, \
                    [x IN [2, 3, null] | pow(x, 3)] AS powers, \
                    [x IN [2, 3] | power(x, 2)] AS squares, \
                    [x IN [1.0, 2.0, null] | isNaN(x)] AS nan_checks",
    )
    .expect("static list comprehension numeric function maps should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Integer(2),
                        Literal::Integer(0),
                        Literal::Integer(3)
                    ],
                    element_type: LiteralListElementType::Integer,
                },
                alias: "absolute_ints".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Float(OrderedFloat(1.5)),
                        Literal::Null,
                        Literal::Float(OrderedFloat(2.5))
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "absolute_floats".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Float(OrderedFloat(2.0)),
                        Literal::Float(OrderedFloat(3.0))
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "roots".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Integer(-1),
                        Literal::Integer(0),
                        Literal::Integer(1),
                        Literal::Null
                    ],
                    element_type: LiteralListElementType::Integer,
                },
                alias: "signs".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Float(OrderedFloat(8.0)),
                        Literal::Float(OrderedFloat(27.0)),
                        Literal::Null
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "powers".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Float(OrderedFloat(4.0)),
                        Literal::Float(OrderedFloat(9.0))
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "squares".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Boolean(false),
                        Literal::Boolean(false),
                        Literal::Null
                    ],
                    element_type: LiteralListElementType::Boolean,
                },
                alias: "nan_checks".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_static_list_comprehension_log_exp_constant_maps() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             RETURN [x IN [0, 1, null] | round(exp(x), 0)] AS exponentials, \
                    [x IN [1.0, 2.718281828459045, null] | round(log(x), 0)] AS natural_logs, \
                    [x IN [1, 100, null] | log10(x)] AS base10_logs, \
                    [x IN [1, 2] | round(pi(), x)] AS rounded_pi, \
                    [x IN [1, 2] | round(e(), x)] AS rounded_e",
    )
    .expect("static list comprehension log/exp/constant maps should compile");

    let rounded_pi_one =
        round_static_float(std::f64::consts::PI, 1, "test").expect("pi should round");
    let rounded_pi_two =
        round_static_float(std::f64::consts::PI, 2, "test").expect("pi should round");
    let rounded_e_one = round_static_float(std::f64::consts::E, 1, "test").expect("e should round");
    let rounded_e_two = round_static_float(std::f64::consts::E, 2, "test").expect("e should round");
    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Float(OrderedFloat(1.0)),
                        Literal::Float(OrderedFloat(3.0)),
                        Literal::Null
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "exponentials".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Float(OrderedFloat(0.0)),
                        Literal::Float(OrderedFloat(1.0)),
                        Literal::Null
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "natural_logs".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Float(OrderedFloat(0.0)),
                        Literal::Float(OrderedFloat(2.0)),
                        Literal::Null
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "base10_logs".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Float(OrderedFloat(rounded_pi_one)),
                        Literal::Float(OrderedFloat(rounded_pi_two))
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "rounded_pi".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Float(OrderedFloat(rounded_e_one)),
                        Literal::Float(OrderedFloat(rounded_e_two))
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "rounded_e".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_static_list_comprehension_unary_trigonometric_function_maps() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             RETURN [x IN [0.0, 1.5707963267948966] | round(sin(x), 0)] AS sines, \
                    [x IN [0.0, 1.5707963267948966] | round(cos(x), 0)] AS cosines, \
                    [x IN [0.0, 0.7853981633974483] | round(tan(x), 0)] AS tangents, \
                    [x IN [0.7853981633974483] | round(cot(x), 0)] AS cotangents, \
                    [x IN [1.0] | round(asin(x), 0)] AS arcsines, \
                    [x IN [1.0] | round(acos(x), 0)] AS arccosines, \
                    [x IN [1.0] | round(atan(x), 0)] AS arctangents",
    )
    .expect("static list comprehension trigonometric maps should compile");

    assert_eq!(
        plan.projections,
        vec![
            typed_float_list_projection("sines", vec![0.0, 1.0]),
            typed_float_list_projection("cosines", vec![1.0, 0.0]),
            typed_float_list_projection("tangents", vec![0.0, 1.0]),
            typed_float_list_projection("cotangents", vec![1.0]),
            typed_float_list_projection("arcsines", vec![2.0]),
            typed_float_list_projection("arccosines", vec![0.0]),
            typed_float_list_projection("arctangents", vec![1.0]),
        ]
    );
}

#[test]
fn compiles_static_list_comprehension_angle_function_maps() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             RETURN [x IN [1.0] | round(atan2(x, x), 2)] AS arctangent_pairs, \
                    [x IN [3.141592653589793] | round(degrees(x), 0)] AS degree_values, \
                    [x IN [180.0] | round(radians(x), 2)] AS radian_values, \
                    [x IN [0.0] | haversin(x)] AS haversines",
    )
    .expect("static list comprehension angle maps should compile");

    let atan2_rounded =
        round_static_float(1.0_f64.atan2(1.0), 2, "test").expect("atan2 should round");
    let radians_rounded =
        round_static_float(180.0_f64.to_radians(), 2, "test").expect("radians should round");
    assert_eq!(
        plan.projections,
        vec![
            typed_float_list_projection("arctangent_pairs", vec![atan2_rounded]),
            typed_float_list_projection("degree_values", vec![180.0]),
            typed_float_list_projection("radian_values", vec![radians_rounded]),
            typed_float_list_projection("haversines", vec![0.0]),
        ]
    );
}

#[test]
fn compiles_static_list_comprehension_rounding_function_maps() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             RETURN [x IN [1.2, 2.8, null] | ceiling(x)] AS ceilings, \
                    [x IN [1.2, 2.8, null] | floor(x)] AS floors, \
                    [x IN [1.24, 1.25, 1.26] | round(x, 1)] AS rounded_tenths, \
                    [x IN [1.4, 1.5, 1.6] | round(x)] AS rounded_wholes",
    )
    .expect("static list comprehension rounding function maps should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Float(OrderedFloat(2.0)),
                        Literal::Float(OrderedFloat(3.0)),
                        Literal::Null
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "ceilings".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Float(OrderedFloat(1.0)),
                        Literal::Float(OrderedFloat(2.0)),
                        Literal::Null
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "floors".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Float(OrderedFloat(1.2)),
                        Literal::Float(OrderedFloat(1.3)),
                        Literal::Float(OrderedFloat(1.3))
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "rounded_tenths".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Float(OrderedFloat(1.0)),
                        Literal::Float(OrderedFloat(2.0)),
                        Literal::Float(OrderedFloat(2.0))
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "rounded_wholes".to_string(),
            },
        ]
    );
}

#[test]
fn rejects_static_list_comprehension_null_maps_with_invalid_arguments() {
    let error = compile_cypher_for_graph(
        &star_test_graph(),
        "MATCH (service:Service) RETURN [k IN keys(service) | coalesce(k)] AS values",
    )
    .expect_err("coalesce() should require at least two arguments");

    assert!(
        error.to_string().contains(
            "coalesce() in static list comprehension maps requires at least two arguments"
        ),
        "{error}"
    );

    let error = compile_cypher_for_graph(
        &star_test_graph(),
        "MATCH (service:Service) RETURN [k IN keys(service) | nullIf(k)] AS values",
    )
    .expect_err("nullIf() should require exactly two arguments");

    assert!(
        error
            .to_string()
            .contains("nullIf() in static list comprehension maps requires exactly two arguments"),
        "{error}"
    );
}

#[test]
fn rejects_static_list_comprehension_numeric_function_maps_with_invalid_arguments() {
    let error = compile_cypher_for_graph(
        &star_test_graph(),
        "MATCH (service:Service) RETURN [x IN [1] | sqrt(x - 2)] AS roots",
    )
    .expect_err("sqrt() should reject negative static values");

    assert!(
        error.to_string().contains(
            "sqrt() in static list comprehension maps requires non-negative numeric arguments"
        ),
        "{error}"
    );

    let error = compile_cypher_for_graph(
        &star_test_graph(),
        "MATCH (service:Service) RETURN [x IN ['not-numeric'] | abs(x)] AS values",
    )
    .expect_err("abs() should reject non-numeric static values");

    assert!(
        error
            .to_string()
            .contains("static numeric map expressions require numeric operands"),
        "{error}"
    );

    let error = compile_cypher_for_graph(
        &star_test_graph(),
        "MATCH (service:Service) RETURN [x IN [1.2] | round(x, '1')] AS values",
    )
    .expect_err("round() should reject non-integer precision");

    assert!(
        error.to_string().contains(
            "round() in static list comprehension maps requires integer precision arguments"
        ),
        "{error}"
    );

    let error = compile_cypher_for_graph(
        &star_test_graph(),
        "MATCH (service:Service) RETURN [x IN [2] | pow(x)] AS values",
    )
    .expect_err("pow() should require two static map arguments");

    assert!(
        error
            .to_string()
            .contains("pow() in static list comprehension maps requires exactly two arguments"),
        "{error}"
    );

    let error = compile_cypher_for_graph(
        &star_test_graph(),
        "MATCH (service:Service) RETURN [x IN [2] | pi(x)] AS values",
    )
    .expect_err("pi() should reject static map arguments");

    assert!(
        error
            .to_string()
            .contains("pi() in static list comprehension maps requires exactly zero arguments"),
        "{error}"
    );

    let error = compile_cypher_for_graph(
        &star_test_graph(),
        "MATCH (service:Service) RETURN [x IN [2] | atan2(x)] AS values",
    )
    .expect_err("atan2() should require two static map arguments");

    assert!(
        error
            .to_string()
            .contains("atan2() in static list comprehension maps requires exactly two arguments"),
        "{error}"
    );
}

#[test]
fn rejects_static_list_comprehension_string_maps_with_invalid_arguments() {
    let error = compile_cypher_for_graph(
        &star_test_graph(),
        "MATCH (service:Service) RETURN [k IN keys(service) | left(k, 'x')] AS key_prefixes",
    )
    .expect_err("left() count should be integer");

    assert!(
        error
            .to_string()
            .contains("left() in static list comprehension maps requires integer arguments"),
        "{error}"
    );

    let error = compile_cypher_for_graph(
        &star_test_graph(),
        "MATCH (service:Service) RETURN [k IN keys(service) | substring(k, -1, 2)] AS key_prefixes",
    )
    .expect_err("substring() start should be non-negative");

    assert!(
        error.to_string().contains(
            "substring() in static list comprehension maps requires non-negative integer arguments"
        ),
        "{error}"
    );
}

#[test]
fn compiles_static_list_comprehension_string_filters() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
            &graph,
            "MATCH (service:Service) \
             RETURN [k IN keys(service) WHERE k STARTS WITH 't'] AS starts_with_t, \
                    [k IN keys(service) WHERE k ENDS WITH 'e'] AS ends_with_e, \
                    [k IN ['billing', 'deployments', 'experiments'] WHERE k CONTAINS 'ing'] AS contains_ing, \
                    [k IN keys(service) WHERE k =~ '^t.*'] AS regex_t, \
                    [k IN keys(service) WHERE k > 'risk'] AS after_risk, \
                    [k IN keys(service) WHERE k <= 'name'] AS through_name",
        )
        .expect("static list comprehension string filters should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("tier".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "starts_with_t".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("name".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "ends_with_e".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("billing".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "contains_ing".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("tier".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "regex_t".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("tier".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "after_risk".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::String("name".to_string())],
                    element_type: LiteralListElementType::String,
                },
                alias: "through_name".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_static_collection_string_ordering_predicates() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             WHERE any(key IN keys(service) WHERE key > 'team') \
             RETURN all(key IN keys(service) WHERE key >= 'name') AS keys_after_name",
    )
    .expect("static collection string ordering predicates should compile");

    assert_eq!(plan.predicate, Some(PredicateExpression::Boolean(true)));
    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::Predicate(Box::new(PredicateExpression::Boolean(true))),
            alias: "keys_after_name".to_string(),
        }]
    );
}

#[test]
fn compiles_numeric_and_boolean_static_list_comprehension_maps() {
    let graph = star_test_graph();
    let parameters = BTreeMap::from([(
        "weights".to_string(),
        CypherParameterValue::List(vec![
            Literal::Integer(2),
            Literal::Integer(4),
            Literal::Null,
        ]),
    )]);
    let plan = compile_cypher_for_graph_with_parameters(
        &graph,
        "MATCH (service:Service) \
             RETURN [x IN [1, 2, 3] | x + 1] AS incremented, \
                    [x IN [1.5, 2.5] | x * 2] AS doubled, \
                    [x IN $weights | x / 2] AS halved_weights, \
                    [k IN keys(service) | k STARTS WITH 't'] AS t_flags, \
                    [x IN ['', 'a', null] | isEmpty(x)] AS empty_flags",
        &parameters,
    )
    .expect("numeric and boolean static list comprehension maps should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Integer(2),
                        Literal::Integer(3),
                        Literal::Integer(4)
                    ],
                    element_type: LiteralListElementType::Integer,
                },
                alias: "incremented".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Float(OrderedFloat(3.0)),
                        Literal::Float(OrderedFloat(5.0))
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "doubled".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Float(OrderedFloat(1.0)),
                        Literal::Float(OrderedFloat(2.0)),
                        Literal::Null
                    ],
                    element_type: LiteralListElementType::Float,
                },
                alias: "halved_weights".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![Literal::Boolean(false), Literal::Boolean(true)],
                    element_type: LiteralListElementType::Boolean,
                },
                alias: "t_flags".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Boolean(true),
                        Literal::Boolean(false),
                        Literal::Null
                    ],
                    element_type: LiteralListElementType::Boolean,
                },
                alias: "empty_flags".to_string(),
            },
        ]
    );
}

#[test]
fn rejects_dynamic_mapped_static_list_comprehensions() {
    let error = compile_cypher_for_graph(
        &star_test_graph(),
        "MATCH (service:Service) RETURN [k IN keys(service) | service.name] AS key_names",
    )
    .expect_err("dynamic mapped static list comprehensions should be rejected");

    assert!(
        error
            .to_string()
            .contains("static list comprehension map expressions support"),
        "{error}"
    );
}

#[test]
fn rejects_incompatible_static_list_concatenation() {
    for (cypher, expected) in [
        (
            "MATCH (service:Service) RETURN ['a'] + [1] AS values",
            "static list concatenation requires compatible non-null element types",
        ),
        (
            "MATCH (service:Service) RETURN ['a'] + 1 AS values",
            "static list concatenation requires compatible non-null element types",
        ),
        (
            "MATCH (service:Service) RETURN ['a', 1] + [] AS values",
            "static list concatenation requires each operand to have a single non-null element type",
        ),
        (
            "MATCH (service:Service) RETURN [] + [null] AS values",
            "static list expressions require a known non-null element type",
        ),
    ] {
        let error = compile_cypher_for_graph(&star_test_graph(), cypher).expect_err(expected);
        assert!(
            error.to_string().contains(expected),
            "expected '{expected}' in error: {error}"
        );
    }
}

#[test]
fn compiles_static_list_comparison_predicates() {
    let graph = star_test_graph();
    let parameters = BTreeMap::from([(
        "tiers".to_string(),
        CypherParameterValue::List(vec![
            Literal::String("prod".to_string()),
            Literal::String("dev".to_string()),
        ]),
    )]);
    let plan = compile_cypher_for_graph_with_parameters(
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE tail(keys(service)) = ['tier'] \
               AND [] = tail(labels(service)) \
               AND tail($tiers) <> ['prod'] \
             RETURN service.name AS service",
        &parameters,
    )
    .expect("static list comparison predicates should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::And {
                left: Box::new(PredicateExpression::Boolean(true)),
                right: Box::new(PredicateExpression::Boolean(true)),
            }),
            right: Box::new(PredicateExpression::Boolean(true)),
        })
    );
}

#[test]
fn compiles_static_list_quantifier_predicates() {
    let graph = star_test_graph();
    let parameters = BTreeMap::from([(
        "selected_keys".to_string(),
        CypherParameterValue::List(vec![
            Literal::String("missing".to_string()),
            Literal::String("tier".to_string()),
        ]),
    )]);
    let plan = compile_cypher_for_graph_with_parameters(
        &graph,
        "MATCH (service:Service) \
             WHERE all(key IN keys(service) WHERE key <> 'deprecated') \
               AND any(key IN tail(keys(service)) WHERE key = 'tier') \
               AND none(label IN labels(service) WHERE label = 'Team') \
               AND single(key IN ['name', 'tier', 'risk'] WHERE key STARTS WITH 'r') \
               AND any(key IN $selected_keys WHERE key IN keys(service)) \
             RETURN all(key IN keys(service) WHERE key <> 'deprecated') AS all_declared, \
                    any(label IN labels(service) WHERE label = 'Service') AS has_label",
        &parameters,
    )
    .expect("static list collection predicates should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::And {
                left: Box::new(PredicateExpression::And {
                    left: Box::new(PredicateExpression::And {
                        left: Box::new(PredicateExpression::Boolean(true)),
                        right: Box::new(PredicateExpression::Boolean(true)),
                    }),
                    right: Box::new(PredicateExpression::Boolean(true)),
                }),
                right: Box::new(PredicateExpression::Boolean(true)),
            }),
            right: Box::new(PredicateExpression::Boolean(true)),
        })
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::Predicate(Box::new(PredicateExpression::Boolean(
                    true
                ),)),
                alias: "all_declared".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Predicate(Box::new(PredicateExpression::Boolean(
                    true
                ),)),
                alias: "has_label".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_static_list_case_and_coalesce_quantifier_predicates() {
    let query = compile_cypher_query_for_graph(
            &star_test_graph(),
            "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN any(key IN CASE WHEN person IS NULL THEN ['fallback'] ELSE keys(person) END WHERE key = 'team') AS case_has_team_key, \
                    all(key IN coalesce(keys(person), ['fallback']) WHERE key <> 'deprecated') AS coalesced_all_declared, \
                    none(key IN CASE WHEN service.tier = 'prod' THEN [] ELSE null END WHERE key = 'x') AS empty_none \
             ORDER BY any(key IN CASE WHEN person IS NULL THEN ['fallback'] ELSE keys(person) END WHERE key = 'team')",
        )
        .expect("static list CASE/coalesce collection predicates should compile");

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
            Projection::Expression {
                expression: ScalarExpression::Predicate(empty_predicate),
                alias: empty_alias,
            },
        ] if case_alias == "case_has_team_key"
            && coalesce_alias == "coalesced_all_declared"
            && empty_alias == "empty_none"
            && matches!(
                case_predicate.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::Case { .. },
                    operator: ComparisonOperator::Equal,
                    rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                        Literal::Boolean(true),
                    )),
                })
            )
            && matches!(
                coalesce_predicate.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::Case { .. },
                    operator: ComparisonOperator::Equal,
                    rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                        Literal::Boolean(true),
                    )),
                })
            )
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
            expression: OrderExpression::Scalar(ScalarExpression::Predicate(_)),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_static_list_case_and_coalesce_slice_quantifier_predicates() {
    let query = compile_cypher_query_for_graph(
            &star_test_graph(),
            "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN any(key IN (CASE WHEN person IS NULL THEN ['fallback', 'owner'] ELSE keys(person) END)[1..2] WHERE key = 'team') AS case_slice_has_team, \
                    all(key IN coalesce(keys(person), ['fallback', 'owner'])[0..1] WHERE key <> 'deprecated') AS coalesced_slice_all_declared, \
                    none(key IN (CASE WHEN service.tier = 'prod' THEN [] ELSE ['not-prod'] END)[0..1] WHERE key = 'prod') AS tier_slice_none_prod, \
                    single(key IN coalesce(keys(person), ['fallback', 'owner'])[0..1] WHERE key STARTS WITH 'f') AS coalesced_slice_single_fallback \
             ORDER BY any(key IN (CASE WHEN person IS NULL THEN ['fallback', 'owner'] ELSE keys(person) END)[1..2] WHERE key = 'team')",
        )
        .expect("sliced static list CASE/coalesce collection predicates should compile");

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
                expression: ScalarExpression::Predicate(all_predicate),
                alias: all_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Predicate(none_predicate),
                alias: none_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Predicate(single_predicate),
                alias: single_alias,
            },
        ] if case_alias == "case_slice_has_team"
            && all_alias == "coalesced_slice_all_declared"
            && none_alias == "tier_slice_none_prod"
            && single_alias == "coalesced_slice_single_fallback"
            && is_case_boolean_scalar_predicate(case_predicate.as_ref())
            && is_case_boolean_scalar_predicate(all_predicate.as_ref())
            && is_case_boolean_scalar_predicate(none_predicate.as_ref())
            && is_case_boolean_scalar_predicate(single_predicate.as_ref())
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
fn compiles_static_list_quantifiers_without_where_and_preserves_unknown() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN all(flag IN [true, true]) AS all_true, \
                    any(flag IN [false, null]) AS any_unknown, \
                    none(flag IN [false]) AS none_true, \
                    single(flag IN [true, null]) AS single_unknown",
    )
    .expect("boolean list collection predicates should compile");

    assert_eq!(
        plan.projections
            .first()
            .expect("expected all_true projection"),
        &Projection::Expression {
            expression: ScalarExpression::Predicate(Box::new(PredicateExpression::Boolean(true))),
            alias: "all_true".to_string(),
        }
    );
    assert!(matches!(
        plan.projections
            .get(1)
            .expect("expected any_unknown projection"),
        Projection::Expression {
            expression: ScalarExpression::Predicate(predicate),
            alias,
        } if alias == "any_unknown"
            && matches!(
                predicate.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::Literal(Literal::Null),
                    operator: ComparisonOperator::Equal,
                    rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                        Literal::Boolean(true)
                    )),
                })
            )
    ));
    assert_eq!(
        plan.projections
            .get(2)
            .expect("expected none_true projection"),
        &Projection::Expression {
            expression: ScalarExpression::Predicate(Box::new(PredicateExpression::Boolean(true))),
            alias: "none_true".to_string(),
        }
    );
    assert!(matches!(
        plan.projections
            .get(3)
            .expect("expected single_unknown projection"),
        Projection::Expression {
            expression: ScalarExpression::Predicate(predicate),
            alias,
        } if alias == "single_unknown"
            && matches!(
                predicate.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::Literal(Literal::Null),
                    operator: ComparisonOperator::Equal,
                    rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                        Literal::Boolean(true)
                    )),
                })
            )
    ));
}

#[test]
fn rejects_dynamic_list_quantifier_collections() {
    let graph = star_test_graph();
    let error = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             WHERE any(key IN service.name WHERE key = 'billing-api') \
             RETURN service.name AS service",
    )
    .expect_err("dynamic collection predicates should be rejected");

    assert!(
            error
                .to_string()
                .contains("collection predicates require a literal list, list parameter, static split(...), range(...), tail(...), or static labels()/keys() metadata list"),
            "unexpected error: {error}"
        );
}

#[test]
fn rejects_tail_with_ambiguous_list_element_type() {
    for cypher in [
        "MATCH (service:Service) RETURN tail([]) AS values",
        "MATCH (service:Service) RETURN tail([null]) AS values",
        "MATCH (service:Service) RETURN tail([1, 'prod']) AS values",
    ] {
        let error = compile_cypher(cypher).expect_err("ambiguous tail() should be rejected");
        assert!(
            error
                .to_string()
                .contains("tail() requires a list with a known non-null element type"),
            "unexpected error: {error}"
        );
    }
}

#[test]
fn compiles_metadata_list_size_scalar_expressions() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE size(labels(service)) = 1 \
               AND size(keys(owns)) = 2 \
             RETURN size(labels(service)) AS service_label_count, \
                    size(keys(service)) AS service_key_count \
             ORDER BY size(keys(owns))",
    )
    .expect("metadata list sizes should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Literal(Literal::Integer(1)),
                operator: ComparisonOperator::Equal,
                rhs: ScalarPredicateRhs::Expression(
                    ScalarExpression::Literal(Literal::Integer(1),)
                ),
            })),
            right: Box::new(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Literal(Literal::Integer(2)),
                operator: ComparisonOperator::Equal,
                rhs: ScalarPredicateRhs::Expression(
                    ScalarExpression::Literal(Literal::Integer(2),)
                ),
            })),
        })
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Integer(1)),
                alias: "service_label_count".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Integer(2)),
                alias: "service_key_count".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Literal(Literal::Integer(2,))),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_ordered_metadata_list_predicates() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             WHERE labels(service) > ['Account'] \
             RETURN service.name AS service",
    )
    .expect("ordered metadata list predicates should compile");

    assert_eq!(plan.predicate, Some(PredicateExpression::Boolean(true)));
}

#[test]
fn compiles_ordered_static_list_predicates_with_literal_left_side() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             WHERE ['Team'] > labels(service) \
               AND labels(service) <= ['Service', 'z'] \
               AND [1, 2] < [1, 3] \
             RETURN service.name AS service",
    )
    .expect("ordered static list predicates should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::And {
                left: Box::new(PredicateExpression::Boolean(true)),
                right: Box::new(PredicateExpression::Boolean(true)),
            }),
            right: Box::new(PredicateExpression::Boolean(true)),
        })
    );
}

#[test]
fn compiles_null_ordered_static_list_predicates_as_unknown() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             RETURN [null] < [null] AS unknown_order",
    )
    .expect("null ordered static list predicates should compile to unknown");

    assert!(matches!(
        plan.projections.as_slice(),
        [Projection::Expression {
            expression: ScalarExpression::Predicate(predicate),
            alias,
        }] if alias == "unknown_order"
            && matches!(
                predicate.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::Literal(Literal::Null),
                    operator: ComparisonOperator::Equal,
                    rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                        Literal::Boolean(true)
                    )),
                })
            )
    ));
}

#[test]
fn rejects_boolean_ordered_static_list_predicates() {
    let graph = star_test_graph();
    let error = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             WHERE [true] < [false] \
             RETURN service.name AS service",
    )
    .expect_err("boolean ordered static list predicates should be rejected");

    assert!(
        error
            .to_string()
            .contains("ordered static list predicates require string or numeric list elements"),
        "{error:?}"
    );
}

#[test]
fn rejects_cross_family_ordered_static_list_predicates() {
    let graph = star_test_graph();
    let error = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             WHERE labels(service) < [1] \
             RETURN service.name AS service",
    )
    .expect_err("cross-family ordered static list predicates should be rejected");

    assert!(
        error.to_string().contains("same orderable element family"),
        "{error:?}"
    );
}

#[test]
fn compiles_parameterized_label_membership_predicates() {
    let parameters = BTreeMap::from([(
        "label".to_string(),
        CypherParameterValue::Literal(Literal::String("Service".to_string())),
    )]);
    let plan = compile_cypher_with_parameters(
        "MATCH (service:Service) \
             WHERE $label IN labels(service) \
             RETURN service.name AS service",
        &parameters,
    )
    .expect("parameterized labels() membership should compile");

    assert_eq!(plan.predicate, Some(PredicateExpression::Boolean(true)));
}

#[test]
fn rejects_non_string_label_membership_predicates() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             WHERE 1 IN labels(service) \
             RETURN service.name AS service",
    )
    .expect_err("label membership should require a string literal");

    assert!(
        error.to_string().contains("label membership predicates"),
        "{error:?}"
    );
}

#[test]
fn compiles_node_label_predicates_as_boolean_constants() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE service:Service AND NOT (service:Team) \
             RETURN service.name AS service",
    )
    .expect("node label predicates should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::Boolean(true)),
            right: Box::new(PredicateExpression::Not {
                expression: Box::new(PredicateExpression::Boolean(false)),
            }),
        })
    );
}

#[test]
fn compiles_compound_node_label_predicates_as_boolean_constants() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE service:Service|Team \
             RETURN service.name AS service",
    )
    .expect("compound node label predicates should compile");

    assert_eq!(plan.predicate, Some(PredicateExpression::Boolean(true)));
}

#[test]
fn compiles_relationship_type_predicates_as_boolean_constants() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE owns:OWNS AND NOT (owns:DEPENDS_ON) \
             RETURN service.name AS service",
    )
    .expect("relationship type predicates should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::Boolean(true)),
            right: Box::new(PredicateExpression::Not {
                expression: Box::new(PredicateExpression::Boolean(false)),
            }),
        })
    );
}

#[test]
fn compiles_boolean_scalar_projections() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN service.risk > 0.8 AS high_risk, \
                    service.tier IS NULL AS missing_tier, \
                    service.name =~ '^billing.*' AS billing_service, \
                    service:Service AS is_service, \
                    owns:OWNS AS is_ownership, \
                    'tier' IN keys(service) AS has_tier \
             ORDER BY service.risk > 0.8 DESC",
    )
    .expect("boolean scalar projections should compile");

    assert_eq!(plan.projections.len(), 6);
    assert!(plan.projections.iter().all(|projection| {
        matches!(
            projection,
            Projection::Expression {
                expression: ScalarExpression::Predicate(_),
                ..
            }
        )
    }));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Predicate(_)),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_is_empty_metadata_as_boolean_scalar_projections() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN isEmpty(labels(service)) AS service_labels_empty, \
                    isEmpty(keys(service)) AS service_keys_empty, \
                    isEmpty(keys(owns)) AS ownership_keys_empty \
             ORDER BY isEmpty(keys(owns))",
    )
    .expect("isEmpty metadata scalar projections should compile");

    assert_eq!(plan.projections.len(), 3);
    assert!(plan.projections.iter().all(|projection| {
        matches!(
            projection,
            Projection::Expression {
                expression: ScalarExpression::Predicate(_),
                ..
            }
        )
    }));
    let Projection::Expression {
        expression: ScalarExpression::Predicate(predicate),
        alias,
    } = plan
        .projections
        .first()
        .expect("expected labels isEmpty projection")
    else {
        panic!("expected labels isEmpty predicate projection");
    };
    assert_eq!(alias, "service_labels_empty");
    assert!(matches!(
        predicate.as_ref(),
        PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Literal(Literal::Boolean(false)),
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Boolean(true))),
        })
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
fn rejects_is_empty_keys_without_graph_declaration() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             RETURN isEmpty(keys(service)) AS service_keys_empty",
    )
    .expect_err("keys metadata emptiness should require graph declaration");

    assert!(
        error.to_string().contains("requires a graph declaration"),
        "{error}"
    );
}

#[test]
fn compiles_to_string_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE toString(service.risk) STARTS WITH '0.9' \
             RETURN toString(service.risk) AS risk_text \
             ORDER BY toString(service.risk)",
    )
    .expect("toString scalar expressions should compile");

    let expected_expression = ScalarExpression::ToString {
        expression: Box::new(ScalarExpression::Property(PropertyRef {
            variable: "service".to_string(),
            property: "risk".to_string(),
        })),
    };
    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: expected_expression.clone(),
            operator: ComparisonOperator::StartsWith,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
                "0.9".to_string()
            ))),
        }))
    );
    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: expected_expression.clone(),
            alias: "risk_text".to_string(),
        }]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(expected_expression),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_string_case_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE toLower(service.name) CONTAINS 'api' \
             RETURN toUpper(service.tier) AS tier_upper \
             ORDER BY toLower(service.name)",
    )
    .expect("string case scalar expressions should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::ToLower {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                })),
            },
            operator: ComparisonOperator::Contains,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
                "api".to_string()
            ))),
        }))
    );
    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::ToUpper {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                })),
            },
            alias: "tier_upper".to_string(),
        }]
    );
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::ToLower { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_trim_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE trim(service.tier) = 'prod' \
             RETURN lTrim(service.name) AS left_trimmed \
             ORDER BY rTrim(service.name)",
    )
    .expect("trim scalar expressions should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Trim {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                })),
            },
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
                "prod".to_string()
            ))),
        }))
    );
    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::LTrim {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                })),
            },
            alias: "left_trimmed".to_string(),
        }]
    );
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::RTrim { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_gql_string_function_aliases() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE lower(service.name) CONTAINS 'api' \
             RETURN upper(service.tier) AS tier_upper, \
                    btrim(service.name) AS name_trimmed \
             ORDER BY btrim(service.name)",
    )
    .expect("GQL string function aliases should compile");

    assert!(matches!(
        &plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::ToLower { .. },
            operator: ComparisonOperator::Contains,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(value))),
        })) if value == "api"
    ));
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::ToUpper { .. },
                alias: tier_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Trim { .. },
                alias: trim_alias,
            },
        ] if tier_alias == "tier_upper" && trim_alias == "name_trimmed"
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Trim { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn rejects_gql_string_aliases_with_unsupported_arity() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             RETURN btrim(service.name, '-') AS name_trimmed",
    )
    .expect_err("btrim() should require one argument");

    assert!(
        error
            .to_string()
            .contains("btrim() requires exactly one argument"),
        "{error}"
    );
}

#[test]
fn compiles_replace_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE replace(service.name, '-', '') = 'billingapi' \
             RETURN replace(service.team, 'platform', 'core') AS normalized_team \
             ORDER BY replace(service.name, '-', '')",
    )
    .expect("replace scalar expressions should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Replace {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                })),
                search: Box::new(ScalarExpression::Literal(Literal::String("-".to_string()))),
                replacement: Box::new(ScalarExpression::Literal(Literal::String(String::new()))),
            },
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
                "billingapi".to_string()
            ))),
        }))
    );
    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::Replace {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "team".to_string(),
                })),
                search: Box::new(ScalarExpression::Literal(Literal::String(
                    "platform".to_string()
                ))),
                replacement: Box::new(ScalarExpression::Literal(Literal::String(
                    "core".to_string()
                ))),
            },
            alias: "normalized_team".to_string(),
        }]
    );
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Replace { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_character_length_and_substring_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE size(service.name) > 10 \
             RETURN substring(service.name, 0, 7) AS prefix, \
                    char_length(service.tier) AS tier_length \
             ORDER BY character_length(service.name)",
    )
    .expect("string length and substring scalar expressions should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::CharacterLength {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                })),
            },
            operator: ComparisonOperator::GreaterThan,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(10))),
        }))
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::Substring {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    })),
                    start: Box::new(ScalarExpression::Literal(Literal::Integer(0))),
                    length: Some(Box::new(ScalarExpression::Literal(Literal::Integer(7)))),
                },
                alias: "prefix".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::CharacterLength {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    })),
                },
                alias: "tier_length".to_string(),
            },
        ]
    );
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::CharacterLength { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_is_empty_string_predicates() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE isEmpty(trim(service.tier)) OR NOT isEmpty(service.name) \
             RETURN service.name",
    )
    .expect("isEmpty predicates should compile");

    assert!(plan.predicates.is_empty());
    assert!(matches!(
        &plan.predicate,
        Some(PredicateExpression::Or { left, right })
            if matches!(
                left.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::CharacterLength { expression },
                    operator: ComparisonOperator::Equal,
                    rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(0))),
                }) if matches!(expression.as_ref(), ScalarExpression::Trim { .. })
            ) && matches!(
                right.as_ref(),
                PredicateExpression::Not { expression }
                    if matches!(
                        expression.as_ref(),
                        PredicateExpression::ScalarComparison(ScalarPredicate {
                            lhs: ScalarExpression::CharacterLength { expression },
                            operator: ComparisonOperator::Equal,
                            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(0))),
                        }) if matches!(
                            expression.as_ref(),
                            ScalarExpression::Property(PropertyRef { property, .. }) if property == "name"
                        )
                    )
            )
    ));
}

#[test]
fn rejects_is_empty_with_unsupported_arity() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             WHERE isEmpty(service.name, service.tier) \
             RETURN service.name",
    )
    .expect_err("isEmpty() requires one argument");

    assert!(
        error
            .to_string()
            .contains("isEmpty() supports exactly one scalar string argument"),
        "{error}"
    );
}

#[test]
fn rejects_substring_with_unsupported_arity() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             RETURN substring(service.name) AS prefix",
    )
    .expect_err("substring() requires a start argument");

    assert!(
        error
            .to_string()
            .contains("substring() requires exactly two or three arguments"),
        "{error}"
    );
}

#[test]
fn compiles_left_right_and_reverse_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE left(service.name, 7) = 'billing' \
             RETURN right(service.name, 3) AS suffix, \
                    reverse(service.tier) AS reversed_tier \
             ORDER BY reverse(service.name)",
    )
    .expect("left, right, and reverse scalar expressions should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Left {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                })),
                count: Box::new(ScalarExpression::Literal(Literal::Integer(7))),
            },
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
                "billing".to_string()
            ))),
        }))
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::Right {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    })),
                    count: Box::new(ScalarExpression::Literal(Literal::Integer(3))),
                },
                alias: "suffix".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Reverse {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    })),
                },
                alias: "reversed_tier".to_string(),
            },
        ]
    );
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Reverse { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_indices_lpad_and_rpad_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE lpad(service.name, 13, '*') = '**billing-api' \
             RETURN indices(service.name, 'i') AS name_indices, \
                    rpad(service.tier, 8, '-') AS padded_tier \
             ORDER BY indices(service.name, 'i')",
    )
    .expect("indices, lpad, and rpad scalar expressions should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::LPad {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                })),
                length: Box::new(ScalarExpression::Literal(Literal::Integer(13))),
                fill: Box::new(ScalarExpression::Literal(Literal::String("*".to_string()))),
            },
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
                "**billing-api".to_string()
            ))),
        }))
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::StringIndices {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    })),
                    pattern: Box::new(ScalarExpression::Literal(Literal::String("i".to_string()))),
                },
                alias: "name_indices".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::RPad {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    })),
                    length: Box::new(ScalarExpression::Literal(Literal::Integer(8))),
                    fill: Box::new(ScalarExpression::Literal(Literal::String("-".to_string()))),
                },
                alias: "padded_tier".to_string(),
            },
        ]
    );
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::StringIndices { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_string_predicate_function_scalar_projections() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN contains(service.name, 'api') AS has_api, \
                    startsWith(service.name, 'bill') AS starts_bill, \
                    endsWith(service.name, 'api') AS ends_api \
             ORDER BY contains(service.name, 'api') DESC",
    )
    .expect("string predicate function scalar projections should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::StringContains {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    })),
                    pattern: Box::new(ScalarExpression::Literal(Literal::String(
                        "api".to_string()
                    ))),
                },
                alias: "has_api".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::StringStartsWith {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    })),
                    pattern: Box::new(ScalarExpression::Literal(Literal::String(
                        "bill".to_string()
                    ))),
                },
                alias: "starts_bill".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::StringEndsWith {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    })),
                    pattern: Box::new(ScalarExpression::Literal(Literal::String(
                        "api".to_string()
                    ))),
                },
                alias: "ends_api".to_string(),
            },
        ]
    );
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::StringContains { .. }),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_string_predicate_functions_as_boolean_predicates() {
    let plan = compile_cypher(
            "MATCH (service:Service) \
             WHERE contains(service.name, 'api') \
             RETURN CASE WHEN startsWith(service.name, 'bill') THEN 'billing' ELSE 'other' END AS bucket \
             ORDER BY endsWith(service.name, 'api') DESC",
        )
        .expect("string predicate functions should compile as boolean predicates");

    assert!(matches!(
        &plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::StringContains {
                expression,
                pattern,
            },
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Boolean(
                true
            ))),
        })) if matches!(
            expression.as_ref(),
            ScalarExpression::Property(PropertyRef { property, .. }) if property == "name"
        ) && matches!(
            pattern.as_ref(),
            ScalarExpression::Literal(Literal::String(pattern)) if pattern == "api"
        )
    ));
    assert!(matches!(
        &plan.projections[..],
        [Projection::Expression {
            expression: ScalarExpression::Case {
                alternatives,
                else_expression: Some(_),
            },
            alias,
        }] if alias == "bucket"
            && matches!(
                alternatives.as_slice(),
                [ScalarCaseAlternative {
                    when: PredicateExpression::ScalarComparison(ScalarPredicate {
                        lhs: ScalarExpression::StringStartsWith { .. },
                        operator: ComparisonOperator::Equal,
                        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                            Literal::Boolean(true)
                        )),
                    }),
                    then: ScalarExpression::Literal(Literal::String(bucket)),
                }] if bucket == "billing"
            )
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::StringEndsWith { .. }),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    ));
}

#[test]
fn rejects_contains_with_unsupported_arity() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             RETURN contains(service.name) AS has_api",
    )
    .expect_err("contains() requires a substring argument");

    assert!(
        error
            .to_string()
            .contains("contains() requires exactly two arguments"),
        "{error}"
    );
}

#[test]
fn rejects_left_with_unsupported_arity() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             RETURN left(service.name) AS prefix",
    )
    .expect_err("left() requires a count argument");

    assert!(
        error
            .to_string()
            .contains("left() requires exactly two arguments"),
        "{error}"
    );
}

#[test]
fn rejects_lpad_with_unsupported_arity() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             RETURN lpad(service.name, 10) AS padded",
    )
    .expect_err("lpad() requires a fill argument");

    assert!(
        error
            .to_string()
            .contains("lpad() requires exactly three arguments"),
        "{error}"
    );
}

#[test]
fn compiles_numeric_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE abs(service.risk - 1.0) < 0.2 \
             RETURN ceil(service.risk) AS risk_ceiling, \
                    floor(service.risk) AS risk_floor, \
                    round(service.risk, 1) AS risk_rounded \
             ORDER BY round(service.risk)",
    )
    .expect("numeric scalar functions should compile");

    assert!(matches!(
        &plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Abs { expression },
            operator: ComparisonOperator::LessThan,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Float(_))),
        })) if matches!(
            expression.as_ref(),
            ScalarExpression::Arithmetic {
                operator: ArithmeticOperator::Subtract,
                ..
            }
        )
    ));
    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::Ceil {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    })),
                },
                alias: "risk_ceiling".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Floor {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    })),
                },
                alias: "risk_floor".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Round {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    })),
                    places: Some(Box::new(ScalarExpression::Literal(Literal::Integer(1)))),
                },
                alias: "risk_rounded".to_string(),
            },
        ]
    );
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Round { places: None, .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_more_numeric_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE sqrt(service.risk) < 1.0 AND sign(service.risk - 0.5) = 1 \
             RETURN sqrt(service.risk) AS risk_root, \
                    sign(service.risk - 0.5) AS risk_sign, \
                    exp(service.risk) AS risk_exp, \
                    log(service.risk) AS risk_log, \
                    log10(service.risk) AS risk_log10 \
             ORDER BY log(service.risk)",
    )
    .expect("additional numeric scalar functions should compile");

    assert!(matches!(
        &plan.predicate,
        Some(PredicateExpression::And { left, right })
            if matches!(
                left.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::Sqrt { .. },
                    operator: ComparisonOperator::LessThan,
                    ..
                })
            ) && matches!(
                right.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::Sign { expression },
                    operator: ComparisonOperator::Equal,
                    rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(1))),
                }) if matches!(
                    expression.as_ref(),
                    ScalarExpression::Arithmetic {
                        operator: ArithmeticOperator::Subtract,
                        ..
                    }
                )
            )
    ));
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Sqrt { .. },
                alias
            },
            Projection::Expression {
                expression: ScalarExpression::Sign { .. },
                ..
            },
            Projection::Expression {
                expression: ScalarExpression::Exp { .. },
                ..
            },
            Projection::Expression {
                expression: ScalarExpression::Log { .. },
                ..
            },
            Projection::Expression {
                expression: ScalarExpression::Log10 { .. },
                ..
            },
        ] if alias == "risk_root"
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Log { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_is_nan_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE isNaN(service.risk) = false \
             RETURN isNaN(service.risk) AS risk_is_nan, \
                    isnan(toFloat(service.risk)) AS coerced_risk_is_nan \
             ORDER BY isNaN(service.risk)",
    )
    .expect("isNaN scalar function should compile");

    assert!(matches!(
        &plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::IsNaN { expression },
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Boolean(false))),
        })) if matches!(
            expression.as_ref(),
            ScalarExpression::Property(PropertyRef { property, .. }) if property == "risk"
        )
    ));
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::IsNaN { .. },
                alias: risk_is_nan,
            },
            Projection::Expression {
                expression: ScalarExpression::IsNaN { expression },
                alias: coerced_risk_is_nan,
            },
        ] if risk_is_nan == "risk_is_nan"
            && coerced_risk_is_nan == "coerced_risk_is_nan"
            && matches!(expression.as_ref(), ScalarExpression::ToFloat { .. })
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::IsNaN { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_gql_numeric_scalar_aliases() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN ceiling(service.risk) AS risk_ceiling, \
                    ln(service.risk) AS risk_ln \
             ORDER BY ln(service.risk)",
    )
    .expect("GQL numeric scalar aliases should compile");

    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Ceil { .. },
                alias: risk_ceiling,
            },
            Projection::Expression {
                expression: ScalarExpression::Log { .. },
                alias: risk_ln,
            },
        ] if risk_ceiling == "risk_ceiling" && risk_ln == "risk_ln"
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Log { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn rejects_round_with_unsupported_arity() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             RETURN round(service.risk, 1, 2) AS rounded",
    )
    .expect_err("round() supports only optional places");

    assert!(
        error
            .to_string()
            .contains("round() requires exactly one or two arguments"),
        "{error}"
    );
}

#[test]
fn rejects_more_numeric_scalars_with_unsupported_arity() {
    for cypher in [
        "MATCH (service:Service) RETURN sqrt() AS value",
        "MATCH (service:Service) RETURN sign(service.risk, 1) AS value",
        "MATCH (service:Service) RETURN exp(service.risk, 1) AS value",
        "MATCH (service:Service) RETURN log(service.risk, 10) AS value",
        "MATCH (service:Service) RETURN log10(service.risk, 10) AS value",
        "MATCH (service:Service) RETURN isNaN() AS value",
        "MATCH (service:Service) RETURN isNaN(service.risk, 1) AS value",
    ] {
        let error = compile_cypher(cypher).expect_err("wrong arity should be rejected");
        assert!(
            error.to_string().contains("requires exactly one argument"),
            "{error}"
        );
    }
}

#[test]
fn compiles_trigonometric_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE sin(service.risk) >= 0 AND atan2(service.risk, 1.0) < 1.0 \
             RETURN sin(service.risk) AS risk_sin, \
                    cos(service.risk) AS risk_cos, \
                    tan(service.risk) AS risk_tan, \
                    cot(service.risk) AS risk_cot, \
                    asin(0.5) AS half_asin, \
                    acos(1.0) AS one_acos, \
                    atan(service.risk) AS risk_atan, \
                    atan2(service.risk, 1.0) AS risk_atan2, \
                    degrees(service.risk) AS risk_degrees, \
                    radians(180.0) AS pi_radians \
             ORDER BY radians(degrees(service.risk))",
    )
    .expect("trigonometric scalar functions should compile");

    assert!(matches!(
        &plan.predicate,
        Some(PredicateExpression::And { left, right })
            if matches!(
                left.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::Sin { .. },
                    operator: ComparisonOperator::GreaterThanOrEqual,
                    ..
                })
            ) && matches!(
                right.as_ref(),
                PredicateExpression::ScalarComparison(ScalarPredicate {
                    lhs: ScalarExpression::Atan2 { .. },
                    operator: ComparisonOperator::LessThan,
                    ..
                })
            )
    ));
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Sin { .. },
                alias
            },
            Projection::Expression {
                expression: ScalarExpression::Cos { .. },
                ..
            },
            Projection::Expression {
                expression: ScalarExpression::Tan { .. },
                ..
            },
            Projection::Expression {
                expression: ScalarExpression::Cot { .. },
                ..
            },
            Projection::Expression {
                expression: ScalarExpression::Asin { .. },
                ..
            },
            Projection::Expression {
                expression: ScalarExpression::Acos { .. },
                ..
            },
            Projection::Expression {
                expression: ScalarExpression::Atan { .. },
                ..
            },
            Projection::Expression {
                expression: ScalarExpression::Atan2 { .. },
                ..
            },
            Projection::Expression {
                expression: ScalarExpression::Degrees { .. },
                ..
            },
            Projection::Expression {
                expression: ScalarExpression::Radians { .. },
                ..
            },
        ] if alias == "risk_sin"
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Radians { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn rejects_trigonometric_scalars_with_unsupported_arity() {
    for cypher in [
        "MATCH (service:Service) RETURN sin() AS value",
        "MATCH (service:Service) RETURN cos(service.risk, 1) AS value",
        "MATCH (service:Service) RETURN tan() AS value",
        "MATCH (service:Service) RETURN cot(service.risk, 1) AS value",
        "MATCH (service:Service) RETURN asin() AS value",
        "MATCH (service:Service) RETURN acos(service.risk, 1) AS value",
        "MATCH (service:Service) RETURN atan() AS value",
        "MATCH (service:Service) RETURN degrees(service.risk, 1) AS value",
        "MATCH (service:Service) RETURN radians() AS value",
    ] {
        let error = compile_cypher(cypher).expect_err("wrong arity should be rejected");
        assert!(
            error.to_string().contains("requires exactly one argument"),
            "{error}"
        );
    }

    for cypher in [
        "MATCH (service:Service) RETURN atan2(service.risk) AS value",
        "MATCH (service:Service) RETURN atan2(service.risk, 1, 2) AS value",
        "MATCH (service:Service) RETURN pow(service.risk) AS value",
        "MATCH (service:Service) RETURN power(service.risk, 2, 3) AS value",
    ] {
        let error = compile_cypher(cypher).expect_err("wrong arity should be rejected");
        assert!(
            error.to_string().contains("requires exactly two arguments"),
            "{error}"
        );
    }
}

#[test]
fn compiles_power_scalar_function_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE pow(service.risk, 2) > 0.5 \
             RETURN power(service.risk, 2) AS risk_squared \
             ORDER BY pow(service.risk, 2)",
    )
    .expect("power scalar functions should compile");

    let expected = ScalarExpression::Arithmetic {
        operator: ArithmeticOperator::Power,
        left: Box::new(ScalarExpression::Property(PropertyRef {
            variable: "service".to_string(),
            property: "risk".to_string(),
        })),
        right: Box::new(ScalarExpression::Literal(Literal::Integer(2))),
    };
    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: expected.clone(),
            operator: ComparisonOperator::GreaterThan,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Float(
                ordered_float::OrderedFloat(0.5),
            ))),
        }))
    );
    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: expected.clone(),
            alias: "risk_squared".to_string(),
        }]
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
fn compiles_math_constant_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE service.risk < pi() \
             RETURN pi() AS pi_value, e() AS e_value, sin(pi()) AS zeroish \
             ORDER BY e()",
    )
    .expect("math constants should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "risk".to_string(),
            }),
            operator: ComparisonOperator::LessThan,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Float(
                ordered_float::OrderedFloat(std::f64::consts::PI),
            ))),
        }))
    );
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Float(pi)),
                alias
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Float(e)),
                ..
            },
            Projection::Expression {
                expression: ScalarExpression::Sin { expression },
                ..
            },
        ] if *pi == ordered_float::OrderedFloat(std::f64::consts::PI)
            && *e == ordered_float::OrderedFloat(std::f64::consts::E)
            && alias == "pi_value"
            && matches!(expression.as_ref(), ScalarExpression::Literal(Literal::Float(value))
                if *value == ordered_float::OrderedFloat(std::f64::consts::PI))
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Literal(Literal::Float(e))),
            direction: OrderDirection::Ascending,
            nulls: None,
        }] if *e == ordered_float::OrderedFloat(std::f64::consts::E)
    ));
}

#[test]
fn rejects_math_constants_with_arguments() {
    for cypher in [
        "MATCH (service:Service) RETURN pi(1) AS value",
        "MATCH (service:Service) RETURN e(service.risk) AS value",
    ] {
        let error = compile_cypher(cypher).expect_err("math constants take no arguments");
        assert!(
            error
                .to_string()
                .contains("requires exactly zero arguments"),
            "{error}"
        );
    }
}

#[test]
fn compiles_haversin_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE haversin(service.risk) < 0.1 \
             RETURN haversin(0.0) AS zero_haversin \
             ORDER BY haversin(service.risk)",
    )
    .expect("haversin() should compile");

    assert!(matches!(
        &plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Arithmetic {
                operator: ArithmeticOperator::Divide,
                left,
                right,
            },
            operator: ComparisonOperator::LessThan,
            ..
        })) if matches!(
            left.as_ref(),
            ScalarExpression::Arithmetic {
                operator: ArithmeticOperator::Subtract,
                ..
            }
        ) && matches!(
            right.as_ref(),
            ScalarExpression::Literal(Literal::Integer(2))
        )
    ));
    assert!(matches!(
        plan.projections.as_slice(),
        [Projection::Expression {
            expression: ScalarExpression::Arithmetic {
                operator: ArithmeticOperator::Divide,
                ..
            },
            alias
        }] if alias == "zero_haversin"
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Arithmetic {
                operator: ArithmeticOperator::Divide,
                ..
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn rejects_haversin_with_unsupported_arity() {
    for cypher in [
        "MATCH (service:Service) RETURN haversin() AS value",
        "MATCH (service:Service) RETURN haversin(service.risk, 1) AS value",
    ] {
        let error = compile_cypher(cypher).expect_err("wrong arity should be rejected");
        assert!(
            error.to_string().contains("requires exactly one argument"),
            "{error}"
        );
    }
}

#[test]
fn compiles_scalar_cast_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE toInteger(service.id) = 10 \
             RETURN toFloat(service.risk) AS risk_float, \
                    toBoolean(service.active) AS active_bool \
             ORDER BY toInteger(service.id)",
    )
    .expect("scalar cast expressions should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::ToInteger {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "id".to_string(),
                })),
            },
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(10))),
        }))
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::ToFloat {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    })),
                },
                alias: "risk_float".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::ToBoolean {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "active".to_string(),
                    })),
                },
                alias: "active_bool".to_string(),
            },
        ]
    );
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::ToInteger { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn compiles_nullable_scalar_cast_expressions() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE toIntegerOrNull(service.id) = 10 \
             RETURN toStringOrNull(service.id) AS id_text, \
                    toFloatOrNull(service.risk) AS risk_float, \
                    toBooleanOrNull(service.active) AS active_bool \
             ORDER BY toIntegerOrNull(service.id)",
    )
    .expect("nullable scalar cast expressions should compile");

    assert!(matches!(
        &plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::ToIntegerOrNull { .. },
            operator: ComparisonOperator::Equal,
            ..
        }))
    ));
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::ToStringOrNull { .. },
                alias
            },
            Projection::Expression {
                expression: ScalarExpression::ToFloatOrNull { .. },
                ..
            },
            Projection::Expression {
                expression: ScalarExpression::ToBooleanOrNull { .. },
                ..
            },
        ] if alias == "id_text"
    ));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::ToIntegerOrNull { .. }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    ));
}

#[test]
fn rejects_nullable_scalar_casts_with_unsupported_arity() {
    for cypher in [
        "MATCH (service:Service) RETURN toStringOrNull() AS value",
        "MATCH (service:Service) RETURN toIntegerOrNull(service.id, 10) AS value",
        "MATCH (service:Service) RETURN toFloatOrNull() AS value",
        "MATCH (service:Service) RETURN toBooleanOrNull(service.active, false) AS value",
    ] {
        let error = compile_cypher(cypher).expect_err("wrong arity should be rejected");
        assert!(
            error.to_string().contains("requires exactly one argument"),
            "{error}"
        );
    }
}

#[test]
fn compiles_static_list_case_endpoint_functions() {
    let query = compile_cypher_query_for_graph(
            &star_test_graph(),
            "MATCH (service:Service) \
             OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
             RETURN head(CASE WHEN person IS NULL THEN [] ELSE keys(person) END) AS first_owner_key, \
                    last(CASE WHEN person IS NULL THEN [] ELSE keys(person) END) AS last_owner_key, \
                    head(coalesce(keys(person), [])) AS coalesced_first_key, \
                    last(CASE WHEN service.tier = 'prod' THEN [] ELSE null END) AS empty_last \
             ORDER BY last(CASE WHEN person IS NULL THEN [] ELSE keys(person) END)",
        )
        .expect("static list CASE head/last should compile with graph metadata");

    let GraphQuery::Plan(plan) = query else {
        panic!("expected single graph plan");
    };
    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Expression {
                expression: ScalarExpression::Case { alternatives, .. },
                alias: first_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Case { .. },
                alias: last_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Coalesce { expressions },
                alias: coalesced_alias,
            },
            Projection::Expression {
                expression: ScalarExpression::Case {
                    alternatives: empty_alternatives,
                    else_expression,
                },
                alias: empty_alias,
            },
        ] if first_alias == "first_owner_key"
            && matches!(
                alternatives.as_slice(),
                [ScalarCaseAlternative {
                    then: ScalarExpression::Literal(Literal::Null),
                    ..
                }]
            )
            && last_alias == "last_owner_key"
            && coalesced_alias == "coalesced_first_key"
            && expressions.len() == 2
            && empty_alias == "empty_last"
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
fn compiles_graph_metadata_predicates_inside_searched_case_predicates() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) \
             MATCH (person:Person)-[owns:OWNS]->(service) \
             RETURN CASE \
                      WHEN type(owns) = 'OWNS' THEN 'relationship' \
                      WHEN service:Service \
                        AND 'Service' IN labels(service) \
                        AND labels(service) = ['Service'] \
                        AND 'source' IN keys(owns) \
                        AND keys(owns) = ['since', 'source'] THEN 'metadata' \
                      ELSE 'unknown' \
                    END AS state \
             ORDER BY CASE WHEN type(owns) IN ['OWNS'] THEN 0 ELSE 1 END",
    )
    .expect("CASE graph metadata predicates should compile");

    let [
        Projection::Expression {
            expression: ScalarExpression::Case { alternatives, .. },
            ..
        },
    ] = plan.projections.as_slice()
    else {
        panic!("expected CASE expression projection");
    };
    let [relationship, metadata] = alternatives.as_slice() else {
        panic!("expected two CASE alternatives");
    };
    assert_eq!(relationship.when, PredicateExpression::Boolean(true));
    assert!(matches!(metadata.when, PredicateExpression::And { .. }));
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Case {
                alternatives,
                ..
            }),
            ..
        }] if matches!(
            alternatives.as_slice(),
            [ScalarCaseAlternative {
                when: PredicateExpression::Boolean(true),
                ..
            }]
        )
    ));
}

#[test]
fn compiles_relationship_type_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN coalesce(type(owns), 'missing') AS rel_type, \
                    CASE WHEN service.tier = 'prod' THEN type(owns) ELSE 'other' END AS rel_bucket \
             ORDER BY coalesce(type(owns), 'missing')",
    )
    .expect("relationship type scalar expressions should compile");

    let [
        Projection::Expression {
            expression:
                ScalarExpression::Coalesce {
                    expressions: coalesce_expressions,
                },
            alias: coalesce_alias,
        },
        Projection::Expression {
            expression:
                ScalarExpression::Case {
                    alternatives,
                    else_expression,
                },
            alias: case_alias,
        },
    ] = plan.projections.as_slice()
    else {
        panic!("expected scalar relationship type projections");
    };
    assert_eq!(coalesce_alias, "rel_type");
    assert_eq!(
        coalesce_expressions,
        &vec![
            ScalarExpression::RelationshipType {
                variable: "owns".to_string(),
                relationship_type: "OWNS".to_string(),
            },
            ScalarExpression::Literal(Literal::String("missing".to_string())),
        ]
    );
    assert_eq!(case_alias, "rel_bucket");
    let [alternative] = alternatives.as_slice() else {
        panic!("expected one CASE alternative");
    };
    assert_eq!(
        alternative.then,
        ScalarExpression::RelationshipType {
            variable: "owns".to_string(),
            relationship_type: "OWNS".to_string(),
        }
    );
    assert_eq!(
        else_expression.as_deref(),
        Some(&ScalarExpression::Literal(Literal::String(
            "other".to_string()
        )))
    );
    assert!(matches!(
        plan.order_by.as_slice(),
        [OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Coalesce { expressions }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }] if matches!(
            expressions.as_slice(),
            [
                ScalarExpression::RelationshipType {
                    variable,
                    relationship_type,
                },
                ScalarExpression::Literal(Literal::String(fallback)),
            ] if variable == "owns" && relationship_type == "OWNS" && fallback == "missing"
        )
    ));
}

#[test]
fn compiles_identity_scalar_expressions() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN id(service) + 1 AS next_service_id, \
                    coalesce(elementId(owns), 'missing') AS ownership_element_id, \
                    CASE WHEN service.tier = 'prod' THEN id(person) ELSE 0 END AS owner_id \
             ORDER BY toString(id(service)), coalesce(elementId(owns), 'missing')",
    )
    .expect("identity scalar expressions should compile");

    let [
        Projection::Expression {
            expression:
                ScalarExpression::Arithmetic {
                    operator: ArithmeticOperator::Add,
                    left,
                    right,
                },
            alias: next_alias,
        },
        Projection::Expression {
            expression:
                ScalarExpression::Coalesce {
                    expressions: coalesce_expressions,
                },
            alias: element_alias,
        },
        Projection::Expression {
            expression:
                ScalarExpression::Case {
                    alternatives,
                    else_expression,
                },
            alias: case_alias,
        },
    ] = plan.projections.as_slice()
    else {
        panic!("expected identity scalar projections");
    };
    assert_eq!(next_alias, "next_service_id");
    assert_eq!(
        left.as_ref(),
        &ScalarExpression::Key {
            variable: "service".to_string(),
        }
    );
    assert_eq!(
        right.as_ref(),
        &ScalarExpression::Literal(Literal::Integer(1))
    );
    assert_eq!(element_alias, "ownership_element_id");
    assert_eq!(
        coalesce_expressions,
        &vec![
            ScalarExpression::ElementId {
                variable: "owns".to_string(),
            },
            ScalarExpression::Literal(Literal::String("missing".to_string())),
        ]
    );
    assert_eq!(case_alias, "owner_id");
    let [alternative] = alternatives.as_slice() else {
        panic!("expected one CASE alternative");
    };
    assert_eq!(
        alternative.then,
        ScalarExpression::Key {
            variable: "person".to_string(),
        }
    );
    assert_eq!(
        else_expression.as_deref(),
        Some(&ScalarExpression::Literal(Literal::Integer(0)))
    );
    assert!(matches!(
        plan.order_by.as_slice(),
        [
            OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::ToString { expression }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::Coalesce { expressions }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ] if matches!(expression.as_ref(), ScalarExpression::Key { variable } if variable == "service")
            && matches!(expressions.as_slice(), [
                ScalarExpression::ElementId { variable },
                ScalarExpression::Literal(Literal::String(fallback)),
            ] if variable == "owns" && fallback == "missing")
    ));
}

#[test]
fn rejects_identity_scalar_expressions_on_unbound_variables() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             RETURN coalesce(id(owner), 0) AS owner_id",
    )
    .expect_err("id() over an unbound variable should be rejected");

    assert!(
        error
            .to_string()
            .contains("id() argument 'owner' is not a bound graph variable"),
        "{error:?}"
    );
}

#[test]
fn rejects_relationship_type_scalar_expressions_on_nodes() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             RETURN coalesce(type(service), 'missing') AS relationship_type",
    )
    .expect_err("type() over a node variable should be rejected");

    assert!(
        error
            .to_string()
            .contains("type() argument 'service' is not a named relationship variable"),
        "{error:?}"
    );
}

#[test]
fn rejects_invalid_coalesce_projections() {
    for (cypher, expected) in [
        (
            "MATCH (service:Service) RETURN coalesce(service.team) AS owner_team",
            "at least two arguments",
        ),
        (
            "MATCH (service:Service) RETURN coalesce(labels(service), 'unknown') AS owner_team",
            "list-valued coalesce() requires every non-null argument to be a static list",
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
fn compiles_unaliased_same_function_aggregates_with_expression_names() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN count(service), count(service.name)",
    )
    .expect("unaliased same-function aggregates should compile");

    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Aggregate {
                function: AggregateFunction::Count,
                target: AggregateTarget::VariableKey { variable },
                distinct: false,
                alias,
            },
            Projection::Aggregate {
                function: AggregateFunction::Count,
                target: AggregateTarget::Property(PropertyRef { variable: property_variable, property }),
                distinct: false,
                alias: property_alias,
            },
        ] if variable == "service"
            && alias == "count(service)"
            && property_variable == "service"
            && property == "name"
            && property_alias == "count(service.name)"
    ));
}

#[test]
fn compiles_properties_function_aggregate_targets() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN properties(service).tier AS tier, \
                    count(properties(service).name) AS services \
             ORDER BY tier",
    )
    .expect("properties(variable).property aggregate targets should compile");

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
                alias: "services".to_string(),
            },
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
fn compiles_numeric_aggregate_projections() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN service.tier AS tier, \
                    sum(service.risk) AS total_risk, \
                    avg(service.risk) AS average_risk, \
                    min(service.risk) AS lowest_risk, \
                    min(DISTINCT service.risk) AS distinct_lowest_risk, \
                    max(DISTINCT service.risk) AS highest_risk \
             ORDER BY average_risk DESC",
    )
    .expect("numeric aggregate query should compile");

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
                function: super::AggregateFunction::Sum,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: false,
                alias: "total_risk".to_string(),
            },
            Projection::Aggregate {
                function: super::AggregateFunction::Avg,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: false,
                alias: "average_risk".to_string(),
            },
            Projection::Aggregate {
                function: super::AggregateFunction::Min,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: false,
                alias: "lowest_risk".to_string(),
            },
            Projection::Aggregate {
                function: super::AggregateFunction::Min,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: true,
                alias: "distinct_lowest_risk".to_string(),
            },
            Projection::Aggregate {
                function: super::AggregateFunction::Max,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: true,
                alias: "highest_risk".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("average_risk".to_string()),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_statistical_aggregate_projections() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN stDev(service.risk) AS sample_risk, \
                    stDevP(service.risk) AS population_risk",
    )
    .expect("statistical aggregate query should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Aggregate {
                function: super::AggregateFunction::StdDev,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: false,
                alias: "sample_risk".to_string(),
            },
            Projection::Aggregate {
                function: super::AggregateFunction::StdDevP,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: false,
                alias: "population_risk".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_percentile_cont_aggregate_projections_and_ordering() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN service.tier AS tier, \
                    percentileCont(service.risk, 0.75) AS p75_risk \
             ORDER BY percentileCont(service.risk, 0.5) DESC, tier",
    )
    .expect("percentileCont aggregate query should compile");

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
                function: super::AggregateFunction::PercentileCont {
                    percentile: ordered_float::OrderedFloat(0.75),
                },
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: false,
                alias: "p75_risk".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::Aggregate {
                    function: AggregateFunction::PercentileCont {
                        percentile: ordered_float::OrderedFloat(0.5),
                    },
                    target: AggregateTarget::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    }),
                    distinct: false,
                },
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
fn compiles_percentile_disc_aggregate_projections_and_ordering() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN service.tier AS tier, \
                    percentileDisc(service.risk, 0.75) AS p75_risk \
             ORDER BY percentileDisc(service.risk, 0.5) DESC, tier",
    )
    .expect("percentileDisc aggregate query should compile");

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
                function: super::AggregateFunction::PercentileDisc {
                    percentile: ordered_float::OrderedFloat(0.75),
                },
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: false,
                alias: "p75_risk".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::Aggregate {
                    function: AggregateFunction::PercentileDisc {
                        percentile: ordered_float::OrderedFloat(0.5),
                    },
                    target: AggregateTarget::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    }),
                    distinct: false,
                },
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
fn rejects_distinct_percentile_cont_aggregates() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             RETURN percentileCont(DISTINCT service.risk, 0.75) AS p75_risk",
    )
    .expect_err("distinct percentileCont should be rejected before SQL lowering");

    assert!(error.to_string().contains("percentileCont(DISTINCT"));
    assert!(error.to_string().contains("DataFusion 53"));
}

#[test]
fn rejects_distinct_percentile_disc_aggregates() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             RETURN percentileDisc(DISTINCT service.risk, 0.75) AS p75_risk",
    )
    .expect_err("distinct percentileDisc should be rejected before SQL lowering");

    assert!(error.to_string().contains("percentileDisc(DISTINCT"));
    assert!(error.to_string().contains("DataFusion 53"));
}

#[test]
fn rejects_nested_percentile_disc_aggregate_targets() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             RETURN sum(percentileDisc(service.risk, 0.75)) AS nested_risk",
    )
    .expect_err("nested percentileDisc should be rejected before SQL lowering");

    assert!(error.to_string().contains("percentileDisc"), "{error}");
    assert!(error.to_string().contains("not supported here"), "{error}");
}

#[test]
fn compiles_gql_aggregate_function_aliases() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN collect_list(service.tier) AS tiers, \
                    stdev_samp(service.risk) AS sample_risk, \
                    stdev_pop(service.risk) AS population_risk",
    )
    .expect("GQL aggregate aliases should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Aggregate {
                function: super::AggregateFunction::Collect,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                }),
                distinct: false,
                alias: "tiers".to_string(),
            },
            Projection::Aggregate {
                function: super::AggregateFunction::StdDev,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: false,
                alias: "sample_risk".to_string(),
            },
            Projection::Aggregate {
                function: super::AggregateFunction::StdDevP,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: false,
                alias: "population_risk".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_distinct_standard_deviation_aggregate_projections() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN stDevP(DISTINCT service.risk) AS population_risk",
    )
    .expect("distinct standard-deviation aggregate should compile");

    assert_eq!(
        plan.projections,
        vec![Projection::Aggregate {
            function: super::AggregateFunction::StdDevP,
            target: AggregateTarget::Property(PropertyRef {
                variable: "service".to_string(),
                property: "risk".to_string(),
            }),
            distinct: true,
            alias: "population_risk".to_string(),
        }]
    );
}

#[test]
fn compiles_median_aggregate_projections() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             RETURN median(service.risk) AS median_risk, \
                    median(DISTINCT service.risk) AS distinct_median_risk",
    )
    .expect("median aggregate query should compile");

    assert!(matches!(
        plan.projections.as_slice(),
        [
            Projection::Aggregate {
                target: AggregateTarget::Property(PropertyRef {
                    variable,
                    property,
                }),
                distinct: false,
                alias,
                ..
            },
            Projection::Aggregate {
                distinct: true,
                alias: distinct_alias,
                ..
            },
        ] if variable == "service"
            && property == "risk"
            && alias == "median_risk"
            && distinct_alias == "distinct_median_risk"
    ));
}

#[test]
fn rejects_unsupported_return_functions() {
    assert_unsupported("MATCH (service:Service) RETURN id(missing)");
    assert_unsupported("MATCH (service:Service) RETURN type(service)");
}
