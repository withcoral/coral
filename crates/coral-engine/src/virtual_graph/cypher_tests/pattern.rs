use super::*;

#[test]
fn compiles_static_node_label_alternatives_as_union_all() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team) \
             RETURN entity.name AS name",
    )
    .expect("static node label alternatives should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert_eq!(
        union.first.nodes.first().expect("first node").label,
        "Person"
    );
    assert_eq!(union.branches.len(), 1);
    let branch = union.branches.first().expect("alternative branch");
    assert!(branch.all);
    assert_eq!(
        branch.plan.nodes.first().expect("branch node").label,
        "Team"
    );
    assert_eq!(projection_names(&union.first), vec!["name".to_string()]);
    assert_eq!(projection_names(&branch.plan), vec!["name".to_string()]);
}

#[test]
fn graph_aware_compiles_unlabeled_standalone_node_scan_as_declared_label_union() {
    let graph = star_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (entity) \
             RETURN entity.name AS name \
             ORDER BY entity.name",
    )
    .expect("graph declaration should expand an unlabeled standalone node scan");

    let GraphQuery::Union(union) = query else {
        panic!("unlabeled graph-aware node scan should expand into a union query");
    };
    let labels = std::iter::once(&union.first)
        .chain(union.branches.iter().map(|branch| &branch.plan))
        .map(|plan| plan.nodes.first().expect("branch node").label.as_str())
        .collect::<Vec<_>>();
    assert_eq!(labels, vec!["Person", "Service", "Team"]);
    assert!(union.branches.iter().all(|branch| branch.all));
    assert_eq!(projection_names(&union.first), vec!["name".to_string()]);
}

#[test]
fn deduplicates_static_node_label_alternatives_before_union_expansion() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Person) \
             RETURN entity.name AS name",
    )
    .expect("duplicate static node label alternatives should compile");

    let GraphQuery::Plan(plan) = query else {
        panic!("duplicate static label alternatives should collapse to one graph plan");
    };
    assert_eq!(plan.nodes.first().expect("first node").label, "Person");
}

#[test]
fn deduplicates_static_relationship_type_alternatives_before_union_expansion() {
    let query = compile_cypher_query(
        "MATCH (source:Service)-[relationship:DEPENDS_ON|DEPENDS_ON]->(target:Service) \
             RETURN type(relationship) AS relationship_type",
    )
    .expect("duplicate static relationship type alternatives should compile");

    let GraphQuery::Plan(plan) = query else {
        panic!("duplicate static relationship type alternatives should collapse to one graph plan");
    };
    assert_eq!(
        plan.relationships
            .first()
            .expect("first relationship")
            .relationship_type,
        "DEPENDS_ON"
    );
}

#[test]
fn rejects_static_label_alternatives_that_exceed_branch_cap() {
    let labels = (0..=MAX_PATTERN_ALTERNATIVE_BRANCHES)
        .map(|index| format!("Label{index}"))
        .collect::<Vec<_>>()
        .join("|");
    let cypher = format!("MATCH (entity:{labels}) RETURN entity.name AS name");

    let error = compile_cypher_query(&cypher)
        .expect_err("excessive static label alternatives should be capped");

    assert!(error.to_string().contains("more than 64 branches"));
}

#[test]
fn compiles_static_relationship_type_alternatives_as_union_all() {
    let query = compile_cypher_query(
        "MATCH (source:Service)-[relationship:DEPENDS_ON|OWNS]->(target:Service) \
             RETURN type(relationship) AS relationship_type",
    )
    .expect("static relationship type alternatives should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected static relationship type alternatives to expand into a union query");
    };
    assert_eq!(
        union
            .first
            .relationships
            .first()
            .expect("first relationship")
            .relationship_type,
        "DEPENDS_ON"
    );
    assert_eq!(union.branches.len(), 1);
    let branch = union.branches.first().expect("alternative branch");
    assert!(branch.all);
    assert_eq!(
        branch
            .plan
            .relationships
            .first()
            .expect("branch relationship")
            .relationship_type,
        "OWNS"
    );
}

#[test]
fn compiles_static_label_alternatives_with_outer_count_star() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team) \
             RETURN count(*) AS count",
    )
    .expect("count(*) should compile as an outer union aggregate");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert_eq!(
        union.outer_projection,
        Some(GraphUnionOuterProjection {
            items: vec![GraphUnionOuterProjectionItem::CountAll {
                alias: "count".to_string(),
            }],
            group_by: Vec::new(),
        })
    );
    assert_eq!(
        union.first.projection_output_names(),
        vec!["__coral_count_row".to_string()]
    );
}

#[test]
fn compiles_static_label_alternatives_with_outer_count_star_ordering() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team) \
             RETURN count(*) AS count \
             ORDER BY count(*)",
    )
    .expect("count(*) order expression should compile as an outer union aggregate alias");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert_eq!(
        union.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("count".to_string()),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_static_label_alternatives_with_grouped_count_star() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team) \
             RETURN entity.name AS name, count(*) AS count",
    )
    .expect("grouped count(*) should compile as an outer union aggregate");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert_eq!(
        union.outer_projection,
        Some(GraphUnionOuterProjection {
            items: vec![
                GraphUnionOuterProjectionItem::Column {
                    name: "name".to_string(),
                },
                GraphUnionOuterProjectionItem::CountAll {
                    alias: "count".to_string(),
                },
            ],
            group_by: vec!["name".to_string()],
        })
    );
    assert_eq!(
        union.first.projection_output_names(),
        vec!["name".to_string()]
    );
}

#[test]
fn compiles_static_label_alternatives_with_grouped_count_star_ordering() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team) \
             RETURN entity.name AS name, count(*) AS count \
             ORDER BY count(*) DESC, entity.name",
    )
    .expect("grouped count(*) order expressions should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert_eq!(
        union.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::ProjectionAlias("count".to_string()),
                direction: OrderDirection::Descending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::ProjectionAlias("name".to_string()),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_static_label_alternatives_with_grouped_count_star_first() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team) \
             RETURN count(*) AS count, entity.name AS name",
    )
    .expect("grouped count(*) should preserve RETURN item order");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    let outer_projection = union
        .outer_projection
        .expect("expected an outer union projection");
    assert_eq!(
        outer_projection.output_names(),
        vec!["count".to_string(), "name".to_string()]
    );
    assert_eq!(outer_projection.group_by, vec!["name".to_string()]);
}

#[test]
fn compiles_static_label_alternatives_with_grouped_count_property() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team)-[:OWNS]->(service:Service) \
             RETURN entity.name AS name, count(service.name) AS named_services \
             ORDER BY count(service.name) DESC, name",
    )
    .expect("grouped count(property) should compile as an outer union aggregate");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert_eq!(
        union.first.projection_output_names(),
        vec!["name".to_string(), "__coral_agg_1".to_string()]
    );
    assert_eq!(
        union.outer_projection,
        Some(GraphUnionOuterProjection {
            items: vec![
                GraphUnionOuterProjectionItem::Column {
                    name: "name".to_string(),
                },
                GraphUnionOuterProjectionItem::Aggregate {
                    function: AggregateFunction::Count,
                    source: "__coral_agg_1".to_string(),
                    distinct: false,
                    alias: "named_services".to_string(),
                },
            ],
            group_by: vec!["name".to_string()],
        })
    );
    assert_eq!(
        union.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::ProjectionAlias("named_services".to_string()),
                direction: OrderDirection::Descending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::ProjectionAlias("name".to_string()),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_static_label_alternatives_with_count_node_projection() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team)-[:OWNS]->(service:Service) \
             RETURN entity.name AS name, count(service) AS services \
             ORDER BY count(service) DESC, name",
    )
    .expect("count(node) should compile as an outer union aggregate");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert_eq!(
        union.first.projection_output_names(),
        vec!["name".to_string(), "__coral_agg_1".to_string()]
    );
    assert!(matches!(
        union.first.projections.get(1),
        Some(Projection::Expression {
            expression: ScalarExpression::GraphPresence { variable },
            alias,
        }) if variable == "service" && alias == "__coral_agg_1"
    ));
    assert_eq!(
        union.outer_projection,
        Some(GraphUnionOuterProjection {
            items: vec![
                GraphUnionOuterProjectionItem::Column {
                    name: "name".to_string(),
                },
                GraphUnionOuterProjectionItem::Aggregate {
                    function: AggregateFunction::Count,
                    source: "__coral_agg_1".to_string(),
                    distinct: false,
                    alias: "services".to_string(),
                },
            ],
            group_by: vec!["name".to_string()],
        })
    );
    assert_eq!(
        union.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::ProjectionAlias("services".to_string()),
                direction: OrderDirection::Descending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::ProjectionAlias("name".to_string()),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_static_label_alternatives_with_distinct_count_node_projection() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team)-[:OWNS]->(service:Service) \
             RETURN count(DISTINCT entity) AS owners",
    )
    .expect("distinct graph variable counts should compile through graph identity");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert_eq!(
        union.first.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::GraphIdentity {
                variable: "entity".to_string(),
            },
            alias: "__coral_agg_0".to_string(),
        }]
    );
    assert_eq!(
        union.outer_projection,
        Some(GraphUnionOuterProjection {
            items: vec![GraphUnionOuterProjectionItem::Aggregate {
                function: AggregateFunction::Count,
                source: "__coral_agg_0".to_string(),
                distinct: true,
                alias: "owners".to_string(),
            }],
            group_by: Vec::new(),
        })
    );
}

#[test]
fn compiles_static_label_alternatives_with_collect_graph_variable_projection() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team) \
             RETURN collect(entity) AS entities, collect(DISTINCT entity) AS distinct_entities",
    )
    .expect("collect(node) should compile as an outer union aggregate");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert_eq!(
        union.first.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::GraphIdentity {
                    variable: "entity".to_string(),
                },
                alias: "__coral_agg_0".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::GraphIdentity {
                    variable: "entity".to_string(),
                },
                alias: "__coral_agg_1".to_string(),
            },
        ]
    );
    assert_eq!(
        union.outer_projection,
        Some(GraphUnionOuterProjection {
            items: vec![
                GraphUnionOuterProjectionItem::Aggregate {
                    function: AggregateFunction::Collect,
                    source: "__coral_agg_0".to_string(),
                    distinct: false,
                    alias: "entities".to_string(),
                },
                GraphUnionOuterProjectionItem::Aggregate {
                    function: AggregateFunction::Collect,
                    source: "__coral_agg_1".to_string(),
                    distinct: true,
                    alias: "distinct_entities".to_string(),
                },
            ],
            group_by: Vec::new(),
        })
    );
}

#[test]
fn compiles_static_label_alternatives_with_distinct_count_property() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team)-[:OWNS]->(service:Service) \
             RETURN count(DISTINCT service.name) AS named_services",
    )
    .expect("count(DISTINCT property) should compile as an outer union aggregate");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    let outer_projection = union
        .outer_projection
        .expect("expected an outer union projection");
    assert_eq!(
        outer_projection.items,
        vec![GraphUnionOuterProjectionItem::Aggregate {
            function: AggregateFunction::Count,
            source: "__coral_agg_0".to_string(),
            distinct: true,
            alias: "named_services".to_string(),
        }]
    );
}

#[test]
fn compiles_static_label_alternatives_with_numeric_property_aggregates() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team)-[:OWNS]->(service:Service) \
             RETURN entity.name AS name, \
                    sum(service.risk) AS total_risk, \
                    avg(service.risk) AS average_risk, \
                    min(service.risk) AS lowest_risk, \
                    max(service.risk) AS highest_risk \
             ORDER BY sum(service.risk) DESC",
    )
    .expect("numeric property aggregates should compile as outer union aggregates");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    let outer_projection = union
        .outer_projection
        .expect("expected an outer union projection");
    assert_eq!(
        outer_projection.output_names(),
        vec![
            "name".to_string(),
            "total_risk".to_string(),
            "average_risk".to_string(),
            "lowest_risk".to_string(),
            "highest_risk".to_string(),
        ]
    );
    assert_eq!(
        union.first.projection_output_names(),
        vec![
            "name".to_string(),
            "__coral_agg_1".to_string(),
            "__coral_agg_2".to_string(),
            "__coral_agg_3".to_string(),
            "__coral_agg_4".to_string(),
        ]
    );
    assert_eq!(
        union.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("total_risk".to_string()),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_static_label_alternatives_with_aggregate_expression_targets() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team)-[:OWNS]->(service:Service) \
             RETURN entity.name AS name, \
                    collect(DISTINCT coalesce(service.tier, 'unknown')) AS tiers, \
                    count(coalesce(service.tier, 'unknown')) AS tier_count, \
                    sum(service.risk + 1) AS adjusted_risk \
             ORDER BY sum(service.risk + 1) DESC, name",
    )
    .expect("aggregate expression targets should compile as outer union aggregates");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert_eq!(
        union.first.projection_output_names(),
        vec![
            "name".to_string(),
            "__coral_agg_1".to_string(),
            "__coral_agg_2".to_string(),
            "__coral_agg_3".to_string(),
        ]
    );
    assert!(matches!(
        union.first.projections.get(1),
        Some(Projection::Expression {
            expression: ScalarExpression::Coalesce { .. },
            alias,
        }) if alias == "__coral_agg_1"
    ));
    assert!(matches!(
        union.first.projections.get(3),
        Some(Projection::Expression {
            expression: ScalarExpression::Arithmetic { .. },
            alias,
        }) if alias == "__coral_agg_3"
    ));
    assert_eq!(
        union.outer_projection,
        Some(GraphUnionOuterProjection {
            items: vec![
                GraphUnionOuterProjectionItem::Column {
                    name: "name".to_string(),
                },
                GraphUnionOuterProjectionItem::Aggregate {
                    function: AggregateFunction::Collect,
                    source: "__coral_agg_1".to_string(),
                    distinct: true,
                    alias: "tiers".to_string(),
                },
                GraphUnionOuterProjectionItem::Aggregate {
                    function: AggregateFunction::Count,
                    source: "__coral_agg_2".to_string(),
                    distinct: false,
                    alias: "tier_count".to_string(),
                },
                GraphUnionOuterProjectionItem::Aggregate {
                    function: AggregateFunction::Sum,
                    source: "__coral_agg_3".to_string(),
                    distinct: false,
                    alias: "adjusted_risk".to_string(),
                },
            ],
            group_by: vec!["name".to_string()],
        })
    );
    assert_eq!(
        union.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::ProjectionAlias("adjusted_risk".to_string()),
                direction: OrderDirection::Descending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::ProjectionAlias("name".to_string()),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_static_label_alternatives_with_predicate_aggregate_targets() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team)-[:OWNS]->(service:Service) \
             RETURN entity.name AS name, collect(service.risk > 0.8) AS high_risk_flags",
    )
    .expect("predicate aggregate target should compile as an outer union aggregate");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert!(matches!(
        union.first.projections.get(1),
        Some(Projection::Expression {
            expression: ScalarExpression::Predicate(_),
            alias,
        }) if alias == "__coral_agg_1"
    ));
    assert_eq!(
        union.outer_projection,
        Some(GraphUnionOuterProjection {
            items: vec![
                GraphUnionOuterProjectionItem::Column {
                    name: "name".to_string(),
                },
                GraphUnionOuterProjectionItem::Aggregate {
                    function: AggregateFunction::Collect,
                    source: "__coral_agg_1".to_string(),
                    distinct: false,
                    alias: "high_risk_flags".to_string(),
                },
            ],
            group_by: vec!["name".to_string()],
        })
    );
}

#[test]
fn compiles_static_label_alternatives_with_collect_property_projection() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team)-[:OWNS]->(service:Service) \
             RETURN entity.name AS name, collect(DISTINCT service.name) AS services \
             ORDER BY name",
    )
    .expect("collect(property) should compile as an outer union aggregate");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert_eq!(
        union.outer_projection,
        Some(GraphUnionOuterProjection {
            items: vec![
                GraphUnionOuterProjectionItem::Column {
                    name: "name".to_string(),
                },
                GraphUnionOuterProjectionItem::Aggregate {
                    function: AggregateFunction::Collect,
                    source: "__coral_agg_1".to_string(),
                    distinct: true,
                    alias: "services".to_string(),
                },
            ],
            group_by: vec!["name".to_string()],
        })
    );
}

#[test]
fn compiles_static_label_alternatives_with_outer_row_modifiers() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team) \
             RETURN entity.name AS name \
             ORDER BY name DESC \
             SKIP 1 \
             LIMIT 5",
    )
    .expect("global row modifiers should compile as outer union modifiers");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert!(union.first.order_by.is_empty());
    assert_eq!(union.first.skip, None);
    assert_eq!(union.first.limit, None);
    assert_eq!(
        union.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("name".to_string()),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
    assert_eq!(union.skip, Some(1));
    assert_eq!(union.limit, Some(5));
}

#[test]
fn compiles_static_label_alternatives_with_outer_distinct() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team) \
             RETURN DISTINCT entity.name AS name \
             ORDER BY name",
    )
    .expect("RETURN DISTINCT should compile as an outer union modifier");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert!(!union.first.distinct);
    assert!(union.distinct);
    assert_eq!(
        union.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("name".to_string()),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_static_label_alternatives_with_projected_global_ordering() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team) \
             RETURN entity.name AS name \
             ORDER BY entity.name",
    )
    .expect("projected global ordering should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert_eq!(
        union.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("name".to_string()),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_static_label_alternatives_with_hidden_global_ordering() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team) \
             RETURN entity.name AS name \
             ORDER BY entity.team",
    )
    .expect("row-preserving hidden global ordering should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert_eq!(
        union.first.projection_output_names(),
        vec!["name".to_string(), "__coral_order_0".to_string()]
    );
    assert_eq!(
        union.outer_projection,
        Some(GraphUnionOuterProjection {
            items: vec![GraphUnionOuterProjectionItem::Column {
                name: "name".to_string(),
            }],
            group_by: Vec::new(),
        })
    );
    assert_eq!(
        union.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("__coral_order_0".to_string()),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn rejects_static_label_alternatives_with_aggregate_hidden_global_ordering() {
    let error = compile_cypher_query(
        "MATCH (entity:Person|Team)-[:OWNS]->(service:Service) \
             RETURN entity.name AS name, count(*) AS services \
             ORDER BY service.name",
    )
    .expect_err("aggregate hidden global ordering should require staged planning");

    assert!(error.to_string().contains("aggregate RETURN"));
}

#[test]
fn compiles_static_label_alternatives_with_terminal_with_projection() {
    let query = compile_cypher_query(
        "MATCH (owner:Person|Team)-[:OWNS]->(service:Service) \
             WITH owner.name AS owner, service.name AS service \
             WHERE service = 'billing-api' \
             RETURN owner, service \
             ORDER BY owner",
    )
    .expect("static alternatives with terminal WITH projection should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected static label alternatives to expand into a union query");
    };
    assert_eq!(
        union.first.projection_output_names(),
        vec!["owner".to_string(), "service".to_string()]
    );
    assert!(union.first.post_projection_predicate.is_some());
    assert!(union.branches.iter().all(|branch| {
        branch.plan.projection_output_names() == vec!["owner".to_string(), "service".to_string()]
            && branch.plan.post_projection_predicate.is_some()
    }));
    assert_eq!(
        union.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("owner".to_string()),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_static_label_alternatives_inside_explicit_union_all() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team) RETURN entity.name AS item \
             UNION ALL \
             MATCH (service:Service) RETURN service.name AS item",
    )
    .expect("static alternatives should flatten into top-level UNION ALL");

    let GraphQuery::Union(union) = query else {
        panic!("expected union query");
    };
    assert_eq!(projection_names(&union.first), vec!["item".to_string()]);
    assert_eq!(union.branches.len(), 2);
    assert!(union.branches.iter().all(|branch| branch.all));
    assert_eq!(
        union
            .branches
            .iter()
            .map(|branch| projection_names(&branch.plan))
            .collect::<Vec<_>>(),
        vec![vec!["item".to_string()], vec!["item".to_string()]]
    );
}

#[test]
fn compiles_static_label_alternatives_inside_explicit_union_distinct() {
    let query = compile_cypher_query(
        "MATCH (entity:Person|Team) RETURN entity.name AS item \
             UNION \
             MATCH (service:Service) RETURN service.name AS item",
    )
    .expect("static alternatives should flatten into top-level UNION distinct");

    let GraphQuery::Union(union) = query else {
        panic!("expected union query");
    };
    assert!(union.distinct);
    assert_eq!(projection_names(&union.first), vec!["item".to_string()]);
    assert_eq!(union.branches.len(), 2);
    assert!(union.branches.iter().all(|branch| branch.all));
    assert_eq!(
        union
            .branches
            .iter()
            .map(|branch| branch
                .plan
                .nodes
                .first()
                .expect("branch node")
                .label
                .as_str())
            .collect::<Vec<_>>(),
        vec!["Team", "Service"]
    );
}

#[test]
fn rejects_static_label_alternatives_inside_mixed_explicit_union() {
    let error = compile_cypher_query(
        "MATCH (entity:Person|Team) RETURN entity.name AS item \
             UNION \
             MATCH (service:Service) RETURN service.name AS item \
             UNION ALL \
             MATCH (person:Person) RETURN person.name AS item",
    )
    .expect_err("mixed UNION operators need nested grouping for static alternatives");

    assert!(error.to_string().contains("mixed UNION and UNION ALL"));
}

#[test]
fn rejects_static_label_alternatives_with_modifiers_inside_explicit_union_all() {
    let error = compile_cypher_query(
        "MATCH (entity:Person|Team) \
             RETURN entity.name AS item \
             ORDER BY item \
             UNION ALL \
             MATCH (service:Service) RETURN service.name AS item",
    )
    .expect_err("branch-level modifiers need nested grouping");

    assert!(error.to_string().contains("nested union grouping"));
}

#[test]
fn rejects_union_projection_mismatches() {
    let error = compile_cypher_query(
        "MATCH (service:Service) RETURN service.name AS item \
             UNION \
             MATCH (person:Person) RETURN person.name AS person",
    )
    .expect_err("mismatched UNION projections should fail");

    assert!(error.to_string().contains("UNION branch projections"));
}

#[test]
fn single_plan_compile_rejects_union_queries() {
    let error = compile_cypher(
        "MATCH (service:Service) RETURN service.name AS item \
             UNION \
             MATCH (person:Person) RETURN person.name AS item",
    )
    .expect_err("single-plan compiler should reject UNION");

    assert!(error.to_string().contains("compile_cypher"));
}

#[test]
fn compiles_ignored_path_variable_patterns() {
    let plan = compile_cypher(
        "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             RETURN person.name AS owner, service.name AS service",
    )
    .expect("non-materialized path binding should compile");

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
}

#[test]
fn compiles_path_length_projection() {
    let plan = compile_cypher(
        "MATCH path = (source:Service)-[:DEPENDS_ON*2]->(target:Service) \
             RETURN source.name AS source, target.name AS target, length(path) AS hops",
    )
    .expect("path length projection should compile");

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
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Integer(2)),
                alias: "hops".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_size_over_path_alias() {
    let plan = compile_cypher(
        "MATCH path = (source:Service)-[:DEPENDS_ON*2]->(target:Service) \
             WHERE size(path) = 2 \
             RETURN source.name AS source, target.name AS target, size(path) AS hops \
             ORDER BY size(path) DESC",
    )
    .expect("size(path) should compile as a path-length alias");

    let path_length = ScalarExpression::Literal(Literal::Integer(2));
    assert_eq!(
        plan.projections.get(2),
        Some(&Projection::Expression {
            expression: path_length.clone(),
            alias: "hops".to_string(),
        })
    );
    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: path_length.clone(),
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(2))),
        }))
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Literal(Literal::Integer(2)),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_size_over_path_element_lists() {
    let plan = compile_cypher(
        "MATCH path = (source:Service)-[:DEPENDS_ON*2]->(target:Service) \
             WHERE size(nodes(path)) = 3 AND size(relationships(path)) = 2 \
             RETURN size(nodes(path)) AS node_count, \
                    size(relationships(path)) AS relationship_count, \
                    size(nodes(path)) + size(relationships(path)) AS path_items \
             ORDER BY size(nodes(path)) DESC",
    )
    .expect("path element-list sizes should compile as folded path metadata");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Integer(3)),
                alias: "node_count".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Integer(2)),
                alias: "relationship_count".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Arithmetic {
                    operator: ArithmeticOperator::Add,
                    left: Box::new(ScalarExpression::Literal(Literal::Integer(3))),
                    right: Box::new(ScalarExpression::Literal(Literal::Integer(2))),
                },
                alias: "path_items".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Literal(Literal::Integer(3)),
                operator: ComparisonOperator::Equal,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(3))),
            })),
            right: Box::new(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Literal(Literal::Integer(2)),
                operator: ComparisonOperator::Equal,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(2))),
            })),
        })
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Literal(Literal::Integer(3)),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_path_metadata_arithmetic() {
    let plan = compile_cypher(
        "MATCH path = (source:Service)-[:DEPENDS_ON*2]->(target:Service) \
             WHERE size(path) + 1 = 3 \
             RETURN source.name AS source, length(path) + 1 AS depth \
             ORDER BY size(path) + 1 DESC",
    )
    .expect("path metadata should compose inside arithmetic expressions");

    let depth = ScalarExpression::Arithmetic {
        operator: ArithmeticOperator::Add,
        left: Box::new(ScalarExpression::Literal(Literal::Integer(2))),
        right: Box::new(ScalarExpression::Literal(Literal::Integer(1))),
    };
    assert_eq!(
        plan.projections.get(1),
        Some(&Projection::Expression {
            expression: depth.clone(),
            alias: "depth".to_string(),
        })
    );
    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: depth.clone(),
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(3))),
        }))
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(depth),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_path_metadata_inside_scalar_functions_and_case() {
    let plan = compile_cypher(
        "MATCH path = (source:Service)-[:DEPENDS_ON*2]->(target:Service) \
             WHERE coalesce(size(path), 0) = 2 \
             RETURN coalesce(length(path), 0) AS hops, \
                    toString(size(path)) AS hops_text, \
                    CASE WHEN length(path) = 2 THEN size(path) ELSE 0 END AS case_hops \
             ORDER BY coalesce(size(path), 0) DESC",
    )
    .expect("path metadata should compose inside scalar functions and CASE");

    let path_length = ScalarExpression::Literal(Literal::Integer(2));
    let coalesced_length = ScalarExpression::Coalesce {
        expressions: vec![
            path_length.clone(),
            ScalarExpression::Literal(Literal::Integer(0)),
        ],
    };

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: coalesced_length.clone(),
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(2))),
        }))
    );
    assert_eq!(
        plan.projections.first(),
        Some(&Projection::Expression {
            expression: coalesced_length.clone(),
            alias: "hops".to_string(),
        })
    );
    assert_eq!(
        plan.projections.get(1),
        Some(&Projection::Expression {
            expression: ScalarExpression::ToString {
                expression: Box::new(path_length.clone()),
            },
            alias: "hops_text".to_string(),
        })
    );
    assert!(matches!(
        plan.projections.get(2),
        Some(Projection::Expression {
            expression: ScalarExpression::Case {
                alternatives,
                else_expression,
            },
            alias,
        }) if alias == "case_hops"
            && matches!(
                alternatives.as_slice(),
                [ScalarCaseAlternative {
                    when: PredicateExpression::ScalarComparison(ScalarPredicate {
                        lhs,
                        operator: ComparisonOperator::Equal,
                        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(
                            Literal::Integer(2),
                        )),
                    }),
                    then,
                }] if lhs == &path_length && then == &path_length
            )
            && else_expression.as_deref()
                == Some(&ScalarExpression::Literal(Literal::Integer(0)))
    ));
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(coalesced_length),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_order_by_path_length() {
    let plan = compile_cypher(
        "MATCH path = (source:Service)-[:DEPENDS_ON*2]->(target:Service) \
             RETURN source.name AS source, target.name AS target \
             ORDER BY length(path) DESC",
    )
    .expect("path length ORDER BY should compile");

    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Literal(Literal::Integer(2)),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_path_length_predicates() {
    let plan = compile_cypher(
        "MATCH path = (source:Service)-[:DEPENDS_ON*2]->(target:Service) \
             WHERE length(path) = 2 AND length(path) IN [1, 2] \
             RETURN source.name AS source, target.name AS target",
    )
    .expect("path length predicates should compile");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Literal(Literal::Integer(2)),
                operator: ComparisonOperator::Equal,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(2))),
            })),
            right: Box::new(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Literal(Literal::Integer(2)),
                operator: ComparisonOperator::In,
                rhs: ScalarPredicateRhs::List(vec![Literal::Integer(1), Literal::Integer(2),]),
            })),
        })
    );
}

#[test]
fn compiles_terminal_with_path_length_projection() {
    let plan = compile_cypher(
        "MATCH path = (source:Service)-[:DEPENDS_ON]->{2}(target:Service) \
             WITH source.name AS source, target.name AS target, length(path) AS hops \
             RETURN source, target, hops",
    )
    .expect("terminal WITH path length projection should compile");

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
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Integer(2)),
                alias: "hops".to_string(),
            },
        ]
    );
}

#[test]
fn rejects_length_over_non_path_variable() {
    for cypher in [
        "MATCH (service:Service) RETURN length(service) AS length",
        "MATCH (service:Service) WHERE length(service) = 1 RETURN service.name AS service",
        "MATCH (service:Service) RETURN service.name AS service ORDER BY length(service)",
    ] {
        let error =
            compile_cypher(cypher).expect_err("length() should only accept bound path variables");

        assert!(
            error
                .to_string()
                .contains("length() argument 'service' is not a bound path variable"),
            "{error}"
        );
    }
}

#[test]
fn compiles_path_element_id_lists() {
    let plan = compile_cypher(
        "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             WHERE nodes(path) IS NOT NULL \
             RETURN nodes(path) AS nodes, relationships(path) AS relationships",
    )
    .expect("fixed path element id lists should compile");

    assert_eq!(
        plan.projection_output_names(),
        vec!["nodes", "relationships"]
    );
}

#[test]
fn compiles_zero_hop_path_value_returns() {
    let plan = compile_cypher(
        "MATCH path = (person:Person) \
             RETURN path",
    )
    .expect("zero-hop path values should compile");

    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::PathValue {
                node_variables: vec!["person".to_string()],
                relationship_variables: Vec::new(),
            },
            alias: "path".to_string(),
        }]
    );
}

#[test]
fn compiles_fixed_hop_path_value_returns() {
    let plan = compile_cypher(
        "MATCH path = (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN path AS p",
    )
    .expect("fixed-hop path values should compile");

    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::PathValue {
                node_variables: vec!["person".to_string(), "service".to_string()],
                relationship_variables: vec!["owns".to_string()],
            },
            alias: "p".to_string(),
        }]
    );
}

#[test]
fn compiles_path_element_list_indexes_and_endpoints_as_keys() {
    let plan = compile_cypher(
        "MATCH path = (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE nodes(path)[0] = id(person) \
             RETURN nodes(path)[0] AS first_node, \
                    nodes(path)[-1] AS last_node, \
                    nodes(path)[2] AS missing_node, \
                    relationships(path)[0] AS first_relationship, \
                    relationships(path)[-1] AS last_relationship, \
                    relationships(path)[1] AS missing_relationship, \
                    head(nodes(path)) AS head_node, \
                    last(relationships(path)) AS last_relationship_endpoint \
             ORDER BY nodes(path)[0], relationships(path)[-1]",
    )
    .expect("path element list scalar access should compile");

    let person_key = ScalarExpression::Key {
        variable: "person".to_string(),
    };
    let service_key = ScalarExpression::Key {
        variable: "service".to_string(),
    };
    let owns_key = ScalarExpression::Key {
        variable: "owns".to_string(),
    };

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: person_key.clone(),
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(person_key.clone()),
        }))
    );
    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: person_key.clone(),
                alias: "first_node".to_string(),
            },
            Projection::Expression {
                expression: service_key,
                alias: "last_node".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Null),
                alias: "missing_node".to_string(),
            },
            Projection::Expression {
                expression: owns_key.clone(),
                alias: "first_relationship".to_string(),
            },
            Projection::Expression {
                expression: owns_key.clone(),
                alias: "last_relationship".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Null),
                alias: "missing_relationship".to_string(),
            },
            Projection::Expression {
                expression: person_key.clone(),
                alias: "head_node".to_string(),
            },
            Projection::Expression {
                expression: owns_key.clone(),
                alias: "last_relationship_endpoint".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::Scalar(person_key),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Scalar(owns_key),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_path_element_list_slices_and_reducers_as_key_lists() {
    let plan = compile_cypher(
        "MATCH path = (person:Person)-[owns:OWNS]->(service:Service) \
             RETURN nodes(path)[1..] AS node_tail_slice, \
                    nodes(path)[..1] AS node_prefix_slice, \
                    relationships(path)[..1] AS relationship_prefix_slice, \
                    tail(nodes(path)) AS node_tail, \
                    tail(relationships(path)) AS relationship_tail, \
                    reverse(nodes(path)) AS reversed_nodes, \
                    reverse(relationships(path)) AS reversed_relationships \
             ORDER BY nodes(path)[1..]",
    )
    .expect("path element list slices and reducers should compile");

    let person_key = "person".to_string();
    let service_key = "service".to_string();
    let owns_key = "owns".to_string();

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::GraphKeyList {
                    variables: vec![service_key.clone()],
                },
                alias: "node_tail_slice".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::GraphKeyList {
                    variables: vec![person_key.clone()],
                },
                alias: "node_prefix_slice".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::GraphKeyList {
                    variables: vec![owns_key.clone()],
                },
                alias: "relationship_prefix_slice".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::GraphKeyList {
                    variables: vec![service_key.clone()],
                },
                alias: "node_tail".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::GraphKeyList {
                    variables: Vec::new(),
                },
                alias: "relationship_tail".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::GraphKeyList {
                    variables: vec![service_key.clone(), person_key],
                },
                alias: "reversed_nodes".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::GraphKeyList {
                    variables: vec![owns_key],
                },
                alias: "reversed_relationships".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::GraphKeyList {
                variables: vec![service_key],
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn rejects_path_element_list_size_over_non_path_variable() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             RETURN size(nodes(service)) AS node_count",
    )
    .expect_err("nodes() should require a bound path variable");

    assert!(
        error
            .to_string()
            .contains("nodes() argument 'service' is not a bound path variable"),
        "{error}"
    );
}

#[test]
fn rejects_path_variable_collisions() {
    let error = compile_cypher(
        "MATCH path = (path:Person)-[:OWNS]->(service:Service) \
             RETURN service.name AS service",
    )
    .expect_err("path bindings must not collide with graph variables");

    assert!(
        error.to_string().contains("path variable 'path' conflicts"),
        "{error}"
    );
}

#[test]
fn rejects_graph_variables_that_shadow_in_scope_path_variables() {
    let error = compile_cypher(
        "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             MATCH (path:Person) \
             RETURN path.name AS person",
    )
    .expect_err("graph variables must not shadow in-scope path variables");

    assert!(
        error
            .to_string()
            .contains("graph variable 'path' conflicts with an in-scope path variable"),
        "{error}"
    );
}

#[test]
fn explicit_with_drops_path_variables() {
    let plan = compile_cypher(
        "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             WITH person, service \
             MATCH (path:Person) \
             RETURN path.name AS person",
    )
    .expect("explicit WITH should drop unsupported path values");

    assert!(plan.nodes.iter().any(|node| node.variable == "path"));
}

#[test]
fn compiles_with_star_over_path_variables() {
    let plan = compile_cypher(
        "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             WITH * \
             RETURN person.name AS owner, length(path) AS hops, size(path) AS path_size",
    )
    .expect("WITH * should carry non-materialized path metadata");

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
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Integer(1)),
                alias: "hops".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Integer(1)),
                alias: "path_size".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_with_star_where_over_path_metadata() {
    let plan = compile_cypher(
        "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             WITH * WHERE length(path) = 1 AND size(path) = 1 \
             RETURN person.name AS owner",
    )
    .expect("WITH * WHERE should see non-materialized path metadata");

    assert_eq!(
        plan.predicate,
        Some(PredicateExpression::And {
            left: Box::new(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Literal(Literal::Integer(1)),
                operator: ComparisonOperator::Equal,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(1))),
            })),
            right: Box::new(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Literal(Literal::Integer(1)),
                operator: ComparisonOperator::Equal,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(1))),
            })),
        })
    );
}

#[test]
fn compiles_explicit_with_over_path_variable_accessors() {
    let plan = compile_cypher(
        "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             WITH path \
             RETURN nodes(path) AS nodes, relationships(path) AS relationships, length(path) AS hops",
    )
    .expect("explicit WITH should carry fixed-hop path variables");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Expression {
                expression: ScalarExpression::GraphKeyList {
                    variables: vec![
                        "__coral_hidden_person".to_string(),
                        "__coral_hidden_service".to_string(),
                    ],
                },
                alias: "nodes".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::GraphKeyList {
                    variables: vec!["__coral_rel_0".to_string()],
                },
                alias: "relationships".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Literal(Literal::Integer(1)),
                alias: "hops".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_explicit_with_over_path_value() {
    let plan = compile_cypher(
        "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             WITH path \
             RETURN path",
    )
    .expect("explicit WITH should carry fixed-hop path values");

    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::PathValue {
                node_variables: vec![
                    "__coral_hidden_person".to_string(),
                    "__coral_hidden_service".to_string(),
                ],
                relationship_variables: vec!["__coral_rel_0".to_string()],
            },
            alias: "path".to_string(),
        }]
    );
}

#[test]
fn compiles_with_star_over_path_value() {
    let plan = compile_cypher(
        "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             WITH * \
             RETURN path",
    )
    .expect("WITH * should carry fixed-hop path values for explicit projection");

    assert_eq!(
        plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::PathValue {
                node_variables: vec!["person".to_string(), "service".to_string()],
                relationship_variables: vec!["__coral_rel_0".to_string()],
            },
            alias: "path".to_string(),
        }]
    );
}

#[test]
fn rejects_explicit_with_over_variable_length_path_value() {
    let error = compile_cypher(
        "MATCH path = (source:Service)-[:DEPENDS_ON*1..2]->(target:Service) \
             WITH path \
             RETURN path",
    )
    .expect_err("variable-length path values should remain unsupported after WITH");

    assert!(
        error.to_string().contains("variable-length path values"),
        "{error}"
    );
}

#[test]
fn rejects_explicit_with_where_over_dropped_path_metadata() {
    let error = compile_cypher(
        "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             WITH person, service WHERE length(path) = 1 \
             RETURN person.name AS owner",
    )
    .expect_err("explicit WITH should drop path metadata before WHERE");

    assert!(
        error
            .to_string()
            .contains("path variable 'path' is not in scope after WITH"),
        "{error}"
    );
}

#[test]
fn rejects_with_star_path_variable_shadowing() {
    let error = compile_cypher(
        "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             WITH * \
             MATCH (path:Person) \
             RETURN path.name AS person",
    )
    .expect_err("WITH * should keep path variable names in scope");

    assert!(
        error
            .to_string()
            .contains("graph variable 'path' conflicts with an in-scope path variable"),
        "{error}"
    );
}

#[test]
fn rejects_path_value_property_projections() {
    let error = compile_cypher(
        "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             RETURN path.name AS path_name",
    )
    .expect_err("path values should not be projected as graph properties");

    assert_path_value_error(&error);
}

#[test]
fn rejects_path_value_property_predicates() {
    let error = compile_cypher(
        "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             WHERE path.name = 'x' \
             RETURN person.name AS owner",
    )
    .expect_err("path values should not be filtered as graph properties");

    assert_path_value_error(&error);
}

#[test]
fn rejects_path_value_property_ordering() {
    let error = compile_cypher(
        "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             RETURN person.name AS owner \
             ORDER BY path.name",
    )
    .expect_err("path values should not be ordered as graph properties");

    assert_path_value_error(&error);
}

#[test]
fn rejects_transparent_with_path_value_predicates_before_dropping_path_values() {
    let error = compile_cypher(
        "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
             WITH person, service WHERE path.name = 'x' \
             RETURN person.name AS owner",
    )
    .expect_err("transparent WITH should reject path values before dropping them");

    assert!(
        error
            .to_string()
            .contains("path variable 'path' is not in scope after WITH"),
        "{error}"
    );
}

#[test]
fn rejects_terminal_with_star_return_star_over_path_variables() {
    let graph = star_test_graph();
    let error = compile_cypher_for_graph(
        &graph,
        "MATCH path = (person:Person)-[ownership:OWNS]->(service:Service) \
             WITH *, service.name AS service_name \
             RETURN *",
    )
    .expect_err("terminal WITH * RETURN * should reject unmaterialized path values");

    assert!(
        error
            .to_string()
            .contains("RETURN * cannot carry path variables"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_dynamic_node_label_predicates() {
    let error = compile_cypher(
        "MATCH (service:Service) \
             WHERE service:$(label) \
             RETURN service.name AS service",
    )
    .expect_err("dynamic node label predicates should be rejected");

    assert!(
        error
            .to_string()
            .contains("dynamic label expressions require a string literal"),
        "{error:?}"
    );
}

#[test]
fn compiles_parameterized_dynamic_node_label_patterns() {
    let parameters = BTreeMap::from([(
        "label".to_string(),
        CypherParameterValue::Literal(Literal::String("Service".to_string())),
    )]);
    let plan = compile_cypher_with_parameters(
        "MATCH (service:$($label)) \
             RETURN service.name AS service",
        &parameters,
    )
    .expect("parameterized dynamic node label pattern should compile");

    assert_eq!(
        plan.nodes,
        vec![NodePattern {
            variable: "service".to_string(),
            label: "Service".to_string(),
        }]
    );
}

#[test]
fn compiles_parameterized_dynamic_relationship_type_patterns() {
    let parameters = BTreeMap::from([(
        "type".to_string(),
        CypherParameterValue::Literal(Literal::String("OWNS".to_string())),
    )]);
    let plan = compile_cypher_with_parameters(
        "MATCH (person:Person)-[owns:$($type)]->(service:Service) \
             RETURN service.name AS service",
        &parameters,
    )
    .expect("parameterized dynamic relationship type pattern should compile");

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
fn compiles_parameterized_dynamic_node_label_predicates() {
    let parameters = BTreeMap::from([
        (
            "label".to_string(),
            CypherParameterValue::Literal(Literal::String("Service".to_string())),
        ),
        (
            "other".to_string(),
            CypherParameterValue::Literal(Literal::String("Team".to_string())),
        ),
    ]);
    let plan = compile_cypher_with_parameters(
        "MATCH (service:Service) \
             WHERE service:$($label) AND NOT service:$($other) \
             RETURN service.name AS service",
        &parameters,
    )
    .expect("parameterized dynamic node label predicate should compile");

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
fn compiles_parameterized_dynamic_relationship_type_predicates() {
    let parameters = BTreeMap::from([(
        "type".to_string(),
        CypherParameterValue::Literal(Literal::String("OWNS".to_string())),
    )]);
    let plan = compile_cypher_with_parameters(
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
             WHERE owns:$($type) \
             RETURN service.name AS service",
        &parameters,
    )
    .expect("parameterized dynamic relationship type predicate should compile");

    assert_eq!(plan.predicate, Some(PredicateExpression::Boolean(true)));
}

#[test]
fn compiles_parameterized_dynamic_label_predicate_list_parameters() {
    let parameters = BTreeMap::from([
        (
            "labels".to_string(),
            CypherParameterValue::List(vec![
                Literal::String("Team".to_string()),
                Literal::String("Service".to_string()),
            ]),
        ),
        (
            "excluded".to_string(),
            CypherParameterValue::List(vec![Literal::String("Team".to_string())]),
        ),
    ]);
    let plan = compile_cypher_with_parameters(
        "MATCH (service:Service) \
             WHERE service:$($labels) AND NOT service:$($excluded) \
             RETURN service.name AS service",
        &parameters,
    )
    .expect("dynamic label predicate list parameters should compile");

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
fn compiles_parameterized_dynamic_label_pattern_list_parameters() {
    let parameters = BTreeMap::from([(
        "labels".to_string(),
        CypherParameterValue::List(vec![
            Literal::String("Team".to_string()),
            Literal::String("Service".to_string()),
        ]),
    )]);
    let query = compile_cypher_query_with_parameters(
        "MATCH (entity:$($labels)) \
             RETURN entity.name AS name",
        &parameters,
    )
    .expect("dynamic label pattern list parameters should compile");

    let GraphQuery::Union(union) = query else {
        panic!("dynamic label list parameters should expand into a union query");
    };
    assert_eq!(
        union.first.nodes.first().map(|node| node.label.as_str()),
        Some("Team")
    );
    assert_eq!(union.branches.len(), 1);
    assert_eq!(
        union
            .branches
            .first()
            .and_then(|branch| branch.plan.nodes.first())
            .map(|node| node.label.as_str()),
        Some("Service")
    );
}

#[test]
fn compiles_literal_dynamic_label_pattern_lists() {
    let query = compile_cypher_query(
        "MATCH (entity:$(['Team', 'Service'])) \
             RETURN entity.name AS name",
    )
    .expect("literal dynamic label pattern lists should compile");

    let GraphQuery::Union(union) = query else {
        panic!("dynamic label literal lists should expand into a union query");
    };
    assert_eq!(
        union.first.nodes.first().map(|node| node.label.as_str()),
        Some("Team")
    );
    assert_eq!(union.branches.len(), 1);
    assert_eq!(
        union
            .branches
            .first()
            .and_then(|branch| branch.plan.nodes.first())
            .map(|node| node.label.as_str()),
        Some("Service")
    );
}

#[test]
fn compiles_literal_dynamic_label_predicate_lists() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE service:$(['Team', 'Service']) AND NOT service:$(['Team']) \
             RETURN service.name AS service",
    )
    .expect("dynamic label predicate literal lists should compile");

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
fn compiles_literal_dynamic_relationship_type_lists() {
    let query = compile_cypher_query(
        "MATCH (source:Service)-[:DEPENDS_ON|$(['OWNS', 'DEPENDS_ON'])]->(target:Service) \
             RETURN target.name AS target",
    )
    .expect("dynamic relationship type literal lists should compile and deduplicate");

    let GraphQuery::Union(union) = query else {
        panic!("dynamic relationship type literal lists should expand into a union query");
    };
    assert_eq!(
        union
            .first
            .relationships
            .first()
            .map(|relationship| relationship.relationship_type.as_str()),
        Some("DEPENDS_ON")
    );
    assert_eq!(union.branches.len(), 1);
    assert_eq!(
        union
            .branches
            .first()
            .and_then(|branch| branch.plan.relationships.first())
            .map(|relationship| relationship.relationship_type.as_str()),
        Some("OWNS")
    );
}

#[test]
fn compiles_folded_dynamic_label_pattern_lists() {
    let query = compile_cypher_query(
        "MATCH (entity:$(split('Team,Service', ','))) \
             RETURN entity.name AS name",
    )
    .expect("folded dynamic label pattern lists should compile");

    let GraphQuery::Union(union) = query else {
        panic!("folded dynamic label lists should expand into a union query");
    };
    assert_eq!(
        union.first.nodes.first().map(|node| node.label.as_str()),
        Some("Team")
    );
    assert_eq!(union.branches.len(), 1);
    assert_eq!(
        union
            .branches
            .first()
            .and_then(|branch| branch.plan.nodes.first())
            .map(|node| node.label.as_str()),
        Some("Service")
    );
}

#[test]
fn compiles_folded_dynamic_label_predicate_lists() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE service:$(tail(['Team'] + split('Service', ','))) \
             RETURN service.name AS service",
    )
    .expect("folded dynamic label predicate lists should compile");

    assert_eq!(plan.predicate, Some(PredicateExpression::Boolean(true)));
}

#[test]
fn compiles_folded_dynamic_relationship_type_lists() {
    let query = compile_cypher_query(
            "MATCH (source:Service)-[:DEPENDS_ON|$(tail(['IGNORE'] + split('OWNS,DEPENDS_ON', ',')))]->(target:Service) \
             RETURN target.name AS target",
        )
        .expect("folded dynamic relationship type lists should compile and deduplicate");

    let GraphQuery::Union(union) = query else {
        panic!("folded dynamic relationship type lists should expand into a union query");
    };
    assert_eq!(
        union
            .first
            .relationships
            .first()
            .map(|relationship| relationship.relationship_type.as_str()),
        Some("DEPENDS_ON")
    );
    assert_eq!(union.branches.len(), 1);
    assert_eq!(
        union
            .branches
            .first()
            .and_then(|branch| branch.plan.relationships.first())
            .map(|relationship| relationship.relationship_type.as_str()),
        Some("OWNS")
    );
}

#[test]
fn compiles_static_case_dynamic_label_pattern_lists() {
    let query = compile_cypher_query(
        "MATCH (entity:$(CASE WHEN true THEN split('Team,Service', ',') ELSE ['Person'] END)) \
             RETURN entity.name AS name",
    )
    .expect("static CASE dynamic label pattern lists should compile");

    let GraphQuery::Union(union) = query else {
        panic!("static CASE dynamic label lists should expand into a union query");
    };
    assert_eq!(
        union.first.nodes.first().map(|node| node.label.as_str()),
        Some("Team")
    );
    assert_eq!(union.branches.len(), 1);
    assert_eq!(
        union
            .branches
            .first()
            .and_then(|branch| branch.plan.nodes.first())
            .map(|node| node.label.as_str()),
        Some("Service")
    );
}

#[test]
fn compiles_static_case_dynamic_label_predicate_lists() {
    let plan = compile_cypher(
        "MATCH (service:Service) \
             WHERE service:$(CASE WHEN false THEN ['Team'] ELSE ['Service'] END) \
             RETURN service.name AS service",
    )
    .expect("static CASE dynamic label predicate lists should compile");

    assert_eq!(plan.predicate, Some(PredicateExpression::Boolean(true)));
}

#[test]
fn compiles_static_case_dynamic_relationship_type_lists() {
    let query = compile_cypher_query(
            "MATCH (source:Service)-[:DEPENDS_ON|$(CASE WHEN true THEN split('OWNS,DEPENDS_ON', ',') ELSE ['ALERTS'] END)]->(target:Service) \
             RETURN target.name AS target",
        )
        .expect("static CASE dynamic relationship type lists should compile and deduplicate");

    let GraphQuery::Union(union) = query else {
        panic!("static CASE dynamic relationship type lists should expand into a union query");
    };
    assert_eq!(
        union
            .first
            .relationships
            .first()
            .map(|relationship| relationship.relationship_type.as_str()),
        Some("DEPENDS_ON")
    );
    assert_eq!(union.branches.len(), 1);
    assert_eq!(
        union
            .branches
            .first()
            .and_then(|branch| branch.plan.relationships.first())
            .map(|relationship| relationship.relationship_type.as_str()),
        Some("OWNS")
    );
}

#[test]
fn compiles_parameterized_dynamic_node_label_pattern_alternatives() {
    let parameters = BTreeMap::from([(
        "label".to_string(),
        CypherParameterValue::Literal(Literal::String("Service".to_string())),
    )]);
    let query = compile_cypher_query_with_parameters(
        "MATCH (service:Team|$($label)) \
             RETURN service.name AS service",
        &parameters,
    )
    .expect("parameterized dynamic label alternatives should compile");

    let GraphQuery::Union(union) = query else {
        panic!("dynamic label alternatives should expand into a union query");
    };
    assert_eq!(
        union.first.nodes.first().map(|node| node.label.as_str()),
        Some("Team")
    );
    assert_eq!(union.branches.len(), 1);
    assert_eq!(
        union
            .branches
            .first()
            .and_then(|branch| branch.plan.nodes.first())
            .map(|node| node.label.as_str()),
        Some("Service")
    );
    assert!(union.branches.iter().all(|branch| branch.all));
}

#[test]
fn compiles_parameterized_dynamic_relationship_type_alternatives() {
    let parameters = BTreeMap::from([(
        "type".to_string(),
        CypherParameterValue::Literal(Literal::String("OWNS".to_string())),
    )]);
    let query = compile_cypher_query_with_parameters(
        "MATCH (source:Service)-[:DEPENDS_ON|$($type)]->(target:Service) \
             RETURN target.name AS target",
        &parameters,
    )
    .expect("parameterized dynamic relationship type alternatives should compile");

    let GraphQuery::Union(union) = query else {
        panic!("dynamic relationship alternatives should expand into a union query");
    };
    assert_eq!(
        union
            .first
            .relationships
            .first()
            .map(|relationship| relationship.relationship_type.as_str()),
        Some("DEPENDS_ON")
    );
    assert_eq!(union.branches.len(), 1);
    assert_eq!(
        union
            .branches
            .first()
            .and_then(|branch| branch.plan.relationships.first())
            .map(|relationship| relationship.relationship_type.as_str()),
        Some("OWNS")
    );
}

#[test]
fn compiles_parameterized_dynamic_label_alternative_list_parameters() {
    let parameters = BTreeMap::from([(
        "labels".to_string(),
        CypherParameterValue::List(vec![
            Literal::String("Service".to_string()),
            Literal::String("Team".to_string()),
        ]),
    )]);
    let query = compile_cypher_query_with_parameters(
        "MATCH (service:Team|$($labels)) \
             RETURN service.name AS service",
        &parameters,
    )
    .expect("dynamic label alternative list parameters should compile and deduplicate");

    let GraphQuery::Union(union) = query else {
        panic!("dynamic label alternatives should expand into a union query");
    };
    assert_eq!(
        union.first.nodes.first().map(|node| node.label.as_str()),
        Some("Team")
    );
    assert_eq!(union.branches.len(), 1);
    assert_eq!(
        union
            .branches
            .first()
            .and_then(|branch| branch.plan.nodes.first())
            .map(|node| node.label.as_str()),
        Some("Service")
    );
}

#[test]
fn compiles_parameterized_dynamic_relationship_type_alternative_list_parameters() {
    let parameters = BTreeMap::from([(
        "types".to_string(),
        CypherParameterValue::List(vec![
            Literal::String("OWNS".to_string()),
            Literal::String("DEPENDS_ON".to_string()),
        ]),
    )]);
    let query = compile_cypher_query_with_parameters(
        "MATCH (source:Service)-[:DEPENDS_ON|$($types)]->(target:Service) \
             RETURN target.name AS target",
        &parameters,
    )
    .expect("dynamic relationship type list parameters should compile and deduplicate");

    let GraphQuery::Union(union) = query else {
        panic!("dynamic relationship type alternatives should expand into a union query");
    };
    assert_eq!(
        union
            .first
            .relationships
            .first()
            .map(|relationship| relationship.relationship_type.as_str()),
        Some("DEPENDS_ON")
    );
    assert_eq!(union.branches.len(), 1);
    assert_eq!(
        union
            .branches
            .first()
            .and_then(|branch| branch.plan.relationships.first())
            .map(|relationship| relationship.relationship_type.as_str()),
        Some("OWNS")
    );
}

#[test]
fn rejects_dynamic_label_list_parameters_without_string_values() {
    let parameters = BTreeMap::from([(
        "labels".to_string(),
        CypherParameterValue::List(vec![
            Literal::String("Service".to_string()),
            Literal::Integer(1),
        ]),
    )]);
    let error = compile_cypher_query_with_parameters(
        "MATCH (service:$($labels)) \
             RETURN service.name AS service",
        &parameters,
    )
    .expect_err("dynamic label list parameters with non-string values should be rejected");

    assert!(
        error
            .to_string()
            .contains("dynamic label list parameters require only strings"),
        "{error:?}"
    );
}

#[test]
fn rejects_literal_dynamic_label_lists_without_string_values() {
    let error = compile_cypher_query(
        "MATCH (service:$(['Service', 1])) \
             RETURN service.name AS service",
    )
    .expect_err("dynamic label literal lists with non-string values should be rejected");

    assert!(
        error
            .to_string()
            .contains("dynamic label literal lists require only strings"),
        "{error:?}"
    );
}

#[test]
fn rejects_empty_literal_dynamic_label_lists() {
    let error = compile_cypher_query(
        "MATCH (service:$([])) \
             RETURN service.name AS service",
    )
    .expect_err("empty dynamic label literal lists should be rejected");

    assert!(
        error
            .to_string()
            .contains("dynamic label literal lists require at least one string"),
        "{error:?}"
    );
}

#[test]
fn rejects_folded_dynamic_label_lists_without_string_values() {
    let error = compile_cypher_query(
        "MATCH (service:$(range(1, 2))) \
             RETURN service.name AS service",
    )
    .expect_err("dynamic label folded lists with non-string values should be rejected");

    assert!(
        error
            .to_string()
            .contains("dynamic label list expressions require only strings"),
        "{error:?}"
    );
}

#[test]
fn rejects_row_dependent_static_case_dynamic_label_lists() {
    let error = compile_cypher_query(
            "MATCH (service:Service) \
             WHERE service:$(CASE WHEN service.name = 'billing' THEN ['Service'] ELSE ['Team'] END) \
             RETURN service.name AS service",
        )
        .expect_err("dynamic label CASE predicates with row dependencies should be rejected");

    assert!(
        error
            .to_string()
            .contains("dynamic label CASE expressions require statically foldable WHEN predicates"),
        "{error:?}"
    );
}

#[test]
fn rejects_empty_dynamic_label_list_parameters() {
    let parameters =
        BTreeMap::from([("labels".to_string(), CypherParameterValue::List(Vec::new()))]);
    let error = compile_cypher_query_with_parameters(
        "MATCH (service:$($labels)) \
             RETURN service.name AS service",
        &parameters,
    )
    .expect_err("empty dynamic label list parameters should be rejected");

    assert!(
        error.to_string().contains("require at least one string"),
        "{error:?}"
    );
}

#[test]
fn compiles_reverse_relationship_direction() {
    let plan = compile_cypher(
        "MATCH (service:Service)<-[ownership:OWNS]-(person:Person) \
             RETURN ownership.source AS source",
    )
    .expect("query should compile");

    assert_eq!(
        plan.relationships,
        vec![RelationshipPattern {
            variable: Some("ownership".to_string()),
            relationship_type: "OWNS".to_string(),
            left: "service".to_string(),
            direction: Direction::Incoming,
            right: "person".to_string(),
        }]
    );
    assert_eq!(
        plan.projections,
        vec![Projection::Property {
            property: PropertyRef {
                variable: "ownership".to_string(),
                property: "source".to_string(),
            },
            alias: Some("source".to_string()),
        }]
    );
}

#[test]
fn compiles_connected_comma_separated_patterns_with_reused_nodes() {
    let plan = compile_cypher(
        "MATCH (source:Service)-[:DEPENDS_ON]->(middle:Service), \
                   (middle)-[:DEPENDS_ON]->(target:Service), \
                   (source)-[:DEPENDS_ON]->(target) \
             RETURN source.name AS source, middle.name AS middle, target.name AS target",
    )
    .expect("query should compile");

    assert_eq!(
        plan.nodes,
        vec![
            NodePattern {
                variable: "source".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "middle".to_string(),
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
        vec![
            RelationshipPattern {
                variable: None,
                relationship_type: "DEPENDS_ON".to_string(),
                left: "source".to_string(),
                direction: Direction::Outgoing,
                right: "middle".to_string(),
            },
            RelationshipPattern {
                variable: None,
                relationship_type: "DEPENDS_ON".to_string(),
                left: "middle".to_string(),
                direction: Direction::Outgoing,
                right: "target".to_string(),
            },
            RelationshipPattern {
                variable: None,
                relationship_type: "DEPENDS_ON".to_string(),
                left: "source".to_string(),
                direction: Direction::Outgoing,
                right: "target".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_repeated_node_property_maps_as_additional_predicates() {
    let plan = compile_cypher(
        "MATCH (service:Service {tier: 'prod'}), (service {team: 'platform'}) \
             RETURN service.name",
    )
    .expect("query should compile");

    assert_eq!(
        plan.nodes,
        vec![NodePattern {
            variable: "service".to_string(),
            label: "Service".to_string(),
        }]
    );
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
                    property: "team".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("platform".to_string())),
            },
        ]
    );
}

#[test]
fn rejects_variable_length_path_value_returns() {
    let graph = single_label_person_knows_test_graph();
    let error = compile_cypher_for_graph(
        &graph,
        "MATCH path = (person:Person)-[:KNOWS*1..2]->(friend:Person) RETURN path",
    )
    .expect_err("variable-length path graph value returns should remain rejected");

    assert!(
        error.to_string().contains("variable-length path values"),
        "{error}"
    );
}

#[test]
fn rejects_return_star_over_path_variables() {
    let graph = star_test_graph();
    let error = compile_cypher_for_graph(
        &graph,
        "MATCH path = (person:Person)-[ownership:OWNS]->(service:Service) RETURN *",
    )
    .expect_err("RETURN * should reject unmaterialized path values");

    assert!(
        error
            .to_string()
            .contains("RETURN * cannot carry path variables"),
        "{error}"
    );
}

#[test]
fn compiles_anonymous_inline_relationship_property_maps_with_internal_variable() {
    let plan = compile_cypher(
        "MATCH (person:Person)-[:OWNS {source: 'catalog'}]->(service:Service) \
             RETURN service.name",
    )
    .expect("query should compile");
    let relationship = plan
        .relationships
        .first()
        .expect("query should contain a relationship");
    let internal_variable = relationship
        .variable
        .as_ref()
        .expect("anonymous property map relationship should get an internal variable");

    assert!(
        internal_variable.starts_with("__coral_rel_"),
        "{internal_variable}"
    );
    assert_eq!(
        plan.predicates,
        vec![PropertyPredicate {
            property: PropertyRef {
                variable: internal_variable.clone(),
                property: "source".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Literal(Literal::String("catalog".to_string())),
        }]
    );
}

#[test]
fn rejects_write_queries() {
    assert_unsupported("CREATE (service:Service) RETURN service");
}

#[test]
fn rejects_variable_length_relationships() {
    for cypher in [
        "MATCH (a:Service)-[:DEPENDS_ON*]->(b:Service) RETURN a.name",
        "MATCH (a:Service)-[:DEPENDS_ON*..3]->(b:Service) RETURN a.name",
        "MATCH (a:Service)-[:DEPENDS_ON]->{1,}(b:Service) RETURN a.name",
        "MATCH (a:Service)-[:DEPENDS_ON*9..9]->(b:Service) RETURN a.name",
        "OPTIONAL MATCH (a:Service)-[:DEPENDS_ON*]->(b:Service) RETURN a.name",
        "MATCH (a:Service) OPTIONAL MATCH (a)-[:DEPENDS_ON*1..2]->(b:Service) RETURN a.name",
        "MATCH (a:Service)-[:DEPENDS_ON*2]->(b:Person) RETURN a.name",
        "MATCH (a:Service)-[r:DEPENDS_ON*0]->(b:Service) RETURN a.name",
        "MATCH (a:Service)-[r:DEPENDS_ON]->{0,1}(b:Service) RETURN a.name",
        "MATCH (a:Service)-[r:DEPENDS_ON*2]->(b:Service) RETURN a.name",
    ] {
        assert_unsupported(cypher);
    }
}

#[test]
fn compiles_cross_label_fixed_relationship_ranges_from_graph_declaration() {
    let graph = route_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH path = (person:Person)-[:ROUTES*2]->(incident:Incident) \
             RETURN person.name AS person, incident.title AS incident, length(path) AS hops",
    )
    .expect("cross-label fixed-hop path should compile from declaration metadata");

    let service = plan
        .nodes
        .iter()
        .find(|node| node.label == "Service")
        .expect("intermediate Service node should be inferred");
    assert_eq!(plan.relationships.len(), 2);
    let first_relationship = plan.relationships.first().expect("first relationship");
    let second_relationship = plan.relationships.get(1).expect("second relationship");
    assert_eq!(
        first_relationship,
        &RelationshipPattern {
            variable: None,
            relationship_type: "ROUTES".to_string(),
            left: "person".to_string(),
            direction: Direction::Outgoing,
            right: service.variable.clone(),
        }
    );
    assert_eq!(
        second_relationship,
        &RelationshipPattern {
            variable: None,
            relationship_type: "ROUTES".to_string(),
            left: service.variable.clone(),
            direction: Direction::Outgoing,
            right: "incident".to_string(),
        }
    );
    assert_eq!(path_length_projection_literal(&plan), Some(2));
}

#[test]
fn compiles_incoming_cross_label_fixed_relationship_ranges_from_graph_declaration() {
    let graph = route_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH path = (incident:Incident)<-[:ROUTES*2]-(person:Person) \
             RETURN person.name AS person, incident.title AS incident, length(path) AS hops",
    )
    .expect("incoming cross-label fixed-hop path should infer reverse labels");

    let service = plan
        .nodes
        .iter()
        .find(|node| node.label == "Service")
        .expect("intermediate Service node should be inferred");
    assert_eq!(plan.relationships.len(), 2);
    let first_relationship = plan.relationships.first().expect("first relationship");
    let second_relationship = plan.relationships.get(1).expect("second relationship");
    assert_eq!(first_relationship.left, "incident");
    assert_eq!(first_relationship.right, service.variable);
    assert_eq!(first_relationship.direction, Direction::Incoming);
    assert_eq!(second_relationship.left, first_relationship.right);
    assert_eq!(second_relationship.right, "person");
    assert_eq!(second_relationship.direction, Direction::Incoming);
}

#[test]
fn rejects_ambiguous_cross_label_fixed_relationship_ranges() {
    let error = compile_cypher_for_graph(
        &route_test_graph(),
        "MATCH (person:Person)-[:ESCALATES_TO*2]->(incident:Incident) \
             RETURN person.name AS person, incident.title AS incident",
    )
    .expect_err("ambiguous intermediate labels should be rejected");

    assert!(
        error
            .to_string()
            .contains("found at least 2 possible 2-hop"),
        "{error}"
    );
    assert!(
        error
            .to_string()
            .contains("use explicit intermediate nodes to disambiguate"),
        "{error}"
    );
}

#[test]
fn caps_fixed_length_label_sequence_collection_after_ambiguity_detected() {
    let sequences = fixed_length_label_sequences(
        &fanout_test_graph(),
        "FANS_OUT",
        Direction::Outgoing,
        "Person",
        "Incident",
        2,
    );

    assert_eq!(sequences.len(), MAX_FIXED_LABEL_SEQUENCE_RESULTS);
}

#[test]
fn rejects_unmapped_cross_label_fixed_relationship_ranges() {
    let error = compile_cypher_for_graph(
        &route_test_graph(),
        "MATCH (team:Team)-[:ROUTES*2]->(incident:Incident) \
             RETURN team.name AS team, incident.title AS incident",
    )
    .expect_err("unmapped fixed-hop label paths should be rejected");

    assert!(error.to_string().contains("could not infer"), "{error}");
}

#[test]
fn compiles_bounded_cross_label_relationship_ranges_from_graph_declaration() {
    let query = compile_cypher_query_for_graph(
        &route_test_graph(),
        "MATCH path = (person:Person)-[:ROUTES*0..2]->(incident:Incident) \
             RETURN person.name AS person, incident.title AS incident, length(path) AS hops",
    )
    .expect("bounded cross-label path should prune impossible lengths and compile");

    let GraphQuery::Plan(plan) = query else {
        panic!("only the feasible two-hop branch should remain");
    };
    assert_eq!(plan.relationships.len(), 2);
    assert_eq!(path_length_projection_literal(&plan), Some(2));
}

#[test]
fn compiles_parameterized_dynamic_bounded_cross_label_ranges_from_graph_declaration() {
    let parameters = BTreeMap::from([
        (
            "from_label".to_string(),
            CypherParameterValue::Literal(Literal::String("Person".to_string())),
        ),
        (
            "relationship_type".to_string(),
            CypherParameterValue::Literal(Literal::String("ROUTES".to_string())),
        ),
        (
            "to_label".to_string(),
            CypherParameterValue::Literal(Literal::String("Incident".to_string())),
        ),
    ]);
    let query = compile_cypher_query_for_graph_with_parameters(
            &route_test_graph(),
            "MATCH path = (person:$($from_label))-[:$($relationship_type)*0..2]->(incident:$($to_label)) \
             RETURN person.name AS person, incident.title AS incident, length(path) AS hops",
            &parameters,
        )
        .expect("parameterized dynamic bounded cross-label path should prune and compile");

    let GraphQuery::Plan(plan) = query else {
        panic!("only the feasible two-hop branch should remain");
    };
    assert_eq!(plan.relationships.len(), 2);
    assert_eq!(path_length_projection_literal(&plan), Some(2));
    assert_eq!(
        plan.nodes
            .iter()
            .map(|node| node.label.as_str())
            .collect::<Vec<_>>(),
        vec!["Person", "Incident", "Service"]
    );
    assert!(
        plan.relationships
            .iter()
            .all(|relationship| relationship.relationship_type == "ROUTES")
    );
}

#[test]
fn compiles_bounded_cross_label_gql_quantifiers_from_graph_declaration() {
    let query = compile_cypher_query_for_graph(
        &route_test_graph(),
        "MATCH path = (person:Person)-[:ROUTES]->{0,2}(incident:Incident) \
             RETURN person.name AS person, incident.title AS incident, length(path) AS hops",
    )
    .expect("bounded cross-label GQL quantifier should prune impossible lengths and compile");

    let GraphQuery::Plan(plan) = query else {
        panic!("only the feasible two-hop branch should remain");
    };
    assert_eq!(plan.relationships.len(), 2);
    assert_eq!(path_length_projection_literal(&plan), Some(2));
}

#[test]
fn compiles_incoming_bounded_cross_label_relationship_ranges_from_graph_declaration() {
    let query = compile_cypher_query_for_graph(
        &route_test_graph(),
        "MATCH path = (incident:Incident)<-[:ROUTES*0..2]-(person:Person) \
             RETURN person.name AS person, incident.title AS incident, length(path) AS hops",
    )
    .expect("incoming bounded cross-label path should prune impossible lengths and compile");

    let GraphQuery::Plan(plan) = query else {
        panic!("only the feasible two-hop branch should remain");
    };
    assert_eq!(plan.relationships.len(), 2);
    assert!(
        plan.relationships
            .iter()
            .all(|relationship| relationship.direction == Direction::Incoming)
    );
    assert_eq!(path_length_projection_literal(&plan), Some(2));
}

#[test]
fn compiles_undirected_bounded_cross_label_relationship_ranges_from_graph_declaration() {
    let query = compile_cypher_query_for_graph(
        &route_test_graph(),
        "MATCH path = (person:Person)-[:ROUTES*0..2]-(incident:Incident) \
             RETURN person.name AS person, incident.title AS incident, length(path) AS hops",
    )
    .expect("undirected bounded cross-label path should prune impossible lengths and compile");

    let GraphQuery::Plan(plan) = query else {
        panic!("only the feasible two-hop branch should remain");
    };
    assert_eq!(plan.relationships.len(), 2);
    assert!(
        plan.relationships
            .iter()
            .all(|relationship| relationship.direction == Direction::Undirected)
    );
    assert_eq!(path_length_projection_literal(&plan), Some(2));
}

#[test]
fn rejects_ambiguous_bounded_cross_label_relationship_ranges() {
    let error = compile_cypher_query_for_graph(
        &route_test_graph(),
        "MATCH (person:Person)-[:ESCALATES_TO*0..2]->(incident:Incident) \
             RETURN person.name AS person, incident.title AS incident",
    )
    .expect_err("ambiguous bounded intermediate labels should be rejected");

    assert!(
        error
            .to_string()
            .contains("found at least 2 possible 2-hop 'ESCALATES_TO' label paths"),
        "{error}"
    );
}

#[test]
fn compiles_bounded_cross_label_ranges_with_no_feasible_schema_paths_as_empty_plans() {
    for cypher in [
        "MATCH path = (team:Team)-[:ROUTES*1..2]->(incident:Incident) \
             RETURN team.name AS team, incident.title AS incident, length(path) AS hops",
        "MATCH path = (team:Team)-[:ROUTES*1..2]->(other:Team) \
             RETURN team.name AS team, other.name AS other, length(path) AS hops",
    ] {
        let query = compile_cypher_query_for_graph(&route_test_graph(), cypher)
            .expect("all-pruned bounded ranges should compile as empty plans");

        let GraphQuery::Plan(plan) = query else {
            panic!("all-pruned bounded range should compile as one empty plan");
        };
        assert_eq!(path_length_projection_literal(&plan), Some(0));
        assert!(
            predicate_contains_boolean_false(plan.predicate.as_ref()),
            "{:#?}",
            plan.predicate
        );
    }
}

#[test]
fn compiles_exact_zero_relationship_ranges_as_same_node_identity() {
    for cypher in [
        "MATCH (a:Service)-[:DEPENDS_ON*0]->(b:Service) RETURN a.name",
        "MATCH (a:Service)-[:DEPENDS_ON*0..0]->(b:Service) RETURN a.name",
        "MATCH (a:Service)-[:DEPENDS_ON]->{0}(b:Service) RETURN a.name",
        "MATCH (a:Service)-[:DEPENDS_ON]->{0,0}(b:Service) RETURN a.name",
    ] {
        let plan = compile_cypher(cypher).expect("exact zero-hop relationship should compile");

        assert_eq!(
            plan.nodes,
            vec![
                NodePattern {
                    variable: "a".to_string(),
                    label: "Service".to_string(),
                },
                NodePattern {
                    variable: "b".to_string(),
                    label: "Service".to_string(),
                },
            ]
        );
        assert!(plan.relationships.is_empty());
        assert_eq!(
            plan.predicate,
            Some(PredicateExpression::KeyComparison(KeyPredicate {
                variable: "a".to_string(),
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Key {
                    variable: "b".to_string(),
                },
            }))
        );
    }
}

#[test]
fn compiles_exact_zero_cross_label_relationship_ranges_as_false() {
    let plan = compile_cypher(
        "MATCH (a:Service)-[:DEPENDS_ON*0]->(b:Person) \
             RETURN a.name AS source, b.name AS target",
    )
    .expect("cross-label exact zero-hop relationship should compile as an empty match");

    assert!(plan.relationships.is_empty());
    assert_eq!(plan.predicate, Some(PredicateExpression::Boolean(false)));
}

#[test]
fn compiles_bounded_variable_length_relationship_ranges_as_union_all() {
    let query = compile_cypher_query(
        "MATCH path = (a:Service)-[:DEPENDS_ON*1..3]->(b:Service) \
             RETURN a.name AS source, b.name AS target, length(path) AS hops \
             ORDER BY source, target, hops",
    )
    .expect("bounded relationship range should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected bounded relationship range to expand into a union query");
    };
    assert_eq!(union.branches.len(), 2);
    let first_branch = union.branches.first().expect("first range branch");
    let second_branch = union.branches.get(1).expect("second range branch");
    assert_eq!(union.first.relationships.len(), 1);
    assert_eq!(first_branch.plan.relationships.len(), 2);
    assert_eq!(second_branch.plan.relationships.len(), 3);
    assert!(union.branches.iter().all(|branch| branch.all));
    assert_eq!(path_length_projection_literal(&union.first), Some(1));
    assert_eq!(path_length_projection_literal(&first_branch.plan), Some(2));
    assert_eq!(path_length_projection_literal(&second_branch.plan), Some(3));
    assert_eq!(union.order_by.len(), 3);
}

#[test]
fn compiles_zero_hop_bounded_variable_length_relationship_ranges_as_union_all() {
    let query = compile_cypher_query(
        "MATCH path = (a:Service)-[:DEPENDS_ON*0..2]->(b:Service) \
             RETURN a.name AS source, b.name AS target, length(path) AS hops \
             ORDER BY source, target, hops",
    )
    .expect("zero-hop bounded relationship range should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected zero-hop bounded relationship range to expand into a union query");
    };
    assert_eq!(union.branches.len(), 2);
    let first_branch = union.branches.first().expect("first range branch");
    let second_branch = union.branches.get(1).expect("second range branch");
    assert!(union.first.relationships.is_empty());
    assert_eq!(first_branch.plan.relationships.len(), 1);
    assert_eq!(second_branch.plan.relationships.len(), 2);
    assert_eq!(
        union.first.predicate,
        Some(PredicateExpression::KeyComparison(KeyPredicate {
            variable: "a".to_string(),
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Key {
                variable: "b".to_string(),
            },
        }))
    );
    assert!(union.branches.iter().all(|branch| branch.all));
    assert_eq!(path_length_projection_literal(&union.first), Some(0));
    assert_eq!(path_length_projection_literal(&first_branch.plan), Some(1));
    assert_eq!(path_length_projection_literal(&second_branch.plan), Some(2));
    assert_eq!(union.order_by.len(), 3);
}

#[test]
fn compiles_bounded_gql_relationship_quantifiers_as_union_all() {
    let query = compile_cypher_query(
        "MATCH (a:Service)-[:DEPENDS_ON]->{1,2}(b:Service) \
             RETURN a.name AS source, b.name AS target",
    )
    .expect("bounded GQL relationship quantifier should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected bounded relationship quantifier to expand into a union query");
    };
    assert_eq!(union.first.relationships.len(), 1);
    assert_eq!(union.branches.len(), 1);
    assert_eq!(
        union
            .branches
            .first()
            .expect("first range branch")
            .plan
            .relationships
            .len(),
        2
    );
}

#[test]
fn compiles_unquantified_parenthesized_path_patterns() {
    let plan = compile_cypher(
        "MATCH ((a:Service)-[:DEPENDS_ON]->(b:Service)) \
             RETURN a.name AS source, b.name AS target",
    )
    .expect("unquantified parenthesized path pattern should compile");

    assert_eq!(
        plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "DEPENDS_ON".to_string(),
            left: "a".to_string(),
            direction: Direction::Outgoing,
            right: "b".to_string(),
        }]
    );

    let path_plan = compile_cypher(
        "MATCH dependency_path = ((a:Service)-[:DEPENDS_ON]->(b:Service)) \
             RETURN length(dependency_path) AS hops",
    )
    .expect("path variable over parenthesized path pattern should compile");
    assert_eq!(path_length_projection_literal(&path_plan), Some(1));

    let optional_plan = compile_cypher(
        "MATCH (a:Service) \
             OPTIONAL MATCH ((a)-[:DEPENDS_ON]->(b:Service)) \
             RETURN a.name AS source, b.name AS target",
    )
    .expect("anchored optional parenthesized path pattern should compile");
    assert_eq!(optional_plan.optional_relationships, vec![0]);

    let ranged_query = compile_cypher_query(
        "MATCH ((a:Service)-[:DEPENDS_ON*1..2]->(b:Service)) \
             RETURN a.name AS source, b.name AS target",
    )
    .expect("bounded range inside parenthesized path should compile");
    assert!(matches!(ranged_query, GraphQuery::Union(_)));

    let alternative_query = compile_cypher_query(
        "MATCH ((a:Service)-[:DEPENDS_ON|CALLS]->(b:Service)) \
             RETURN a.name AS source, b.name AS target",
    )
    .expect("relationship type alternatives inside parenthesized path should compile");
    assert!(matches!(alternative_query, GraphQuery::Union(_)));
}

#[test]
fn compiles_exact_one_quantified_parenthesized_path_patterns() {
    let plan = compile_cypher(
        "MATCH ((a:Service)-[:DEPENDS_ON]->(b:Service)){1} \
             RETURN a.name AS source, b.name AS target",
    )
    .expect("exact-one quantified parenthesized path pattern should compile");

    assert_eq!(
        plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "DEPENDS_ON".to_string(),
            left: "a".to_string(),
            direction: Direction::Outgoing,
            right: "b".to_string(),
        }]
    );

    let path_plan = compile_cypher(
        "MATCH dependency_path = ((a:Service)-[:DEPENDS_ON]->(b:Service)){1,1} \
             RETURN length(dependency_path) AS hops",
    )
    .expect("path variable over exact-one quantified parenthesized path should compile");
    assert_eq!(path_length_projection_literal(&path_plan), Some(1));

    let optional_plan = compile_cypher(
        "MATCH (a:Service) \
             OPTIONAL MATCH ((a)-[:DEPENDS_ON]->(b:Service)){1} \
             RETURN a.name AS source, b.name AS target",
    )
    .expect("anchored optional exact-one quantified parenthesized path should compile");
    assert_eq!(optional_plan.optional_relationships, vec![0]);
}

#[test]
fn rejects_quantified_parenthesized_path_patterns() {
    let error = compile_cypher(
        "MATCH ((a:Service)-[:DEPENDS_ON]->(b:Service)){1,2} \
             RETURN a.name AS source",
    )
    .expect_err("quantified parenthesized path patterns should remain rejected");

    assert!(
        error
            .to_string()
            .contains("quantified path patterns are not supported yet"),
        "{error}"
    );
}

#[test]
fn compiles_exact_one_relationship_ranges_as_single_hop() {
    for cypher in [
        "MATCH (a:Service)-[:DEPENDS_ON*1]->(b:Service) RETURN a.name",
        "MATCH (a:Service)-[:DEPENDS_ON*1..1]->(b:Service) RETURN a.name",
        "MATCH (a:Service)-[:DEPENDS_ON]->{1}(b:Service) RETURN a.name",
        "MATCH (a:Service)-[:DEPENDS_ON]->{1,1}(b:Service) RETURN a.name",
    ] {
        let plan = compile_cypher(cypher).expect("exact-one relationship should compile");

        assert_eq!(
            plan.relationships,
            vec![RelationshipPattern {
                variable: None,
                relationship_type: "DEPENDS_ON".to_string(),
                left: "a".to_string(),
                direction: Direction::Outgoing,
                right: "b".to_string(),
            }]
        );
    }
}

#[test]
fn compiles_exact_fixed_relationship_ranges_as_repeated_hops() {
    for cypher in [
        "MATCH (a:Service)-[:DEPENDS_ON*2]->(b:Service) RETURN a.name",
        "MATCH (a:Service)-[:DEPENDS_ON*2..2]->(b:Service) RETURN a.name",
        "MATCH (a:Service)-[:DEPENDS_ON]->{2}(b:Service) RETURN a.name",
        "MATCH (a:Service)-[:DEPENDS_ON]->{2,2}(b:Service) RETURN a.name",
    ] {
        let plan = compile_cypher(cypher).expect("exact fixed relationship should compile");

        assert_eq!(
            plan.nodes,
            vec![
                NodePattern {
                    variable: "a".to_string(),
                    label: "Service".to_string(),
                },
                NodePattern {
                    variable: "b".to_string(),
                    label: "Service".to_string(),
                },
                NodePattern {
                    variable: "__coral_node_0_1".to_string(),
                    label: "Service".to_string(),
                },
            ]
        );
        assert_eq!(
            plan.relationships,
            vec![
                RelationshipPattern {
                    variable: None,
                    relationship_type: "DEPENDS_ON".to_string(),
                    left: "a".to_string(),
                    direction: Direction::Outgoing,
                    right: "__coral_node_0_1".to_string(),
                },
                RelationshipPattern {
                    variable: None,
                    relationship_type: "DEPENDS_ON".to_string(),
                    left: "__coral_node_0_1".to_string(),
                    direction: Direction::Outgoing,
                    right: "b".to_string(),
                },
            ]
        );
    }
}

#[test]
fn compiles_exact_fixed_relationship_range_property_maps_per_hop() {
    let plan = compile_cypher(
        "MATCH (a:Service)-[:DEPENDS_ON*2 {source: 'catalog'}]->(b:Service) RETURN a.name",
    )
    .expect("exact fixed relationship property map should compile");

    assert_eq!(
        plan.relationships,
        vec![
            RelationshipPattern {
                variable: Some("__coral_rel_0".to_string()),
                relationship_type: "DEPENDS_ON".to_string(),
                left: "a".to_string(),
                direction: Direction::Outgoing,
                right: "__coral_node_0_1".to_string(),
            },
            RelationshipPattern {
                variable: Some("__coral_rel_1".to_string()),
                relationship_type: "DEPENDS_ON".to_string(),
                left: "__coral_node_0_1".to_string(),
                direction: Direction::Outgoing,
                right: "b".to_string(),
            },
        ]
    );
    assert_eq!(
        plan.predicates,
        vec![
            PropertyPredicate {
                property: PropertyRef {
                    variable: "__coral_rel_0".to_string(),
                    property: "source".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("catalog".to_string())),
            },
            PropertyPredicate {
                property: PropertyRef {
                    variable: "__coral_rel_1".to_string(),
                    property: "source".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("catalog".to_string())),
            },
        ]
    );
}

#[test]
fn compiles_undirected_relationships() {
    let plan = compile_cypher("MATCH (a:Service)-[:DEPENDS_ON]-(b:Service) RETURN a.name")
        .expect("undirected relationship should compile");

    assert_eq!(
        plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "DEPENDS_ON".to_string(),
            left: "a".to_string(),
            direction: Direction::Undirected,
            right: "b".to_string(),
        }]
    );
}

#[test]
fn rejects_reserved_internal_variable_prefix() {
    assert_unsupported("MATCH (__coral_rel_0:Service) RETURN __coral_rel_0.name");
}

#[test]
fn rejects_unlabeled_first_node_binding() {
    assert_unsupported("MATCH (source)-[:DEPENDS_ON]->(target:Service) RETURN target.name");
}

#[test]
fn graph_aware_cypher_infers_unlabeled_outgoing_endpoint_labels() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[:OWNS]->(service) RETURN service.name",
    )
    .expect("graph declaration should infer the unlabeled outgoing endpoint");

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
}

#[test]
fn graph_aware_cypher_infers_unlabeled_incoming_endpoint_labels() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (service)<-[:OWNS]-(person:Person) RETURN service.name",
    )
    .expect("graph declaration should infer the unlabeled incoming endpoint");

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
            left: "service".to_string(),
            direction: Direction::Incoming,
            right: "person".to_string(),
        }]
    );
}

#[test]
fn graph_aware_cypher_infers_unlabeled_exact_one_endpoint_labels() {
    let graph = star_test_graph();
    for cypher in [
        "MATCH (person:Person)-[:OWNS*1]->(service) RETURN service.name",
        "MATCH (person:Person)-[:OWNS*1..1]->(service) RETURN service.name",
        "MATCH (person:Person)-[:OWNS]->{1}(service) RETURN service.name",
    ] {
        let plan = compile_cypher_for_graph(&graph, cypher)
            .expect("graph declaration should infer exact-one endpoint labels");
        assert!(
            plan.nodes
                .iter()
                .any(|node| { node.variable == "service" && node.label == "Service" }),
            "service endpoint label was not inferred for {cypher}: {:?}",
            plan.nodes
        );
    }
}

#[test]
fn graph_aware_cypher_infers_unlabeled_fixed_length_endpoint_labels() {
    let graph = route_test_graph();
    for cypher in [
        "MATCH (person:Person)-[:ROUTES*2]->(incident) RETURN incident.title",
        "MATCH (incident)<-[:ROUTES*2]-(person:Person) RETURN incident.title",
        "MATCH (person:Person)-[:ROUTES]->{2}(incident) RETURN incident.title",
    ] {
        let plan = compile_cypher_for_graph(&graph, cypher)
            .expect("graph declaration should infer fixed-length endpoint labels");
        assert!(
            plan.nodes
                .iter()
                .any(|node| { node.variable == "incident" && node.label == "Incident" }),
            "incident endpoint label was not inferred for {cypher}: {:?}",
            plan.nodes
        );
    }
}

#[test]
fn graph_aware_cypher_infers_unlabeled_zero_hop_endpoint_labels() {
    let graph = route_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[:ROUTES*0]->(same) RETURN same.name",
    )
    .expect("graph declaration should infer zero-hop endpoint labels");

    assert!(
        plan.nodes
            .iter()
            .any(|node| { node.variable == "same" && node.label == "Person" }),
        "zero-hop endpoint label was not inferred: {:?}",
        plan.nodes
    );
}

#[test]
fn graph_aware_cypher_infers_anonymous_outgoing_endpoint_labels() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[:OWNS]->() RETURN person.name",
    )
    .expect("graph declaration should infer the anonymous outgoing endpoint");

    let anonymous = plan
        .nodes
        .iter()
        .find(|node| node.variable.starts_with("__coral_node_"))
        .expect("anonymous endpoint should be bound internally");
    assert_eq!(anonymous.label, "Service");
    assert_eq!(
        plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "OWNS".to_string(),
            left: "person".to_string(),
            direction: Direction::Outgoing,
            right: anonymous.variable.clone(),
        }]
    );
}

#[test]
fn graph_aware_cypher_infers_anonymous_incoming_endpoint_labels() {
    let graph = route_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH ()-[:ROUTES]->(service:Service) RETURN service.name",
    )
    .expect("graph declaration should infer the anonymous incoming endpoint");

    let anonymous = plan
        .nodes
        .iter()
        .find(|node| node.variable.starts_with("__coral_node_"))
        .expect("anonymous endpoint should be bound internally");
    assert_eq!(anonymous.label, "Person");
    assert_eq!(
        plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "ROUTES".to_string(),
            left: anonymous.variable.clone(),
            direction: Direction::Outgoing,
            right: "service".to_string(),
        }]
    );
}

#[test]
fn graph_aware_cypher_infers_anonymous_fixed_and_zero_hop_endpoint_labels() {
    let graph = route_test_graph();
    let fixed = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[:ROUTES*2]->() RETURN person.name",
    )
    .expect("graph declaration should infer the anonymous fixed-hop endpoint");
    assert!(
        fixed
            .nodes
            .iter()
            .any(|node| { node.variable.starts_with("__coral_node_") && node.label == "Incident" }),
        "fixed-hop anonymous endpoint label was not inferred: {:?}",
        fixed.nodes
    );

    let zero = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[:ROUTES*0]->() RETURN person.name",
    )
    .expect("graph declaration should infer the anonymous zero-hop endpoint");
    assert!(
        zero.nodes
            .iter()
            .any(|node| { node.variable.starts_with("__coral_node_") && node.label == "Person" }),
        "zero-hop anonymous endpoint label was not inferred: {:?}",
        zero.nodes
    );
}

#[test]
fn graph_aware_cypher_preserves_fixed_length_intermediate_ambiguity() {
    let graph = route_test_graph();
    let error = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[:ESCALATES_TO*2]->(incident) RETURN incident.title",
    )
    .expect_err("ambiguous fixed-length intermediate labels should still fail");

    assert!(
        error
            .to_string()
            .contains("found at least 2 possible 2-hop"),
        "{error}"
    );
}

#[test]
fn graph_aware_cypher_compile_rejects_unknown_declared_properties() {
    let graph = star_test_graph();
    let error = compile_cypher_for_graph(
        &graph,
        "MATCH (service:Service) RETURN service.missing AS value",
    )
    .expect_err("graph-aware Cypher compile should validate declared properties");

    assert!(error.to_string().contains("UNKNOWN_PROPERTY"), "{error}");
}

#[test]
fn graph_aware_cypher_query_compile_validates_union_branches() {
    let graph = star_test_graph();
    let error = compile_cypher_query_for_graph(
        &graph,
        "MATCH (service:Service) RETURN service.name AS value \
             UNION ALL \
             MATCH (service:Service) RETURN service.missing AS value",
    )
    .expect_err("graph-aware Cypher query compile should validate union branches");

    assert!(error.to_string().contains("UNKNOWN_PROPERTY"), "{error}");
}

#[test]
fn graph_aware_cypher_rejects_ambiguous_unlabeled_endpoint_labels() {
    let graph = star_test_graph();
    let error = compile_cypher_for_graph(
        &graph,
        "MATCH (owner)-[:OWNS]->(service:Service) RETURN service.name",
    )
    .expect_err("ambiguous unlabeled endpoint labels should be rejected");

    assert!(
        error.to_string().contains("could not infer a unique label"),
        "{error:?}"
    );
}

#[test]
fn graph_aware_cypher_rejects_ambiguous_anonymous_endpoint_labels() {
    let graph = star_test_graph();
    let error = compile_cypher_for_graph(
        &graph,
        "MATCH ()-[:OWNS]->(service:Service) RETURN service.name",
    )
    .expect_err("ambiguous anonymous endpoint labels should be rejected");

    assert!(
        error
            .to_string()
            .contains("anonymous node at path position 0"),
        "{error:?}"
    );
}

#[test]
fn graph_aware_cypher_infers_untyped_relationship_types() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-->(service:Service) RETURN service.name",
    )
    .expect("graph declaration should infer an untyped relationship");

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
fn graph_aware_cypher_infers_untyped_relationship_endpoint_labels() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-->(service) RETURN service.name",
    )
    .expect("graph declaration should infer the untyped relationship endpoint");

    assert!(
        plan.nodes
            .iter()
            .any(|node| node.variable == "service" && node.label == "Service"),
        "service endpoint label was not inferred: {:?}",
        plan.nodes
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
fn graph_aware_cypher_infers_untyped_anonymous_endpoint_labels() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(&graph, "MATCH (person:Person)-->() RETURN person.name")
        .expect("graph declaration should infer the untyped anonymous endpoint");

    let anonymous = plan
        .nodes
        .iter()
        .find(|node| node.variable.starts_with("__coral_node_"))
        .expect("anonymous endpoint should be bound internally");
    assert_eq!(anonymous.label, "Service");
    assert_eq!(
        plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "OWNS".to_string(),
            left: "person".to_string(),
            direction: Direction::Outgoing,
            right: anonymous.variable.clone(),
        }]
    );
}

#[test]
fn graph_aware_cypher_infers_untyped_relationship_variables() {
    let graph = star_test_graph();
    let plan = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[ownership]->(service:Service) RETURN type(ownership)",
    )
    .expect("graph declaration should infer an untyped relationship variable");

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
}

#[test]
fn graph_aware_cypher_rejects_ambiguous_untyped_endpoint_labels() {
    let graph = route_test_graph();
    let error = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-->(target) RETURN person.name",
    )
    .expect_err("ambiguous untyped endpoint label should be rejected");

    assert!(
            error.to_string().contains(
                "could not infer a unique label for node variable 'target' from untyped relationship mappings"
            ),
            "{error:?}"
        );
}

#[test]
fn graph_aware_cypher_rejects_ambiguous_untyped_relationship_types() {
    let graph = route_test_graph();
    let error = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-->(service:Service) RETURN service.name",
    )
    .expect_err("ambiguous untyped relationship should be rejected");

    assert!(
        error
            .to_string()
            .contains("could not infer a unique relationship type"),
        "{error:?}"
    );
}

#[test]
fn graph_aware_cypher_rejects_unmapped_untyped_relationship_types() {
    let graph = route_test_graph();
    let error = compile_cypher_for_graph(
        &graph,
        "MATCH (incident:Incident)-->(person:Person) RETURN incident.title",
    )
    .expect_err("unmapped untyped relationship should be rejected");

    assert!(
        error
            .to_string()
            .contains("could not infer a relationship type"),
        "{error:?}"
    );
}

#[test]
fn graph_aware_cypher_rejects_untyped_relationship_ranges() {
    let graph = route_test_graph();
    let error = compile_cypher_for_graph(
        &graph,
        "MATCH (person:Person)-[*2]->(incident:Incident) RETURN incident.title",
    )
    .expect_err("untyped relationship ranges should be rejected");

    assert!(
        error
            .to_string()
            .contains("untyped relationship ranges require an explicit relationship type"),
        "{error:?}"
    );
}

#[test]
fn rejects_untyped_relationships_without_graph_declaration() {
    let error = compile_cypher("MATCH (person:Person)-->(service:Service) RETURN service.name")
        .expect_err("declaration-free untyped relationships should be rejected");

    assert!(
        error.to_string().contains("relationship type is required"),
        "{error:?}"
    );
}

#[test]
fn compiles_anonymous_labeled_node_patterns() {
    let plan = compile_cypher(
        "MATCH (:Service {tier: 'prod'})-[:DEPENDS_ON]->(target:Service) \
             RETURN target.name",
    )
    .expect("anonymous labeled node pattern should compile");

    assert_eq!(plan.nodes.len(), 2);
    let anonymous_node = plan.nodes.first().expect("anonymous node should exist");
    let target_node = plan.nodes.get(1).expect("target node should exist");
    let relationship = plan
        .relationships
        .first()
        .expect("relationship should exist");
    let anonymous_variable = &anonymous_node.variable;
    assert!(anonymous_variable.starts_with("__coral_node_"));
    assert_eq!(anonymous_node.label, "Service");
    assert_eq!(target_node.variable, "target");
    assert_eq!(relationship.left, anonymous_variable.as_str());
    assert_eq!(relationship.right, "target");
    assert_eq!(
        plan.predicates,
        vec![PropertyPredicate {
            property: PropertyRef {
                variable: anonymous_variable.clone(),
                property: "tier".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
        }]
    );
}

#[test]
fn compiles_static_label_expression_patterns() {
    let plan = compile_cypher(
            "MATCH (person:Person&!(Team|Service))-[owns:OWNS&!(DEPENDS_ON|ALERTS)]->(service:Service&!Team) \
             RETURN person.name AS owner, service.name AS service",
        )
        .expect("static label expression patterns should compile");

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
            variable: Some("owns".to_string()),
            relationship_type: "OWNS".to_string(),
            left: "person".to_string(),
            direction: Direction::Outgoing,
            right: "service".to_string(),
        }]
    );
}

#[test]
fn rejects_contradictory_compound_label_exclusion_patterns() {
    let error = compile_cypher(
        "MATCH (service:Service&!(Service|Team)) \
             RETURN service.name AS service",
    )
    .expect_err("contradictory compound label exclusion should be rejected");

    assert!(
        error
            .to_string()
            .contains("contradictory label expressions"),
        "{error:?}"
    );
}

#[test]
fn rejects_unlabeled_anonymous_node_patterns() {
    assert_unsupported("MATCH ()-[:DEPENDS_ON]->(target:Service) RETURN target.name");
}

#[test]
fn rejects_conflicting_labels_for_reused_node_variables() {
    assert_unsupported(
        "MATCH (source:Service)-[:DEPENDS_ON]->(target:Service), \
                   (source:Person)-[:OWNS]->(target) \
             RETURN target.name",
    );
}

#[test]
fn rejects_ambiguous_label_expression_patterns() {
    assert_unsupported("MATCH (service:Service|Team) RETURN service.name");
    assert_unsupported("MATCH (service:Service&Team) RETURN service.name");
    assert_unsupported("MATCH (service:Service&!Service) RETURN service.name");
    assert_unsupported("MATCH (service:!Team) RETURN service.name");
    assert_unsupported(
        "MATCH (person:Person)-[:OWNS|DEPENDS_ON]->(service:Service) RETURN service.name",
    );
}
