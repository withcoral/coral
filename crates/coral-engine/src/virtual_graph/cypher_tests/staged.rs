use super::*;

#[test]
fn compiles_staged_with_order_limit_before_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person) \
             WITH a ORDER BY a.age LIMIT 2 \
             MATCH (a)-[:KNOWS]->(b:Person) \
             RETURN a.name AS a, b.name AS b",
    )
    .expect("staged WITH ORDER BY LIMIT before MATCH should compile");

    let GraphQuery::Staged(staged) = query else {
        panic!("target query should compile to a staged graph query");
    };
    assert_eq!(staged.stages.len(), 1);
    let stage = staged
        .stages
        .first()
        .expect("staged query should have stage 0");
    assert_eq!(
        stage.exports,
        vec![GraphStageExport::NodeKey {
            variable: "a".to_string(),
            column: "a_id".to_string(),
        }]
    );
    assert_eq!(
        stage.plan.projections,
        vec![Projection::Key {
            variable: "a".to_string(),
            alias: "a_id".to_string(),
        }]
    );
    assert_eq!(
        stage.plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "a".to_string(),
                property: "age".to_string(),
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
    assert_eq!(stage.plan.limit, Some(2));
    assert_eq!(
        staged.final_plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "KNOWS".to_string(),
            left: "a".to_string(),
            direction: Direction::Outgoing,
            right: "b".to_string(),
        }]
    );
    assert_eq!(
        staged.final_plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "a".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("a".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "b".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("b".to_string()),
            },
        ]
    );
}

#[test]
fn compiles_staged_with_incoming_final_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person) \
             WITH a ORDER BY a.age LIMIT 2 \
             MATCH (b:Person)-[:KNOWS]->(a) \
             RETURN a.name AS a, b.name AS b",
    )
    .expect("staged route should allow incoming final matches into carried variables");

    let GraphQuery::Staged(staged) = query else {
        panic!("incoming final match should compile to a staged graph query");
    };
    assert_eq!(
        staged.final_plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "KNOWS".to_string(),
            left: "b".to_string(),
            direction: Direction::Outgoing,
            right: "a".to_string(),
        }]
    );
}

#[test]
fn compiles_staged_with_undirected_final_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person) \
             WITH a ORDER BY a.age LIMIT 2 \
             MATCH (a)-[:KNOWS]-(b:Person) \
             RETURN a.name AS a, b.name AS b",
    )
    .expect("staged route should allow undirected final matches from carried variables");

    let GraphQuery::Staged(staged) = query else {
        panic!("undirected final match should compile to a staged graph query");
    };
    assert_eq!(
        staged.final_plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "KNOWS".to_string(),
            left: "a".to_string(),
            direction: Direction::Undirected,
            right: "b".to_string(),
        }]
    );
}

#[test]
fn compiles_staged_with_multihop_final_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person) \
             WITH a ORDER BY a.age LIMIT 2 \
             MATCH (a)-[:KNOWS]->(x:Person)-[:KNOWS]->(b:Person) \
             RETURN a.name AS a, b.name AS b",
    )
    .expect("staged route should allow fixed multi-hop final matches");

    let GraphQuery::Staged(staged) = query else {
        panic!("multi-hop final match should compile to a staged graph query");
    };
    assert_eq!(
        staged.final_plan.relationships,
        vec![
            RelationshipPattern {
                variable: None,
                relationship_type: "KNOWS".to_string(),
                left: "a".to_string(),
                direction: Direction::Outgoing,
                right: "x".to_string(),
            },
            RelationshipPattern {
                variable: None,
                relationship_type: "KNOWS".to_string(),
                left: "x".to_string(),
                direction: Direction::Outgoing,
                right: "b".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_staged_with_incoming_multihop_final_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person) \
             WITH a ORDER BY a.age LIMIT 2 \
             MATCH (x:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(a) \
             RETURN a.name AS a, x.name AS x",
    )
    .expect("staged route should allow incoming fixed multi-hop final matches");

    let GraphQuery::Staged(staged) = query else {
        panic!("incoming multi-hop final match should compile to a staged graph query");
    };
    assert_eq!(
        staged.final_plan.relationships,
        vec![
            RelationshipPattern {
                variable: None,
                relationship_type: "KNOWS".to_string(),
                left: "x".to_string(),
                direction: Direction::Outgoing,
                right: "b".to_string(),
            },
            RelationshipPattern {
                variable: None,
                relationship_type: "KNOWS".to_string(),
                left: "b".to_string(),
                direction: Direction::Outgoing,
                right: "a".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_staged_with_second_relationship_type() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person) \
             WITH a ORDER BY a.age LIMIT 2 \
             MATCH (a)-[:LIKES]->(b:Person) \
             RETURN a.name AS a, b.name AS b",
    )
    .expect("staged route should allow any explicit relationship type");

    let GraphQuery::Staged(staged) = query else {
        panic!("second relationship type should compile to a staged graph query");
    };
    assert_eq!(
        staged.final_plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "LIKES".to_string(),
            left: "a".to_string(),
            direction: Direction::Outgoing,
            right: "b".to_string(),
        }]
    );
}

#[test]
fn compiles_staged_with_multiple_carried_property_returns() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person) \
             WITH a ORDER BY a.age LIMIT 2 \
             MATCH (a)-[:OWNS]->(b:Service) \
             RETURN a.name AS a, a.age AS age, b.name AS b",
    )
    .expect("staged route should rehydrate carried node property columns");

    let GraphQuery::Staged(staged) = query else {
        panic!("multi-property carried return should compile to a staged graph query");
    };
    assert_eq!(
        staged.final_plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "a".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("a".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "a".to_string(),
                    property: "age".to_string(),
                },
                alias: Some("age".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "b".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("b".to_string()),
            },
        ]
    );
}

#[test]
fn compiles_staged_with_optional_final_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person) \
             WITH a ORDER BY a.age LIMIT 1 \
             OPTIONAL MATCH (a)-[:LIKES]->(b:Person) \
             RETURN a.name AS a, b.name AS b",
    )
    .expect("staged route should allow optional final matches from carried variables");

    let GraphQuery::Staged(staged) = query else {
        panic!("optional final match should compile to a staged graph query");
    };
    assert_eq!(
        staged.final_plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "LIKES".to_string(),
            left: "a".to_string(),
            direction: Direction::Outgoing,
            right: "b".to_string(),
        }]
    );
    assert_eq!(staged.final_plan.optional_relationships, vec![0]);
}

#[test]
fn compiles_staged_relationship_carry_optional_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) \
             WITH r LIMIT 1 \
             OPTIONAL MATCH (a2:Person)-[r:KNOWS]->(b2:Person) \
             RETURN a2.name AS a, id(r) AS r, b2.name AS b",
    )
    .expect("staged route should carry relationship keys into optional final matches");

    let GraphQuery::Staged(staged) = query else {
        panic!("relationship carry should compile to a staged graph query");
    };
    let stage = staged
        .stages
        .first()
        .expect("staged query should have stage 0");
    assert_eq!(
        stage.exports,
        vec![GraphStageExport::RelationshipKey {
            variable: "r".to_string(),
            column: "r_id".to_string(),
        }]
    );
    assert_eq!(
        stage.plan.projections,
        vec![Projection::Key {
            variable: "r".to_string(),
            alias: "r_id".to_string(),
        }]
    );
    assert_eq!(stage.plan.limit, Some(1));
    assert_eq!(
        staged.final_plan.relationships,
        vec![RelationshipPattern {
            variable: Some("r".to_string()),
            relationship_type: "KNOWS".to_string(),
            left: "a2".to_string(),
            direction: Direction::Outgoing,
            right: "b2".to_string(),
        }]
    );
    assert_eq!(staged.final_plan.optional_relationships, vec![0]);
}

#[test]
fn compiles_staged_bare_relationship_carry_optional_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (:Person)-[r:KNOWS]->(:Person) \
             WITH r LIMIT 1 \
             OPTIONAL MATCH (a2:Person)-[r]->(b2:Person) \
             RETURN a2, r, b2",
    )
    .expect("staged route should infer the carried relationship type for bare optional reuse");

    let GraphQuery::Staged(staged) = query else {
        panic!("bare relationship carry should compile to a staged graph query");
    };
    let stage = staged
        .stages
        .first()
        .expect("staged query should have stage 0");
    assert_eq!(
        stage.exports,
        vec![GraphStageExport::RelationshipKey {
            variable: "r".to_string(),
            column: "r_id".to_string(),
        }]
    );
    assert_eq!(
        staged.final_plan.relationships,
        vec![RelationshipPattern {
            variable: Some("r".to_string()),
            relationship_type: "KNOWS".to_string(),
            left: "a2".to_string(),
            direction: Direction::Outgoing,
            right: "b2".to_string(),
        }]
    );
    assert_eq!(staged.final_plan.optional_relationships, vec![0]);
    assert_eq!(
        staged.final_plan.projection_output_names(),
        vec![
            "a2.__id",
            "a2.__labels",
            "a2.age",
            "a2.name",
            "r.__id",
            "r.__type",
            "b2.__id",
            "b2.__labels",
            "b2.age",
            "b2.name",
        ]
    );
}

#[test]
fn compiles_staged_relationship_carry_with_declaration_inferred_optional_endpoints() {
    let graph = staged_aggregate_relationship_carry_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH ()-[r]->() \
             WITH r LIMIT 1 \
             OPTIONAL MATCH (a2)-[r]->(b2) \
             RETURN a2, r, b2",
    )
    .expect("staged route should infer carried relationship endpoints from declaration");

    let GraphQuery::Staged(staged) = query else {
        panic!("inferred relationship carry should compile to a staged graph query");
    };
    let stage = staged
        .stages
        .first()
        .expect("staged query should have stage 0");
    assert_eq!(
        stage.exports,
        vec![GraphStageExport::RelationshipKey {
            variable: "r".to_string(),
            column: "r_id".to_string(),
        }]
    );
    assert_eq!(
        staged.final_plan.nodes,
        vec![
            NodePattern {
                variable: "a2".to_string(),
                label: "X".to_string(),
            },
            NodePattern {
                variable: "b2".to_string(),
                label: "X".to_string(),
            },
        ]
    );
    assert_eq!(
        staged.final_plan.relationships,
        vec![RelationshipPattern {
            variable: Some("r".to_string()),
            relationship_type: "REL".to_string(),
            left: "a2".to_string(),
            direction: Direction::Outgoing,
            right: "b2".to_string(),
        }]
    );
    assert_eq!(staged.final_plan.optional_relationships, vec![0]);
    let optional_scope = staged
        .final_plan
        .optional_matches
        .first()
        .expect("final plan should have one optional scope");
    assert_eq!(optional_scope.node_indices, vec![0, 1]);
}

#[test]
fn compiles_staged_node_and_relationship_carry_with_declaration_inferred_optional_target() {
    let graph = staged_aggregate_relationship_carry_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a1)-[r]->() \
             WITH r, a1 LIMIT 1 \
             OPTIONAL MATCH (a1)-[r]->(b2) \
             RETURN a1, r, b2",
    )
    .expect("staged route should infer carried node and optional target labels");

    let GraphQuery::Staged(staged) = query else {
        panic!("node plus inferred relationship carry should compile to staged");
    };
    let stage = staged
        .stages
        .first()
        .expect("staged query should have stage 0");
    assert_eq!(
        stage.exports,
        vec![
            GraphStageExport::RelationshipKey {
                variable: "r".to_string(),
                column: "r_id".to_string(),
            },
            GraphStageExport::NodeKey {
                variable: "a1".to_string(),
                column: "a1_id".to_string(),
            },
        ]
    );
    assert_eq!(
        staged.final_plan.nodes,
        vec![
            NodePattern {
                variable: "a1".to_string(),
                label: "X".to_string(),
            },
            NodePattern {
                variable: "b2".to_string(),
                label: "X".to_string(),
            },
        ]
    );
    assert_eq!(
        staged.final_plan.relationships,
        vec![RelationshipPattern {
            variable: Some("r".to_string()),
            relationship_type: "REL".to_string(),
            left: "a1".to_string(),
            direction: Direction::Outgoing,
            right: "b2".to_string(),
        }]
    );
    assert_eq!(staged.final_plan.optional_relationships, vec![0]);
    let optional_scope = staged
        .final_plan
        .optional_matches
        .first()
        .expect("final plan should have one optional scope");
    assert_eq!(optional_scope.node_indices, vec![1]);
}

#[test]
fn compiles_staged_node_and_relationship_carry_optional_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a1:Person)-[r:KNOWS]->(:Person) \
             WITH r, a1 LIMIT 1 \
             OPTIONAL MATCH (a1)-[r:KNOWS]->(b2:Person) \
             RETURN a1.name AS a, id(r) AS r, b2.name AS b",
    )
    .expect("staged route should carry node and relationship keys together");

    let GraphQuery::Staged(staged) = query else {
        panic!("node plus relationship carry should compile to a staged graph query");
    };
    let stage = staged
        .stages
        .first()
        .expect("staged query should have stage 0");
    assert_eq!(
        stage.exports,
        vec![
            GraphStageExport::RelationshipKey {
                variable: "r".to_string(),
                column: "r_id".to_string(),
            },
            GraphStageExport::NodeKey {
                variable: "a1".to_string(),
                column: "a1_id".to_string(),
            },
        ]
    );
    assert_eq!(
        staged.final_plan.relationships,
        vec![RelationshipPattern {
            variable: Some("r".to_string()),
            relationship_type: "KNOWS".to_string(),
            left: "a1".to_string(),
            direction: Direction::Outgoing,
            right: "b2".to_string(),
        }]
    );
    assert_eq!(staged.final_plan.optional_relationships, vec![0]);
}

#[test]
fn compiles_staged_node_and_bare_relationship_carry_optional_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a1:Person)-[r:KNOWS]->(:Person) \
             WITH r, a1 LIMIT 1 \
             OPTIONAL MATCH (a1)-[r]->(b2:Person) \
             RETURN a1, r, b2",
    )
    .expect("staged route should carry node and bare relationship keys together");

    let GraphQuery::Staged(staged) = query else {
        panic!("node plus bare relationship carry should compile to a staged graph query");
    };
    let stage = staged
        .stages
        .first()
        .expect("staged query should have stage 0");
    assert_eq!(
        stage.exports,
        vec![
            GraphStageExport::RelationshipKey {
                variable: "r".to_string(),
                column: "r_id".to_string(),
            },
            GraphStageExport::NodeKey {
                variable: "a1".to_string(),
                column: "a1_id".to_string(),
            },
        ]
    );
    assert_eq!(
        staged.final_plan.relationships,
        vec![RelationshipPattern {
            variable: Some("r".to_string()),
            relationship_type: "KNOWS".to_string(),
            left: "a1".to_string(),
            direction: Direction::Outgoing,
            right: "b2".to_string(),
        }]
    );
    assert_eq!(staged.final_plan.optional_relationships, vec![0]);
    assert_eq!(
        staged.final_plan.projection_output_names(),
        vec![
            "a1.__id",
            "a1.__labels",
            "a1.age",
            "a1.name",
            "r.__id",
            "r.__type",
            "b2.__id",
            "b2.__labels",
            "b2.age",
            "b2.name",
        ]
    );
}

#[test]
fn compiles_staged_scalar_alias_with_order_limit_before_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person) \
             WITH a.id AS friendId ORDER BY a.age LIMIT 1 \
             MATCH (b:Person) WHERE b.id = friendId \
             RETURN b.name AS name",
    )
    .expect("staged scalar alias WITH ORDER BY LIMIT before MATCH should compile");

    let GraphQuery::Staged(staged) = query else {
        panic!("scalar alias query should compile to a staged graph query");
    };
    assert_eq!(staged.stages.len(), 1);
    let stage = staged
        .stages
        .first()
        .expect("staged query should have stage 0");
    assert_eq!(
        stage.exports,
        vec![GraphStageExport::ScalarValue {
            alias: "friendId".to_string(),
            source: "friendId".to_string(),
        }]
    );
    assert_eq!(
        stage.plan.projections,
        vec![Projection::Property {
            property: PropertyRef {
                variable: "a".to_string(),
                property: "id".to_string(),
            },
            alias: Some("friendId".to_string()),
        }]
    );
    assert_eq!(
        stage.plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "a".to_string(),
                property: "age".to_string(),
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
    assert_eq!(stage.plan.limit, Some(1));
    assert_eq!(
        staged.final_plan.predicate,
        Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Property(PropertyRef {
                variable: "b".to_string(),
                property: "id".to_string(),
            }),
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::StageValue {
                alias: "friendId".to_string(),
            }),
        }))
    );
}

#[test]
fn compiles_staged_scalar_alias_with_labeled_final_target_on_single_label_graph() {
    let graph = single_label_person_knows_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person) \
             WITH a.id AS friendId ORDER BY a.age LIMIT 1 \
             MATCH (b:Person) WHERE b.id = friendId \
             RETURN b.name AS name",
    )
    .expect("explicitly labeled scalar alias target should compile to staged");

    assert!(matches!(query, GraphQuery::Staged(_)));
}

#[test]
fn rejects_staged_scalar_alias_unlabeled_final_target_on_single_label_graph() {
    let graph = single_label_person_knows_test_graph();
    let error = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person) \
             WITH a.id AS y ORDER BY a.age LIMIT 1 \
             MATCH (b) WHERE b.id = y \
             RETURN b.name AS name",
    )
    .expect_err("unlabeled scalar alias target should require broader staged planning");

    assert!(
        error.to_string().contains("staged query planning"),
        "{error}"
    );
}

#[test]
fn compiles_staged_string_scalar_alias_with_skip_limit_before_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person) \
             WITH a.name AS selectedName ORDER BY a.age SKIP 1 LIMIT 1 \
             MATCH (b:Person) WHERE b.name = selectedName \
             RETURN b.name AS name",
    )
    .expect("staged string scalar alias WITH SKIP LIMIT before MATCH should compile");

    let GraphQuery::Staged(staged) = query else {
        panic!("string scalar alias query should compile to a staged graph query");
    };
    let stage = staged
        .stages
        .first()
        .expect("staged query should have stage 0");
    assert_eq!(stage.plan.skip, Some(1));
    assert_eq!(stage.plan.limit, Some(1));
    assert_eq!(
        stage.exports,
        vec![GraphStageExport::ScalarValue {
            alias: "selectedName".to_string(),
            source: "selectedName".to_string(),
        }]
    );
}

#[test]
fn compiles_staged_scalar_alias_return_after_final_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person) \
             WITH a.id AS friendId ORDER BY a.age LIMIT 1 \
             MATCH (b:Person) WHERE b.name = 'Alice' \
             RETURN friendId AS id",
    )
    .expect("staged scalar alias should be usable in final RETURN");

    let GraphQuery::Staged(staged) = query else {
        panic!("scalar alias return query should compile to a staged graph query");
    };
    assert_eq!(
        staged.final_plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::StageValue {
                alias: "friendId".to_string(),
            },
            alias: "id".to_string(),
        }]
    );
}

#[test]
fn keeps_bare_scalar_alias_before_match_transparent() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person) \
             WITH a.id AS friendId \
             MATCH (b:Person) WHERE b.id = friendId \
             RETURN b.name AS name",
    )
    .expect("bare scalar alias without row modifiers should remain transparent");

    assert!(matches!(query, GraphQuery::Plan(_)));
}

#[test]
fn compiles_staged_with_count_aggregation_before_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) \
         WITH a, count(b) AS deg \
         MATCH (a)-[:KNOWS]->(c:Person) \
         RETURN a.name AS name, deg",
    )
    .expect("staged aggregate WITH before MATCH should compile");

    let GraphQuery::Staged(staged) = query else {
        panic!("aggregate stage should compile to a staged graph query");
    };
    let stage = staged
        .stages
        .first()
        .expect("staged query should have stage 0");
    assert_eq!(
        stage.exports,
        vec![
            GraphStageExport::NodeKey {
                variable: "a".to_string(),
                column: "a_id".to_string(),
            },
            GraphStageExport::AggregateValue {
                alias: "deg".to_string(),
                column: "deg".to_string(),
            },
        ]
    );
    assert_eq!(
        stage.plan.projections,
        vec![
            Projection::Key {
                variable: "a".to_string(),
                alias: "a_id".to_string(),
            },
            Projection::Aggregate {
                function: AggregateFunction::Count,
                target: AggregateTarget::VariableKey {
                    variable: "b".to_string(),
                },
                distinct: false,
                alias: "deg".to_string(),
            },
        ]
    );
    assert_eq!(
        staged.final_plan.projections,
        vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "a".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("name".to_string()),
            },
            Projection::Expression {
                expression: ScalarExpression::StageValue {
                    alias: "deg".to_string(),
                },
                alias: "deg".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_staged_with_sum_aggregation_before_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) \
         WITH a, sum(b.age) AS total_age \
         MATCH (a)-[:KNOWS]->(c:Person) \
         RETURN a.name AS name, total_age",
    )
    .expect("staged sum aggregate WITH before MATCH should compile");

    let GraphQuery::Staged(staged) = query else {
        panic!("sum aggregate stage should compile to a staged graph query");
    };
    assert!(matches!(
        staged
            .stages
            .first()
            .and_then(|stage| stage.plan.projections.get(1)),
        Some(Projection::Aggregate {
            function: AggregateFunction::Sum,
            target: AggregateTarget::Property(PropertyRef { variable, property }),
            alias,
            ..
        }) if variable == "b" && property == "age" && alias == "total_age"
    ));
}

#[test]
fn compiles_staged_with_two_group_keys_before_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) \
         WITH a, b, count(*) AS c \
         MATCH (a)-[:KNOWS]->(b) \
         RETURN a.name AS a, b.name AS b, c",
    )
    .expect("staged aggregate WITH with two group keys should compile");

    let GraphQuery::Staged(staged) = query else {
        panic!("two-key aggregate stage should compile to a staged graph query");
    };
    let stage = staged
        .stages
        .first()
        .expect("staged query should have stage 0");
    assert_eq!(
        stage.exports,
        vec![
            GraphStageExport::NodeKey {
                variable: "a".to_string(),
                column: "a_id".to_string(),
            },
            GraphStageExport::NodeKey {
                variable: "b".to_string(),
                column: "b_id".to_string(),
            },
            GraphStageExport::AggregateValue {
                alias: "c".to_string(),
                column: "c".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_staged_aggregate_relationship_alias_carry_before_match() {
    let graph = staged_aggregate_relationship_carry_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH ()-[r1]->(:X) \
         WITH r1 AS r2, count(*) AS c \
         MATCH ()-[r2]->() \
         RETURN r2 AS rel",
    )
    .expect("staged aggregate should carry aliased relationship key");

    let GraphQuery::Staged(staged) = query else {
        panic!("relationship aggregate carry should compile to a staged graph query");
    };
    let stage = staged
        .stages
        .first()
        .expect("staged query should have stage 0");
    assert_eq!(
        stage.exports,
        vec![
            GraphStageExport::RelationshipKey {
                variable: "r2".to_string(),
                column: "r2_id".to_string(),
            },
            GraphStageExport::AggregateValue {
                alias: "c".to_string(),
                column: "c".to_string(),
            },
        ]
    );
    assert_eq!(
        staged.final_plan.relationships,
        vec![RelationshipPattern {
            variable: Some("r2".to_string()),
            relationship_type: "REL".to_string(),
            left: "__coral_node_0_0".to_string(),
            direction: Direction::Outgoing,
            right: "__coral_node_0_1".to_string(),
        }]
    );
    assert_eq!(
        staged.final_plan.projection_output_names(),
        vec!["rel.__id", "rel.__type"]
    );
}

#[test]
fn compiles_staged_aggregate_node_and_relationship_alias_carry_before_match() {
    let graph = staged_aggregate_relationship_carry_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a)-[r1]->(b:X) \
         WITH a, r1 AS r2, b, count(*) AS c \
         MATCH (a)-[r2]->(b) \
         RETURN r2 AS rel",
    )
    .expect("staged aggregate should carry node and aliased relationship keys");

    let GraphQuery::Staged(staged) = query else {
        panic!("node plus relationship aggregate carry should compile to staged");
    };
    let stage = staged
        .stages
        .first()
        .expect("staged query should have stage 0");
    assert_eq!(
        stage.exports,
        vec![
            GraphStageExport::NodeKey {
                variable: "a".to_string(),
                column: "a_id".to_string(),
            },
            GraphStageExport::RelationshipKey {
                variable: "r2".to_string(),
                column: "r2_id".to_string(),
            },
            GraphStageExport::NodeKey {
                variable: "b".to_string(),
                column: "b_id".to_string(),
            },
            GraphStageExport::AggregateValue {
                alias: "c".to_string(),
                column: "c".to_string(),
            },
        ]
    );
    assert_eq!(
        staged.final_plan.relationships,
        vec![RelationshipPattern {
            variable: Some("r2".to_string()),
            relationship_type: "REL".to_string(),
            left: "a".to_string(),
            direction: Direction::Outgoing,
            right: "b".to_string(),
        }]
    );
}

#[test]
fn compiles_staged_aggregate_relationship_carry_with_return_order() {
    let graph = staged_aggregate_relationship_carry_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a)-[r]->(b:X) \
         WITH a, r, b, count(*) AS c ORDER BY c \
         MATCH (a)-[r]->(b) \
         RETURN r AS rel ORDER BY rel.id",
    )
    .expect("staged aggregate should carry relationship key through ordered stage and return");

    let GraphQuery::Staged(staged) = query else {
        panic!("ordered relationship aggregate carry should compile to staged");
    };
    let stage = staged
        .stages
        .first()
        .expect("staged query should have stage 0");
    assert_eq!(
        stage.plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("c".to_string()),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
    assert_eq!(
        staged.final_plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("rel.__id".to_string()),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_staged_aggregate_alias_in_final_where() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) \
         WITH a, count(b) AS deg \
         MATCH (a)-[:KNOWS]->(c:Person) WHERE deg > 1 \
         RETURN a.name AS name, deg",
    )
    .expect("staged aggregate alias should be usable in final WHERE");

    assert!(matches!(query, GraphQuery::Staged(_)));
}

#[test]
fn compiles_staged_aggregate_order_limit_before_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) \
         WITH a, count(b) AS deg ORDER BY deg DESC LIMIT 1 \
         MATCH (a)-[:KNOWS]->(c:Person) \
         RETURN a.name AS name, deg",
    )
    .expect("staged aggregate WITH ORDER BY/LIMIT should compile");

    let GraphQuery::Staged(staged) = query else {
        panic!("ordered aggregate stage should compile to a staged graph query");
    };
    let stage = staged
        .stages
        .first()
        .expect("staged query should have stage 0");
    assert_eq!(
        stage.plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("deg".to_string()),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
    assert_eq!(stage.plan.limit, Some(1));
}

#[test]
fn compiles_staged_aggregate_with_incoming_final_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) \
         WITH a, count(b) AS deg \
         MATCH (c:Person)-[:KNOWS]->(a) \
         RETURN a.name AS name, deg",
    )
    .expect("staged aggregate route should allow incoming final matches");

    let GraphQuery::Staged(staged) = query else {
        panic!("incoming aggregate final match should compile to a staged graph query");
    };
    assert_eq!(
        staged.final_plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "KNOWS".to_string(),
            left: "c".to_string(),
            direction: Direction::Outgoing,
            right: "a".to_string(),
        }]
    );
}

#[test]
fn compiles_staged_aggregate_with_undirected_final_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) \
         WITH a, count(b) AS deg \
         MATCH (a)-[:KNOWS]-(c:Person) \
         RETURN a.name AS name, deg",
    )
    .expect("staged aggregate route should allow undirected final matches");

    let GraphQuery::Staged(staged) = query else {
        panic!("undirected aggregate final match should compile to a staged graph query");
    };
    assert_eq!(
        staged.final_plan.relationships,
        vec![RelationshipPattern {
            variable: None,
            relationship_type: "KNOWS".to_string(),
            left: "a".to_string(),
            direction: Direction::Undirected,
            right: "c".to_string(),
        }]
    );
}

#[test]
fn compiles_staged_aggregate_with_multihop_final_match() {
    let graph = staged_planning_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) \
         WITH a, count(b) AS deg \
         MATCH (a)-[:KNOWS]->(x:Person)-[:KNOWS]->(c:Person) \
         RETURN a.name AS name, c.name AS c, deg",
    )
    .expect("staged aggregate route should allow fixed multi-hop final matches");

    let GraphQuery::Staged(staged) = query else {
        panic!("aggregate multi-hop final match should compile to a staged graph query");
    };
    assert_eq!(
        staged.final_plan.relationships,
        vec![
            RelationshipPattern {
                variable: None,
                relationship_type: "KNOWS".to_string(),
                left: "a".to_string(),
                direction: Direction::Outgoing,
                right: "x".to_string(),
            },
            RelationshipPattern {
                variable: None,
                relationship_type: "KNOWS".to_string(),
                left: "x".to_string(),
                direction: Direction::Outgoing,
                right: "c".to_string(),
            },
        ]
    );
}

#[test]
fn rejects_adjacent_staged_aggregation_shapes() {
    let cases = [
        (
            "distinct aggregate stage",
            "MATCH (a:Person)-[:KNOWS]->(b:Person) \
             WITH DISTINCT a, count(b) AS deg \
             MATCH (a)-[:KNOWS]->(c:Person) \
             RETURN a.name AS name, deg",
        ),
        (
            "scalar alias carry",
            "MATCH (a:Person)-[:KNOWS]->(b:Person) \
             WITH a.name AS name, count(b) AS deg \
             MATCH (a)-[:KNOWS]->(c:Person) \
             RETURN name, deg",
        ),
        (
            "initial WHERE before aggregate WITH",
            "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE b.age > 30 \
             WITH a, count(b) AS deg \
             MATCH (a)-[:KNOWS]->(c:Person) \
             RETURN a.name AS name, deg",
        ),
        (
            "post-aggregate WITH WHERE",
            "MATCH (a:Person)-[:KNOWS]->(b:Person) \
             WITH a, count(b) AS deg WHERE deg > 1 \
             MATCH (a)-[:KNOWS]->(c:Person) \
             RETURN a.name AS name, deg",
        ),
        (
            "graph-object return",
            "MATCH (a:Person)-[:KNOWS]->(b:Person) \
             WITH a, count(b) AS deg \
             MATCH (a)-[:KNOWS]->(c:Person) \
             RETURN a, deg",
        ),
        (
            "unlabeled final target",
            "MATCH (a:Person)-[:KNOWS]->(b:Person) \
             WITH a, count(b) AS deg \
             MATCH (a)-[:KNOWS]->(c) \
             RETURN a.name AS name, deg",
        ),
        (
            "two aggregate aliases",
            "MATCH (a:Person)-[:KNOWS]->(b:Person) \
             WITH a, count(b) AS deg, sum(b.age) AS total_age \
             MATCH (a)-[:KNOWS]->(c:Person) \
             RETURN a.name AS name, deg",
        ),
        (
            "subquery alias stage",
            "MATCH (a:Person)-[:KNOWS]->(b:Person) \
             WITH a, COUNT { MATCH (b)-[:KNOWS]->(:Person) } AS downstream \
             MATCH (a)-[:KNOWS]->(c:Person) \
             RETURN a.name AS name, downstream",
        ),
        (
            "multi-stage aggregate pipeline",
            "MATCH (a:Person)-[:KNOWS]->(b:Person) \
             WITH a, count(b) AS deg \
             MATCH (a)-[:KNOWS]->(c:Person) \
             WITH a, deg \
             MATCH (a)-[:KNOWS]->(d:Person) \
             RETURN a.name AS name, deg",
        ),
        (
            "unlabeled intermediate multi-hop final match",
            "MATCH (a:Person)-[:KNOWS]->(b:Person) \
             WITH a, count(b) AS deg \
             MATCH (a)-[:KNOWS]->(c)-[:KNOWS]->(d:Person) \
             RETURN a.name AS name, deg",
        ),
    ];

    for (name, cypher) in cases {
        assert_staged_aggregation_reject(name, cypher);
    }
}

#[test]
fn rejects_adjacent_staged_with_order_limit_shapes() {
    let cases = [
        (
            "initial WHERE before WITH",
            "MATCH (a:Person) \
             WHERE a.age > 30 \
             WITH a ORDER BY a.age LIMIT 2 \
             MATCH (a)-[:KNOWS]->(b:Person) \
             RETURN a.name AS a, b.name AS b",
        ),
        (
            "graph-object return",
            "MATCH (a:Person) \
             WITH a ORDER BY a.age LIMIT 2 \
             MATCH (a)-[:KNOWS]->(b:Person) \
             RETURN a AS a, b.name AS b",
        ),
        (
            "unverified ORDER BY property",
            "MATCH (a:Person) \
             WITH a ORDER BY a.city LIMIT 2 \
             MATCH (a)-[:KNOWS]->(b:Person) \
             RETURN a.name AS a, b.name AS b",
        ),
        (
            "scalar alias DISTINCT stage",
            "MATCH (a:Person) \
             WITH DISTINCT a.id AS friendId ORDER BY friendId LIMIT 1 \
             MATCH (b:Person) WHERE b.id = friendId \
             RETURN b.name AS b",
        ),
        (
            "subquery alias stage",
            "MATCH (a:Person) \
             WITH COUNT { MATCH (b:Person) } AS total ORDER BY total LIMIT 1 \
             MATCH (b:Person) WHERE b.id = total \
             RETURN b.name AS b",
        ),
        (
            "scalar alias graph-object return",
            "MATCH (a:Person) \
             WITH a.id AS friendId ORDER BY a.age LIMIT 1 \
             MATCH (b:Person) WHERE b.id = friendId \
             RETURN b AS b",
        ),
        (
            "scalar alias unlabeled final target",
            "MATCH (a:Person) \
             WITH a.id AS friendId ORDER BY a.age LIMIT 1 \
             MATCH (b) WHERE b.id = friendId \
             RETURN b.name AS b",
        ),
        (
            "unlabeled intermediate multi-hop final match",
            "MATCH (a:Person) \
             WITH a ORDER BY a.age LIMIT 2 \
             MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c:Person) \
             RETURN a.name AS a, c.name AS c",
        ),
        (
            "unlabeled final multi-hop target",
            "MATCH (a:Person) \
             WITH a ORDER BY a.age LIMIT 2 \
             MATCH (a)-[:KNOWS]->(b:Person)-[:KNOWS]->(c) \
             RETURN a.name AS a, b.name AS b",
        ),
    ];

    for (name, cypher) in cases {
        assert_staged_planning_reject(name, cypher);
    }
}

#[test]
fn rejects_staged_with_variable_length_final_relationship() {
    let graph = staged_planning_test_graph();
    let error = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person) \
         WITH a ORDER BY a.age LIMIT 2 \
         MATCH (a)-[:KNOWS*]->(b:Person) \
         RETURN a.name AS a, b.name AS b",
    )
    .expect_err("variable-length final relationship should remain outside multi-hop staging");

    assert!(
        error
            .to_string()
            .contains("variable-length relationship ranges require finite non-negative bounds"),
        "{error}"
    );
}

#[test]
fn rejects_second_staged_scalar_alias_with() {
    let graph = staged_planning_test_graph();
    let error = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person) \
         WITH a.id AS friendId ORDER BY a.age LIMIT 1 \
         MATCH (b:Person) WHERE b.id = friendId \
         WITH b.name AS name ORDER BY b.age LIMIT 1 \
         MATCH (c:Person) WHERE c.name = name \
         RETURN c.name AS c",
    )
    .expect_err("second staged scalar WITH should remain outside the narrow route");

    assert!(
        error
            .to_string()
            .contains("exactly one MATCH ... WITH ... RETURN query part"),
        "{error}"
    );
}

#[test]
fn rejects_staged_with_unlabeled_final_target() {
    assert_staged_planning_reject(
        "unlabeled final target",
        "MATCH (a:Person) \
         WITH a ORDER BY a.age LIMIT 2 \
         MATCH (a)-[:KNOWS]->(b) \
         RETURN a.name AS a, b.name AS b",
    );
}

#[test]
fn rejects_staged_with_untyped_final_relationship() {
    assert_staged_planning_reject(
        "untyped final relationship",
        "MATCH (a:Person) \
         WITH a ORDER BY a.age LIMIT 2 \
         MATCH (a)-->(b:Person) \
         RETURN a.name AS a, b.name AS b",
    );
}

#[test]
fn rejects_staged_bare_relationship_carry_ambiguous_unlabeled_optional_targets() {
    assert_staged_planning_reject(
        "ambiguous bare relationship carry unlabeled optional targets",
        "MATCH ()-[r]->() \
         WITH r LIMIT 1 \
         OPTIONAL MATCH (a2)-[r]->(b2) \
         RETURN id(r) AS r",
    );
}

#[test]
fn rejects_staged_with_limit_zero_before_match() {
    let graph = staged_planning_test_graph();
    let error = compile_cypher_query_for_graph(
        &graph,
        "MATCH (a:Person) \
             WITH a ORDER BY a.age LIMIT 0 \
             MATCH (a)-[:KNOWS]->(b:Person) \
             RETURN b.name AS b",
    )
    .expect_err("LIMIT 0 staged planning remains outside the minimal spike slice");

    assert!(
        error
            .to_string()
            .contains("WITH DISTINCT, ORDER BY, SKIP, and LIMIT before another MATCH require staged query planning"),
        "{error}"
    );
}
