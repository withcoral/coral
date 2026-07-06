use super::*;

#[test]
fn lower_graph_query_renders_literal_unwind_row_source() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let query = GraphQuery::Unwind(GraphUnwind {
        input: None,
        list: ScalarExpression::TypedLiteralList {
            literals: vec![
                Literal::Integer(1),
                Literal::Integer(2),
                Literal::Integer(3),
            ],
            element_type: LiteralListElementType::Integer,
        },
        element_type: LiteralListElementType::Integer,
        variable: "x".to_string(),
        projections: vec![GraphUnwindProjection::Variable {
            alias: "x".to_string(),
        }],
    });

    let translation = graph
        .lower_graph_query(&query)
        .expect("literal UNWIND row source should lower");

    assert_eq!(
        translation.sql(),
        "SELECT UNNEST(make_array(1, 2, 3)) AS \"x\""
    );
}

#[test]
fn lower_graph_query_renders_empty_literal_unwind_row_source() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let query = GraphQuery::Unwind(GraphUnwind {
        input: None,
        list: ScalarExpression::TypedLiteralList {
            literals: Vec::new(),
            element_type: LiteralListElementType::Integer,
        },
        element_type: LiteralListElementType::Integer,
        variable: "x".to_string(),
        projections: vec![GraphUnwindProjection::Variable {
            alias: "x".to_string(),
        }],
    });

    let translation = graph
        .lower_graph_query(&query)
        .expect("empty literal UNWIND row source should lower");

    assert_eq!(
        translation.sql(),
        "SELECT UNNEST(array_resize(make_array(CAST(NULL AS BIGINT)), 0)) AS \"x\""
    );
}

#[test]
fn lower_graph_query_renders_nested_literal_unwind_row_source() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let query = GraphQuery::Unwind(GraphUnwind {
        input: None,
        list: ScalarExpression::TypedLiteralList {
            literals: vec![
                Literal::List(vec![Literal::Integer(1), Literal::Integer(2)]),
                Literal::List(vec![Literal::Integer(3), Literal::Integer(4)]),
            ],
            element_type: LiteralListElementType::IntegerList,
        },
        element_type: LiteralListElementType::IntegerList,
        variable: "pair".to_string(),
        projections: vec![GraphUnwindProjection::Variable {
            alias: "pair".to_string(),
        }],
    });

    let translation = graph
        .lower_graph_query(&query)
        .expect("nested literal UNWIND row source should lower");

    assert_eq!(
        translation.sql(),
        "SELECT UNNEST(make_array(make_array(1, 2), make_array(3, 4))) AS \"pair\""
    );
}

#[test]
fn lower_graph_query_renders_nested_literal_unwind_list_index_projection() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let query = GraphQuery::UnwindPipeline(GraphUnwindPipeline {
        unwind: GraphUnwind {
            input: None,
            list: ScalarExpression::TypedLiteralList {
                literals: vec![
                    Literal::List(vec![Literal::Integer(1), Literal::Integer(2)]),
                    Literal::List(vec![Literal::Integer(3), Literal::Integer(4)]),
                ],
                element_type: LiteralListElementType::IntegerList,
            },
            element_type: LiteralListElementType::IntegerList,
            variable: "pair".to_string(),
            projections: vec![GraphUnwindProjection::Variable {
                alias: "pair".to_string(),
            }],
        },
        final_plan: GraphPlan {
            projections: vec![Projection::Expression {
                expression: ScalarExpression::ListIndex {
                    list: Box::new(ScalarExpression::StageValue {
                        alias: "pair".to_string(),
                    }),
                    index: 0,
                    element_type: LiteralListElementType::Integer,
                },
                alias: "first".to_string(),
            }],
            order_by: vec![OrderKey {
                expression: OrderExpression::ProjectionAlias("first".to_string()),
                direction: OrderDirection::Ascending,
                nulls: None,
            }],
            ..GraphPlan::default()
        },
    });

    let translation = graph
        .lower_graph_query(&query)
        .expect("nested literal UNWIND list index projection should lower");

    assert_eq!(
        translation.sql(),
        "WITH \"stage0\" AS (SELECT UNNEST(make_array(make_array(1, 2), make_array(3, 4))) AS \"pair\") \
         SELECT \"stage0\".\"pair\"[1] AS \"first\" FROM \"stage0\" AS \"stage0\" ORDER BY \"first\" ASC"
    );
}

#[test]
fn lower_graph_query_renders_literal_unwind_aggregate_projection() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let query = GraphQuery::UnwindPipeline(GraphUnwindPipeline {
        unwind: GraphUnwind {
            input: None,
            list: ScalarExpression::TypedLiteralList {
                literals: vec![
                    Literal::Integer(1),
                    Literal::Integer(2),
                    Literal::Integer(3),
                ],
                element_type: LiteralListElementType::Integer,
            },
            element_type: LiteralListElementType::Integer,
            variable: "x".to_string(),
            projections: vec![GraphUnwindProjection::Variable {
                alias: "x".to_string(),
            }],
        },
        final_plan: GraphPlan {
            projections: vec![Projection::Aggregate {
                function: AggregateFunction::Count,
                target: AggregateTarget::Expression(ScalarExpression::StageValue {
                    alias: "x".to_string(),
                }),
                distinct: false,
                alias: "c".to_string(),
            }],
            ..GraphPlan::default()
        },
    });

    let translation = graph
        .lower_graph_query(&query)
        .expect("literal UNWIND aggregate projection should lower");

    assert_eq!(
        translation.sql(),
        "WITH \"stage0\" AS (SELECT UNNEST(make_array(1, 2, 3)) AS \"x\") \
         SELECT COUNT(\"stage0\".\"x\") AS \"c\" FROM \"stage0\" AS \"stage0\""
    );
}

#[test]
fn lower_graph_query_renders_literal_unwind_ordered_projection() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let query = GraphQuery::UnwindPipeline(GraphUnwindPipeline {
        unwind: GraphUnwind {
            input: None,
            list: ScalarExpression::TypedLiteralList {
                literals: vec![
                    Literal::Integer(3),
                    Literal::Integer(1),
                    Literal::Integer(2),
                ],
                element_type: LiteralListElementType::Integer,
            },
            element_type: LiteralListElementType::Integer,
            variable: "x".to_string(),
            projections: vec![GraphUnwindProjection::Variable {
                alias: "x".to_string(),
            }],
        },
        final_plan: GraphPlan {
            projections: vec![Projection::Expression {
                expression: ScalarExpression::StageValue {
                    alias: "x".to_string(),
                },
                alias: "x".to_string(),
            }],
            order_by: vec![OrderKey {
                expression: OrderExpression::ProjectionAlias("x".to_string()),
                direction: OrderDirection::Ascending,
                nulls: None,
            }],
            ..GraphPlan::default()
        },
    });

    let translation = graph
        .lower_graph_query(&query)
        .expect("literal UNWIND ordered projection should lower");

    assert_eq!(
        translation.sql(),
        "WITH \"stage0\" AS (SELECT UNNEST(make_array(3, 1, 2)) AS \"x\") \
         SELECT \"stage0\".\"x\" AS \"x\" FROM \"stage0\" AS \"stage0\" ORDER BY \"x\" ASC"
    );
}

#[test]
fn lower_graph_query_renders_with_alias_unwind_row_source() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let query = GraphQuery::Unwind(GraphUnwind {
        input: Some(GraphUnwindInput {
            projections: vec![GraphUnwindInputProjection {
                expression: ScalarExpression::TypedLiteralList {
                    literals: vec![
                        Literal::Integer(1),
                        Literal::Integer(2),
                        Literal::Integer(3),
                    ],
                    element_type: LiteralListElementType::Integer,
                },
                alias: "list".to_string(),
                element_type: LiteralListElementType::Integer,
            }],
        }),
        list: ScalarExpression::StageValue {
            alias: "list".to_string(),
        },
        element_type: LiteralListElementType::Integer,
        variable: "x".to_string(),
        projections: vec![GraphUnwindProjection::Variable {
            alias: "x".to_string(),
        }],
    });

    let translation = graph
        .lower_graph_query(&query)
        .expect("WITH alias UNWIND row source should lower");

    assert_eq!(
        translation.sql(),
        "SELECT UNNEST(\"__coral_unwind_input\".\"list\") AS \"x\" \
         FROM (SELECT make_array(1, 2, 3) AS \"list\") AS \"__coral_unwind_input\""
    );
}

#[test]
fn lower_graph_query_renders_with_alias_concat_unwind_row_source() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let query = GraphQuery::Unwind(GraphUnwind {
        input: Some(GraphUnwindInput {
            projections: vec![
                GraphUnwindInputProjection {
                    expression: ScalarExpression::TypedLiteralList {
                        literals: vec![Literal::Integer(1), Literal::Integer(2)],
                        element_type: LiteralListElementType::Integer,
                    },
                    alias: "first".to_string(),
                    element_type: LiteralListElementType::Integer,
                },
                GraphUnwindInputProjection {
                    expression: ScalarExpression::TypedLiteralList {
                        literals: vec![Literal::Integer(3), Literal::Integer(4)],
                        element_type: LiteralListElementType::Integer,
                    },
                    alias: "second".to_string(),
                    element_type: LiteralListElementType::Integer,
                },
            ],
        }),
        list: ScalarExpression::ListConcat {
            left: Box::new(ScalarExpression::StageValue {
                alias: "first".to_string(),
            }),
            right: Box::new(ScalarExpression::StageValue {
                alias: "second".to_string(),
            }),
        },
        element_type: LiteralListElementType::Integer,
        variable: "x".to_string(),
        projections: vec![GraphUnwindProjection::Variable {
            alias: "x".to_string(),
        }],
    });

    let translation = graph
        .lower_graph_query(&query)
        .expect("WITH alias concat UNWIND row source should lower");

    assert_eq!(
        translation.sql(),
        "SELECT UNNEST(array_concat(\"__coral_unwind_input\".\"first\", \"__coral_unwind_input\".\"second\")) AS \"x\" \
         FROM (SELECT make_array(1, 2) AS \"first\", make_array(3, 4) AS \"second\") AS \"__coral_unwind_input\""
    );
}

#[test]
fn lower_graph_plan_renders_forward_relationship_sql() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = ownership_plan(Direction::Outgoing);

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("plan should lower to SQL");

    assert_eq!(
        translation.sql(),
        "SELECT \"n0\".\"full_name\" AS \"owner\", \"n1\".\"service_name\" AS \"service\" \
             FROM \"ops\".\"people\" AS \"n0\" \
             JOIN \"ops\".\"ownerships\" AS \"r0\" ON \"r0\".\"person_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n1\" ON \"r0\".\"service_id\" = \"n1\".\"id\" \
             WHERE \"n1\".\"tier\" = 'prod' ORDER BY \"n0\".\"full_name\" ASC LIMIT 25"
    );
}

#[test]
fn lower_graph_query_renders_staged_with_order_limit_cte() {
    let graph = Declaration::from_yaml(STAGED_GRAPH).expect("graph should parse");
    let query = GraphQuery::Staged(GraphStagedQuery {
        stages: vec![GraphStage {
            plan: GraphPlan {
                nodes: vec![NodePattern {
                    variable: "a".to_string(),
                    label: "Person".to_string(),
                }],
                relationships: Vec::new(),
                optional_relationships: Vec::new(),
                optional_matches: Vec::new(),
                distinct: false,
                projections: vec![Projection::Key {
                    variable: "a".to_string(),
                    alias: "a_id".to_string(),
                }],
                predicates: Vec::new(),
                predicate: None,
                post_projection_predicate: None,
                order_by: vec![OrderKey {
                    expression: OrderExpression::Property(PropertyRef {
                        variable: "a".to_string(),
                        property: "age".to_string(),
                    }),
                    direction: OrderDirection::Ascending,
                    nulls: None,
                }],
                skip: None,
                limit: Some(2),
            },
            exports: vec![GraphStageExport::NodeKey {
                variable: "a".to_string(),
                column: "a_id".to_string(),
            }],
        }],
        final_plan: GraphPlan {
            nodes: vec![
                NodePattern {
                    variable: "a".to_string(),
                    label: "Person".to_string(),
                },
                NodePattern {
                    variable: "b".to_string(),
                    label: "Person".to_string(),
                },
            ],
            relationships: vec![RelationshipPattern {
                variable: None,
                relationship_type: "KNOWS".to_string(),
                left: "a".to_string(),
                direction: Direction::Outgoing,
                right: "b".to_string(),
            }],
            optional_relationships: Vec::new(),
            optional_matches: Vec::new(),
            distinct: false,
            projections: vec![
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
            ],
            predicates: Vec::new(),
            predicate: None,
            post_projection_predicate: None,
            order_by: Vec::new(),
            skip: None,
            limit: None,
        },
    });

    let translation = graph
        .lower_graph_query(&query)
        .expect("staged graph query should lower");

    assert_eq!(
        translation.sql(),
        "WITH \"stage0\" AS (SELECT \"n0\".\"id\" AS \"a_id\" \
             FROM \"ops\".\"people\" AS \"n0\" ORDER BY \"n0\".\"age\" ASC LIMIT 2) \
             SELECT \"n0\".\"full_name\" AS \"a\", \"n1\".\"full_name\" AS \"b\" \
             FROM \"stage0\" AS \"stage0\" \
             JOIN \"ops\".\"people\" AS \"n0\" ON \"n0\".\"id\" = \"stage0\".\"a_id\" \
             JOIN \"ops\".\"knows\" AS \"r0\" ON \"r0\".\"person_id\" = \"stage0\".\"a_id\" \
             JOIN \"ops\".\"people\" AS \"n1\" ON \"r0\".\"friend_id\" = \"n1\".\"id\""
    );
}

#[test]
fn lower_graph_query_renders_staged_incoming_final_match() {
    let graph = Declaration::from_yaml(STAGED_GRAPH).expect("graph should parse");
    let query = staged_order_limit_query(RelationshipPattern {
        variable: None,
        relationship_type: "KNOWS".to_string(),
        left: "b".to_string(),
        direction: Direction::Outgoing,
        right: "a".to_string(),
    });

    let translation = graph
        .lower_graph_query(&query)
        .expect("staged incoming graph query should lower");

    assert_eq!(
        translation.sql(),
        "WITH \"stage0\" AS (SELECT \"n0\".\"id\" AS \"a_id\" \
             FROM \"ops\".\"people\" AS \"n0\" ORDER BY \"n0\".\"age\" ASC LIMIT 2) \
             SELECT \"n0\".\"full_name\" AS \"a\", \"n1\".\"full_name\" AS \"b\" \
             FROM \"stage0\" AS \"stage0\" \
             JOIN \"ops\".\"people\" AS \"n0\" ON \"n0\".\"id\" = \"stage0\".\"a_id\" \
             JOIN \"ops\".\"knows\" AS \"r0\" ON \"r0\".\"friend_id\" = \"stage0\".\"a_id\" \
             JOIN \"ops\".\"people\" AS \"n1\" ON \"r0\".\"person_id\" = \"n1\".\"id\""
    );
}

#[test]
fn lower_graph_query_renders_staged_undirected_final_match() {
    let graph = Declaration::from_yaml(STAGED_GRAPH).expect("graph should parse");
    let query = staged_order_limit_query(RelationshipPattern {
        variable: None,
        relationship_type: "KNOWS".to_string(),
        left: "a".to_string(),
        direction: Direction::Undirected,
        right: "b".to_string(),
    });

    let translation = graph
        .lower_graph_query(&query)
        .expect("staged undirected graph query should lower");

    assert_eq!(
        translation.sql(),
        "WITH \"stage0\" AS (SELECT \"n0\".\"id\" AS \"a_id\" \
             FROM \"ops\".\"people\" AS \"n0\" ORDER BY \"n0\".\"age\" ASC LIMIT 2) \
             SELECT \"n0\".\"full_name\" AS \"a\", \"n1\".\"full_name\" AS \"b\" \
             FROM \"stage0\" AS \"stage0\" \
             JOIN \"ops\".\"people\" AS \"n0\" ON \"n0\".\"id\" = \"stage0\".\"a_id\" \
             JOIN \"ops\".\"knows\" AS \"r0\" ON (\"r0\".\"person_id\" = \"stage0\".\"a_id\" OR \"r0\".\"friend_id\" = \"stage0\".\"a_id\") \
             JOIN \"ops\".\"people\" AS \"n1\" ON ((\"r0\".\"person_id\" = \"stage0\".\"a_id\" AND \"r0\".\"friend_id\" = \"n1\".\"id\") OR (\"r0\".\"friend_id\" = \"stage0\".\"a_id\" AND \"r0\".\"person_id\" = \"n1\".\"id\"))"
    );
}

#[test]
fn lower_graph_query_renders_staged_multihop_final_match() {
    let graph = Declaration::from_yaml(STAGED_GRAPH).expect("graph should parse");
    let mut query = staged_order_limit_query(RelationshipPattern {
        variable: None,
        relationship_type: "KNOWS".to_string(),
        left: "a".to_string(),
        direction: Direction::Outgoing,
        right: "x".to_string(),
    });
    let GraphQuery::Staged(staged) = &mut query else {
        panic!("helper should produce a staged query");
    };
    let Some(target_node) = staged.final_plan.nodes.get_mut(1) else {
        panic!("helper should include a final target node");
    };
    target_node.variable = "x".to_string();
    staged.final_plan.nodes.push(NodePattern {
        variable: "b".to_string(),
        label: "Person".to_string(),
    });
    staged.final_plan.relationships.push(RelationshipPattern {
        variable: None,
        relationship_type: "KNOWS".to_string(),
        left: "x".to_string(),
        direction: Direction::Outgoing,
        right: "b".to_string(),
    });

    let translation = graph
        .lower_graph_query(&query)
        .expect("staged multi-hop graph query should lower");

    assert_eq!(
        translation.sql(),
        "WITH \"stage0\" AS (SELECT \"n0\".\"id\" AS \"a_id\" \
             FROM \"ops\".\"people\" AS \"n0\" ORDER BY \"n0\".\"age\" ASC LIMIT 2) \
             SELECT \"n0\".\"full_name\" AS \"a\", \"n2\".\"full_name\" AS \"b\" \
             FROM \"stage0\" AS \"stage0\" \
             JOIN \"ops\".\"people\" AS \"n0\" ON \"n0\".\"id\" = \"stage0\".\"a_id\" \
             JOIN \"ops\".\"knows\" AS \"r0\" ON \"r0\".\"person_id\" = \"stage0\".\"a_id\" \
             JOIN \"ops\".\"people\" AS \"n1\" ON \"r0\".\"friend_id\" = \"n1\".\"id\" \
             JOIN \"ops\".\"knows\" AS \"r1\" ON \"r1\".\"person_id\" = \"n1\".\"id\" \
             JOIN \"ops\".\"people\" AS \"n2\" ON \"r1\".\"friend_id\" = \"n2\".\"id\""
    );
}

#[test]
fn lower_graph_query_renders_staged_optional_final_match() {
    let graph = Declaration::from_yaml(STAGED_GRAPH).expect("graph should parse");
    let mut query = staged_order_limit_query(RelationshipPattern {
        variable: None,
        relationship_type: "KNOWS".to_string(),
        left: "a".to_string(),
        direction: Direction::Outgoing,
        right: "b".to_string(),
    });
    let GraphQuery::Staged(staged) = &mut query else {
        panic!("helper should produce a staged query");
    };
    staged.final_plan.optional_relationships = vec![0];
    staged.final_plan.optional_matches = vec![OptionalMatchScope {
        node_indices: vec![1],
        relationship_indices: vec![0],
        predicate: None,
    }];

    let translation = graph
        .lower_graph_query(&query)
        .expect("staged optional graph query should lower");

    assert_eq!(
        translation.sql(),
        "WITH \"stage0\" AS (SELECT \"n0\".\"id\" AS \"a_id\" \
             FROM \"ops\".\"people\" AS \"n0\" ORDER BY \"n0\".\"age\" ASC LIMIT 2) \
             SELECT \"n0\".\"full_name\" AS \"a\", \"n1\".\"full_name\" AS \"b\" \
             FROM \"stage0\" AS \"stage0\" \
             JOIN \"ops\".\"people\" AS \"n0\" ON \"n0\".\"id\" = \"stage0\".\"a_id\" \
             LEFT JOIN \"ops\".\"knows\" AS \"r0\" ON \"r0\".\"person_id\" = \"stage0\".\"a_id\" \
             LEFT JOIN \"ops\".\"people\" AS \"n1\" ON \"r0\".\"friend_id\" = \"n1\".\"id\""
    );
}

#[test]
fn lower_graph_query_renders_staged_relationship_key_optional_match() {
    let graph = Declaration::from_yaml(STAGED_GRAPH).expect("graph should parse");
    let query = staged_relationship_key_optional_query();

    let translation = graph
        .lower_graph_query(&query)
        .expect("staged relationship-key optional graph query should lower");

    assert_eq!(
        translation.sql(),
        "WITH \"stage0\" AS (SELECT \"r0\".\"id\" AS \"r_id\" \
             FROM \"ops\".\"people\" AS \"n0\" \
             JOIN \"ops\".\"knows\" AS \"r0\" ON \"r0\".\"person_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"people\" AS \"n1\" ON \"r0\".\"friend_id\" = \"n1\".\"id\" LIMIT 1) \
             SELECT \"n0\".\"full_name\" AS \"a\", \"stage0\".\"r_id\" AS \"r\", \"n1\".\"full_name\" AS \"b\" \
             FROM \"stage0\" AS \"stage0\" \
             JOIN \"ops\".\"knows\" AS \"r0\" ON \"r0\".\"id\" = \"stage0\".\"r_id\" \
             LEFT JOIN \"ops\".\"people\" AS \"n0\" ON \"r0\".\"person_id\" = \"n0\".\"id\" \
             LEFT JOIN \"ops\".\"people\" AS \"n1\" ON \"r0\".\"friend_id\" = \"n1\".\"id\""
    );
}

#[test]
fn lower_graph_query_renders_staged_node_relationship_key_optional_miss() {
    let graph = Declaration::from_yaml(STAGED_GRAPH).expect("graph should parse");
    let query = staged_node_relationship_key_optional_miss_query();

    let translation = graph
        .lower_graph_query(&query)
        .expect("staged node and relationship-key optional graph query should lower");

    assert_eq!(
        translation.sql(),
        "WITH \"stage0\" AS (SELECT \"r0\".\"id\" AS \"r_id\", \"n0\".\"id\" AS \"a1_id\" \
             FROM \"ops\".\"people\" AS \"n0\" \
             JOIN \"ops\".\"knows\" AS \"r0\" ON \"r0\".\"person_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"people\" AS \"n1\" ON \"r0\".\"friend_id\" = \"n1\".\"id\" ORDER BY \"r0\".\"id\" ASC LIMIT 1) \
             SELECT \"n0\".\"full_name\" AS \"a\", \"stage0\".\"r_id\" AS \"r\", \"n1\".\"full_name\" AS \"b\" \
             FROM \"stage0\" AS \"stage0\" \
             JOIN \"ops\".\"people\" AS \"n0\" ON \"n0\".\"id\" = \"stage0\".\"a1_id\" \
             JOIN \"ops\".\"knows\" AS \"r0\" ON \"r0\".\"id\" = \"stage0\".\"r_id\" \
             LEFT JOIN \"ops\".\"people\" AS \"n1\" ON (\"r0\".\"person_id\" = \"n1\".\"id\" AND \"r0\".\"friend_id\" = \"stage0\".\"a1_id\") AND (\"r0\".\"id\" = \"stage0\".\"r_id\")"
    );
}

#[test]
fn lower_graph_query_renders_staged_scalar_alias_cte() {
    let graph = Declaration::from_yaml(STAGED_GRAPH).expect("graph should parse");
    let query = GraphQuery::Staged(GraphStagedQuery {
        stages: vec![GraphStage {
            plan: GraphPlan {
                nodes: vec![NodePattern {
                    variable: "a".to_string(),
                    label: "Person".to_string(),
                }],
                relationships: Vec::new(),
                optional_relationships: Vec::new(),
                optional_matches: Vec::new(),
                distinct: false,
                projections: vec![Projection::Property {
                    property: PropertyRef {
                        variable: "a".to_string(),
                        property: "id".to_string(),
                    },
                    alias: Some("friendId".to_string()),
                }],
                predicates: Vec::new(),
                predicate: None,
                post_projection_predicate: None,
                order_by: vec![OrderKey {
                    expression: OrderExpression::Property(PropertyRef {
                        variable: "a".to_string(),
                        property: "age".to_string(),
                    }),
                    direction: OrderDirection::Ascending,
                    nulls: None,
                }],
                skip: None,
                limit: Some(1),
            },
            exports: vec![GraphStageExport::ScalarValue {
                alias: "friendId".to_string(),
                source: "friendId".to_string(),
            }],
        }],
        final_plan: GraphPlan {
            nodes: vec![NodePattern {
                variable: "b".to_string(),
                label: "Person".to_string(),
            }],
            relationships: Vec::new(),
            optional_relationships: Vec::new(),
            optional_matches: Vec::new(),
            distinct: false,
            projections: vec![Projection::Property {
                property: PropertyRef {
                    variable: "b".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("name".to_string()),
            }],
            predicates: Vec::new(),
            predicate: Some(PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs: ScalarExpression::Property(PropertyRef {
                    variable: "b".to_string(),
                    property: "id".to_string(),
                }),
                operator: ComparisonOperator::Equal,
                rhs: ScalarPredicateRhs::Expression(ScalarExpression::StageValue {
                    alias: "friendId".to_string(),
                }),
            })),
            post_projection_predicate: None,
            order_by: Vec::new(),
            skip: None,
            limit: None,
        },
    });

    let translation = graph
        .lower_graph_query(&query)
        .expect("staged scalar alias graph query should lower");

    assert_eq!(
        translation.sql(),
        "WITH \"stage0\" AS (SELECT \"n0\".\"id\" AS \"friendId\" \
             FROM \"ops\".\"people\" AS \"n0\" ORDER BY \"n0\".\"age\" ASC LIMIT 1) \
             SELECT \"n0\".\"full_name\" AS \"name\" \
             FROM \"ops\".\"people\" AS \"n0\" CROSS JOIN \"stage0\" AS \"stage0\" \
             WHERE \"n0\".\"id\" = \"stage0\".\"friendId\""
    );
}

#[test]
fn lower_graph_query_renders_staged_aggregate_cte() {
    let graph = Declaration::from_yaml(STAGED_GRAPH).expect("graph should parse");
    let query = staged_aggregate_query();

    let translation = graph
        .lower_graph_query(&query)
        .expect("staged aggregate graph query should lower");

    assert_eq!(
        translation.sql(),
        "WITH \"stage0\" AS (SELECT \"n0\".\"id\" AS \"a_id\", COUNT(\"n1\".\"id\") AS \"deg\" \
             FROM \"ops\".\"people\" AS \"n0\" \
             JOIN \"ops\".\"knows\" AS \"r0\" ON \"r0\".\"person_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"people\" AS \"n1\" ON \"r0\".\"friend_id\" = \"n1\".\"id\" \
             GROUP BY \"n0\".\"id\") \
             SELECT \"n0\".\"full_name\" AS \"name\", \"stage0\".\"deg\" AS \"deg\" \
             FROM \"stage0\" AS \"stage0\" \
             JOIN \"ops\".\"people\" AS \"n0\" ON \"n0\".\"id\" = \"stage0\".\"a_id\" \
             JOIN \"ops\".\"knows\" AS \"r0\" ON \"r0\".\"person_id\" = \"stage0\".\"a_id\" \
             JOIN \"ops\".\"people\" AS \"n1\" ON \"r0\".\"friend_id\" = \"n1\".\"id\""
    );
}

#[test]
fn lower_graph_query_renders_staged_aggregate_relationship_key_cte() {
    let graph = Declaration::from_yaml(STAGED_GRAPH).expect("graph should parse");
    let query = staged_aggregate_relationship_key_query();

    let translation = graph
        .lower_graph_query(&query)
        .expect("staged aggregate relationship-key graph query should lower");

    assert_eq!(
        translation.sql(),
        "WITH \"stage0\" AS (SELECT \"n0\".\"id\" AS \"a_id\", \"r0\".\"id\" AS \"r_id\", \"n1\".\"id\" AS \"b_id\", COUNT(*) AS \"c\" \
             FROM \"ops\".\"people\" AS \"n0\" \
             JOIN \"ops\".\"knows\" AS \"r0\" ON \"r0\".\"person_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"people\" AS \"n1\" ON \"r0\".\"friend_id\" = \"n1\".\"id\" \
             GROUP BY \"n0\".\"id\", \"r0\".\"id\", \"n1\".\"id\") \
             SELECT \"stage0\".\"r_id\" AS \"rel.__id\", CASE WHEN \"stage0\".\"r_id\" IS NULL THEN NULL ELSE 'KNOWS' END AS \"rel.__type\" \
             FROM \"stage0\" AS \"stage0\" \
             JOIN \"ops\".\"people\" AS \"n0\" ON \"n0\".\"id\" = \"stage0\".\"a_id\" \
             JOIN \"ops\".\"knows\" AS \"r0\" ON (\"r0\".\"person_id\" = \"stage0\".\"a_id\") AND (\"r0\".\"id\" = \"stage0\".\"r_id\") \
             JOIN \"ops\".\"people\" AS \"n1\" ON ((\"r0\".\"friend_id\" = \"stage0\".\"b_id\") AND (\"r0\".\"id\" = \"stage0\".\"r_id\")) AND (\"n1\".\"id\" = \"stage0\".\"b_id\")"
    );
}

#[test]
fn lower_graph_query_renders_staged_collect_unwind_ctes() {
    let graph = Declaration::from_yaml(STAGED_GRAPH).expect("graph should parse");
    let query = staged_collect_unwind_query();

    let translation = graph
        .lower_graph_query(&query)
        .expect("staged collect UNWIND query should lower");

    assert_eq!(
        translation.sql(),
        "WITH \"stage0\" AS (SELECT \"n0\".\"id\" AS \"a_id\", COALESCE(ARRAY_AGG(\"n1\".\"id\") FILTER (WHERE (\"n1\".\"id\") IS NOT NULL), make_array()) AS \"bees\" \
             FROM \"ops\".\"people\" AS \"n0\" \
             JOIN \"ops\".\"knows\" AS \"r0\" ON \"r0\".\"person_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"people\" AS \"n1\" ON \"r0\".\"friend_id\" = \"n1\".\"id\" \
             GROUP BY \"n0\".\"id\"), \
             \"stage1\" AS (SELECT \"stage0\".\"a_id\" AS \"a_id\", UNNEST(\"stage0\".\"bees\") AS \"b2\" FROM \"stage0\" AS \"stage0\") \
             SELECT \"n0\".\"full_name\" AS \"a\", \"n1\".\"full_name\" AS \"b\" \
             FROM \"stage1\" AS \"stage1\" \
             JOIN \"ops\".\"people\" AS \"n0\" ON \"n0\".\"id\" = \"stage1\".\"a_id\" \
             JOIN \"ops\".\"knows\" AS \"r0\" ON \"r0\".\"person_id\" = \"stage1\".\"a_id\" \
             JOIN \"ops\".\"people\" AS \"n1\" ON (\"r0\".\"friend_id\" = \"stage1\".\"b2\") AND (\"n1\".\"id\" = \"stage1\".\"b2\")"
    );
}

#[test]
fn lower_graph_plan_renders_disconnected_components_as_cross_joins() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![
            NodePattern {
                variable: "source".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "target".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "person".to_string(),
                label: "Person".to_string(),
            },
        ],
        relationships: vec![RelationshipPattern {
            variable: None,
            relationship_type: "DEPENDS_ON".to_string(),
            left: "source".to_string(),
            direction: Direction::Outgoing,
            right: "target".to_string(),
        }],
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![
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
            Projection::Property {
                property: PropertyRef {
                    variable: "person".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("person".to_string()),
            },
        ],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("disconnected mandatory components should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"n0\".\"service_name\" AS \"source\", \"n1\".\"service_name\" AS \"target\", \"n2\".\"full_name\" AS \"person\" \
             FROM \"ops\".\"services\" AS \"n0\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r0\" ON \"r0\".\"from_service_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n1\" ON \"r0\".\"to_service_id\" = \"n1\".\"id\" \
             CROSS JOIN \"ops\".\"people\" AS \"n2\""
    );
}

#[test]
fn lower_graph_plan_renders_reverse_relationship_sql() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![
            NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "person".to_string(),
                label: "Person".to_string(),
            },
        ],
        relationships: vec![RelationshipPattern {
            variable: None,
            relationship_type: "OWNS".to_string(),
            left: "service".to_string(),
            direction: Direction::Incoming,
            right: "person".to_string(),
        }],
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![Projection::Property {
            property: PropertyRef {
                variable: "person".to_string(),
                property: "name".to_string(),
            },
            alias: Some("owner".to_string()),
        }],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("reverse relationship should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"n1\".\"full_name\" AS \"owner\" FROM \"ops\".\"services\" AS \"n0\" \
             JOIN \"ops\".\"ownerships\" AS \"r0\" ON \"r0\".\"service_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"people\" AS \"n1\" ON \"r0\".\"person_id\" = \"n1\".\"id\""
    );
}

#[test]
fn lower_graph_plan_renders_optional_relationship_sql() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![
            NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "person".to_string(),
                label: "Person".to_string(),
            },
        ],
        relationships: vec![RelationshipPattern {
            variable: None,
            relationship_type: "OWNS".to_string(),
            left: "person".to_string(),
            direction: Direction::Outgoing,
            right: "service".to_string(),
        }],
        optional_relationships: vec![0],
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("service".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "person".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("owner".to_string()),
            },
        ],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("optional relationship should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"n0\".\"service_name\" AS \"service\", \"n1\".\"full_name\" AS \"owner\" \
             FROM \"ops\".\"services\" AS \"n0\" \
             LEFT JOIN \"ops\".\"ownerships\" AS \"r0\" ON \"r0\".\"service_id\" = \"n0\".\"id\" \
             LEFT JOIN \"ops\".\"people\" AS \"n1\" ON \"r0\".\"person_id\" = \"n1\".\"id\""
    );
}

#[test]
fn lower_graph_plan_renders_optional_relationship_from_disconnected_component() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![
            NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "person".to_string(),
                label: "Person".to_string(),
            },
            NodePattern {
                variable: "owned".to_string(),
                label: "Service".to_string(),
            },
        ],
        relationships: vec![RelationshipPattern {
            variable: None,
            relationship_type: "OWNS".to_string(),
            left: "person".to_string(),
            direction: Direction::Outgoing,
            right: "owned".to_string(),
        }],
        optional_relationships: vec![0],
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("service".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "person".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("person".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "owned".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("owned".to_string()),
            },
        ],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("optional relationship from disconnected component should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"n0\".\"service_name\" AS \"service\", \"n1\".\"full_name\" AS \"person\", \"n2\".\"service_name\" AS \"owned\" \
             FROM \"ops\".\"services\" AS \"n0\" \
             CROSS JOIN \"ops\".\"people\" AS \"n1\" \
             LEFT JOIN \"ops\".\"ownerships\" AS \"r0\" ON \"r0\".\"person_id\" = \"n1\".\"id\" \
             LEFT JOIN \"ops\".\"services\" AS \"n2\" ON \"r0\".\"service_id\" = \"n2\".\"id\""
    );
}

#[test]
fn lower_graph_plan_renders_node_only_optional_scope_with_single_row_driver() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![NodePattern {
            variable: "person".to_string(),
            label: "Person".to_string(),
        }],
        relationships: Vec::new(),
        optional_relationships: Vec::new(),
        optional_matches: vec![OptionalMatchScope {
            node_indices: vec![0],
            relationship_indices: Vec::new(),
            predicate: None,
        }],
        distinct: false,
        projections: vec![Projection::Property {
            property: PropertyRef {
                variable: "person".to_string(),
                property: "name".to_string(),
            },
            alias: Some("name".to_string()),
        }],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("node-only optional scope should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"n0\".\"full_name\" AS \"name\" \
             FROM (VALUES (1)) AS \"__coral_optional_driver\" \
             LEFT JOIN \"ops\".\"people\" AS \"n0\" ON true"
    );
}

#[test]
fn lower_graph_plan_renders_leading_optional_relationship_scope_with_single_row_driver() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![
            NodePattern {
                variable: "person".to_string(),
                label: "Person".to_string(),
            },
            NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            },
        ],
        relationships: vec![RelationshipPattern {
            variable: Some("owns".to_string()),
            relationship_type: "OWNS".to_string(),
            left: "person".to_string(),
            direction: Direction::Outgoing,
            right: "service".to_string(),
        }],
        optional_relationships: vec![0],
        optional_matches: vec![OptionalMatchScope {
            node_indices: vec![0, 1],
            relationship_indices: vec![0],
            predicate: None,
        }],
        distinct: false,
        projections: vec![Projection::Property {
            property: PropertyRef {
                variable: "owns".to_string(),
                property: "since".to_string(),
            },
            alias: Some("since".to_string()),
        }],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("leading optional relationship scope should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"r0\".\"since\" AS \"since\" \
             FROM (VALUES (1)) AS \"__coral_optional_driver\" \
             LEFT JOIN (\"ops\".\"ownerships\" AS \"r0\" \
             JOIN \"ops\".\"people\" AS \"n0\" ON \"r0\".\"person_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n1\" ON \"r0\".\"service_id\" = \"n1\".\"id\") ON true"
    );
}

#[test]
fn lower_graph_union_preserves_empty_node_only_optional_alternatives_once() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let person_plan = GraphPlan {
        nodes: vec![NodePattern {
            variable: "entity".to_string(),
            label: "Person".to_string(),
        }],
        relationships: Vec::new(),
        optional_relationships: Vec::new(),
        optional_matches: vec![OptionalMatchScope {
            node_indices: vec![0],
            relationship_indices: Vec::new(),
            predicate: None,
        }],
        distinct: false,
        projections: vec![Projection::Property {
            property: PropertyRef {
                variable: "entity".to_string(),
                property: "name".to_string(),
            },
            alias: Some("name".to_string()),
        }],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };
    let mut service_plan = person_plan.clone();
    service_plan.nodes = vec![NodePattern {
        variable: "entity".to_string(),
        label: "Service".to_string(),
    }];
    let query = GraphQuery::Union(GraphUnion {
        first: person_plan,
        branches: vec![GraphUnionBranch {
            all: true,
            plan: service_plan,
        }],
        preserve_empty_result_with_null_row: true,
        outer_projection: None,
        distinct: false,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    });

    let translation = graph
        .lower_graph_query(&query)
        .expect("null-preserving node-only optional union should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"__coral_optional_union\".* \
             FROM (VALUES (1)) AS \"__coral_optional_driver\" \
             LEFT JOIN (SELECT * FROM (SELECT \"n0\".\"full_name\" AS \"name\" FROM \"ops\".\"people\" AS \"n0\") AS \"__coral_union_b0\" \
             UNION ALL SELECT * FROM (SELECT \"n0\".\"service_name\" AS \"name\" FROM \"ops\".\"services\" AS \"n0\") AS \"__coral_union_b1\") AS \"__coral_optional_union\" ON true"
    );
}

#[test]
fn lower_graph_plan_renders_multihop_optional_scope_as_grouped_left_join() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![
            NodePattern {
                variable: "service".to_string(),
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
        ],
        relationships: vec![
            RelationshipPattern {
                variable: None,
                relationship_type: "DEPENDS_ON".to_string(),
                left: "service".to_string(),
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
        ],
        optional_relationships: vec![0, 1],
        optional_matches: vec![OptionalMatchScope {
            node_indices: vec![1, 2],
            relationship_indices: vec![0, 1],
            predicate: None,
        }],
        distinct: false,
        projections: vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("service".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "middle".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("middle".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "target".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("target".to_string()),
            },
        ],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("multi-hop optional scope should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"n0\".\"service_name\" AS \"service\", \"n1\".\"service_name\" AS \"middle\", \"n2\".\"service_name\" AS \"target\" \
             FROM \"ops\".\"services\" AS \"n0\" \
             LEFT JOIN (\"ops\".\"service_dependencies\" AS \"r0\" \
             JOIN \"ops\".\"services\" AS \"n1\" ON \"r0\".\"to_service_id\" = \"n1\".\"id\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r1\" ON \"r1\".\"from_service_id\" = \"n1\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n2\" ON \"r1\".\"to_service_id\" = \"n2\".\"id\") \
             ON \"r0\".\"from_service_id\" = \"n0\".\"id\""
    );
}

#[test]
fn lower_graph_plan_renders_multihop_optional_scope_between_bound_endpoints_as_grouped_left_join() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![
            NodePattern {
                variable: "source".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "target".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "middle".to_string(),
                label: "Service".to_string(),
            },
        ],
        relationships: vec![
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
        ],
        optional_relationships: vec![0, 1],
        optional_matches: vec![OptionalMatchScope {
            node_indices: vec![2],
            relationship_indices: vec![0, 1],
            predicate: None,
        }],
        distinct: false,
        projections: vec![
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
            Projection::Property {
                property: PropertyRef {
                    variable: "middle".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("middle".to_string()),
            },
        ],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("bound-endpoint multi-hop optional scope should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"n0\".\"service_name\" AS \"source\", \"n1\".\"service_name\" AS \"target\", \"n2\".\"service_name\" AS \"middle\" \
             FROM \"ops\".\"services\" AS \"n0\" \
             CROSS JOIN \"ops\".\"services\" AS \"n1\" \
             LEFT JOIN (\"ops\".\"service_dependencies\" AS \"r0\" \
             JOIN \"ops\".\"services\" AS \"n2\" ON \"r0\".\"to_service_id\" = \"n2\".\"id\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r1\" ON \"r1\".\"from_service_id\" = \"n2\".\"id\") \
             ON (\"r0\".\"from_service_id\" = \"n0\".\"id\") AND (\"r1\".\"to_service_id\" = \"n1\".\"id\")"
    );
}

#[test]
fn lower_graph_plan_renders_optional_predicates_inside_join_scope() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![
            NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "person".to_string(),
                label: "Person".to_string(),
            },
        ],
        relationships: vec![RelationshipPattern {
            variable: Some("owns".to_string()),
            relationship_type: "OWNS".to_string(),
            left: "person".to_string(),
            direction: Direction::Outgoing,
            right: "service".to_string(),
        }],
        optional_relationships: vec![0],
        optional_matches: vec![OptionalMatchScope {
            node_indices: vec![1],
            relationship_indices: vec![0],
            predicate: Some(PredicateExpression::Comparison(PropertyPredicate {
                property: PropertyRef {
                    variable: "person".to_string(),
                    property: "team".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("platform".to_string())),
            })),
        }],
        distinct: false,
        projections: vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("service".to_string()),
            },
            Projection::Key {
                variable: "owns".to_string(),
                alias: "ownership_id".to_string(),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "person".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("owner".to_string()),
            },
        ],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("optional predicate should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"n0\".\"service_name\" AS \"service\", \"r0\".\"ownership_id\" AS \"ownership_id\", \"n1\".\"full_name\" AS \"owner\" \
             FROM \"ops\".\"services\" AS \"n0\" \
             LEFT JOIN (\"ops\".\"ownerships\" AS \"r0\" JOIN \"ops\".\"people\" AS \"n1\" ON \"r0\".\"person_id\" = \"n1\".\"id\") \
             ON (\"r0\".\"service_id\" = \"n0\".\"id\") AND (\"n1\".\"team\" = 'platform')"
    );
}

#[test]
fn lower_graph_plan_renders_undirected_optional_predicates_inside_join_scope() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![
            NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "dependency".to_string(),
                label: "Service".to_string(),
            },
        ],
        relationships: vec![RelationshipPattern {
            variable: Some("dependency_edge".to_string()),
            relationship_type: "DEPENDS_ON".to_string(),
            left: "service".to_string(),
            direction: Direction::Undirected,
            right: "dependency".to_string(),
        }],
        optional_relationships: vec![0],
        optional_matches: vec![OptionalMatchScope {
            node_indices: vec![1],
            relationship_indices: vec![0],
            predicate: Some(PredicateExpression::Comparison(PropertyPredicate {
                property: PropertyRef {
                    variable: "dependency".to_string(),
                    property: "tier".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("dev".to_string())),
            })),
        }],
        distinct: false,
        projections: vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("service".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "dependency".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("dependency".to_string()),
            },
        ],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("undirected optional predicate should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"n0\".\"service_name\" AS \"service\", \"n1\".\"service_name\" AS \"dependency\" \
             FROM \"ops\".\"services\" AS \"n0\" \
             LEFT JOIN (\"ops\".\"service_dependencies\" AS \"r0\" JOIN \"ops\".\"services\" AS \"n1\" ON (\"r0\".\"to_service_id\" = \"n1\".\"id\" OR \"r0\".\"from_service_id\" = \"n1\".\"id\")) \
             ON (((\"r0\".\"from_service_id\" = \"n0\".\"id\" AND \"r0\".\"to_service_id\" = \"n1\".\"id\") OR (\"r0\".\"to_service_id\" = \"n0\".\"id\" AND \"r0\".\"from_service_id\" = \"n1\".\"id\"))) AND (\"n1\".\"tier\" = 'dev')"
    );
}

#[test]
fn lower_graph_plan_renders_undirected_distinct_label_relationship_sql() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![
            NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "person".to_string(),
                label: "Person".to_string(),
            },
        ],
        relationships: vec![RelationshipPattern {
            variable: None,
            relationship_type: "OWNS".to_string(),
            left: "service".to_string(),
            direction: Direction::Undirected,
            right: "person".to_string(),
        }],
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![Projection::Property {
            property: PropertyRef {
                variable: "person".to_string(),
                property: "name".to_string(),
            },
            alias: Some("owner".to_string()),
        }],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("undirected relationship should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"n1\".\"full_name\" AS \"owner\" FROM \"ops\".\"services\" AS \"n0\" \
             JOIN \"ops\".\"ownerships\" AS \"r0\" ON \"r0\".\"service_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"people\" AS \"n1\" ON \"r0\".\"person_id\" = \"n1\".\"id\""
    );
}

#[test]
fn lower_graph_plan_renders_undirected_same_label_relationship_sql() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![
            NodePattern {
                variable: "source".to_string(),
                label: "Service".to_string(),
            },
            NodePattern {
                variable: "neighbor".to_string(),
                label: "Service".to_string(),
            },
        ],
        relationships: vec![RelationshipPattern {
            variable: None,
            relationship_type: "DEPENDS_ON".to_string(),
            left: "source".to_string(),
            direction: Direction::Undirected,
            right: "neighbor".to_string(),
        }],
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![Projection::Property {
            property: PropertyRef {
                variable: "neighbor".to_string(),
                property: "name".to_string(),
            },
            alias: Some("neighbor".to_string()),
        }],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("undirected same-label relationship should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"n1\".\"service_name\" AS \"neighbor\" FROM \"ops\".\"services\" AS \"n0\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r0\" ON (\"r0\".\"from_service_id\" = \"n0\".\"id\" OR \"r0\".\"to_service_id\" = \"n0\".\"id\") \
             JOIN \"ops\".\"services\" AS \"n1\" ON ((\"r0\".\"from_service_id\" = \"n0\".\"id\" AND \"r0\".\"to_service_id\" = \"n1\".\"id\") OR (\"r0\".\"to_service_id\" = \"n0\".\"id\" AND \"r0\".\"from_service_id\" = \"n1\".\"id\"))"
    );
}

#[test]
fn lower_graph_plan_renders_relationship_between_joined_nodes() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![
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
        ],
        relationships: vec![
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
        ],
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "source".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("source".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "middle".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("middle".to_string()),
            },
            Projection::Property {
                property: PropertyRef {
                    variable: "target".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("target".to_string()),
            },
        ],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("closed service dependency path should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"n0\".\"service_name\" AS \"source\", \"n1\".\"service_name\" AS \"middle\", \"n2\".\"service_name\" AS \"target\" \
             FROM \"ops\".\"services\" AS \"n0\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r0\" ON \"r0\".\"from_service_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n1\" ON \"r0\".\"to_service_id\" = \"n1\".\"id\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r1\" ON \"r1\".\"from_service_id\" = \"n1\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n2\" ON \"r1\".\"to_service_id\" = \"n2\".\"id\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r2\" ON \"r2\".\"from_service_id\" = \"n0\".\"id\" AND \"r2\".\"to_service_id\" = \"n2\".\"id\""
    );
}

#[test]
fn lower_graph_plan_reorders_connected_relationships() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![
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
        ],
        relationships: vec![
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
                right: "middle".to_string(),
            },
        ],
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![
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
        ],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("connected out-of-order relationship plan should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"n0\".\"service_name\" AS \"source\", \"n2\".\"service_name\" AS \"target\" \
             FROM \"ops\".\"services\" AS \"n0\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r1\" ON \"r1\".\"from_service_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n1\" ON \"r1\".\"to_service_id\" = \"n1\".\"id\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r0\" ON \"r0\".\"from_service_id\" = \"n1\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n2\" ON \"r0\".\"to_service_id\" = \"n2\".\"id\""
    );
}

#[test]
fn lower_graph_plan_rejects_endpoint_mismatch() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    let service_node = plan
        .nodes
        .get_mut(1)
        .expect("ownership fixture should include a service node");
    service_node.label = "Person".to_string();

    let error = graph
        .lower_graph_plan(&plan)
        .expect_err("endpoint mismatch should fail");

    assert!(
        error.to_string().contains("RELATIONSHIP_ENDPOINT_MISMATCH"),
        "{error:?}"
    );
}
