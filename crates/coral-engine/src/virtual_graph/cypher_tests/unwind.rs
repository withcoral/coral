use super::*;

#[test]
fn compiles_literal_unwind_row_source() {
    let query = compile_cypher_query("UNWIND [1, 2, 3] AS x RETURN x")
        .expect("literal UNWIND row source should compile");

    let GraphQuery::Unwind(unwind) = query else {
        panic!("expected literal UNWIND row source query");
    };
    assert_eq!(unwind.variable, "x");
    assert_eq!(
        unwind.list,
        ScalarExpression::TypedLiteralList {
            literals: vec![
                Literal::Integer(1),
                Literal::Integer(2),
                Literal::Integer(3),
            ],
            element_type: LiteralListElementType::Integer,
        }
    );
    assert_eq!(
        unwind.projections,
        vec![GraphUnwindProjection::Variable {
            alias: "x".to_string(),
        }]
    );
}

#[test]
fn compiles_string_literal_unwind_row_source() {
    let query = compile_cypher_query("UNWIND ['a', 'b'] AS n RETURN n")
        .expect("string literal UNWIND row source should compile");

    let GraphQuery::Unwind(unwind) = query else {
        panic!("expected literal UNWIND row source query");
    };
    assert_eq!(
        unwind.list,
        ScalarExpression::TypedLiteralList {
            literals: vec![
                Literal::String("a".to_string()),
                Literal::String("b".to_string()),
            ],
            element_type: LiteralListElementType::String,
        }
    );
    assert_eq!(
        unwind.projections,
        vec![GraphUnwindProjection::Variable {
            alias: "n".to_string(),
        }]
    );
}

#[test]
fn compiles_empty_literal_unwind_row_source() {
    let query = compile_cypher_query("UNWIND [] AS x RETURN x")
        .expect("empty literal UNWIND row source should compile");

    let GraphQuery::Unwind(unwind) = query else {
        panic!("expected literal UNWIND row source query");
    };
    assert_eq!(
        unwind.list,
        ScalarExpression::TypedLiteralList {
            literals: Vec::new(),
            element_type: LiteralListElementType::Integer,
        }
    );
}

#[test]
fn compiles_nested_literal_unwind_row_source() {
    let query = compile_cypher_query("UNWIND [[1, 2], [3, 4]] AS pair RETURN pair")
        .expect("nested literal UNWIND row source should compile");

    let GraphQuery::Unwind(unwind) = query else {
        panic!("expected nested literal UNWIND row source query");
    };
    assert_eq!(unwind.variable, "pair");
    assert_eq!(unwind.element_type, LiteralListElementType::IntegerList);
    assert_eq!(
        unwind.list,
        ScalarExpression::TypedLiteralList {
            literals: vec![
                Literal::List(vec![Literal::Integer(1), Literal::Integer(2)]),
                Literal::List(vec![Literal::Integer(3), Literal::Integer(4)]),
            ],
            element_type: LiteralListElementType::IntegerList,
        }
    );
    assert_eq!(
        unwind.projections,
        vec![GraphUnwindProjection::Variable {
            alias: "pair".to_string(),
        }]
    );
}

#[test]
fn compiles_literal_unwind_aggregate_return_as_pipeline() {
    let query = compile_cypher_query("UNWIND [1, 2, 3] AS x RETURN count(x) AS c")
        .expect("literal UNWIND aggregate row source should compile");

    let GraphQuery::UnwindPipeline(pipeline) = query else {
        panic!("expected literal UNWIND aggregate to compile as a row-source pipeline");
    };
    assert_eq!(pipeline.unwind.variable, "x");
    assert_eq!(
        pipeline.unwind.projections,
        vec![GraphUnwindProjection::Variable {
            alias: "x".to_string(),
        }]
    );
    assert!(pipeline.final_plan.nodes.is_empty());
    assert_eq!(
        pipeline.final_plan.projections,
        vec![Projection::Aggregate {
            function: AggregateFunction::Count,
            target: AggregateTarget::Expression(ScalarExpression::StageValue {
                alias: "x".to_string(),
            }),
            distinct: false,
            alias: "c".to_string(),
        }]
    );
}

#[test]
fn compiles_literal_unwind_ordered_return_as_pipeline() {
    let query = compile_cypher_query("UNWIND [3, 1, 2] AS x RETURN x ORDER BY x")
        .expect("literal UNWIND ordered row source should compile");

    let GraphQuery::UnwindPipeline(pipeline) = query else {
        panic!("expected ordered literal UNWIND to compile as a row-source pipeline");
    };
    assert!(pipeline.final_plan.nodes.is_empty());
    assert_eq!(
        pipeline.final_plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::StageValue {
                alias: "x".to_string(),
            },
            alias: "x".to_string(),
        }]
    );
    assert_eq!(
        pipeline.final_plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("x".to_string()),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_nested_literal_unwind_list_index_return_as_pipeline() {
    let query = compile_cypher_query(
        "UNWIND [[1, 2], [3, 4]] AS pair RETURN pair[0] AS first ORDER BY first",
    )
    .expect("nested literal UNWIND list index return should compile");

    let GraphQuery::UnwindPipeline(pipeline) = query else {
        panic!("expected nested list-index UNWIND to compile as a row-source pipeline");
    };
    assert_eq!(pipeline.unwind.variable, "pair");
    assert_eq!(
        pipeline.unwind.element_type,
        LiteralListElementType::IntegerList
    );
    assert_eq!(
        pipeline.final_plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::ListIndex {
                list: Box::new(ScalarExpression::StageValue {
                    alias: "pair".to_string(),
                }),
                index: 0,
                element_type: LiteralListElementType::Integer,
            },
            alias: "first".to_string(),
        }]
    );
    assert_eq!(
        pipeline.final_plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("first".to_string()),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_with_alias_unwind_row_source() {
    let query = compile_cypher_query("WITH [1, 2, 3] AS list UNWIND list AS x RETURN x")
        .expect("WITH alias UNWIND row source should compile");

    let GraphQuery::Unwind(unwind) = query else {
        panic!("expected dynamic UNWIND row source query");
    };
    assert_eq!(unwind.variable, "x");
    assert_eq!(unwind.element_type, LiteralListElementType::Integer);
    assert_eq!(
        unwind
            .input
            .as_ref()
            .expect("UNWIND input should exist")
            .projections,
        vec![GraphUnwindInputProjection {
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
        }]
    );
    assert_eq!(
        unwind.list,
        ScalarExpression::StageValue {
            alias: "list".to_string(),
        }
    );
}

#[test]
fn compiles_with_alias_list_concat_unwind_row_source() {
    let query = compile_cypher_query(
        "WITH [1, 2, 3] AS first, [4, 5, 6] AS second \
         UNWIND (first + second) AS x \
         RETURN x",
    )
    .expect("WITH alias list concatenation UNWIND row source should compile");

    let GraphQuery::Unwind(unwind) = query else {
        panic!("expected dynamic UNWIND row source query");
    };
    assert_eq!(unwind.element_type, LiteralListElementType::Integer);
    assert_eq!(
        unwind.list,
        ScalarExpression::ListConcat {
            left: Box::new(ScalarExpression::StageValue {
                alias: "first".to_string(),
            }),
            right: Box::new(ScalarExpression::StageValue {
                alias: "second".to_string(),
            }),
        }
    );
}

#[test]
fn compiles_with_alias_unwind_arithmetic_return_as_pipeline() {
    let query = compile_cypher_query("WITH [1, 2, 3] AS list UNWIND list AS x RETURN x * 2 AS d")
        .expect("WITH alias UNWIND arithmetic row source should compile");

    let GraphQuery::UnwindPipeline(pipeline) = query else {
        panic!("expected arithmetic UNWIND return to compile as a row-source pipeline");
    };
    assert!(pipeline.final_plan.nodes.is_empty());
    assert_eq!(
        pipeline.final_plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::Arithmetic {
                operator: ArithmeticOperator::Multiply,
                left: Box::new(ScalarExpression::StageValue {
                    alias: "x".to_string(),
                }),
                right: Box::new(ScalarExpression::Literal(Literal::Integer(2))),
            },
            alias: "d".to_string(),
        }]
    );
}

#[test]
fn compiles_literal_unwind_terminal_with_return_as_pipeline() {
    let query = compile_cypher_query("UNWIND [1, 2, 3] AS x WITH x * 2 AS d RETURN d ORDER BY d")
        .expect("literal UNWIND terminal WITH row source should compile");

    let GraphQuery::UnwindPipeline(pipeline) = query else {
        panic!("expected terminal WITH over UNWIND to compile as a row-source pipeline");
    };
    assert!(pipeline.final_plan.nodes.is_empty());
    assert_eq!(
        pipeline.final_plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::Arithmetic {
                operator: ArithmeticOperator::Multiply,
                left: Box::new(ScalarExpression::StageValue {
                    alias: "x".to_string(),
                }),
                right: Box::new(ScalarExpression::Literal(Literal::Integer(2))),
            },
            alias: "d".to_string(),
        }]
    );
    assert_eq!(
        pipeline.final_plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("d".to_string()),
            direction: OrderDirection::Ascending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_collect_scalar_unwind_row_source_as_staged_unwind() {
    let query = compile_cypher_query(
        "MATCH (person:Person) \
         WITH collect(1) AS numbers \
         UNWIND numbers AS n \
         RETURN n ORDER BY n",
    )
    .expect("collect-sourced scalar UNWIND should compile");

    let GraphQuery::StagedUnwind(staged) = query else {
        panic!("expected collect-sourced UNWIND to compile as a staged row-source query");
    };
    assert_eq!(staged.unwind.source_alias, "numbers");
    assert_eq!(staged.unwind.variable, "n");
    assert_eq!(
        staged.unwind.binding,
        GraphStagedUnwindBinding::Scalar {
            element_type: LiteralListElementType::Integer,
        }
    );
    assert_eq!(
        staged.stage.exports,
        vec![GraphStageExport::AggregateValue {
            alias: "numbers".to_string(),
            column: "numbers".to_string(),
        }]
    );
    assert_eq!(
        staged.final_plan.projections,
        vec![Projection::Expression {
            expression: ScalarExpression::StageValue {
                alias: "n".to_string(),
            },
            alias: "n".to_string(),
        }]
    );
}

#[test]
fn compiles_collect_node_unwind_match_as_staged_unwind() {
    let query = compile_cypher_query(
        "MATCH (a:Person)-[:KNOWS]->(b1:Person) \
         WITH a, collect(b1) AS bees \
         UNWIND bees AS b2 \
         MATCH (a)-[:LIKES]->(b2) \
         RETURN a.name AS a, b2.name AS b",
    )
    .expect("collect(node)-sourced UNWIND feeding MATCH should compile");

    let GraphQuery::StagedUnwind(staged) = query else {
        panic!("expected collect(node)-sourced UNWIND to compile as a staged row-source query");
    };
    assert_eq!(staged.unwind.source_alias, "bees");
    assert_eq!(staged.unwind.variable, "b2");
    assert_eq!(
        staged.unwind.binding,
        GraphStagedUnwindBinding::NodeKey {
            label: "Person".to_string(),
        }
    );
    assert_eq!(
        staged.stage.exports,
        vec![
            GraphStageExport::NodeKey {
                variable: "a".to_string(),
                column: "a_id".to_string(),
            },
            GraphStageExport::AggregateValue {
                alias: "bees".to_string(),
                column: "bees".to_string(),
            },
        ]
    );
    assert!(
        staged
            .final_plan
            .relationships
            .iter()
            .any(|relationship| relationship.left == "a" && relationship.right == "b2")
    );
}

#[test]
fn rejects_collect_unwind_nested_list_elements_for_later_widening() {
    let error = compile_cypher_query(
        "MATCH (person:Person) \
         WITH collect([1]) AS lists \
         UNWIND lists AS x \
         RETURN x",
    )
    .expect_err("nested collect-sourced UNWIND should remain out of scope");

    assert!(
        error
            .to_string()
            .contains("list-valued collect elements are not supported yet"),
        "unexpected error: {error}"
    );
}

#[test]
fn compiles_property_key_unwind_row_sources() {
    let graph = star_test_graph();
    let node_query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (person:Person) \
         UNWIND keys(person) AS key \
         RETURN DISTINCT key AS property_key \
         ORDER BY property_key",
    )
    .expect("node keys() UNWIND should compile");
    assert_eq!(
        static_unwind_literal_outputs(&node_query, "property_key"),
        vec!["name".to_string(), "team".to_string()]
    );

    let relationship_query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
         UNWIND keys(owns) AS key \
         RETURN DISTINCT key AS property_key \
         ORDER BY property_key",
    )
    .expect("relationship keys() UNWIND should compile");
    assert_eq!(
        static_unwind_literal_outputs(&relationship_query, "property_key"),
        vec!["since".to_string(), "source".to_string()]
    );

    let unlabeled_query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (entity) \
         UNWIND keys(entity) AS key \
         RETURN DISTINCT key AS property_key \
         ORDER BY property_key",
    )
    .expect("unlabeled node keys() UNWIND should compile through label branches");
    assert_eq!(
        static_unwind_literal_outputs(&unlabeled_query, "property_key"),
        vec![
            "name".to_string(),
            "team".to_string(),
            "name".to_string(),
            "tier".to_string(),
            "name".to_string(),
        ]
    );
}

#[test]
fn compiles_optional_property_key_unwind_with_presence_filters() {
    let graph = star_test_graph();
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
         UNWIND keys(person) AS key \
         RETURN DISTINCT key AS property_key \
         ORDER BY property_key",
    )
    .expect("optional node keys() UNWIND should compile with presence filters");

    let plans = graph_query_plans(&query);
    assert_eq!(
        static_unwind_literal_outputs(&query, "property_key"),
        vec!["name".to_string(), "team".to_string()]
    );
    assert!(plans.iter().all(|plan| {
        predicate_contains_presence(
            plan.predicate.as_ref(),
            "person",
            ComparisonOperator::NotEqual,
        )
    }));
}

#[test]
fn compiles_wide_property_key_unwind_as_dynamic_pipeline() {
    let graph = wide_property_test_graph(MAX_STATIC_UNWIND_BRANCHES + 1);
    let query = compile_cypher_query_for_graph(
        &graph,
        "MATCH (n:Wide) \
         UNWIND keys(n) AS key \
         RETURN DISTINCT key AS property_key \
         ORDER BY property_key",
    )
    .expect("wide node keys() UNWIND should compile through dynamic UNNEST");

    let GraphQuery::UnwindPipeline(pipeline) = query else {
        panic!("expected wide keys() UNWIND to fall through to a dynamic pipeline");
    };
    assert!(pipeline.unwind.input.is_none());
    assert_eq!(pipeline.unwind.variable, "key");
    assert_eq!(pipeline.unwind.element_type, LiteralListElementType::String);
    let ScalarExpression::TypedLiteralList {
        literals,
        element_type,
    } = &pipeline.unwind.list
    else {
        panic!("expected wide keys() UNWIND list to be a typed literal list");
    };
    assert_eq!(*element_type, LiteralListElementType::String);
    assert_eq!(literals.len(), MAX_STATIC_UNWIND_BRANCHES + 1);
    assert_eq!(literals.first(), Some(&Literal::String("p00".to_string())));
    assert_eq!(literals.last(), Some(&Literal::String("p64".to_string())));
    assert_eq!(
        projection_names(&pipeline.final_plan),
        vec!["property_key".to_string()]
    );
}

#[test]
fn compiles_wide_optional_property_key_unwind_with_dynamic_presence_filter() {
    let graph = wide_property_test_graph(MAX_STATIC_UNWIND_BRANCHES + 1);
    let query = compile_cypher_query_for_graph(
        &graph,
        "OPTIONAL MATCH (n:Wide) \
         UNWIND keys(n) AS key \
         RETURN DISTINCT key AS property_key \
         ORDER BY property_key",
    )
    .expect("wide optional keys() UNWIND should compile through dynamic UNNEST");

    let GraphQuery::UnwindPipeline(pipeline) = query else {
        panic!("expected wide optional keys() UNWIND to fall through to a dynamic pipeline");
    };
    assert!(predicate_contains_presence(
        pipeline.final_plan.predicate.as_ref(),
        "n",
        ComparisonOperator::NotEqual,
    ));
}

#[test]
fn compiles_with_alias_unwind_match_as_pipeline() {
    let query = compile_cypher_query(
        "WITH [1, 2, 3] AS ordinals \
         UNWIND ordinals AS ordinal \
         MATCH (service:Service) \
         WHERE service.id = ordinal * 10 \
         RETURN ordinal AS ordinal, service.name AS service \
         ORDER BY ordinal",
    )
    .expect("WITH alias UNWIND feeding MATCH should compile");

    let GraphQuery::UnwindPipeline(pipeline) = query else {
        panic!("expected dynamic UNWIND pipeline query");
    };
    assert_eq!(pipeline.unwind.variable, "ordinal");
    assert_eq!(
        pipeline.unwind.list,
        ScalarExpression::StageValue {
            alias: "ordinals".to_string(),
        }
    );
    assert_eq!(
        projection_names(&pipeline.final_plan),
        vec!["ordinal".to_string(), "service".to_string()]
    );
}

#[test]
fn rejects_heterogeneous_nested_literal_unwind_row_source() {
    let error = compile_cypher_query("UNWIND [['a', 1], [1, null]] AS x RETURN x")
        .expect_err("heterogeneous nested literal UNWIND should be rejected");
    assert!(
        error
            .to_string()
            .contains("nested lists require each non-empty nested list"),
        "unexpected error: {error}"
    );
}

#[test]
fn leaves_static_unwind_with_downstream_match_on_existing_expansion_path() {
    let query = compile_cypher_query(
        "UNWIND ['prod', 'dev'] AS tier \
             MATCH (service:Service) \
             WHERE service.tier = tier \
             RETURN tier AS tier, service.name AS service \
             ORDER BY tier, service",
    )
    .expect("static UNWIND with downstream MATCH should still compile");

    assert!(matches!(query, GraphQuery::Union(_)));
}

#[test]
fn compiles_static_unwind_as_union_all_branches() {
    let query = compile_cypher_query(
        "UNWIND ['prod', 'dev'] AS tier \
             MATCH (service:Service) \
             WHERE service.tier = tier \
             RETURN tier AS tier, service.name AS service \
             ORDER BY tier, service",
    )
    .expect("static UNWIND query should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected static UNWIND to expand into a union query");
    };
    assert_eq!(union.branches.len(), 1);
    assert!(union.branches.first().expect("branch").all);
    assert_eq!(
        union.first.projections.first(),
        Some(&Projection::Literal {
            literal: Literal::String("prod".to_string()),
            alias: "tier".to_string(),
        })
    );
    assert_eq!(
        union
            .branches
            .first()
            .expect("static UNWIND branch should exist")
            .plan
            .projections
            .first(),
        Some(&Projection::Literal {
            literal: Literal::String("dev".to_string()),
            alias: "tier".to_string(),
        })
    );
    assert_eq!(
        union.first.predicates,
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
fn missing_branch_temporal_predicate_rewrite_fails_loudly() {
    let graph = star_test_graph();
    let mut nodes = BTreeMap::new();
    nodes.insert("person".to_string(), "Person".to_string());

    let error = missing_branch_property_predicate_expression(
        PropertyPredicate {
            property: PropertyRef {
                variable: "person".to_string(),
                property: "joined".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::TemporalCoercion {
                source: "2024-01-01".to_string(),
            },
        },
        &graph,
        &nodes,
        &[],
    )
    .expect_err("temporal coercion must not be silently downgraded");

    assert!(
        error
            .to_string()
            .contains("static branch rewrite cannot preserve temporal predicate coercion"),
        "unexpected error: {error}"
    );
}

#[test]
fn compiles_static_unwind_after_transparent_with_as_union_all_branches() {
    let query = compile_cypher_query(
        "MATCH (service:Service) \
             WITH service \
             UNWIND [1, 2] AS n \
             RETURN service.name AS service, n \
             ORDER BY service, n",
    )
    .expect("WITH-separated static UNWIND query should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected static UNWIND after WITH to expand into a union query");
    };
    assert_eq!(union.branches.len(), 1);
    assert!(union.branches.first().expect("branch").all);
    assert_eq!(
        union.first.projections.get(1),
        Some(&Projection::Literal {
            literal: Literal::Integer(1),
            alias: "n".to_string(),
        })
    );
    assert_eq!(
        union
            .branches
            .first()
            .expect("static UNWIND branch should exist")
            .plan
            .projections
            .get(1),
        Some(&Projection::Literal {
            literal: Literal::Integer(2),
            alias: "n".to_string(),
        })
    );
    assert_eq!(
        union.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::ProjectionAlias("service".to_string()),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::ProjectionAlias("n".to_string()),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_static_unwind_over_list_case_expressions() {
    let query = compile_cypher_query(
        "UNWIND (CASE WHEN true THEN ['prod', 'dev', 'stage'] ELSE ['legacy'] END)[0..2] AS tier \
             MATCH (service:Service) \
             WHERE service.tier = tier \
             RETURN tier AS tier, service.name AS service \
             ORDER BY tier, service",
    )
    .expect("static UNWIND over sliced list CASE should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected static CASE UNWIND to expand into a union query");
    };
    assert_eq!(union.branches.len(), 1);
    assert_eq!(
        union.first.projections.first(),
        Some(&Projection::Literal {
            literal: Literal::String("prod".to_string()),
            alias: "tier".to_string(),
        })
    );
    assert_eq!(
        union
            .branches
            .first()
            .expect("static UNWIND branch should exist")
            .plan
            .projections
            .first(),
        Some(&Projection::Literal {
            literal: Literal::String("dev".to_string()),
            alias: "tier".to_string(),
        })
    );

    let generic = compile_cypher_query(
        "UNWIND CASE 'prod' WHEN 'dev' THEN ['dev'] ELSE coalesce(null, ['prod']) END AS tier \
             MATCH (service:Service) \
             WHERE service.tier = tier \
             RETURN tier AS tier, service.name AS service",
    )
    .expect("static UNWIND over generic list CASE should compile");

    let GraphQuery::Plan(plan) = generic else {
        panic!("single selected static CASE branch should compile as a plan");
    };
    assert_eq!(
        plan.projections.first(),
        Some(&Projection::Literal {
            literal: Literal::String("prod".to_string()),
            alias: "tier".to_string(),
        })
    );
}

#[test]
fn rejects_static_unwind_over_dynamic_list_case_predicates() {
    let error = compile_cypher_query(
        "MATCH (service:Service) \
             UNWIND CASE WHEN service.tier = 'prod' THEN ['prod'] ELSE ['other'] END AS tier \
             RETURN tier",
    )
    .expect_err("dynamic CASE predicate in static UNWIND should be rejected");

    assert!(
        error.to_string().contains(
            "UNWIND over list-valued CASE expressions requires statically foldable WHEN predicates"
        ),
        "{error}"
    );
}

#[test]
fn compiles_duplicate_static_unwind_aggregates_as_outer_union_aggregates() {
    let query = compile_cypher_query(
        "UNWIND ['prod', 'prod', 'dev'] AS tier \
             MATCH (service:Service) \
             WHERE service.tier = tier \
             RETURN tier AS tier, count(*) AS services \
             ORDER BY tier",
    )
    .expect("static UNWIND aggregate query should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected duplicate static UNWIND to expand into a union query");
    };
    assert_eq!(union.branches.len(), 2);
    assert_eq!(
        union.outer_projection,
        Some(GraphUnionOuterProjection {
            items: vec![
                GraphUnionOuterProjectionItem::Column {
                    name: "tier".to_string(),
                },
                GraphUnionOuterProjectionItem::CountAll {
                    alias: "services".to_string(),
                },
            ],
            group_by: vec!["tier".to_string()],
        })
    );
    assert_eq!(projection_names(&union.first), vec!["tier".to_string()]);
}

#[test]
fn compiles_empty_static_unwind_as_forced_empty_plan() {
    let query = compile_cypher_query(
        "UNWIND [] AS tier \
             MATCH (service:Service) \
             RETURN tier AS tier, count(*) AS services",
    )
    .expect("empty static UNWIND query should compile");

    let GraphQuery::Plan(plan) = query else {
        panic!("single empty static UNWIND branch should compile as a plan");
    };
    assert!(matches!(
        plan.predicate,
        Some(PredicateExpression::Boolean(false))
    ));
    assert_eq!(
        plan.projections.first(),
        Some(&Projection::Literal {
            literal: Literal::Null,
            alias: "tier".to_string(),
        })
    );
}

#[test]
fn rejects_dynamic_static_unwind_sources() {
    assert_unsupported(
        "MATCH (service:Service) \
             UNWIND service.tier AS tier \
             RETURN tier",
    );
}

#[test]
fn rejects_dynamic_static_unwind_sources_after_with() {
    assert_unsupported(
        "MATCH (service:Service) \
             WITH service \
             UNWIND [service.tier] AS tier \
             RETURN tier",
    );
}

#[test]
fn compiles_static_unwind_hidden_order_expressions() {
    let query = compile_cypher_query(
        "UNWIND ['prod', 'dev'] AS tier \
             MATCH (service:Service) \
             WHERE service.tier = tier \
             RETURN service.name AS service \
             ORDER BY CASE WHEN tier = 'prod' THEN 0 ELSE 1 END, service",
    )
    .expect("static UNWIND hidden ORDER BY expression should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected static UNWIND to expand into a union query");
    };
    assert_eq!(
        union.first.projection_output_names(),
        vec!["service".to_string(), "__coral_order_0".to_string()]
    );
    assert_eq!(
        union.outer_projection,
        Some(GraphUnionOuterProjection {
            items: vec![GraphUnionOuterProjectionItem::Column {
                name: "service".to_string(),
            }],
            group_by: Vec::new(),
        })
    );
    assert_eq!(
        union.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::ProjectionAlias("__coral_order_0".to_string()),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::ProjectionAlias("service".to_string()),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_static_unwind_outer_order_null_placement() {
    let query = compile_cypher_query(
        "UNWIND ['prod', 'dev'] AS tier \
             MATCH (service:Service) \
             WHERE service.tier = tier \
             RETURN service.name AS service \
             ORDER BY CASE WHEN tier = 'prod' THEN service.name ELSE NULL END NULLS LAST, \
                      service DESC NULLS FIRST",
    )
    .expect("static UNWIND ORDER BY NULLS FIRST/LAST should compile");

    let GraphQuery::Union(union) = query else {
        panic!("expected static UNWIND to expand into a union query");
    };
    assert_eq!(
        union.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::ProjectionAlias("__coral_order_0".to_string()),
                direction: OrderDirection::Ascending,
                nulls: Some(NullOrder::Last),
            },
            OrderKey {
                expression: OrderExpression::ProjectionAlias("service".to_string()),
                direction: OrderDirection::Descending,
                nulls: Some(NullOrder::First),
            },
        ]
    );
}
