use super::*;

#[test]
fn lower_graph_plan_renders_path_value_projection_as_struct() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.relationships
        .get_mut(0)
        .expect("ownership plan should include one relationship")
        .variable = Some("owns".to_string());
    plan.projections = vec![Projection::Expression {
        expression: ScalarExpression::PathValue {
            node_variables: vec!["person".to_string(), "service".to_string()],
            relationship_variables: vec!["owns".to_string()],
        },
        alias: "path".to_string(),
    }];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("path value projection should lower");

    assert!(
        translation.sql().contains(
            "named_struct('node_ids', make_array(\"n0\".\"id\", \"n1\".\"id\"), 'relationship_ids', make_array(\"r0\".\"ownership_id\")) AS \"path\""
        ),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_property_keys_ordering() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::PropertyKeys {
            variable: "service".to_string(),
        },
        direction: OrderDirection::Descending,
        nulls: None,
    }];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("property key ordering should lower");

    assert!(
        translation.sql().contains(
            "ORDER BY CASE WHEN \"n1\".\"id\" IS NULL THEN NULL ELSE \
                 make_array('name', 'risk', 'tier') END DESC"
        ),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_quotes_identifiers_and_literals() {
    let graph = Declaration::from_yaml(
        r#"
version: 1
name: quoting
nodes:
  - label: Weird
    table: { schema: weird-schema, name: table"name }
    key: id"key
    properties:
      display: display"name
relationships: []
"#,
    )
    .expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![NodePattern {
            variable: "weird".to_string(),
            label: "Weird".to_string(),
        }],
        relationships: Vec::new(),
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![Projection::Property {
            property: PropertyRef {
                variable: "weird".to_string(),
                property: "display".to_string(),
            },
            alias: Some("value".to_string()),
        }],
        predicates: vec![PropertyPredicate {
            property: PropertyRef {
                variable: "weird".to_string(),
                property: "display".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Literal(Literal::String("Ada's laptop".to_string())),
        }],
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("quoted plan should lower");

    assert_eq!(
        translation.sql(),
        "SELECT \"n0\".\"display\"\"name\" AS \"value\" \
             FROM \"weird-schema\".\"table\"\"name\" AS \"n0\" \
             WHERE \"n0\".\"display\"\"name\" = 'Ada''s laptop'"
    );
}

#[test]
fn lower_graph_plan_renders_distinct_projection() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.distinct = true;
    plan.projections = vec![Projection::Property {
        property: PropertyRef {
            variable: "service".to_string(),
            property: "tier".to_string(),
        },
        alias: Some("tier".to_string()),
    }];
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::Property(PropertyRef {
            variable: "service".to_string(),
            property: "tier".to_string(),
        }),
        direction: OrderDirection::Ascending,
        nulls: None,
    }];
    plan.limit = None;

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("distinct plan should lower");

    assert!(
        translation
            .sql()
            .starts_with("SELECT DISTINCT \"n1\".\"tier\" AS \"tier\""),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_offset() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.skip = Some(5);

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("offset plan should lower");

    assert!(
        translation.sql().ends_with(" LIMIT 25 OFFSET 5"),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_grouped_count_projection() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.projections.push(Projection::CountAll {
        alias: "ownership_count".to_string(),
    });

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("grouped aggregate projection should lower");

    assert!(
            translation.sql().contains(
                " GROUP BY \"n0\".\"full_name\", \"n1\".\"service_name\" ORDER BY \"n0\".\"full_name\" ASC"
            ),
            "{}",
            translation.sql()
        );
}

#[test]
fn lower_graph_plan_orders_by_count_alias() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.projections.push(Projection::CountAll {
        alias: "ownership_count".to_string(),
    });
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::ProjectionAlias("ownership_count".to_string()),
        direction: OrderDirection::Descending,
        nulls: None,
    }];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("aggregate alias ordering should lower");

    assert!(
        translation
            .sql()
            .contains(" ORDER BY \"ownership_count\" DESC"),
        "{}",
        translation.sql()
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "The test keeps the correlated node-count plan inline so the SQL shape under test is explicit"
)]
fn lower_graph_plan_precomputes_hidden_correlated_node_count_ordering() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = GraphPlan {
        nodes: vec![NodePattern {
            variable: "service".to_string(),
            label: "Service".to_string(),
        }],
        relationships: Vec::new(),
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![Projection::Property {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            },
            alias: Some("service".to_string()),
        }],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::CountSubquery {
                pattern: Box::new(CountSubqueryPattern::Nodes {
                    nodes: vec![NodePattern {
                        variable: "other".to_string(),
                        label: "Service".to_string(),
                    }],
                    predicates: vec![
                        PropertyPredicate {
                            property: PropertyRef {
                                variable: "other".to_string(),
                                property: "tier".to_string(),
                            },
                            operator: ComparisonOperator::Equal,
                            rhs: PredicateRhs::Property(PropertyRef {
                                variable: "service".to_string(),
                                property: "tier".to_string(),
                            }),
                        },
                        PropertyPredicate {
                            property: PropertyRef {
                                variable: "other".to_string(),
                                property: "name".to_string(),
                            },
                            operator: ComparisonOperator::NotEqual,
                            rhs: PredicateRhs::Literal(Literal::String("legacy".to_string())),
                        },
                    ],
                    predicate: None,
                }),
                distinct_target: None,
            }),
            direction: OrderDirection::Descending,
            nulls: None,
        }],
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("correlated node count ordering should lower");

    assert!(
        translation.sql().contains(
            "LEFT JOIN (SELECT \"__coral_count_n0\".\"tier\" AS \"__coral_outer_key\", \
                 COUNT(*) AS \"__coral_value\" FROM \"ops\".\"services\" AS \"__coral_count_n0\" \
                 WHERE \"__coral_count_n0\".\"service_name\" <> 'legacy' \
                 GROUP BY \"__coral_count_n0\".\"tier\") AS \"__coral_scalar_subquery_0\" \
                 ON \"__coral_scalar_subquery_0\".\"__coral_outer_key\" = \"n0\".\"tier\""
        ),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("ORDER BY COALESCE(\"__coral_scalar_subquery_0\".\"__coral_value\", 0) DESC"),
        "{}",
        translation.sql()
    );

    let order_expression = &mut plan.order_by.first_mut().expect("order key").expression;
    let CountSubqueryPattern::Nodes { predicates, .. } = (match order_expression {
        OrderExpression::Scalar(ScalarExpression::CountSubquery { pattern, .. }) => {
            pattern.as_mut()
        }
        _ => panic!("expected count subquery order expression"),
    }) else {
        panic!("expected node count subquery");
    };
    predicates.push(PropertyPredicate {
        property: PropertyRef {
            variable: "other".to_string(),
            property: "name".to_string(),
        },
        operator: ComparisonOperator::Equal,
        rhs: PredicateRhs::Property(PropertyRef {
            variable: "service".to_string(),
            property: "name".to_string(),
        }),
    });
    let error = graph
        .lower_graph_plan(&plan)
        .expect_err("multiple correlated node-count keys should remain rejected");
    assert!(
        error
            .to_string()
            .contains("requires a precomputable single-anchor relationship or node pattern"),
        "{error}"
    );
}

#[test]
fn lower_graph_plan_precomputes_hidden_correlated_node_exists_ordering() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = GraphPlan {
        nodes: vec![NodePattern {
            variable: "service".to_string(),
            label: "Service".to_string(),
        }],
        relationships: Vec::new(),
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![Projection::Property {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            },
            alias: Some("service".to_string()),
        }],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Predicate(Box::new(
                PredicateExpression::ExistsPattern(ExistsPatternPredicate {
                    nodes: vec![NodePattern {
                        variable: "other".to_string(),
                        label: "Service".to_string(),
                    }],
                    relationships: Vec::new(),
                    predicates: vec![PropertyPredicate {
                        property: PropertyRef {
                            variable: "other".to_string(),
                            property: "tier".to_string(),
                        },
                        operator: ComparisonOperator::Equal,
                        rhs: PredicateRhs::Property(PropertyRef {
                            variable: "service".to_string(),
                            property: "tier".to_string(),
                        }),
                    }],
                    predicate: None,
                }),
            ))),
            direction: OrderDirection::Descending,
            nulls: None,
        }],
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("correlated node exists ordering should lower");

    assert!(
            translation.sql().contains(
                "LEFT JOIN (SELECT \"__coral_exists_n0\".\"tier\" AS \"__coral_outer_key\", \
                 COUNT(*) > 0 AS \"__coral_value\" FROM \"ops\".\"services\" AS \"__coral_exists_n0\" \
                 GROUP BY \"__coral_exists_n0\".\"tier\") AS \"__coral_scalar_subquery_0\" \
                 ON \"__coral_scalar_subquery_0\".\"__coral_outer_key\" = \"n0\".\"tier\""
            ),
            "{}",
            translation.sql()
        );
    assert!(
        translation.sql().contains(
            "ORDER BY COALESCE(\"__coral_scalar_subquery_0\".\"__coral_value\", FALSE) DESC"
        ),
        "{}",
        translation.sql()
    );

    let order_expression = &mut plan.order_by.first_mut().expect("order key").expression;
    let exists_predicate = match order_expression {
        OrderExpression::Scalar(ScalarExpression::Predicate(predicate)) => {
            match predicate.as_mut() {
                PredicateExpression::ExistsPattern(predicate) => predicate,
                _ => panic!("expected exists predicate order expression"),
            }
        }
        _ => panic!("expected exists predicate order expression"),
    };
    exists_predicate.predicates.push(PropertyPredicate {
        property: PropertyRef {
            variable: "other".to_string(),
            property: "name".to_string(),
        },
        operator: ComparisonOperator::Equal,
        rhs: PredicateRhs::Property(PropertyRef {
            variable: "service".to_string(),
            property: "name".to_string(),
        }),
    });
    let error = graph
        .lower_graph_plan(&plan)
        .expect_err("multiple correlated node-exists keys should remain rejected");
    assert!(
        error
            .to_string()
            .contains("requires a precomputable single-anchor relationship or node pattern"),
        "{error}"
    );
}

#[test]
fn lower_graph_plan_renders_count_property_projection() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.projections.push(Projection::Aggregate {
        function: AggregateFunction::Count,
        target: AggregateTarget::Property(PropertyRef {
            variable: "service".to_string(),
            property: "tier".to_string(),
        }),
        distinct: true,
        alias: "tier_count".to_string(),
    });

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("count property projection should lower");

    assert!(
        translation
            .sql()
            .contains("COUNT(DISTINCT \"n1\".\"tier\") AS \"tier_count\""),
        "{}",
        translation.sql()
    );
    assert!(
        translation.sql().contains(" GROUP BY "),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_collect_property_projection() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.projections.push(Projection::Aggregate {
        function: AggregateFunction::Collect,
        target: AggregateTarget::Property(PropertyRef {
            variable: "service".to_string(),
            property: "name".to_string(),
        }),
        distinct: true,
        alias: "services".to_string(),
    });

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("collect property projection should lower");

    assert!(
            translation
                .sql()
                .contains("COALESCE(ARRAY_AGG(DISTINCT \"n1\".\"service_name\") FILTER (WHERE (\"n1\".\"service_name\") IS NOT NULL), make_array()) AS \"services\""),
            "{}",
            translation.sql()
        );
    assert!(
        translation.sql().contains(" GROUP BY "),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_numeric_aggregate_projections() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.projections.push(Projection::Aggregate {
        function: AggregateFunction::Sum,
        target: AggregateTarget::Property(PropertyRef {
            variable: "service".to_string(),
            property: "risk".to_string(),
        }),
        distinct: false,
        alias: "total_risk".to_string(),
    });
    plan.projections.push(Projection::Aggregate {
        function: AggregateFunction::Avg,
        target: AggregateTarget::Property(PropertyRef {
            variable: "service".to_string(),
            property: "risk".to_string(),
        }),
        distinct: false,
        alias: "average_risk".to_string(),
    });
    plan.projections.push(Projection::Aggregate {
        function: AggregateFunction::Min,
        target: AggregateTarget::Property(PropertyRef {
            variable: "service".to_string(),
            property: "risk".to_string(),
        }),
        distinct: false,
        alias: "lowest_risk".to_string(),
    });
    plan.projections.push(Projection::Aggregate {
        function: AggregateFunction::Max,
        target: AggregateTarget::Property(PropertyRef {
            variable: "service".to_string(),
            property: "risk".to_string(),
        }),
        distinct: true,
        alias: "highest_risk".to_string(),
    });

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("numeric aggregate projections should lower");

    assert!(
        translation.sql().contains(
            "SUM(\"n1\".\"risk_score\") AS \"total_risk\", \
                 AVG(\"n1\".\"risk_score\") AS \"average_risk\", \
                 MIN(\"n1\".\"risk_score\") AS \"lowest_risk\", \
                 MAX(DISTINCT \"n1\".\"risk_score\") AS \"highest_risk\""
        ),
        "{}",
        translation.sql()
    );
    assert!(
        translation.sql().contains(" GROUP BY "),
        "{}",
        translation.sql()
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "The test keeps related statistical aggregate SQL assertions together."
)]
fn lower_graph_plan_renders_statistical_aggregate_projections() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.projections.push(Projection::Aggregate {
        function: AggregateFunction::Median,
        target: AggregateTarget::Property(PropertyRef {
            variable: "service".to_string(),
            property: "risk".to_string(),
        }),
        distinct: false,
        alias: "median_risk".to_string(),
    });
    plan.projections.push(Projection::Aggregate {
        function: AggregateFunction::PercentileCont {
            percentile: ordered_float::OrderedFloat(0.75),
        },
        target: AggregateTarget::Property(PropertyRef {
            variable: "service".to_string(),
            property: "risk".to_string(),
        }),
        distinct: false,
        alias: "p75_risk".to_string(),
    });
    plan.projections.push(Projection::Aggregate {
        function: AggregateFunction::PercentileDisc {
            percentile: ordered_float::OrderedFloat(0.75),
        },
        target: AggregateTarget::Property(PropertyRef {
            variable: "service".to_string(),
            property: "risk".to_string(),
        }),
        distinct: false,
        alias: "p75_disc_risk".to_string(),
    });
    plan.projections.push(Projection::Aggregate {
        function: AggregateFunction::StdDev,
        target: AggregateTarget::Property(PropertyRef {
            variable: "service".to_string(),
            property: "risk".to_string(),
        }),
        distinct: false,
        alias: "sample_risk".to_string(),
    });
    plan.projections.push(Projection::Aggregate {
        function: AggregateFunction::StdDevP,
        target: AggregateTarget::Property(PropertyRef {
            variable: "service".to_string(),
            property: "risk".to_string(),
        }),
        distinct: false,
        alias: "population_risk".to_string(),
    });
    plan.projections.push(Projection::Aggregate {
        function: AggregateFunction::Median,
        target: AggregateTarget::Property(PropertyRef {
            variable: "service".to_string(),
            property: "risk".to_string(),
        }),
        distinct: true,
        alias: "distinct_median_risk".to_string(),
    });
    plan.projections.push(Projection::Aggregate {
        function: AggregateFunction::StdDev,
        target: AggregateTarget::Property(PropertyRef {
            variable: "service".to_string(),
            property: "risk".to_string(),
        }),
        distinct: true,
        alias: "distinct_sample_risk".to_string(),
    });
    plan.projections.push(Projection::Aggregate {
        function: AggregateFunction::StdDevP,
        target: AggregateTarget::Property(PropertyRef {
            variable: "service".to_string(),
            property: "risk".to_string(),
        }),
        distinct: true,
        alias: "distinct_population_risk".to_string(),
    });

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("statistical aggregate projections should lower");

    assert!(
        translation
            .sql()
            .contains("MEDIAN(CAST(\"n1\".\"risk_score\" AS DOUBLE)) AS \"median_risk\""),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("PERCENTILE_CONT(\"n1\".\"risk_score\", 0.75) AS \"p75_risk\""),
        "{}",
        translation.sql()
    );
    assert!(
        translation.sql().contains(
            "STDDEV_SAMP(\"n1\".\"risk_score\") AS \"sample_risk\", \
             STDDEV_POP(\"n1\".\"risk_score\") AS \"population_risk\", \
             MEDIAN(DISTINCT CAST(\"n1\".\"risk_score\" AS DOUBLE)) AS \"distinct_median_risk\", \
             SQRT(VAR_SAMP(DISTINCT \"n1\".\"risk_score\")) AS \"distinct_sample_risk\", \
             SQRT(VAR_POP(DISTINCT \"n1\".\"risk_score\")) AS \"distinct_population_risk\""
        ),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("MAX(\"__coral_percentile_disc_0\".\"__coral_value\") AS \"p75_disc_risk\""),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("LEFT JOIN (SELECT \"__coral_percentile_disc_0_rows\""),
        "{}",
        translation.sql()
    );
    assert!(
        translation.sql().contains(
            "row_number() OVER (PARTITION BY \"__coral_percentile_disc_0_n0\".\"full_name\", \"__coral_percentile_disc_0_n1\".\"service_name\" ORDER BY \"__coral_percentile_disc_0_n1\".\"risk_score\")"
        ),
        "{}",
        translation.sql()
    );
    assert!(
        translation.sql().contains(
            "((\"__coral_percentile_disc_0\".\"__coral_group_0\" = \"n0\".\"full_name\") OR (\"__coral_percentile_disc_0\".\"__coral_group_0\" IS NULL AND \"n0\".\"full_name\" IS NULL))"
        ),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("(\"__coral_percentile_disc_0_n1\".\"risk_score\") IS NOT NULL"),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains("CASE WHEN CAST(ceil(0.75 * \"__coral_percentile_disc_0_rows\".\"__coral_n\") AS BIGINT) < 1 THEN 1 ELSE CAST(ceil(0.75 * \"__coral_percentile_disc_0_rows\".\"__coral_n\") AS BIGINT) END"),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_grouped_percentile_disc_projection_and_ordering() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![NodePattern {
            variable: "service".to_string(),
            label: "Service".to_string(),
        }],
        relationships: Vec::new(),
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                alias: Some("tier".to_string()),
            },
            Projection::Aggregate {
                function: AggregateFunction::PercentileDisc {
                    percentile: ordered_float::OrderedFloat(0.5),
                },
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: false,
                alias: "median_disc_risk".to_string(),
            },
        ],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: vec![OrderKey {
            expression: OrderExpression::Aggregate {
                function: AggregateFunction::PercentileDisc {
                    percentile: ordered_float::OrderedFloat(0.75),
                },
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: false,
            },
            direction: OrderDirection::Descending,
            nulls: None,
        }],
        skip: None,
        limit: None,
    };

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("grouped percentileDisc projection should lower");

    assert!(
        translation.sql().contains(
            "((\"__coral_percentile_disc_0\".\"__coral_group_0\" = \"n0\".\"tier\") OR (\"__coral_percentile_disc_0\".\"__coral_group_0\" IS NULL AND \"n0\".\"tier\" IS NULL))"
        ),
        "{}",
        translation.sql()
    );
    assert!(
        translation.sql().contains("GROUP BY \"n0\".\"tier\""),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains(" ORDER BY MAX(\"__coral_percentile_disc_1\".\"__coral_value\") DESC"),
        "{}",
        translation.sql()
    );
    assert!(
        translation.sql().contains("ceil(0.75 *"),
        "{}",
        translation.sql()
    );
    assert!(
        translation.sql().contains("ceil(0.5 *"),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_count_node_projection() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.projections = vec![Projection::Aggregate {
        function: AggregateFunction::Count,
        target: AggregateTarget::VariableKey {
            variable: "service".to_string(),
        },
        distinct: true,
        alias: "service_count".to_string(),
    }];
    plan.order_by = vec![OrderKey {
        expression: OrderExpression::ProjectionAlias("service_count".to_string()),
        direction: OrderDirection::Descending,
        nulls: None,
    }];

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("count node projection should lower");

    assert!(
        translation
            .sql()
            .contains("COUNT(DISTINCT \"n1\".\"id\") AS \"service_count\""),
        "{}",
        translation.sql()
    );
    assert!(
        translation
            .sql()
            .contains(" ORDER BY \"service_count\" DESC"),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_count_keyed_relationship_projection() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.relationships
        .first_mut()
        .expect("ownership plan should include a relationship")
        .variable = Some("owns".to_string());
    plan.projections = vec![Projection::Aggregate {
        function: AggregateFunction::Count,
        target: AggregateTarget::VariableKey {
            variable: "owns".to_string(),
        },
        distinct: true,
        alias: "ownership_count".to_string(),
    }];
    plan.order_by.clear();

    let translation = graph
        .lower_graph_plan(&plan)
        .expect("count keyed relationship projection should lower");

    assert!(
        translation
            .sql()
            .contains("COUNT(DISTINCT \"r0\".\"ownership_id\") AS \"ownership_count\""),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_renders_count_keyless_relationship_projection() {
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
        ],
        relationships: vec![RelationshipPattern {
            variable: Some("dependency".to_string()),
            relationship_type: "DEPENDS_ON".to_string(),
            left: "source".to_string(),
            direction: Direction::Outgoing,
            right: "target".to_string(),
        }],
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![Projection::Aggregate {
            function: AggregateFunction::Count,
            target: AggregateTarget::VariableKey {
                variable: "dependency".to_string(),
            },
            distinct: false,
            alias: "dependency_count".to_string(),
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
        .expect("count keyless relationship projection should lower");

    assert!(
        translation
            .sql()
            .contains("COUNT(\"r0\".\"from_service_id\") AS \"dependency_count\""),
        "{}",
        translation.sql()
    );
}

#[test]
fn lower_graph_plan_rejects_count_with_property_ordering() {
    let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
    let mut plan = ownership_plan(Direction::Outgoing);
    plan.projections = vec![Projection::CountAll {
        alias: "ownership_count".to_string(),
    }];

    let error = graph
        .lower_graph_plan(&plan)
        .expect_err("count with property ordering should fail");

    assert!(
        error.to_string().contains("UNSUPPORTED_AGGREGATION"),
        "{error:?}"
    );
}
