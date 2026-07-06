use super::*;

#[test]
fn compiles_graphql_flat_aggregate_fields() {
    let plan = compile_graphql(
        r"
            query {
              Service {
                tier
                services: _count
                namedServices: _count(field: name)
                tiers: _countDistinct(field: tier)
                totalRisk: _sum(field: risk)
                averageRisk: _avg(field: risk)
                minRisk: _min(field: risk)
                maxRisk: _max(field: risk)
              }
            }
            ",
    )
    .expect("GraphQL flat aggregate fields should compile");

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
            Projection::Aggregate {
                function: AggregateFunction::Count,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                }),
                distinct: false,
                alias: "namedServices".to_string(),
            },
            Projection::Aggregate {
                function: AggregateFunction::Count,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                }),
                distinct: true,
                alias: "tiers".to_string(),
            },
            Projection::Aggregate {
                function: AggregateFunction::Sum,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: false,
                alias: "totalRisk".to_string(),
            },
            Projection::Aggregate {
                function: AggregateFunction::Avg,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: false,
                alias: "averageRisk".to_string(),
            },
            Projection::Aggregate {
                function: AggregateFunction::Min,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: false,
                alias: "minRisk".to_string(),
            },
            Projection::Aggregate {
                function: AggregateFunction::Max,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: false,
                alias: "maxRisk".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_graphql_statistical_aggregate_fields() {
    let plan = compile_graphql(
        r"
            query {
              Service {
                sampleRisk: _stDev(field: risk)
                populationRisk: _stDevP(field: risk)
                distinctTotalRisk: _sumDistinct(field: risk)
                distinctAverageRisk: _avgDistinct(field: risk)
                medianRisk: _median(field: risk)
                distinctMedianRisk: _medianDistinct(field: risk)
                distinctMinRisk: _minDistinct(field: risk)
                distinctMaxRisk: _maxDistinct(field: risk)
              }
            }
            ",
    )
    .expect("GraphQL statistical aggregate fields should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Aggregate {
                function: AggregateFunction::StdDev,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: false,
                alias: "sampleRisk".to_string(),
            },
            Projection::Aggregate {
                function: AggregateFunction::StdDevP,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: false,
                alias: "populationRisk".to_string(),
            },
            Projection::Aggregate {
                function: AggregateFunction::Sum,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: true,
                alias: "distinctTotalRisk".to_string(),
            },
            Projection::Aggregate {
                function: AggregateFunction::Avg,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: true,
                alias: "distinctAverageRisk".to_string(),
            },
            Projection::Aggregate {
                function: AggregateFunction::Median,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: false,
                alias: "medianRisk".to_string(),
            },
            Projection::Aggregate {
                function: AggregateFunction::Median,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: true,
                alias: "distinctMedianRisk".to_string(),
            },
            Projection::Aggregate {
                function: AggregateFunction::Min,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: true,
                alias: "distinctMinRisk".to_string(),
            },
            Projection::Aggregate {
                function: AggregateFunction::Max,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                distinct: true,
                alias: "distinctMaxRisk".to_string(),
            },
        ]
    );
}

#[test]
fn compiles_graphql_percentile_cont_with_variable_argument() {
    let variables = BTreeMap::from([(
        "percentile".to_string(),
        GraphqlVariableValue::Literal(Literal::Float(OrderedFloat(0.9))),
    )]);
    let plan = compile_graphql_with_variables(
        r"
            query Percentile($percentile: Float!) {
              Service {
                p90Risk: _percentileCont(percentile: $percentile, field: risk)
              }
            }
            ",
        &variables,
    )
    .expect("GraphQL percentile aggregate variable should compile");

    assert_eq!(
        plan.projections,
        vec![Projection::Aggregate {
            function: AggregateFunction::PercentileCont {
                percentile: OrderedFloat(0.9),
            },
            target: AggregateTarget::Property(PropertyRef {
                variable: "service".to_string(),
                property: "risk".to_string(),
            }),
            distinct: false,
            alias: "p90Risk".to_string(),
        }]
    );
}

#[test]
fn compiles_graphql_collect_aggregate_fields() {
    let plan = compile_graphql(
        r"
            query {
              Service {
                serviceNames: _collect(field: name)
                uniqueTiers: _collectDistinct(field: tier)
              }
            }
            ",
    )
    .expect("GraphQL collect aggregate fields should compile");

    assert_eq!(
        plan.projections,
        vec![
            Projection::Aggregate {
                function: AggregateFunction::Collect,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                }),
                distinct: false,
                alias: "serviceNames".to_string(),
            },
            Projection::Aggregate {
                function: AggregateFunction::Collect,
                target: AggregateTarget::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                }),
                distinct: true,
                alias: "uniqueTiers".to_string(),
            },
        ]
    );
}

#[test]
fn rejects_invalid_graphql_flat_aggregate_arguments() {
    for query in [
        r"
            query {
              Service {
                _sum
              }
            }
            ",
        r"
            query {
              Service {
                _avg(property: risk)
              }
            }
            ",
        r"
            query {
              Service {
                _countDistinct {
                  value
                }
              }
            }
            ",
        r"
            query {
              Service {
                _percentileCont(field: risk)
              }
            }
            ",
        r"
            query {
              Service {
                _percentileCont(field: risk, percentile: 2.0)
              }
            }
            ",
    ] {
        let error =
            compile_graphql(query).expect_err("invalid GraphQL flat aggregate field should fail");

        assert!(
            error.to_string().contains("GraphQL aggregate")
                || error.to_string().contains("GraphQL percentile aggregate")
                || error.to_string().contains("unsupported GraphQL aggregate"),
            "{error}"
        );
    }
}

#[test]
fn rejects_graphql_schema_sdl_for_reserved_aggregate_property_names() {
    let graph = Declaration::from_yaml(
        r"
version: 1
name: reserved_aggregate_property
nodes:
  - label: Service
    table: { schema: ops, name: services }
    key: id
    properties:
      _median: risk_score
",
    )
    .expect("graph should parse");

    let error = graphql_schema_sdl_for_graph(&graph)
        .expect_err("reserved GraphQL aggregate property names should be rejected");

    assert!(
        error.to_string().contains("reserved GraphQL virtual field"),
        "unexpected error: {error}"
    );
}
