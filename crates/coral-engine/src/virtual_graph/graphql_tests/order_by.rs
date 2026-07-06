use super::*;

#[test]
fn compiles_graphql_shorthand_order_by_fields() {
    let plan = compile_graphql(
        r"
            query {
              Service(
                orderBy: [
                  { risk: DESC }
                  { name: ASCENDING }
                ]
              ) {
                name
              }
            }
            ",
    )
    .expect("GraphQL shorthand orderBy fields should compile");

    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                }),
                direction: OrderDirection::Descending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn compiles_graphql_order_by_null_placement() {
    let plan = compile_graphql(
        r"
            query {
              Service(
                orderBy: [
                  { field: tier, direction: ASC, nulls: LAST }
                  { field: name, direction: DESC, nulls: FIRST }
                ]
              ) {
                name
              }
            }
            ",
    )
    .expect("GraphQL orderBy null placement should compile");

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
}

#[test]
fn compiles_root_query_with_order_by_object_variable() {
    let variables = BTreeMap::from([(
        "order".to_string(),
        variable_object([
            (
                "field",
                GraphqlVariableValue::Literal(Literal::String("name".to_string())),
            ),
            (
                "direction",
                GraphqlVariableValue::Literal(Literal::String("DESC".to_string())),
            ),
        ]),
    )]);
    let plan = compile_graphql_with_variables(
        r"
            query Services($order: ServiceOrder!) {
              Service(orderBy: $order) { name }
            }
            ",
        &variables,
    )
    .expect("GraphQL orderBy object variable should compile");

    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            }),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_root_query_with_order_by_null_placement_variable() {
    let variables = BTreeMap::from([(
        "order".to_string(),
        variable_object([
            (
                "field",
                GraphqlVariableValue::Literal(Literal::String("tier".to_string())),
            ),
            (
                "direction",
                GraphqlVariableValue::Literal(Literal::String("ASC".to_string())),
            ),
            (
                "nulls",
                GraphqlVariableValue::Literal(Literal::String("NULLS_LAST".to_string())),
            ),
        ]),
    )]);
    let plan = compile_graphql_with_variables(
        r"
            query Services($order: ServiceOrder!) {
              Service(orderBy: $order) { name }
            }
            ",
        &variables,
    )
    .expect("GraphQL orderBy null placement variable should compile");

    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            }),
            direction: OrderDirection::Ascending,
            nulls: Some(NullOrder::Last),
        }]
    );
}

#[test]
fn compiles_root_query_with_shorthand_order_by_object_variable() {
    let variables = BTreeMap::from([(
        "order".to_string(),
        variable_object([(
            "name",
            GraphqlVariableValue::Literal(Literal::String("DESC".to_string())),
        )]),
    )]);
    let plan = compile_graphql_with_variables(
        r"
            query Services($order: ServiceOrder!) {
              Service(orderBy: $order) { name }
            }
            ",
        &variables,
    )
    .expect("GraphQL shorthand orderBy object variable should compile");

    assert_eq!(
        plan.order_by,
        vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            }),
            direction: OrderDirection::Descending,
            nulls: None,
        }]
    );
}

#[test]
fn compiles_root_query_with_order_by_object_list_variable() {
    let variables = BTreeMap::from([(
        "orders".to_string(),
        GraphqlVariableValue::ObjectList(vec![
            variable_object_map([
                (
                    "field",
                    GraphqlVariableValue::Literal(Literal::String("tier".to_string())),
                ),
                (
                    "direction",
                    GraphqlVariableValue::Literal(Literal::String("ASC".to_string())),
                ),
            ]),
            variable_object_map([
                (
                    "field",
                    GraphqlVariableValue::Literal(Literal::String("name".to_string())),
                ),
                (
                    "direction",
                    GraphqlVariableValue::Literal(Literal::String("DESC".to_string())),
                ),
            ]),
        ]),
    )]);
    let plan = compile_graphql_with_variables(
        r"
            query Services($orders: [ServiceOrder!]!) {
              Service(orderBy: $orders) { name }
            }
            ",
        &variables,
    )
    .expect("GraphQL orderBy object-list variable should compile");

    assert_eq!(
        plan.order_by,
        vec![
            OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            },
            OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                }),
                direction: OrderDirection::Descending,
                nulls: None,
            },
        ]
    );
}

#[test]
fn rejects_graphql_scalar_variable_in_order_by_position() {
    let variables = BTreeMap::from([(
        "order".to_string(),
        GraphqlVariableValue::Literal(Literal::String("name".to_string())),
    )]);
    let error = compile_graphql_with_variables(
        r"
            query Services($order: ServiceOrder!) {
              Service(orderBy: $order) { name }
            }
            ",
        &variables,
    )
    .expect_err("scalar variable in orderBy position should fail");

    assert!(
        error
            .to_string()
            .contains("must be an orderBy object or list of objects"),
        "{error}"
    );
}

#[test]
fn compiles_empty_order_by_list_variable_default_as_no_order_keys() {
    let plan = compile_graphql_with_variables(
        r"
            query Services($orders: [ServiceOrder!] = []) {
              Service(orderBy: $orders) { name }
            }
            ",
        &BTreeMap::new(),
    )
    .expect("empty GraphQL orderBy defaults should compile as no-op ordering");

    assert!(plan.order_by.is_empty());
}

#[test]
fn compiles_empty_order_by_list_variable_as_no_order_keys() {
    let variables =
        BTreeMap::from([("orders".to_string(), GraphqlVariableValue::List(Vec::new()))]);
    let plan = compile_graphql_with_variables(
        r"
            query Services($orders: [ServiceOrder!]!) {
              Service(orderBy: $orders) { name }
            }
            ",
        &variables,
    )
    .expect("empty GraphQL orderBy variables should compile as no-op ordering");

    assert!(plan.order_by.is_empty());
}

#[test]
fn rejects_unknown_graphql_order_by_keys() {
    let error = compile_graphql(
        r"
            {
              Service(orderBy: { field: name, direction: ASC, collation: CASE_INSENSITIVE }) {
                name
              }
            }
            ",
    )
    .expect_err("unknown orderBy keys should be rejected");

    assert!(
        error
            .to_string()
            .contains("unsupported GraphQL orderBy key"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_unknown_graphql_order_by_null_placement() {
    let error = compile_graphql(
        r"
            {
              Service(orderBy: { field: name, direction: ASC, nulls: MIDDLE }) {
                name
              }
            }
            ",
    )
    .expect_err("unknown orderBy null placement should be rejected");

    assert!(
        error
            .to_string()
            .contains("GraphQL orderBy nulls must be FIRST, LAST"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_multi_field_graphql_shorthand_order_by_objects() {
    let error = compile_graphql(
        r"
            {
              Service(orderBy: { risk: DESC, name: ASC }) {
                name
              }
            }
            ",
    )
    .expect_err("multi-field shorthand orderBy object should fail");

    assert!(
        error
            .to_string()
            .contains("shorthand orderBy entries must contain exactly one field"),
        "unexpected error: {error}"
    );
}

#[test]
fn graphql_schema_sdl_skips_reserved_shorthand_order_by_fields() {
    let graph = Declaration::from_yaml(
        r"
version: 1
name: order_reserved
nodes:
  - label: Service
    table: { schema: ops, name: services }
    key: id
    properties:
      field: field_column
      direction: direction_column
      nulls: nulls_column
      name: service_name
",
    )
    .expect("graph should parse");

    let sdl = graphql_schema_sdl_for_graph(&graph).expect("schema SDL should generate");
    graphql_parser::schema::parse_schema::<String>(&sdl)
        .expect("generated SDL should parse as GraphQL schema");

    let (_, order_input) = sdl
        .split_once("input ServiceOrderBy {")
        .expect("ServiceOrderBy input should exist");
    let (order_input, _) = order_input
        .split_once("}\n\n")
        .expect("ServiceOrderBy input should terminate");

    assert_eq!(order_input.matches("  field:").count(), 1);
    assert_eq!(order_input.matches("  direction:").count(), 1);
    assert_eq!(order_input.matches("  nulls:").count(), 1);
    assert!(order_input.contains("  _id: CoralGraphOrderDirection"));
    assert!(order_input.contains("  _elementId: CoralGraphOrderDirection"));
    assert!(order_input.contains("  id: CoralGraphOrderDirection"));
    assert!(order_input.contains("  name: CoralGraphOrderDirection"));
}
