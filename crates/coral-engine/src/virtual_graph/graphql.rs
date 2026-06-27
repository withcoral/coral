use graphql_parser::query::{
    Definition, Field, OperationDefinition, Selection, SelectionSet, Value, parse_query,
};
use ordered_float::OrderedFloat;

use super::diagnostic::Diagnostic;
use super::ir::{
    ComparisonOperator, GraphPlan, Literal, NodePattern, OrderDirection, OrderExpression, OrderKey,
    PredicateRhs, Projection, PropertyPredicate, PropertyRef,
};
use crate::CoreError;

/// Parses and compiles the Coral-supported read-only GraphQL virtual graph subset.
///
/// The first GraphQL slice is intentionally root-node oriented: one query
/// operation with one root field whose name is the graph node label. Selected
/// scalar fields become property projections, and supported root arguments
/// compile into predicates, ordering, offsets, limits, and distinct row
/// selection. Relationship nesting is intentionally rejected until a
/// declaration-aware traversal convention is added.
///
/// # Errors
///
/// Returns [`CoreError::InvalidInput`] when the query cannot be parsed or uses
/// GraphQL features outside Coral's current read-only virtual graph subset.
pub fn compile_graphql(graphql: &str) -> Result<GraphPlan, CoreError> {
    let document = parse_query::<String>(graphql).map_err(|error| {
        Diagnostic::new("GRAPHQL_PARSE_ERROR", "query", error.to_string()).into_core_error()
    })?;
    compile_document(&document)
}

fn compile_document(
    document: &graphql_parser::query::Document<'_, String>,
) -> Result<GraphPlan, CoreError> {
    let [definition] = document.definitions.as_slice() else {
        return Err(unsupported(
            "query",
            "GraphQL virtual graph queries must contain exactly one operation",
        ));
    };
    match definition {
        Definition::Operation(operation) => compile_operation(operation),
        Definition::Fragment(_) => Err(unsupported(
            "query.definitions[0]",
            "GraphQL fragments are not supported yet",
        )),
    }
}

fn compile_operation(operation: &OperationDefinition<'_, String>) -> Result<GraphPlan, CoreError> {
    match operation {
        OperationDefinition::SelectionSet(selection_set) => {
            compile_root_selection_set(selection_set, "query.selectionSet")
        }
        OperationDefinition::Query(query) => {
            if !query.variable_definitions.is_empty() {
                return Err(unsupported(
                    "query.variables",
                    "GraphQL variables are not supported yet; use literal root arguments",
                ));
            }
            if !query.directives.is_empty() {
                return Err(unsupported(
                    "query.directives",
                    "GraphQL query directives are not supported yet",
                ));
            }
            compile_root_selection_set(&query.selection_set, "query.selectionSet")
        }
        OperationDefinition::Mutation(_) | OperationDefinition::Subscription(_) => {
            Err(unsupported(
                "query",
                "GraphQL mutations and subscriptions are not supported",
            ))
        }
    }
}

fn compile_root_selection_set(
    selection_set: &SelectionSet<'_, String>,
    path: impl Into<String>,
) -> Result<GraphPlan, CoreError> {
    let path = path.into();
    let [selection] = selection_set.items.as_slice() else {
        return Err(unsupported(
            path,
            "GraphQL virtual graph queries must select exactly one root node field",
        ));
    };
    let Selection::Field(root) = selection else {
        return Err(unsupported(
            format!("{path}.items[0]"),
            "GraphQL fragments are not supported yet",
        ));
    };
    compile_root_field(root, format!("{path}.items[0]"))
}

fn compile_root_field(
    root: &Field<'_, String>,
    path: impl Into<String>,
) -> Result<GraphPlan, CoreError> {
    let path = path.into();
    if !root.directives.is_empty() {
        return Err(unsupported(
            format!("{path}.directives"),
            "GraphQL field directives are not supported yet",
        ));
    }
    if root.alias.is_some() {
        return Err(unsupported(
            format!("{path}.alias"),
            "GraphQL root field aliases are not supported yet",
        ));
    }
    if root.selection_set.items.is_empty() {
        return Err(unsupported(
            format!("{path}.selectionSet"),
            "GraphQL root node fields must select at least one property",
        ));
    }

    let variable = variable_for_label(&root.name);
    let mut plan = GraphPlan {
        nodes: vec![NodePattern {
            variable: variable.clone(),
            label: root.name.clone(),
        }],
        relationships: Vec::new(),
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: compile_projection_selection_set(
            &root.selection_set,
            &variable,
            format!("{path}.selectionSet"),
        )?,
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    for (index, (name, value)) in root.arguments.iter().enumerate() {
        compile_root_argument(
            &mut plan,
            &variable,
            name,
            value,
            format!("{path}.arguments[{index}]"),
        )?;
    }

    Ok(plan)
}

fn compile_projection_selection_set(
    selection_set: &SelectionSet<'_, String>,
    variable: &str,
    path: impl Into<String>,
) -> Result<Vec<Projection>, CoreError> {
    let path = path.into();
    let mut projections = Vec::with_capacity(selection_set.items.len());
    for (index, selection) in selection_set.items.iter().enumerate() {
        let Selection::Field(field) = selection else {
            return Err(unsupported(
                format!("{path}.items[{index}]"),
                "GraphQL fragments are not supported yet",
            ));
        };
        if !field.arguments.is_empty() {
            return Err(unsupported(
                format!("{path}.items[{index}].arguments"),
                "GraphQL property field arguments are not supported",
            ));
        }
        if !field.directives.is_empty() {
            return Err(unsupported(
                format!("{path}.items[{index}].directives"),
                "GraphQL property field directives are not supported",
            ));
        }
        if !field.selection_set.items.is_empty() {
            return Err(unsupported(
                format!("{path}.items[{index}].selectionSet"),
                "GraphQL relationship nesting is not supported yet",
            ));
        }
        projections.push(Projection::Property {
            property: PropertyRef {
                variable: variable.to_string(),
                property: field.name.clone(),
            },
            alias: field.alias.clone().or_else(|| Some(field.name.clone())),
        });
    }
    Ok(projections)
}

fn compile_root_argument(
    plan: &mut GraphPlan,
    variable: &str,
    name: &str,
    value: &Value<'_, String>,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    match name {
        "where" => {
            plan.predicates
                .extend(compile_where_argument(variable, value, path)?);
            Ok(())
        }
        "orderBy" => {
            plan.order_by
                .extend(compile_order_by_argument(variable, value, path)?);
            Ok(())
        }
        "limit" => {
            plan.limit = Some(compile_non_negative_u64(value, path, "limit")?);
            Ok(())
        }
        "offset" | "skip" => {
            plan.skip = Some(compile_non_negative_u64(value, path, name)?);
            Ok(())
        }
        "distinct" => {
            plan.distinct = compile_boolean(value, path, "distinct")?;
            Ok(())
        }
        _ => Err(unsupported(
            path,
            format!("unsupported GraphQL root argument '{name}'"),
        )),
    }
}

fn compile_where_argument(
    variable: &str,
    value: &Value<'_, String>,
    path: impl Into<String>,
) -> Result<Vec<PropertyPredicate>, CoreError> {
    let path = path.into();
    let Value::Object(properties) = value else {
        return Err(unsupported(path, "GraphQL where must be an object"));
    };
    let mut predicates = Vec::new();
    for (property, condition) in properties {
        let condition_path = format!("{path}.{property}");
        let Value::Object(operators) = condition else {
            return Err(unsupported(
                condition_path,
                "GraphQL where property conditions must be objects",
            ));
        };
        for (operator, value) in operators {
            predicates.push(compile_where_operator(
                variable,
                property,
                operator,
                value,
                format!("{path}.{property}.{operator}"),
            )?);
        }
    }
    Ok(predicates)
}

fn compile_where_operator(
    variable: &str,
    property: &str,
    operator: &str,
    value: &Value<'_, String>,
    path: impl Into<String>,
) -> Result<PropertyPredicate, CoreError> {
    let path = path.into();
    let property = PropertyRef {
        variable: variable.to_string(),
        property: property.to_string(),
    };
    match operator {
        "eq" => Ok(PropertyPredicate {
            property,
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Literal(compile_literal(value, path)?),
        }),
        "ne" => Ok(PropertyPredicate {
            property,
            operator: ComparisonOperator::NotEqual,
            rhs: PredicateRhs::Literal(compile_literal(value, path)?),
        }),
        "gt" => Ok(PropertyPredicate {
            property,
            operator: ComparisonOperator::GreaterThan,
            rhs: PredicateRhs::Literal(compile_literal(value, path)?),
        }),
        "gte" => Ok(PropertyPredicate {
            property,
            operator: ComparisonOperator::GreaterThanOrEqual,
            rhs: PredicateRhs::Literal(compile_literal(value, path)?),
        }),
        "lt" => Ok(PropertyPredicate {
            property,
            operator: ComparisonOperator::LessThan,
            rhs: PredicateRhs::Literal(compile_literal(value, path)?),
        }),
        "lte" => Ok(PropertyPredicate {
            property,
            operator: ComparisonOperator::LessThanOrEqual,
            rhs: PredicateRhs::Literal(compile_literal(value, path)?),
        }),
        "startsWith" => Ok(PropertyPredicate {
            property,
            operator: ComparisonOperator::StartsWith,
            rhs: PredicateRhs::Literal(compile_literal(value, path)?),
        }),
        "endsWith" => Ok(PropertyPredicate {
            property,
            operator: ComparisonOperator::EndsWith,
            rhs: PredicateRhs::Literal(compile_literal(value, path)?),
        }),
        "contains" => Ok(PropertyPredicate {
            property,
            operator: ComparisonOperator::Contains,
            rhs: PredicateRhs::Literal(compile_literal(value, path)?),
        }),
        "in" => Ok(PropertyPredicate {
            property,
            operator: ComparisonOperator::In,
            rhs: PredicateRhs::List(compile_literal_list(value, path)?),
        }),
        "isNull" => {
            let is_null = compile_boolean(value, path, "isNull")?;
            Ok(PropertyPredicate {
                property,
                operator: if is_null {
                    ComparisonOperator::Equal
                } else {
                    ComparisonOperator::NotEqual
                },
                rhs: PredicateRhs::Literal(Literal::Null),
            })
        }
        _ => Err(unsupported(
            path,
            format!("unsupported GraphQL where operator '{operator}'"),
        )),
    }
}

fn compile_order_by_argument(
    variable: &str,
    value: &Value<'_, String>,
    path: impl Into<String>,
) -> Result<Vec<OrderKey>, CoreError> {
    let path = path.into();
    match value {
        Value::Object(_) => Ok(vec![compile_order_by_object(variable, value, path)?]),
        Value::List(items) => items
            .iter()
            .enumerate()
            .map(|(index, value)| {
                compile_order_by_object(variable, value, format!("{path}[{index}]"))
            })
            .collect(),
        _ => Err(unsupported(
            path,
            "GraphQL orderBy must be an object or list of objects",
        )),
    }
}

fn compile_order_by_object(
    variable: &str,
    value: &Value<'_, String>,
    path: impl Into<String>,
) -> Result<OrderKey, CoreError> {
    let path = path.into();
    let Value::Object(object) = value else {
        return Err(unsupported(path, "GraphQL orderBy entries must be objects"));
    };
    for name in object.keys() {
        if name != "field" && name != "direction" {
            return Err(unsupported(
                format!("{path}.{name}"),
                format!("unsupported GraphQL orderBy key '{name}'"),
            ));
        }
    }
    let field_value = object
        .get("field")
        .ok_or_else(|| unsupported(format!("{path}.field"), "GraphQL orderBy requires field"))?;
    let field = compile_name_value(field_value, format!("{path}.field"))?;
    let direction = object
        .get("direction")
        .map_or(Ok(OrderDirection::Ascending), |value| {
            compile_order_direction(value, format!("{path}.direction"))
        })?;
    Ok(OrderKey {
        expression: OrderExpression::Property(PropertyRef {
            variable: variable.to_string(),
            property: field,
        }),
        direction,
    })
}

fn compile_order_direction(
    value: &Value<'_, String>,
    path: impl Into<String>,
) -> Result<OrderDirection, CoreError> {
    let path = path.into();
    let direction = compile_name_value(value, path.clone())?;
    match direction.as_str() {
        "ASC" | "asc" => Ok(OrderDirection::Ascending),
        "DESC" | "desc" => Ok(OrderDirection::Descending),
        _ => Err(unsupported(
            path,
            "GraphQL orderBy direction must be ASC or DESC",
        )),
    }
}

fn compile_literal(
    value: &Value<'_, String>,
    path: impl Into<String>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    match value {
        Value::Int(number) => number
            .as_i64()
            .map(Literal::Integer)
            .ok_or_else(|| unsupported(path, "GraphQL integer literal is out of range")),
        Value::Float(value) if value.is_finite() => Ok(Literal::Float(OrderedFloat(*value))),
        Value::Float(_) => Err(unsupported(
            path,
            "GraphQL float literals must be finite numbers",
        )),
        Value::String(value) => Ok(Literal::String(value.clone())),
        Value::Boolean(value) => Ok(Literal::Boolean(*value)),
        Value::Null => Ok(Literal::Null),
        Value::Variable(_) => Err(unsupported(path, "GraphQL variables are not supported yet")),
        Value::Enum(_) | Value::List(_) | Value::Object(_) => {
            Err(unsupported(path, "GraphQL value must be a scalar literal"))
        }
    }
}

fn compile_literal_list(
    value: &Value<'_, String>,
    path: impl Into<String>,
) -> Result<Vec<Literal>, CoreError> {
    let path = path.into();
    let Value::List(items) = value else {
        return Err(unsupported(
            path,
            "GraphQL IN values must be a literal list",
        ));
    };
    items
        .iter()
        .enumerate()
        .map(|(index, value)| compile_literal(value, format!("{path}[{index}]")))
        .collect()
}

fn compile_non_negative_u64(
    value: &Value<'_, String>,
    path: impl Into<String>,
    name: &str,
) -> Result<u64, CoreError> {
    let path = path.into();
    let Value::Int(number) = value else {
        return Err(unsupported(
            path,
            format!("GraphQL {name} must be a non-negative integer"),
        ));
    };
    let value = number
        .as_i64()
        .ok_or_else(|| unsupported(path.clone(), format!("GraphQL {name} is out of range")))?;
    u64::try_from(value).map_err(|error| {
        unsupported(
            path,
            format!("GraphQL {name} must be a non-negative integer: {error}"),
        )
    })
}

fn compile_boolean(
    value: &Value<'_, String>,
    path: impl Into<String>,
    name: &str,
) -> Result<bool, CoreError> {
    let path = path.into();
    let Value::Boolean(value) = value else {
        return Err(unsupported(
            path,
            format!("GraphQL {name} must be a boolean"),
        ));
    };
    Ok(*value)
}

fn compile_name_value(
    value: &Value<'_, String>,
    path: impl Into<String>,
) -> Result<String, CoreError> {
    let path = path.into();
    match value {
        Value::Enum(value) | Value::String(value) => Ok(value.clone()),
        _ => Err(unsupported(
            path,
            "GraphQL value must be an enum or string name",
        )),
    }
}

fn variable_for_label(label: &str) -> String {
    let mut chars = label.chars();
    match chars.next() {
        Some(first) => first.to_lowercase().chain(chars).collect(),
        None => "node".to_string(),
    }
}

fn unsupported(path: impl Into<String>, message: impl Into<String>) -> CoreError {
    Diagnostic::new("UNSUPPORTED_GRAPHQL", path, message).into_core_error()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compiles_root_node_query() {
        let plan = compile_graphql(
            r#"
            query {
              Service(
                where: { tier: { eq: "prod" }, risk: { gte: 0.5 } }
                orderBy: [{ field: name, direction: ASC }]
                limit: 10
                offset: 2
              ) {
                serviceName: name
                tier
              }
            }
            "#,
        )
        .expect("GraphQL query should compile");

        assert_eq!(
            plan.nodes,
            vec![NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            }]
        );
        assert_eq!(
            plan.projections,
            vec![
                Projection::Property {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("serviceName".to_string()),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    },
                    alias: Some("tier".to_string()),
                },
            ]
        );
        assert_eq!(plan.predicates.len(), 2);
        assert_eq!(
            plan.order_by,
            vec![OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                }),
                direction: OrderDirection::Ascending,
            }]
        );
        assert_eq!(plan.limit, Some(10));
        assert_eq!(plan.skip, Some(2));
    }

    #[test]
    fn rejects_nested_graphql_selection() {
        let error = compile_graphql(
            r"
            {
              Service {
                name
                out_DEPENDS_ON { name }
              }
            }
            ",
        )
        .expect_err("nested selections should be rejected for first GraphQL slice");

        assert!(
            error.to_string().contains("relationship nesting"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_unknown_graphql_order_by_keys() {
        let error = compile_graphql(
            r"
            {
              Service(orderBy: { field: name, direction: ASC, nulls: LAST }) {
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
}
