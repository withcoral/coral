use graphql_parser::query::{
    Definition, Field, OperationDefinition, Selection, SelectionSet, Value, parse_query,
};
use ordered_float::OrderedFloat;

use super::declaration::Declaration;
use super::diagnostic::Diagnostic;
use super::ir::{
    ComparisonOperator, Direction, GraphPlan, Literal, NodePattern, OrderDirection,
    OrderExpression, OrderKey, PredicateRhs, Projection, PropertyPredicate, PropertyRef,
    RelationshipPattern,
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
    compile_document(&document, None)
}

/// Parses and compiles the Coral-supported read-only GraphQL virtual graph
/// subset using a graph declaration for relationship nesting.
///
/// # Errors
///
/// Returns [`CoreError::InvalidInput`] when the query cannot be parsed or uses
/// GraphQL features outside Coral's current read-only virtual graph subset.
pub fn compile_graphql_for_graph(
    graph: &Declaration,
    graphql: &str,
) -> Result<GraphPlan, CoreError> {
    let document = parse_query::<String>(graphql).map_err(|error| {
        Diagnostic::new("GRAPHQL_PARSE_ERROR", "query", error.to_string()).into_core_error()
    })?;
    compile_document(&document, Some(graph))
}

fn compile_document(
    document: &graphql_parser::query::Document<'_, String>,
    graph: Option<&Declaration>,
) -> Result<GraphPlan, CoreError> {
    let [definition] = document.definitions.as_slice() else {
        return Err(unsupported(
            "query",
            "GraphQL virtual graph queries must contain exactly one operation",
        ));
    };
    match definition {
        Definition::Operation(operation) => compile_operation(operation, graph),
        Definition::Fragment(_) => Err(unsupported(
            "query.definitions[0]",
            "GraphQL fragments are not supported yet",
        )),
    }
}

fn compile_operation(
    operation: &OperationDefinition<'_, String>,
    graph: Option<&Declaration>,
) -> Result<GraphPlan, CoreError> {
    match operation {
        OperationDefinition::SelectionSet(selection_set) => {
            compile_root_selection_set(selection_set, graph, "query.selectionSet")
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
            compile_root_selection_set(&query.selection_set, graph, "query.selectionSet")
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
    graph: Option<&Declaration>,
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
    compile_root_field(root, graph, format!("{path}.items[0]"))
}

fn compile_root_field(
    root: &Field<'_, String>,
    graph: Option<&Declaration>,
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
        projections: Vec::new(),
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    compile_selection_set_into_plan(
        &mut plan,
        graph,
        &root.selection_set,
        &NodeContext {
            variable: variable.clone(),
            label: root.name.clone(),
            is_root: true,
            edge_variable: None,
        },
        format!("{path}.selectionSet"),
    )?;

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

#[derive(Debug, Clone)]
struct NodeContext {
    variable: String,
    label: String,
    is_root: bool,
    edge_variable: Option<String>,
}

fn compile_selection_set_into_plan(
    plan: &mut GraphPlan,
    graph: Option<&Declaration>,
    selection_set: &SelectionSet<'_, String>,
    context: &NodeContext,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    for (index, selection) in selection_set.items.iter().enumerate() {
        let Selection::Field(field) = selection else {
            return Err(unsupported(
                format!("{path}.items[{index}]"),
                "GraphQL fragments are not supported yet",
            ));
        };
        if !field.directives.is_empty() {
            return Err(unsupported(
                format!("{path}.items[{index}].directives"),
                "GraphQL field directives are not supported",
            ));
        }
        if field.name == "_edge" {
            compile_edge_field(plan, field, context, format!("{path}.items[{index}]"))?;
        } else if field.selection_set.items.is_empty() {
            if !field.arguments.is_empty() {
                return Err(unsupported(
                    format!("{path}.items[{index}].arguments"),
                    "GraphQL property field arguments are not supported",
                ));
            }
            plan.projections.push(Projection::Property {
                property: PropertyRef {
                    variable: context.variable.clone(),
                    property: field.name.clone(),
                },
                alias: Some(projection_alias(field, context)),
            });
        } else {
            let graph = graph.ok_or_else(|| {
                unsupported(
                    format!("{path}.items[{index}].selectionSet"),
                    "GraphQL relationship nesting requires a graph declaration",
                )
            })?;
            compile_relationship_field(
                plan,
                graph,
                context,
                field,
                format!("{path}.items[{index}]"),
            )?;
        }
    }
    Ok(())
}

fn compile_relationship_field(
    plan: &mut GraphPlan,
    graph: &Declaration,
    source: &NodeContext,
    field: &Field<'_, String>,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    let (direction, relationship_type, endpoint_argument) =
        compile_relationship_field_name(&field.name, format!("{path}.name"))?;
    let target_label =
        compile_relationship_target_label(field, endpoint_argument, format!("{path}.arguments"))?;

    ensure_node_label(
        graph,
        &target_label,
        format!("{path}.arguments.{endpoint_argument}"),
    )?;
    ensure_relationship_mapping(
        graph,
        &relationship_type,
        direction,
        &source.label,
        &target_label,
        &path,
    )?;

    let relationship_index = plan.relationships.len();
    let needs_relationship_variable = field
        .arguments
        .iter()
        .any(|(name, _)| name == "relationshipWhere")
        || field
            .selection_set
            .items
            .iter()
            .any(selection_is_edge_field);
    let relationship_variable = needs_relationship_variable
        .then(|| relationship_variable_for_field(field, relationship_index));
    let target_variable = nested_variable_for_field(field, &target_label, plan.nodes.len());

    for (index, (name, value)) in field.arguments.iter().enumerate() {
        let argument_path = format!("{path}.arguments[{index}]");
        match name.as_str() {
            "to" | "from" | "label" => {
                if name != endpoint_argument {
                    return Err(unsupported(
                        argument_path,
                        format!(
                            "GraphQL relationship field '{}' requires '{}' instead of '{}'",
                            field.name, endpoint_argument, name
                        ),
                    ));
                }
            }
            "where" => plan.predicates.extend(compile_where_argument(
                &target_variable,
                value,
                argument_path,
            )?),
            "relationshipWhere" => {
                let relationship_variable = relationship_variable
                    .as_deref()
                    .ok_or_else(|| CoreError::internal("relationshipWhere variable missing"))?;
                plan.predicates.extend(compile_where_argument(
                    relationship_variable,
                    value,
                    argument_path,
                )?);
            }
            "orderBy" | "limit" | "offset" | "skip" | "distinct" => {
                return Err(unsupported(
                    argument_path,
                    "GraphQL nested relationship fields do not support row modifiers yet",
                ));
            }
            _ => {
                return Err(unsupported(
                    argument_path,
                    format!("unsupported GraphQL relationship argument '{name}'"),
                ));
            }
        }
    }

    plan.nodes.push(NodePattern {
        variable: target_variable.clone(),
        label: target_label.clone(),
    });
    plan.relationships.push(RelationshipPattern {
        variable: relationship_variable.clone(),
        relationship_type,
        left: source.variable.clone(),
        direction,
        right: target_variable.clone(),
    });

    compile_selection_set_into_plan(
        plan,
        Some(graph),
        &field.selection_set,
        &NodeContext {
            variable: target_variable,
            label: target_label,
            is_root: false,
            edge_variable: relationship_variable,
        },
        format!("{path}.selectionSet"),
    )
}

fn compile_edge_field(
    plan: &mut GraphPlan,
    field: &Field<'_, String>,
    context: &NodeContext,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    if field.selection_set.items.is_empty() {
        return Err(unsupported(
            format!("{path}.selectionSet"),
            "GraphQL _edge fields must select relationship properties",
        ));
    }
    if !field.arguments.is_empty() {
        return Err(unsupported(
            format!("{path}.arguments"),
            "GraphQL _edge fields do not support arguments",
        ));
    }
    let edge_variable = context.edge_variable.as_deref().ok_or_else(|| {
        unsupported(
            path.clone(),
            "GraphQL _edge selections are only valid inside relationship fields",
        )
    })?;
    for (index, selection) in field.selection_set.items.iter().enumerate() {
        let Selection::Field(property) = selection else {
            return Err(unsupported(
                format!("{path}.selectionSet.items[{index}]"),
                "GraphQL fragments are not supported inside _edge selections",
            ));
        };
        if !property.arguments.is_empty() {
            return Err(unsupported(
                format!("{path}.selectionSet.items[{index}].arguments"),
                "GraphQL _edge property arguments are not supported",
            ));
        }
        if !property.directives.is_empty() {
            return Err(unsupported(
                format!("{path}.selectionSet.items[{index}].directives"),
                "GraphQL _edge property directives are not supported",
            ));
        }
        if !property.selection_set.items.is_empty() {
            return Err(unsupported(
                format!("{path}.selectionSet.items[{index}].selectionSet"),
                "GraphQL _edge properties must be scalar fields",
            ));
        }
        plan.projections.push(Projection::Property {
            property: PropertyRef {
                variable: edge_variable.to_string(),
                property: property.name.clone(),
            },
            alias: Some(edge_projection_alias(property, edge_variable)),
        });
    }
    Ok(())
}

fn compile_relationship_field_name(
    name: &str,
    path: impl Into<String>,
) -> Result<(Direction, String, &'static str), CoreError> {
    let path = path.into();
    if let Some(relationship_type) = name.strip_prefix("out_") {
        return non_empty_relationship_type(relationship_type, path, Direction::Outgoing, "to");
    }
    if let Some(relationship_type) = name.strip_prefix("in_") {
        return non_empty_relationship_type(relationship_type, path, Direction::Incoming, "from");
    }
    if let Some(relationship_type) = name.strip_prefix("any_") {
        return non_empty_relationship_type(
            relationship_type,
            path,
            Direction::Undirected,
            "label",
        );
    }
    Err(unsupported(
        path,
        "GraphQL relationship fields must be named out_TYPE, in_TYPE, or any_TYPE",
    ))
}

fn non_empty_relationship_type(
    relationship_type: &str,
    path: String,
    direction: Direction,
    endpoint_argument: &'static str,
) -> Result<(Direction, String, &'static str), CoreError> {
    if relationship_type.is_empty() {
        return Err(unsupported(
            path,
            "GraphQL relationship field is missing a relationship type",
        ));
    }
    Ok((direction, relationship_type.to_string(), endpoint_argument))
}

fn compile_relationship_target_label(
    field: &Field<'_, String>,
    endpoint_argument: &str,
    path: impl Into<String>,
) -> Result<String, CoreError> {
    let path = path.into();
    let mut target_label = None;
    for (index, (name, value)) in field.arguments.iter().enumerate() {
        if name == endpoint_argument {
            if target_label.is_some() {
                return Err(unsupported(
                    format!("{path}[{index}]"),
                    format!("GraphQL relationship argument '{endpoint_argument}' is duplicated"),
                ));
            }
            target_label = Some(compile_name_value(
                value,
                format!("{path}.{endpoint_argument}"),
            )?);
        }
    }
    target_label.ok_or_else(|| {
        unsupported(
            path,
            format!("GraphQL relationship field requires '{endpoint_argument}'"),
        )
    })
}

fn ensure_node_label(
    graph: &Declaration,
    label: &str,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    if graph.node(label).is_some() {
        return Ok(());
    }
    Err(unsupported(
        path,
        format!("unknown GraphQL target node label '{label}'"),
    ))
}

fn ensure_relationship_mapping(
    graph: &Declaration,
    relationship_type: &str,
    direction: Direction,
    left_label: &str,
    right_label: &str,
    path: &str,
) -> Result<(), CoreError> {
    let candidates = graph
        .relationships_for_type(relationship_type)
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Err(unsupported(
            format!("{path}.name"),
            format!("unknown GraphQL relationship type '{relationship_type}'"),
        ));
    }
    let matches = candidates
        .iter()
        .copied()
        .filter(|relationship| {
            let matches_forward =
                left_label == relationship.from.label && right_label == relationship.to.label;
            let matches_reverse =
                left_label == relationship.to.label && right_label == relationship.from.label;
            match direction {
                Direction::Outgoing => matches_forward,
                Direction::Incoming => matches_reverse,
                Direction::Undirected => matches_forward || matches_reverse,
            }
        })
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [_] => Ok(()),
        [] => {
            let available = candidates
                .iter()
                .map(|relationship| {
                    format!("{} -> {}", relationship.from.label, relationship.to.label)
                })
                .collect::<Vec<_>>()
                .join(", ");
            Err(unsupported(
                path.to_string(),
                format!(
                    "GraphQL relationship type '{relationship_type}' has no mapping for {left_label} -> {right_label}; available endpoint mappings: {available}"
                ),
            ))
        }
        _ => Err(unsupported(
            path.to_string(),
            format!(
                "GraphQL relationship type '{relationship_type}' with endpoints {left_label} -> {right_label} is ambiguous"
            ),
        )),
    }
}

fn projection_alias(field: &Field<'_, String>, context: &NodeContext) -> String {
    field.alias.clone().unwrap_or_else(|| {
        if context.is_root {
            field.name.clone()
        } else {
            format!("{}_{}", context.variable, field.name)
        }
    })
}

fn edge_projection_alias(field: &Field<'_, String>, edge_variable: &str) -> String {
    field
        .alias
        .clone()
        .unwrap_or_else(|| format!("{edge_variable}_{}", field.name))
}

fn selection_is_edge_field(selection: &Selection<'_, String>) -> bool {
    matches!(selection, Selection::Field(field) if field.name == "_edge")
}

fn relationship_variable_for_field(field: &Field<'_, String>, index: usize) -> String {
    field.alias.as_ref().map_or_else(
        || format!("relationship{index}"),
        |alias| format!("{alias}_edge"),
    )
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

fn nested_variable_for_label(label: &str, index: usize) -> String {
    format!("{}{}", variable_for_label(label), index)
}

fn nested_variable_for_field(field: &Field<'_, String>, label: &str, index: usize) -> String {
    field
        .alias
        .clone()
        .unwrap_or_else(|| nested_variable_for_label(label, index))
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
    fn compiles_nested_outgoing_relationship_query_with_declaration() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        let plan = compile_graphql_for_graph(
            &graph,
            r#"
            {
              Person(where: { team: { eq: "infra" } }) {
                owner: name
                out_OWNS(
                  to: Service
                  relationshipWhere: { source: { eq: "pagerduty" } }
                  where: { tier: { eq: "prod" } }
                ) {
                  service: name
                  risk
                  _edge {
                    ownershipSource: source
                  }
                }
              }
            }
            "#,
        )
        .expect("nested GraphQL query should compile");

        assert_eq!(
            plan.nodes,
            vec![
                NodePattern {
                    variable: "person".to_string(),
                    label: "Person".to_string(),
                },
                NodePattern {
                    variable: "service1".to_string(),
                    label: "Service".to_string(),
                },
            ]
        );
        assert_eq!(
            plan.relationships,
            vec![RelationshipPattern {
                variable: Some("relationship0".to_string()),
                relationship_type: "OWNS".to_string(),
                left: "person".to_string(),
                direction: Direction::Outgoing,
                right: "service1".to_string(),
            }]
        );
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
                        variable: "service1".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("service".to_string()),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "service1".to_string(),
                        property: "risk".to_string(),
                    },
                    alias: Some("service1_risk".to_string()),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "relationship0".to_string(),
                        property: "source".to_string(),
                    },
                    alias: Some("ownershipSource".to_string()),
                },
            ]
        );
        assert_eq!(plan.predicates.len(), 3);
        assert!(plan.predicates.iter().any(|predicate| {
            predicate.property.variable == "person" && predicate.property.property == "team"
        }));
        assert!(plan.predicates.iter().any(|predicate| {
            predicate.property.variable == "service1" && predicate.property.property == "tier"
        }));
        assert!(plan.predicates.iter().any(|predicate| {
            predicate.property.variable == "relationship0"
                && predicate.property.property == "source"
        }));
    }

    #[test]
    fn compiles_nested_incoming_relationship_query_with_declaration() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        let plan = compile_graphql_for_graph(
            &graph,
            r"
            {
              Service {
                service: name
                owners: in_OWNS(from: Person) {
                  owner: name
                  team
                  _edge {
                    source
                  }
                }
              }
            }
            ",
        )
        .expect("incoming nested GraphQL query should compile");

        assert_eq!(
            plan.relationships,
            vec![RelationshipPattern {
                variable: Some("owners_edge".to_string()),
                relationship_type: "OWNS".to_string(),
                left: "service".to_string(),
                direction: Direction::Incoming,
                right: "owners".to_string(),
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
                    alias: Some("service".to_string()),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "owners".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("owner".to_string()),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "owners".to_string(),
                        property: "team".to_string(),
                    },
                    alias: Some("owners_team".to_string()),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "owners_edge".to_string(),
                        property: "source".to_string(),
                    },
                    alias: Some("owners_edge_source".to_string()),
                },
            ]
        );
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

    #[test]
    fn rejects_nested_graphql_relationship_endpoint_mismatches() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        let error = compile_graphql_for_graph(
            &graph,
            r"
            {
              Person {
                out_OWNS(to: Person) { name }
              }
            }
            ",
        )
        .expect_err("endpoint mismatch should be rejected");

        assert!(
            error.to_string().contains("has no mapping"),
            "unexpected error: {error}"
        );
    }

    const TEST_GRAPH: &str = r"
version: 1
name: test
nodes:
  - label: Person
    table: { schema: ops, name: people }
    key: id
    properties:
      name: full_name
      team: team
  - label: Service
    table: { schema: ops, name: services }
    key: id
    properties:
      name: service_name
      tier: tier
      risk: risk_score
relationships:
  - type: OWNS
    table: { schema: ops, name: ownerships }
    key: ownership_id
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
    properties:
      source: source
";
}
