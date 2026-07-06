//! GraphQL where/filter predicate compilation.

use std::collections::BTreeMap;

use graphql_parser::query::Value;
use regex::Regex;

use super::super::declaration::Declaration;
use super::super::ir::{
    ComparisonOperator, Direction, ElementIdPredicate, ExistsPatternPredicate, GraphPlan,
    KeyPredicate, Literal, NodePattern, PredicateExpression, PredicateRhs, PropertyPredicate,
    PropertyRef, RelationshipPattern,
};
use super::{
    GraphqlCompileContext, GraphqlVariableValue, NodeContext, RelationshipEndpointContext,
    compile_boolean, compile_literal, compile_name_value, compile_relationship_field_name,
    compile_variable_name_value, ensure_node_label, ensure_relationship_mapping,
    infer_relationship_target_label, unsupported, variable_for_label,
};
use crate::CoreError;

#[derive(Debug, Clone, Copy)]
pub(super) struct GraphqlWhereScope<'a> {
    pub(super) graph: Option<&'a Declaration>,
    pub(super) variable: &'a str,
    pub(super) label: Option<&'a str>,
}

impl<'a> GraphqlWhereScope<'a> {
    pub(super) fn node(graph: Option<&'a Declaration>, context: &'a NodeContext) -> Self {
        Self {
            graph,
            variable: &context.variable,
            label: Some(&context.label),
        }
    }

    pub(super) fn graph_variable(variable: &'a str) -> Self {
        Self {
            graph: None,
            variable,
            label: None,
        }
    }
}

pub(super) fn compile_where_argument(
    scope: GraphqlWhereScope<'_>,
    value: &Value<'_, String>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<Option<PredicateExpression>, CoreError> {
    let path = path.into();
    if let Value::Variable(variable) = value {
        let GraphqlVariableValue::Object(object) =
            context.parameter_value(variable, path.clone())?
        else {
            return Err(unsupported(
                path,
                format!("GraphQL variable '${variable}' must be an object"),
            ));
        };
        return compile_where_variable_object(scope, object, path, context);
    }
    let Value::Object(properties) = value else {
        return Err(unsupported(path, "GraphQL where must be an object"));
    };
    let mut expression = None;
    for (property, condition) in properties {
        let next = if let Some(operator) = graphql_boolean_operator(property) {
            compile_where_boolean_operator(
                scope,
                operator,
                condition,
                format!("{path}.{property}"),
                context,
            )?
        } else if is_graphql_relationship_filter_key(scope, property) {
            compile_relationship_existence_filter(
                scope,
                property,
                condition,
                format!("{path}.{property}"),
                context,
            )?
        } else {
            compile_where_property_conditions(
                scope.variable,
                property,
                condition,
                format!("{path}.{property}"),
                context,
            )?
        };
        expression = append_optional_and(expression, next);
    }
    Ok(expression)
}

fn compile_where_variable_object(
    scope: GraphqlWhereScope<'_>,
    object: &BTreeMap<String, GraphqlVariableValue>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<Option<PredicateExpression>, CoreError> {
    let path = path.into();
    let mut expression = None;
    for (property, condition) in object {
        let next = if let Some(operator) = graphql_boolean_operator(property) {
            compile_where_variable_boolean_operator(
                scope,
                operator,
                condition,
                format!("{path}.{property}"),
                context,
            )?
        } else if is_graphql_relationship_filter_key(scope, property) {
            compile_relationship_existence_filter_variable(
                scope,
                property,
                condition,
                format!("{path}.{property}"),
                context,
            )?
        } else {
            let next_path = format!("{path}.{property}");
            match condition {
                GraphqlVariableValue::Object(condition_object) => {
                    compile_where_variable_property_conditions(
                        scope.variable,
                        property,
                        condition_object,
                        next_path,
                    )?
                }
                GraphqlVariableValue::Literal(literal) => Some(compile_where_shorthand_expression(
                    scope.variable,
                    property,
                    comparison_rhs_from_literal(literal.clone()),
                    next_path,
                )?),
                GraphqlVariableValue::List(_) | GraphqlVariableValue::ObjectList(_) => {
                    return Err(unsupported(
                        next_path,
                        "GraphQL where property shorthand variables must be scalar literals or property condition objects",
                    ));
                }
            }
        };
        expression = append_optional_and(expression, next);
    }
    Ok(expression)
}

fn is_graphql_relationship_filter_key(scope: GraphqlWhereScope<'_>, name: &str) -> bool {
    let Some(graph) = scope.graph else {
        return false;
    };
    let Some(source_label) = scope.label else {
        return false;
    };
    let (direction, relationship_type) = if let Some(relationship_type) = name.strip_prefix("out_")
    {
        (Direction::Outgoing, relationship_type)
    } else if let Some(relationship_type) = name.strip_prefix("in_") {
        (Direction::Incoming, relationship_type)
    } else if let Some(relationship_type) = name.strip_prefix("any_") {
        (Direction::Undirected, relationship_type)
    } else {
        return false;
    };
    if relationship_type.is_empty() {
        return false;
    }
    graph
        .relationships_for_type(relationship_type)
        .any(|relationship| match direction {
            Direction::Outgoing => relationship.from.label == source_label,
            Direction::Incoming => relationship.to.label == source_label,
            Direction::Undirected => {
                relationship.from.label == source_label || relationship.to.label == source_label
            }
        })
}

fn compile_relationship_existence_filter(
    scope: GraphqlWhereScope<'_>,
    field_name: &str,
    value: &Value<'_, String>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<Option<PredicateExpression>, CoreError> {
    let path = path.into();
    if let Value::Variable(variable) = value {
        return compile_relationship_existence_filter_variable(
            scope,
            field_name,
            context.parameter_value(variable, path.clone())?,
            path,
            context,
        );
    }
    let Value::Object(arguments) = value else {
        return Err(unsupported(
            path,
            "GraphQL relationship existence filters must be objects",
        ));
    };
    let graph = scope
        .graph
        .ok_or_else(|| CoreError::internal("relationship filter graph missing"))?;
    let source_label = scope
        .label
        .ok_or_else(|| CoreError::internal("relationship filter source label missing"))?;
    let (direction, relationship_type, endpoint_argument) =
        compile_relationship_field_name(field_name, format!("{path}.name"))?;
    let endpoint = RelationshipEndpointContext {
        graph,
        source_label,
        relationship_type: &relationship_type,
        direction,
        endpoint_argument,
    };

    let target_label =
        compile_relationship_filter_target_label(&endpoint, arguments, &path, context)?;
    let filter_context = RelationshipFilterContext {
        endpoint,
        target_label: &target_label,
        path: &path,
    };
    let (target_where, relationship_where) =
        compile_relationship_filter_predicates(&filter_context, arguments, context)?;
    Ok(Some(build_relationship_exists_predicate(
        scope.variable,
        direction,
        relationship_type,
        target_label,
        target_where,
        relationship_where,
    )))
}

fn compile_relationship_existence_filter_variable(
    scope: GraphqlWhereScope<'_>,
    field_name: &str,
    value: &GraphqlVariableValue,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<Option<PredicateExpression>, CoreError> {
    let path = path.into();
    let GraphqlVariableValue::Object(arguments) = value else {
        return Err(unsupported(
            path,
            "GraphQL relationship existence filter variables must be objects",
        ));
    };
    let graph = scope
        .graph
        .ok_or_else(|| CoreError::internal("relationship filter graph missing"))?;
    let source_label = scope
        .label
        .ok_or_else(|| CoreError::internal("relationship filter source label missing"))?;
    let (direction, relationship_type, endpoint_argument) =
        compile_relationship_field_name(field_name, format!("{path}.name"))?;
    let endpoint = RelationshipEndpointContext {
        graph,
        source_label,
        relationship_type: &relationship_type,
        direction,
        endpoint_argument,
    };

    let target_label =
        compile_relationship_filter_variable_target_label(&endpoint, arguments, &path)?;
    let filter_context = RelationshipFilterContext {
        endpoint,
        target_label: &target_label,
        path: &path,
    };
    let (target_where, relationship_where) =
        compile_relationship_filter_variable_predicates(&filter_context, arguments, context)?;
    Ok(Some(build_relationship_exists_predicate(
        scope.variable,
        direction,
        relationship_type,
        target_label,
        target_where,
        relationship_where,
    )))
}

fn compile_relationship_filter_target_label(
    endpoint: &RelationshipEndpointContext<'_>,
    arguments: &BTreeMap<String, Value<'_, String>>,
    path: &str,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<String, CoreError> {
    let mut target_label = None;
    for (name, value) in arguments {
        let argument_path = format!("{path}.{name}");
        match name.as_str() {
            "to" | "from" | "label" => {
                if name != endpoint.endpoint_argument {
                    return Err(unsupported(
                        argument_path,
                        format!(
                            "GraphQL relationship filter '{}' requires '{}' instead of '{}'",
                            endpoint.relationship_type, endpoint.endpoint_argument, name
                        ),
                    ));
                }
                if target_label.is_some() {
                    return Err(unsupported(
                        argument_path,
                        format!(
                            "GraphQL relationship filter argument '{}' is duplicated",
                            endpoint.endpoint_argument
                        ),
                    ));
                }
                target_label = Some(compile_name_value(value, argument_path, context)?);
            }
            "where" | "relationshipWhere" => {}
            _ => {
                return Err(unsupported(
                    argument_path,
                    format!("unsupported GraphQL relationship filter argument '{name}'"),
                ));
            }
        }
    }
    match target_label {
        Some(target_label) => Ok(target_label),
        None => infer_relationship_target_label(
            endpoint,
            format!("{path}.{}", endpoint.endpoint_argument),
        ),
    }
}

fn compile_relationship_filter_variable_target_label(
    endpoint: &RelationshipEndpointContext<'_>,
    arguments: &BTreeMap<String, GraphqlVariableValue>,
    path: &str,
) -> Result<String, CoreError> {
    let mut target_label = None;
    for (name, value) in arguments {
        let argument_path = format!("{path}.{name}");
        match name.as_str() {
            "to" | "from" | "label" => {
                if name != endpoint.endpoint_argument {
                    return Err(unsupported(
                        argument_path,
                        format!(
                            "GraphQL relationship filter '{}' requires '{}' instead of '{}'",
                            endpoint.relationship_type, endpoint.endpoint_argument, name
                        ),
                    ));
                }
                if target_label.is_some() {
                    return Err(unsupported(
                        argument_path,
                        format!(
                            "GraphQL relationship filter argument '{}' is duplicated",
                            endpoint.endpoint_argument
                        ),
                    ));
                }
                target_label = Some(compile_variable_name_value(value, argument_path)?);
            }
            "where" | "relationshipWhere" => {}
            _ => {
                return Err(unsupported(
                    argument_path,
                    format!("unsupported GraphQL relationship filter argument '{name}'"),
                ));
            }
        }
    }
    match target_label {
        Some(target_label) => Ok(target_label),
        None => infer_relationship_target_label(
            endpoint,
            format!("{path}.{}", endpoint.endpoint_argument),
        ),
    }
}

fn compile_relationship_filter_predicates(
    filter: &RelationshipFilterContext<'_>,
    arguments: &BTreeMap<String, Value<'_, String>>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<(Option<PredicateExpression>, Option<PredicateExpression>), CoreError> {
    ensure_node_label(
        filter.endpoint.graph,
        filter.target_label,
        format!("{}.{}", filter.path, filter.endpoint.endpoint_argument),
    )?;
    ensure_relationship_mapping(
        filter.endpoint.graph,
        filter.endpoint.relationship_type,
        filter.endpoint.direction,
        filter.endpoint.source_label,
        filter.target_label,
        filter.path,
    )?;
    let target_variable = relationship_filter_target_variable(filter.target_label);
    let relationship_variable = relationship_filter_relationship_variable();
    let mut target_where = None;
    let mut relationship_where = None;
    for (name, value) in arguments {
        match name.as_str() {
            "where" => {
                target_where = compile_where_argument(
                    GraphqlWhereScope {
                        graph: Some(filter.endpoint.graph),
                        variable: &target_variable,
                        label: Some(filter.target_label),
                    },
                    value,
                    format!("{}.{}", filter.path, name),
                    context,
                )?;
            }
            "relationshipWhere" => {
                relationship_where = compile_where_argument(
                    GraphqlWhereScope::graph_variable(&relationship_variable),
                    value,
                    format!("{}.{}", filter.path, name),
                    context,
                )?;
            }
            "to" | "from" | "label" => {}
            _ => {
                unreachable!("relationship filter arguments validated before predicate compilation")
            }
        }
    }
    Ok((target_where, relationship_where))
}

fn compile_relationship_filter_variable_predicates(
    filter: &RelationshipFilterContext<'_>,
    arguments: &BTreeMap<String, GraphqlVariableValue>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<(Option<PredicateExpression>, Option<PredicateExpression>), CoreError> {
    ensure_node_label(
        filter.endpoint.graph,
        filter.target_label,
        format!("{}.{}", filter.path, filter.endpoint.endpoint_argument),
    )?;
    ensure_relationship_mapping(
        filter.endpoint.graph,
        filter.endpoint.relationship_type,
        filter.endpoint.direction,
        filter.endpoint.source_label,
        filter.target_label,
        filter.path,
    )?;
    let target_variable = relationship_filter_target_variable(filter.target_label);
    let relationship_variable = relationship_filter_relationship_variable();
    let mut target_where = None;
    let mut relationship_where = None;
    for (name, value) in arguments {
        match name.as_str() {
            "where" => {
                let GraphqlVariableValue::Object(object) = value else {
                    return Err(unsupported(
                        format!("{}.{}", filter.path, name),
                        "GraphQL relationship filter where must be an object",
                    ));
                };
                target_where = compile_where_variable_object(
                    GraphqlWhereScope {
                        graph: Some(filter.endpoint.graph),
                        variable: &target_variable,
                        label: Some(filter.target_label),
                    },
                    object,
                    format!("{}.{}", filter.path, name),
                    context,
                )?;
            }
            "relationshipWhere" => {
                let GraphqlVariableValue::Object(object) = value else {
                    return Err(unsupported(
                        format!("{}.{}", filter.path, name),
                        "GraphQL relationship filter relationshipWhere must be an object",
                    ));
                };
                relationship_where = compile_where_variable_object(
                    GraphqlWhereScope::graph_variable(&relationship_variable),
                    object,
                    format!("{}.{}", filter.path, name),
                    context,
                )?;
            }
            "to" | "from" | "label" => {}
            _ => {
                unreachable!("relationship filter arguments validated before predicate compilation")
            }
        }
    }
    Ok((target_where, relationship_where))
}

struct RelationshipFilterContext<'a> {
    endpoint: RelationshipEndpointContext<'a>,
    target_label: &'a str,
    path: &'a str,
}

fn build_relationship_exists_predicate(
    source_variable: &str,
    direction: Direction,
    relationship_type: String,
    target_label: String,
    target_where: Option<PredicateExpression>,
    relationship_where: Option<PredicateExpression>,
) -> PredicateExpression {
    let target_variable = relationship_filter_target_variable(&target_label);
    let relationship_variable = relationship_where
        .as_ref()
        .map(|_| relationship_filter_relationship_variable());
    let predicate = append_optional_and(target_where, relationship_where);
    PredicateExpression::ExistsPattern(ExistsPatternPredicate {
        nodes: vec![NodePattern {
            variable: target_variable.clone(),
            label: target_label,
        }],
        relationships: vec![RelationshipPattern {
            variable: relationship_variable,
            relationship_type,
            left: source_variable.to_string(),
            direction,
            right: target_variable,
        }],
        predicates: Vec::new(),
        predicate: predicate.map(Box::new),
    })
}

fn relationship_filter_target_variable(label: &str) -> String {
    format!("graphql_exists_{}", variable_for_label(label))
}

fn relationship_filter_relationship_variable() -> String {
    "graphql_exists_relationship".to_string()
}

#[derive(Debug, Clone, Copy)]
pub(super) enum GraphqlBooleanOperator {
    And,
    Or,
    Xor,
    Not,
}

pub(super) fn graphql_boolean_operator(name: &str) -> Option<GraphqlBooleanOperator> {
    match name {
        "and" | "AND" | "_and" => Some(GraphqlBooleanOperator::And),
        "or" | "OR" | "_or" => Some(GraphqlBooleanOperator::Or),
        "xor" | "XOR" | "_xor" => Some(GraphqlBooleanOperator::Xor),
        "not" | "NOT" | "_not" => Some(GraphqlBooleanOperator::Not),
        _ => None,
    }
}

fn compile_where_boolean_operator(
    scope: GraphqlWhereScope<'_>,
    operator: GraphqlBooleanOperator,
    value: &Value<'_, String>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<Option<PredicateExpression>, CoreError> {
    let path = path.into();
    if let Value::Variable(variable) = value {
        return compile_where_variable_boolean_operator(
            scope,
            operator,
            context.parameter_value(variable, path.clone())?,
            path,
            context,
        );
    }
    match operator {
        GraphqlBooleanOperator::And | GraphqlBooleanOperator::Or => {
            let Value::List(items) = value else {
                return Err(unsupported(
                    path,
                    "GraphQL where and/or/xor operators must contain a list of objects",
                ));
            };
            if items.is_empty() {
                return Err(unsupported(
                    path,
                    "GraphQL where and/or operators require at least one object",
                ));
            }
            let mut expression = None;
            for (index, item) in items.iter().enumerate() {
                let next =
                    compile_where_argument(scope, item, format!("{path}[{index}]"), context)?;
                expression = match operator {
                    GraphqlBooleanOperator::And => append_optional_and(expression, next),
                    GraphqlBooleanOperator::Or => append_optional_or(expression, next),
                    GraphqlBooleanOperator::Xor => unreachable!("XOR is handled separately"),
                    GraphqlBooleanOperator::Not => unreachable!("NOT is handled separately"),
                };
            }
            expression
                .map(Some)
                .ok_or_else(|| unsupported(path, "GraphQL where boolean list was empty"))
        }
        GraphqlBooleanOperator::Xor => {
            let Value::List(items) = value else {
                return Err(unsupported(
                    path,
                    "GraphQL where and/or/xor operators must contain a list of objects",
                ));
            };
            let [left_item, right_item] = items.as_slice() else {
                return Err(unsupported(
                    path,
                    "GraphQL where xor operator requires exactly two objects",
                ));
            };
            let left = compile_where_argument(scope, left_item, format!("{path}[0]"), context)?
                .ok_or_else(|| {
                    unsupported(
                        format!("{path}[0]"),
                        "GraphQL where xor operands must not be empty",
                    )
                })?;
            let right = compile_where_argument(scope, right_item, format!("{path}[1]"), context)?
                .ok_or_else(|| {
                unsupported(
                    format!("{path}[1]"),
                    "GraphQL where xor operands must not be empty",
                )
            })?;
            Ok(Some(PredicateExpression::Xor {
                left: Box::new(left),
                right: Box::new(right),
            }))
        }
        GraphqlBooleanOperator::Not => {
            let expression = compile_where_argument(scope, value, path.clone(), context)?
                .ok_or_else(|| unsupported(path, "GraphQL where not requires an object"))?;
            Ok(Some(PredicateExpression::Not {
                expression: Box::new(expression),
            }))
        }
    }
}

fn compile_where_variable_boolean_operator(
    scope: GraphqlWhereScope<'_>,
    operator: GraphqlBooleanOperator,
    value: &GraphqlVariableValue,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<Option<PredicateExpression>, CoreError> {
    let path = path.into();
    match operator {
        GraphqlBooleanOperator::And | GraphqlBooleanOperator::Or => {
            let GraphqlVariableValue::ObjectList(items) = value else {
                return Err(unsupported(
                    path,
                    "GraphQL where and/or/xor operators must contain a list of objects",
                ));
            };
            if items.is_empty() {
                return Err(unsupported(
                    path,
                    "GraphQL where and/or operators require at least one object",
                ));
            }
            let mut expression = None;
            for (index, item) in items.iter().enumerate() {
                let next = compile_where_variable_object(
                    scope,
                    item,
                    format!("{path}[{index}]"),
                    context,
                )?;
                expression = match operator {
                    GraphqlBooleanOperator::And => append_optional_and(expression, next),
                    GraphqlBooleanOperator::Or => append_optional_or(expression, next),
                    GraphqlBooleanOperator::Xor => unreachable!("XOR is handled separately"),
                    GraphqlBooleanOperator::Not => unreachable!("NOT is handled separately"),
                };
            }
            expression
                .map(Some)
                .ok_or_else(|| unsupported(path, "GraphQL where boolean list was empty"))
        }
        GraphqlBooleanOperator::Xor => {
            let GraphqlVariableValue::ObjectList(items) = value else {
                return Err(unsupported(
                    path,
                    "GraphQL where and/or/xor operators must contain a list of objects",
                ));
            };
            let [left_item, right_item] = items.as_slice() else {
                return Err(unsupported(
                    path,
                    "GraphQL where xor operator requires exactly two objects",
                ));
            };
            let left =
                compile_where_variable_object(scope, left_item, format!("{path}[0]"), context)?
                    .ok_or_else(|| {
                        unsupported(
                            format!("{path}[0]"),
                            "GraphQL where xor operands must not be empty",
                        )
                    })?;
            let right =
                compile_where_variable_object(scope, right_item, format!("{path}[1]"), context)?
                    .ok_or_else(|| {
                        unsupported(
                            format!("{path}[1]"),
                            "GraphQL where xor operands must not be empty",
                        )
                    })?;
            Ok(Some(PredicateExpression::Xor {
                left: Box::new(left),
                right: Box::new(right),
            }))
        }
        GraphqlBooleanOperator::Not => {
            let GraphqlVariableValue::Object(object) = value else {
                return Err(unsupported(path, "GraphQL where not requires an object"));
            };
            let expression =
                compile_where_variable_object(scope, object, path.clone(), context)?
                    .ok_or_else(|| unsupported(path, "GraphQL where not requires an object"))?;
            Ok(Some(PredicateExpression::Not {
                expression: Box::new(expression),
            }))
        }
    }
}

fn compile_where_property_conditions(
    graph_variable: &str,
    property: &str,
    condition: &Value<'_, String>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<Option<PredicateExpression>, CoreError> {
    let path = path.into();
    if let Value::Variable(variable) = condition {
        return match context.parameter_value(variable, path.clone())? {
            GraphqlVariableValue::Object(object) => {
                compile_where_variable_property_conditions(graph_variable, property, object, path)
            }
            GraphqlVariableValue::Literal(literal) => Ok(Some(compile_where_shorthand_expression(
                graph_variable,
                property,
                comparison_rhs_from_literal(literal.clone()),
                path,
            )?)),
            GraphqlVariableValue::List(_) | GraphqlVariableValue::ObjectList(_) => {
                Err(unsupported(
                    path,
                    format!(
                        "GraphQL variable '${variable}' must be a scalar literal or property condition object"
                    ),
                ))
            }
        };
    }
    let Value::Object(operators) = condition else {
        return Ok(Some(compile_where_shorthand_expression(
            graph_variable,
            property,
            compile_comparison_rhs(condition, path.clone(), context)?,
            path,
        )?));
    };
    let mut expression = None;
    for (operator, value) in operators {
        let predicate = compile_where_operator_expression(
            graph_variable,
            property,
            operator,
            value,
            format!("{path}.{operator}"),
            context,
        )?;
        expression = append_optional_and(expression, Some(predicate));
    }
    Ok(expression)
}

fn compile_where_variable_property_conditions(
    graph_variable: &str,
    property: &str,
    operators: &BTreeMap<String, GraphqlVariableValue>,
    path: impl Into<String>,
) -> Result<Option<PredicateExpression>, CoreError> {
    let path = path.into();
    let mut expression = None;
    for (operator, value) in operators {
        let predicate = compile_where_variable_operator_expression(
            graph_variable,
            property,
            operator,
            value,
            format!("{path}.{operator}"),
        )?;
        expression = append_optional_and(expression, Some(predicate));
    }
    Ok(expression)
}

pub(super) fn append_where_predicate(
    plan: &mut GraphPlan,
    expression: Option<PredicateExpression>,
) {
    let Some(expression) = expression else {
        return;
    };
    if is_conjunctive_property_expression(&expression) {
        append_conjunctive_property_expression(expression, &mut plan.predicates);
    } else {
        plan.predicate = Some(match plan.predicate.take() {
            Some(existing) => PredicateExpression::And {
                left: Box::new(existing),
                right: Box::new(expression),
            },
            None => expression,
        });
    }
}

fn append_optional_and(
    expression: Option<PredicateExpression>,
    next: Option<PredicateExpression>,
) -> Option<PredicateExpression> {
    match (expression, next) {
        (Some(left), Some(right)) => Some(PredicateExpression::And {
            left: Box::new(left),
            right: Box::new(right),
        }),
        (Some(expression), None) | (None, Some(expression)) => Some(expression),
        (None, None) => None,
    }
}

fn append_optional_or(
    expression: Option<PredicateExpression>,
    next: Option<PredicateExpression>,
) -> Option<PredicateExpression> {
    match (expression, next) {
        (Some(left), Some(right)) => Some(PredicateExpression::Or {
            left: Box::new(left),
            right: Box::new(right),
        }),
        (Some(expression), None) | (None, Some(expression)) => Some(expression),
        (None, None) => None,
    }
}

fn is_conjunctive_property_expression(expression: &PredicateExpression) -> bool {
    match expression {
        PredicateExpression::Comparison(_) => true,
        PredicateExpression::And { left, right } => {
            is_conjunctive_property_expression(left) && is_conjunctive_property_expression(right)
        }
        PredicateExpression::Boolean(_)
        | PredicateExpression::KeyComparison(_)
        | PredicateExpression::ElementIdComparison(_)
        | PredicateExpression::Presence(_)
        | PredicateExpression::PropertyKeyMembership(_)
        | PredicateExpression::ExistsPattern(_)
        | PredicateExpression::ScalarComparison(_)
        | PredicateExpression::Or { .. }
        | PredicateExpression::Xor { .. }
        | PredicateExpression::Not { .. } => false,
    }
}

fn append_conjunctive_property_expression(
    expression: PredicateExpression,
    predicates: &mut Vec<PropertyPredicate>,
) {
    match expression {
        PredicateExpression::Comparison(predicate) => predicates.push(predicate),
        PredicateExpression::And { left, right } => {
            append_conjunctive_property_expression(*left, predicates);
            append_conjunctive_property_expression(*right, predicates);
        }
        PredicateExpression::Boolean(_)
        | PredicateExpression::KeyComparison(_)
        | PredicateExpression::ElementIdComparison(_)
        | PredicateExpression::Presence(_)
        | PredicateExpression::PropertyKeyMembership(_)
        | PredicateExpression::ExistsPattern(_)
        | PredicateExpression::ScalarComparison(_)
        | PredicateExpression::Or { .. }
        | PredicateExpression::Xor { .. }
        | PredicateExpression::Not { .. } => {
            unreachable!("non-conjunctive GraphQL predicate reached conjunctive appender")
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GraphqlWhereOperator {
    Comparison(ComparisonOperator),
    RegexMatch,
    In,
    IsNull,
    IsNotNull,
    NegatedComparison(ComparisonOperator),
    NegatedRegexMatch,
    NotIn,
}

pub(super) fn classify_graphql_where_operator(operator: &str) -> Option<GraphqlWhereOperator> {
    match operator {
        "eq" | "equals" => Some(GraphqlWhereOperator::Comparison(ComparisonOperator::Equal)),
        "ne" | "neq" | "notEq" | "notEqual" | "notEquals" => Some(
            GraphqlWhereOperator::Comparison(ComparisonOperator::NotEqual),
        ),
        "gt" | "greaterThan" => Some(GraphqlWhereOperator::Comparison(
            ComparisonOperator::GreaterThan,
        )),
        "gte" | "ge" | "greaterThanEqual" | "greaterThanOrEqual" => Some(
            GraphqlWhereOperator::Comparison(ComparisonOperator::GreaterThanOrEqual),
        ),
        "lt" | "lessThan" => Some(GraphqlWhereOperator::Comparison(
            ComparisonOperator::LessThan,
        )),
        "lte" | "le" | "lessThanEqual" | "lessThanOrEqual" => Some(
            GraphqlWhereOperator::Comparison(ComparisonOperator::LessThanOrEqual),
        ),
        "startsWith" | "starts_with" => Some(GraphqlWhereOperator::Comparison(
            ComparisonOperator::StartsWith,
        )),
        "endsWith" | "ends_with" => Some(GraphqlWhereOperator::Comparison(
            ComparisonOperator::EndsWith,
        )),
        "contains" => Some(GraphqlWhereOperator::Comparison(
            ComparisonOperator::Contains,
        )),
        "notStartsWith" | "not_starts_with" => Some(GraphqlWhereOperator::NegatedComparison(
            ComparisonOperator::StartsWith,
        )),
        "notEndsWith" | "not_ends_with" => Some(GraphqlWhereOperator::NegatedComparison(
            ComparisonOperator::EndsWith,
        )),
        "notContains" | "not_contains" => Some(GraphqlWhereOperator::NegatedComparison(
            ComparisonOperator::Contains,
        )),
        "matches" | "regex" => Some(GraphqlWhereOperator::RegexMatch),
        "notMatches" | "notRegex" | "not_regex" => Some(GraphqlWhereOperator::NegatedRegexMatch),
        "in" => Some(GraphqlWhereOperator::In),
        "notIn" | "not_in" => Some(GraphqlWhereOperator::NotIn),
        "isNull" | "is_null" => Some(GraphqlWhereOperator::IsNull),
        "isNotNull" | "is_not_null" => Some(GraphqlWhereOperator::IsNotNull),
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum GraphqlWhereTarget {
    Property(PropertyRef),
    Key { variable: String },
    ElementId { variable: String },
}

fn graphql_where_target(variable: &str, property: &str) -> GraphqlWhereTarget {
    match property {
        "_id" => GraphqlWhereTarget::Key {
            variable: variable.to_string(),
        },
        "_elementId" => GraphqlWhereTarget::ElementId {
            variable: variable.to_string(),
        },
        _ => GraphqlWhereTarget::Property(PropertyRef {
            variable: variable.to_string(),
            property: property.to_string(),
        }),
    }
}

fn comparison_expression(
    target: GraphqlWhereTarget,
    operator: ComparisonOperator,
    rhs: PredicateRhs,
    path: &str,
) -> Result<PredicateExpression, CoreError> {
    if matches!(target, GraphqlWhereTarget::Key { .. })
        && matches!(
            operator,
            ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains
                | ComparisonOperator::RegexMatch
        )
    {
        return Err(unsupported(
            path,
            "GraphQL _id filters do not support string predicates; use _elementId for string identity filters",
        ));
    }
    Ok(match target {
        GraphqlWhereTarget::Property(property) => {
            PredicateExpression::Comparison(PropertyPredicate {
                property,
                operator,
                rhs,
            })
        }
        GraphqlWhereTarget::Key { variable } => PredicateExpression::KeyComparison(KeyPredicate {
            variable,
            operator,
            rhs,
        }),
        GraphqlWhereTarget::ElementId { variable } => {
            PredicateExpression::ElementIdComparison(ElementIdPredicate {
                variable,
                operator,
                rhs,
            })
        }
    })
}

fn negated_comparison_expression(
    target: GraphqlWhereTarget,
    operator: ComparisonOperator,
    rhs: PredicateRhs,
    path: &str,
) -> Result<PredicateExpression, CoreError> {
    Ok(PredicateExpression::Not {
        expression: Box::new(comparison_expression(target, operator, rhs, path)?),
    })
}

fn compile_where_shorthand_expression(
    variable: &str,
    property: &str,
    rhs: PredicateRhs,
    path: impl Into<String>,
) -> Result<PredicateExpression, CoreError> {
    let path = path.into();
    comparison_expression(
        graphql_where_target(variable, property),
        ComparisonOperator::Equal,
        rhs,
        &path,
    )
}

fn compile_where_operator_expression(
    variable: &str,
    property: &str,
    operator: &str,
    value: &Value<'_, String>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<PredicateExpression, CoreError> {
    let path = path.into();
    let target = graphql_where_target(variable, property);
    match classify_graphql_where_operator(operator) {
        Some(GraphqlWhereOperator::Comparison(operator)) => Ok(comparison_expression(
            target,
            operator,
            compile_operator_comparison_rhs(operator, value, path.clone(), context)?,
            &path,
        )?),
        Some(GraphqlWhereOperator::RegexMatch) => Ok(comparison_expression(
            target,
            ComparisonOperator::RegexMatch,
            PredicateRhs::Literal(compile_regex_literal(value, path.clone(), context)?),
            &path,
        )?),
        Some(GraphqlWhereOperator::In) => Ok(comparison_expression(
            target,
            ComparisonOperator::In,
            list_rhs_from_literals(compile_literal_list(value, path.clone(), context)?),
            &path,
        )?),
        Some(GraphqlWhereOperator::IsNull) => {
            let is_null = compile_boolean(value, path.clone(), "isNull", context)?;
            Ok(comparison_expression(
                target,
                if is_null {
                    ComparisonOperator::Equal
                } else {
                    ComparisonOperator::NotEqual
                },
                PredicateRhs::Literal(Literal::Null),
                &path,
            )?)
        }
        Some(GraphqlWhereOperator::IsNotNull) => {
            let is_not_null = compile_boolean(value, path.clone(), "isNotNull", context)?;
            Ok(comparison_expression(
                target,
                if is_not_null {
                    ComparisonOperator::NotEqual
                } else {
                    ComparisonOperator::Equal
                },
                PredicateRhs::Literal(Literal::Null),
                &path,
            )?)
        }
        Some(GraphqlWhereOperator::NegatedComparison(operator)) => {
            Ok(negated_comparison_expression(
                target,
                operator,
                compile_operator_comparison_rhs(operator, value, path.clone(), context)?,
                &path,
            )?)
        }
        Some(GraphqlWhereOperator::NegatedRegexMatch) => Ok(negated_comparison_expression(
            target,
            ComparisonOperator::RegexMatch,
            PredicateRhs::Literal(compile_regex_literal(value, path.clone(), context)?),
            &path,
        )?),
        Some(GraphqlWhereOperator::NotIn) => Ok(negated_comparison_expression(
            target,
            ComparisonOperator::In,
            list_rhs_from_literals(compile_literal_list(value, path.clone(), context)?),
            &path,
        )?),
        None => Err(unsupported(
            path,
            format!("unsupported GraphQL where operator '{operator}'"),
        )),
    }
}

fn compile_where_variable_operator_expression(
    variable: &str,
    property: &str,
    operator: &str,
    value: &GraphqlVariableValue,
    path: impl Into<String>,
) -> Result<PredicateExpression, CoreError> {
    let path = path.into();
    let target = graphql_where_target(variable, property);
    match classify_graphql_where_operator(operator) {
        Some(GraphqlWhereOperator::Comparison(operator)) => Ok(comparison_expression(
            target,
            operator,
            compile_variable_operator_comparison_rhs(operator, value, path.clone())?,
            &path,
        )?),
        Some(GraphqlWhereOperator::RegexMatch) => Ok(comparison_expression(
            target,
            ComparisonOperator::RegexMatch,
            PredicateRhs::Literal(compile_variable_regex_literal(value, path.clone())?),
            &path,
        )?),
        Some(GraphqlWhereOperator::In) => Ok(comparison_expression(
            target,
            ComparisonOperator::In,
            list_rhs_from_literals(compile_variable_literal_list(value, path.clone())?),
            &path,
        )?),
        Some(GraphqlWhereOperator::IsNull) => {
            let is_null = compile_variable_boolean(value, path.clone(), "isNull")?;
            Ok(comparison_expression(
                target,
                if is_null {
                    ComparisonOperator::Equal
                } else {
                    ComparisonOperator::NotEqual
                },
                PredicateRhs::Literal(Literal::Null),
                &path,
            )?)
        }
        Some(GraphqlWhereOperator::IsNotNull) => {
            let is_not_null = compile_variable_boolean(value, path.clone(), "isNotNull")?;
            Ok(comparison_expression(
                target,
                if is_not_null {
                    ComparisonOperator::NotEqual
                } else {
                    ComparisonOperator::Equal
                },
                PredicateRhs::Literal(Literal::Null),
                &path,
            )?)
        }
        Some(GraphqlWhereOperator::NegatedComparison(operator)) => {
            Ok(negated_comparison_expression(
                target,
                operator,
                compile_variable_operator_comparison_rhs(operator, value, path.clone())?,
                &path,
            )?)
        }
        Some(GraphqlWhereOperator::NegatedRegexMatch) => Ok(negated_comparison_expression(
            target,
            ComparisonOperator::RegexMatch,
            PredicateRhs::Literal(compile_variable_regex_literal(value, path.clone())?),
            &path,
        )?),
        Some(GraphqlWhereOperator::NotIn) => Ok(negated_comparison_expression(
            target,
            ComparisonOperator::In,
            list_rhs_from_literals(compile_variable_literal_list(value, path.clone())?),
            &path,
        )?),
        None => Err(unsupported(
            path,
            format!("unsupported GraphQL where operator '{operator}'"),
        )),
    }
}

fn compile_variable_literal(
    value: &GraphqlVariableValue,
    path: impl Into<String>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    match value {
        GraphqlVariableValue::Literal(value) => Ok(value.clone()),
        GraphqlVariableValue::List(_)
        | GraphqlVariableValue::Object(_)
        | GraphqlVariableValue::ObjectList(_) => Err(unsupported(
            path,
            "GraphQL variable value must be a scalar literal",
        )),
    }
}

fn comparison_rhs_from_literal(literal: Literal) -> PredicateRhs {
    match literal {
        Literal::String(source) => PredicateRhs::TemporalCoercion { source },
        literal => PredicateRhs::Literal(literal),
    }
}

fn operator_comparison_rhs_from_literal(
    operator: ComparisonOperator,
    literal: Literal,
) -> PredicateRhs {
    if matches!(
        operator,
        ComparisonOperator::StartsWith
            | ComparisonOperator::EndsWith
            | ComparisonOperator::Contains
    ) {
        PredicateRhs::Literal(literal)
    } else {
        comparison_rhs_from_literal(literal)
    }
}

fn compile_comparison_rhs(
    value: &Value<'_, String>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<PredicateRhs, CoreError> {
    compile_literal(value, path, context).map(comparison_rhs_from_literal)
}

fn compile_operator_comparison_rhs(
    operator: ComparisonOperator,
    value: &Value<'_, String>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<PredicateRhs, CoreError> {
    compile_literal(value, path, context)
        .map(|literal| operator_comparison_rhs_from_literal(operator, literal))
}

fn compile_variable_operator_comparison_rhs(
    operator: ComparisonOperator,
    value: &GraphqlVariableValue,
    path: impl Into<String>,
) -> Result<PredicateRhs, CoreError> {
    compile_variable_literal(value, path)
        .map(|literal| operator_comparison_rhs_from_literal(operator, literal))
}

fn list_rhs_from_literals(literals: Vec<Literal>) -> PredicateRhs {
    if !literals.is_empty()
        && literals
            .iter()
            .all(|literal| matches!(literal, Literal::String(_)))
    {
        PredicateRhs::TemporalCoercionList(
            literals
                .into_iter()
                .map(|literal| match literal {
                    Literal::String(source) => source,
                    _ => unreachable!("all-String checked"),
                })
                .collect(),
        )
    } else {
        PredicateRhs::List(literals)
    }
}

fn compile_variable_literal_list(
    value: &GraphqlVariableValue,
    path: impl Into<String>,
) -> Result<Vec<Literal>, CoreError> {
    let path = path.into();
    match value {
        GraphqlVariableValue::List(values) => Ok(values.clone()),
        GraphqlVariableValue::Literal(_)
        | GraphqlVariableValue::Object(_)
        | GraphqlVariableValue::ObjectList(_) => Err(unsupported(
            path,
            "GraphQL variable value must be a scalar-list literal",
        )),
    }
}

fn compile_variable_regex_literal(
    value: &GraphqlVariableValue,
    path: impl Into<String>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    let literal = compile_variable_literal(value, path.clone())?;
    let Literal::String(pattern) = &literal else {
        return Err(unsupported(
            path,
            "GraphQL regex filters require a string literal",
        ));
    };
    Regex::new(pattern)
        .map_err(|error| unsupported(path, format!("invalid GraphQL regex literal: {error}")))?;
    Ok(literal)
}

fn compile_variable_boolean(
    value: &GraphqlVariableValue,
    path: impl Into<String>,
    name: &str,
) -> Result<bool, CoreError> {
    let path = path.into();
    let GraphqlVariableValue::Literal(Literal::Boolean(value)) = value else {
        return Err(unsupported(
            path,
            format!("GraphQL variable value for {name} must be a boolean"),
        ));
    };
    Ok(*value)
}

fn compile_regex_literal(
    value: &Value<'_, String>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    let literal = compile_literal(value, path.clone(), context)?;
    let Literal::String(pattern) = &literal else {
        return Err(unsupported(
            path,
            "GraphQL regex filters require a string literal",
        ));
    };
    Regex::new(pattern)
        .map_err(|error| unsupported(path, format!("invalid GraphQL regex literal: {error}")))?;
    Ok(literal)
}

fn compile_literal_list(
    value: &Value<'_, String>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<Vec<Literal>, CoreError> {
    let path = path.into();
    if let Value::Variable(variable) = value {
        return match context.parameter_value(variable, path.clone())? {
            GraphqlVariableValue::List(values) => Ok(values.clone()),
            GraphqlVariableValue::Literal(_)
            | GraphqlVariableValue::Object(_)
            | GraphqlVariableValue::ObjectList(_) => Err(unsupported(
                path,
                format!("GraphQL variable '${variable}' must be a scalar-list literal"),
            )),
        };
    }
    let Value::List(items) = value else {
        return Err(unsupported(
            path,
            "GraphQL IN values must be a literal list",
        ));
    };
    items
        .iter()
        .enumerate()
        .map(|(index, value)| compile_literal(value, format!("{path}[{index}]"), context))
        .collect()
}
