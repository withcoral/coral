//! GraphQL `orderBy` argument compilation.

use std::collections::BTreeMap;

use graphql_parser::query::Value;

use super::super::ir::{
    Literal, NullOrder, OrderDirection, OrderExpression, OrderKey, PropertyRef,
};
use super::{GraphqlCompileContext, GraphqlVariableValue, compile_name_value, unsupported};
use crate::CoreError;

pub(super) fn compile_order_by_argument(
    variable: &str,
    value: &Value<'_, String>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<Vec<OrderKey>, CoreError> {
    let path = path.into();
    match value {
        Value::Variable(graphql_variable) => {
            match context.parameter_value(graphql_variable, path.clone())? {
                GraphqlVariableValue::Object(object) => {
                    compile_order_by_variable_object(variable, object, path)
                }
                GraphqlVariableValue::ObjectList(items) => {
                    let mut order_keys = Vec::with_capacity(items.len());
                    for (index, object) in items.iter().enumerate() {
                        order_keys.extend(compile_order_by_variable_object(
                            variable,
                            object,
                            format!("{path}[{index}]"),
                        )?);
                    }
                    Ok(order_keys)
                }
                GraphqlVariableValue::List(values) if values.is_empty() => Ok(Vec::new()),
                GraphqlVariableValue::Literal(_) | GraphqlVariableValue::List(_) => {
                    Err(unsupported(
                        path,
                        format!(
                            "GraphQL variable '${graphql_variable}' must be an orderBy object or list of objects"
                        ),
                    ))
                }
            }
        }
        Value::Object(_) => compile_order_by_object(variable, value, path, context),
        Value::List(items) => {
            let mut order_keys = Vec::with_capacity(items.len());
            for (index, value) in items.iter().enumerate() {
                order_keys.extend(compile_order_by_object(
                    variable,
                    value,
                    format!("{path}[{index}]"),
                    context,
                )?);
            }
            Ok(order_keys)
        }
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
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<Vec<OrderKey>, CoreError> {
    let path = path.into();
    let Value::Object(object) = value else {
        return Err(unsupported(path, "GraphQL orderBy entries must be objects"));
    };
    if !object.contains_key("field")
        && !object.contains_key("direction")
        && !object.contains_key("nulls")
    {
        return compile_order_by_shorthand_object(variable, object, path, context);
    }
    for name in object.keys() {
        if name != "field" && name != "direction" && name != "nulls" {
            return Err(unsupported(
                format!("{path}.{name}"),
                format!("unsupported GraphQL orderBy key '{name}'"),
            ));
        }
    }
    let field_value = object
        .get("field")
        .ok_or_else(|| unsupported(format!("{path}.field"), "GraphQL orderBy requires field"))?;
    let field = compile_name_value(field_value, format!("{path}.field"), context)?;
    let direction = object
        .get("direction")
        .map_or(Ok(OrderDirection::Ascending), |value| {
            compile_order_direction(value, format!("{path}.direction"), context)
        })?;
    let nulls = object
        .get("nulls")
        .map(|value| compile_null_order(value, format!("{path}.nulls"), context))
        .transpose()?;
    Ok(vec![OrderKey {
        expression: graphql_order_expression(variable, &field),
        direction,
        nulls,
    }])
}

fn compile_order_by_shorthand_object(
    variable: &str,
    object: &BTreeMap<String, Value<'_, String>>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<Vec<OrderKey>, CoreError> {
    let path = path.into();
    if object.len() != 1 {
        return Err(unsupported(
            path,
            "GraphQL shorthand orderBy entries must contain exactly one field",
        ));
    }
    let (field, direction_value) = object
        .iter()
        .next()
        .expect("shorthand orderBy object length was checked");
    Ok(vec![OrderKey {
        expression: graphql_order_expression(variable, field),
        direction: compile_order_direction(direction_value, format!("{path}.{field}"), context)?,
        nulls: None,
    }])
}

fn compile_order_by_variable_object(
    variable: &str,
    object: &BTreeMap<String, GraphqlVariableValue>,
    path: impl Into<String>,
) -> Result<Vec<OrderKey>, CoreError> {
    let path = path.into();
    if !object.contains_key("field")
        && !object.contains_key("direction")
        && !object.contains_key("nulls")
    {
        return compile_order_by_variable_shorthand_object(variable, object, path);
    }
    for name in object.keys() {
        if name != "field" && name != "direction" && name != "nulls" {
            return Err(unsupported(
                format!("{path}.{name}"),
                format!("unsupported GraphQL orderBy key '{name}'"),
            ));
        }
    }
    let field_value = object
        .get("field")
        .ok_or_else(|| unsupported(format!("{path}.field"), "GraphQL orderBy requires field"))?;
    let field = compile_variable_name_value(field_value, format!("{path}.field"))?;
    let direction = object
        .get("direction")
        .map_or(Ok(OrderDirection::Ascending), |value| {
            compile_variable_order_direction(value, format!("{path}.direction"))
        })?;
    let nulls = object
        .get("nulls")
        .map(|value| compile_variable_null_order(value, format!("{path}.nulls")))
        .transpose()?;
    Ok(vec![OrderKey {
        expression: graphql_order_expression(variable, &field),
        direction,
        nulls,
    }])
}

fn compile_order_by_variable_shorthand_object(
    variable: &str,
    object: &BTreeMap<String, GraphqlVariableValue>,
    path: impl Into<String>,
) -> Result<Vec<OrderKey>, CoreError> {
    let path = path.into();
    if object.len() != 1 {
        return Err(unsupported(
            path,
            "GraphQL shorthand orderBy variable entries must contain exactly one field",
        ));
    }
    let (field, direction_value) = object
        .iter()
        .next()
        .expect("shorthand orderBy variable object length was checked");
    Ok(vec![OrderKey {
        expression: graphql_order_expression(variable, field),
        direction: compile_variable_order_direction(direction_value, format!("{path}.{field}"))?,
        nulls: None,
    }])
}

fn graphql_order_expression(variable: &str, field: &str) -> OrderExpression {
    match field {
        "_id" => OrderExpression::Key {
            variable: variable.to_string(),
        },
        "_elementId" => OrderExpression::ElementId {
            variable: variable.to_string(),
        },
        _ => OrderExpression::Property(PropertyRef {
            variable: variable.to_string(),
            property: field.to_string(),
        }),
    }
}

fn compile_order_direction(
    value: &Value<'_, String>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<OrderDirection, CoreError> {
    let path = path.into();
    let direction = compile_name_value(value, path.clone(), context)?;
    compile_order_direction_name(&direction, path)
}

fn compile_null_order(
    value: &Value<'_, String>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<NullOrder, CoreError> {
    let path = path.into();
    let nulls = compile_name_value(value, path.clone(), context)?;
    compile_null_order_name(&nulls, path)
}

pub(super) fn compile_variable_name_value(
    value: &GraphqlVariableValue,
    path: impl Into<String>,
) -> Result<String, CoreError> {
    let path = path.into();
    match value {
        GraphqlVariableValue::Literal(Literal::String(value)) => Ok(value.clone()),
        GraphqlVariableValue::Literal(_)
        | GraphqlVariableValue::List(_)
        | GraphqlVariableValue::Object(_)
        | GraphqlVariableValue::ObjectList(_) => Err(unsupported(
            path,
            "GraphQL variable value must be a string or enum name",
        )),
    }
}

fn compile_variable_order_direction(
    value: &GraphqlVariableValue,
    path: impl Into<String>,
) -> Result<OrderDirection, CoreError> {
    let path = path.into();
    let direction = compile_variable_name_value(value, path.clone())?;
    compile_order_direction_name(&direction, path)
}

fn compile_variable_null_order(
    value: &GraphqlVariableValue,
    path: impl Into<String>,
) -> Result<NullOrder, CoreError> {
    let path = path.into();
    let nulls = compile_variable_name_value(value, path.clone())?;
    compile_null_order_name(&nulls, path)
}

pub(crate) fn compile_order_direction_name(
    direction: &str,
    path: impl Into<String>,
) -> Result<OrderDirection, CoreError> {
    let path = path.into();
    if direction.eq_ignore_ascii_case("ASC") || direction.eq_ignore_ascii_case("ASCENDING") {
        return Ok(OrderDirection::Ascending);
    }
    if direction.eq_ignore_ascii_case("DESC") || direction.eq_ignore_ascii_case("DESCENDING") {
        return Ok(OrderDirection::Descending);
    }
    Err(unsupported(
        path,
        "GraphQL orderBy direction must be ASC, ASCENDING, DESC, or DESCENDING",
    ))
}

pub(crate) fn compile_null_order_name(
    nulls: &str,
    path: impl Into<String>,
) -> Result<NullOrder, CoreError> {
    let path = path.into();
    if nulls.eq_ignore_ascii_case("FIRST") || nulls.eq_ignore_ascii_case("NULLS_FIRST") {
        return Ok(NullOrder::First);
    }
    if nulls.eq_ignore_ascii_case("LAST") || nulls.eq_ignore_ascii_case("NULLS_LAST") {
        return Ok(NullOrder::Last);
    }
    Err(unsupported(
        path,
        "GraphQL orderBy nulls must be FIRST, LAST, NULLS_FIRST, or NULLS_LAST",
    ))
}
