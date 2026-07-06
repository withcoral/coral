use graphql_parser::query::{Field, Value};
use ordered_float::OrderedFloat;

use super::super::graphql_aggregate::{
    GraphqlAggregateArgumentSpec, GraphqlAggregateFieldSpec, GraphqlAggregateFunctionSpec,
    GraphqlAggregateReturnType, graphql_property_aggregate_field,
};
use super::super::ir::{AggregateFunction, AggregateTarget, Literal, Projection, PropertyRef};
use super::response_signatures::projection_alias;
use super::{GraphqlCompileContext, NodeContext, compile_literal, compile_name_value, unsupported};
use crate::CoreError;

pub(super) fn is_node_aggregate_field(name: &str) -> bool {
    name == "_count" || graphql_property_aggregate_field(name).is_some()
}

pub(super) fn compile_node_aggregate_field(
    field: &Field<'_, String>,
    context: &NodeContext,
    path: &str,
    compile_context: &GraphqlCompileContext<'_, '_>,
) -> Result<Option<Projection>, CoreError> {
    if !is_node_aggregate_field(&field.name) {
        return Ok(None);
    }
    if !field.selection_set.items.is_empty() {
        return Err(unsupported(
            format!("{path}.selectionSet"),
            "GraphQL aggregate fields must not select nested fields",
        ));
    }

    let alias = projection_alias(field, context);
    if field.name == "_count" {
        return Ok(Some(compile_count_aggregate_field(
            field,
            context,
            path,
            compile_context,
            alias,
        )?));
    }
    let aggregate = graphql_property_aggregate_field(&field.name)
        .ok_or_else(|| CoreError::internal("aggregate field name was checked"))?;
    Ok(Some(compile_property_aggregate_field(
        field,
        context,
        path,
        compile_context,
        aggregate,
        alias,
    )?))
}

fn compile_count_aggregate_field(
    field: &Field<'_, String>,
    context: &NodeContext,
    path: &str,
    compile_context: &GraphqlCompileContext<'_, '_>,
    alias: String,
) -> Result<Projection, CoreError> {
    if field.arguments.is_empty() {
        return Ok(Projection::CountAll { alias });
    }
    compile_property_aggregate_field(
        field,
        context,
        path,
        compile_context,
        &GraphqlAggregateFieldSpec {
            field_name: "_count",
            function: GraphqlAggregateFunctionSpec::Fixed(AggregateFunction::Count),
            distinct: false,
            arguments: GraphqlAggregateArgumentSpec::Field,
            return_type: GraphqlAggregateReturnType::Int,
        },
        alias,
    )
}

fn compile_property_aggregate_field(
    field: &Field<'_, String>,
    context: &NodeContext,
    path: &str,
    compile_context: &GraphqlCompileContext<'_, '_>,
    aggregate: &GraphqlAggregateFieldSpec,
    alias: String,
) -> Result<Projection, CoreError> {
    let (property, function) =
        compile_property_aggregate_arguments(field, path, compile_context, aggregate)?;
    Ok(Projection::Aggregate {
        function,
        target: AggregateTarget::Property(PropertyRef {
            variable: context.variable.clone(),
            property,
        }),
        distinct: aggregate.distinct,
        alias,
    })
}

fn compile_property_aggregate_arguments(
    field: &Field<'_, String>,
    path: &str,
    compile_context: &GraphqlCompileContext<'_, '_>,
    aggregate: &GraphqlAggregateFieldSpec,
) -> Result<(String, AggregateFunction), CoreError> {
    let property = match aggregate.arguments {
        GraphqlAggregateArgumentSpec::Field => {
            compile_single_aggregate_field_argument(field, path, compile_context)?
        }
        GraphqlAggregateArgumentSpec::FieldAndPercentile => {
            let (property, percentile) =
                compile_field_and_percentile_aggregate_arguments(field, path, compile_context)?;
            return Ok((property, AggregateFunction::PercentileCont { percentile }));
        }
    };
    let GraphqlAggregateFunctionSpec::Fixed(function) = aggregate.function else {
        return Err(CoreError::internal(
            "GraphQL aggregate argument shape did not match function",
        ));
    };
    Ok((property, function))
}

fn compile_single_aggregate_field_argument(
    field: &Field<'_, String>,
    path: &str,
    compile_context: &GraphqlCompileContext<'_, '_>,
) -> Result<String, CoreError> {
    let [(name, value)] = field.arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            format!(
                "GraphQL aggregate field '{}' requires exactly one 'field' argument",
                field.name
            ),
        ));
    };
    if name != "field" {
        return Err(unsupported(
            format!("{path}.arguments[0].{name}"),
            format!("unsupported GraphQL aggregate argument '{name}'"),
        ));
    }
    compile_name_value(value, format!("{path}.arguments[0].field"), compile_context)
}

fn compile_field_and_percentile_aggregate_arguments(
    field: &Field<'_, String>,
    path: &str,
    compile_context: &GraphqlCompileContext<'_, '_>,
) -> Result<(String, OrderedFloat<f64>), CoreError> {
    if field.arguments.len() != 2 {
        return Err(unsupported(
            format!("{path}.arguments"),
            format!(
                "GraphQL aggregate field '{}' requires exactly 'field' and 'percentile' arguments",
                field.name
            ),
        ));
    }
    let mut property = None;
    let mut percentile = None;
    for (index, (name, value)) in field.arguments.iter().enumerate() {
        match name.as_str() {
            "field" => {
                if property.is_some() {
                    return Err(unsupported(
                        format!("{path}.arguments[{index}].field"),
                        "GraphQL aggregate field argument 'field' was provided more than once",
                    ));
                }
                property = Some(compile_name_value(
                    value,
                    format!("{path}.arguments[{index}].field"),
                    compile_context,
                )?);
            }
            "percentile" => {
                if percentile.is_some() {
                    return Err(unsupported(
                        format!("{path}.arguments[{index}].percentile"),
                        "GraphQL aggregate field argument 'percentile' was provided more than once",
                    ));
                }
                percentile = Some(compile_percentile_aggregate_argument(
                    value,
                    format!("{path}.arguments[{index}].percentile"),
                    compile_context,
                )?);
            }
            _ => {
                return Err(unsupported(
                    format!("{path}.arguments[{index}].{name}"),
                    format!("unsupported GraphQL aggregate argument '{name}'"),
                ));
            }
        }
    }
    let property = property.ok_or_else(|| {
        unsupported(
            format!("{path}.arguments"),
            format!(
                "GraphQL aggregate field '{}' requires a 'field' argument",
                field.name
            ),
        )
    })?;
    let percentile = percentile.ok_or_else(|| {
        unsupported(
            format!("{path}.arguments"),
            format!(
                "GraphQL aggregate field '{}' requires a 'percentile' argument",
                field.name
            ),
        )
    })?;
    Ok((property, percentile))
}

fn compile_percentile_aggregate_argument(
    value: &Value<'_, String>,
    path: impl Into<String>,
    compile_context: &GraphqlCompileContext<'_, '_>,
) -> Result<OrderedFloat<f64>, CoreError> {
    let path = path.into();
    let literal = compile_literal(value, path.clone(), compile_context)?;
    let value = match literal {
        Literal::Integer(0) => 0.0,
        Literal::Integer(1) => 1.0,
        Literal::Integer(_) => {
            return Err(unsupported(
                path,
                "GraphQL percentile aggregate argument must be between 0.0 and 1.0 inclusive",
            ));
        }
        Literal::Float(value) => value.into_inner(),
        _ => {
            return Err(unsupported(
                path,
                "GraphQL percentile aggregate argument must be a numeric literal",
            ));
        }
    };
    if !value.is_finite() || !(0.0..=1.0).contains(&value) {
        return Err(unsupported(
            path,
            "GraphQL percentile aggregate argument must be between 0.0 and 1.0 inclusive",
        ));
    }
    Ok(OrderedFloat(value))
}
