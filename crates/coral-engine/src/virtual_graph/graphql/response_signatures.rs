//! GraphQL response aliasing and selection signatures: derives stable projection
//! and response-key aliases from selection fields (`projection_alias`,
//! `graphql_response_name`, `edge_projection_alias`) and computes comparable
//! structural signatures of root and relationship selections and their sorted
//! arguments (`graphql_*_selection_signature`, `graphql_value_signature`) used to
//! detect duplicate or conflicting selections and to name SQL projection columns.
//! Stateless `pub(super)` helpers split out of `graphql.rs`.

use graphql_parser::query::{Field, Value};
use ordered_float::OrderedFloat;

use super::{
    GraphqlRelationshipSelectionSignature, GraphqlRootSelectionSignature, GraphqlValueSignature,
    NodeContext,
};

pub(super) fn projection_alias(field: &Field<'_, String>, context: &NodeContext) -> String {
    field.alias.clone().unwrap_or_else(|| {
        if context.is_root {
            field.name.clone()
        } else {
            format!("{}_{}", context.variable, field.name)
        }
    })
}

pub(super) fn graphql_response_name(field: &Field<'_, String>) -> String {
    field.alias.clone().unwrap_or_else(|| field.name.clone())
}

pub(super) fn graphql_root_selection_signature(
    field: &Field<'_, String>,
) -> GraphqlRootSelectionSignature {
    GraphqlRootSelectionSignature {
        field_name: field.name.clone(),
        arguments: graphql_field_argument_signature(field),
    }
}

pub(super) fn graphql_relationship_selection_signature(
    field: &Field<'_, String>,
) -> GraphqlRelationshipSelectionSignature {
    GraphqlRelationshipSelectionSignature {
        field_name: field.name.clone(),
        arguments: graphql_field_argument_signature(field),
    }
}

pub(super) fn graphql_field_argument_signature(
    field: &Field<'_, String>,
) -> Vec<(String, GraphqlValueSignature)> {
    let mut arguments = field
        .arguments
        .iter()
        .map(|(name, value)| (name.clone(), graphql_value_signature(value)))
        .collect::<Vec<_>>();
    arguments.sort_by(|(left, _), (right, _)| left.cmp(right));
    arguments
}

pub(super) fn graphql_value_signature(value: &Value<'_, String>) -> GraphqlValueSignature {
    match value {
        Value::Variable(variable) => GraphqlValueSignature::Variable(variable.clone()),
        Value::Int(number) => GraphqlValueSignature::Integer(
            number.as_i64().expect("GraphQL parser stores Int as i64"),
        ),
        Value::Float(value) => GraphqlValueSignature::Float(OrderedFloat(*value)),
        Value::String(value) => GraphqlValueSignature::String(value.clone()),
        Value::Boolean(value) => GraphqlValueSignature::Boolean(*value),
        Value::Null => GraphqlValueSignature::Null,
        Value::Enum(value) => GraphqlValueSignature::Enum(value.clone()),
        Value::List(values) => GraphqlValueSignature::List(
            values
                .iter()
                .map(graphql_value_signature)
                .collect::<Vec<_>>(),
        ),
        Value::Object(values) => GraphqlValueSignature::Object(
            values
                .iter()
                .map(|(name, value)| (name.clone(), graphql_value_signature(value)))
                .collect::<Vec<_>>(),
        ),
    }
}

pub(super) fn edge_projection_alias(field: &Field<'_, String>, edge_variable: &str) -> String {
    field
        .alias
        .clone()
        .unwrap_or_else(|| format!("{edge_variable}_{}", field.name))
}
