//! Type-classification helpers for the graph plan validator: stateless free functions that
//! derive `ScalarType`s from literals, literal lists and column data types, apply numeric
//! result-type promotion, and expose aggregate-function metadata (SQL name, numeric-target
//! and graph-key-target rules, DISTINCT `PERCENTILE_CONT` rejection) plus projection-alias
//! extraction. Pure helpers with no validator state, unlike the sibling `validation/*` impls.

use super::{
    AggregateFunction, CoreError, Diagnostic, Literal, LiteralListElementType, Projection,
    ScalarType,
};

pub(super) fn projection_alias_name(projection: &Projection) -> Option<&str> {
    match projection {
        Projection::Property {
            alias: Some(alias), ..
        }
        | Projection::CountAll { alias }
        | Projection::Key { alias, .. }
        | Projection::ElementId { alias, .. }
        | Projection::RelationshipType { alias, .. }
        | Projection::NodeLabels { alias, .. }
        | Projection::PropertyKeys { alias, .. }
        | Projection::Literal { alias, .. }
        | Projection::LiteralList { alias, .. }
        | Projection::Expression { alias, .. }
        | Projection::Aggregate { alias, .. } => Some(alias),
        Projection::Property { alias: None, .. } => None,
    }
}

pub(super) fn literal_scalar_type(literal: &Literal) -> ScalarType {
    match literal {
        Literal::String(_) => ScalarType::String,
        Literal::Integer(_) => ScalarType::Integer,
        Literal::Float(_) => ScalarType::Float,
        Literal::Boolean(_) => ScalarType::Boolean,
        Literal::Null => ScalarType::Null,
    }
}

pub(super) fn literal_list_scalar_type(literals: &[Literal]) -> Result<ScalarType, CoreError> {
    let mut result_type = ScalarType::Null;
    for literal in literals {
        result_type = super::GraphPlanValidator::merge_scalar_types(
            result_type,
            literal_scalar_type(literal),
            "rhs",
            "literal list elements",
        )?;
    }
    Ok(result_type)
}

pub(super) fn numeric_result_type(scalar_type: ScalarType) -> ScalarType {
    match scalar_type {
        ScalarType::Integer => ScalarType::Integer,
        ScalarType::Float => ScalarType::Float,
        ScalarType::Unknown | ScalarType::Null => ScalarType::Unknown,
        ScalarType::String | ScalarType::Boolean | ScalarType::Other => {
            unreachable!("numeric result requested for non-numeric type")
        }
    }
}

pub(super) fn numeric_binary_result_type(left: ScalarType, right: ScalarType) -> ScalarType {
    match (left, right) {
        (ScalarType::Float, _) | (_, ScalarType::Float) => ScalarType::Float,
        (ScalarType::Integer, ScalarType::Integer) => ScalarType::Integer,
        _ => ScalarType::Unknown,
    }
}

pub(super) fn scalar_type_for_data_type(data_type: &str) -> ScalarType {
    let data_type = data_type.trim();
    if data_type.is_empty() {
        return ScalarType::Unknown;
    }
    if data_type.contains("Utf8") {
        return ScalarType::String;
    }
    if data_type.starts_with("Int") || data_type.starts_with("UInt") {
        return ScalarType::Integer;
    }
    if data_type.starts_with("Float") || data_type.starts_with("Decimal") {
        return ScalarType::Float;
    }
    if data_type == "Boolean" {
        return ScalarType::Boolean;
    }
    if data_type.starts_with("Dictionary") {
        return scalar_type_for_dictionary_data_type(data_type);
    }
    if matches!(data_type, "Null" | "NullType") {
        return ScalarType::Null;
    }
    ScalarType::Other
}

fn scalar_type_for_dictionary_data_type(data_type: &str) -> ScalarType {
    if data_type.contains("Utf8") {
        ScalarType::String
    } else if data_type.contains("Float") || data_type.contains("Decimal") {
        ScalarType::Float
    } else if data_type.contains("Int") || data_type.contains("UInt") {
        ScalarType::Integer
    } else if data_type.contains("Boolean") {
        ScalarType::Boolean
    } else {
        ScalarType::Other
    }
}

pub(super) fn literal_list_element_kind(literal: &Literal) -> Option<LiteralListElementType> {
    match literal {
        Literal::String(_) => Some(LiteralListElementType::String),
        Literal::Integer(_) => Some(LiteralListElementType::Integer),
        Literal::Float(_) => Some(LiteralListElementType::Float),
        Literal::Boolean(_) => Some(LiteralListElementType::Boolean),
        Literal::Null => None,
    }
}

pub(super) fn aggregate_function_name(function: AggregateFunction) -> &'static str {
    match function {
        AggregateFunction::Count => "count",
        AggregateFunction::Collect => "collect",
        AggregateFunction::Sum => "sum",
        AggregateFunction::Avg => "avg",
        AggregateFunction::Median => "median",
        AggregateFunction::PercentileCont { .. } => "percentileCont",
        AggregateFunction::StdDev => "stDev",
        AggregateFunction::StdDevP => "stDevP",
        AggregateFunction::Min => "min",
        AggregateFunction::Max => "max",
    }
}

pub(super) fn aggregate_function_requires_numeric_target(function: AggregateFunction) -> bool {
    matches!(
        function,
        AggregateFunction::Sum
            | AggregateFunction::Avg
            | AggregateFunction::Median
            | AggregateFunction::PercentileCont { .. }
            | AggregateFunction::StdDev
            | AggregateFunction::StdDevP
    )
}

pub(super) fn unsupported_distinct_percentile_cont_error(path: impl Into<String>) -> CoreError {
    Diagnostic::new(
        "INVALID_AGGREGATE_TARGET",
        path,
        "percentileCont(DISTINCT ...) is not supported because DataFusion 53 cannot execute distinct percentile_cont aggregates",
    )
    .into_core_error()
}

pub(super) fn aggregate_function_accepts_graph_variable_key(function: AggregateFunction) -> bool {
    matches!(
        function,
        AggregateFunction::Count | AggregateFunction::Collect
    )
}
