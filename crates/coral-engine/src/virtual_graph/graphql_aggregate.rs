//! Shared GraphQL aggregate field metadata.

use super::ir::AggregateFunction;

/// Return type category for generated GraphQL aggregate fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GraphqlAggregateReturnType {
    /// GraphQL `Int`.
    Int,
    /// GraphQL `CoralGraphValue`.
    GraphValue,
    /// GraphQL `[CoralGraphValue!]`.
    GraphValueList,
}

/// Metadata for a property-backed GraphQL aggregate field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GraphqlAggregateFieldSpec {
    /// GraphQL field name, including the leading underscore.
    pub field_name: &'static str,
    /// Shared graph aggregate function.
    pub function: AggregateFunction,
    /// Whether the aggregate should lower with `DISTINCT`.
    pub distinct: bool,
    /// SDL return type family.
    pub return_type: GraphqlAggregateReturnType,
}

/// Property-backed aggregate fields accepted by the GraphQL frontend.
///
/// `_count` is intentionally excluded because it may be used without a
/// `field:` argument as `COUNT(*)`; `_countDistinct` is property-backed and
/// belongs here.
pub(crate) const GRAPHQL_PROPERTY_AGGREGATE_FIELDS: &[GraphqlAggregateFieldSpec] = &[
    GraphqlAggregateFieldSpec {
        field_name: "_countDistinct",
        function: AggregateFunction::Count,
        distinct: true,
        return_type: GraphqlAggregateReturnType::Int,
    },
    GraphqlAggregateFieldSpec {
        field_name: "_collect",
        function: AggregateFunction::Collect,
        distinct: false,
        return_type: GraphqlAggregateReturnType::GraphValueList,
    },
    GraphqlAggregateFieldSpec {
        field_name: "_collectDistinct",
        function: AggregateFunction::Collect,
        distinct: true,
        return_type: GraphqlAggregateReturnType::GraphValueList,
    },
    GraphqlAggregateFieldSpec {
        field_name: "_sum",
        function: AggregateFunction::Sum,
        distinct: false,
        return_type: GraphqlAggregateReturnType::GraphValue,
    },
    GraphqlAggregateFieldSpec {
        field_name: "_sumDistinct",
        function: AggregateFunction::Sum,
        distinct: true,
        return_type: GraphqlAggregateReturnType::GraphValue,
    },
    GraphqlAggregateFieldSpec {
        field_name: "_avg",
        function: AggregateFunction::Avg,
        distinct: false,
        return_type: GraphqlAggregateReturnType::GraphValue,
    },
    GraphqlAggregateFieldSpec {
        field_name: "_avgDistinct",
        function: AggregateFunction::Avg,
        distinct: true,
        return_type: GraphqlAggregateReturnType::GraphValue,
    },
    GraphqlAggregateFieldSpec {
        field_name: "_median",
        function: AggregateFunction::Median,
        distinct: false,
        return_type: GraphqlAggregateReturnType::GraphValue,
    },
    GraphqlAggregateFieldSpec {
        field_name: "_medianDistinct",
        function: AggregateFunction::Median,
        distinct: true,
        return_type: GraphqlAggregateReturnType::GraphValue,
    },
    GraphqlAggregateFieldSpec {
        field_name: "_stDev",
        function: AggregateFunction::StdDev,
        distinct: false,
        return_type: GraphqlAggregateReturnType::GraphValue,
    },
    GraphqlAggregateFieldSpec {
        field_name: "_stDevP",
        function: AggregateFunction::StdDevP,
        distinct: false,
        return_type: GraphqlAggregateReturnType::GraphValue,
    },
    GraphqlAggregateFieldSpec {
        field_name: "_min",
        function: AggregateFunction::Min,
        distinct: false,
        return_type: GraphqlAggregateReturnType::GraphValue,
    },
    GraphqlAggregateFieldSpec {
        field_name: "_minDistinct",
        function: AggregateFunction::Min,
        distinct: true,
        return_type: GraphqlAggregateReturnType::GraphValue,
    },
    GraphqlAggregateFieldSpec {
        field_name: "_max",
        function: AggregateFunction::Max,
        distinct: false,
        return_type: GraphqlAggregateReturnType::GraphValue,
    },
    GraphqlAggregateFieldSpec {
        field_name: "_maxDistinct",
        function: AggregateFunction::Max,
        distinct: true,
        return_type: GraphqlAggregateReturnType::GraphValue,
    },
];

/// Finds a property-backed aggregate field by GraphQL field name.
#[must_use]
pub(crate) fn graphql_property_aggregate_field(
    field_name: &str,
) -> Option<&'static GraphqlAggregateFieldSpec> {
    GRAPHQL_PROPERTY_AGGREGATE_FIELDS
        .iter()
        .find(|field| field.field_name == field_name)
}

/// Returns whether a node property name would collide with a GraphQL virtual field.
#[must_use]
pub(crate) fn is_reserved_graphql_node_property_name(name: &str) -> bool {
    name == "_count"
        || GRAPHQL_PROPERTY_AGGREGATE_FIELDS
            .iter()
            .any(|field| field.field_name == name)
}
