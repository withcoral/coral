//! Shared GraphQL aggregate field metadata.

use super::ir::AggregateFunction;

/// Aggregate function shape for GraphQL aggregate fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GraphqlAggregateFunctionSpec {
    /// Aggregate function with no extra GraphQL arguments beyond `field:`.
    Fixed(AggregateFunction),
    /// Exact continuous percentile with an additional `percentile:` argument.
    PercentileCont,
}

/// Argument shape for property-backed GraphQL aggregate fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GraphqlAggregateArgumentSpec {
    /// `field: <Property>`.
    Field,
    /// `field: <Property>, percentile: <Float>`.
    FieldAndPercentile,
}

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
    pub function: GraphqlAggregateFunctionSpec,
    /// Whether the aggregate should lower with `DISTINCT`.
    pub distinct: bool,
    /// GraphQL argument shape.
    pub arguments: GraphqlAggregateArgumentSpec,
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
        function: GraphqlAggregateFunctionSpec::Fixed(AggregateFunction::Count),
        distinct: true,
        arguments: GraphqlAggregateArgumentSpec::Field,
        return_type: GraphqlAggregateReturnType::Int,
    },
    GraphqlAggregateFieldSpec {
        field_name: "_collect",
        function: GraphqlAggregateFunctionSpec::Fixed(AggregateFunction::Collect),
        distinct: false,
        arguments: GraphqlAggregateArgumentSpec::Field,
        return_type: GraphqlAggregateReturnType::GraphValueList,
    },
    GraphqlAggregateFieldSpec {
        field_name: "_collectDistinct",
        function: GraphqlAggregateFunctionSpec::Fixed(AggregateFunction::Collect),
        distinct: true,
        arguments: GraphqlAggregateArgumentSpec::Field,
        return_type: GraphqlAggregateReturnType::GraphValueList,
    },
    GraphqlAggregateFieldSpec {
        field_name: "_sum",
        function: GraphqlAggregateFunctionSpec::Fixed(AggregateFunction::Sum),
        distinct: false,
        arguments: GraphqlAggregateArgumentSpec::Field,
        return_type: GraphqlAggregateReturnType::GraphValue,
    },
    GraphqlAggregateFieldSpec {
        field_name: "_sumDistinct",
        function: GraphqlAggregateFunctionSpec::Fixed(AggregateFunction::Sum),
        distinct: true,
        arguments: GraphqlAggregateArgumentSpec::Field,
        return_type: GraphqlAggregateReturnType::GraphValue,
    },
    GraphqlAggregateFieldSpec {
        field_name: "_avg",
        function: GraphqlAggregateFunctionSpec::Fixed(AggregateFunction::Avg),
        distinct: false,
        arguments: GraphqlAggregateArgumentSpec::Field,
        return_type: GraphqlAggregateReturnType::GraphValue,
    },
    GraphqlAggregateFieldSpec {
        field_name: "_avgDistinct",
        function: GraphqlAggregateFunctionSpec::Fixed(AggregateFunction::Avg),
        distinct: true,
        arguments: GraphqlAggregateArgumentSpec::Field,
        return_type: GraphqlAggregateReturnType::GraphValue,
    },
    GraphqlAggregateFieldSpec {
        field_name: "_median",
        function: GraphqlAggregateFunctionSpec::Fixed(AggregateFunction::Median),
        distinct: false,
        arguments: GraphqlAggregateArgumentSpec::Field,
        return_type: GraphqlAggregateReturnType::GraphValue,
    },
    GraphqlAggregateFieldSpec {
        field_name: "_medianDistinct",
        function: GraphqlAggregateFunctionSpec::Fixed(AggregateFunction::Median),
        distinct: true,
        arguments: GraphqlAggregateArgumentSpec::Field,
        return_type: GraphqlAggregateReturnType::GraphValue,
    },
    GraphqlAggregateFieldSpec {
        field_name: "_percentileCont",
        function: GraphqlAggregateFunctionSpec::PercentileCont,
        distinct: false,
        arguments: GraphqlAggregateArgumentSpec::FieldAndPercentile,
        return_type: GraphqlAggregateReturnType::GraphValue,
    },
    GraphqlAggregateFieldSpec {
        field_name: "_stDev",
        function: GraphqlAggregateFunctionSpec::Fixed(AggregateFunction::StdDev),
        distinct: false,
        arguments: GraphqlAggregateArgumentSpec::Field,
        return_type: GraphqlAggregateReturnType::GraphValue,
    },
    GraphqlAggregateFieldSpec {
        field_name: "_stDevP",
        function: GraphqlAggregateFunctionSpec::Fixed(AggregateFunction::StdDevP),
        distinct: false,
        arguments: GraphqlAggregateArgumentSpec::Field,
        return_type: GraphqlAggregateReturnType::GraphValue,
    },
    GraphqlAggregateFieldSpec {
        field_name: "_min",
        function: GraphqlAggregateFunctionSpec::Fixed(AggregateFunction::Min),
        distinct: false,
        arguments: GraphqlAggregateArgumentSpec::Field,
        return_type: GraphqlAggregateReturnType::GraphValue,
    },
    GraphqlAggregateFieldSpec {
        field_name: "_minDistinct",
        function: GraphqlAggregateFunctionSpec::Fixed(AggregateFunction::Min),
        distinct: true,
        arguments: GraphqlAggregateArgumentSpec::Field,
        return_type: GraphqlAggregateReturnType::GraphValue,
    },
    GraphqlAggregateFieldSpec {
        field_name: "_max",
        function: GraphqlAggregateFunctionSpec::Fixed(AggregateFunction::Max),
        distinct: false,
        arguments: GraphqlAggregateArgumentSpec::Field,
        return_type: GraphqlAggregateReturnType::GraphValue,
    },
    GraphqlAggregateFieldSpec {
        field_name: "_maxDistinct",
        function: GraphqlAggregateFunctionSpec::Fixed(AggregateFunction::Max),
        distinct: true,
        arguments: GraphqlAggregateArgumentSpec::Field,
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
