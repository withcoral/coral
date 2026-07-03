//! openCypher function classification: case-insensitive `pub(super)` predicates
//! over `decypher` `FunctionInvocation`s that identify which built-in a call is —
//! aggregates (`count`, `collect`, `sum`, `avg`, `min`/`max`, `percentileCont`,
//! `stDev`...), scalar/string/list functions (`toString`, `toInteger`,
//! `substring`, `split`, `coalesce`...), identity (`id`, `elementId`, `type`,
//! `labels`, `keys`, `properties`), relationship endpoints, existence and
//! collection quantifiers, and internal graph functions — plus canonical-name
//! mapping helpers. Pure classifiers routing calls to IR lowering in `cypher.rs`.

use decypher::ast::expr::{Expression, FunctionInvocation, Literal as CypherLiteral};

use super::super::ir::AggregateFunction;
use super::{
    INTERNAL_GRAPH_IDENTITY_FUNCTION, INTERNAL_GRAPH_PRESENCE_FUNCTION,
    INTERNAL_STATIC_RANGE_FUNCTION, INTERNAL_STRING_CONTAINS_FUNCTION,
    INTERNAL_STRING_ENDS_WITH_FUNCTION, INTERNAL_STRING_STARTS_WITH_FUNCTION, RelationshipEndpoint,
    StaticListCastTarget, StaticListQuantifier,
};

pub(super) fn is_aggregate_function_call(function: &FunctionInvocation) -> bool {
    aggregate_function_default_alias(function).is_some()
}

pub(super) fn aggregate_function_default_alias(
    function: &FunctionInvocation,
) -> Option<&'static str> {
    let [name] = function.name.as_slice() else {
        return None;
    };
    if name.name.eq_ignore_ascii_case("count") {
        Some("count")
    } else if name.name.eq_ignore_ascii_case("collect")
        || name.name.eq_ignore_ascii_case("collect_list")
    {
        Some("collect")
    } else if name.name.eq_ignore_ascii_case("sum") {
        Some("sum")
    } else if name.name.eq_ignore_ascii_case("avg") {
        Some("avg")
    } else if name.name.eq_ignore_ascii_case("median") {
        Some("median")
    } else if name.name.eq_ignore_ascii_case("percentileCont")
        || name.name.eq_ignore_ascii_case("percentile_cont")
    {
        Some("percentileCont")
    } else if name.name.eq_ignore_ascii_case("stDev")
        || name.name.eq_ignore_ascii_case("stdev_samp")
    {
        Some("stDev")
    } else if name.name.eq_ignore_ascii_case("stDevP")
        || name.name.eq_ignore_ascii_case("stdev_pop")
    {
        Some("stDevP")
    } else if name.name.eq_ignore_ascii_case("min") {
        Some("min")
    } else if name.name.eq_ignore_ascii_case("max") {
        Some("max")
    } else {
        None
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

pub(super) fn is_exists_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("exists")
    )
}

pub(super) fn is_empty_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("isEmpty")
    )
}

pub(super) fn collection_quantifier_function(
    function: &FunctionInvocation,
) -> Option<StaticListQuantifier> {
    let [name] = function.name.as_slice() else {
        return None;
    };
    if name.name.eq_ignore_ascii_case("all") {
        Some(StaticListQuantifier::All)
    } else if name.name.eq_ignore_ascii_case("any") {
        Some(StaticListQuantifier::Any)
    } else if name.name.eq_ignore_ascii_case("none") {
        Some(StaticListQuantifier::None)
    } else if name.name.eq_ignore_ascii_case("single") {
        Some(StaticListQuantifier::Single)
    } else {
        None
    }
}

pub(super) fn is_id_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("id")
    )
}

pub(super) fn is_element_id_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("elementId")
    )
}

pub(super) fn is_internal_graph_identity_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name == INTERNAL_GRAPH_IDENTITY_FUNCTION
    )
}

pub(super) fn is_internal_graph_presence_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name == INTERNAL_GRAPH_PRESENCE_FUNCTION
    )
}

pub(super) fn is_internal_static_range_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name == INTERNAL_STATIC_RANGE_FUNCTION
    )
}

pub(super) fn is_type_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("type")
    )
}

pub(super) fn is_labels_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("labels")
    )
}

pub(super) fn is_keys_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("keys")
    )
}

pub(super) fn is_literal_map_keys_function(function: &FunctionInvocation) -> bool {
    is_keys_function(function)
        && matches!(
            function.arguments.as_slice(),
            [Expression::Literal(CypherLiteral::Map(_))]
        )
}

pub(super) fn is_properties_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("properties")
    )
}

pub(super) fn is_start_node_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("startNode")
    )
}

pub(super) fn is_end_node_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("endNode")
    )
}

pub(super) fn relationship_endpoint_function(
    function: &FunctionInvocation,
) -> Option<RelationshipEndpoint> {
    if is_start_node_function(function) {
        Some(RelationshipEndpoint::Start)
    } else if is_end_node_function(function) {
        Some(RelationshipEndpoint::End)
    } else {
        None
    }
}

pub(super) fn relationship_endpoint_function_name(endpoint: RelationshipEndpoint) -> &'static str {
    match endpoint {
        RelationshipEndpoint::Start => "startNode",
        RelationshipEndpoint::End => "endNode",
    }
}

pub(super) fn is_length_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("length")
    )
}

pub(super) fn is_size_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("size")
    )
}

pub(super) fn is_nodes_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("nodes")
    )
}

pub(super) fn is_relationships_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("relationships")
    )
}

pub(super) fn is_coalesce_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("coalesce")
    )
}

pub(super) fn is_null_if_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("nullIf")
    )
}

pub(super) fn is_to_string_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("toString")
    )
}

pub(super) fn is_to_integer_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("toInteger")
    )
}

pub(super) fn is_to_float_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("toFloat")
    )
}

pub(super) fn is_to_boolean_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("toBoolean")
    )
}

pub(super) fn is_to_string_or_null_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("toStringOrNull")
    )
}

pub(super) fn is_to_integer_or_null_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("toIntegerOrNull")
    )
}

pub(super) fn is_to_float_or_null_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("toFloatOrNull")
    )
}

pub(super) fn is_to_boolean_or_null_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("toBooleanOrNull")
    )
}

pub(super) fn static_list_cast_function(
    function: &FunctionInvocation,
) -> Option<StaticListCastTarget> {
    match function.name.as_slice() {
        [name] if name.name.eq_ignore_ascii_case("toStringList") => {
            Some(StaticListCastTarget::String)
        }
        [name] if name.name.eq_ignore_ascii_case("toIntegerList") => {
            Some(StaticListCastTarget::Integer)
        }
        [name] if name.name.eq_ignore_ascii_case("toFloatList") => {
            Some(StaticListCastTarget::Float)
        }
        [name] if name.name.eq_ignore_ascii_case("toBooleanList") => {
            Some(StaticListCastTarget::Boolean)
        }
        _ => None,
    }
}

pub(super) fn is_static_list_cast_function(function: &FunctionInvocation) -> bool {
    static_list_cast_function(function).is_some()
}

pub(super) fn is_to_lower_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("toLower")
            || name.name.eq_ignore_ascii_case("lower")
    )
}

pub(super) fn is_to_upper_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("toUpper")
            || name.name.eq_ignore_ascii_case("upper")
    )
}

pub(super) fn is_trim_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("trim")
            || name.name.eq_ignore_ascii_case("btrim")
    )
}

pub(super) fn is_ltrim_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("lTrim")
    )
}

pub(super) fn is_rtrim_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("rTrim")
    )
}

pub(super) fn is_replace_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("replace")
    )
}

pub(super) fn is_head_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("head")
    )
}

pub(super) fn is_last_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("last")
    )
}

pub(super) fn is_tail_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("tail")
    )
}

pub(super) fn is_reduce_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("reduce")
    )
}

pub(super) fn is_filter_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("filter")
    )
}

pub(super) fn is_extract_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("extract")
    )
}

pub(super) fn is_split_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("split")
    )
}

pub(super) fn is_character_length_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("char_length")
            || name.name.eq_ignore_ascii_case("character_length")
    ) || is_size_function(function)
}

pub(super) fn is_substring_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("substring")
    )
}

pub(super) fn is_left_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("left")
    )
}

pub(super) fn is_right_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("right")
    )
}

pub(super) fn is_indices_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("indices")
    )
}

pub(super) fn is_lpad_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("lpad")
    )
}

pub(super) fn is_rpad_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("rpad")
    )
}

pub(super) fn is_contains_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("contains")
            || name.name == INTERNAL_STRING_CONTAINS_FUNCTION
    )
}

pub(super) fn is_starts_with_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("startsWith")
            || name.name == INTERNAL_STRING_STARTS_WITH_FUNCTION
    )
}

pub(super) fn is_ends_with_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("endsWith")
            || name.name == INTERNAL_STRING_ENDS_WITH_FUNCTION
    )
}

pub(super) fn is_string_predicate_function(function: &FunctionInvocation) -> bool {
    is_contains_function(function)
        || is_starts_with_function(function)
        || is_ends_with_function(function)
}

pub(super) fn is_reverse_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("reverse")
    )
}

pub(super) fn is_abs_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("abs")
    )
}

pub(super) fn is_ceil_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("ceil")
            || name.name.eq_ignore_ascii_case("ceiling")
    )
}

pub(super) fn is_floor_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("floor")
    )
}

pub(super) fn is_round_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("round")
    )
}

pub(super) fn is_sqrt_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("sqrt")
    )
}

pub(super) fn is_sign_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("sign")
    )
}

pub(super) fn is_exp_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("exp")
    )
}

pub(super) fn is_log_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("log")
            || name.name.eq_ignore_ascii_case("ln")
    )
}

pub(super) fn is_log10_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("log10")
    )
}

pub(super) fn is_power_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("pow")
            || name.name.eq_ignore_ascii_case("power")
    )
}

pub(super) fn is_pi_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("pi")
    )
}

pub(super) fn is_e_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("e")
    )
}

pub(super) fn is_sin_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("sin")
    )
}

pub(super) fn is_cos_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("cos")
    )
}

pub(super) fn is_tan_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("tan")
    )
}

pub(super) fn is_cot_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("cot")
    )
}

pub(super) fn is_asin_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("asin")
    )
}

pub(super) fn is_acos_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("acos")
    )
}

pub(super) fn is_atan_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("atan")
    )
}

pub(super) fn is_atan2_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("atan2")
    )
}

pub(super) fn is_degrees_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("degrees")
    )
}

pub(super) fn is_radians_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("radians")
    )
}

pub(super) fn is_is_nan_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("isNaN")
            || name.name.eq_ignore_ascii_case("isnan")
    )
}

pub(super) fn is_haversin_function(function: &FunctionInvocation) -> bool {
    matches!(
        function.name.as_slice(),
        [name] if name.name.eq_ignore_ascii_case("haversin")
    )
}
