//! Cypher scalar-expression, arithmetic, and function lowering helpers split
//! out of `cypher.rs` without changing behavior.

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "Expression lowering helpers intentionally inherit parent-private Cypher compile context."
)]
use super::*;

pub(super) fn compile_coalesce_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    if function.arguments.len() < 2 {
        return Err(unsupported(
            format!("{path}.arguments"),
            "coalesce() requires at least two arguments",
        ));
    }
    if let Some(expression) = compile_optional_static_list_coalesce_scalar_expression(
        function,
        path.clone(),
        mode.static_metadata_plan(),
        context,
    )? {
        return Ok(expression);
    }
    let expressions = function
        .arguments
        .iter()
        .enumerate()
        .map(|(index, expression)| {
            compile_scalar_expression_in_predicate_mode(
                expression,
                format!("{path}.arguments[{index}]"),
                mode,
                context,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ScalarExpression::Coalesce { expressions })
}

pub(super) fn compile_null_if_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let (expression, value) =
        compile_two_scalar_function_arguments(function, path, "nullIf", mode, context)?;
    Ok(ScalarExpression::NullIf {
        expression: Box::new(expression),
        value: Box::new(value),
    })
}

pub(super) fn compile_to_string_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::ToString {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "toString", mode, context,
        )?),
    })
}

pub(super) fn compile_to_integer_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::ToInteger {
        expression: Box::new(compile_single_scalar_function_argument(
            function,
            path,
            "toInteger",
            mode,
            context,
        )?),
    })
}

pub(super) fn compile_to_float_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::ToFloat {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "toFloat", mode, context,
        )?),
    })
}

pub(super) fn compile_to_boolean_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::ToBoolean {
        expression: Box::new(compile_single_scalar_function_argument(
            function,
            path,
            "toBoolean",
            mode,
            context,
        )?),
    })
}

pub(super) fn compile_to_string_or_null_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::ToStringOrNull {
        expression: Box::new(compile_single_scalar_function_argument(
            function,
            path,
            "toStringOrNull",
            mode,
            context,
        )?),
    })
}

pub(super) fn compile_to_integer_or_null_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::ToIntegerOrNull {
        expression: Box::new(compile_single_scalar_function_argument(
            function,
            path,
            "toIntegerOrNull",
            mode,
            context,
        )?),
    })
}

pub(super) fn compile_to_float_or_null_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::ToFloatOrNull {
        expression: Box::new(compile_single_scalar_function_argument(
            function,
            path,
            "toFloatOrNull",
            mode,
            context,
        )?),
    })
}

pub(super) fn compile_to_boolean_or_null_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::ToBooleanOrNull {
        expression: Box::new(compile_single_scalar_function_argument(
            function,
            path,
            "toBooleanOrNull",
            mode,
            context,
        )?),
    })
}

pub(super) fn compile_to_lower_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let function_name = single_segment_function_name(function).unwrap_or("toLower");
    Ok(ScalarExpression::ToLower {
        expression: Box::new(compile_single_scalar_function_argument(
            function,
            path,
            function_name,
            mode,
            context,
        )?),
    })
}

pub(super) fn compile_to_upper_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let function_name = single_segment_function_name(function).unwrap_or("toUpper");
    Ok(ScalarExpression::ToUpper {
        expression: Box::new(compile_single_scalar_function_argument(
            function,
            path,
            function_name,
            mode,
            context,
        )?),
    })
}

pub(super) fn compile_trim_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let function_name = single_segment_function_name(function).unwrap_or("trim");
    Ok(ScalarExpression::Trim {
        expression: Box::new(compile_single_scalar_function_argument(
            function,
            path,
            function_name,
            mode,
            context,
        )?),
    })
}

pub(super) fn compile_ltrim_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::LTrim {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "lTrim", mode, context,
        )?),
    })
}

pub(super) fn compile_rtrim_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::RTrim {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "rTrim", mode, context,
        )?),
    })
}

pub(super) fn compile_replace_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let [expression, search, replacement] = function.arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            "replace() requires exactly three arguments",
        ));
    };
    Ok(ScalarExpression::Replace {
        expression: Box::new(compile_scalar_expression_in_predicate_mode(
            expression,
            format!("{path}.arguments[0]"),
            mode,
            context,
        )?),
        search: Box::new(compile_scalar_expression_in_predicate_mode(
            search,
            format!("{path}.arguments[1]"),
            mode,
            context,
        )?),
        replacement: Box::new(compile_scalar_expression_in_predicate_mode(
            replacement,
            format!("{path}.arguments[2]"),
            mode,
            context,
        )?),
    })
}

pub(super) fn compile_character_length_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let plan = mode.static_metadata_plan();
    let [argument] = function.arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            format!(
                "{}() requires exactly one argument",
                qualified_function_name(function)
            ),
        ));
    };
    if let Expression::Case(case) = argument
        && let Some(length) = compile_optional_static_list_case_length_scalar_expression(
            case,
            format!("{path}.arguments[0]"),
            mode,
            context,
        )?
    {
        return Ok(length);
    }
    if let Some(plan) = plan
        && let Some(length) = compile_optional_metadata_list_length_scalar_expression(
            argument,
            format!("{path}.arguments[0]"),
            plan,
            context,
        )?
    {
        return Ok(length);
    }
    if let Some(length) = compile_literal_list_length_scalar_expression(
        argument,
        format!("{path}.arguments[0]"),
        context,
    )? {
        return Ok(length);
    }
    if let Some(length) = compile_static_list_function_length_scalar_expression(
        argument,
        format!("{path}.arguments[0]"),
        plan,
        context,
    )? {
        return Ok(length);
    }
    if is_size_function(function)
        && let Some(length) = compile_optional_count_only_collection_size_scalar_expression(
            argument,
            format!("{path}.arguments[0]"),
            mode,
            context,
        )?
    {
        return Ok(length);
    }
    let function_name = qualified_function_name(function);
    Ok(ScalarExpression::CharacterLength {
        expression: Box::new(compile_single_scalar_function_argument(
            function,
            path,
            function_name.as_str(),
            mode,
            context,
        )?),
    })
}

pub(super) fn compile_optional_count_only_collection_size_scalar_expression(
    expression: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_optional_count_only_collection_size_scalar_expression(
                inner, path, mode, context,
            )
        }
        Expression::PatternComprehension(comprehension) => {
            compile_pattern_comprehension_count_scalar_expression(
                comprehension,
                path,
                mode.static_metadata_plan(),
                context,
            )
            .map(Some)
        }
        Expression::CollectSubquery(collect) => compile_collect_subquery_count_scalar_expression(
            collect,
            path,
            mode.static_metadata_plan(),
            context,
        )
        .map(Some),
        _ => Ok(None),
    }
}

pub(super) fn compile_optional_metadata_list_length_scalar_expression(
    expression: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_optional_metadata_list_length_scalar_expression(inner, path, plan, context)
        }
        expression => {
            let Some(value) =
                compile_optional_metadata_list_value(expression, path.clone(), plan, context)?
            else {
                return Ok(None);
            };
            Ok(Some(presence_gate_scalar_expression(
                value.presence_variable,
                list_length_scalar_expression(value.literals.len())?,
            )))
        }
    }
}

pub(super) fn compile_literal_list_length_scalar_expression(
    expression: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_literal_list_length_scalar_expression(inner, path, context)
        }
        Expression::ListSlice { list, .. } if !is_literal_list_source_expression(list) => Ok(None),
        Expression::Literal(CypherLiteral::List(_)) | Expression::ListSlice { .. } => Ok(Some(
            list_length_scalar_expression(compile_literal_list(expression, path, context)?.len())?,
        )),
        Expression::Parameter(parameter) => {
            match context.parameter_value(parameter, path.clone())? {
                CypherParameterValue::List(values) => {
                    Ok(Some(list_length_scalar_expression(values.len())?))
                }
                CypherParameterValue::Literal(_) => Ok(None),
            }
        }
        _ => Ok(None),
    }
}

pub(super) fn compile_static_list_function_length_scalar_expression(
    expression: &Expression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_static_list_function_length_scalar_expression(inner, path, plan, context)
        }
        Expression::ListSlice {
            list, start, end, ..
        } => {
            if let Some(length) = compile_optional_static_list_case_slice_length_scalar_expression(
                list,
                start.as_deref(),
                end.as_deref(),
                path.clone(),
                PredicateCompileMode::CaseWhen { plan },
                context,
            )? {
                return Ok(Some(length));
            }
            if let Some(length) =
                compile_optional_static_list_coalesce_slice_length_scalar_expression(
                    list,
                    start.as_deref(),
                    end.as_deref(),
                    path.clone(),
                    plan,
                    context,
                )?
            {
                return Ok(Some(length));
            }
            let Some(value) = compile_optional_static_list_value(expression, path, plan, context)?
            else {
                return Ok(None);
            };
            Ok(Some(static_list_length_scalar_expression(value)?))
        }
        Expression::FunctionCall(function) if is_coalesce_function(function) => {
            compile_optional_static_list_coalesce_length_scalar_expression(
                function, path, plan, context,
            )
        }
        expression => {
            let Some(value) = compile_optional_static_list_value(expression, path, plan, context)?
            else {
                return Ok(None);
            };
            Ok(Some(static_list_length_scalar_expression(value)?))
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct StaticReduceEvaluation<'a> {
    items: &'a [Literal],
    accumulator_variable: &'a str,
    item_variable: &'a str,
    expression: &'a Expression,
    mode: PredicateCompileMode<'a>,
    context: &'a CypherCompileContext,
}

pub(super) fn compile_static_reduce_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    if function.distinct {
        return Err(unsupported(
            path,
            "reduce(DISTINCT ...) is not valid Cypher syntax",
        ));
    }
    let source = context.static_reduce_source(function).ok_or_else(|| {
        unsupported(
            format!("{path}.arguments"),
            "reduce() requires accumulator initialization, item IN collection, and reducer expression",
        )
    })?;
    if source.accumulator_variable == source.item_variable {
        return Err(unsupported(
            format!("{path}.arguments[1]"),
            "reduce() accumulator and item variables must be distinct",
        ));
    }
    let (initial, initial_context) = parse_cypher_expression_fragment(
        &source.initial_source,
        format!("{path}.arguments[0].initial"),
        context,
    )?;
    let accumulator = compile_static_reduce_initial_literal(
        &initial,
        format!("{path}.arguments[0].initial"),
        mode,
        &initial_context,
    )?;
    let collection = compile_static_list_value_source(
        &source.collection_source,
        format!("{path}.arguments[1].collection"),
        mode.static_metadata_plan(),
        context,
    )?
    .ok_or_else(|| {
        unsupported(
            format!("{path}.arguments[1].collection"),
            "reduce() requires a literal list, list parameter, static split(...), range(...), tail(...), or static labels()/keys() metadata list",
        )
    })?;
    let (expression, expression_context) = parse_cypher_expression_fragment(
        &source.expression_source,
        format!("{path}.expression"),
        context,
    )?;
    let presence_variable = collection.presence_variable.clone();
    let accumulator = evaluate_static_reduce(
        accumulator,
        StaticReduceEvaluation {
            items: &collection.literals,
            accumulator_variable: &source.accumulator_variable,
            item_variable: &source.item_variable,
            expression: &expression,
            mode,
            context: &expression_context,
        },
        path,
    )?;
    Ok(presence_gate_scalar_expression(
        presence_variable,
        ScalarExpression::Literal(accumulator),
    ))
}

pub(super) fn compile_static_reduce_initial_literal(
    expression: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Literal, CoreError> {
    let path = path.into();
    compile_optional_static_literal_scalar_operand(expression, path.clone(), mode, context)?
        .ok_or_else(|| {
            unsupported(
                path,
                "reduce() initial accumulator must be a scalar literal, scalar parameter, or static scalar expression",
            )
        })
}

pub(super) fn evaluate_static_reduce(
    mut accumulator: Literal,
    reduce: StaticReduceEvaluation<'_>,
    path: impl Into<String>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    for (index, item) in reduce.items.iter().enumerate() {
        let evaluation = StaticFilterEvaluation {
            variable: reduce.item_variable,
            item,
            accumulator_variable: Some(reduce.accumulator_variable),
            accumulator: Some(&accumulator),
            mode: reduce.mode,
            context: reduce.context,
        };
        accumulator = evaluate_static_map_expression(
            reduce.expression,
            evaluation,
            format!("{path}.expression[{index}]"),
        )?;
    }
    Ok(accumulator)
}

pub(super) fn compile_substring_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    match function.arguments.as_slice() {
        [expression, start] => Ok(ScalarExpression::Substring {
            expression: Box::new(compile_scalar_expression_in_predicate_mode(
                expression,
                format!("{path}.arguments[0]"),
                mode,
                context,
            )?),
            start: Box::new(compile_scalar_expression_in_predicate_mode(
                start,
                format!("{path}.arguments[1]"),
                mode,
                context,
            )?),
            length: None,
        }),
        [expression, start, length] => Ok(ScalarExpression::Substring {
            expression: Box::new(compile_scalar_expression_in_predicate_mode(
                expression,
                format!("{path}.arguments[0]"),
                mode,
                context,
            )?),
            start: Box::new(compile_scalar_expression_in_predicate_mode(
                start,
                format!("{path}.arguments[1]"),
                mode,
                context,
            )?),
            length: Some(Box::new(compile_scalar_expression_in_predicate_mode(
                length,
                format!("{path}.arguments[2]"),
                mode,
                context,
            )?)),
        }),
        _ => Err(unsupported(
            format!("{path}.arguments"),
            "substring() requires exactly two or three arguments",
        )),
    }
}

pub(super) fn compile_left_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let (expression, count) =
        compile_two_scalar_function_arguments(function, path, "left", mode, context)?;
    Ok(ScalarExpression::Left {
        expression: Box::new(expression),
        count: Box::new(count),
    })
}

pub(super) fn compile_right_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let (expression, count) =
        compile_two_scalar_function_arguments(function, path, "right", mode, context)?;
    Ok(ScalarExpression::Right {
        expression: Box::new(expression),
        count: Box::new(count),
    })
}

pub(super) fn compile_indices_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let (expression, pattern) =
        compile_two_scalar_function_arguments(function, path, "indices", mode, context)?;
    Ok(ScalarExpression::StringIndices {
        expression: Box::new(expression),
        pattern: Box::new(pattern),
    })
}

pub(super) fn compile_lpad_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let (expression, length, fill) =
        compile_three_scalar_function_arguments(function, path, "lpad", mode, context)?;
    Ok(ScalarExpression::LPad {
        expression: Box::new(expression),
        length: Box::new(length),
        fill: Box::new(fill),
    })
}

pub(super) fn compile_rpad_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let (expression, length, fill) =
        compile_three_scalar_function_arguments(function, path, "rpad", mode, context)?;
    Ok(ScalarExpression::RPad {
        expression: Box::new(expression),
        length: Box::new(length),
        fill: Box::new(fill),
    })
}

pub(super) fn compile_contains_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let (expression, substring) =
        compile_two_scalar_function_arguments(function, path, "contains", mode, context)?;
    Ok(ScalarExpression::StringContains {
        expression: Box::new(expression),
        pattern: Box::new(substring),
    })
}

pub(super) fn compile_starts_with_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let (expression, prefix) =
        compile_two_scalar_function_arguments(function, path, "startsWith", mode, context)?;
    Ok(ScalarExpression::StringStartsWith {
        expression: Box::new(expression),
        pattern: Box::new(prefix),
    })
}

pub(super) fn compile_ends_with_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let (expression, suffix) =
        compile_two_scalar_function_arguments(function, path, "endsWith", mode, context)?;
    Ok(ScalarExpression::StringEndsWith {
        expression: Box::new(expression),
        pattern: Box::new(suffix),
    })
}

pub(super) fn compile_reverse_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let plan = mode.static_metadata_plan();
    let [argument] = function.arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            "reverse() requires exactly one argument",
        ));
    };
    if let Some(expression) = compile_optional_path_list_reverse_scalar_expression(
        argument,
        format!("{path}.arguments[0]"),
        mode,
        context,
    )? {
        return Ok(expression);
    }
    if let Some(value) =
        compile_optional_static_list_value(argument, format!("{path}.arguments[0]"), plan, context)?
    {
        let argument_path = format!("{path}.arguments[0]");
        let value = reverse_static_list_value(value, argument_path.clone())?;
        return static_list_value_scalar_expression(value, argument_path);
    }
    Ok(ScalarExpression::Reverse {
        expression: Box::new(compile_scalar_expression_in_predicate_mode(
            argument,
            format!("{path}.arguments[0]"),
            mode,
            context,
        )?),
    })
}

pub(super) fn compile_abs_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Abs {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "abs", mode, context,
        )?),
    })
}

pub(super) fn compile_ceil_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Ceil {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "ceil", mode, context,
        )?),
    })
}

pub(super) fn compile_floor_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Floor {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "floor", mode, context,
        )?),
    })
}

pub(super) fn compile_round_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    match function.arguments.as_slice() {
        [expression] => Ok(ScalarExpression::Round {
            expression: Box::new(compile_scalar_expression_in_predicate_mode(
                expression,
                format!("{path}.arguments[0]"),
                mode,
                context,
            )?),
            places: None,
        }),
        [expression, places] => Ok(ScalarExpression::Round {
            expression: Box::new(compile_scalar_expression_in_predicate_mode(
                expression,
                format!("{path}.arguments[0]"),
                mode,
                context,
            )?),
            places: Some(Box::new(compile_scalar_expression_in_predicate_mode(
                places,
                format!("{path}.arguments[1]"),
                mode,
                context,
            )?)),
        }),
        _ => Err(unsupported(
            format!("{path}.arguments"),
            "round() requires exactly one or two arguments",
        )),
    }
}

pub(super) fn compile_sqrt_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Sqrt {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "sqrt", mode, context,
        )?),
    })
}

pub(super) fn compile_sign_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Sign {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "sign", mode, context,
        )?),
    })
}

pub(super) fn compile_exp_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Exp {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "exp", mode, context,
        )?),
    })
}

pub(super) fn compile_log_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Log {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "log", mode, context,
        )?),
    })
}

pub(super) fn compile_log10_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Log10 {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "log10", mode, context,
        )?),
    })
}

pub(super) fn compile_power_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let function_name = single_segment_function_name(function).unwrap_or("power");
    let (left, right) =
        compile_two_scalar_function_arguments(function, path, function_name, mode, context)?;
    Ok(ScalarExpression::Arithmetic {
        operator: ArithmeticOperator::Power,
        left: Box::new(left),
        right: Box::new(right),
    })
}

pub(super) fn compile_sin_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Sin {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "sin", mode, context,
        )?),
    })
}

pub(super) fn compile_cos_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Cos {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "cos", mode, context,
        )?),
    })
}

pub(super) fn compile_tan_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Tan {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "tan", mode, context,
        )?),
    })
}

pub(super) fn compile_cot_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Cot {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "cot", mode, context,
        )?),
    })
}

pub(super) fn compile_asin_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Asin {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "asin", mode, context,
        )?),
    })
}

pub(super) fn compile_acos_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Acos {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "acos", mode, context,
        )?),
    })
}

pub(super) fn compile_atan_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Atan {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "atan", mode, context,
        )?),
    })
}

pub(super) fn compile_atan2_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let (y, x) = compile_two_scalar_function_arguments(function, path, "atan2", mode, context)?;
    Ok(ScalarExpression::Atan2 {
        y: Box::new(y),
        x: Box::new(x),
    })
}

pub(super) fn compile_degrees_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Degrees {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "degrees", mode, context,
        )?),
    })
}

pub(super) fn compile_radians_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Radians {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "radians", mode, context,
        )?),
    })
}

pub(super) fn compile_is_nan_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::IsNaN {
        expression: Box::new(compile_single_scalar_function_argument(
            function, path, "isNaN", mode, context,
        )?),
    })
}

pub(super) fn compile_haversin_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(haversin_expression(
        compile_single_scalar_function_argument(function, path, "haversin", mode, context)?,
    ))
}

pub(super) fn haversin_expression(expression: ScalarExpression) -> ScalarExpression {
    ScalarExpression::Arithmetic {
        operator: ArithmeticOperator::Divide,
        left: Box::new(ScalarExpression::Arithmetic {
            operator: ArithmeticOperator::Subtract,
            left: Box::new(ScalarExpression::Literal(Literal::Integer(1))),
            right: Box::new(ScalarExpression::Cos {
                expression: Box::new(expression),
            }),
        }),
        right: Box::new(ScalarExpression::Literal(Literal::Integer(2))),
    }
}

pub(super) fn compile_zero_scalar_function_arguments(
    function: &FunctionInvocation,
    path: impl Into<String>,
    function_name: &str,
) -> Result<(), CoreError> {
    let path = path.into();
    if function.arguments.is_empty() {
        return Ok(());
    }
    Err(unsupported(
        format!("{path}.arguments"),
        format!("{function_name}() requires exactly zero arguments"),
    ))
}

pub(super) fn compile_single_scalar_function_argument(
    function: &FunctionInvocation,
    path: impl Into<String>,
    function_name: &str,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let [argument] = function.arguments.as_slice() else {
        if function.arguments.is_empty()
            && let Some(variable) = context.variable_function_argument(function)
            && let Some(state) = mode.scalar_alias_state()
            && let Some(projection) = scalar_alias_projection(state, variable)
        {
            return scalar_alias_projection_expression(projection, format!("{path}.arguments"));
        }
        return Err(unsupported(
            format!("{path}.arguments"),
            format!("{function_name}() requires exactly one argument"),
        ));
    };
    compile_scalar_expression_in_predicate_mode(
        argument,
        format!("{path}.arguments[0]"),
        mode,
        context,
    )
}

pub(super) fn single_segment_function_name(function: &FunctionInvocation) -> Option<&str> {
    match function.name.as_slice() {
        [name] => Some(name.name.as_str()),
        _ => None,
    }
}

pub(super) fn compile_two_scalar_function_arguments(
    function: &FunctionInvocation,
    path: impl Into<String>,
    function_name: &str,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<(ScalarExpression, ScalarExpression), CoreError> {
    let path = path.into();
    let [left, right] = function.arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            format!("{function_name}() requires exactly two arguments"),
        ));
    };
    Ok((
        compile_scalar_expression_in_predicate_mode(
            left,
            format!("{path}.arguments[0]"),
            mode,
            context,
        )?,
        compile_scalar_expression_in_predicate_mode(
            right,
            format!("{path}.arguments[1]"),
            mode,
            context,
        )?,
    ))
}

pub(super) fn compile_three_scalar_function_arguments(
    function: &FunctionInvocation,
    path: impl Into<String>,
    function_name: &str,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<(ScalarExpression, ScalarExpression, ScalarExpression), CoreError> {
    let path = path.into();
    let [first, second, third] = function.arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            format!("{function_name}() requires exactly three arguments"),
        ));
    };
    Ok((
        compile_scalar_expression_in_predicate_mode(
            first,
            format!("{path}.arguments[0]"),
            mode,
            context,
        )?,
        compile_scalar_expression_in_predicate_mode(
            second,
            format!("{path}.arguments[1]"),
            mode,
            context,
        )?,
        compile_scalar_expression_in_predicate_mode(
            third,
            format!("{path}.arguments[2]"),
            mode,
            context,
        )?,
    ))
}

pub(super) fn compile_scalar_function_expression_with_path_state(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: &GraphPlan,
    path_state: Option<&CypherCompileState>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    compile_scalar_function_expression_in_mode(
        function,
        path,
        PredicateCompileMode::Graph { plan, path_state },
        context,
    )
}

pub(super) fn compile_scalar_function_expression_in_mode(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    let plan = mode.static_metadata_plan();
    if is_id_function(function) {
        match plan {
            Some(plan) => {
                compile_key_scalar_expression(function, path.clone(), plan, context).map(Some)
            }
            None => Err(unsupported(
                path,
                "id() scalar expressions require graph context",
            )),
        }
    } else if is_element_id_function(function) {
        match plan {
            Some(plan) => {
                compile_element_id_scalar_expression(function, path.clone(), plan, context)
                    .map(Some)
            }
            None => Err(unsupported(
                path,
                "elementId() scalar expressions require graph context",
            )),
        }
    } else if is_type_function(function) {
        match plan {
            Some(plan) => {
                compile_relationship_type_scalar_expression(function, path.clone(), plan, context)
                    .map(Some)
            }
            None => Err(unsupported(
                path,
                "type() scalar expressions require graph context",
            )),
        }
    } else if let Some(target) = path_list_size_target(function) {
        compile_path_element_id_list_scalar_expression(function, target, path, mode, context)
            .map(Some)
    } else if let Some(expression) =
        compile_temporal_scalar_function_expression(function, &path, mode, context)?
    {
        Ok(Some(expression))
    } else if let Some(expression) =
        compile_core_scalar_function_expression(function, &path, mode, context)?
    {
        Ok(Some(expression))
    } else {
        compile_numeric_scalar_function_expression(function, &path, mode, context)
    }
}

pub(super) fn compile_core_scalar_function_expression(
    function: &FunctionInvocation,
    path: &str,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let plan = mode.static_metadata_plan();
    let expression = if is_coalesce_function(function) {
        compile_coalesce_scalar_expression(function, path, mode, context)?
    } else if is_null_if_function(function) {
        compile_null_if_scalar_expression(function, path, mode, context)?
    } else if is_to_string_function(function) {
        compile_to_string_scalar_expression(function, path, mode, context)?
    } else if is_to_integer_function(function) {
        compile_to_integer_scalar_expression(function, path, mode, context)?
    } else if is_to_float_function(function) {
        compile_to_float_scalar_expression(function, path, mode, context)?
    } else if is_to_boolean_function(function) {
        compile_to_boolean_scalar_expression(function, path, mode, context)?
    } else if is_to_string_or_null_function(function) {
        compile_to_string_or_null_scalar_expression(function, path, mode, context)?
    } else if is_to_integer_or_null_function(function) {
        compile_to_integer_or_null_scalar_expression(function, path, mode, context)?
    } else if is_to_float_or_null_function(function) {
        compile_to_float_or_null_scalar_expression(function, path, mode, context)?
    } else if is_to_boolean_or_null_function(function) {
        compile_to_boolean_or_null_scalar_expression(function, path, mode, context)?
    } else if is_static_list_cast_function(function) {
        compile_static_list_cast_scalar_expression(function, path, plan, context)?
    } else if is_to_lower_function(function) {
        compile_to_lower_scalar_expression(function, path, mode, context)?
    } else if is_to_upper_function(function) {
        compile_to_upper_scalar_expression(function, path, mode, context)?
    } else if is_trim_function(function) {
        compile_trim_scalar_expression(function, path, mode, context)?
    } else if is_ltrim_function(function) {
        compile_ltrim_scalar_expression(function, path, mode, context)?
    } else if is_rtrim_function(function) {
        compile_rtrim_scalar_expression(function, path, mode, context)?
    } else if is_replace_function(function) {
        compile_replace_scalar_expression(function, path, mode, context)?
    } else if is_head_function(function) {
        compile_static_list_endpoint_scalar_expression(
            function,
            path,
            mode,
            context,
            ListEndpoint::Head,
        )?
    } else if is_last_function(function) {
        compile_static_list_endpoint_scalar_expression(
            function,
            path,
            mode,
            context,
            ListEndpoint::Last,
        )?
    } else if is_tail_function(function) {
        compile_static_list_tail_scalar_expression(function, path, mode, context)?
    } else if is_reduce_function(function) {
        compile_static_reduce_scalar_expression(function, path, mode, context)?
    } else if is_character_length_function(function) {
        compile_character_length_scalar_expression(function, path, mode, context)?
    } else if is_substring_function(function) {
        compile_substring_scalar_expression(function, path, mode, context)?
    } else if is_left_function(function) {
        compile_left_scalar_expression(function, path, mode, context)?
    } else if is_right_function(function) {
        compile_right_scalar_expression(function, path, mode, context)?
    } else if is_indices_function(function) {
        compile_indices_scalar_expression(function, path, mode, context)?
    } else if is_lpad_function(function) {
        compile_lpad_scalar_expression(function, path, mode, context)?
    } else if is_rpad_function(function) {
        compile_rpad_scalar_expression(function, path, mode, context)?
    } else if is_contains_function(function) {
        compile_contains_scalar_expression(function, path, mode, context)?
    } else if is_starts_with_function(function) {
        compile_starts_with_scalar_expression(function, path, mode, context)?
    } else if is_ends_with_function(function) {
        compile_ends_with_scalar_expression(function, path, mode, context)?
    } else if is_reverse_function(function) {
        compile_reverse_scalar_expression(function, path, mode, context)?
    } else {
        return Ok(None);
    };
    Ok(Some(expression))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ListEndpoint {
    Head,
    Last,
}

pub(super) fn compile_static_list_endpoint_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
    endpoint: ListEndpoint,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let plan = mode.static_metadata_plan();
    let function_name = match endpoint {
        ListEndpoint::Head => "head",
        ListEndpoint::Last => "last",
    };
    let [argument] = function.arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            format!("{function_name}() requires exactly one list argument"),
        ));
    };
    if let Some(expression) = compile_optional_path_list_endpoint_scalar_expression(
        argument,
        format!("{path}.arguments[0]"),
        mode,
        context,
        endpoint,
    )? {
        return Ok(expression);
    }
    if let Expression::ListSlice {
        list, start, end, ..
    } = argument
    {
        if let Some(expression) =
            compile_optional_static_list_case_slice_endpoint_scalar_expression(
                list,
                start.as_deref(),
                end.as_deref(),
                format!("{path}.arguments[0]"),
                mode,
                context,
                endpoint,
            )?
        {
            return Ok(expression);
        }
        if let Some(expression) =
            compile_optional_static_list_coalesce_slice_endpoint_scalar_expression(
                list,
                start.as_deref(),
                end.as_deref(),
                format!("{path}.arguments[0]"),
                plan,
                context,
                endpoint,
            )?
        {
            return Ok(expression);
        }
    }
    if let Expression::Case(case) = argument
        && let Some(expression) = compile_optional_static_list_case_endpoint_scalar_expression(
            case,
            format!("{path}.arguments[0]"),
            mode,
            context,
            endpoint,
        )?
    {
        return Ok(expression);
    }
    if let Expression::FunctionCall(function) = argument
        && is_coalesce_function(function)
        && let Some(coalesce) = compile_optional_static_list_coalesce_arguments(
            function,
            format!("{path}.arguments[0]"),
            plan,
            context,
        )?
    {
        return Ok(static_list_coalesce_endpoint_scalar_expression(
            coalesce, endpoint,
        ));
    }
    let Some(value) = compile_optional_static_list_value(
        argument,
        format!("{path}.arguments[0]"),
        plan,
        context,
    )?
    else {
        return Err(unsupported(
            format!("{path}.arguments[0]"),
            format!(
                "{function_name}() requires a literal list, list parameter, static split(...), range(...), or static labels()/keys() metadata list"
            ),
        ));
    };
    Ok(static_list_value_endpoint_scalar_expression(
        value, endpoint,
    ))
}

pub(super) fn compile_static_list_tail_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let plan = mode.static_metadata_plan();
    let [argument] = function.arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            "tail() requires exactly one list argument",
        ));
    };
    if let Some(expression) = compile_optional_path_list_tail_scalar_expression(
        argument,
        format!("{path}.arguments[0]"),
        mode,
        context,
    )? {
        return Ok(expression);
    }
    let Some(value) = compile_optional_static_list_value(
        argument,
        format!("{path}.arguments[0]"),
        plan,
        context,
    )?
    else {
        return Err(unsupported(
            format!("{path}.arguments[0]"),
            "tail() requires a literal list, list parameter, static split(...), range(...), or static labels()/keys() metadata list",
        ));
    };
    static_list_tail_expression(value, format!("{path}.arguments[0]"))
}

pub(super) fn compile_static_list_cast_scalar_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let value = compile_static_list_cast_value(function, path.clone(), plan, context)?;
    static_list_value_scalar_expression(value, path)
}

pub(super) fn compile_numeric_scalar_function_expression(
    function: &FunctionInvocation,
    path: &str,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let expression = if is_abs_function(function) {
        compile_abs_scalar_expression(function, path, mode, context)?
    } else if is_ceil_function(function) {
        compile_ceil_scalar_expression(function, path, mode, context)?
    } else if is_floor_function(function) {
        compile_floor_scalar_expression(function, path, mode, context)?
    } else if is_round_function(function) {
        compile_round_scalar_expression(function, path, mode, context)?
    } else if is_sqrt_function(function) {
        compile_sqrt_scalar_expression(function, path, mode, context)?
    } else if is_sign_function(function) {
        compile_sign_scalar_expression(function, path, mode, context)?
    } else if is_exp_function(function) {
        compile_exp_scalar_expression(function, path, mode, context)?
    } else if is_log_function(function) {
        compile_log_scalar_expression(function, path, mode, context)?
    } else if is_log10_function(function) {
        compile_log10_scalar_expression(function, path, mode, context)?
    } else if is_power_function(function) {
        compile_power_scalar_expression(function, path, mode, context)?
    } else if is_pi_function(function) {
        compile_pi_scalar_expression(function, path)?
    } else if is_e_function(function) {
        compile_e_scalar_expression(function, path)?
    } else if is_sin_function(function) {
        compile_sin_scalar_expression(function, path, mode, context)?
    } else if is_cos_function(function) {
        compile_cos_scalar_expression(function, path, mode, context)?
    } else if is_tan_function(function) {
        compile_tan_scalar_expression(function, path, mode, context)?
    } else if is_cot_function(function) {
        compile_cot_scalar_expression(function, path, mode, context)?
    } else if is_asin_function(function) {
        compile_asin_scalar_expression(function, path, mode, context)?
    } else if is_acos_function(function) {
        compile_acos_scalar_expression(function, path, mode, context)?
    } else if is_atan_function(function) {
        compile_atan_scalar_expression(function, path, mode, context)?
    } else if is_atan2_function(function) {
        compile_atan2_scalar_expression(function, path, mode, context)?
    } else if is_degrees_function(function) {
        compile_degrees_scalar_expression(function, path, mode, context)?
    } else if is_radians_function(function) {
        compile_radians_scalar_expression(function, path, mode, context)?
    } else if is_is_nan_function(function) {
        compile_is_nan_scalar_expression(function, path, mode, context)?
    } else if is_haversin_function(function) {
        compile_haversin_scalar_expression(function, path, mode, context)?
    } else {
        return Ok(None);
    };
    Ok(Some(expression))
}

pub(super) fn compile_scalar_expression_with_path_state(
    expression: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    path_state: Option<&CypherCompileState>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    compile_scalar_expression_in_predicate_mode(
        expression,
        path,
        PredicateCompileMode::Graph { plan, path_state },
        context,
    )
}

pub(super) fn compile_list_index_scalar_expression_in_mode(
    expression: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let plan = mode.static_metadata_plan();
    if let Some(expression) = compile_optional_stage_list_index_scalar_expression(
        expression,
        path.clone(),
        mode.scalar_alias_state(),
        context,
    )? {
        return Ok(expression);
    }
    if let Some(expression) =
        compile_optional_path_list_index_scalar_expression(expression, path.clone(), mode, context)?
    {
        return Ok(expression);
    }
    if let Some(plan) = plan
        && let Some(expression) = compile_optional_metadata_list_index_scalar_expression(
            expression,
            path.clone(),
            plan,
            context,
        )?
    {
        return Ok(expression);
    }
    if let Some(expression) = compile_optional_static_list_index_scalar_expression(
        expression,
        path.clone(),
        plan,
        context,
    )? {
        return Ok(expression);
    }
    Ok(ScalarExpression::Literal(compile_literal(
        expression, path, context,
    )?))
}

pub(super) fn compile_optional_stage_list_index_scalar_expression(
    expression: &Expression,
    path: impl Into<String>,
    state: Option<&CypherCompileState>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    let Expression::ListIndex { list, index, .. } = expression else {
        return Ok(None);
    };
    let Some(state) = state else {
        return Ok(None);
    };
    let Some(alias) = expression_variable_name(list) else {
        return Ok(None);
    };
    let Some(element_type) = state.list_alias_element_types.get(&alias).copied() else {
        return Ok(None);
    };
    let Literal::Integer(index) = compile_literal(index, format!("{path}.index"), context)? else {
        return Err(unsupported(
            format!("{path}.index"),
            "UNWIND list indexes require an integer literal or scalar integer parameter",
        ));
    };
    if index < 0 {
        return Err(unsupported(
            format!("{path}.index"),
            "UNWIND list indexes over dynamic list values require a non-negative integer index",
        ));
    }
    Ok(Some(ScalarExpression::ListIndex {
        list: Box::new(ScalarExpression::StageValue { alias }),
        index,
        element_type,
    }))
}

pub(super) fn compile_scalar_expression_in_mode(
    expression: &Expression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    compile_scalar_expression_in_predicate_mode(
        expression,
        path,
        PredicateCompileMode::CaseWhen { plan },
        context,
    )
}

pub(super) fn compile_scalar_expression_in_predicate_mode(
    expression: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let plan = mode.static_metadata_plan();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_scalar_expression_in_predicate_mode(inner, path, mode, context)
        }
        Expression::PropertyLookup { .. } => {
            compile_property_lookup_scalar_expression_in_mode(expression, path, mode, context)
        }
        Expression::ListIndex { .. } => compile_list_index_scalar_or_property_expression_in_mode(
            expression, path, mode, context,
        ),
        Expression::ListSlice { .. } => {
            compile_list_slice_scalar_expression_in_mode(expression, path, mode, context)
        }
        Expression::Variable(_) => {
            compile_scalar_alias_expression(expression, path, mode.scalar_alias_state())
        }
        expression if is_literal_expression(expression) => Ok(ScalarExpression::Literal(
            compile_literal(expression, path, context)?,
        )),
        Expression::BinaryOp { .. } => compile_binary_scalar_expression_in_predicate_mode(
            expression, &path, mode, plan, context,
        ),
        Expression::UnaryOp {
            op: UnaryOperator::Negate,
            operand,
            ..
        } => Ok(ScalarExpression::Negate {
            expression: Box::new(compile_scalar_expression_in_predicate_mode(
                operand,
                format!("{path}.operand"),
                mode,
                context,
            )?),
        }),
        Expression::Case(case) => compile_case_scalar_expression_in_mode(case, path, mode, context),
        Expression::CountSubquery(count) => {
            compile_count_subquery_scalar_expression(count, path, plan, context)
        }
        Expression::CollectSubquery(collect) => {
            compile_collect_subquery_scalar_expression(collect, path, plan, context)
        }
        Expression::PatternComprehension(comprehension) => {
            compile_pattern_comprehension_scalar_expression(comprehension, path, plan, context)
        }
        Expression::FunctionCall(function) => {
            if let Some(expression) = compile_optional_path_length_scalar_expression(
                expression,
                path.clone(),
                mode,
                context,
            )? {
                return Ok(expression);
            }
            compile_scalar_function_expression_in_mode(function, path.clone(), mode, context)?
                .ok_or_else(|| {
                    unsupported(
                        path,
                        format!(
                            "scalar function '{}' is not supported here",
                            qualified_function_name(function)
                        ),
                    )
                })
        }
        _ => Err(unsupported(
            path,
            "scalar expressions must be variable.property expressions, scalar literals, scalar parameters, arithmetic expressions, unary negation, nested coalesce(), nullIf(), date(), localdatetime(), localtime(), duration(), toString(), toInteger(), toFloat(), toBoolean(), nullable scalar casts, toLower()/lower(), toUpper()/upper(), trim()/btrim(), lTrim(), rTrim(), replace(), head(), last(), tail(), reduce(), size(), char_length(), character_length(), substring(), left(), right(), reverse(), abs(), ceil(), floor(), round(), sqrt(), sign(), exp(), log(), log10(), pow()/power(), pi(), e(), sin(), cos(), tan(), cot(), asin(), acos(), atan(), atan2(), degrees(), radians(), or haversin() expressions",
        )),
    }
}

pub(super) fn compile_binary_scalar_expression_in_predicate_mode(
    expression: &Expression,
    path: &str,
    mode: PredicateCompileMode<'_>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let Expression::BinaryOp { op, lhs, rhs, .. } = expression else {
        return Err(CoreError::internal(
            "binary scalar expression compiler received a non-binary expression",
        ));
    };
    if let Some(expression) =
        compile_optional_static_list_scalar_expression(expression, path.to_string(), plan, context)?
    {
        return Ok(expression);
    }
    let operator = compile_arithmetic_operator(*op, format!("{path}.operator"))?;
    let left =
        compile_scalar_expression_in_predicate_mode(lhs, format!("{path}.lhs"), mode, context)?;
    let right =
        compile_scalar_expression_in_predicate_mode(rhs, format!("{path}.rhs"), mode, context)?;
    if let Some(expression) = compile_duration_multiply_expression(operator, &left, &right, path)? {
        return Ok(expression);
    }
    Ok(ScalarExpression::Arithmetic {
        operator,
        left: Box::new(left),
        right: Box::new(right),
    })
}

pub(super) fn compile_property_lookup_scalar_expression_in_mode(
    expression: &Expression,
    path: String,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let plan = mode.static_metadata_plan();
    if let Some((expression, _)) = compile_optional_endpoint_property_scalar_expression(
        expression,
        path.clone(),
        plan,
        context,
    )? {
        return Ok(expression);
    }
    if let Some(expression) = compile_optional_temporal_component_scalar_expression(
        expression,
        path.clone(),
        mode,
        context,
    )? {
        return Ok(expression);
    }
    if let Some(expression) = compile_optional_static_map_lookup_scalar_expression(
        expression,
        path.clone(),
        mode,
        context,
    )? {
        return Ok(expression);
    }
    Ok(ScalarExpression::Property(compile_property_ref(
        expression, path, plan, context,
    )?))
}

pub(super) fn compile_list_index_scalar_or_property_expression_in_mode(
    expression: &Expression,
    path: String,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let plan = mode.static_metadata_plan();
    if let Some(expression) = compile_optional_stage_list_index_scalar_expression(
        expression,
        path.clone(),
        mode.scalar_alias_state(),
        context,
    )? {
        return Ok(expression);
    }
    if let Some((expression, _)) = compile_optional_endpoint_property_scalar_expression(
        expression,
        path.clone(),
        plan,
        context,
    )? {
        return Ok(expression);
    }
    if let Some(property) = compile_optional_property_ref(expression, path.clone(), plan, context)?
    {
        return Ok(ScalarExpression::Property(property));
    }
    compile_list_index_scalar_expression_in_mode(expression, path, mode, context)
}

pub(super) fn compile_list_slice_scalar_expression_in_mode(
    expression: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let plan = mode.static_metadata_plan();
    if let Some(expression) =
        compile_optional_path_list_slice_scalar_expression(expression, path.clone(), mode, context)?
    {
        return Ok(expression);
    }
    if let Some(expression) =
        compile_optional_static_list_scalar_expression(expression, path.clone(), plan, context)?
    {
        return Ok(expression);
    }
    Ok(ScalarExpression::LiteralList {
        literals: compile_literal_list(expression, path, context)?,
    })
}

pub(super) fn compile_scalar_alias_expression(
    expression: &Expression,
    path: impl Into<String>,
    state: Option<&CypherCompileState>,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    compile_optional_scalar_alias_expression(expression, path.clone(), state)?.ok_or_else(|| {
        unsupported(
            path,
            "bare variables in scalar expressions must be in-scope WITH scalar aliases",
        )
    })
}

pub(super) fn compile_optional_boolean_scalar_expression(
    expression: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    if is_boolean_scalar_expression(expression) {
        return compile_boolean_scalar_expression(expression, path, plan, context).map(Some);
    }
    Ok(None)
}

pub(super) fn compile_boolean_scalar_expression(
    expression: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    compile_predicate_expression(expression, path, plan, context)
        .map(|predicate| ScalarExpression::Predicate(Box::new(predicate)))
}

pub(super) fn is_boolean_scalar_expression(expression: &Expression) -> bool {
    match expression {
        Expression::Parenthesized(inner) => is_boolean_scalar_expression(inner),
        Expression::BinaryOp {
            op: CypherBinaryOperator::And | CypherBinaryOperator::Or | CypherBinaryOperator::Xor,
            ..
        }
        | Expression::UnaryOp {
            op: UnaryOperator::Not,
            ..
        }
        | Expression::Comparison { .. }
        | Expression::In { .. }
        | Expression::IsNull { .. }
        | Expression::NodeLabels { .. }
        | Expression::Exists(_)
        | Expression::All(_)
        | Expression::Any(_)
        | Expression::None(_)
        | Expression::Single(_) => true,
        Expression::FunctionCall(function) => {
            is_exists_function(function)
                || is_empty_function(function)
                || collection_quantifier_function(function).is_some()
        }
        _ => false,
    }
}

pub(super) fn compile_optional_predicate_scalar_expression(
    expression: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    let plan = mode.static_metadata_plan();
    if let Some(expression) =
        compile_optional_static_map_lookup_predicate_scalar(expression, &path, mode, context)?
    {
        return Ok(Some(expression));
    }
    match expression {
        Expression::Parenthesized(inner) => {
            compile_optional_predicate_scalar_expression(inner, path, mode, context)
        }
        Expression::ListIndex { .. } => {
            compile_optional_predicate_list_index_scalar_expression(expression, path, mode, context)
        }
        Expression::ListSlice { .. } => {
            compile_optional_predicate_list_slice_scalar_expression(expression, path, mode, context)
        }
        Expression::PropertyLookup { .. } => {
            if let Some(expression) = compile_optional_temporal_component_scalar_expression(
                expression,
                path.clone(),
                mode,
                context,
            )? {
                return Ok(Some(expression));
            }
            Ok(compile_optional_endpoint_property_scalar_expression(
                expression, path, plan, context,
            )?
            .map(|(expression, _)| expression))
        }
        Expression::Variable(_) => {
            compile_optional_scalar_alias_expression(expression, path, mode.scalar_alias_state())
        }
        Expression::BinaryOp { .. } => Ok(Some(compile_scalar_expression_in_predicate_mode(
            expression, path, mode, context,
        )?)),
        Expression::UnaryOp {
            op: UnaryOperator::Negate,
            operand,
            ..
        } if !is_literal_expression(operand) => Ok(Some(
            compile_scalar_expression_in_predicate_mode(expression, path, mode, context)?,
        )),
        Expression::Case(case) => Ok(Some(compile_case_scalar_expression_in_mode(
            case,
            path,
            PredicateCompileMode::CaseWhen { plan },
            context,
        )?)),
        Expression::CountSubquery(count) => Ok(Some(compile_count_subquery_scalar_expression(
            count, path, plan, context,
        )?)),
        Expression::FunctionCall(function) if is_id_function(function) => {
            let Some(plan) = plan else {
                return Ok(None);
            };
            let value = compile_id_graph_value_ref(function, path, plan, context)?;
            Ok(value
                .presence_variable
                .is_some()
                .then(|| graph_value_key_scalar_expression(value)))
        }
        Expression::FunctionCall(function) if is_element_id_function(function) => {
            let Some(plan) = plan else {
                return Ok(None);
            };
            let value = compile_element_id_graph_value_ref(function, path, plan, context)?;
            Ok(value
                .presence_variable
                .is_some()
                .then(|| graph_value_element_id_scalar_expression(value)))
        }
        Expression::FunctionCall(function)
            if is_start_node_function(function) || is_end_node_function(function) =>
        {
            let Some(plan) = plan else {
                return Ok(None);
            };
            let value = compile_relationship_endpoint_ref(function, path, plan, context)?;
            Ok(value
                .presence_variable
                .is_some()
                .then(|| graph_value_presence_scalar_expression(value)))
        }
        Expression::FunctionCall(function) if is_type_function(function) => Ok(None),
        Expression::FunctionCall(function) => {
            compile_scalar_function_expression_in_mode(function, path, mode, context)
        }
        _ => Ok(None),
    }
}

pub(super) fn compile_optional_predicate_list_index_scalar_expression(
    expression: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    let Some(plan) = mode.static_metadata_plan() else {
        return Ok(None);
    };
    if let Some(expression) =
        compile_optional_path_list_index_scalar_expression(expression, path.clone(), mode, context)?
    {
        return Ok(Some(expression));
    }
    if let Some(expression) = compile_optional_metadata_list_index_scalar_expression(
        expression,
        path.clone(),
        plan,
        context,
    )? {
        return Ok(Some(expression));
    }
    compile_optional_static_list_index_scalar_expression(expression, path, Some(plan), context)
}

pub(super) fn compile_optional_static_map_lookup_predicate_scalar(
    expression: &Expression,
    path: &str,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    compile_optional_non_literal_static_map_lookup_scalar_expression(
        expression,
        path.to_string(),
        mode,
        context,
    )
}

pub(super) fn compile_optional_predicate_list_slice_scalar_expression(
    expression: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    let Some(plan) = mode.static_metadata_plan() else {
        return Ok(None);
    };
    if let Some(expression) =
        compile_optional_path_list_slice_scalar_expression(expression, path.clone(), mode, context)?
    {
        return Ok(Some(expression));
    }
    compile_optional_static_list_scalar_expression(expression, path, Some(plan), context)
}

pub(super) fn compile_scalar_predicate_rhs(
    expression: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarPredicateRhs, CoreError> {
    let path = path.into();
    if let Some(expression) =
        compile_optional_path_length_scalar_expression(expression, path.clone(), mode, context)?
    {
        return Ok(ScalarPredicateRhs::Expression(expression));
    }
    let plan = mode.static_metadata_plan();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_scalar_predicate_rhs(inner, path, mode, context)
        }
        Expression::BinaryOp { .. }
        | Expression::UnaryOp {
            op: UnaryOperator::Negate,
            ..
        }
        | Expression::PropertyLookup { .. }
        | Expression::ListIndex { .. }
        | Expression::ListSlice { .. } => Ok(ScalarPredicateRhs::Expression(
            compile_scalar_expression_in_predicate_mode(expression, path, mode, context)?,
        )),
        Expression::Variable(_) => {
            let Some(expression) = compile_optional_scalar_alias_expression(
                expression,
                path.clone(),
                mode.scalar_alias_state(),
            )?
            else {
                return Err(unsupported(
                    path,
                    "scalar predicates can only use bare variables when they are in-scope WITH scalar aliases",
                ));
            };
            Ok(ScalarPredicateRhs::Expression(expression))
        }
        Expression::Case(case) => Ok(ScalarPredicateRhs::Expression(
            compile_case_scalar_expression_in_mode(case, path, mode, context)?,
        )),
        Expression::CountSubquery(count) => Ok(ScalarPredicateRhs::Expression(
            compile_count_subquery_scalar_expression(count, path, plan, context)?,
        )),
        Expression::FunctionCall(function) => {
            match compile_scalar_function_expression_in_mode(function, path.clone(), mode, context)?
            {
                Some(expression) => Ok(ScalarPredicateRhs::Expression(expression)),
                None => Err(unsupported(
                    path,
                    "scalar predicates support variable.property expressions, scalar literals, scalar parameters, arithmetic expressions, unary negation, nested coalesce(), nullIf(), toString(), toInteger(), toFloat(), toBoolean(), nullable scalar casts, toLower()/lower(), toUpper()/upper(), trim()/btrim(), lTrim(), rTrim(), replace(), head(), last(), tail(), size(), char_length(), character_length(), substring(), left(), right(), reverse(), abs(), ceil(), floor(), round(), sqrt(), sign(), exp(), log(), log10(), pi(), e(), sin(), cos(), tan(), cot(), asin(), acos(), atan(), atan2(), degrees(), radians(), or haversin() expressions",
                )),
            }
        }
        expression if is_literal_expression(expression) => Ok(ScalarPredicateRhs::Expression(
            ScalarExpression::Literal(compile_literal(expression, path, context)?),
        )),
        _ => Err(unsupported(
            path,
            "scalar predicates support variable.property expressions, scalar literals, scalar parameters, arithmetic expressions, unary negation, nested coalesce(), nullIf(), toString(), toInteger(), toFloat(), toBoolean(), nullable scalar casts, toLower()/lower(), toUpper()/upper(), trim()/btrim(), lTrim(), rTrim(), replace(), head(), last(), tail(), size(), char_length(), character_length(), substring(), left(), right(), reverse(), abs(), ceil(), floor(), round(), sqrt(), sign(), exp(), log(), log10(), pi(), e(), sin(), cos(), tan(), cot(), asin(), acos(), atan(), atan2(), degrees(), radians(), or haversin() expressions",
        )),
    }
}

pub(super) fn compile_optional_path_length_scalar_expression(
    expression: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    let function = match expression {
        Expression::Parenthesized(inner) => {
            return compile_optional_path_length_scalar_expression(inner, path, mode, context);
        }
        Expression::FunctionCall(function) if is_length_function(function) => function,
        Expression::FunctionCall(function) if is_size_function(function) => {
            let Some(state) = mode.path_state() else {
                return Ok(None);
            };
            return compile_optional_size_path_length_scalar_expression(
                function,
                format!("{path}.arguments"),
                state,
                context,
            );
        }
        _ => return Ok(None),
    };
    let Some(state) = mode.path_state() else {
        return Ok(None);
    };
    let length = compile_path_length_scalar_expression(
        function,
        format!("{path}.arguments"),
        state,
        context,
    )?;
    Ok(Some(length))
}

pub(super) fn compile_arithmetic_operator(
    operator: CypherBinaryOperator,
    path: impl Into<String>,
) -> Result<ArithmeticOperator, CoreError> {
    match operator {
        CypherBinaryOperator::Add => Ok(ArithmeticOperator::Add),
        CypherBinaryOperator::Subtract => Ok(ArithmeticOperator::Subtract),
        CypherBinaryOperator::Multiply => Ok(ArithmeticOperator::Multiply),
        CypherBinaryOperator::Divide => Ok(ArithmeticOperator::Divide),
        CypherBinaryOperator::Modulo => Ok(ArithmeticOperator::Modulo),
        CypherBinaryOperator::Power => Ok(ArithmeticOperator::Power),
        CypherBinaryOperator::And | CypherBinaryOperator::Or | CypherBinaryOperator::Xor => {
            Err(unsupported(
                path,
                "boolean operators are not scalar arithmetic expressions",
            ))
        }
    }
}

pub(super) fn compile_case_scalar_expression_with_path_state(
    case: &CaseExpression,
    path: impl Into<String>,
    plan: &GraphPlan,
    path_state: Option<&CypherCompileState>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    compile_case_scalar_expression_in_mode(
        case,
        path,
        PredicateCompileMode::Graph { plan, path_state },
        context,
    )
}

pub(super) fn compile_case_scalar_expression_in_mode(
    case: &CaseExpression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    if case.alternatives.is_empty() {
        return Err(unsupported(
            format!("{path}.alternatives"),
            "CASE expressions require at least one WHEN/THEN alternative",
        ));
    }

    if let Some(expression) =
        compile_optional_static_list_case_scalar_expression(case, path.clone(), mode, context)?
    {
        return Ok(expression);
    }

    let alternatives = case
        .alternatives
        .iter()
        .enumerate()
        .map(|(index, alternative)| {
            let when = if let Some(scrutinee) = &case.scrutinee {
                compile_binary_comparison(
                    scrutinee,
                    CypherComparisonOperator::Eq,
                    &alternative.when,
                    format!("{path}.alternatives[{index}].when"),
                    mode,
                    context,
                )?
            } else {
                compile_predicate_expression_in_mode(
                    &alternative.when,
                    format!("{path}.alternatives[{index}].when"),
                    mode,
                    context,
                )?
            };
            Ok(ScalarCaseAlternative {
                when,
                then: compile_scalar_expression_in_predicate_mode(
                    &alternative.then,
                    format!("{path}.alternatives[{index}].then"),
                    mode,
                    context,
                )?,
            })
        })
        .collect::<Result<Vec<_>, CoreError>>()?;
    let else_expression = case
        .default
        .as_ref()
        .map(|expression| {
            compile_scalar_expression_in_predicate_mode(
                expression,
                format!("{path}.default"),
                mode,
                context,
            )
            .map(Box::new)
        })
        .transpose()?;

    Ok(ScalarExpression::Case {
        alternatives,
        else_expression,
    })
}

pub(super) fn compile_optional_static_list_case_scalar_expression(
    case: &CaseExpression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    let Some(parts) = compile_optional_static_list_case_parts(case, path.clone(), mode, context)?
    else {
        return Ok(None);
    };
    let element_type = require_static_list_case_element_type(&parts, path)?;

    Ok(Some(ScalarExpression::Case {
        alternatives: parts
            .alternatives
            .into_iter()
            .map(|(when, result)| ScalarCaseAlternative {
                when,
                then: static_list_case_result_scalar_expression(result, element_type),
            })
            .collect(),
        else_expression: parts.default.map(|result| {
            Box::new(static_list_case_result_scalar_expression(
                result,
                element_type,
            ))
        }),
    }))
}

pub(super) struct StaticListCaseParts {
    pub(super) alternatives: Vec<(PredicateExpression, StaticListCaseResult)>,
    pub(super) default: Option<StaticListCaseResult>,
    pub(super) element_type: Option<LiteralListElementType>,
}

pub(super) fn require_static_list_case_element_type(
    parts: &StaticListCaseParts,
    path: impl Into<String>,
) -> Result<LiteralListElementType, CoreError> {
    parts.element_type.ok_or_else(|| {
        unsupported(
            format!("{}.alternatives", path.into()),
            "list-valued CASE result branches require at least one non-null list element type",
        )
    })
}

pub(super) fn compile_static_list_case_alternative(
    case: &CaseExpression,
    index: usize,
    path: &str,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<(PredicateExpression, Option<StaticListCaseResult>), CoreError> {
    let alternative = case
        .alternatives
        .get(index)
        .ok_or_else(|| CoreError::internal("CASE alternative index out of bounds"))?;
    let when = if let Some(scrutinee) = &case.scrutinee {
        compile_binary_comparison(
            scrutinee,
            CypherComparisonOperator::Eq,
            &alternative.when,
            format!("{path}.alternatives[{index}].when"),
            mode,
            context,
        )?
    } else {
        compile_predicate_expression_in_mode(
            &alternative.when,
            format!("{path}.alternatives[{index}].when"),
            mode,
            context,
        )?
    };
    let result = compile_optional_static_list_case_result(
        &alternative.then,
        format!("{path}.alternatives[{index}].then"),
        mode.static_metadata_plan(),
        context,
    )?;
    Ok((when, result))
}

pub(super) fn compile_optional_static_list_case_default(
    case: &CaseExpression,
    path: &str,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<StaticListCaseResult>, CoreError> {
    case.default
        .as_ref()
        .map(|expression| {
            compile_optional_static_list_case_result(
                expression,
                format!("{path}.default"),
                mode.static_metadata_plan(),
                context,
            )
        })
        .transpose()
        .map(Option::flatten)
}

pub(super) fn compile_optional_static_list_case_parts(
    case: &CaseExpression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<StaticListCaseParts>, CoreError> {
    let path = path.into();
    let mut saw_list_result = false;
    let mut saw_non_list_result = false;
    let mut element_type = None;
    let mut alternatives = Vec::with_capacity(case.alternatives.len());

    for index in 0..case.alternatives.len() {
        let (when, result) =
            compile_static_list_case_alternative(case, index, &path, mode, context)?;
        if let Some(result) = result.as_ref() {
            saw_list_result |= !matches!(result, StaticListCaseResult::Null);
            element_type = merge_static_list_case_result_element_type(
                element_type,
                result,
                &format!("{path}.alternatives[{index}].then"),
            )?;
        } else {
            saw_non_list_result = true;
        }
        alternatives.push((when, result));
    }

    let default = compile_optional_static_list_case_default(case, &path, mode, context)?;
    if let Some(default) = default.as_ref() {
        saw_list_result |= !matches!(default, StaticListCaseResult::Null);
        element_type = merge_static_list_case_result_element_type(
            element_type,
            default,
            &format!("{path}.default"),
        )?;
    } else if case.default.is_some() {
        saw_non_list_result = true;
    }

    if !saw_list_result {
        return Ok(None);
    }
    if saw_non_list_result {
        return Err(unsupported(
            format!("{path}.alternatives"),
            "list-valued CASE result branches require every non-null branch to be a static list",
        ));
    }
    let alternatives = alternatives
        .into_iter()
        .enumerate()
        .map(|(index, (when, result))| {
            let Some(result) = result else {
                return Err(unsupported(
                    format!("{path}.alternatives[{index}].then"),
                    "list-valued CASE result branches require every non-null branch to be a static list",
                ));
            };
            Ok((when, result))
        })
        .collect::<Result<Vec<_>, CoreError>>()?;

    Ok(Some(StaticListCaseParts {
        alternatives,
        default,
        element_type,
    }))
}

pub(super) fn compile_node_function_target_ref(
    function: &FunctionInvocation,
    path: impl Into<String>,
    message: &'static str,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<(GraphValueRef, String), CoreError> {
    let path = path.into();
    let value = compile_single_graph_value_function_argument_ref(
        function,
        path.clone(),
        message,
        plan,
        context,
    )?;
    let node = plan
        .nodes
        .iter()
        .find(|node| node.variable == value.variable)
        .ok_or_else(|| {
            unsupported(
                format!("{path}[0]"),
                format!(
                    "labels() argument '{}' is not a node variable",
                    value.variable
                ),
            )
        })?;
    Ok((value, node.label.clone()))
}

pub(super) fn compile_single_graph_value_function_argument(
    function: &FunctionInvocation,
    path: impl Into<String>,
    message: &'static str,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<String, CoreError> {
    let path = path.into();
    let value = compile_single_graph_value_function_argument_ref(
        function,
        path.clone(),
        message,
        plan,
        context,
    )?;
    reject_optional_graph_value_ref(value, path)
}

pub(super) fn compile_single_graph_value_function_argument_ref(
    function: &FunctionInvocation,
    path: impl Into<String>,
    message: &'static str,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<GraphValueRef, CoreError> {
    let path = path.into();
    match function.arguments.as_slice() {
        [argument] => compile_graph_value_expression_ref(
            argument,
            format!("{path}[0]"),
            message,
            plan,
            context,
        ),
        [] => {
            let variable = context
                .variable_function_argument(function)
                .map(str::to_string)
                .ok_or_else(|| unsupported(path.clone(), message))?;
            Ok(GraphValueRef {
                variable,
                presence_variable: None,
            })
        }
        _ => Err(unsupported(path, message)),
    }
}

pub(super) fn compile_graph_value_expression_variable(
    expression: &Expression,
    path: impl Into<String>,
    message: &'static str,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<String, CoreError> {
    let path = path.into();
    let value =
        compile_graph_value_expression_ref(expression, path.clone(), message, plan, context)?;
    reject_optional_graph_value_ref(value, path)
}

pub(super) fn compile_graph_value_expression_ref(
    expression: &Expression,
    path: impl Into<String>,
    message: &'static str,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<GraphValueRef, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_graph_value_expression_ref(inner, path, message, plan, context)
        }
        Expression::Variable(variable) => {
            let variable = variable_name(variable);
            Ok(GraphValueRef {
                variable,
                presence_variable: None,
            })
        }
        Expression::FunctionCall(function)
            if is_start_node_function(function) || is_end_node_function(function) =>
        {
            compile_relationship_endpoint_ref(function, path, plan, context)
        }
        _ => Err(unsupported(path, message)),
    }
}

pub(super) fn compile_single_variable_function_argument(
    function: &FunctionInvocation,
    path: impl Into<String>,
    message: &'static str,
    context: &CypherCompileContext,
) -> Result<String, CoreError> {
    let path = path.into();
    if let Some(variable) = optional_single_variable_function_argument(function, context) {
        Ok(variable)
    } else if matches!(
        function.arguments.as_slice(),
        [Expression::Parenthesized(_)]
    ) {
        Err(unsupported(format!("{path}[0]"), message))
    } else {
        Err(unsupported(path, message))
    }
}

pub(super) fn optional_single_variable_function_argument(
    function: &FunctionInvocation,
    context: &CypherCompileContext,
) -> Option<String> {
    match function.arguments.as_slice() {
        [Expression::Parenthesized(inner)] => match inner.as_ref() {
            Expression::Variable(variable) => Some(variable_name(variable)),
            _ => None,
        },
        [Expression::Variable(variable)] => Some(variable_name(variable)),
        [] => context
            .variable_function_argument(function)
            .map(str::to_string),
        _ => None,
    }
}

pub(super) fn compile_function_aggregate_target(
    function: &FunctionInvocation,
    function_kind: AggregateFunction,
    path: &str,
    plan: Option<&GraphPlan>,
    state: Option<&CypherCompileState>,
    context: &CypherCompileContext,
) -> Result<AggregateTarget, CoreError> {
    if function.arguments.is_empty()
        && let Some(variable) = context.variable_function_argument(function)
        && let Some(target) = compile_optional_scalar_alias_aggregate_target(
            variable,
            format!("{path}.expression.arguments"),
            state,
        )?
    {
        return Ok(target);
    }

    match function.arguments.as_slice() {
        [argument, _]
            if matches!(
                function_kind,
                AggregateFunction::PercentileCont { .. } | AggregateFunction::PercentileDisc { .. }
            ) =>
        {
            compile_aggregate_target(
                argument,
                format!("{path}.expression.arguments[0]"),
                plan,
                state,
                context,
            )
        }
        [argument] => compile_aggregate_target(
            argument,
            format!("{path}.expression.arguments[0]"),
            plan,
            state,
            context,
        ),
        [] if matches!(
            function_kind,
            AggregateFunction::Count | AggregateFunction::Collect
        ) =>
        {
            let variable = context
                .variable_function_argument(function)
                .ok_or_else(|| {
                    unsupported(
                        format!("{path}.expression.arguments"),
                        format!(
                            "{}() supports exactly one graph property or graph variable argument",
                            aggregate_function_name(function_kind)
                        ),
                    )
                })?;
            if let Some(target) = compile_optional_scalar_alias_aggregate_target(
                variable,
                format!("{path}.expression.arguments"),
                state,
            )? {
                return Ok(target);
            }
            Ok(AggregateTarget::VariableKey {
                variable: variable.to_string(),
            })
        }
        _ => Err(unsupported(
            format!("{path}.expression.arguments"),
            format!(
                "{}() supports exactly one graph property argument",
                aggregate_function_name(function_kind)
            ),
        )),
    }
}
