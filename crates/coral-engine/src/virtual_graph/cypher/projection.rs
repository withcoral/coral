//! Projection, RETURN/WITH, ordering, and row-modifier helpers split out of
//! `cypher.rs` without changing behavior.

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "Projection helpers intentionally inherit parent-private Cypher compile context."
)]
use super::*;

pub(crate) fn clear_final_return_outer_modifiers(
    single_query: &mut SingleQuery,
    path: &str,
) -> Result<(), CoreError> {
    let return_clause = final_return_clause_mut(single_query, path)?;
    return_clause.distinct = false;
    return_clause.order = None;
    return_clause.skip = None;
    return_clause.limit = None;
    Ok(())
}

#[derive(Debug, Clone)]
pub(crate) struct StaticAlternativeOuterProjectionPlan {
    items: Vec<StaticAlternativeOuterProjectionItem>,
    group_item_indices: Vec<usize>,
}

#[derive(Debug, Clone)]
pub(crate) enum StaticAlternativeOuterProjectionItem {
    Column {
        return_index: usize,
    },
    CountAll {
        alias: String,
    },
    Aggregate {
        function: AggregateFunction,
        source_alias: String,
        source_expression: Box<Expression>,
        distinct: bool,
        alias: String,
    },
}

pub(crate) fn analyze_static_alternative_outer_projection(
    single_query: &SingleQuery,
    path: &str,
    context: &CypherCompileContext,
) -> Result<Option<StaticAlternativeOuterProjectionPlan>, CoreError> {
    let return_clause = final_return_clause(single_query, path)?;
    let mut items = Vec::new();
    let mut group_item_indices = Vec::new();
    let has_outer_aggregate = return_clause
        .items
        .iter()
        .any(|item| expression_contains_aggregate(&item.expression));

    for (index, item) in return_clause.items.iter().enumerate() {
        if let Some(alias) = count_star_item_alias(item) {
            items.push(StaticAlternativeOuterProjectionItem::CountAll { alias });
        } else if let Some(item) =
            compile_static_alternative_outer_aggregate_item(item, index, path, context)?
        {
            items.push(item);
        } else if expression_contains_aggregate(&item.expression) {
            return Err(unsupported(
                format!("{path}.return.items[{index}].expression"),
                "pattern alternatives with property or non-count aggregate RETURN projections require staged query planning and are not supported yet",
            ));
        } else if has_outer_aggregate {
            group_item_indices.push(index);
            items.push(StaticAlternativeOuterProjectionItem::Column {
                return_index: index,
            });
        }
    }

    if has_outer_aggregate {
        return Ok(Some(StaticAlternativeOuterProjectionPlan {
            items,
            group_item_indices,
        }));
    }
    Ok(None)
}

pub(crate) fn compile_static_alternative_outer_projection(
    plan: Option<&StaticAlternativeOuterProjectionPlan>,
    branch_projection_names: &[String],
) -> Result<Option<GraphUnionOuterProjection>, CoreError> {
    let Some(plan) = plan else {
        return Ok(None);
    };
    let mut group_names = BTreeMap::new();
    for (position, return_index) in plan.group_item_indices.iter().enumerate() {
        let name = branch_projection_names
            .get(position)
            .cloned()
            .ok_or_else(|| {
                CoreError::internal("static alternative group projection names were not aligned")
            })?;
        group_names.insert(*return_index, name);
    }

    let mut items = Vec::with_capacity(plan.items.len());
    for item in &plan.items {
        match item {
            StaticAlternativeOuterProjectionItem::Column { return_index } => {
                let name = group_names.get(return_index).cloned().ok_or_else(|| {
                    CoreError::internal(
                        "static alternative group projection item had no branch output name",
                    )
                })?;
                items.push(GraphUnionOuterProjectionItem::Column { name });
            }
            StaticAlternativeOuterProjectionItem::CountAll { alias } => {
                items.push(GraphUnionOuterProjectionItem::CountAll {
                    alias: alias.clone(),
                });
            }
            StaticAlternativeOuterProjectionItem::Aggregate {
                function,
                source_alias,
                distinct,
                alias,
                ..
            } => items.push(GraphUnionOuterProjectionItem::Aggregate {
                function: *function,
                source: source_alias.clone(),
                distinct: *distinct,
                alias: alias.clone(),
            }),
        }
    }

    Ok(Some(GraphUnionOuterProjection {
        items,
        group_by: plan
            .group_item_indices
            .iter()
            .map(|return_index| {
                group_names.get(return_index).cloned().ok_or_else(|| {
                    CoreError::internal(
                        "static alternative group projection item had no GROUP BY name",
                    )
                })
            })
            .collect::<Result<Vec<_>, CoreError>>()?,
    }))
}

pub(crate) fn apply_static_alternative_outer_projection_rewrite(
    single_query: &mut SingleQuery,
    outer_projection: Option<&StaticAlternativeOuterProjectionPlan>,
    path: &str,
) -> Result<(), CoreError> {
    let Some(outer_projection) = outer_projection else {
        return Ok(());
    };
    let return_clause = final_return_clause_mut(single_query, path)?;
    return_clause.distinct = false;
    let aggregate_source_items = outer_projection
        .items
        .iter()
        .filter_map(|item| match item {
            StaticAlternativeOuterProjectionItem::Aggregate {
                source_alias,
                source_expression,
                ..
            } => Some((source_alias, source_expression.as_ref())),
            StaticAlternativeOuterProjectionItem::Column { .. }
            | StaticAlternativeOuterProjectionItem::CountAll { .. } => None,
        })
        .collect::<Vec<_>>();
    if outer_projection.group_item_indices.is_empty() && aggregate_source_items.is_empty() {
        let span = return_clause.span;
        return_clause.items = vec![ProjectionItem {
            expression: Expression::Literal(CypherLiteral::Number(NumberLiteral::Integer(1))),
            alias: Some(Variable {
                name: SymbolicName {
                    name: "__coral_count_row".to_string(),
                    span,
                },
            }),
        }];
    } else {
        let original_items = return_clause.items.clone();
        let mut rewritten_items = outer_projection
            .group_item_indices
            .iter()
            .map(|index| {
                original_items.get(*index).cloned().ok_or_else(|| {
                    CoreError::internal(
                        "static alternative group projection index was out of bounds",
                    )
                })
            })
            .collect::<Result<Vec<_>, CoreError>>()?;
        let span = return_clause.span;
        rewritten_items.extend(aggregate_source_items.into_iter().map(
            |(source_alias, source_expression)| ProjectionItem {
                expression: source_expression.clone(),
                alias: Some(Variable {
                    name: SymbolicName {
                        name: source_alias.clone(),
                        span,
                    },
                }),
            },
        ));
        return_clause.items = rewritten_items;
    }
    Ok(())
}

pub(crate) fn compile_static_alternative_outer_aggregate_item(
    item: &ProjectionItem,
    index: usize,
    path: &str,
    context: &CypherCompileContext,
) -> Result<Option<StaticAlternativeOuterProjectionItem>, CoreError> {
    let Some(function) = aggregate_function_call(&item.expression) else {
        return Ok(None);
    };
    let item_path = format!("{path}.return.items[{index}]");
    let function_kind =
        compile_aggregate_function(function, &format!("{item_path}.expression"), context)?
            .ok_or_else(|| {
                unsupported(
                    format!("{item_path}.expression"),
                    "pattern alternatives only support property aggregates after expansion",
                )
            })?;
    let source_expression = compile_static_alternative_outer_aggregate_source_expression(
        function,
        function_kind,
        &item_path,
        context,
    )?;
    Ok(Some(StaticAlternativeOuterProjectionItem::Aggregate {
        function: function_kind,
        source_alias: format!("__coral_agg_{index}"),
        source_expression: Box::new(source_expression),
        distinct: function.distinct,
        alias: item.alias.as_ref().map_or_else(
            || aggregate_function_name(function_kind).to_string(),
            variable_name,
        ),
    }))
}

pub(crate) fn compile_static_alternative_outer_aggregate_source_expression(
    function: &FunctionInvocation,
    function_kind: AggregateFunction,
    item_path: &str,
    context: &CypherCompileContext,
) -> Result<Expression, CoreError> {
    let target =
        compile_function_aggregate_target(function, function_kind, item_path, None, None, context);
    match target {
        Ok(AggregateTarget::Property(_)) => {
            let [argument] = function.arguments.as_slice() else {
                return Err(unsupported(
                    format!("{item_path}.expression.arguments"),
                    "pattern alternatives with property aggregates require one graph property argument",
                ));
            };
            Ok(argument.clone())
        }
        Ok(AggregateTarget::PresenceGatedProperty { .. }) => {
            let [argument] = function.arguments.as_slice() else {
                return Err(unsupported(
                    format!("{item_path}.expression.arguments"),
                    "pattern alternatives with optional relationship endpoint property aggregates require one graph property argument",
                ));
            };
            Ok(argument.clone())
        }
        Ok(AggregateTarget::Expression(_)) => Err(unsupported(
            format!("{item_path}.expression.arguments"),
            "pattern alternatives do not support aggregate expression targets yet",
        )),
        Ok(AggregateTarget::VariableKey { variable }) => match function_kind {
            AggregateFunction::Count => {
                if function.distinct {
                    Ok(graph_identity_function_expression_for_variable(
                        &variable, function,
                    ))
                } else {
                    Ok(graph_presence_function_expression_for_variable(
                        &variable, function,
                    ))
                }
            }
            AggregateFunction::Collect => Ok(graph_identity_function_expression_for_variable(
                &variable, function,
            )),
            _ => Err(unsupported(
                format!("{item_path}.expression.arguments"),
                "pattern alternatives only support count(variable) and collect(variable) over graph variables",
            )),
        },
        Ok(AggregateTarget::PresenceGatedVariableKey { .. }) => {
            if !matches!(
                function_kind,
                AggregateFunction::Count | AggregateFunction::Collect
            ) {
                return Err(unsupported(
                    format!("{item_path}.expression.arguments"),
                    "pattern alternatives only support count(endpoint) and collect(endpoint) over optional relationship endpoints",
                ));
            }
            let [argument] = function.arguments.as_slice() else {
                return Err(unsupported(
                    format!("{item_path}.expression.arguments"),
                    "pattern alternatives with optional relationship endpoint aggregates require one graph endpoint argument",
                ));
            };
            Ok(id_function_expression_for_expression(argument, function))
        }
        Err(error) => {
            if let Some(source) = compile_static_alternative_outer_endpoint_aggregate_source(
                function,
                function_kind,
                item_path,
            )? {
                Ok(source)
            } else {
                compile_static_alternative_outer_aggregate_expression_source(function, item_path)?
                    .ok_or(error)
            }
        }
    }
}

pub(crate) fn compile_static_alternative_outer_endpoint_aggregate_source(
    function: &FunctionInvocation,
    function_kind: AggregateFunction,
    path: &str,
) -> Result<Option<Expression>, CoreError> {
    let [argument] = function.arguments.as_slice() else {
        return Ok(None);
    };
    if is_relationship_endpoint_property_expression(argument) {
        return Ok(Some(argument.clone()));
    }
    if !is_relationship_endpoint_expression(argument) {
        return Ok(None);
    }
    if !matches!(
        function_kind,
        AggregateFunction::Count | AggregateFunction::Collect
    ) {
        return Err(unsupported(
            format!("{path}.expression.arguments[0]"),
            "pattern alternatives only support count(endpoint) and collect(endpoint) over relationship endpoints",
        ));
    }
    Ok(Some(id_function_expression_for_expression(
        argument, function,
    )))
}

pub(crate) fn is_relationship_endpoint_property_expression(expression: &Expression) -> bool {
    match expression {
        Expression::Parenthesized(inner) => is_relationship_endpoint_property_expression(inner),
        Expression::PropertyLookup { base, .. } => is_relationship_endpoint_expression(base),
        _ => false,
    }
}

pub(crate) fn is_relationship_endpoint_expression(expression: &Expression) -> bool {
    match expression {
        Expression::Parenthesized(inner) => is_relationship_endpoint_expression(inner),
        Expression::FunctionCall(function)
            if is_start_node_function(function) || is_end_node_function(function) =>
        {
            true
        }
        _ => false,
    }
}

pub(crate) fn compile_static_alternative_outer_aggregate_expression_source(
    function: &FunctionInvocation,
    path: &str,
) -> Result<Option<Expression>, CoreError> {
    let [argument] = function.arguments.as_slice() else {
        return Ok(None);
    };
    if !is_static_alternative_aggregate_scalar_source(argument) {
        return Ok(None);
    }
    if expression_contains_aggregate(argument) || expression_contains_subquery(argument) {
        return Err(unsupported(
            format!("{path}.expression.arguments[0]"),
            "pattern alternatives do not support nested aggregates or correlated subqueries inside aggregate expression targets",
        ));
    }
    Ok(Some(argument.clone()))
}

pub(crate) fn is_static_alternative_aggregate_scalar_source(expression: &Expression) -> bool {
    match expression {
        Expression::Parenthesized(inner) => is_static_alternative_aggregate_scalar_source(inner),
        expression if is_boolean_scalar_expression(expression) => true,
        expression if is_literal_expression(expression) => true,
        Expression::BinaryOp {
            op:
                CypherBinaryOperator::Add
                | CypherBinaryOperator::Subtract
                | CypherBinaryOperator::Multiply
                | CypherBinaryOperator::Divide
                | CypherBinaryOperator::Modulo
                | CypherBinaryOperator::Power,
            ..
        }
        | Expression::UnaryOp {
            op: UnaryOperator::Negate,
            ..
        }
        | Expression::Case(_)
        | Expression::ListIndex { .. } => true,
        Expression::FunctionCall(function) => {
            is_static_alternative_aggregate_scalar_function(function)
        }
        _ => false,
    }
}

pub(crate) fn is_static_alternative_aggregate_scalar_function(
    function: &FunctionInvocation,
) -> bool {
    is_id_function(function)
        || is_element_id_function(function)
        || is_type_function(function)
        || is_coalesce_function(function)
        || is_null_if_function(function)
        || is_date_function(function)
        || is_datetime_function(function)
        || is_localdatetime_function(function)
        || is_localtime_function(function)
        || is_to_string_function(function)
        || is_to_integer_function(function)
        || is_to_float_function(function)
        || is_to_boolean_function(function)
        || is_to_string_or_null_function(function)
        || is_to_integer_or_null_function(function)
        || is_to_float_or_null_function(function)
        || is_to_boolean_or_null_function(function)
        || is_static_list_cast_function(function)
        || is_to_lower_function(function)
        || is_to_upper_function(function)
        || is_trim_function(function)
        || is_ltrim_function(function)
        || is_rtrim_function(function)
        || is_replace_function(function)
        || is_head_function(function)
        || is_last_function(function)
        || is_tail_function(function)
        || is_character_length_function(function)
        || is_substring_function(function)
        || is_left_function(function)
        || is_right_function(function)
        || is_contains_function(function)
        || is_starts_with_function(function)
        || is_ends_with_function(function)
        || is_reverse_function(function)
        || is_abs_function(function)
        || is_ceil_function(function)
        || is_floor_function(function)
        || is_round_function(function)
        || is_sqrt_function(function)
        || is_sign_function(function)
        || is_exp_function(function)
        || is_log_function(function)
        || is_log10_function(function)
        || is_power_function(function)
        || is_pi_function(function)
        || is_e_function(function)
        || is_sin_function(function)
        || is_cos_function(function)
        || is_tan_function(function)
        || is_cot_function(function)
        || is_asin_function(function)
        || is_acos_function(function)
        || is_atan_function(function)
        || is_atan2_function(function)
        || is_degrees_function(function)
        || is_radians_function(function)
        || is_is_nan_function(function)
        || is_haversin_function(function)
}

pub(crate) fn expression_contains_subquery(expression: &Expression) -> bool {
    match expression {
        Expression::CountStar { .. }
        | Expression::CountSubquery(_)
        | Expression::CollectSubquery(_)
        | Expression::Exists(_) => true,
        Expression::FunctionCall(function) => {
            function.arguments.iter().any(expression_contains_subquery)
        }
        Expression::Literal(literal) => literal_contains_subquery(literal),
        Expression::PropertyLookup { base, .. }
        | Expression::IsNull { operand: base, .. }
        | Expression::UnaryOp { operand: base, .. }
        | Expression::Parenthesized(base) => expression_contains_subquery(base),
        Expression::NodeLabels { base, labels, .. } => {
            expression_contains_subquery(base)
                || labels.iter().any(label_expression_contains_subquery)
        }
        Expression::BinaryOp { lhs, rhs, .. } | Expression::In { lhs, rhs, .. } => {
            expression_contains_subquery(lhs) || expression_contains_subquery(rhs)
        }
        Expression::Comparison { lhs, operators, .. } => {
            expression_contains_subquery(lhs)
                || operators
                    .iter()
                    .any(|(_, rhs)| expression_contains_subquery(rhs))
        }
        Expression::ListIndex { list, index, .. } => {
            expression_contains_subquery(list) || expression_contains_subquery(index)
        }
        Expression::ListSlice {
            list, start, end, ..
        } => {
            expression_contains_subquery(list)
                || start.as_deref().is_some_and(expression_contains_subquery)
                || end.as_deref().is_some_and(expression_contains_subquery)
        }
        Expression::Case(case) => case_contains_subquery(case),
        Expression::ListComprehension(comprehension) => {
            list_comprehension_contains_subquery(comprehension)
        }
        Expression::PatternComprehension(comprehension) => {
            pattern_comprehension_contains_subquery(comprehension)
        }
        Expression::All(filter)
        | Expression::Any(filter)
        | Expression::None(filter)
        | Expression::Single(filter) => filter_expression_contains_subquery(filter),
        Expression::MapProjection(map) => map_projection_contains_subquery(map),
        Expression::Variable(_) | Expression::Parameter(_) | Expression::Pattern(_) => false,
    }
}

pub(crate) fn literal_contains_subquery(literal: &CypherLiteral) -> bool {
    match literal {
        CypherLiteral::List(list) => list.elements.iter().any(expression_contains_subquery),
        CypherLiteral::Map(map) => map
            .entries
            .iter()
            .any(|(_, value)| expression_contains_subquery(value)),
        CypherLiteral::Number(_)
        | CypherLiteral::String(_)
        | CypherLiteral::Boolean(_)
        | CypherLiteral::Null => false,
    }
}

pub(crate) fn case_contains_subquery(case: &CaseExpression) -> bool {
    case.scrutinee
        .as_deref()
        .is_some_and(expression_contains_subquery)
        || case.alternatives.iter().any(|alternative| {
            expression_contains_subquery(&alternative.when)
                || expression_contains_subquery(&alternative.then)
        })
        || case
            .default
            .as_deref()
            .is_some_and(expression_contains_subquery)
}

pub(crate) fn list_comprehension_contains_subquery(
    comprehension: &decypher::ast::expr::ListComprehension,
) -> bool {
    comprehension
        .filter
        .as_deref()
        .is_some_and(expression_contains_subquery)
        || comprehension
            .map
            .as_ref()
            .is_some_and(expression_contains_subquery)
}

pub(crate) fn pattern_comprehension_contains_subquery(
    comprehension: &decypher::ast::expr::PatternComprehension,
) -> bool {
    comprehension
        .where_clause
        .as_ref()
        .is_some_and(expression_contains_subquery)
        || expression_contains_subquery(&comprehension.map)
}

pub(crate) fn filter_expression_contains_subquery(
    filter: &decypher::ast::expr::FilterExpression,
) -> bool {
    expression_contains_subquery(&filter.collection)
        || filter
            .predicate
            .as_deref()
            .is_some_and(expression_contains_subquery)
}

pub(crate) fn map_projection_contains_subquery(map: &decypher::ast::expr::MapProjection) -> bool {
    map.items.iter().any(|item| match item {
        decypher::ast::expr::MapProjectionItem::Literal { value, .. } => {
            expression_contains_subquery(value)
        }
        decypher::ast::expr::MapProjectionItem::AllProperties { .. }
        | decypher::ast::expr::MapProjectionItem::PropertyLookup { .. } => false,
    })
}

pub(crate) fn label_expression_contains_subquery(expression: &LabelExpression) -> bool {
    match expression {
        LabelExpression::Dynamic {
            expression: dynamic,
            ..
        } => expression_contains_subquery(dynamic),
        LabelExpression::Or { lhs, rhs, .. } | LabelExpression::And { lhs, rhs, .. } => {
            label_expression_contains_subquery(lhs) || label_expression_contains_subquery(rhs)
        }
        LabelExpression::Not { inner, .. } | LabelExpression::Group { inner, .. } => {
            label_expression_contains_subquery(inner)
        }
        LabelExpression::Static(_) => false,
    }
}

pub(crate) fn graph_identity_function_expression_for_variable(
    variable: &str,
    source_function: &FunctionInvocation,
) -> Expression {
    function_expression_for_variable(INTERNAL_GRAPH_IDENTITY_FUNCTION, variable, source_function)
}

pub(crate) fn graph_presence_function_expression_for_variable(
    variable: &str,
    source_function: &FunctionInvocation,
) -> Expression {
    function_expression_for_variable(INTERNAL_GRAPH_PRESENCE_FUNCTION, variable, source_function)
}

pub(crate) fn function_expression_for_variable(
    function_name: &str,
    variable: &str,
    source_function: &FunctionInvocation,
) -> Expression {
    let span = source_function.span;
    Expression::FunctionCall(FunctionInvocation {
        name: vec![SymbolicName {
            name: function_name.to_string(),
            span,
        }],
        distinct: false,
        arguments: vec![Expression::Variable(Variable {
            name: SymbolicName {
                name: variable.to_string(),
                span,
            },
        })],
        span,
    })
}

pub(crate) fn id_function_expression_for_expression(
    argument: &Expression,
    source_function: &FunctionInvocation,
) -> Expression {
    let span = source_function.span;
    Expression::FunctionCall(FunctionInvocation {
        name: vec![SymbolicName {
            name: "id".to_string(),
            span,
        }],
        distinct: false,
        arguments: vec![argument.clone()],
        span,
    })
}

pub(crate) fn aggregate_function_call(expression: &Expression) -> Option<&FunctionInvocation> {
    match expression {
        Expression::Parenthesized(inner) => aggregate_function_call(inner),
        Expression::FunctionCall(function) if is_aggregate_function_call(function) => {
            Some(function)
        }
        _ => None,
    }
}

pub(crate) fn count_star_item_alias(item: &ProjectionItem) -> Option<String> {
    if !matches!(item.expression, Expression::CountStar { .. }) {
        return None;
    }
    Some(
        item.alias
            .as_ref()
            .map_or_else(|| "count".to_string(), variable_name),
    )
}

#[derive(Debug, Clone)]
pub(crate) struct StaticAlternativeHiddenOrderPlan {
    pub(crate) items: Vec<StaticAlternativeHiddenOrderItem>,
}

#[derive(Debug, Clone)]
pub(crate) struct StaticAlternativeHiddenOrderItem {
    pub(crate) order_index: usize,
    pub(crate) expression: Expression,
    pub(crate) alias: String,
}

impl StaticAlternativeHiddenOrderPlan {
    fn alias_for_order_index(&self, order_index: usize) -> Option<&str> {
        self.items
            .iter()
            .find(|item| item.order_index == order_index)
            .map(|item| item.alias.as_str())
    }
}

pub(crate) fn analyze_static_alternative_hidden_order(
    single_query: &SingleQuery,
    outer_projection: Option<&StaticAlternativeOuterProjectionPlan>,
    context: &CypherCompileContext,
    path: &str,
) -> Result<Option<StaticAlternativeHiddenOrderPlan>, CoreError> {
    let return_clause = final_return_clause(single_query, path)?;
    let Some(order) = &return_clause.order else {
        return Ok(None);
    };
    let mut hidden_items = Vec::new();
    let projection_names = return_clause
        .items
        .iter()
        .map(|item| return_item_projection_name_with_context(item, Some(context)))
        .collect::<Vec<_>>();
    for (index, item) in order.items.iter().enumerate() {
        if resolve_projected_static_alternative_outer_order_alias(
            &item.expression,
            return_clause,
            &projection_names,
            format!("{path}.return.order.items[{index}].expression"),
        )?
        .is_some()
        {
            continue;
        }
        if outer_projection.is_some() {
            return Err(unsupported(
                format!("{path}.return.order.items[{index}].expression"),
                "pattern alternatives with aggregate RETURN projections cannot ORDER BY unprojected expressions yet",
            ));
        }
        if return_clause.distinct {
            return Err(unsupported(
                format!("{path}.return.order.items[{index}].expression"),
                "pattern alternatives with RETURN DISTINCT cannot ORDER BY unprojected expressions yet",
            ));
        }
        hidden_items.push(StaticAlternativeHiddenOrderItem {
            order_index: index,
            expression: item.expression.clone(),
            alias: format!("__coral_order_{index}"),
        });
    }
    if hidden_items.is_empty() {
        Ok(None)
    } else {
        Ok(Some(StaticAlternativeHiddenOrderPlan {
            items: hidden_items,
        }))
    }
}

pub(crate) fn return_item_projection_name(item: &ProjectionItem) -> String {
    return_item_projection_name_with_context(item, None)
}

pub(crate) fn return_item_projection_name_with_context(
    item: &ProjectionItem,
    context: Option<&CypherCompileContext>,
) -> String {
    item.alias.as_ref().map_or_else(
        || match &item.expression {
            Expression::PropertyLookup { base, property, .. } => match base.as_ref() {
                Expression::Variable(variable) => {
                    format!("{}_{}", variable_name(variable), property.name.name)
                }
                _ => "expression".to_string(),
            },
            Expression::Variable(variable) => variable_name(variable),
            Expression::CountStar { .. } => "count".to_string(),
            Expression::FunctionCall(function) => {
                if is_aggregate_function_call(function)
                    && let Some(name) =
                        context.and_then(|context| context.function_source_text(function))
                {
                    name
                } else if let Some(alias) = aggregate_function_default_alias(function) {
                    alias.to_string()
                } else {
                    default_scalar_function_alias(function)
                }
            }
            Expression::Case(_) => "case".to_string(),
            _ => "expression".to_string(),
        },
        variable_name,
    )
}

pub(crate) fn apply_static_alternative_hidden_order_rewrite(
    single_query: &mut SingleQuery,
    hidden_order: Option<&StaticAlternativeHiddenOrderPlan>,
    path: &str,
) -> Result<(), CoreError> {
    let Some(hidden_order) = hidden_order else {
        return Ok(());
    };
    let return_clause = final_return_clause_mut(single_query, path)?;
    let span = return_clause.span;
    return_clause
        .items
        .extend(hidden_order.items.iter().map(|item| ProjectionItem {
            expression: item.expression.clone(),
            alias: Some(Variable {
                name: SymbolicName {
                    name: item.alias.clone(),
                    span,
                },
            }),
        }));
    Ok(())
}

pub(crate) fn compile_static_alternative_hidden_order_outer_projection(
    outer_projection: Option<GraphUnionOuterProjection>,
    hidden_order: Option<&StaticAlternativeHiddenOrderPlan>,
    branch_projection_names: &[String],
    return_item_count: usize,
) -> Result<Option<GraphUnionOuterProjection>, CoreError> {
    if hidden_order.is_none() {
        return Ok(outer_projection);
    }
    if outer_projection.is_some() {
        return Err(CoreError::internal(
            "hidden static alternative ORDER BY should have been rejected for aggregate outer projections",
        ));
    }
    let items = branch_projection_names
        .get(..return_item_count)
        .ok_or_else(|| {
            CoreError::internal(
                "static alternative hidden ORDER BY projection names were not aligned",
            )
        })?
        .iter()
        .cloned()
        .map(|name| GraphUnionOuterProjectionItem::Column { name })
        .collect();
    Ok(Some(GraphUnionOuterProjection {
        items,
        group_by: Vec::new(),
    }))
}

pub(crate) fn compile_static_alternative_outer_order_by(
    single_query: &SingleQuery,
    projection_names: &[String],
    hidden_order: Option<&StaticAlternativeHiddenOrderPlan>,
    context: &CypherCompileContext,
    path: &str,
) -> Result<Vec<OrderKey>, CoreError> {
    let return_clause = final_return_clause(single_query, path)?;
    let Some(order) = &return_clause.order else {
        return Ok(Vec::new());
    };

    let mut order_by = Vec::with_capacity(order.items.len());
    for (index, item) in order.items.iter().enumerate() {
        let alias = resolve_projected_static_alternative_outer_order_alias(
            &item.expression,
            return_clause,
            projection_names,
            format!("{path}.return.order.items[{index}].expression"),
        )?
        .or_else(|| {
            hidden_order.and_then(|hidden_order| {
                hidden_order
                    .alias_for_order_index(index)
                    .map(ToString::to_string)
            })
        })
        .ok_or_else(|| {
            unsupported(
                format!("{path}.return.order.items[{index}].expression"),
                "pattern alternatives with global ORDER BY currently require projected aliases, projected expressions, or row-preserving hidden sort expressions",
            )
        })?;
        order_by.push(OrderKey {
            expression: OrderExpression::ProjectionAlias(alias),
            direction: match item.direction {
                Some(SortDirection::Descending) => OrderDirection::Descending,
                Some(SortDirection::Ascending) | None => OrderDirection::Ascending,
            },
            nulls: context.order_null_placement(item),
        });
    }
    Ok(order_by)
}

pub(crate) fn resolve_projected_static_alternative_outer_order_alias(
    expression: &Expression,
    return_clause: &Return,
    projection_names: &[String],
    path: impl Into<String>,
) -> Result<Option<String>, CoreError> {
    let path = path.into();
    if let Expression::Variable(variable) = expression {
        let alias = variable_name(variable);
        if projection_names.iter().any(|name| name == &alias) {
            return Ok(Some(alias));
        }
        return Err(unsupported(
            path,
            format!("ORDER BY alias '{alias}' does not match a RETURN projection"),
        ));
    }

    for (index, item) in return_clause.items.iter().enumerate() {
        if expressions_equivalent_ignoring_span(&item.expression, expression) {
            return projection_names
                .get(index)
                .cloned()
                .map(Some)
                .ok_or_else(|| {
                    CoreError::internal(
                        "RETURN projection names were not aligned with RETURN items",
                    )
                });
        }
    }

    Ok(None)
}

pub(crate) fn expressions_equivalent_ignoring_span(left: &Expression, right: &Expression) -> bool {
    match (left, right) {
        (Expression::Parenthesized(left), right) => {
            expressions_equivalent_ignoring_span(left, right)
        }
        (left, Expression::Parenthesized(right)) => {
            expressions_equivalent_ignoring_span(left, right)
        }
        (Expression::Variable(left), Expression::Variable(right)) => {
            variable_name(left) == variable_name(right)
        }
        (Expression::Parameter(left), Expression::Parameter(right)) => {
            left.name.name == right.name.name
        }
        (
            Expression::PropertyLookup {
                base: left_base,
                property: left_property,
                ..
            },
            Expression::PropertyLookup {
                base: right_base,
                property: right_property,
                ..
            },
        ) => {
            left_property.name.name == right_property.name.name
                && expressions_equivalent_ignoring_span(left_base, right_base)
        }
        (Expression::FunctionCall(left), Expression::FunctionCall(right)) => {
            left.name.len() == right.name.len()
                && left
                    .name
                    .iter()
                    .zip(&right.name)
                    .all(|(left, right)| left.name == right.name)
                && left.distinct == right.distinct
                && left.arguments.len() == right.arguments.len()
                && left
                    .arguments
                    .iter()
                    .zip(&right.arguments)
                    .all(|(left, right)| expressions_equivalent_ignoring_span(left, right))
        }
        (
            Expression::UnaryOp {
                op: left_op,
                operand: left_operand,
                ..
            },
            Expression::UnaryOp {
                op: right_op,
                operand: right_operand,
                ..
            },
        ) => {
            left_op == right_op && expressions_equivalent_ignoring_span(left_operand, right_operand)
        }
        (
            Expression::BinaryOp {
                op: left_op,
                lhs: left_lhs,
                rhs: left_rhs,
                ..
            },
            Expression::BinaryOp {
                op: right_op,
                lhs: right_lhs,
                rhs: right_rhs,
                ..
            },
        ) => {
            left_op == right_op
                && expressions_equivalent_ignoring_span(left_lhs, right_lhs)
                && expressions_equivalent_ignoring_span(left_rhs, right_rhs)
        }
        (Expression::Literal(left), Expression::Literal(right)) => {
            literals_equivalent_ignoring_span(left, right)
        }
        (Expression::CountStar { .. }, Expression::CountStar { .. }) => true,
        _ => false,
    }
}

pub(crate) fn literals_equivalent_ignoring_span(
    left: &CypherLiteral,
    right: &CypherLiteral,
) -> bool {
    match (left, right) {
        (CypherLiteral::Number(left), CypherLiteral::Number(right)) => left == right,
        (CypherLiteral::String(left), CypherLiteral::String(right)) => left.value == right.value,
        (CypherLiteral::Boolean(left), CypherLiteral::Boolean(right)) => left == right,
        (CypherLiteral::Null, CypherLiteral::Null) => true,
        (CypherLiteral::List(left), CypherLiteral::List(right)) => {
            left.elements.len() == right.elements.len()
                && left
                    .elements
                    .iter()
                    .zip(&right.elements)
                    .all(|(left, right)| expressions_equivalent_ignoring_span(left, right))
        }
        (CypherLiteral::Map(left), CypherLiteral::Map(right)) => {
            left.entries.len() == right.entries.len()
                && left.entries.iter().zip(&right.entries).all(
                    |((left_key, left_value), (right_key, right_value))| {
                        left_key.name.name == right_key.name.name
                            && expressions_equivalent_ignoring_span(left_value, right_value)
                    },
                )
        }
        _ => false,
    }
}

pub(crate) fn compile_static_alternative_outer_skip_limit(
    single_query: &SingleQuery,
    context: &CypherCompileContext,
    path: &str,
) -> Result<(Option<u64>, Option<u64>), CoreError> {
    let return_clause = final_return_clause(single_query, path)?;
    let skip = return_clause
        .skip
        .as_ref()
        .map(|skip| compile_skip(skip, format!("{path}.return.skip"), context))
        .transpose()?;
    let limit = return_clause
        .limit
        .as_ref()
        .map(|limit| compile_limit(limit, format!("{path}.return.limit"), context))
        .transpose()?;
    Ok((skip, limit))
}

pub(crate) fn final_return_clause<'a>(
    single_query: &'a SingleQuery,
    path: &str,
) -> Result<&'a Return, CoreError> {
    match &single_query.kind {
        SingleQueryKind::SinglePart(single_part) => {
            return_clause_from_single_part(single_part, path)
        }
        SingleQueryKind::MultiPart(multi_part) => {
            return_clause_from_single_part(&multi_part.final_part, format!("{path}.final_part"))
        }
    }
}

pub(crate) fn final_return_clause_mut<'a>(
    single_query: &'a mut SingleQuery,
    path: &str,
) -> Result<&'a mut Return, CoreError> {
    match &mut single_query.kind {
        SingleQueryKind::SinglePart(single_part) => {
            return_clause_mut_from_single_part(single_part, path)
        }
        SingleQueryKind::MultiPart(multi_part) => return_clause_mut_from_single_part(
            &mut multi_part.final_part,
            format!("{path}.final_part"),
        ),
    }
}

pub(crate) fn return_clause_mut_from_single_part(
    query: &mut SinglePartQuery,
    path: impl Into<String>,
) -> Result<&mut Return, CoreError> {
    let path = path.into();
    match &mut query.body {
        SinglePartBody::Return(return_clause) => Ok(return_clause),
        SinglePartBody::Updating { .. } => Err(unsupported(
            path,
            "write clauses are not supported by Coral virtual graphs",
        )),
        SinglePartBody::Finish(_) => Err(unsupported(
            path,
            "FINISH is not supported because virtual graph queries must return rows",
        )),
    }
}

pub(crate) fn compile_terminal_with_projection(
    query: &MultiPartQuery,
    context: &CypherCompileContext,
) -> Result<Option<GraphPlan>, CoreError> {
    let terminal_projection_candidate = query
        .parts
        .iter()
        .any(|part| with_requires_terminal_projection(&part.with));
    if !terminal_projection_candidate {
        return Ok(None);
    }
    if query.parts.len() != 1 || !query.final_part.reading_clauses.is_empty() {
        return Ok(None);
    }
    let [part] = query.parts.as_slice() else {
        return Err(unsupported(
            "query.parts",
            "terminal WITH projections currently support exactly one MATCH ... WITH ... RETURN query part",
        ));
    };
    if !part.updating_clauses.is_empty() {
        return Err(unsupported(
            "parts[0].updating_clauses",
            "write clauses are not supported by Coral virtual graphs",
        ));
    }
    let return_clause = return_clause_from_single_part(&query.final_part, "final_part")?;
    let mut plan = GraphPlan::default();
    let mut state = compile_state_for_multi_part(query, context);
    compile_reading_clauses_into(
        &part.reading_clauses,
        "parts[0].match",
        &mut plan,
        &mut state,
        context,
    )?;

    compile_terminal_with_clause(&part.with, &mut plan, &state, context)?;
    apply_terminal_return_projection_aliases(
        return_clause,
        &mut plan,
        &state,
        context,
        part.with.star,
    )?;
    apply_terminal_return_modifiers(return_clause, &mut plan, context)?;
    reject_ignored_path_variable_references(&plan, &state, "with")?;
    Ok(Some(plan))
}

pub(crate) fn with_requires_terminal_projection(with: &With) -> bool {
    with.items
        .iter()
        .any(|item| !matches!(&item.expression, Expression::Variable(_)))
}

pub(crate) fn compile_terminal_with_graph_modifiers(
    query: &MultiPartQuery,
    context: &CypherCompileContext,
) -> Result<Option<GraphPlan>, CoreError> {
    let terminal_graph_candidate = query
        .parts
        .iter()
        .any(|part| part.with.distinct || with_has_row_modifiers(&part.with));
    if !terminal_graph_candidate {
        return Ok(None);
    }
    let [part] = query.parts.as_slice() else {
        return Err(unsupported(
            "query.parts",
            "terminal graph-variable WITH DISTINCT, ORDER BY, SKIP, and LIMIT currently support exactly one MATCH ... WITH ... RETURN query part",
        ));
    };
    if with_requires_terminal_projection(&part.with) {
        return Ok(None);
    }
    if !part.updating_clauses.is_empty() {
        return Err(unsupported(
            "parts[0].updating_clauses",
            "write clauses are not supported by Coral virtual graphs",
        ));
    }
    if !query.final_part.reading_clauses.is_empty() {
        return Err(unsupported(
            "final_part.reading_clauses",
            "WITH DISTINCT, ORDER BY, SKIP, and LIMIT before another MATCH require staged query planning and are not supported yet",
        ));
    }

    let return_clause = return_clause_from_single_part(&query.final_part, "final_part")?;
    if with_has_row_modifiers(&part.with)
        && (return_clause.order.is_some()
            || return_clause.skip.is_some()
            || return_clause.limit.is_some())
    {
        return Err(unsupported(
            "final_part.return",
            "terminal WITH and RETURN cannot both define ORDER BY, SKIP, or LIMIT without staged query planning",
        ));
    }

    let mut plan = GraphPlan::default();
    let mut state = compile_state_for_multi_part(query, context);
    compile_reading_clauses_into(
        &part.reading_clauses,
        "parts[0].match",
        &mut plan,
        &mut state,
        context,
    )?;
    if let Some(predicate) =
        apply_transparent_with_scope(&part.with, &mut plan, &mut state, "parts[0].with", context)?
    {
        append_predicate_expression(predicate, &mut plan);
    }
    apply_terminal_graph_with_modifiers(&part.with, &mut plan, &state, context)?;
    if part.with.distinct {
        validate_terminal_distinct_graph_return(return_clause, &plan, &state)?;
    }
    compile_return(return_clause, &mut plan, &state, context)?;
    if part.with.distinct {
        plan.distinct = true;
    }
    reject_ignored_path_variable_references(&plan, &state, "return")?;
    Ok(Some(plan))
}

pub(crate) fn with_has_row_modifiers(with: &With) -> bool {
    with.order.is_some() || with.skip.is_some() || with.limit.is_some()
}

pub(crate) fn validate_terminal_distinct_graph_return(
    return_clause: &Return,
    plan: &GraphPlan,
    state: &CypherCompileState,
) -> Result<(), CoreError> {
    if return_clause.star {
        if return_clause.items.is_empty() {
            return Ok(());
        }
        return Err(unsupported(
            "final_part.return.star",
            "RETURN * mixed with explicit projections after WITH DISTINCT requires staged query planning and is not supported yet",
        ));
    }

    let visible = visible_graph_variables(plan, state);
    for (index, item) in return_clause.items.iter().enumerate() {
        let Some(variable) = terminal_return_graph_variable(&item.expression) else {
            return Err(unsupported(
                format!("final_part.return.items[{index}].expression"),
                "terminal WITH DISTINCT over graph variables currently requires RETURN of carried graph variables or RETURN *; scalar projections require staged query planning and are not supported yet",
            ));
        };
        if !visible.contains(&variable) {
            return Err(unsupported(
                format!("final_part.return.items[{index}].expression"),
                format!("terminal RETURN references unknown WITH graph variable '{variable}'"),
            ));
        }
    }
    Ok(())
}

pub(crate) fn terminal_return_graph_variable(expression: &Expression) -> Option<String> {
    match expression {
        Expression::Parenthesized(inner) => terminal_return_graph_variable(inner),
        Expression::Variable(variable) => Some(variable_name(variable)),
        _ => None,
    }
}

pub(crate) fn apply_terminal_graph_with_modifiers(
    with: &With,
    plan: &mut GraphPlan,
    state: &CypherCompileState,
    context: &CypherCompileContext,
) -> Result<(), CoreError> {
    if let Some(order) = &with.order {
        for (index, item) in order.items.iter().enumerate() {
            plan.order_by.push(OrderKey {
                expression: compile_order_expression(
                    &item.expression,
                    &[],
                    plan,
                    state,
                    context,
                    format!("with.order.items[{index}].expression"),
                )?,
                direction: match item.direction {
                    Some(SortDirection::Descending) => OrderDirection::Descending,
                    Some(SortDirection::Ascending) | None => OrderDirection::Ascending,
                },
                nulls: context.order_null_placement(item),
            });
        }
    }
    if let Some(skip) = &with.skip {
        plan.skip = Some(compile_skip(skip, "with.skip", context)?);
    }
    if let Some(limit) = &with.limit {
        plan.limit = Some(compile_limit(limit, "with.limit", context)?);
    }
    Ok(())
}

pub(crate) fn compile_terminal_with_clause(
    with: &With,
    plan: &mut GraphPlan,
    state: &CypherCompileState,
    context: &CypherCompileContext,
) -> Result<(), CoreError> {
    plan.distinct = with.distinct;
    if with.items.is_empty() {
        return Err(unsupported(
            "with.items",
            "WITH must include at least one projection",
        ));
    }

    let mut aliases = BTreeSet::new();
    for (index, item) in with.items.iter().enumerate() {
        let Some(alias) = item.alias.as_ref().map(variable_name) else {
            return Err(unsupported(
                format!("with.items[{index}].alias"),
                "terminal WITH projections require explicit aliases",
            ));
        };
        if !aliases.insert(alias.clone()) {
            return Err(unsupported(
                format!("with.items[{index}].alias"),
                format!("terminal WITH projection alias '{alias}' is defined more than once"),
            ));
        }
        if matches!(&item.expression, Expression::Variable(_)) {
            return Err(unsupported(
                format!("with.items[{index}].expression"),
                "terminal WITH projections support graph properties and aggregates, not graph variable aliases",
            ));
        }
        let projection =
            compile_projection(item, format!("with.items[{index}]"), context, plan, state)?;
        plan.projections.push(projection);
    }
    if let Some(where_clause) = &with.where_clause {
        plan.post_projection_predicate = Some(compile_projection_predicate_expression(
            where_clause,
            "with.where",
            context,
        )?);
    }

    if let Some(order) = &with.order {
        for (index, item) in order.items.iter().enumerate() {
            plan.order_by.push(OrderKey {
                expression: compile_terminal_alias_order_expression(
                    &item.expression,
                    &plan.projections,
                    format!("with.order.items[{index}].expression"),
                )?,
                direction: match item.direction {
                    Some(SortDirection::Descending) => OrderDirection::Descending,
                    Some(SortDirection::Ascending) | None => OrderDirection::Ascending,
                },
                nulls: context.order_null_placement(item),
            });
        }
    }
    if let Some(skip) = &with.skip {
        plan.skip = Some(compile_skip(skip, "with.skip", context)?);
    }
    if let Some(limit) = &with.limit {
        plan.limit = Some(compile_limit(limit, "with.limit", context)?);
    }
    Ok(())
}

pub(crate) fn apply_terminal_return_projection_aliases(
    return_clause: &Return,
    plan: &mut GraphPlan,
    state: &CypherCompileState,
    context: &CypherCompileContext,
    with_star: bool,
) -> Result<(), CoreError> {
    if return_clause.star {
        if return_clause.items.is_empty() {
            if with_star {
                expand_terminal_with_star_return_star(
                    plan,
                    state,
                    context,
                    "final_part.return.star",
                )?;
            }
            return Ok(());
        }
        return Err(unsupported(
            "final_part.return.star",
            "RETURN * mixed with explicit projections after WITH requires scoped query planning and is not supported yet",
        ));
    }
    let mut reordered = Vec::with_capacity(plan.projections.len());
    let mut available = plan.projections.clone();
    let required_aliases = plan
        .projections
        .iter()
        .filter_map(projection_output_alias)
        .map(str::to_string)
        .collect::<BTreeSet<_>>();
    let mut returned_aliases = BTreeSet::new();
    let mut component_aliases = BTreeSet::new();
    for (index, item) in return_clause.items.iter().enumerate() {
        let item_path = format!("final_part.return.items[{index}].expression");
        if let Some((projection, consumed_alias)) =
            compile_optional_terminal_temporal_component_projection(
                &item.expression,
                item.alias.as_ref(),
                &available,
                plan,
                context,
                item_path.clone(),
            )?
        {
            if returned_aliases.contains(&consumed_alias) {
                return Err(unsupported(
                    item_path,
                    format!(
                        "terminal RETURN projects WITH alias '{consumed_alias}' more than once"
                    ),
                ));
            }
            component_aliases.insert(consumed_alias);
            reordered.push(projection);
            continue;
        }
        let Expression::Variable(variable) = &item.expression else {
            return Err(unsupported(
                format!("final_part.return.items[{index}].expression"),
                "terminal RETURN after WITH must project WITH aliases",
            ));
        };
        let alias = variable_name(variable);
        if component_aliases.contains(&alias) {
            return Err(unsupported(
                format!("final_part.return.items[{index}].expression"),
                format!("terminal RETURN projects WITH alias '{alias}' more than once"),
            ));
        }
        if !returned_aliases.insert(alias.clone()) {
            return Err(unsupported(
                format!("final_part.return.items[{index}].expression"),
                format!("terminal RETURN projects WITH alias '{alias}' more than once"),
            ));
        }
        let position = available
            .iter()
            .position(|projection| projection_output_alias(projection) == Some(alias.as_str()))
            .ok_or_else(|| {
                unsupported(
                    format!("final_part.return.items[{index}].expression"),
                    format!("terminal RETURN references unknown WITH alias '{alias}'"),
                )
            })?;
        let mut projection = available.remove(position);
        if let Some(alias) = &item.alias {
            set_projection_output_alias(&mut projection, variable_name(alias));
        }
        reordered.push(projection);
    }
    let consumed_aliases = returned_aliases
        .union(&component_aliases)
        .cloned()
        .collect::<BTreeSet<_>>();
    if consumed_aliases != required_aliases {
        return Err(unsupported(
            "final_part.return.items",
            "terminal RETURN after WITH must pass through every WITH alias",
        ));
    }
    plan.projections = reordered;
    Ok(())
}

pub(crate) fn expand_terminal_with_star_return_star(
    plan: &mut GraphPlan,
    state: &CypherCompileState,
    context: &CypherCompileContext,
    path: &str,
) -> Result<(), CoreError> {
    let explicit_projections = std::mem::take(&mut plan.projections);
    if explicit_projections
        .iter()
        .any(terminal_with_star_projection_requires_grouping)
    {
        return Err(unsupported(
            path,
            "RETURN * after WITH * with aggregate aliases requires grouped scoped query planning and is not supported yet",
        ));
    }
    compile_return_star(plan, state, context, path)?;
    for projection in explicit_projections {
        push_unique_terminal_with_star_projection(plan, projection, path)?;
    }
    Ok(())
}

pub(crate) fn terminal_with_star_projection_requires_grouping(projection: &Projection) -> bool {
    matches!(
        projection,
        Projection::CountAll { .. } | Projection::Aggregate { .. }
    )
}

pub(crate) fn push_unique_terminal_with_star_projection(
    plan: &mut GraphPlan,
    projection: Projection,
    path: &str,
) -> Result<(), CoreError> {
    let alias = projection.output_name();
    if plan
        .projections
        .iter()
        .any(|existing| existing.output_name() == alias)
    {
        return Err(unsupported(
            path.to_string(),
            format!("RETURN * expansion produced duplicate output column '{alias}'"),
        ));
    }
    plan.projections.push(projection);
    Ok(())
}

pub(crate) fn set_projection_output_alias(projection: &mut Projection, alias: String) {
    match projection {
        Projection::Property {
            alias: projection_alias,
            ..
        } => *projection_alias = Some(alias),
        Projection::Key {
            alias: projection_alias,
            ..
        }
        | Projection::ElementId {
            alias: projection_alias,
            ..
        }
        | Projection::NodeLabels {
            alias: projection_alias,
            ..
        }
        | Projection::PropertyKeys {
            alias: projection_alias,
            ..
        }
        | Projection::RelationshipType {
            alias: projection_alias,
            ..
        }
        | Projection::Literal {
            alias: projection_alias,
            ..
        }
        | Projection::LiteralList {
            alias: projection_alias,
            ..
        }
        | Projection::Expression {
            alias: projection_alias,
            ..
        }
        | Projection::CountAll {
            alias: projection_alias,
        }
        | Projection::Aggregate {
            alias: projection_alias,
            ..
        } => *projection_alias = alias,
    }
}

pub(crate) fn scalar_alias_names(state: &CypherCompileState) -> BTreeSet<String> {
    state
        .scalar_aliases
        .iter()
        .map(Projection::output_name)
        .collect()
}

pub(crate) fn scalar_alias_projection<'a>(
    state: &'a CypherCompileState,
    alias: &str,
) -> Option<&'a Projection> {
    state
        .scalar_aliases
        .iter()
        .find(|projection| projection.output_name() == alias)
}

pub(crate) fn scalar_alias_projection_expression(
    projection: &Projection,
    path: impl Into<String>,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    match projection {
        Projection::Property { property, .. } => Ok(ScalarExpression::Property(property.clone())),
        Projection::Key { variable, .. } => Ok(ScalarExpression::Key {
            variable: variable.clone(),
        }),
        Projection::ElementId { variable, .. } => Ok(ScalarExpression::ElementId {
            variable: variable.clone(),
        }),
        Projection::RelationshipType {
            variable,
            relationship_type,
            ..
        } => Ok(ScalarExpression::RelationshipType {
            variable: variable.clone(),
            relationship_type: relationship_type.clone(),
        }),
        Projection::NodeLabels {
            variable, label, ..
        } => Ok(ScalarExpression::NodeLabels {
            variable: variable.clone(),
            label: label.clone(),
        }),
        Projection::PropertyKeys { variable, .. } => Ok(ScalarExpression::PropertyKeys {
            variable: variable.clone(),
        }),
        Projection::Literal { literal, .. } => Ok(ScalarExpression::Literal(literal.clone())),
        Projection::LiteralList { literals, .. } => Ok(ScalarExpression::LiteralList {
            literals: literals.clone(),
        }),
        Projection::Expression { expression, .. } => Ok(expression.clone()),
        Projection::CountAll { .. } | Projection::Aggregate { .. } => Err(unsupported(
            path,
            "aggregate WITH aliases require staged query planning and are not supported before another MATCH",
        )),
    }
}

pub(crate) fn expression_variable_name(expression: &Expression) -> Option<String> {
    match expression {
        Expression::Parenthesized(inner) => expression_variable_name(inner),
        Expression::Variable(variable) => Some(variable_name(variable)),
        _ => None,
    }
}

pub(crate) fn compile_optional_scalar_alias_expression(
    expression: &Expression,
    path: impl Into<String>,
    state: Option<&CypherCompileState>,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    let Some(state) = state else {
        return Ok(None);
    };
    let Some(alias) = expression_variable_name(expression) else {
        return Ok(None);
    };
    let Some(projection) = scalar_alias_projection(state, &alias) else {
        return Ok(None);
    };
    scalar_alias_projection_expression(projection, path).map(Some)
}

pub(crate) fn compile_optional_scalar_alias_return_item(
    item: &ProjectionItem,
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<Option<Projection>, CoreError> {
    let path = path.into();
    let Some(alias) = expression_variable_name(&item.expression) else {
        return Ok(None);
    };
    let Some(projection) = scalar_alias_projection(state, &alias) else {
        return Ok(None);
    };
    let mut projection = projection.clone();
    if let Some(alias) = &item.alias {
        set_projection_output_alias(&mut projection, variable_name(alias));
    }
    reject_ignored_path_variable_references_in_projection(&projection, state, path)?;
    Ok(Some(projection))
}

pub(crate) fn apply_terminal_return_modifiers(
    return_clause: &Return,
    plan: &mut GraphPlan,
    context: &CypherCompileContext,
) -> Result<(), CoreError> {
    plan.distinct |= return_clause.distinct;
    if (return_clause.order.is_some()
        || return_clause.skip.is_some()
        || return_clause.limit.is_some())
        && (!plan.order_by.is_empty() || plan.skip.is_some() || plan.limit.is_some())
    {
        return Err(unsupported(
            "final_part.return",
            "terminal WITH and RETURN cannot both define ORDER BY, SKIP, or LIMIT without staged query planning",
        ));
    }
    if let Some(skip) = &return_clause.skip {
        plan.skip = Some(compile_skip(skip, "final_part.return.skip", context)?);
    }
    if let Some(order) = &return_clause.order {
        for (index, item) in order.items.iter().enumerate() {
            plan.order_by.push(OrderKey {
                expression: compile_terminal_alias_order_expression(
                    &item.expression,
                    &plan.projections,
                    format!("final_part.return.order.items[{index}].expression"),
                )?,
                direction: match item.direction {
                    Some(SortDirection::Descending) => OrderDirection::Descending,
                    Some(SortDirection::Ascending) | None => OrderDirection::Ascending,
                },
                nulls: context.order_null_placement(item),
            });
        }
    }
    if let Some(limit) = &return_clause.limit {
        plan.limit = Some(compile_limit(limit, "final_part.return.limit", context)?);
    }
    Ok(())
}

pub(crate) fn compile_terminal_alias_order_expression(
    expression: &Expression,
    projections: &[Projection],
    path: impl Into<String>,
) -> Result<OrderExpression, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_terminal_alias_order_expression(inner, projections, path)
        }
        Expression::Variable(variable) => {
            projection_order_expression_for_alias(variable, projections, path)
        }
        _ => Err(unsupported(
            path,
            "ORDER BY after terminal WITH only supports projected aliases",
        )),
    }
}

pub(crate) fn projection_output_alias(projection: &Projection) -> Option<&str> {
    match projection {
        Projection::Property { alias, .. } => alias.as_deref(),
        Projection::Key { alias, .. }
        | Projection::ElementId { alias, .. }
        | Projection::NodeLabels { alias, .. }
        | Projection::PropertyKeys { alias, .. }
        | Projection::RelationshipType { alias, .. }
        | Projection::Literal { alias, .. }
        | Projection::LiteralList { alias, .. }
        | Projection::Expression { alias, .. }
        | Projection::CountAll { alias }
        | Projection::Aggregate { alias, .. } => Some(alias),
    }
}

pub(crate) fn projection_contains_correlated_subquery(projection: &Projection) -> bool {
    match projection {
        Projection::Expression { expression, .. } => {
            scalar_expression_contains_correlated_subquery(expression)
        }
        Projection::Aggregate { target, .. } => {
            aggregate_target_contains_correlated_subquery(target)
        }
        Projection::Property { .. }
        | Projection::Key { .. }
        | Projection::ElementId { .. }
        | Projection::RelationshipType { .. }
        | Projection::NodeLabels { .. }
        | Projection::PropertyKeys { .. }
        | Projection::Literal { .. }
        | Projection::LiteralList { .. }
        | Projection::CountAll { .. } => false,
    }
}

pub(crate) fn aggregate_target_contains_correlated_subquery(target: &AggregateTarget) -> bool {
    matches!(target, AggregateTarget::Expression(expression) if scalar_expression_contains_correlated_subquery(expression))
}

pub(crate) fn return_clause_from_single_part(
    query: &SinglePartQuery,
    path: impl Into<String>,
) -> Result<&Return, CoreError> {
    let path = path.into();
    match &query.body {
        SinglePartBody::Return(return_clause) => Ok(return_clause),
        SinglePartBody::Updating { .. } => Err(unsupported(
            path,
            "write clauses are not supported by Coral virtual graphs",
        )),
        SinglePartBody::Finish(_) => Err(unsupported(
            path,
            "FINISH is not supported because virtual graph queries must return rows",
        )),
    }
}

pub(crate) fn compile_return(
    return_clause: &Return,
    plan: &mut GraphPlan,
    state: &CypherCompileState,
    context: &CypherCompileContext,
) -> Result<(), CoreError> {
    plan.distinct = return_clause.distinct;
    if return_clause.star {
        compile_return_star(plan, state, context, "return.star")?;
    }
    if let Some(skip) = &return_clause.skip {
        plan.skip = Some(compile_skip(skip, "return.skip", context)?);
    }
    if return_clause.items.is_empty() && !return_clause.star {
        return Err(unsupported(
            "return.items",
            "RETURN must include at least one projection",
        ));
    }

    for (index, item) in return_clause.items.iter().enumerate() {
        if let Some(projection) = compile_optional_scalar_alias_return_item(
            item,
            state,
            format!("return.items[{index}]"),
        )? {
            plan.projections.push(projection);
            continue;
        }
        if let Some(projections) = compile_graph_variable_return_item(
            item,
            plan,
            state,
            context,
            format!("return.items[{index}]"),
        )? {
            plan.projections.extend(projections);
            continue;
        }
        if let Some(projections) = compile_graph_endpoint_return_item(
            item,
            plan,
            context,
            format!("return.items[{index}]"),
        )? {
            plan.projections.extend(projections);
            continue;
        }
        let projection =
            compile_projection(item, format!("return.items[{index}]"), context, plan, state)?;
        plan.projections.push(projection);
    }

    if let Some(order) = &return_clause.order {
        for (index, item) in order.items.iter().enumerate() {
            plan.order_by.push(OrderKey {
                expression: compile_order_expression(
                    &item.expression,
                    &plan.projections,
                    plan,
                    state,
                    context,
                    format!("return.order.items[{index}].expression"),
                )?,
                direction: match item.direction {
                    Some(SortDirection::Descending) => OrderDirection::Descending,
                    Some(SortDirection::Ascending) | None => OrderDirection::Ascending,
                },
                nulls: context.order_null_placement(item),
            });
        }
    }

    if let Some(limit) = &return_clause.limit {
        plan.limit = Some(compile_limit(limit, "return.limit", context)?);
    }

    Ok(())
}

pub(crate) fn compile_return_star(
    plan: &mut GraphPlan,
    state: &CypherCompileState,
    context: &CypherCompileContext,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    if !state.path_variables.is_empty() {
        return Err(unsupported(
            path,
            "RETURN * cannot carry path variables because star expansion only materializes node and relationship graph variables; explicitly project fixed-hop path variables or supported path metadata",
        ));
    }
    let graph = context.graph_declaration(path.clone())?;
    let visible = visible_graph_variables(plan, state);
    let node_labels_by_variable = plan
        .nodes
        .iter()
        .map(|node| (node.variable.as_str(), node.label.as_str()))
        .collect::<BTreeMap<_, _>>();
    let mut expansion = ReturnStarExpansion::default();

    for node in &plan.nodes {
        append_return_star_node_projections(node, graph, &visible, &mut expansion, &path)?;
    }

    for relationship in &plan.relationships {
        append_return_star_relationship_projections(
            relationship,
            graph,
            &node_labels_by_variable,
            &visible,
            &mut expansion,
            &path,
        )?;
    }
    for projection in &state.scalar_aliases {
        push_unique_star_projection(&mut expansion, projection.clone(), &path)?;
    }

    if expansion.projections.is_empty() {
        return Err(unsupported(
            path,
            "RETURN * did not resolve any visible graph variables",
        ));
    }
    plan.projections.extend(expansion.projections);
    Ok(())
}

#[derive(Default)]
pub(crate) struct ReturnStarExpansion {
    projections: Vec<Projection>,
    aliases: BTreeSet<String>,
}

pub(crate) fn append_return_star_node_projections(
    node: &NodePattern,
    graph: &Declaration,
    visible: &BTreeSet<String>,
    expansion: &mut ReturnStarExpansion,
    path: &str,
) -> Result<(), CoreError> {
    if !is_visible_star_variable(&node.variable, visible) {
        return Ok(());
    }
    append_node_variable_expansion(node, graph, &node.variable, expansion, path)
}

pub(crate) fn append_node_variable_expansion(
    node: &NodePattern,
    graph: &Declaration,
    output_prefix: &str,
    expansion: &mut ReturnStarExpansion,
    path: &str,
) -> Result<(), CoreError> {
    let mapping = graph.node(&node.label).ok_or_else(|| {
        unsupported(
            path.to_string(),
            format!("could not resolve node label '{}'", node.label),
        )
    })?;
    push_unique_star_projection(
        expansion,
        Projection::Key {
            variable: node.variable.clone(),
            alias: format!("{output_prefix}.__id"),
        },
        path,
    )?;
    push_unique_star_projection(
        expansion,
        Projection::NodeLabels {
            variable: node.variable.clone(),
            label: node.label.clone(),
            alias: format!("{output_prefix}.__labels"),
        },
        path,
    )?;
    for property in mapping.properties.keys() {
        push_unique_star_projection(
            expansion,
            Projection::Property {
                property: PropertyRef {
                    variable: node.variable.clone(),
                    property: property.clone(),
                },
                alias: Some(format!("{output_prefix}.{property}")),
            },
            path,
        )?;
    }
    Ok(())
}

pub(crate) fn append_graph_value_ref_expansion(
    value: &GraphValueRef,
    label: &str,
    graph: &Declaration,
    output_prefix: &str,
    expansion: &mut ReturnStarExpansion,
    path: &str,
) -> Result<(), CoreError> {
    let mapping = graph.node(label).ok_or_else(|| {
        unsupported(
            path.to_string(),
            format!("could not resolve node label '{label}'"),
        )
    })?;
    push_unique_star_projection(
        expansion,
        Projection::Expression {
            expression: graph_value_key_scalar_expression(value.clone()),
            alias: format!("{output_prefix}.__id"),
        },
        path,
    )?;
    push_unique_star_projection(
        expansion,
        Projection::Expression {
            expression: graph_value_labels_scalar_expression(value.clone(), label.to_string()),
            alias: format!("{output_prefix}.__labels"),
        },
        path,
    )?;
    for property in mapping.properties.keys() {
        push_unique_star_projection(
            expansion,
            Projection::Expression {
                expression: graph_value_property_scalar_expression(value.clone(), property.clone()),
                alias: format!("{output_prefix}.{property}"),
            },
            path,
        )?;
    }
    Ok(())
}

pub(crate) fn append_same_label_undirected_endpoint_expansion(
    value: &SameLabelUndirectedEndpointRef,
    graph: &Declaration,
    output_prefix: &str,
    expansion: &mut ReturnStarExpansion,
    path: &str,
) -> Result<(), CoreError> {
    let mapping = graph.node(&value.label).ok_or_else(|| {
        unsupported(
            path.to_string(),
            format!("could not resolve node label '{}'", value.label),
        )
    })?;
    push_unique_star_projection(
        expansion,
        Projection::Expression {
            expression: same_label_undirected_endpoint_key_scalar_expression(value.clone()),
            alias: format!("{output_prefix}.__id"),
        },
        path,
    )?;
    push_unique_star_projection(
        expansion,
        Projection::Expression {
            expression: same_label_undirected_endpoint_labels_scalar_expression(value.clone()),
            alias: format!("{output_prefix}.__labels"),
        },
        path,
    )?;
    for property in mapping.properties.keys() {
        push_unique_star_projection(
            expansion,
            Projection::Expression {
                expression: ScalarExpression::UndirectedEndpointProperty {
                    relationship: value.relationship.clone(),
                    endpoint: value.endpoint,
                    property: property.clone(),
                },
                alias: format!("{output_prefix}.{property}"),
            },
            path,
        )?;
    }
    Ok(())
}

pub(crate) fn append_return_star_relationship_projections(
    relationship: &RelationshipPattern,
    graph: &Declaration,
    node_labels_by_variable: &BTreeMap<&str, &str>,
    visible: &BTreeSet<String>,
    expansion: &mut ReturnStarExpansion,
    path: &str,
) -> Result<(), CoreError> {
    let Some(variable) = relationship.variable.as_ref() else {
        return Ok(());
    };
    if !is_visible_star_variable(variable, visible) {
        return Ok(());
    }
    let mapping =
        return_star_relationship_mapping(graph, relationship, node_labels_by_variable, path)?;
    append_relationship_variable_expansion(relationship, mapping, variable, expansion, path)
}

pub(crate) fn append_relationship_variable_expansion(
    relationship: &RelationshipPattern,
    mapping: &DeclaredRelationship,
    output_prefix: &str,
    expansion: &mut ReturnStarExpansion,
    path: &str,
) -> Result<(), CoreError> {
    let variable = relationship
        .variable
        .as_ref()
        .ok_or_else(|| CoreError::internal("relationship expansion requires a variable"))?;
    if mapping.key.is_some() {
        push_unique_star_projection(
            expansion,
            Projection::Key {
                variable: variable.clone(),
                alias: format!("{output_prefix}.__id"),
            },
            path,
        )?;
    }
    push_unique_star_projection(
        expansion,
        Projection::RelationshipType {
            variable: variable.clone(),
            relationship_type: relationship.relationship_type.clone(),
            alias: format!("{output_prefix}.__type"),
        },
        path,
    )?;
    for property in mapping.properties.keys() {
        push_unique_star_projection(
            expansion,
            Projection::Property {
                property: PropertyRef {
                    variable: variable.clone(),
                    property: property.clone(),
                },
                alias: Some(format!("{output_prefix}.{property}")),
            },
            path,
        )?;
    }
    Ok(())
}

pub(crate) fn compile_graph_variable_return_item(
    item: &ProjectionItem,
    plan: &GraphPlan,
    state: &CypherCompileState,
    context: &CypherCompileContext,
    path: impl Into<String>,
) -> Result<Option<Vec<Projection>>, CoreError> {
    let path = path.into();
    let Expression::Variable(variable) = &item.expression else {
        return Ok(None);
    };
    let name = variable_name(variable);
    if state.path_variables.contains_key(&name) {
        return Ok(None);
    }
    reject_ignored_path_variable(&name, state, format!("{path}.expression"))?;
    let visible = visible_graph_variables(plan, state);
    if !is_visible_star_variable(&name, &visible) {
        return Ok(None);
    }
    let graph = context.graph_declaration(format!("{path}.expression"))?;
    let output_prefix = item
        .alias
        .as_ref()
        .map(validate_variable)
        .transpose()?
        .unwrap_or_else(|| name.clone());
    let mut expansion = ReturnStarExpansion::default();
    if let Some(node) = plan.nodes.iter().find(|node| node.variable == name) {
        append_node_variable_expansion(node, graph, &output_prefix, &mut expansion, &path)?;
        return Ok(Some(expansion.projections));
    }
    if let Some(relationship) = plan
        .relationships
        .iter()
        .find(|relationship| relationship.variable.as_deref() == Some(name.as_str()))
    {
        let node_labels_by_variable = plan
            .nodes
            .iter()
            .map(|node| (node.variable.as_str(), node.label.as_str()))
            .collect::<BTreeMap<_, _>>();
        let mapping =
            return_star_relationship_mapping(graph, relationship, &node_labels_by_variable, &path)?;
        append_relationship_variable_expansion(
            relationship,
            mapping,
            &output_prefix,
            &mut expansion,
            &path,
        )?;
        return Ok(Some(expansion.projections));
    }
    Ok(None)
}

pub(crate) fn compile_graph_endpoint_return_item(
    item: &ProjectionItem,
    plan: &GraphPlan,
    context: &CypherCompileContext,
    path: impl Into<String>,
) -> Result<Option<Vec<Projection>>, CoreError> {
    let path = path.into();
    match &item.expression {
        Expression::Parenthesized(inner) => {
            let nested = ProjectionItem {
                expression: inner.as_ref().clone(),
                alias: item.alias.clone(),
            };
            compile_graph_endpoint_return_item(&nested, plan, context, path)
        }
        Expression::FunctionCall(function)
            if is_start_node_function(function) || is_end_node_function(function) =>
        {
            let graph = context.graph_declaration(format!("{path}.expression"))?;
            let output_prefix = relationship_endpoint_return_output_prefix(
                function,
                item.alias.as_ref(),
                context,
                format!("{path}.expression"),
            )?;
            let mut expansion = ReturnStarExpansion::default();
            if let Some(value) = compile_optional_same_label_undirected_relationship_endpoint(
                &item.expression,
                format!("{path}.expression"),
                plan,
                context,
            )? {
                append_same_label_undirected_endpoint_expansion(
                    &value,
                    graph,
                    &output_prefix,
                    &mut expansion,
                    &path,
                )?;
                return Ok(Some(expansion.projections));
            }
            let value = compile_relationship_endpoint_ref(
                function,
                format!("{path}.expression"),
                plan,
                context,
            )?;
            let label =
                node_label_for_variable(plan, &value.variable, format!("{path}.expression"))?;
            append_graph_value_ref_expansion(
                &value,
                label,
                graph,
                &output_prefix,
                &mut expansion,
                &path,
            )?;
            Ok(Some(expansion.projections))
        }
        _ => Ok(None),
    }
}

pub(crate) fn relationship_endpoint_return_output_prefix(
    function: &FunctionInvocation,
    alias: Option<&Variable>,
    context: &CypherCompileContext,
    path: impl Into<String>,
) -> Result<String, CoreError> {
    if let Some(alias) = alias {
        return validate_variable(alias);
    }
    let path = path.into();
    let endpoint = relationship_endpoint_function(function).ok_or_else(|| {
        unsupported(
            path.clone(),
            format!(
                "function '{}' is not a relationship endpoint function",
                qualified_function_name(function)
            ),
        )
    })?;
    let function_name = relationship_endpoint_function_name(endpoint);
    let variable = compile_single_variable_function_argument(
        function,
        format!("{path}.arguments"),
        match endpoint {
            RelationshipEndpoint::Start => {
                "startNode() supports exactly one relationship variable argument"
            }
            RelationshipEndpoint::End => {
                "endNode() supports exactly one relationship variable argument"
            }
        },
        context,
    )?;
    Ok(format!("{function_name}({variable})"))
}

pub(crate) fn is_visible_star_variable(variable: &str, visible: &BTreeSet<String>) -> bool {
    visible.contains(variable) && !is_internal_graph_variable(variable)
}

pub(crate) fn push_unique_star_projection(
    expansion: &mut ReturnStarExpansion,
    projection: Projection,
    path: &str,
) -> Result<(), CoreError> {
    let alias = projection.output_name();
    if !expansion.aliases.insert(alias.clone()) {
        return Err(unsupported(
            path.to_string(),
            format!("RETURN * expansion produced duplicate output column '{alias}'"),
        ));
    }
    expansion.projections.push(projection);
    Ok(())
}

pub(crate) fn return_star_relationship_mapping<'a>(
    graph: &'a Declaration,
    relationship: &RelationshipPattern,
    node_labels_by_variable: &BTreeMap<&str, &str>,
    path: &str,
) -> Result<&'a DeclaredRelationship, CoreError> {
    let left_label = node_labels_by_variable
        .get(relationship.left.as_str())
        .copied()
        .ok_or_else(|| {
            unsupported(
                path.to_string(),
                format!(
                    "RETURN * could not resolve left endpoint '{}'",
                    relationship.left
                ),
            )
        })?;
    let right_label = node_labels_by_variable
        .get(relationship.right.as_str())
        .copied()
        .ok_or_else(|| {
            unsupported(
                path.to_string(),
                format!(
                    "RETURN * could not resolve right endpoint '{}'",
                    relationship.right
                ),
            )
        })?;
    let matches = graph
        .relationships_for_type(&relationship.relationship_type)
        .filter(|mapping| {
            relationship_mapping_matches_pattern(
                mapping,
                relationship.direction,
                left_label,
                right_label,
            )
        })
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [mapping] => Ok(*mapping),
        [] => Err(unsupported(
            path.to_string(),
            format!(
                "RETURN * could not resolve relationship type '{}' for {left_label} -> {right_label}",
                relationship.relationship_type
            ),
        )),
        _ => Err(unsupported(
            path.to_string(),
            format!(
                "RETURN * relationship type '{}' for {left_label} -> {right_label} is ambiguous; add direction or use distinct relationship types",
                relationship.relationship_type
            ),
        )),
    }
}

pub(crate) fn relationship_mapping_matches_pattern(
    mapping: &DeclaredRelationship,
    direction: Direction,
    left_label: &str,
    right_label: &str,
) -> bool {
    let matches_forward = left_label == mapping.from.label && right_label == mapping.to.label;
    let matches_reverse = left_label == mapping.to.label && right_label == mapping.from.label;
    match direction {
        Direction::Outgoing => matches_forward,
        Direction::Incoming => matches_reverse,
        Direction::Undirected => matches_forward || matches_reverse,
    }
}

pub(crate) fn compile_order_expression(
    expression: &Expression,
    projections: &[Projection],
    plan: &GraphPlan,
    state: &CypherCompileState,
    context: &CypherCompileContext,
    path: impl Into<String>,
) -> Result<OrderExpression, CoreError> {
    let path = path.into();
    if let Some((expression, _)) = compile_optional_endpoint_property_scalar_expression(
        expression,
        path.clone(),
        Some(plan),
        context,
    )? {
        return Ok(OrderExpression::Scalar(expression));
    }
    match expression {
        Expression::Parenthesized(inner) => {
            compile_order_expression(inner, projections, plan, state, context, path)
        }
        Expression::Variable(variable) => {
            if let Some(order) =
                optional_projection_order_expression_for_alias(variable, projections, path.clone())?
            {
                return Ok(order);
            }
            if let Some(expression) =
                compile_optional_scalar_alias_expression(expression, path.clone(), Some(state))?
            {
                return compile_scalar_order_expression(expression, projections, path);
            }
            projection_order_expression_for_alias(variable, projections, path)
        }
        Expression::CountStar { .. } => {
            count_star_order_expression_for_projection(projections, path)
        }
        Expression::CountSubquery(count) => compile_scalar_order_expression(
            compile_count_subquery_scalar_expression(count, path.clone(), Some(plan), context)?,
            projections,
            path,
        ),
        expression => {
            compile_order_expression_fallback(expression, projections, plan, state, context, path)
        }
    }
}

pub(crate) fn compile_order_expression_fallback(
    expression: &Expression,
    projections: &[Projection],
    plan: &GraphPlan,
    state: &CypherCompileState,
    context: &CypherCompileContext,
    path: String,
) -> Result<OrderExpression, CoreError> {
    if let Some(order) = compile_optional_path_list_index_order_expression(
        expression,
        projections,
        plan,
        state,
        context,
        path.clone(),
    )? {
        return Ok(order);
    }
    if let Some(expression) = compile_optional_path_list_slice_scalar_expression(
        expression,
        path.clone(),
        PredicateCompileMode::Graph {
            plan,
            path_state: Some(state),
        },
        context,
    )? {
        return compile_scalar_order_expression(expression, projections, path);
    }
    if let Some(expression) = compile_optional_metadata_list_index_scalar_expression(
        expression,
        path.clone(),
        plan,
        context,
    )? {
        return compile_scalar_order_expression(expression, projections, path);
    }
    if let Some(expression) = compile_optional_non_literal_static_list_index_scalar_expression(
        expression,
        path.clone(),
        Some(plan),
        context,
    )? {
        return compile_scalar_order_expression(expression, projections, path);
    }
    if !is_direct_metadata_list_function(expression)
        && let Some(expression) = compile_optional_static_list_scalar_expression(
            expression,
            path.clone(),
            Some(plan),
            context,
        )?
    {
        return compile_scalar_order_expression(expression, projections, path);
    }
    if is_list_slice_expression(expression)
        && let Some(value) =
            compile_optional_metadata_list_value(expression, path.clone(), plan, context)?
    {
        return compile_scalar_order_expression(
            metadata_list_value_scalar_expression(value, plan),
            projections,
            path,
        );
    }
    compile_order_expression_after_metadata_list_index(
        expression,
        projections,
        plan,
        state,
        context,
        path,
    )
}

pub(crate) fn is_direct_metadata_list_function(expression: &Expression) -> bool {
    match expression {
        Expression::Parenthesized(inner) => is_direct_metadata_list_function(inner),
        Expression::FunctionCall(function) => {
            is_labels_function(function) || is_keys_function(function)
        }
        _ => false,
    }
}

pub(crate) fn compile_optional_path_list_index_order_expression(
    expression: &Expression,
    projections: &[Projection],
    plan: &GraphPlan,
    state: &CypherCompileState,
    context: &CypherCompileContext,
    path: impl Into<String>,
) -> Result<Option<OrderExpression>, CoreError> {
    let path = path.into();
    let Some(expression) = compile_optional_path_list_index_scalar_expression(
        expression,
        path.clone(),
        PredicateCompileMode::Graph {
            plan,
            path_state: Some(state),
        },
        context,
    )?
    else {
        return Ok(None);
    };
    compile_scalar_order_expression(expression, projections, path).map(Some)
}

pub(crate) fn compile_order_expression_after_metadata_list_index(
    expression: &Expression,
    projections: &[Projection],
    plan: &GraphPlan,
    state: &CypherCompileState,
    context: &CypherCompileContext,
    path: String,
) -> Result<OrderExpression, CoreError> {
    if let Some(expression) =
        compile_projected_graph_object_property_order_expression(expression, projections, plan)
    {
        return Ok(expression);
    }
    if let Some(expression) = compile_optional_graph_property_order_expression(
        expression,
        projections,
        plan,
        context,
        &path,
    )? {
        return Ok(expression);
    }
    match expression {
        expression if is_literal_expression(expression) => Ok(OrderExpression::Literal(
            compile_literal(expression, path, context)?,
        )),
        Expression::UnaryOp {
            op: UnaryOperator::Negate,
            ..
        } => compile_path_aware_arithmetic_order_expression(
            expression,
            path,
            projections,
            plan,
            state,
            context,
        ),
        Expression::BinaryOp { .. } => {
            compile_binary_order_expression(expression, path, projections, plan, state, context)
        }
        expression if is_boolean_scalar_expression(expression) => compile_scalar_order_expression(
            compile_boolean_scalar_expression(expression, path.clone(), plan, context)?,
            projections,
            path,
        ),
        Expression::Case(case) => {
            compile_case_order_expression(case, path, projections, plan, state, context)
        }
        Expression::FunctionCall(function) => {
            compile_function_call_order_expression_after_metadata_list_index(
                expression,
                function,
                projections,
                plan,
                state,
                context,
                path,
            )
        }
        _ => Ok(OrderExpression::Property(compile_property_ref(
            expression,
            path,
            Some(plan),
            context,
        )?)),
    }
}

pub(crate) fn compile_projected_graph_object_property_order_expression(
    expression: &Expression,
    projections: &[Projection],
    plan: &GraphPlan,
) -> Option<OrderExpression> {
    match expression {
        Expression::Parenthesized(inner) => {
            compile_projected_graph_object_property_order_expression(inner, projections, plan)
        }
        Expression::PropertyLookup { base, property, .. } => {
            let Expression::Variable(variable) = base.as_ref() else {
                return None;
            };
            let base_name = variable_name(variable);
            if plan_uses_variable(plan, &base_name) {
                return None;
            }
            let property_name = property.name.name.as_str();
            let projected_alias = if property_name == "id" {
                format!("{base_name}.__id")
            } else {
                format!("{base_name}.{property_name}")
            };
            projections
                .iter()
                .any(|projection| projection.output_name() == projected_alias)
                .then_some(OrderExpression::ProjectionAlias(projected_alias))
        }
        _ => None,
    }
}

pub(crate) fn compile_function_call_order_expression_after_metadata_list_index(
    expression: &Expression,
    function: &FunctionInvocation,
    projections: &[Projection],
    plan: &GraphPlan,
    state: &CypherCompileState,
    context: &CypherCompileContext,
    path: String,
) -> Result<OrderExpression, CoreError> {
    if is_id_function(function) {
        return compile_id_order_expression(function, path, plan, context);
    }
    if is_element_id_function(function) {
        return compile_element_id_order_expression(function, path, plan, context);
    }
    if is_type_function(function) {
        return compile_type_order_expression(function, path, plan, context);
    }
    if is_labels_function(function) {
        return compile_labels_order_expression(function, path, plan, context);
    }
    if is_keys_function(function) {
        return compile_keys_order_expression_after_metadata_list_index(
            expression,
            function,
            projections,
            plan,
            context,
            path,
        );
    }
    if is_length_function(function) {
        return compile_path_length_order_expression(function, path, state, context);
    }
    if let Some(expression) =
        compile_optional_size_path_length_order_expression(function, path.clone(), state, context)?
    {
        return Ok(expression);
    }
    if let Some(expression) = compile_scalar_function_expression_with_path_state(
        function,
        path.clone(),
        plan,
        Some(state),
        context,
    )? {
        return compile_scalar_order_expression(expression, projections, path);
    }
    if is_aggregate_function_call(function) {
        return aggregate_order_expression_for_projection(
            function,
            projections,
            path,
            plan,
            state,
            context,
        );
    }
    Ok(OrderExpression::Property(compile_property_ref(
        expression,
        path,
        Some(plan),
        context,
    )?))
}

pub(crate) fn compile_keys_order_expression_after_metadata_list_index(
    expression: &Expression,
    function: &FunctionInvocation,
    projections: &[Projection],
    plan: &GraphPlan,
    context: &CypherCompileContext,
    path: String,
) -> Result<OrderExpression, CoreError> {
    if is_literal_map_keys_function(function)
        && let Some(expression) = compile_optional_static_list_scalar_expression(
            expression,
            path.clone(),
            Some(plan),
            context,
        )?
    {
        return compile_scalar_order_expression(expression, projections, path);
    }
    compile_keys_order_expression(function, path, plan, context)
}

pub(crate) fn compile_optional_graph_property_order_expression(
    expression: &Expression,
    projections: &[Projection],
    plan: &GraphPlan,
    context: &CypherCompileContext,
    path: &str,
) -> Result<Option<OrderExpression>, CoreError> {
    if let Some((expression, _)) = compile_optional_endpoint_property_scalar_expression(
        expression,
        path.to_string(),
        Some(plan),
        context,
    )? {
        return compile_scalar_order_expression(expression, projections, path.to_string())
            .map(Some);
    }
    if let Some(literal) =
        compile_optional_static_map_lookup_literal(expression, path.to_string(), context)
            .ok()
            .flatten()
    {
        return Ok(Some(OrderExpression::Literal(literal)));
    }
    if let Some(expression) = compile_optional_non_literal_static_map_lookup_scalar_expression(
        expression,
        path.to_string(),
        PredicateCompileMode::Graph {
            plan,
            path_state: None,
        },
        context,
    )? {
        if let ScalarExpression::Property(property) = expression {
            return Ok(Some(OrderExpression::Property(property)));
        }
        return compile_scalar_order_expression(expression, projections, path.to_string())
            .map(Some);
    }
    Ok(
        compile_optional_property_ref(expression, path.to_string(), Some(plan), context)?
            .map(OrderExpression::Property),
    )
}

pub(crate) fn compile_scalar_order_expression(
    expression: ScalarExpression,
    projections: &[Projection],
    path: impl Into<String>,
) -> Result<OrderExpression, CoreError> {
    if scalar_expression_contains_correlated_subquery(&expression) {
        if let Some(alias) = projected_scalar_expression_alias(&expression, projections) {
            return Ok(OrderExpression::ProjectionAlias(alias));
        }
        if hidden_subquery_order_expression_can_be_precomputed(&expression) {
            return Ok(OrderExpression::Scalar(expression));
        }
        return Err(unsupported(
            path,
            "ORDER BY over correlated subqueries must use a projected alias, for example RETURN EXISTS { MATCH ... } AS has_match ORDER BY has_match",
        ));
    }
    Ok(OrderExpression::Scalar(expression))
}

pub(crate) fn hidden_subquery_order_expression_can_be_precomputed(
    expression: &ScalarExpression,
) -> bool {
    if let Some(operand) = unary_scalar_expression_operand(expression) {
        return hidden_subquery_order_expression_can_be_precomputed(operand);
    }

    if let Some(precomputable) = hidden_subquery_order_leaf_can_be_precomputed(expression) {
        return precomputable;
    }

    hidden_subquery_order_structural_expression_can_be_precomputed(expression)
}

pub(crate) fn hidden_subquery_order_leaf_can_be_precomputed(
    expression: &ScalarExpression,
) -> Option<bool> {
    match expression {
        ScalarExpression::Predicate(predicate) => Some(
            hidden_subquery_order_predicate_can_be_precomputed(predicate),
        ),
        ScalarExpression::CountSubquery { pattern, .. } => Some(match pattern.as_ref() {
            CountSubqueryPattern::Relationships(_) | CountSubqueryPattern::Nodes { .. } => true,
        }),
        ScalarExpression::CollectSubquery { .. } => Some(false),
        ScalarExpression::Property(_)
        | ScalarExpression::StageValue { .. }
        | ScalarExpression::UndirectedEndpointProperty { .. }
        | ScalarExpression::UndirectedEndpointKey { .. }
        | ScalarExpression::UndirectedEndpointElementId { .. }
        | ScalarExpression::UndirectedEndpointLabels { .. }
        | ScalarExpression::UndirectedEndpointPropertyKeys { .. }
        | ScalarExpression::Literal(_)
        | ScalarExpression::LiteralList { .. }
        | ScalarExpression::TypedLiteralList { .. }
        | ScalarExpression::GraphKeyList { .. }
        | ScalarExpression::PathValue { .. }
        | ScalarExpression::Key { .. }
        | ScalarExpression::ElementId { .. }
        | ScalarExpression::GraphIdentity { .. }
        | ScalarExpression::GraphPresence { .. }
        | ScalarExpression::NodeLabels { .. }
        | ScalarExpression::PropertyKeys { .. }
        | ScalarExpression::RelationshipType { .. } => Some(true),
        ScalarExpression::PresenceGated { .. }
        | ScalarExpression::Coalesce { .. }
        | ScalarExpression::NullIf { .. }
        | ScalarExpression::Replace { .. }
        | ScalarExpression::Substring { .. }
        | ScalarExpression::Left { .. }
        | ScalarExpression::Right { .. }
        | ScalarExpression::StringIndices { .. }
        | ScalarExpression::LPad { .. }
        | ScalarExpression::RPad { .. }
        | ScalarExpression::StringContains { .. }
        | ScalarExpression::StringStartsWith { .. }
        | ScalarExpression::StringEndsWith { .. }
        | ScalarExpression::Round { .. }
        | ScalarExpression::Temporal(_)
        | ScalarExpression::Arithmetic { .. }
        | ScalarExpression::ListConcat { .. }
        | ScalarExpression::ListIndex { .. }
        | ScalarExpression::Atan2 { .. }
        | ScalarExpression::Case { .. } => None,
        ScalarExpression::ToString { .. }
        | ScalarExpression::ToInteger { .. }
        | ScalarExpression::ToFloat { .. }
        | ScalarExpression::ToBoolean { .. }
        | ScalarExpression::ToStringOrNull { .. }
        | ScalarExpression::ToIntegerOrNull { .. }
        | ScalarExpression::ToFloatOrNull { .. }
        | ScalarExpression::ToBooleanOrNull { .. }
        | ScalarExpression::ToLower { .. }
        | ScalarExpression::ToUpper { .. }
        | ScalarExpression::Trim { .. }
        | ScalarExpression::LTrim { .. }
        | ScalarExpression::RTrim { .. }
        | ScalarExpression::CharacterLength { .. }
        | ScalarExpression::Reverse { .. }
        | ScalarExpression::Abs { .. }
        | ScalarExpression::Ceil { .. }
        | ScalarExpression::Floor { .. }
        | ScalarExpression::Sqrt { .. }
        | ScalarExpression::Sign { .. }
        | ScalarExpression::Exp { .. }
        | ScalarExpression::Log { .. }
        | ScalarExpression::Log10 { .. }
        | ScalarExpression::Sin { .. }
        | ScalarExpression::Cos { .. }
        | ScalarExpression::Tan { .. }
        | ScalarExpression::Cot { .. }
        | ScalarExpression::Asin { .. }
        | ScalarExpression::Acos { .. }
        | ScalarExpression::Atan { .. }
        | ScalarExpression::Degrees { .. }
        | ScalarExpression::Radians { .. }
        | ScalarExpression::IsNaN { .. }
        | ScalarExpression::Negate { .. } => {
            unreachable!("unary scalar expressions handled above")
        }
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "This exhaustive structural scalar dispatcher stays total over every scalar variant."
)]
pub(crate) fn hidden_subquery_order_structural_expression_can_be_precomputed(
    expression: &ScalarExpression,
) -> bool {
    match expression {
        ScalarExpression::PresenceGated { expression, .. }
        | ScalarExpression::Temporal(TemporalExpr::ZonedDateTimeAccessor { expression, .. }) => {
            hidden_subquery_order_expression_can_be_precomputed(expression)
        }
        ScalarExpression::Coalesce { expressions } => expressions
            .iter()
            .all(hidden_subquery_order_expression_can_be_precomputed),
        ScalarExpression::NullIf { expression, value } => {
            hidden_subquery_order_expression_can_be_precomputed(expression)
                && hidden_subquery_order_expression_can_be_precomputed(value)
        }
        ScalarExpression::Round { expression, places } => {
            hidden_subquery_order_expression_can_be_precomputed(expression)
                && optional_hidden_subquery_order_expression_can_be_precomputed(places.as_deref())
        }
        ScalarExpression::Left { expression, count }
        | ScalarExpression::Right { expression, count } => {
            hidden_subquery_order_expression_can_be_precomputed(expression)
                && hidden_subquery_order_expression_can_be_precomputed(count)
        }
        ScalarExpression::StringIndices {
            expression,
            pattern: operand,
        }
        | ScalarExpression::StringContains {
            expression,
            pattern: operand,
        }
        | ScalarExpression::StringStartsWith {
            expression,
            pattern: operand,
        }
        | ScalarExpression::StringEndsWith {
            expression,
            pattern: operand,
        } => {
            hidden_subquery_order_expression_can_be_precomputed(expression)
                && hidden_subquery_order_expression_can_be_precomputed(operand)
        }
        ScalarExpression::LPad {
            expression,
            length,
            fill,
        }
        | ScalarExpression::RPad {
            expression,
            length,
            fill,
        } => {
            hidden_subquery_order_expression_can_be_precomputed(expression)
                && hidden_subquery_order_expression_can_be_precomputed(length)
                && hidden_subquery_order_expression_can_be_precomputed(fill)
        }
        ScalarExpression::Replace {
            expression,
            search,
            replacement,
        } => {
            hidden_subquery_order_expression_can_be_precomputed(expression)
                && hidden_subquery_order_expression_can_be_precomputed(search)
                && hidden_subquery_order_expression_can_be_precomputed(replacement)
        }
        ScalarExpression::Substring {
            expression,
            start,
            length,
        } => {
            hidden_subquery_order_expression_can_be_precomputed(expression)
                && hidden_subquery_order_expression_can_be_precomputed(start)
                && optional_hidden_subquery_order_expression_can_be_precomputed(length.as_deref())
        }
        ScalarExpression::Arithmetic { left, right, .. }
        | ScalarExpression::ListConcat { left, right } => {
            hidden_subquery_order_expression_can_be_precomputed(left)
                && hidden_subquery_order_expression_can_be_precomputed(right)
        }
        ScalarExpression::ListIndex { list, .. } => {
            hidden_subquery_order_expression_can_be_precomputed(list)
        }
        ScalarExpression::Atan2 { y, x } => {
            hidden_subquery_order_expression_can_be_precomputed(y)
                && hidden_subquery_order_expression_can_be_precomputed(x)
        }
        ScalarExpression::Temporal(TemporalExpr::MakeDate { year, month, day }) => {
            hidden_subquery_order_expression_can_be_precomputed(year)
                && hidden_subquery_order_expression_can_be_precomputed(month)
                && hidden_subquery_order_expression_can_be_precomputed(day)
        }
        ScalarExpression::Temporal(TemporalExpr::MakeLocalDateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            millisecond,
            microsecond,
            nanosecond,
        }) => [
            year,
            month,
            day,
            hour,
            minute,
            second,
            millisecond,
            microsecond,
            nanosecond,
        ]
        .iter()
        .all(|expression| hidden_subquery_order_expression_can_be_precomputed(expression)),
        ScalarExpression::Temporal(TemporalExpr::MakeZonedDateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            millisecond,
            microsecond,
            nanosecond,
            ..
        }) => [
            year,
            month,
            day,
            hour,
            minute,
            second,
            millisecond,
            microsecond,
            nanosecond,
        ]
        .iter()
        .all(|expression| hidden_subquery_order_expression_can_be_precomputed(expression)),
        ScalarExpression::Temporal(TemporalExpr::MakeLocalTime {
            hour,
            minute,
            second,
            millisecond,
            microsecond,
            nanosecond,
        }) => [hour, minute, second, millisecond, microsecond, nanosecond]
            .iter()
            .all(|expression| hidden_subquery_order_expression_can_be_precomputed(expression)),
        ScalarExpression::Temporal(TemporalExpr::MakeDuration { .. }) => true,
        ScalarExpression::Temporal(TemporalExpr::DurationInUnits { start, end, .. }) => {
            hidden_subquery_order_expression_can_be_precomputed(start)
                && hidden_subquery_order_expression_can_be_precomputed(end)
        }
        ScalarExpression::Case {
            alternatives,
            else_expression,
        } => {
            hidden_subquery_order_case_can_be_precomputed(alternatives, else_expression.as_deref())
        }
        _ => unreachable!("leaf and unary scalar expressions handled above"),
    }
}

pub(crate) fn optional_hidden_subquery_order_expression_can_be_precomputed(
    expression: Option<&ScalarExpression>,
) -> bool {
    expression.is_none_or(hidden_subquery_order_expression_can_be_precomputed)
}

pub(crate) fn hidden_subquery_order_case_can_be_precomputed(
    alternatives: &[ScalarCaseAlternative],
    else_expression: Option<&ScalarExpression>,
) -> bool {
    alternatives.iter().all(|alternative| {
        hidden_subquery_order_predicate_can_be_precomputed(&alternative.when)
            && hidden_subquery_order_expression_can_be_precomputed(&alternative.then)
    }) && optional_hidden_subquery_order_expression_can_be_precomputed(else_expression)
}

pub(crate) fn hidden_subquery_order_predicate_can_be_precomputed(
    predicate: &PredicateExpression,
) -> bool {
    match predicate {
        PredicateExpression::ScalarComparison(predicate) => {
            hidden_subquery_order_expression_can_be_precomputed(&predicate.lhs)
                && match &predicate.rhs {
                    ScalarPredicateRhs::Expression(expression) => {
                        hidden_subquery_order_expression_can_be_precomputed(expression)
                    }
                    ScalarPredicateRhs::List(_) => true,
                }
        }
        PredicateExpression::And { left, right }
        | PredicateExpression::Or { left, right }
        | PredicateExpression::Xor { left, right } => {
            hidden_subquery_order_predicate_can_be_precomputed(left)
                && hidden_subquery_order_predicate_can_be_precomputed(right)
        }
        PredicateExpression::Not { expression } => {
            hidden_subquery_order_predicate_can_be_precomputed(expression)
        }
        PredicateExpression::ExistsPattern(_)
        | PredicateExpression::Boolean(_)
        | PredicateExpression::Comparison(_)
        | PredicateExpression::KeyComparison(_)
        | PredicateExpression::ElementIdComparison(_)
        | PredicateExpression::Presence(_)
        | PredicateExpression::PropertyKeyMembership(_) => true,
    }
}

pub(crate) fn projected_scalar_expression_alias(
    expression: &ScalarExpression,
    projections: &[Projection],
) -> Option<String> {
    projections.iter().find_map(|projection| match projection {
        Projection::Expression {
            expression: projected,
            alias,
        } if projected == expression => Some(alias.clone()),
        _ => None,
    })
}

pub(crate) fn scalar_expression_contains_correlated_subquery(
    expression: &ScalarExpression,
) -> bool {
    scalar_expression_correlated_subquery_count(expression) > 0
}

pub(crate) fn scalar_expression_correlated_subquery_count(expression: &ScalarExpression) -> usize {
    if let Some(count) = compound_scalar_expression_correlated_subquery_count(expression) {
        return count;
    }
    scalar_expression_leaf_correlated_subquery_count(expression)
}

#[expect(
    clippy::too_many_lines,
    reason = "This exhaustive correlated-subquery dispatcher stays total over every scalar variant."
)]
pub(crate) fn compound_scalar_expression_correlated_subquery_count(
    expression: &ScalarExpression,
) -> Option<usize> {
    if let Some(operand) = unary_scalar_expression_operand(expression) {
        return Some(scalar_expression_correlated_subquery_count(operand));
    }

    match expression {
        ScalarExpression::PresenceGated { expression, .. }
        | ScalarExpression::Temporal(TemporalExpr::ZonedDateTimeAccessor { expression, .. }) => {
            Some(scalar_expression_correlated_subquery_count(expression))
        }
        ScalarExpression::Coalesce { expressions } => Some(
            expressions
                .iter()
                .map(scalar_expression_correlated_subquery_count)
                .sum(),
        ),
        ScalarExpression::NullIf { expression, value } => Some(
            scalar_expression_correlated_subquery_count(expression)
                + scalar_expression_correlated_subquery_count(value),
        ),
        ScalarExpression::Round { expression, places } => Some(
            scalar_expression_correlated_subquery_count(expression)
                + optional_scalar_expression_correlated_subquery_count(places.as_deref()),
        ),
        ScalarExpression::Left { expression, count }
        | ScalarExpression::Right { expression, count } => Some(
            scalar_expression_correlated_subquery_count(expression)
                + scalar_expression_correlated_subquery_count(count),
        ),
        ScalarExpression::StringIndices {
            expression,
            pattern: operand,
        }
        | ScalarExpression::StringContains {
            expression,
            pattern: operand,
        }
        | ScalarExpression::StringStartsWith {
            expression,
            pattern: operand,
        }
        | ScalarExpression::StringEndsWith {
            expression,
            pattern: operand,
        } => Some(
            scalar_expression_correlated_subquery_count(expression)
                + scalar_expression_correlated_subquery_count(operand),
        ),
        ScalarExpression::LPad {
            expression,
            length,
            fill,
        }
        | ScalarExpression::RPad {
            expression,
            length,
            fill,
        } => Some(
            scalar_expression_correlated_subquery_count(expression)
                + scalar_expression_correlated_subquery_count(length)
                + scalar_expression_correlated_subquery_count(fill),
        ),
        ScalarExpression::Replace {
            expression,
            search,
            replacement,
        } => Some(
            scalar_expression_correlated_subquery_count(expression)
                + scalar_expression_correlated_subquery_count(search)
                + scalar_expression_correlated_subquery_count(replacement),
        ),
        ScalarExpression::Substring {
            expression,
            start,
            length,
        } => Some(
            scalar_expression_correlated_subquery_count(expression)
                + scalar_expression_correlated_subquery_count(start)
                + optional_scalar_expression_correlated_subquery_count(length.as_deref()),
        ),
        ScalarExpression::Arithmetic { left, right, .. }
        | ScalarExpression::ListConcat { left, right } => Some(
            scalar_expression_correlated_subquery_count(left)
                + scalar_expression_correlated_subquery_count(right),
        ),
        ScalarExpression::ListIndex { list, .. } => {
            Some(scalar_expression_correlated_subquery_count(list))
        }
        ScalarExpression::Atan2 { y, x } => Some(
            scalar_expression_correlated_subquery_count(y)
                + scalar_expression_correlated_subquery_count(x),
        ),
        ScalarExpression::Temporal(TemporalExpr::MakeDate { year, month, day }) => Some(
            scalar_expression_correlated_subquery_count(year)
                + scalar_expression_correlated_subquery_count(month)
                + scalar_expression_correlated_subquery_count(day),
        ),
        ScalarExpression::Temporal(TemporalExpr::MakeLocalDateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            millisecond,
            microsecond,
            nanosecond,
        }) => Some(
            [
                year,
                month,
                day,
                hour,
                minute,
                second,
                millisecond,
                microsecond,
                nanosecond,
            ]
            .iter()
            .map(|expression| scalar_expression_correlated_subquery_count(expression))
            .sum(),
        ),
        ScalarExpression::Temporal(TemporalExpr::MakeZonedDateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            millisecond,
            microsecond,
            nanosecond,
            ..
        }) => Some(
            [
                year,
                month,
                day,
                hour,
                minute,
                second,
                millisecond,
                microsecond,
                nanosecond,
            ]
            .iter()
            .map(|expression| scalar_expression_correlated_subquery_count(expression))
            .sum(),
        ),
        ScalarExpression::Temporal(TemporalExpr::MakeLocalTime {
            hour,
            minute,
            second,
            millisecond,
            microsecond,
            nanosecond,
        }) => Some(
            [hour, minute, second, millisecond, microsecond, nanosecond]
                .iter()
                .map(|expression| scalar_expression_correlated_subquery_count(expression))
                .sum(),
        ),
        ScalarExpression::Temporal(TemporalExpr::MakeDuration { .. }) => Some(0),
        ScalarExpression::Temporal(TemporalExpr::DurationInUnits { start, end, .. }) => Some(
            scalar_expression_correlated_subquery_count(start)
                + scalar_expression_correlated_subquery_count(end),
        ),
        _ => None,
    }
}

pub(crate) fn optional_scalar_expression_correlated_subquery_count(
    expression: Option<&ScalarExpression>,
) -> usize {
    expression.map_or(0, scalar_expression_correlated_subquery_count)
}

pub(crate) fn scalar_expression_leaf_correlated_subquery_count(
    expression: &ScalarExpression,
) -> usize {
    match expression {
        ScalarExpression::Predicate(predicate) => {
            predicate_expression_correlated_subquery_count(predicate)
        }
        ScalarExpression::CountSubquery {
            pattern,
            distinct_target,
        } => usize::from(pattern.references_outer_variables() || distinct_target.is_some()),
        ScalarExpression::CollectSubquery { .. } => 1,
        ScalarExpression::Case {
            alternatives,
            else_expression,
        } => scalar_case_expression_correlated_subquery_count(
            alternatives,
            else_expression.as_deref(),
        ),
        ScalarExpression::Property(_)
        | ScalarExpression::StageValue { .. }
        | ScalarExpression::UndirectedEndpointProperty { .. }
        | ScalarExpression::UndirectedEndpointKey { .. }
        | ScalarExpression::UndirectedEndpointElementId { .. }
        | ScalarExpression::UndirectedEndpointLabels { .. }
        | ScalarExpression::UndirectedEndpointPropertyKeys { .. }
        | ScalarExpression::Literal(_)
        | ScalarExpression::LiteralList { .. }
        | ScalarExpression::TypedLiteralList { .. }
        | ScalarExpression::GraphKeyList { .. }
        | ScalarExpression::PathValue { .. }
        | ScalarExpression::Key { .. }
        | ScalarExpression::ElementId { .. }
        | ScalarExpression::GraphIdentity { .. }
        | ScalarExpression::GraphPresence { .. }
        | ScalarExpression::NodeLabels { .. }
        | ScalarExpression::PropertyKeys { .. }
        | ScalarExpression::RelationshipType { .. } => 0,
        ScalarExpression::PresenceGated { .. }
        | ScalarExpression::Coalesce { .. }
        | ScalarExpression::NullIf { .. }
        | ScalarExpression::Replace { .. }
        | ScalarExpression::Substring { .. }
        | ScalarExpression::Left { .. }
        | ScalarExpression::Right { .. }
        | ScalarExpression::StringIndices { .. }
        | ScalarExpression::LPad { .. }
        | ScalarExpression::RPad { .. }
        | ScalarExpression::StringContains { .. }
        | ScalarExpression::StringStartsWith { .. }
        | ScalarExpression::StringEndsWith { .. }
        | ScalarExpression::Round { .. }
        | ScalarExpression::Temporal(_)
        | ScalarExpression::Arithmetic { .. }
        | ScalarExpression::ListConcat { .. }
        | ScalarExpression::ListIndex { .. }
        | ScalarExpression::Atan2 { .. }
        | ScalarExpression::ToString { .. }
        | ScalarExpression::ToInteger { .. }
        | ScalarExpression::ToFloat { .. }
        | ScalarExpression::ToBoolean { .. }
        | ScalarExpression::ToStringOrNull { .. }
        | ScalarExpression::ToIntegerOrNull { .. }
        | ScalarExpression::ToFloatOrNull { .. }
        | ScalarExpression::ToBooleanOrNull { .. }
        | ScalarExpression::ToLower { .. }
        | ScalarExpression::ToUpper { .. }
        | ScalarExpression::Trim { .. }
        | ScalarExpression::LTrim { .. }
        | ScalarExpression::RTrim { .. }
        | ScalarExpression::CharacterLength { .. }
        | ScalarExpression::Reverse { .. }
        | ScalarExpression::Abs { .. }
        | ScalarExpression::Ceil { .. }
        | ScalarExpression::Floor { .. }
        | ScalarExpression::Sqrt { .. }
        | ScalarExpression::Sign { .. }
        | ScalarExpression::Exp { .. }
        | ScalarExpression::Log { .. }
        | ScalarExpression::Log10 { .. }
        | ScalarExpression::Sin { .. }
        | ScalarExpression::Cos { .. }
        | ScalarExpression::Tan { .. }
        | ScalarExpression::Cot { .. }
        | ScalarExpression::Asin { .. }
        | ScalarExpression::Acos { .. }
        | ScalarExpression::Atan { .. }
        | ScalarExpression::Degrees { .. }
        | ScalarExpression::Radians { .. }
        | ScalarExpression::IsNaN { .. }
        | ScalarExpression::Negate { .. } => {
            unreachable!("child-bearing scalar expressions handled above")
        }
    }
}

pub(crate) fn scalar_case_expression_correlated_subquery_count(
    alternatives: &[ScalarCaseAlternative],
    else_expression: Option<&ScalarExpression>,
) -> usize {
    alternatives
        .iter()
        .map(|alternative| {
            predicate_expression_correlated_subquery_count(&alternative.when)
                + scalar_expression_correlated_subquery_count(&alternative.then)
        })
        .sum::<usize>()
        + optional_scalar_expression_correlated_subquery_count(else_expression)
}

pub(crate) fn predicate_expression_correlated_subquery_count(
    predicate: &PredicateExpression,
) -> usize {
    match predicate {
        PredicateExpression::ExistsPattern(_) => 1,
        PredicateExpression::ScalarComparison(predicate) => {
            scalar_expression_correlated_subquery_count(&predicate.lhs)
                + match &predicate.rhs {
                    ScalarPredicateRhs::Expression(expression) => {
                        scalar_expression_correlated_subquery_count(expression)
                    }
                    ScalarPredicateRhs::List(_) => 0,
                }
        }
        PredicateExpression::And { left, right }
        | PredicateExpression::Or { left, right }
        | PredicateExpression::Xor { left, right } => {
            predicate_expression_correlated_subquery_count(left)
                + predicate_expression_correlated_subquery_count(right)
        }
        PredicateExpression::Not { expression } => {
            predicate_expression_correlated_subquery_count(expression)
        }
        PredicateExpression::Boolean(_)
        | PredicateExpression::Comparison(_)
        | PredicateExpression::KeyComparison(_)
        | PredicateExpression::ElementIdComparison(_)
        | PredicateExpression::Presence(_)
        | PredicateExpression::PropertyKeyMembership(_) => 0,
    }
}

pub(crate) fn count_star_order_expression_for_projection(
    projections: &[Projection],
    path: impl Into<String>,
) -> Result<OrderExpression, CoreError> {
    let path = path.into();
    let aliases = projections
        .iter()
        .filter_map(|projection| match projection {
            Projection::CountAll { alias } => Some(alias.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    match aliases.as_slice() {
        [alias] => Ok(OrderExpression::ProjectionAlias(alias.clone())),
        [] => Ok(OrderExpression::CountAll),
        _ => Err(unsupported(
            path,
            "ORDER BY count(*) is ambiguous because multiple RETURN projections match",
        )),
    }
}

pub(crate) fn aggregate_order_expression_for_projection(
    function: &FunctionInvocation,
    projections: &[Projection],
    path: impl Into<String>,
    plan: &GraphPlan,
    state: &CypherCompileState,
    context: &CypherCompileContext,
) -> Result<OrderExpression, CoreError> {
    let path = path.into();
    let function_kind = compile_aggregate_function(function, &path, context)?.ok_or_else(|| {
        unsupported(
            path.clone(),
            format!(
                "ORDER BY function '{}' is not supported yet",
                qualified_function_name(function)
            ),
        )
    })?;
    let target = compile_function_aggregate_target(
        function,
        function_kind,
        &path,
        Some(plan),
        Some(state),
        context,
    )?;
    let aliases = projections
        .iter()
        .filter_map(|projection| match projection {
            Projection::Aggregate {
                function: projection_function,
                target: projection_target,
                distinct,
                alias,
            } if *projection_function == function_kind
                && projection_target == &target
                && *distinct == function.distinct =>
            {
                Some(alias.clone())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    match aliases.as_slice() {
        [alias] => Ok(OrderExpression::ProjectionAlias(alias.clone())),
        [] => Ok(OrderExpression::Aggregate {
            function: function_kind,
            target,
            distinct: function.distinct,
        }),
        _ => Err(unsupported(
            path,
            format!(
                "ORDER BY {}() is ambiguous because multiple RETURN projections match",
                aggregate_function_name(function_kind)
            ),
        )),
    }
}

pub(crate) fn compile_id_order_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<OrderExpression, CoreError> {
    let path = path.into();
    if let Some(value) = compile_optional_same_label_undirected_endpoint_function_argument(
        function,
        format!("{path}.arguments"),
        plan,
        context,
    )? {
        return Ok(OrderExpression::Scalar(
            same_label_undirected_endpoint_key_scalar_expression(value),
        ));
    }
    let value = compile_id_graph_value_ref(function, path, plan, context)?;
    Ok(match value.presence_variable {
        Some(_) => OrderExpression::Scalar(graph_value_key_scalar_expression(value)),
        None => OrderExpression::Key {
            variable: value.variable,
        },
    })
}

pub(crate) fn compile_element_id_order_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<OrderExpression, CoreError> {
    let path = path.into();
    if let Some(value) = compile_optional_same_label_undirected_endpoint_function_argument(
        function,
        format!("{path}.arguments"),
        plan,
        context,
    )? {
        return Ok(OrderExpression::Scalar(
            same_label_undirected_endpoint_element_id_scalar_expression(value),
        ));
    }
    let value = compile_element_id_graph_value_ref(function, path, plan, context)?;
    Ok(match value.presence_variable {
        Some(_) => OrderExpression::Scalar(graph_value_element_id_scalar_expression(value)),
        None => OrderExpression::ElementId {
            variable: value.variable,
        },
    })
}

pub(crate) fn compile_type_order_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<OrderExpression, CoreError> {
    let ScalarExpression::RelationshipType {
        variable,
        relationship_type,
    } = compile_relationship_type_scalar_expression(function, path, plan, context)?
    else {
        unreachable!("relationship type helper returned non-relationship scalar expression");
    };
    Ok(OrderExpression::RelationshipType {
        variable,
        relationship_type,
    })
}

pub(crate) fn projection_order_expression_for_alias(
    variable: &Variable,
    projections: &[Projection],
    path: impl Into<String>,
) -> Result<OrderExpression, CoreError> {
    let path = path.into();
    let alias = variable_name(variable);
    optional_projection_order_expression_for_alias(variable, projections, path.clone())?.ok_or_else(
        || {
            unsupported(
                path,
                format!("ORDER BY alias '{alias}' does not match a projection"),
            )
        },
    )
}

pub(crate) fn optional_projection_order_expression_for_alias(
    variable: &Variable,
    projections: &[Projection],
    path: impl Into<String>,
) -> Result<Option<OrderExpression>, CoreError> {
    let path = path.into();
    let alias = variable_name(variable);
    let mut found_property = None;
    let mut found_projected_alias = false;
    for projection in projections {
        match projection {
            Projection::Property {
                property,
                alias: Some(projection_alias),
            } if projection_alias == &alias => {
                if found_property.is_some() {
                    return Err(unsupported(
                        path,
                        format!("ORDER BY alias '{alias}' is ambiguous"),
                    ));
                }
                found_property = Some(property.clone());
            }
            Projection::CountAll {
                alias: projection_alias,
            }
            | Projection::Key {
                alias: projection_alias,
                ..
            }
            | Projection::ElementId {
                alias: projection_alias,
                ..
            }
            | Projection::NodeLabels {
                alias: projection_alias,
                ..
            }
            | Projection::PropertyKeys {
                alias: projection_alias,
                ..
            }
            | Projection::RelationshipType {
                alias: projection_alias,
                ..
            }
            | Projection::Literal {
                alias: projection_alias,
                ..
            }
            | Projection::LiteralList {
                alias: projection_alias,
                ..
            }
            | Projection::Expression {
                alias: projection_alias,
                ..
            }
            | Projection::Aggregate {
                alias: projection_alias,
                ..
            } if projection_alias == &alias => {
                if found_projected_alias {
                    return Err(unsupported(
                        path,
                        format!("ORDER BY alias '{alias}' is ambiguous"),
                    ));
                }
                found_projected_alias = true;
            }
            _ => {}
        }
    }
    if found_property.is_some() && found_projected_alias {
        return Err(unsupported(
            path,
            format!("ORDER BY alias '{alias}' is ambiguous"),
        ));
    }
    if let Some(property) = found_property {
        return Ok(Some(OrderExpression::Property(property)));
    }
    if found_projected_alias {
        return Ok(Some(OrderExpression::ProjectionAlias(alias)));
    }
    Ok(None)
}

pub(crate) fn compile_projection(
    item: &ProjectionItem,
    path: impl Into<String>,
    context: &CypherCompileContext,
    plan: &GraphPlan,
    state: &CypherCompileState,
) -> Result<Projection, CoreError> {
    let path = path.into();
    if let Some(projection) =
        compile_optional_stage_list_index_projection(item, path.clone(), state, context)?
    {
        return Ok(projection);
    }
    if let Some(projection) =
        compile_optional_graph_scalar_projection(item, path.clone(), context, plan, state)?
    {
        return Ok(projection);
    }
    match &item.expression {
        Expression::CountStar { .. } => Ok(Projection::CountAll {
            alias: item
                .alias
                .as_ref()
                .map_or_else(|| "count".to_string(), variable_name),
        }),
        Expression::CountSubquery(count) => {
            compile_count_subquery_projection(count, item, path, plan, context)
        }
        Expression::CollectSubquery(collect) => {
            compile_collect_subquery_projection(collect, item, path, plan, context)
        }
        Expression::PatternComprehension(comprehension) => {
            compile_pattern_comprehension_projection(comprehension, item, path, plan, context)
        }
        expression if is_literal_projection_expression(expression) => {
            compile_literal_projection(expression, item, path, context)
        }
        expression if is_boolean_scalar_expression(expression) => {
            compile_boolean_scalar_projection(expression, item, path, plan, state, context)
        }
        Expression::Parenthesized(inner) if is_arithmetic_expression(inner) => {
            compile_arithmetic_projection(item, path, plan, state, context)
        }
        Expression::UnaryOp {
            op: UnaryOperator::Negate,
            ..
        } => compile_arithmetic_projection(item, path, plan, state, context),
        Expression::BinaryOp { .. } => {
            if let Some(projection) =
                compile_optional_static_list_projection(item, path.clone(), plan, context)?
            {
                return Ok(projection);
            }
            compile_arithmetic_projection(item, path, plan, state, context)
        }
        Expression::ListComprehension(_) => {
            if let Some(projection) =
                compile_optional_static_list_projection(item, path.clone(), plan, context)?
            {
                return Ok(projection);
            }
            Err(unsupported(
                format!("{path}.expression"),
                "list comprehensions require a literal list, list parameter, static split(...), range(...), tail(...), or static labels()/keys() metadata list",
            ))
        }
        Expression::Case(case) => compile_case_projection(case, item, path, plan, state, context),
        Expression::FunctionCall(function) if is_id_function(function) => {
            compile_id_projection(function, item, path, plan, context)
        }
        Expression::FunctionCall(function) if is_element_id_function(function) => {
            compile_element_id_projection(function, item, path, plan, context)
        }
        Expression::FunctionCall(function) if is_internal_graph_identity_function(function) => {
            compile_internal_graph_identity_projection(function, item, path, plan, context)
        }
        Expression::FunctionCall(function) if is_internal_graph_presence_function(function) => {
            compile_internal_graph_presence_projection(function, item, path, plan, context)
        }
        Expression::FunctionCall(function) if is_type_function(function) => {
            compile_type_projection(function, item, path, plan, context)
        }
        Expression::FunctionCall(function) if is_labels_function(function) => {
            compile_labels_projection(function, item, path, plan, context)
        }
        Expression::FunctionCall(function) if is_keys_function(function) => {
            compile_keys_or_static_map_keys_projection(function, item, path, plan, context)
        }
        Expression::FunctionCall(function) if is_length_function(function) => {
            compile_path_length_projection(function, item, path, state, context)
        }
        Expression::FunctionCall(function) => {
            compile_other_function_projection(function, item, path, plan, state, context)
        }
        expression => Ok(Projection::Property {
            property: compile_property_ref(
                expression,
                format!("{path}.expression"),
                Some(plan),
                context,
            )?,
            alias: item.alias.as_ref().map(variable_name),
        }),
    }
}

pub(crate) fn compile_optional_stage_list_index_projection(
    item: &ProjectionItem,
    path: impl Into<String>,
    state: &CypherCompileState,
    context: &CypherCompileContext,
) -> Result<Option<Projection>, CoreError> {
    let path = path.into();
    compile_optional_stage_list_index_scalar_expression(
        &item.expression,
        format!("{path}.expression"),
        Some(state),
        context,
    )
    .map(|expression| {
        expression.map(|expression| Projection::Expression {
            expression,
            alias: item
                .alias
                .as_ref()
                .map_or_else(|| "expression".to_string(), variable_name),
        })
    })
}

pub(crate) fn compile_keys_or_static_map_keys_projection(
    function: &FunctionInvocation,
    item: &ProjectionItem,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    if is_literal_map_keys_function(function)
        && let Some(projection) =
            compile_optional_static_list_projection(item, path.clone(), plan, context)?
    {
        return Ok(projection);
    }
    compile_keys_projection(function, item, path, plan, context)
}

pub(crate) fn compile_other_function_projection(
    function: &FunctionInvocation,
    item: &ProjectionItem,
    path: impl Into<String>,
    plan: &GraphPlan,
    state: &CypherCompileState,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    if let Some(projection) = compile_optional_path_list_reducer_projection(
        function,
        item,
        path.clone(),
        plan,
        state,
        context,
    )? {
        return Ok(projection);
    }
    if let Some(projection) =
        compile_optional_static_list_projection(item, path.clone(), plan, context)?
    {
        return Ok(projection);
    }
    if let Some(projection) =
        compile_optional_size_path_length_projection(function, item, path.clone(), state, context)?
    {
        return Ok(projection);
    }
    if let Some(projection) =
        compile_scalar_function_projection(function, item, path.clone(), plan, state, context)?
    {
        return Ok(projection);
    }
    if is_aggregate_function_call(function) {
        return compile_aggregate_projection(function, item, path, plan, state, context);
    }
    Err(unsupported(
        format!("{path}.expression"),
        format!(
            "RETURN function '{}' is not supported yet",
            qualified_function_name(function)
        ),
    ))
}

pub(crate) fn compile_optional_path_list_reducer_projection(
    function: &FunctionInvocation,
    item: &ProjectionItem,
    path: impl Into<String>,
    plan: &GraphPlan,
    state: &CypherCompileState,
    context: &CypherCompileContext,
) -> Result<Option<Projection>, CoreError> {
    let path = path.into();
    let [argument] = function.arguments.as_slice() else {
        return Ok(None);
    };
    let mode = PredicateCompileMode::Graph {
        plan,
        path_state: Some(state),
    };
    let expression = if is_tail_function(function) {
        compile_optional_path_list_tail_scalar_expression(
            argument,
            format!("{path}.expression.arguments[0]"),
            mode,
            context,
        )?
    } else if is_reverse_function(function) {
        compile_optional_path_list_reverse_scalar_expression(
            argument,
            format!("{path}.expression.arguments[0]"),
            mode,
            context,
        )?
    } else {
        None
    };
    let Some(expression) = expression else {
        return Ok(None);
    };
    Ok(Some(Projection::Expression {
        expression,
        alias: item
            .alias
            .as_ref()
            .map_or_else(|| "list".to_string(), variable_name),
    }))
}

pub(crate) fn compile_optional_static_list_projection(
    item: &ProjectionItem,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<Projection>, CoreError> {
    let path = path.into();
    let Some(expression) = compile_optional_static_list_scalar_expression(
        &item.expression,
        format!("{path}.expression"),
        Some(plan),
        context,
    )?
    else {
        return Ok(None);
    };
    Ok(Some(Projection::Expression {
        expression,
        alias: item
            .alias
            .as_ref()
            .map_or_else(|| "list".to_string(), variable_name),
    }))
}

pub(crate) fn compile_optional_graph_scalar_projection(
    item: &ProjectionItem,
    path: impl Into<String>,
    context: &CypherCompileContext,
    plan: &GraphPlan,
    state: &CypherCompileState,
) -> Result<Option<Projection>, CoreError> {
    let path = path.into();
    if let Some((expression, output_name)) = compile_optional_endpoint_property_scalar_expression(
        &item.expression,
        format!("{path}.expression"),
        Some(plan),
        context,
    )? {
        return Ok(Some(Projection::Expression {
            expression,
            alias: item.alias.as_ref().map_or(output_name, variable_name),
        }));
    }
    if let Some(projection) = compile_optional_path_value_projection(item, path.clone(), state)? {
        return Ok(Some(projection));
    }
    if let Some(expression) = compile_optional_temporal_component_scalar_expression(
        &item.expression,
        format!("{path}.expression"),
        PredicateCompileMode::Graph {
            plan,
            path_state: Some(state),
        },
        context,
    )? {
        return Ok(Some(Projection::Expression {
            expression,
            alias: item
                .alias
                .as_ref()
                .map_or_else(|| "expression".to_string(), variable_name),
        }));
    }
    if let Some(projection) =
        compile_optional_static_map_lookup_projection(item, path.clone(), plan, state, context)?
    {
        return Ok(Some(projection));
    }
    if let Some(property) = compile_optional_property_ref(
        &item.expression,
        format!("{path}.expression"),
        Some(plan),
        context,
    )? {
        return Ok(Some(Projection::Property {
            property,
            alias: item.alias.as_ref().map(variable_name),
        }));
    }
    if let Some(projection) =
        compile_optional_graph_list_scalar_projection(item, path.clone(), plan, state, context)?
    {
        return Ok(Some(projection));
    }
    Ok(None)
}

pub(crate) fn compile_optional_path_value_projection(
    item: &ProjectionItem,
    path: impl Into<String>,
    state: &CypherCompileState,
) -> Result<Option<Projection>, CoreError> {
    let path = path.into();
    let Expression::Variable(variable) = &item.expression else {
        return Ok(None);
    };
    let name = variable_name(variable);
    let Some(binding) = state.path_variables.get(&name) else {
        return Ok(None);
    };
    if binding.uses_relationship_range_syntax {
        return Err(unsupported(
            format!("{path}.expression"),
            format!(
                "path variable '{name}' cannot be used as a graph value because Coral does not materialize variable-length path values yet"
            ),
        ));
    }
    let expression = ScalarExpression::PathValue {
        node_variables: binding.node_variables.clone(),
        relationship_variables: binding.relationship_variables.clone(),
    };
    let expression =
        path_binding_presence_gated_scalar_expression(binding, expression, path, "path value")?;
    Ok(Some(Projection::Expression {
        expression,
        alias: item.alias.as_ref().map_or(name, variable_name),
    }))
}

pub(crate) fn compile_optional_graph_list_scalar_projection(
    item: &ProjectionItem,
    path: impl Into<String>,
    plan: &GraphPlan,
    state: &CypherCompileState,
    context: &CypherCompileContext,
) -> Result<Option<Projection>, CoreError> {
    let path = path.into();
    if let Some(expression) = compile_optional_path_list_index_scalar_expression(
        &item.expression,
        format!("{path}.expression"),
        PredicateCompileMode::Graph {
            plan,
            path_state: Some(state),
        },
        context,
    )? {
        return Ok(Some(Projection::Expression {
            expression,
            alias: item
                .alias
                .as_ref()
                .map_or_else(|| "expression".to_string(), variable_name),
        }));
    }
    if let Some(expression) = compile_optional_path_list_slice_scalar_expression(
        &item.expression,
        format!("{path}.expression"),
        PredicateCompileMode::Graph {
            plan,
            path_state: Some(state),
        },
        context,
    )? {
        return Ok(Some(Projection::Expression {
            expression,
            alias: item
                .alias
                .as_ref()
                .map_or_else(|| "list".to_string(), variable_name),
        }));
    }
    if let Some(projection) =
        compile_optional_metadata_list_slice_projection(item, path.clone(), plan, context)?
    {
        return Ok(Some(projection));
    }
    if let Some(expression) = compile_optional_metadata_list_index_scalar_expression(
        &item.expression,
        format!("{path}.expression"),
        plan,
        context,
    )? {
        return Ok(Some(Projection::Expression {
            expression,
            alias: item
                .alias
                .as_ref()
                .map_or_else(|| "expression".to_string(), variable_name),
        }));
    }
    if let Some(expression) = compile_optional_non_literal_static_list_index_scalar_expression(
        &item.expression,
        format!("{path}.expression"),
        Some(plan),
        context,
    )? {
        return Ok(Some(Projection::Expression {
            expression,
            alias: item
                .alias
                .as_ref()
                .map_or_else(|| "expression".to_string(), variable_name),
        }));
    }
    if let Some(projection) =
        compile_optional_non_literal_static_list_slice_projection(item, path, plan, context)?
    {
        return Ok(Some(projection));
    }
    Ok(None)
}

pub(crate) fn compile_optional_static_map_lookup_projection(
    item: &ProjectionItem,
    path: impl Into<String>,
    plan: &GraphPlan,
    state: &CypherCompileState,
    context: &CypherCompileContext,
) -> Result<Option<Projection>, CoreError> {
    let path = path.into();
    if let Some(literal) = compile_optional_static_map_lookup_literal(
        &item.expression,
        format!("{path}.expression"),
        context,
    )
    .ok()
    .flatten()
    {
        let alias = item.alias.as_ref().map_or_else(
            || {
                static_map_lookup_output_name(&item.expression, context)
                    .unwrap_or_else(|| "literal".to_string())
            },
            variable_name,
        );
        return Ok(Some(Projection::Literal { literal, alias }));
    }

    let mode = PredicateCompileMode::Graph {
        plan,
        path_state: Some(state),
    };
    let Some(expression) = compile_optional_non_literal_static_map_lookup_scalar_expression(
        &item.expression,
        format!("{path}.expression"),
        mode,
        context,
    )?
    else {
        return Ok(None);
    };
    let alias = item.alias.as_ref().map_or_else(
        || {
            static_map_lookup_output_name(&item.expression, context)
                .unwrap_or_else(|| "expression".to_string())
        },
        variable_name,
    );
    if let ScalarExpression::Property(property) = expression {
        return Ok(Some(Projection::Property {
            property,
            alias: Some(alias),
        }));
    }
    Ok(Some(Projection::Expression { expression, alias }))
}

pub(crate) fn compile_optional_non_literal_static_list_slice_projection(
    item: &ProjectionItem,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<Projection>, CoreError> {
    let path = path.into();
    let Expression::ListSlice { list, .. } = &item.expression else {
        return Ok(None);
    };
    if is_literal_list_source_expression(list) {
        return Ok(None);
    }
    let Some(expression) = compile_optional_static_list_scalar_expression(
        &item.expression,
        format!("{path}.expression"),
        Some(plan),
        context,
    )?
    else {
        return Ok(None);
    };
    Ok(Some(Projection::Expression {
        expression,
        alias: item
            .alias
            .as_ref()
            .map_or_else(|| "list".to_string(), variable_name),
    }))
}

pub(crate) fn compile_optional_metadata_list_slice_projection(
    item: &ProjectionItem,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<Projection>, CoreError> {
    let path = path.into();
    if !is_list_slice_expression(&item.expression) {
        return Ok(None);
    }
    let Some(value) = compile_optional_metadata_list_value(
        &item.expression,
        format!("{path}.expression"),
        plan,
        context,
    )?
    else {
        return Ok(None);
    };
    let alias = item
        .alias
        .as_ref()
        .map_or_else(|| "list".to_string(), variable_name);
    if value.presence_variable.is_none() && !value.literals.is_empty() {
        return Ok(Some(Projection::LiteralList {
            literals: value.literals,
            alias,
        }));
    }
    Ok(Some(Projection::Expression {
        expression: metadata_list_value_scalar_expression(value, plan),
        alias,
    }))
}

pub(crate) fn is_list_slice_expression(expression: &Expression) -> bool {
    match expression {
        Expression::Parenthesized(inner) => is_list_slice_expression(inner),
        Expression::ListSlice { .. } => true,
        _ => false,
    }
}

pub(crate) fn compile_path_length_projection(
    function: &FunctionInvocation,
    item: &ProjectionItem,
    path: impl Into<String>,
    state: &CypherCompileState,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    let expression = compile_path_length_scalar_expression(
        function,
        format!("{path}.expression.arguments"),
        state,
        context,
    )?;
    Ok(Projection::Expression {
        expression,
        alias: item
            .alias
            .as_ref()
            .map_or_else(|| path_length_function_alias(function), variable_name),
    })
}

pub(crate) fn compile_optional_size_path_length_projection(
    function: &FunctionInvocation,
    item: &ProjectionItem,
    path: impl Into<String>,
    state: &CypherCompileState,
    context: &CypherCompileContext,
) -> Result<Option<Projection>, CoreError> {
    let path = path.into();
    let Some(expression) = compile_optional_size_path_length_scalar_expression(
        function,
        format!("{path}.expression.arguments"),
        state,
        context,
    )?
    else {
        return Ok(None);
    };
    Ok(Some(Projection::Expression {
        expression,
        alias: item
            .alias
            .as_ref()
            .map_or_else(|| path_length_function_alias(function), variable_name),
    }))
}

pub(crate) fn compile_path_length_order_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    state: &CypherCompileState,
    context: &CypherCompileContext,
) -> Result<OrderExpression, CoreError> {
    let path = path.into();
    let expression = compile_path_length_scalar_expression(
        function,
        format!("{path}.arguments"),
        state,
        context,
    )?;
    Ok(match expression {
        ScalarExpression::Literal(literal) => OrderExpression::Literal(literal),
        expression => OrderExpression::Scalar(expression),
    })
}

pub(crate) fn compile_optional_size_path_length_order_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    state: &CypherCompileState,
    context: &CypherCompileContext,
) -> Result<Option<OrderExpression>, CoreError> {
    let path = path.into();
    let Some(expression) = compile_optional_size_path_length_scalar_expression(
        function,
        format!("{path}.arguments"),
        state,
        context,
    )?
    else {
        return Ok(None);
    };
    Ok(Some(match expression {
        ScalarExpression::Literal(literal) => OrderExpression::Literal(literal),
        expression => OrderExpression::Scalar(expression),
    }))
}

pub(crate) fn compile_path_length_scalar_expression(
    function: &FunctionInvocation,
    arguments_path: impl Into<String>,
    state: &CypherCompileState,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let arguments_path = arguments_path.into();
    let binding =
        compile_path_function_binding(function, arguments_path.clone(), "length", state, context)?;
    compile_path_length_binding_scalar_expression(binding, arguments_path, "length")
}

pub(crate) fn compile_path_function_binding<'a>(
    function: &FunctionInvocation,
    arguments_path: impl Into<String>,
    function_name: &str,
    state: &'a CypherCompileState,
    context: &CypherCompileContext,
) -> Result<&'a PathBinding, CoreError> {
    let arguments_path = arguments_path.into();
    let variable = compile_single_variable_function_argument(
        function,
        arguments_path.clone(),
        match function_name {
            "length" => "length() supports exactly one path variable argument",
            "nodes" => "nodes() supports exactly one path variable argument",
            "relationships" => "relationships() supports exactly one path variable argument",
            _ => "path metadata function supports exactly one path variable argument",
        },
        context,
    )?;
    let binding = state.path_variables.get(&variable).ok_or_else(|| {
        unsupported(
            format!("{arguments_path}[0]"),
            format!("{function_name}() argument '{variable}' is not a bound path variable"),
        )
    })?;
    Ok(binding)
}

pub(crate) fn compile_path_length_binding_scalar_expression(
    binding: &PathBinding,
    arguments_path: impl Into<String>,
    function_name: &str,
) -> Result<ScalarExpression, CoreError> {
    let arguments_path = arguments_path.into();
    let length = i64::try_from(binding.length)
        .map_err(|error| CoreError::internal(format!("path length overflow: {error}")))?;
    let expression = ScalarExpression::Literal(Literal::Integer(length));
    if binding.optional {
        let Some(presence_gate) = binding.presence_gate.clone() else {
            return if binding.length == 0 {
                Ok(expression)
            } else {
                Err(unsupported(
                    format!("{arguments_path}[0]"),
                    format!(
                        "{function_name}() over an OPTIONAL MATCH path requires a relationship binding so null-preserving path metadata can be gated"
                    ),
                ))
            };
        };
        return Ok(match presence_gate {
            PathPresenceGate::Variable(presence_variable) => {
                presence_gate_scalar_expression(Some(presence_variable), expression)
            }
            PathPresenceGate::Predicate(predicate) => ScalarExpression::Case {
                alternatives: vec![ScalarCaseAlternative {
                    when: predicate,
                    then: expression,
                }],
                else_expression: Some(Box::new(ScalarExpression::Literal(Literal::Null))),
            },
        });
    }
    Ok(expression)
}

pub(crate) fn compile_optional_size_path_length_scalar_expression(
    function: &FunctionInvocation,
    arguments_path: impl Into<String>,
    state: &CypherCompileState,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    if !is_size_function(function) {
        return Ok(None);
    }
    let arguments_path = arguments_path.into();
    if let Some(variable) = optional_single_variable_function_argument(function, context) {
        if !state.path_variables.contains_key(&variable) {
            return Ok(None);
        }
        return compile_path_length_scalar_expression(function, arguments_path, state, context)
            .map(Some);
    }
    let [argument] = function.arguments.as_slice() else {
        return Ok(None);
    };
    let path_list_function = match argument {
        Expression::Parenthesized(inner) => match inner.as_ref() {
            Expression::FunctionCall(function) => function,
            _ => return Ok(None),
        },
        Expression::FunctionCall(function) => function,
        _ => return Ok(None),
    };
    let Some(target) = path_list_size_target(path_list_function) else {
        return Ok(None);
    };
    compile_path_list_size_scalar_expression(
        path_list_function,
        target,
        format!("{arguments_path}[0].arguments"),
        state,
        context,
    )
    .map(Some)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PathListSizeTarget {
    Nodes,
    Relationships,
}

impl PathListSizeTarget {
    fn function_name(self) -> &'static str {
        match self {
            Self::Nodes => "nodes",
            Self::Relationships => "relationships",
        }
    }
}

pub(crate) fn path_list_size_target(function: &FunctionInvocation) -> Option<PathListSizeTarget> {
    if is_nodes_function(function) {
        Some(PathListSizeTarget::Nodes)
    } else if is_relationships_function(function) {
        Some(PathListSizeTarget::Relationships)
    } else {
        None
    }
}

pub(crate) fn compile_path_list_size_scalar_expression(
    function: &FunctionInvocation,
    target: PathListSizeTarget,
    arguments_path: impl Into<String>,
    state: &CypherCompileState,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let arguments_path = arguments_path.into();
    let binding = compile_path_function_binding(
        function,
        arguments_path.clone(),
        target.function_name(),
        state,
        context,
    )?;
    let length = compile_path_length_binding_scalar_expression(
        binding,
        arguments_path,
        target.function_name(),
    )?;
    match target {
        PathListSizeTarget::Relationships => Ok(length),
        PathListSizeTarget::Nodes => add_one_to_path_length_scalar_expression(length),
    }
}

pub(crate) fn compile_path_element_id_list_scalar_expression(
    function: &FunctionInvocation,
    target: PathListSizeTarget,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let state = mode.path_state().ok_or_else(|| {
        unsupported(
            path.clone(),
            format!(
                "{}() path element list expressions require path-variable scope",
                target.function_name()
            ),
        )
    })?;
    let binding = compile_path_function_binding(
        function,
        format!("{path}.arguments"),
        target.function_name(),
        state,
        context,
    )?;
    let variables = match target {
        PathListSizeTarget::Nodes => binding.node_variables.clone(),
        PathListSizeTarget::Relationships => binding.relationship_variables.clone(),
    };
    let expression = ScalarExpression::GraphKeyList { variables };
    path_binding_presence_gated_scalar_expression(
        binding,
        expression,
        format!("{path}.arguments"),
        target.function_name(),
    )
}

pub(crate) fn path_binding_element_key_list_scalar_expression(
    binding: &PathBinding,
    variables: Vec<String>,
    arguments_path: impl Into<String>,
    function_name: &str,
) -> Result<ScalarExpression, CoreError> {
    path_binding_presence_gated_scalar_expression(
        binding,
        ScalarExpression::GraphKeyList { variables },
        arguments_path,
        function_name,
    )
}

pub(crate) fn path_list_function_expression(
    expression: &Expression,
) -> Option<(&FunctionInvocation, PathListSizeTarget)> {
    match expression {
        Expression::Parenthesized(inner) => path_list_function_expression(inner),
        Expression::FunctionCall(function) => {
            path_list_size_target(function).map(|target| (function, target))
        }
        _ => None,
    }
}

pub(crate) fn path_binding_element_variables(
    binding: &PathBinding,
    target: PathListSizeTarget,
) -> &[String] {
    match target {
        PathListSizeTarget::Nodes => &binding.node_variables,
        PathListSizeTarget::Relationships => &binding.relationship_variables,
    }
}

pub(crate) fn compile_path_list_slice_variables(
    variables: &[String],
    start: Option<&Expression>,
    end: Option<&Expression>,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Vec<String>, CoreError> {
    let path = path.into();
    let len = i64::try_from(variables.len()).map_err(|error| {
        CoreError::internal(format!("path element list length overflow: {error}"))
    })?;
    let start = compile_list_slice_bound(
        start,
        0,
        len,
        format!("{path}.start"),
        context,
        "path element list slice bounds require integer literals or scalar integer parameters",
    )?;
    let end = compile_list_slice_bound(
        end,
        len,
        len,
        format!("{path}.end"),
        context,
        "path element list slice bounds require integer literals or scalar integer parameters",
    )?;
    if start >= end {
        return Ok(Vec::new());
    }
    variables
        .get(start..end)
        .map(<[String]>::to_vec)
        .ok_or_else(|| CoreError::internal("path element list slice bounds were invalid"))
}

pub(crate) fn compile_optional_path_list_slice_scalar_expression(
    expression: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_optional_path_list_slice_scalar_expression(inner, path, mode, context)
        }
        Expression::ListSlice {
            list, start, end, ..
        } => {
            let Some((function, target)) = path_list_function_expression(list) else {
                return Ok(None);
            };
            let state = mode.path_state().ok_or_else(|| {
                unsupported(
                    format!("{path}.list"),
                    format!(
                        "{}() path element list slices require path-variable scope",
                        target.function_name()
                    ),
                )
            })?;
            let binding = compile_path_function_binding(
                function,
                format!("{path}.list.arguments"),
                target.function_name(),
                state,
                context,
            )?;
            let variables = compile_path_list_slice_variables(
                path_binding_element_variables(binding, target),
                start.as_deref(),
                end.as_deref(),
                path.clone(),
                context,
            )?;
            path_binding_element_key_list_scalar_expression(
                binding,
                variables,
                format!("{path}.list.arguments"),
                target.function_name(),
            )
            .map(Some)
        }
        _ => Ok(None),
    }
}

pub(crate) fn compile_path_list_static_index(
    index: &Expression,
    length: usize,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Option<usize>, CoreError> {
    let path = path.into();
    let index = compile_literal(index, format!("{path}.index"), context)?;
    let Literal::Integer(index) = index else {
        return Err(unsupported(
            format!("{path}.index"),
            "path element list indexes require an integer literal or scalar integer parameter",
        ));
    };
    let length = i64::try_from(length).map_err(|error| {
        CoreError::internal(format!("path element list length overflow: {error}"))
    })?;
    let normalized = if index < 0 { length + index } else { index };
    if normalized < 0 || normalized >= length {
        return Ok(None);
    }
    usize::try_from(normalized)
        .map(Some)
        .map_err(|error| CoreError::internal(format!("path element list index overflow: {error}")))
}

pub(crate) fn compile_optional_path_list_index_scalar_expression(
    expression: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_optional_path_list_index_scalar_expression(inner, path, mode, context)
        }
        Expression::ListIndex { list, index, .. } => {
            let Some((function, target)) = path_list_function_expression(list) else {
                return Ok(None);
            };
            let state = mode.path_state().ok_or_else(|| {
                unsupported(
                    format!("{path}.list"),
                    format!(
                        "{}() path element list indexes require path-variable scope",
                        target.function_name()
                    ),
                )
            })?;
            let binding = compile_path_function_binding(
                function,
                format!("{path}.list.arguments"),
                target.function_name(),
                state,
                context,
            )?;
            let variables = path_binding_element_variables(binding, target);
            let expression =
                compile_path_list_static_index(index, variables.len(), path.clone(), context)?
                    .and_then(|index| variables.get(index))
                    .map_or(ScalarExpression::Literal(Literal::Null), |variable| {
                        ScalarExpression::Key {
                            variable: variable.clone(),
                        }
                    });
            path_binding_presence_gated_scalar_expression(
                binding,
                expression,
                format!("{path}.list.arguments"),
                target.function_name(),
            )
            .map(Some)
        }
        _ => Ok(None),
    }
}

pub(crate) fn compile_optional_path_list_tail_scalar_expression(
    argument: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    let Some((function, target)) = path_list_function_expression(argument) else {
        return Ok(None);
    };
    let state = mode.path_state().ok_or_else(|| {
        unsupported(
            path.clone(),
            format!(
                "{}() path element list tail access requires path-variable scope",
                target.function_name()
            ),
        )
    })?;
    let binding = compile_path_function_binding(
        function,
        format!("{path}.arguments"),
        target.function_name(),
        state,
        context,
    )?;
    let variables = path_binding_element_variables(binding, target)
        .get(1..)
        .unwrap_or_default()
        .to_vec();
    path_binding_element_key_list_scalar_expression(
        binding,
        variables,
        format!("{path}.arguments"),
        target.function_name(),
    )
    .map(Some)
}

pub(crate) fn compile_optional_path_list_reverse_scalar_expression(
    argument: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    let Some((function, target)) = path_list_function_expression(argument) else {
        return Ok(None);
    };
    let state = mode.path_state().ok_or_else(|| {
        unsupported(
            path.clone(),
            format!(
                "{}() path element list reverse access requires path-variable scope",
                target.function_name()
            ),
        )
    })?;
    let binding = compile_path_function_binding(
        function,
        format!("{path}.arguments"),
        target.function_name(),
        state,
        context,
    )?;
    let mut variables = path_binding_element_variables(binding, target).to_vec();
    variables.reverse();
    path_binding_element_key_list_scalar_expression(
        binding,
        variables,
        format!("{path}.arguments"),
        target.function_name(),
    )
    .map(Some)
}

pub(crate) fn compile_optional_path_list_endpoint_scalar_expression(
    argument: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
    endpoint: ListEndpoint,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    let Some((function, target)) = path_list_function_expression(argument) else {
        return Ok(None);
    };
    let state = mode.path_state().ok_or_else(|| {
        unsupported(
            path.clone(),
            format!(
                "{}() path element list endpoint access requires path-variable scope",
                target.function_name()
            ),
        )
    })?;
    let binding = compile_path_function_binding(
        function,
        format!("{path}.arguments"),
        target.function_name(),
        state,
        context,
    )?;
    let variables = path_binding_element_variables(binding, target);
    let variable = match endpoint {
        ListEndpoint::Head => variables.first(),
        ListEndpoint::Last => variables.last(),
    };
    let expression = variable.map_or(ScalarExpression::Literal(Literal::Null), |variable| {
        ScalarExpression::Key {
            variable: variable.clone(),
        }
    });
    path_binding_presence_gated_scalar_expression(
        binding,
        expression,
        format!("{path}.arguments"),
        target.function_name(),
    )
    .map(Some)
}

pub(crate) fn path_binding_presence_gated_scalar_expression(
    binding: &PathBinding,
    expression: ScalarExpression,
    arguments_path: impl Into<String>,
    function_name: &str,
) -> Result<ScalarExpression, CoreError> {
    let arguments_path = arguments_path.into();
    if binding.optional {
        let Some(presence_gate) = binding.presence_gate.clone() else {
            return if binding.length == 0 {
                Ok(expression)
            } else {
                Err(unsupported(
                    format!("{arguments_path}[0]"),
                    format!(
                        "{function_name}() over an OPTIONAL MATCH path requires a relationship binding so null-preserving path values can be gated"
                    ),
                ))
            };
        };
        return Ok(match presence_gate {
            PathPresenceGate::Variable(presence_variable) => {
                presence_gate_scalar_expression(Some(presence_variable), expression)
            }
            PathPresenceGate::Predicate(predicate) => ScalarExpression::Case {
                alternatives: vec![ScalarCaseAlternative {
                    when: predicate,
                    then: expression,
                }],
                else_expression: Some(Box::new(ScalarExpression::Literal(Literal::Null))),
            },
        });
    }
    Ok(expression)
}

pub(crate) fn path_length_function_alias(function: &FunctionInvocation) -> String {
    if is_size_function(function) {
        "size".to_string()
    } else {
        "length".to_string()
    }
}

pub(crate) fn compile_literal_projection(
    expression: &Expression,
    item: &ProjectionItem,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    match compile_projection_literal(expression, format!("{path}.expression"), context)? {
        ProjectionLiteral::Scalar(literal) => Ok(Projection::Literal {
            literal,
            alias: item
                .alias
                .as_ref()
                .map_or_else(|| "literal".to_string(), variable_name),
        }),
        ProjectionLiteral::List(literals) => Ok(Projection::LiteralList {
            literals,
            alias: item
                .alias
                .as_ref()
                .map_or_else(|| "list".to_string(), variable_name),
        }),
    }
}

pub(crate) fn compile_arithmetic_projection(
    item: &ProjectionItem,
    path: impl Into<String>,
    plan: &GraphPlan,
    state: &CypherCompileState,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    let expression = compile_scalar_expression_with_path_state(
        &item.expression,
        format!("{path}.expression"),
        plan,
        Some(state),
        context,
    )?;
    Ok(Projection::Expression {
        expression,
        alias: item
            .alias
            .as_ref()
            .map_or_else(|| "expression".to_string(), variable_name),
    })
}

pub(crate) fn compile_boolean_scalar_projection(
    expression: &Expression,
    item: &ProjectionItem,
    path: impl Into<String>,
    plan: &GraphPlan,
    state: &CypherCompileState,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    let expression = compile_predicate_expression_with_path_state(
        expression,
        format!("{path}.expression"),
        plan,
        Some(state),
        context,
    )
    .map(|predicate| ScalarExpression::Predicate(Box::new(predicate)))?;
    Ok(Projection::Expression {
        expression,
        alias: item
            .alias
            .as_ref()
            .map_or_else(|| "expression".to_string(), variable_name),
    })
}

pub(crate) fn compile_case_projection(
    case: &CaseExpression,
    item: &ProjectionItem,
    path: impl Into<String>,
    plan: &GraphPlan,
    state: &CypherCompileState,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    let expression = compile_case_scalar_expression_with_path_state(
        case,
        format!("{path}.expression"),
        plan,
        Some(state),
        context,
    )?;
    Ok(Projection::Expression {
        expression,
        alias: item
            .alias
            .as_ref()
            .map_or_else(|| "case".to_string(), variable_name),
    })
}

pub(crate) fn compile_scalar_function_projection(
    function: &FunctionInvocation,
    item: &ProjectionItem,
    path: impl Into<String>,
    plan: &GraphPlan,
    state: &CypherCompileState,
    context: &CypherCompileContext,
) -> Result<Option<Projection>, CoreError> {
    let path = path.into();
    let Some(expression) = compile_scalar_function_expression_with_path_state(
        function,
        format!("{path}.expression"),
        plan,
        Some(state),
        context,
    )?
    else {
        return Ok(None);
    };
    Ok(Some(Projection::Expression {
        expression,
        alias: item
            .alias
            .as_ref()
            .map_or_else(|| default_scalar_function_alias(function), variable_name),
    }))
}

pub(crate) fn validate_aggregate_scalar_target_correlated_subqueries(
    expression: &ScalarExpression,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    if scalar_expression_contains_correlated_subquery(expression) {
        return Err(unsupported(
            path,
            "aggregate expression targets do not support correlated COUNT { ... } or EXISTS { MATCH ... } subqueries",
        ));
    }
    Ok(())
}

pub(crate) fn default_scalar_function_alias(function: &FunctionInvocation) -> String {
    if is_character_length_function(function) {
        return "size".to_string();
    }
    if is_contains_function(function) {
        return "contains".to_string();
    }
    if is_starts_with_function(function) {
        return "startsWith".to_string();
    }
    if is_ends_with_function(function) {
        return "endsWith".to_string();
    }
    qualified_function_name(function)
}

pub(crate) enum ProjectionLiteral {
    Scalar(Literal),
    List(Vec<Literal>),
}

pub(crate) fn compile_projection_literal(
    expression: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ProjectionLiteral, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_projection_literal(inner, path, context),
        Expression::Literal(CypherLiteral::List(list)) => {
            let literals = list
                .elements
                .iter()
                .enumerate()
                .map(|(index, expression)| {
                    compile_literal(expression, format!("{path}[{index}]"), context)
                })
                .collect::<Result<Vec<_>, _>>()?;
            validate_literal_list_projection(&literals, path)?;
            Ok(ProjectionLiteral::List(literals))
        }
        Expression::ListSlice { .. } => {
            let literals = compile_literal_list(expression, path.clone(), context)?;
            validate_literal_list_projection(&literals, path)?;
            Ok(ProjectionLiteral::List(literals))
        }
        Expression::Parameter(parameter) => {
            match context.parameter_value(parameter, path.clone())? {
                CypherParameterValue::Literal(value) => {
                    Ok(ProjectionLiteral::Scalar(value.clone()))
                }
                CypherParameterValue::List(values) => {
                    validate_literal_list_projection(values, path)?;
                    Ok(ProjectionLiteral::List(values.clone()))
                }
            }
        }
        _ => compile_literal(expression, path, context).map(ProjectionLiteral::Scalar),
    }
}

pub(crate) fn compile_id_projection(
    function: &FunctionInvocation,
    item: &ProjectionItem,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    let alias = item
        .alias
        .as_ref()
        .map_or_else(|| "id".to_string(), variable_name);
    if let Some(value) = compile_optional_same_label_undirected_endpoint_function_argument(
        function,
        format!("{path}.expression.arguments"),
        plan,
        context,
    )? {
        return Ok(Projection::Expression {
            expression: same_label_undirected_endpoint_key_scalar_expression(value),
            alias,
        });
    }
    let value = compile_id_graph_value_ref(function, format!("{path}.expression"), plan, context)?;
    Ok(match value.presence_variable {
        Some(_) => Projection::Expression {
            expression: graph_value_key_scalar_expression(value),
            alias,
        },
        None => Projection::Key {
            variable: value.variable,
            alias,
        },
    })
}

pub(crate) fn compile_element_id_projection(
    function: &FunctionInvocation,
    item: &ProjectionItem,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    let alias = item
        .alias
        .as_ref()
        .map_or_else(|| "elementId".to_string(), variable_name);
    if let Some(value) = compile_optional_same_label_undirected_endpoint_function_argument(
        function,
        format!("{path}.expression.arguments"),
        plan,
        context,
    )? {
        return Ok(Projection::Expression {
            expression: same_label_undirected_endpoint_element_id_scalar_expression(value),
            alias,
        });
    }
    let value =
        compile_element_id_graph_value_ref(function, format!("{path}.expression"), plan, context)?;
    Ok(match value.presence_variable {
        Some(_) => Projection::Expression {
            expression: graph_value_element_id_scalar_expression(value),
            alias,
        },
        None => Projection::ElementId {
            variable: value.variable,
            alias,
        },
    })
}

pub(crate) fn compile_internal_graph_identity_projection(
    function: &FunctionInvocation,
    item: &ProjectionItem,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    let variable = compile_single_variable_function_argument(
        function,
        format!("{path}.expression.arguments"),
        "internal graph identity requires exactly one graph variable argument",
        context,
    )?;
    if !plan_uses_variable(plan, &variable) {
        return Err(unsupported(
            format!("{path}.expression.arguments[0]"),
            format!("internal graph identity argument '{variable}' is not a bound graph variable"),
        ));
    }
    Ok(Projection::Expression {
        expression: ScalarExpression::GraphIdentity { variable },
        alias: item
            .alias
            .as_ref()
            .map_or_else(|| "graphIdentity".to_string(), variable_name),
    })
}

pub(crate) fn compile_internal_graph_presence_projection(
    function: &FunctionInvocation,
    item: &ProjectionItem,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    let variable = compile_single_variable_function_argument(
        function,
        format!("{path}.expression.arguments"),
        "internal graph presence requires exactly one graph variable argument",
        context,
    )?;
    if !plan_uses_variable(plan, &variable) {
        return Err(unsupported(
            format!("{path}.expression.arguments[0]"),
            format!("internal graph presence argument '{variable}' is not a bound graph variable"),
        ));
    }
    Ok(Projection::Expression {
        expression: ScalarExpression::GraphPresence { variable },
        alias: item
            .alias
            .as_ref()
            .map_or_else(|| "graphPresence".to_string(), variable_name),
    })
}

pub(crate) fn compile_type_projection(
    function: &FunctionInvocation,
    item: &ProjectionItem,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    let variable = compile_single_variable_function_argument(
        function,
        format!("{path}.expression.arguments"),
        "type() supports exactly one relationship variable argument",
        context,
    )?;
    let relationship = plan
        .relationships
        .iter()
        .find(|relationship| relationship.variable.as_deref() == Some(variable.as_str()))
        .ok_or_else(|| {
            unsupported(
                format!("{path}.expression.arguments[0]"),
                format!("type() argument '{variable}' is not a named relationship variable"),
            )
        })?;
    Ok(Projection::RelationshipType {
        variable,
        relationship_type: relationship.relationship_type.clone(),
        alias: item
            .alias
            .as_ref()
            .map_or_else(|| "type".to_string(), variable_name),
    })
}

pub(crate) fn compile_labels_projection(
    function: &FunctionInvocation,
    item: &ProjectionItem,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    let alias = item
        .alias
        .as_ref()
        .map_or_else(|| "labels".to_string(), variable_name);
    if has_single_literal_null_argument(function) {
        return Ok(Projection::Expression {
            expression: ScalarExpression::Literal(Literal::Null),
            alias,
        });
    }
    if let Some(value) = compile_optional_same_label_undirected_endpoint_function_argument(
        function,
        format!("{path}.expression.arguments"),
        plan,
        context,
    )? {
        return Ok(Projection::Expression {
            expression: same_label_undirected_endpoint_labels_scalar_expression(value),
            alias,
        });
    }
    let (value, label) = compile_node_function_target_ref(
        function,
        format!("{path}.expression.arguments"),
        "labels() supports exactly one node variable argument",
        plan,
        context,
    )?;
    if value.presence_variable.is_some() {
        return Ok(Projection::Expression {
            expression: graph_value_labels_scalar_expression(value, label),
            alias,
        });
    }
    Ok(Projection::NodeLabels {
        variable: value.variable,
        label,
        alias,
    })
}

pub(crate) fn compile_labels_order_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<OrderExpression, CoreError> {
    let path = path.into();
    if has_single_literal_null_argument(function) {
        return Ok(OrderExpression::Literal(Literal::Null));
    }
    if let Some(value) = compile_optional_same_label_undirected_endpoint_function_argument(
        function,
        format!("{path}.arguments"),
        plan,
        context,
    )? {
        return Ok(OrderExpression::Scalar(
            same_label_undirected_endpoint_labels_scalar_expression(value),
        ));
    }
    let (value, label) = compile_node_function_target_ref(
        function,
        format!("{path}.arguments"),
        "labels() supports exactly one node variable argument",
        plan,
        context,
    )?;
    if value.presence_variable.is_some() {
        return Ok(OrderExpression::Scalar(
            graph_value_labels_scalar_expression(value, label),
        ));
    }
    Ok(OrderExpression::NodeLabels {
        variable: value.variable,
        label,
    })
}

pub(crate) fn has_single_literal_null_argument(function: &FunctionInvocation) -> bool {
    matches!(function.arguments.as_slice(), [argument] if is_literal_null_expression(argument))
}

pub(crate) fn is_literal_null_expression(expression: &Expression) -> bool {
    match expression {
        Expression::Parenthesized(inner) => is_literal_null_expression(inner),
        Expression::Literal(CypherLiteral::Null) => true,
        _ => false,
    }
}

pub(crate) fn compile_keys_order_expression(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<OrderExpression, CoreError> {
    let path = path.into();
    if let Some(value) = compile_optional_same_label_undirected_endpoint_function_argument(
        function,
        format!("{path}.arguments"),
        plan,
        context,
    )? {
        return Ok(OrderExpression::Scalar(
            same_label_undirected_endpoint_keys_scalar_expression(value),
        ));
    }
    let value = compile_single_graph_value_function_argument_ref(
        function,
        format!("{path}.arguments"),
        "keys() supports exactly one graph variable argument",
        plan,
        context,
    )?;
    if !plan_uses_variable(plan, &value.variable) {
        return Err(unsupported(
            format!("{path}.arguments[0]"),
            format!(
                "keys() argument '{}' is not a bound graph variable",
                value.variable
            ),
        ));
    }
    if value.presence_variable.is_some() {
        return Ok(OrderExpression::Scalar(graph_value_keys_scalar_expression(
            value,
        )));
    }
    Ok(OrderExpression::PropertyKeys {
        variable: value.variable,
    })
}

pub(crate) fn compile_arithmetic_order_expression(
    expression: &Expression,
    path: impl Into<String>,
    projections: &[Projection],
    plan: &GraphPlan,
    path_state: Option<&CypherCompileState>,
    context: &CypherCompileContext,
) -> Result<OrderExpression, CoreError> {
    let path = path.into();
    compile_scalar_order_expression(
        compile_scalar_expression_with_path_state(
            expression,
            path.clone(),
            plan,
            path_state,
            context,
        )?,
        projections,
        path,
    )
}

pub(crate) fn compile_binary_order_expression(
    expression: &Expression,
    path: impl Into<String>,
    projections: &[Projection],
    plan: &GraphPlan,
    state: &CypherCompileState,
    context: &CypherCompileContext,
) -> Result<OrderExpression, CoreError> {
    let path = path.into();
    if let Some(expression) = compile_optional_static_list_scalar_expression(
        expression,
        path.clone(),
        Some(plan),
        context,
    )? {
        return compile_scalar_order_expression(expression, projections, path);
    }
    if let Some(expression) =
        compile_optional_boolean_scalar_expression(expression, path.clone(), plan, context)?
    {
        return compile_scalar_order_expression(expression, projections, path);
    }
    compile_path_aware_arithmetic_order_expression(
        expression,
        path,
        projections,
        plan,
        state,
        context,
    )
}

pub(crate) fn compile_path_aware_arithmetic_order_expression(
    expression: &Expression,
    path: impl Into<String>,
    projections: &[Projection],
    plan: &GraphPlan,
    state: &CypherCompileState,
    context: &CypherCompileContext,
) -> Result<OrderExpression, CoreError> {
    compile_arithmetic_order_expression(expression, path, projections, plan, Some(state), context)
}

pub(crate) fn compile_case_order_expression(
    case: &CaseExpression,
    path: impl Into<String>,
    projections: &[Projection],
    plan: &GraphPlan,
    state: &CypherCompileState,
    context: &CypherCompileContext,
) -> Result<OrderExpression, CoreError> {
    let path = path.into();
    compile_scalar_order_expression(
        compile_case_scalar_expression_with_path_state(
            case,
            path.clone(),
            plan,
            Some(state),
            context,
        )?,
        projections,
        path,
    )
}

pub(crate) fn compile_keys_projection(
    function: &FunctionInvocation,
    item: &ProjectionItem,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    if let Some(value) = compile_optional_same_label_undirected_endpoint_function_argument(
        function,
        format!("{path}.expression.arguments"),
        plan,
        context,
    )? {
        return Ok(Projection::Expression {
            expression: same_label_undirected_endpoint_keys_scalar_expression(value),
            alias: item
                .alias
                .as_ref()
                .map_or_else(|| "keys".to_string(), variable_name),
        });
    }
    let value = compile_single_graph_value_function_argument_ref(
        function,
        format!("{path}.expression.arguments"),
        "keys() supports exactly one graph variable argument",
        plan,
        context,
    )?;
    if !plan_uses_variable(plan, &value.variable) {
        return Err(unsupported(
            format!("{path}.expression.arguments[0]"),
            format!(
                "keys() argument '{}' is not a bound graph variable",
                value.variable
            ),
        ));
    }
    if value.presence_variable.is_some() {
        return Ok(Projection::Expression {
            expression: graph_value_keys_scalar_expression(value),
            alias: item
                .alias
                .as_ref()
                .map_or_else(|| "keys".to_string(), variable_name),
        });
    }
    Ok(Projection::PropertyKeys {
        variable: value.variable,
        alias: item
            .alias
            .as_ref()
            .map_or_else(|| "keys".to_string(), variable_name),
    })
}

pub(crate) fn compile_aggregate_projection(
    function: &FunctionInvocation,
    item: &ProjectionItem,
    path: impl Into<String>,
    plan: &GraphPlan,
    state: &CypherCompileState,
    context: &CypherCompileContext,
) -> Result<Projection, CoreError> {
    let path = path.into();
    let function_kind =
        compile_aggregate_function(function, &format!("{path}.expression"), context)?.ok_or_else(
            || {
                unsupported(
                    format!("{path}.expression"),
                    format!(
                        "RETURN function '{}' is not supported yet",
                        qualified_function_name(function)
                    ),
                )
            },
        )?;
    let target = compile_function_aggregate_target(
        function,
        function_kind,
        &path,
        Some(plan),
        Some(state),
        context,
    )?;
    Ok(Projection::Aggregate {
        function: function_kind,
        target,
        distinct: function.distinct,
        alias: item.alias.as_ref().map_or_else(
            || {
                context
                    .function_source_text(function)
                    .unwrap_or_else(|| aggregate_function_name(function_kind).to_string())
            },
            variable_name,
        ),
    })
}

#[derive(Debug)]
pub(crate) struct OrderNullPlacementNormalization<'a> {
    pub(crate) cypher: Cow<'a, str>,
    pub(crate) placements: Vec<Option<NullOrder>>,
}

pub(crate) fn compile_optional_scalar_alias_aggregate_target(
    alias: &str,
    path: impl Into<String>,
    state: Option<&CypherCompileState>,
) -> Result<Option<AggregateTarget>, CoreError> {
    let path = path.into();
    let Some(state) = state else {
        return Ok(None);
    };
    let Some(projection) = scalar_alias_projection(state, alias) else {
        return Ok(None);
    };
    let expression = scalar_alias_projection_expression(projection, path.clone())?;
    validate_aggregate_scalar_target_correlated_subqueries(&expression, path)?;
    Ok(Some(AggregateTarget::Expression(expression)))
}

pub(crate) fn compile_aggregate_target(
    expression: &Expression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    state: Option<&CypherCompileState>,
    context: &CypherCompileContext,
) -> Result<AggregateTarget, CoreError> {
    let path = path.into();
    if let Some(expression) =
        compile_optional_scalar_alias_expression(expression, path.clone(), state)?
    {
        validate_aggregate_scalar_target_correlated_subqueries(&expression, path)?;
        return Ok(AggregateTarget::Expression(expression));
    }
    match expression {
        Expression::Parenthesized(inner) => {
            compile_aggregate_target(inner, path, plan, state, context)
        }
        Expression::Variable(variable) => Ok(AggregateTarget::VariableKey {
            variable: variable_name(variable),
        }),
        Expression::FunctionCall(function)
            if is_start_node_function(function) || is_end_node_function(function) =>
        {
            let Some(plan) = plan else {
                return Err(unsupported(
                    path,
                    "relationship endpoint aggregate targets require graph context",
                ));
            };
            if let Some(value) = compile_optional_same_label_undirected_relationship_endpoint(
                expression,
                path.clone(),
                plan,
                context,
            )? {
                return Ok(AggregateTarget::Expression(
                    same_label_undirected_endpoint_key_scalar_expression(value),
                ));
            }
            let value = compile_relationship_endpoint_ref(function, path, plan, context)?;
            Ok(match value.presence_variable {
                Some(presence_variable) => AggregateTarget::PresenceGatedVariableKey {
                    variable: value.variable,
                    presence_variable,
                },
                None => AggregateTarget::VariableKey {
                    variable: value.variable,
                },
            })
        }
        Expression::PropertyLookup { .. } => {
            if let Some(target) = compile_optional_static_map_lookup_aggregate_target(
                expression,
                path.clone(),
                plan,
                context,
            )? {
                return Ok(target);
            }
            if let Some(plan) = plan
                && let Some((expression, _)) =
                    compile_optional_same_label_undirected_endpoint_property_scalar_expression(
                        expression,
                        path.clone(),
                        plan,
                        context,
                    )?
            {
                return Ok(AggregateTarget::Expression(expression));
            }
            if let Some(plan) = plan
                && let Some((property, presence_variable, _)) =
                    compile_optional_endpoint_property_ref(expression, path.clone(), plan, context)?
            {
                return Ok(AggregateTarget::PresenceGatedProperty {
                    property,
                    presence_variable,
                });
            }
            Ok(AggregateTarget::Property(compile_property_ref(
                expression, path, plan, context,
            )?))
        }
        _ => {
            let Some(plan) = plan else {
                return Ok(AggregateTarget::Property(compile_property_ref(
                    expression, path, plan, context,
                )?));
            };
            let expression = compile_aggregate_scalar_target_expression(
                expression,
                path.clone(),
                plan,
                state,
                context,
            )?;
            validate_aggregate_scalar_target_correlated_subqueries(&expression, path)?;
            Ok(AggregateTarget::Expression(expression))
        }
    }
}

pub(crate) fn compile_optional_static_map_lookup_aggregate_target(
    expression: &Expression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<AggregateTarget>, CoreError> {
    let path = path.into();
    let Some(expression) = compile_optional_static_map_lookup_scalar_expression(
        expression,
        path.clone(),
        PredicateCompileMode::CaseWhen { plan },
        context,
    )?
    else {
        return Ok(None);
    };
    validate_aggregate_scalar_target_correlated_subqueries(&expression, path)?;
    Ok(Some(match expression {
        ScalarExpression::Property(property) => AggregateTarget::Property(property),
        expression => AggregateTarget::Expression(expression),
    }))
}

pub(crate) fn compile_aggregate_scalar_target_expression(
    expression: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    state: Option<&CypherCompileState>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    if is_boolean_scalar_expression(expression) {
        return compile_predicate_expression_with_path_state(
            expression, path, plan, state, context,
        )
        .map(|predicate| ScalarExpression::Predicate(Box::new(predicate)));
    }
    compile_scalar_expression_with_path_state(expression, path, plan, state, context)
}

pub(crate) fn compile_aggregate_function(
    function: &FunctionInvocation,
    path: &str,
    context: &CypherCompileContext,
) -> Result<Option<AggregateFunction>, CoreError> {
    let [name] = function.name.as_slice() else {
        return Ok(None);
    };
    if name.name.eq_ignore_ascii_case("count") {
        Ok(Some(AggregateFunction::Count))
    } else if name.name.eq_ignore_ascii_case("collect")
        || name.name.eq_ignore_ascii_case("collect_list")
    {
        Ok(Some(AggregateFunction::Collect))
    } else if name.name.eq_ignore_ascii_case("sum") {
        Ok(Some(AggregateFunction::Sum))
    } else if name.name.eq_ignore_ascii_case("avg") {
        Ok(Some(AggregateFunction::Avg))
    } else if name.name.eq_ignore_ascii_case("median") {
        Ok(Some(AggregateFunction::Median))
    } else if name.name.eq_ignore_ascii_case("percentileCont")
        || name.name.eq_ignore_ascii_case("percentile_cont")
    {
        if function.distinct {
            return Err(unsupported(
                format!("{path}.distinct"),
                "percentileCont(DISTINCT ...) is not supported because DataFusion 53 cannot execute distinct percentile_cont aggregates",
            ));
        }
        Ok(Some(AggregateFunction::PercentileCont {
            percentile: compile_percentile_argument(function, path, context, "percentileCont")?,
        }))
    } else if name.name.eq_ignore_ascii_case("percentileDisc")
        || name.name.eq_ignore_ascii_case("percentile_disc")
    {
        if function.distinct {
            return Err(unsupported(
                format!("{path}.distinct"),
                "percentileDisc(DISTINCT ...) is not supported because DataFusion 53 cannot execute distinct percentile_disc aggregates",
            ));
        }
        Ok(Some(AggregateFunction::PercentileDisc {
            percentile: compile_percentile_argument(function, path, context, "percentileDisc")?,
        }))
    } else if name.name.eq_ignore_ascii_case("stDev")
        || name.name.eq_ignore_ascii_case("stdev_samp")
    {
        Ok(Some(AggregateFunction::StdDev))
    } else if name.name.eq_ignore_ascii_case("stDevP")
        || name.name.eq_ignore_ascii_case("stdev_pop")
    {
        Ok(Some(AggregateFunction::StdDevP))
    } else if name.name.eq_ignore_ascii_case("min") {
        Ok(Some(AggregateFunction::Min))
    } else if name.name.eq_ignore_ascii_case("max") {
        Ok(Some(AggregateFunction::Max))
    } else {
        Ok(None)
    }
}

pub(crate) fn compile_percentile_argument(
    function: &FunctionInvocation,
    path: &str,
    context: &CypherCompileContext,
    function_name: &str,
) -> Result<OrderedFloat<f64>, CoreError> {
    let [_, percentile] = function.arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            format!("{function_name}() supports exactly two arguments: value and percentile"),
        ));
    };
    let literal = compile_literal(percentile, format!("{path}.arguments[1]"), context)?;
    let value = match literal {
        Literal::Integer(0) => 0.0,
        Literal::Integer(1) => 1.0,
        Literal::Integer(_) => {
            return Err(unsupported(
                format!("{path}.arguments[1]"),
                format!("{function_name}() percentile must be between 0.0 and 1.0 inclusive"),
            ));
        }
        Literal::Float(value) => value.into_inner(),
        _ => {
            return Err(unsupported(
                format!("{path}.arguments[1]"),
                format!("{function_name}() requires a numeric percentile literal"),
            ));
        }
    };
    if !value.is_finite() || !(0.0..=1.0).contains(&value) {
        return Err(unsupported(
            format!("{path}.arguments[1]"),
            format!("{function_name}() percentile must be between 0.0 and 1.0 inclusive"),
        ));
    }
    Ok(OrderedFloat(value))
}

pub(crate) fn qualified_function_name(function: &FunctionInvocation) -> String {
    function
        .name
        .iter()
        .map(|part| part.name.as_str())
        .collect::<Vec<_>>()
        .join(".")
}

pub(crate) fn compile_projection_predicate_expression(
    expression: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ProjectionPredicateExpression, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_projection_predicate_expression(inner, path, context)
        }
        Expression::BinaryOp {
            op: CypherBinaryOperator::Xor,
            lhs,
            rhs,
            ..
        }
        | Expression::BinaryOp {
            op: CypherBinaryOperator::And | CypherBinaryOperator::Or,
            lhs,
            rhs,
            ..
        } => compile_binary_projection_predicate_expression(expression, lhs, rhs, &path, context),
        Expression::UnaryOp {
            op: UnaryOperator::Not,
            operand,
            ..
        } => Ok(ProjectionPredicateExpression::Not {
            expression: Box::new(compile_projection_predicate_expression(
                operand,
                format!("{path}.operand"),
                context,
            )?),
        }),
        Expression::Comparison { lhs, operators, .. } => {
            compile_projection_comparison_expression(lhs, operators.as_slice(), path, context)
        }
        Expression::In { lhs, rhs, .. } => Ok(ProjectionPredicateExpression::Comparison(
            compile_projection_in_predicate(lhs, rhs, path, context)?,
        )),
        Expression::Literal(CypherLiteral::Boolean(value)) => {
            Ok(ProjectionPredicateExpression::Boolean(*value))
        }
        Expression::IsNull {
            operand, negated, ..
        } => Ok(ProjectionPredicateExpression::Comparison(
            ProjectionPredicate {
                alias: compile_projection_alias_ref(operand, format!("{path}.operand"))?,
                operator: if *negated {
                    ComparisonOperator::NotEqual
                } else {
                    ComparisonOperator::Equal
                },
                rhs: ProjectionPredicateRhs::Literal(Literal::Null),
            },
        )),
        Expression::Variable(variable) => Ok(ProjectionPredicateExpression::Comparison(
            ProjectionPredicate {
                alias: variable_name(variable),
                operator: ComparisonOperator::Equal,
                rhs: ProjectionPredicateRhs::Literal(Literal::Boolean(true)),
            },
        )),
        _ => Err(unsupported(
            path,
            "WITH WHERE only supports projected alias comparisons combined with AND, OR, XOR, and NOT",
        )),
    }
}

pub(crate) fn compile_binary_projection_predicate_expression(
    expression: &Expression,
    lhs: &Expression,
    rhs: &Expression,
    path: &str,
    context: &CypherCompileContext,
) -> Result<ProjectionPredicateExpression, CoreError> {
    let Expression::BinaryOp { op, .. } = expression else {
        unreachable!("binary projection predicate helper called with non-binary expression");
    };
    let left = Box::new(compile_projection_predicate_expression(
        lhs,
        format!("{path}.lhs"),
        context,
    )?);
    let right = Box::new(compile_projection_predicate_expression(
        rhs,
        format!("{path}.rhs"),
        context,
    )?);
    match op {
        CypherBinaryOperator::And => Ok(ProjectionPredicateExpression::And { left, right }),
        CypherBinaryOperator::Or => Ok(ProjectionPredicateExpression::Or { left, right }),
        CypherBinaryOperator::Xor => Ok(ProjectionPredicateExpression::Xor { left, right }),
        _ => unreachable!("non-boolean operator reached binary projection predicate helper"),
    }
}

pub(crate) fn compile_projection_comparison_expression(
    lhs: &Expression,
    operators: &[(CypherComparisonOperator, Box<Expression>)],
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ProjectionPredicateExpression, CoreError> {
    let path = path.into();
    if operators.is_empty() {
        return Err(unsupported(path, "comparison must include an operator"));
    }

    let (prefix, mut current_lhs) =
        compile_projection_comparison_prefix(lhs, format!("{path}.lhs"), context)?;
    let mut expression = prefix;
    for (index, (operator, rhs)) in operators.iter().enumerate() {
        let predicate = compile_binary_projection_comparison(
            current_lhs,
            *operator,
            rhs,
            format!("{path}.operators[{index}]"),
            context,
        )?;
        let next = ProjectionPredicateExpression::Comparison(predicate);
        expression = Some(append_projection_expression_conjunct(expression, next));
        current_lhs = rhs;
    }

    expression.ok_or_else(|| CoreError::internal("projection comparison expression was empty"))
}

pub(crate) fn compile_projection_comparison_prefix<'a>(
    expression: &'a Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<(Option<ProjectionPredicateExpression>, &'a Expression), CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_projection_comparison_prefix(inner, path, context)
        }
        Expression::Comparison { lhs, operators, .. } => Ok((
            Some(compile_projection_comparison_expression(
                lhs,
                operators.as_slice(),
                path,
                context,
            )?),
            terminal_comparison_operand(lhs, operators.as_slice()),
        )),
        _ => Ok((None, expression)),
    }
}

pub(crate) fn append_projection_expression_conjunct(
    expression: Option<ProjectionPredicateExpression>,
    next: ProjectionPredicateExpression,
) -> ProjectionPredicateExpression {
    match expression {
        Some(previous) => ProjectionPredicateExpression::And {
            left: Box::new(previous),
            right: Box::new(next),
        },
        None => next,
    }
}

pub(crate) fn compile_binary_projection_comparison(
    lhs: &Expression,
    operator: CypherComparisonOperator,
    rhs: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ProjectionPredicate, CoreError> {
    let path = path.into();
    let operator = compile_comparison_operator(operator);
    if let Some(alias) = compile_optional_projection_alias_ref(lhs) {
        return Ok(ProjectionPredicate {
            alias,
            operator,
            rhs: compile_projection_predicate_rhs(rhs, format!("{path}.rhs"), context)?,
        });
    }
    if let Some(alias) = compile_optional_projection_alias_ref(rhs) {
        return Ok(ProjectionPredicate {
            alias,
            operator: invert_comparison_operator(operator, format!("{path}.operator"))?,
            rhs: ProjectionPredicateRhs::Literal(compile_literal(
                lhs,
                format!("{path}.lhs"),
                context,
            )?),
        });
    }

    Err(unsupported(
        path,
        "WITH WHERE comparisons must include at least one projected alias operand",
    ))
}

pub(crate) fn compile_projection_in_predicate(
    lhs: &Expression,
    rhs: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ProjectionPredicate, CoreError> {
    let path = path.into();
    Ok(ProjectionPredicate {
        alias: compile_projection_alias_ref(lhs, format!("{path}.lhs"))?,
        operator: ComparisonOperator::In,
        rhs: ProjectionPredicateRhs::List(compile_static_list_in_rhs_literals(
            rhs,
            format!("{path}.rhs"),
            None,
            context,
        )?),
    })
}

pub(crate) fn compile_projection_predicate_rhs(
    expression: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ProjectionPredicateRhs, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_projection_predicate_rhs(inner, path, context),
        Expression::Variable(variable) => {
            Ok(ProjectionPredicateRhs::Alias(variable_name(variable)))
        }
        _ => Ok(ProjectionPredicateRhs::Literal(compile_literal(
            expression, path, context,
        )?)),
    }
}

pub(crate) fn compile_projection_alias_ref(
    expression: &Expression,
    path: impl Into<String>,
) -> Result<String, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_projection_alias_ref(inner, path),
        Expression::Variable(variable) => Ok(variable_name(variable)),
        _ => Err(unsupported(
            path,
            "only projected alias expressions are supported here",
        )),
    }
}

pub(crate) fn compile_optional_projection_alias_ref(expression: &Expression) -> Option<String> {
    match expression {
        Expression::Parenthesized(inner) => compile_optional_projection_alias_ref(inner),
        Expression::Variable(variable) => Some(variable_name(variable)),
        _ => None,
    }
}

pub(crate) fn single_return_expression<'a>(
    query: &'a Query,
    path: &str,
) -> Result<&'a Expression, CoreError> {
    let [
        QueryBody::SingleQuery(SingleQuery {
            kind: SingleQueryKind::SinglePart(single),
        }),
    ] = query.statements.as_slice()
    else {
        return Err(CoreError::internal(format!(
            "expression fragment at {path} did not parse as a single RETURN query"
        )));
    };
    let SinglePartBody::Return(return_clause) = &single.body else {
        return Err(CoreError::internal(format!(
            "expression fragment at {path} did not produce a RETURN clause"
        )));
    };
    let [item] = return_clause.items.as_slice() else {
        return Err(CoreError::internal(format!(
            "expression fragment at {path} did not produce exactly one RETURN item"
        )));
    };
    Ok(&item.expression)
}

pub(crate) fn static_map_lookup_output_name(
    expression: &Expression,
    context: &CypherCompileContext,
) -> Option<String> {
    match expression {
        Expression::Parenthesized(inner) => static_map_lookup_output_name(inner, context),
        Expression::PropertyLookup { base, property, .. }
            if literal_map_expression(base).is_some() =>
        {
            Some(property.name.name.clone())
        }
        Expression::ListIndex { list, index, .. } if literal_map_expression(list).is_some() => {
            compile_property_index_name(index, "projection.alias", context).ok()
        }
        _ => None,
    }
}

pub(crate) fn validate_literal_list_projection(
    literals: &[Literal],
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    if literals.is_empty() {
        return Err(unsupported(
            path,
            "literal list projections require at least one element",
        ));
    }

    let mut expected = None;
    for literal in literals {
        let Some(kind) = literal_list_element_kind(literal) else {
            continue;
        };
        match expected {
            Some(expected) if expected != kind => {
                return Err(unsupported(
                    path,
                    "literal list projections require all non-null elements to have the same type",
                ));
            }
            Some(_) => {}
            None => expected = Some(kind),
        }
    }

    if expected.is_none() {
        return Err(unsupported(
            path,
            "literal list projections require at least one non-null element",
        ));
    }

    Ok(())
}

pub(crate) fn literal_list_element_kind(literal: &Literal) -> Option<LiteralListElementType> {
    match literal {
        Literal::String(_) => Some(LiteralListElementType::String),
        Literal::Integer(_) => Some(LiteralListElementType::Integer),
        Literal::Float(_) => Some(LiteralListElementType::Float),
        Literal::Boolean(_) => Some(LiteralListElementType::Boolean),
        Literal::Null => None,
        Literal::List(values) => {
            infer_scalar_literal_list_element_type(values).and_then(LiteralListElementType::list_of)
        }
    }
}

pub(crate) fn infer_scalar_literal_list_element_type(
    literals: &[Literal],
) -> Option<LiteralListElementType> {
    let mut expected = None;
    for literal in literals {
        let kind = match literal {
            Literal::String(_) => Some(LiteralListElementType::String),
            Literal::Integer(_) => Some(LiteralListElementType::Integer),
            Literal::Float(_) => Some(LiteralListElementType::Float),
            Literal::Boolean(_) => Some(LiteralListElementType::Boolean),
            Literal::Null => None,
            Literal::List(_) => return None,
        };
        let Some(kind) = kind else {
            continue;
        };
        match expected {
            Some(expected) if expected != kind => return None,
            Some(_) => {}
            None => expected = Some(kind),
        }
    }
    expected
}

pub(crate) fn is_literal_projection_expression(expression: &Expression) -> bool {
    match expression {
        Expression::Parenthesized(inner) => is_literal_projection_expression(inner),
        expression if is_static_map_lookup_expression(expression) => true,
        Expression::ListIndex { .. }
        | Expression::ListSlice { .. }
        | Expression::Literal(_)
        | Expression::Parameter(_) => true,
        Expression::UnaryOp {
            op: UnaryOperator::Negate,
            operand,
            ..
        } => is_literal_expression(operand),
        _ => false,
    }
}

pub(crate) fn is_literal_expression(expression: &Expression) -> bool {
    match expression {
        Expression::Parenthesized(inner) => is_literal_expression(inner),
        Expression::Literal(CypherLiteral::List(_)) => false,
        expression if is_static_map_lookup_expression(expression) => true,
        Expression::ListIndex { .. } | Expression::Literal(_) | Expression::Parameter(_) => true,
        Expression::UnaryOp {
            op: UnaryOperator::Negate,
            operand,
            ..
        } => is_literal_expression(operand),
        _ => false,
    }
}

pub(crate) fn is_arithmetic_expression(expression: &Expression) -> bool {
    match expression {
        Expression::Parenthesized(inner) => is_arithmetic_expression(inner),
        Expression::BinaryOp { op, .. } => matches!(
            op,
            CypherBinaryOperator::Add
                | CypherBinaryOperator::Subtract
                | CypherBinaryOperator::Multiply
                | CypherBinaryOperator::Divide
                | CypherBinaryOperator::Modulo
                | CypherBinaryOperator::Power
        ),
        Expression::UnaryOp {
            op: UnaryOperator::Negate,
            ..
        } => true,
        _ => false,
    }
}

pub(crate) fn compile_limit(
    expression: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<u64, CoreError> {
    compile_non_negative_integer(expression, path, "LIMIT", context)
}

pub(crate) fn compile_skip(
    expression: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<u64, CoreError> {
    compile_non_negative_integer(expression, path, "SKIP", context)
}
