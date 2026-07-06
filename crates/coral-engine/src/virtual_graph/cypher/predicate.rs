//! Cypher WHERE predicate, comparison, boolean, and literal lowering helpers split
//! out of `cypher.rs` without changing behavior.

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "Predicate lowering helpers intentionally inherit parent-private Cypher compile context."
)]
use super::*;

pub(super) fn compile_predicate_expression(
    expression: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<PredicateExpression, CoreError> {
    compile_predicate_expression_with_path_state(expression, path, plan, None, context)
}

pub(super) fn compile_predicate_expression_with_path_state(
    expression: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    path_state: Option<&CypherCompileState>,
    context: &CypherCompileContext,
) -> Result<PredicateExpression, CoreError> {
    compile_predicate_expression_in_mode(
        expression,
        path,
        PredicateCompileMode::Graph { plan, path_state },
        context,
    )
}

pub(super) fn compile_predicate_expression_in_mode(
    expression: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<PredicateExpression, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_predicate_expression_in_mode(inner, path, mode, context)
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
        } => compile_binary_predicate_expression(expression, lhs, rhs, &path, mode, context),
        Expression::UnaryOp {
            op: UnaryOperator::Not,
            operand,
            ..
        } => Ok(PredicateExpression::Not {
            expression: Box::new(compile_predicate_expression_in_mode(
                operand,
                format!("{path}.operand"),
                mode,
                context,
            )?),
        }),
        Expression::Comparison { lhs, operators, .. } => {
            compile_comparison_expression(lhs, operators.as_slice(), path, mode, context)
        }
        Expression::In { lhs, rhs, .. } => compile_in_predicate(lhs, rhs, path, mode, context),
        Expression::NodeLabels { base, labels, .. } => match mode.static_metadata_plan() {
            Some(plan) => compile_graph_label_predicate(base, labels, path, plan, context),
            None => Err(unsupported(path, "label predicates require graph context")),
        },
        Expression::Literal(CypherLiteral::Boolean(value)) => {
            Ok(PredicateExpression::Boolean(*value))
        }
        Expression::IsNull {
            operand, negated, ..
        } => compile_null_predicate(operand, *negated, path, mode, context),
        Expression::FunctionCall(function) if is_exists_function(function) => {
            Ok(PredicateExpression::Comparison(compile_exists_predicate(
                function,
                path,
                mode.graph_plan(),
                context,
            )?))
        }
        Expression::Exists(exists) => {
            compile_exists_pattern_predicate(exists, path, mode.graph_plan(), context)
        }
        Expression::FunctionCall(function) if is_empty_function(function) => {
            Ok(PredicateExpression::ScalarComparison(
                compile_is_empty_predicate(function, path, mode, context)?,
            ))
        }
        Expression::FunctionCall(function) if is_string_predicate_function(function) => {
            Ok(PredicateExpression::ScalarComparison(
                compile_string_predicate_function_predicate(function, path, mode, context)?,
            ))
        }
        Expression::FunctionCall(function)
            if collection_quantifier_function(function).is_some() =>
        {
            compile_static_list_quantifier_function_predicate(function, path, mode, context)
        }
        Expression::All(_) | Expression::Any(_) | Expression::None(_) | Expression::Single(_) => {
            compile_static_list_quantifier_ast_predicate(expression, path, mode, context)
        }
        Expression::PropertyLookup { .. } => {
            Ok(PredicateExpression::Comparison(PropertyPredicate {
                property: compile_property_ref(expression, path, mode.graph_plan(), context)?,
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::Boolean(true)),
            }))
        }
        _ => Err(unsupported(path, mode.unsupported_predicate_message())),
    }
}

pub(super) fn compile_binary_predicate_expression(
    expression: &Expression,
    lhs: &Expression,
    rhs: &Expression,
    path: &str,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<PredicateExpression, CoreError> {
    let Expression::BinaryOp { op, .. } = expression else {
        unreachable!("binary predicate helper called with non-binary expression");
    };
    let left = Box::new(compile_predicate_expression_in_mode(
        lhs,
        format!("{path}.lhs"),
        mode,
        context,
    )?);
    let right = Box::new(compile_predicate_expression_in_mode(
        rhs,
        format!("{path}.rhs"),
        mode,
        context,
    )?);
    match op {
        CypherBinaryOperator::And => Ok(PredicateExpression::And { left, right }),
        CypherBinaryOperator::Or => Ok(PredicateExpression::Or { left, right }),
        CypherBinaryOperator::Xor => Ok(PredicateExpression::Xor { left, right }),
        _ => unreachable!("non-boolean operator reached binary predicate helper"),
    }
}

pub(super) fn compile_is_empty_predicate(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarPredicate, CoreError> {
    let path = path.into();
    let plan = mode.static_metadata_plan();
    let [argument] = function.arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            "isEmpty() supports exactly one scalar string argument",
        ));
    };
    if let Expression::Case(case) = argument
        && let Some(expression) = compile_optional_static_list_case_is_empty_scalar_expression(
            case,
            format!("{path}.arguments[0]"),
            mode,
            context,
        )?
    {
        return Ok(scalar_is_true_predicate(expression));
    }
    if let Some(expression) = compile_optional_static_list_coalesce_is_empty_scalar_expression(
        argument,
        format!("{path}.arguments[0]"),
        plan,
        context,
    )? {
        return Ok(scalar_is_true_predicate(expression));
    }
    if let Some(expression) = compile_optional_static_list_slice_is_empty_scalar_expression(
        argument,
        format!("{path}.arguments[0]"),
        mode,
        context,
    )? {
        return Ok(scalar_is_true_predicate(expression));
    }
    if let Some(plan) = plan
        && let Some(expression) = compile_is_empty_metadata_scalar_expression(
            argument,
            format!("{path}.arguments[0]"),
            plan,
            context,
        )?
    {
        return Ok(scalar_is_true_predicate(expression));
    }
    if let Some(length) = compile_literal_list_length_scalar_expression(
        argument,
        format!("{path}.arguments[0]"),
        context,
    )? {
        return Ok(ScalarPredicate {
            lhs: length,
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(0))),
        });
    }
    if let Some(length) = compile_static_list_function_length_scalar_expression(
        argument,
        format!("{path}.arguments[0]"),
        plan,
        context,
    )? {
        return Ok(ScalarPredicate {
            lhs: length,
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(0))),
        });
    }
    if let Some(length) = compile_optional_count_only_collection_size_scalar_expression(
        argument,
        format!("{path}.arguments[0]"),
        mode,
        context,
    )? {
        return Ok(ScalarPredicate {
            lhs: length,
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(0))),
        });
    }
    Ok(ScalarPredicate {
        lhs: ScalarExpression::CharacterLength {
            expression: Box::new(compile_scalar_expression_in_mode(
                argument,
                format!("{path}.arguments[0]"),
                plan,
                context,
            )?),
        },
        operator: ComparisonOperator::Equal,
        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(0))),
    })
}

pub(super) fn scalar_is_true_predicate(expression: ScalarExpression) -> ScalarPredicate {
    ScalarPredicate {
        lhs: expression,
        operator: ComparisonOperator::Equal,
        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Boolean(true))),
    }
}

pub(super) fn compile_string_predicate_function_predicate(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarPredicate, CoreError> {
    let path = path.into();
    let expression =
        compile_scalar_function_expression_in_mode(function, path.clone(), mode, context)?
            .ok_or_else(|| {
                CoreError::internal(format!(
                    "string predicate function at {path} did not compile to a scalar expression"
                ))
            })?;
    Ok(scalar_is_true_predicate(expression))
}

pub(super) fn compile_optional_static_list_slice_is_empty_scalar_expression(
    expression: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    let Expression::ListSlice {
        list, start, end, ..
    } = expression
    else {
        return Ok(None);
    };
    if let Some(expression) = compile_optional_static_list_case_slice_is_empty_scalar_expression(
        list,
        start.as_deref(),
        end.as_deref(),
        path.clone(),
        mode,
        context,
    )? {
        return Ok(Some(expression));
    }
    compile_optional_static_list_coalesce_slice_is_empty_scalar_expression(
        list,
        start.as_deref(),
        end.as_deref(),
        path,
        mode.static_metadata_plan(),
        context,
    )
}

pub(super) fn compile_optional_static_list_coalesce_is_empty_scalar_expression(
    expression: &Expression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_coalesce_is_empty_scalar_expression(
                inner, path, plan, context,
            )
        }
        Expression::FunctionCall(function) if is_coalesce_function(function) => {
            let Some(coalesce) =
                compile_optional_static_list_coalesce_arguments(function, path, plan, context)?
            else {
                return Ok(None);
            };
            Ok(Some(static_list_coalesce_is_empty_scalar_expression(
                coalesce,
            )))
        }
        _ => Ok(None),
    }
}

pub(super) fn compile_is_empty_metadata_scalar_expression(
    expression: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    let Some(value) = compile_optional_metadata_list_value(expression, path, plan, context)? else {
        return Ok(None);
    };
    Ok(Some(metadata_is_empty_scalar_expression(
        value.presence_variable,
        value.literals.is_empty(),
    )))
}

pub(super) fn metadata_is_empty_scalar_expression(
    presence_variable: Option<String>,
    is_empty: bool,
) -> ScalarExpression {
    presence_gate_scalar_expression(
        presence_variable,
        ScalarExpression::Literal(Literal::Boolean(is_empty)),
    )
}

pub(super) fn declared_graph_value_property_names(
    graph: &Declaration,
    plan: &GraphPlan,
    value: &GraphValueRef,
    path: &str,
) -> Result<Vec<String>, CoreError> {
    if let Some(node) = plan
        .nodes
        .iter()
        .find(|node| node.variable == value.variable)
    {
        let mapping = graph.node(&node.label).ok_or_else(|| {
            unsupported(
                path.to_string(),
                format!(
                    "keys() metadata expression could not resolve node label '{}'",
                    node.label
                ),
            )
        })?;
        return Ok(mapping.properties.keys().cloned().collect());
    }

    let relationship = plan
        .relationships
        .iter()
        .find(|relationship| relationship.variable.as_deref() == Some(value.variable.as_str()))
        .ok_or_else(|| {
            unsupported(
                path.to_string(),
                format!(
                    "keys() metadata expression argument '{}' is not a bound graph variable",
                    value.variable
                ),
            )
        })?;
    let node_labels_by_variable = plan
        .nodes
        .iter()
        .map(|node| (node.variable.as_str(), node.label.as_str()))
        .collect::<BTreeMap<_, _>>();
    let mapping =
        return_star_relationship_mapping(graph, relationship, &node_labels_by_variable, path)?;
    Ok(mapping.properties.keys().cloned().collect())
}

pub(super) fn append_predicate_expression(expression: PredicateExpression, plan: &mut GraphPlan) {
    if is_conjunctive_expression(&expression) {
        append_conjunctive_expression(expression, &mut plan.predicates);
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

pub(super) fn is_conjunctive_expression(expression: &PredicateExpression) -> bool {
    match expression {
        PredicateExpression::Comparison(_) => true,
        PredicateExpression::And { left, right } => {
            is_conjunctive_expression(left) && is_conjunctive_expression(right)
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

pub(super) fn append_conjunctive_expression(
    expression: PredicateExpression,
    predicates: &mut Vec<PropertyPredicate>,
) {
    match expression {
        PredicateExpression::Comparison(predicate) => predicates.push(predicate),
        PredicateExpression::And { left, right } => {
            append_conjunctive_expression(*left, predicates);
            append_conjunctive_expression(*right, predicates);
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
            unreachable!("non-conjunctive predicate expression reached conjunctive appender")
        }
    }
}

pub(super) fn compile_comparison_expression(
    lhs: &Expression,
    operators: &[(CypherComparisonOperator, Box<Expression>)],
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<PredicateExpression, CoreError> {
    let path = path.into();
    if operators.is_empty() {
        return Err(unsupported(path, "comparison must include an operator"));
    }

    let (prefix, mut current_lhs) =
        compile_comparison_prefix(lhs, format!("{path}.lhs"), mode, context)?;
    let mut expression = prefix;
    for (index, (operator, rhs)) in operators.iter().enumerate() {
        let next = compile_binary_comparison(
            current_lhs,
            *operator,
            rhs,
            format!("{path}.operators[{index}]"),
            mode,
            context,
        )?;
        expression = Some(append_expression_conjunct(expression, next));
        current_lhs = rhs;
    }

    expression.ok_or_else(|| CoreError::internal("comparison expression was empty"))
}

pub(super) fn compile_comparison_prefix<'a>(
    expression: &'a Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<(Option<PredicateExpression>, &'a Expression), CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_comparison_prefix(inner, path, mode, context),
        Expression::Comparison { lhs, operators, .. } => Ok((
            Some(compile_comparison_expression(
                lhs,
                operators.as_slice(),
                path,
                mode,
                context,
            )?),
            terminal_comparison_operand(lhs, operators.as_slice()),
        )),
        _ => Ok((None, expression)),
    }
}

pub(super) fn terminal_comparison_operand<'a>(
    lhs: &'a Expression,
    operators: &'a [(CypherComparisonOperator, Box<Expression>)],
) -> &'a Expression {
    operators.last().map_or(lhs, |(_, rhs)| rhs.as_ref())
}

pub(super) fn append_expression_conjunct(
    expression: Option<PredicateExpression>,
    next: PredicateExpression,
) -> PredicateExpression {
    match expression {
        Some(previous) => PredicateExpression::And {
            left: Box::new(previous),
            right: Box::new(next),
        },
        None => next,
    }
}

pub(super) fn compile_binary_comparison(
    lhs: &Expression,
    operator: CypherComparisonOperator,
    rhs: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<PredicateExpression, CoreError> {
    let path = path.into();
    let operator = compile_comparison_operator(operator);
    if let Some(plan) = mode.static_metadata_plan()
        && let Some(predicate) =
            compile_optional_static_list_comparison(lhs, operator, rhs, &path, plan, context)?
    {
        return Ok(predicate);
    }
    if let Some(property) =
        compile_optional_property_ref(lhs, format!("{path}.lhs"), mode.graph_plan(), context)?
    {
        return compile_left_property_comparison(property, operator, rhs, &path, mode, context);
    }
    if let Some(plan) = mode.graph_plan() {
        if let Some(variable) = compile_optional_id_ref(lhs, format!("{path}.lhs"), plan, context)?
        {
            return Ok(PredicateExpression::KeyComparison(KeyPredicate {
                variable,
                operator,
                rhs: compile_predicate_rhs(rhs, format!("{path}.rhs"), mode, context)?,
            }));
        }
        if let Some(variable) =
            compile_optional_element_id_ref(lhs, format!("{path}.lhs"), plan, context)?
        {
            return Ok(PredicateExpression::ElementIdComparison(
                ElementIdPredicate {
                    variable,
                    operator,
                    rhs: compile_predicate_rhs(rhs, format!("{path}.rhs"), mode, context)?,
                },
            ));
        }
    }
    if let Some(predicate) =
        compile_optional_static_literal_scalar_comparison(lhs, operator, rhs, &path, mode, context)?
    {
        return Ok(predicate);
    }
    if let Some(predicate) =
        compile_optional_scalar_binary_comparison(lhs, operator, rhs, &path, mode, context)?
    {
        return Ok(predicate);
    }
    if let Some(property) =
        compile_optional_property_ref(rhs, format!("{path}.rhs"), mode.graph_plan(), context)?
    {
        return Ok(PredicateExpression::Comparison(PropertyPredicate {
            property,
            operator: invert_comparison_operator(operator, format!("{path}.operator"))?,
            rhs: compile_literal_predicate_rhs(lhs, format!("{path}.lhs"), mode, context)?,
        }));
    }
    if let Some(plan) = mode.graph_plan() {
        if let Some(variable) = compile_optional_id_ref(rhs, format!("{path}.rhs"), plan, context)?
        {
            return Ok(PredicateExpression::KeyComparison(KeyPredicate {
                variable,
                operator: invert_comparison_operator(operator, format!("{path}.operator"))?,
                rhs: compile_literal_predicate_rhs(lhs, format!("{path}.lhs"), mode, context)?,
            }));
        }
        if let Some(variable) =
            compile_optional_element_id_ref(rhs, format!("{path}.rhs"), plan, context)?
        {
            return Ok(PredicateExpression::ElementIdComparison(
                ElementIdPredicate {
                    variable,
                    operator: invert_comparison_operator(operator, format!("{path}.operator"))?,
                    rhs: compile_literal_predicate_rhs(lhs, format!("{path}.lhs"), mode, context)?,
                },
            ));
        }
    }

    if let Some(plan) = mode.static_metadata_plan()
        && (contains_type_function(lhs) || contains_type_function(rhs))
    {
        let lhs = compile_predicate_literal(lhs, format!("{path}.lhs"), plan, context)?;
        let rhs = compile_predicate_literal(rhs, format!("{path}.rhs"), plan, context)?;
        return Ok(PredicateExpression::Boolean(evaluate_literal_comparison(
            &lhs, operator, &rhs, path,
        )?));
    }
    if is_literal_expression(lhs) && is_literal_expression(rhs) {
        let lhs = compile_literal(lhs, format!("{path}.lhs"), context)?;
        let rhs = compile_literal(rhs, format!("{path}.rhs"), context)?;
        return Ok(PredicateExpression::Boolean(
            evaluate_literal_only_comparison(&lhs, operator, &rhs, path)?,
        ));
    }

    Err(unsupported(path, mode.unsupported_comparison_message()))
}

pub(super) fn compile_optional_static_literal_scalar_comparison(
    lhs: &Expression,
    operator: ComparisonOperator,
    rhs: &Expression,
    path: &str,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<PredicateExpression>, CoreError> {
    let Some(lhs) =
        compile_optional_static_literal_scalar_operand(lhs, format!("{path}.lhs"), mode, context)?
    else {
        return Ok(None);
    };
    let Some(rhs) =
        compile_optional_static_literal_scalar_operand(rhs, format!("{path}.rhs"), mode, context)?
    else {
        return Ok(None);
    };
    Ok(Some(PredicateExpression::Boolean(
        evaluate_literal_only_comparison(&lhs, operator, &rhs, path)?,
    )))
}

pub(super) fn compile_optional_static_literal_scalar_operand(
    expression: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<Literal>, CoreError> {
    let path = path.into();
    if is_direct_static_literal_scalar_operand(expression) {
        return compile_literal(expression, path, context).map(Some);
    }
    if !is_static_literal_scalar_operand(expression, context) {
        return Ok(None);
    }

    let mut variables = BTreeSet::new();
    expression_variables(expression, &mut variables);
    if !variables.is_empty()
        || expression_contains_recovered_function_variable_argument(expression, context)
    {
        return Ok(None);
    }

    let item = Literal::Null;
    let evaluation = StaticFilterEvaluation {
        variable: "__coral_static_literal_operand",
        item: &item,
        accumulator_variable: None,
        accumulator: None,
        mode,
        context,
    };
    evaluate_static_map_expression(expression, evaluation, path).map(Some)
}

pub(super) fn is_static_literal_scalar_operand(
    expression: &Expression,
    context: &CypherCompileContext,
) -> bool {
    match expression {
        Expression::Parenthesized(inner) => is_static_literal_scalar_operand(inner, context),
        expression if is_direct_static_literal_scalar_operand(expression) => true,
        Expression::UnaryOp {
            op: UnaryOperator::Negate | UnaryOperator::Not,
            operand,
            ..
        }
        | Expression::IsNull { operand, .. } => is_static_literal_scalar_operand(operand, context),
        Expression::BinaryOp {
            op:
                CypherBinaryOperator::Add
                | CypherBinaryOperator::Subtract
                | CypherBinaryOperator::Multiply
                | CypherBinaryOperator::Divide
                | CypherBinaryOperator::Modulo
                | CypherBinaryOperator::Power
                | CypherBinaryOperator::And
                | CypherBinaryOperator::Or
                | CypherBinaryOperator::Xor,
            lhs,
            rhs,
            ..
        } => {
            is_static_literal_scalar_operand(lhs, context)
                && is_static_literal_scalar_operand(rhs, context)
        }
        Expression::Comparison { lhs, operators, .. } => {
            is_static_literal_scalar_operand(lhs, context)
                && operators
                    .iter()
                    .all(|(_, rhs)| is_static_literal_scalar_operand(rhs, context))
        }
        Expression::In { lhs, rhs, .. } => {
            is_static_literal_scalar_operand(lhs, context)
                && is_static_literal_list_operand(rhs, context)
        }
        Expression::FunctionCall(function) => {
            if context.variable_function_argument(function).is_some() {
                return false;
            }
            if is_character_length_function(function) {
                return function
                    .arguments
                    .iter()
                    .all(|argument| is_static_literal_character_length_operand(argument, context));
            }
            is_static_map_operand_function(function)
                && function.arguments.iter().all(|argument| {
                    is_static_literal_scalar_operand(argument, context)
                        || (is_empty_function(function)
                            && is_static_literal_list_operand(argument, context))
                })
        }
        _ => false,
    }
}

pub(super) fn is_static_literal_character_length_operand(
    expression: &Expression,
    context: &CypherCompileContext,
) -> bool {
    match expression {
        Expression::Parenthesized(inner) => {
            is_static_literal_character_length_operand(inner, context)
        }
        Expression::Parameter(parameter) => !matches!(
            context.parameters.get(parameter.name.name.as_str()),
            Some(CypherParameterValue::List(_))
        ),
        expression => is_static_literal_scalar_operand(expression, context),
    }
}

pub(super) fn is_direct_static_literal_scalar_operand(expression: &Expression) -> bool {
    match expression {
        Expression::Parenthesized(inner) => is_direct_static_literal_scalar_operand(inner),
        Expression::ListIndex { list, .. } => is_literal_list_source_expression(list),
        expression => is_literal_expression(expression),
    }
}

pub(super) fn is_static_literal_list_operand(
    expression: &Expression,
    context: &CypherCompileContext,
) -> bool {
    match expression {
        Expression::Parenthesized(inner) => is_static_literal_list_operand(inner, context),
        Expression::Literal(CypherLiteral::List(_)) | Expression::Parameter(_) => true,
        Expression::ListSlice {
            list, start, end, ..
        } => {
            is_static_literal_list_operand(list, context)
                && start
                    .as_deref()
                    .is_none_or(|start| is_static_literal_scalar_operand(start, context))
                && end
                    .as_deref()
                    .is_none_or(|end| is_static_literal_scalar_operand(end, context))
        }
        Expression::BinaryOp {
            op: CypherBinaryOperator::Add,
            lhs,
            rhs,
            ..
        } => {
            is_static_literal_list_operand(lhs, context)
                && is_static_literal_list_operand(rhs, context)
        }
        Expression::FunctionCall(function) => {
            context.variable_function_argument(function).is_none()
                && (is_tail_function(function) || is_reverse_function(function))
                && function
                    .arguments
                    .iter()
                    .all(|argument| is_static_literal_list_operand(argument, context))
        }
        _ => false,
    }
}

pub(super) fn expression_contains_recovered_function_variable_argument(
    expression: &Expression,
    context: &CypherCompileContext,
) -> bool {
    match expression {
        Expression::Parenthesized(inner) => {
            expression_contains_recovered_function_variable_argument(inner, context)
        }
        Expression::UnaryOp { operand, .. } | Expression::IsNull { operand, .. } => {
            expression_contains_recovered_function_variable_argument(operand, context)
        }
        Expression::BinaryOp { lhs, rhs, .. } | Expression::In { lhs, rhs, .. } => {
            expression_contains_recovered_function_variable_argument(lhs, context)
                || expression_contains_recovered_function_variable_argument(rhs, context)
        }
        Expression::Comparison { lhs, operators, .. } => {
            expression_contains_recovered_function_variable_argument(lhs, context)
                || operators.iter().any(|(_, rhs)| {
                    expression_contains_recovered_function_variable_argument(rhs, context)
                })
        }
        Expression::ListIndex { list, index, .. } => {
            expression_contains_recovered_function_variable_argument(list, context)
                || expression_contains_recovered_function_variable_argument(index, context)
        }
        Expression::ListSlice {
            list, start, end, ..
        } => {
            expression_contains_recovered_function_variable_argument(list, context)
                || start.as_deref().is_some_and(|start| {
                    expression_contains_recovered_function_variable_argument(start, context)
                })
                || end.as_deref().is_some_and(|end| {
                    expression_contains_recovered_function_variable_argument(end, context)
                })
        }
        Expression::Case(case) => {
            case.scrutinee.as_deref().is_some_and(|expression| {
                expression_contains_recovered_function_variable_argument(expression, context)
            }) || case.alternatives.iter().any(|alternative| {
                expression_contains_recovered_function_variable_argument(&alternative.when, context)
                    || expression_contains_recovered_function_variable_argument(
                        &alternative.then,
                        context,
                    )
            }) || case.default.as_deref().is_some_and(|expression| {
                expression_contains_recovered_function_variable_argument(expression, context)
            })
        }
        Expression::FunctionCall(function) => {
            context.variable_function_argument(function).is_some()
                || function.arguments.iter().any(|argument| {
                    expression_contains_recovered_function_variable_argument(argument, context)
                })
        }
        _ => false,
    }
}

pub(super) fn compile_optional_static_list_comparison(
    lhs: &Expression,
    operator: ComparisonOperator,
    rhs: &Expression,
    path: &str,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<PredicateExpression>, CoreError> {
    if !is_parameter_expression(lhs)
        && let Some(predicate) =
            compile_optional_static_list_slice_comparison(lhs, operator, rhs, path, plan, context)?
    {
        return Ok(Some(predicate));
    }
    if !is_parameter_expression(lhs)
        && let Some(predicate) = compile_optional_static_list_comprehension_comparison(
            lhs, operator, rhs, path, plan, context,
        )?
    {
        return Ok(Some(predicate));
    }
    if !is_parameter_expression(rhs) && is_branch_local_static_list_slice_expression(rhs) {
        let inverted_operator = invert_comparison_operator(operator, format!("{path}.operator"))?;
        if let Some(predicate) = compile_optional_static_list_slice_comparison(
            rhs,
            inverted_operator,
            lhs,
            path,
            plan,
            context,
        )? {
            return Ok(Some(predicate));
        }
    }
    if !is_parameter_expression(rhs) && is_static_list_comprehension_expression(rhs) {
        let inverted_operator = invert_comparison_operator(operator, format!("{path}.operator"))?;
        if let Some(predicate) = compile_optional_static_list_comprehension_comparison(
            rhs,
            inverted_operator,
            lhs,
            path,
            plan,
            context,
        )? {
            return Ok(Some(predicate));
        }
    }
    if !is_parameter_expression(lhs)
        && let Some(actual) =
            compile_optional_static_list_value(lhs, format!("{path}.lhs"), Some(plan), context)?
    {
        let expected =
            compile_static_list_comparison_rhs(rhs, format!("{path}.rhs"), plan, context)?;
        return Ok(Some(compile_static_list_predicate(
            &actual, operator, &expected, path,
        )?));
    }
    if is_parameter_expression(rhs) {
        return Ok(None);
    }
    let Some(actual) =
        compile_optional_static_list_value(rhs, format!("{path}.rhs"), Some(plan), context)?
    else {
        return Ok(None);
    };
    let expected = compile_static_list_comparison_rhs(lhs, format!("{path}.lhs"), plan, context)?;
    Ok(Some(compile_static_list_predicate(
        &actual,
        invert_comparison_operator(operator, format!("{path}.operator"))?,
        &expected,
        path,
    )?))
}

pub(super) fn is_static_list_comprehension_expression(expression: &Expression) -> bool {
    match expression {
        Expression::Parenthesized(inner) => is_static_list_comprehension_expression(inner),
        Expression::ListComprehension(_) => true,
        _ => false,
    }
}

pub(super) fn compile_optional_static_list_comprehension_comparison(
    actual: &Expression,
    operator: ComparisonOperator,
    expected: &Expression,
    path: &str,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<PredicateExpression>, CoreError> {
    if !is_static_list_comprehension_expression(actual) {
        return Ok(None);
    }
    let expected =
        compile_static_list_comparison_rhs(expected, format!("{path}.expected"), plan, context)?;
    let Some(expression) = compile_optional_static_list_comprehension_comparison_scalar_expression(
        actual,
        operator,
        &expected,
        path,
        PredicateCompileMode::CaseWhen { plan: Some(plan) },
        context,
    )?
    else {
        return Ok(None);
    };
    Ok(Some(boolean_scalar_expression_predicate(expression)))
}

pub(super) fn compile_optional_static_list_comprehension_comparison_scalar_expression(
    actual: &Expression,
    operator: ComparisonOperator,
    expected: &StaticListValue,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match actual {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_comprehension_comparison_scalar_expression(
                inner, operator, expected, path, mode, context,
            )
        }
        Expression::ListComprehension(comprehension) => {
            let source = context
                .list_comprehension_source(comprehension)
                .ok_or_else(|| {
                    unsupported(
                        path.clone(),
                        "list comprehensions require a recoverable `variable IN collection` source",
                    )
                })?;
            let variable = variable_name(&comprehension.variable);
            if source.variable != variable {
                return Err(unsupported(
                    path,
                    "list comprehension variable recovery did not match the parsed AST",
                ));
            }
            let map = if source.has_map {
                Some(comprehension.map.as_ref().ok_or_else(|| {
                    unsupported(
                        format!("{path}.map"),
                        "mapped static list comprehensions require a recoverable map expression",
                    )
                })?)
            } else {
                None
            };
            let recovered_filter =
                recover_static_list_comprehension_filter(comprehension, source, &path, context)?;
            let filter = comprehension
                .filter
                .as_deref()
                .or_else(|| recovered_filter.as_ref().map(|(filter, _)| filter));
            let filter_context = recovered_filter
                .as_ref()
                .map_or(context, |(_, filter_context)| filter_context);
            let (collection_expression, collection_context) = parse_cypher_expression_fragment(
                &source.collection_source,
                format!("{path}.collection"),
                context,
            )?;
            let evaluation = StaticListComprehensionEvaluation {
                variable: &variable,
                filter,
                filter_context,
                map,
                map_context: context,
                mode,
            };
            let comparison = StaticListSliceComparison {
                bounds: StaticListSliceBounds {
                    start: None,
                    end: None,
                },
                operator,
                expected,
            };
            compile_optional_static_list_comprehension_source_comparison_scalar_expression(
                &collection_expression,
                &comparison,
                path,
                evaluation,
                &collection_context,
                context,
            )
        }
        _ => Ok(None),
    }
}

pub(super) fn compile_optional_static_list_comprehension_source_comparison_scalar_expression(
    collection: &Expression,
    comparison: &StaticListSliceComparison<'_>,
    path: impl Into<String>,
    evaluation: StaticListComprehensionEvaluation<'_>,
    collection_context: &CypherCompileContext,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match collection {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_comprehension_source_comparison_scalar_expression(
                inner,
                comparison,
                path,
                evaluation,
                collection_context,
                context,
            )
        }
        Expression::Case(case) => {
            let Some(parts) = compile_optional_static_list_case_parts(
                case,
                format!("{path}.collection"),
                evaluation.mode,
                collection_context,
            )?
            else {
                return Ok(None);
            };
            Ok(Some(ScalarExpression::Case {
                alternatives: parts
                    .alternatives
                    .into_iter()
                    .enumerate()
                    .map(|(index, (when, result))| {
                        let result = static_list_case_result_comprehension_result(
                            result,
                            format!("{path}.collection.alternatives[{index}].then"),
                            evaluation,
                        )?;
                        Ok(ScalarCaseAlternative {
                            when,
                            then: static_list_case_result_slice_comparison_scalar_expression(
                                result,
                                comparison,
                                format!("{path}.collection.alternatives[{index}].then"),
                                context,
                            )?,
                        })
                    })
                    .collect::<Result<Vec<_>, CoreError>>()?,
                else_expression: parts
                    .default
                    .map(|result| {
                        let result = static_list_case_result_comprehension_result(
                            result,
                            format!("{path}.collection.default"),
                            evaluation,
                        )?;
                        static_list_case_result_slice_comparison_scalar_expression(
                            result,
                            comparison,
                            format!("{path}.collection.default"),
                            context,
                        )
                        .map(Box::new)
                    })
                    .transpose()?,
            }))
        }
        Expression::FunctionCall(function) if is_coalesce_function(function) => {
            let Some(coalesce) = compile_optional_static_list_coalesce_arguments(
                function,
                format!("{path}.collection"),
                evaluation.mode.static_metadata_plan(),
                collection_context,
            )?
            else {
                return Ok(None);
            };
            let coalesce =
                static_list_coalesce_comprehension_arguments(coalesce, path.clone(), evaluation)?;
            static_list_coalesce_slice_comparison_scalar_expression(
                coalesce, comparison, path, context,
            )
            .map(Some)
        }
        Expression::ListSlice {
            list, start, end, ..
        } => compile_optional_static_list_slice_comprehension_comparison_scalar_expression(
            list,
            StaticListSliceBounds {
                start: start.as_deref(),
                end: end.as_deref(),
            },
            comparison,
            path,
            evaluation,
            collection_context,
            context,
        ),
        _ => Ok(None),
    }
}

pub(super) fn compile_optional_static_list_slice_comprehension_comparison_scalar_expression(
    list: &Expression,
    bounds: StaticListSliceBounds<'_>,
    comparison: &StaticListSliceComparison<'_>,
    path: impl Into<String>,
    evaluation: StaticListComprehensionEvaluation<'_>,
    collection_context: &CypherCompileContext,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match list {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_slice_comprehension_comparison_scalar_expression(
                inner,
                bounds,
                comparison,
                path,
                evaluation,
                collection_context,
                context,
            )
        }
        Expression::Case(case) => {
            compile_optional_static_list_case_slice_comprehension_comparison_scalar_expression(
                case,
                bounds,
                comparison,
                path,
                evaluation,
                collection_context,
                context,
            )
        }
        Expression::FunctionCall(function) if is_coalesce_function(function) => {
            compile_optional_static_list_coalesce_slice_comprehension_comparison_scalar_expression(
                function,
                bounds,
                comparison,
                path,
                evaluation,
                collection_context,
                context,
            )
        }
        _ => Ok(None),
    }
}

pub(super) fn compile_optional_static_list_case_slice_comprehension_comparison_scalar_expression(
    case: &CaseExpression,
    bounds: StaticListSliceBounds<'_>,
    comparison: &StaticListSliceComparison<'_>,
    path: impl Into<String>,
    evaluation: StaticListComprehensionEvaluation<'_>,
    collection_context: &CypherCompileContext,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    let Some(parts) = compile_optional_static_list_case_parts(
        case,
        format!("{path}.collection.list"),
        evaluation.mode,
        collection_context,
    )?
    else {
        return Ok(None);
    };
    Ok(Some(ScalarExpression::Case {
        alternatives: parts
            .alternatives
            .into_iter()
            .enumerate()
            .map(|(index, (when, result))| {
                let result = static_list_case_result_slice_comprehension_result(
                    result,
                    bounds,
                    format!("{path}.collection.alternatives[{index}].then"),
                    evaluation,
                    collection_context,
                )?;
                Ok(ScalarCaseAlternative {
                    when,
                    then: static_list_case_result_slice_comparison_scalar_expression(
                        result,
                        comparison,
                        format!("{path}.collection.alternatives[{index}].then"),
                        context,
                    )?,
                })
            })
            .collect::<Result<Vec<_>, CoreError>>()?,
        else_expression: parts
            .default
            .map(|result| {
                let result = static_list_case_result_slice_comprehension_result(
                    result,
                    bounds,
                    format!("{path}.collection.default"),
                    evaluation,
                    collection_context,
                )?;
                static_list_case_result_slice_comparison_scalar_expression(
                    result,
                    comparison,
                    format!("{path}.collection.default"),
                    context,
                )
                .map(Box::new)
            })
            .transpose()?,
    }))
}

pub(super) fn compile_optional_static_list_coalesce_slice_comprehension_comparison_scalar_expression(
    function: &FunctionInvocation,
    bounds: StaticListSliceBounds<'_>,
    comparison: &StaticListSliceComparison<'_>,
    path: impl Into<String>,
    evaluation: StaticListComprehensionEvaluation<'_>,
    collection_context: &CypherCompileContext,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    let Some(coalesce) = compile_optional_static_list_coalesce_arguments(
        function,
        format!("{path}.collection.list"),
        evaluation.mode.static_metadata_plan(),
        collection_context,
    )?
    else {
        return Ok(None);
    };
    let coalesce = static_list_coalesce_slice_comprehension_arguments(
        coalesce,
        bounds,
        path.clone(),
        evaluation,
        collection_context,
    )?;
    static_list_coalesce_slice_comparison_scalar_expression(coalesce, comparison, path, context)
        .map(Some)
}

pub(super) fn compile_optional_static_list_slice_comparison(
    actual: &Expression,
    operator: ComparisonOperator,
    expected: &Expression,
    path: &str,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<PredicateExpression>, CoreError> {
    if !is_branch_local_static_list_slice_expression(actual) {
        return Ok(None);
    }
    let expected =
        compile_static_list_comparison_rhs(expected, format!("{path}.expected"), plan, context)?;
    let Some(expression) = compile_optional_static_list_slice_comparison_scalar_expression(
        actual,
        operator,
        &expected,
        path,
        PredicateCompileMode::CaseWhen { plan: Some(plan) },
        context,
    )?
    else {
        return Ok(None);
    };
    Ok(Some(boolean_scalar_expression_predicate(expression)))
}

pub(super) fn is_branch_local_static_list_slice_expression(expression: &Expression) -> bool {
    match expression {
        Expression::Parenthesized(inner) => is_branch_local_static_list_slice_expression(inner),
        Expression::ListSlice { list, .. } => is_static_list_case_or_coalesce_source(list),
        _ => false,
    }
}

pub(super) fn is_static_list_case_or_coalesce_source(expression: &Expression) -> bool {
    match expression {
        Expression::Parenthesized(inner) => is_static_list_case_or_coalesce_source(inner),
        Expression::Case(_) => true,
        Expression::FunctionCall(function) => is_coalesce_function(function),
        _ => false,
    }
}

pub(super) fn compile_optional_static_list_slice_comparison_scalar_expression(
    actual: &Expression,
    operator: ComparisonOperator,
    expected: &StaticListValue,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match actual {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_slice_comparison_scalar_expression(
                inner, operator, expected, path, mode, context,
            )
        }
        Expression::ListSlice {
            list, start, end, ..
        } => {
            let bounds = StaticListSliceBounds {
                start: start.as_deref(),
                end: end.as_deref(),
            };
            let comparison = StaticListSliceComparison {
                bounds,
                operator,
                expected,
            };
            if let Some(expression) =
                compile_optional_static_list_case_slice_comparison_scalar_expression(
                    list,
                    &comparison,
                    path.clone(),
                    mode,
                    context,
                )?
            {
                return Ok(Some(expression));
            }
            compile_optional_static_list_coalesce_slice_comparison_scalar_expression(
                list,
                &comparison,
                path,
                mode.static_metadata_plan(),
                context,
            )
        }
        _ => Ok(None),
    }
}

#[derive(Clone, Copy)]
pub(super) struct StaticListSliceBounds<'a> {
    pub(super) start: Option<&'a Expression>,
    pub(super) end: Option<&'a Expression>,
}

pub(super) struct StaticListSliceComparison<'a> {
    bounds: StaticListSliceBounds<'a>,
    operator: ComparisonOperator,
    expected: &'a StaticListValue,
}

pub(super) fn compile_optional_static_list_case_slice_comparison_scalar_expression(
    list: &Expression,
    comparison: &StaticListSliceComparison<'_>,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match list {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_case_slice_comparison_scalar_expression(
                inner, comparison, path, mode, context,
            )
        }
        Expression::Case(case) => {
            let Some(parts) = compile_optional_static_list_case_parts(
                case,
                format!("{path}.list"),
                mode,
                context,
            )?
            else {
                return Ok(None);
            };
            Ok(Some(ScalarExpression::Case {
                alternatives: parts
                    .alternatives
                    .into_iter()
                    .enumerate()
                    .map(|(alternative_index, (when, result))| {
                        Ok(ScalarCaseAlternative {
                            when,
                            then: static_list_case_result_slice_comparison_scalar_expression(
                                result,
                                comparison,
                                format!("{path}.list.alternatives[{alternative_index}].then"),
                                context,
                            )?,
                        })
                    })
                    .collect::<Result<Vec<_>, CoreError>>()?,
                else_expression: parts
                    .default
                    .map(|result| {
                        static_list_case_result_slice_comparison_scalar_expression(
                            result,
                            comparison,
                            format!("{path}.list.default"),
                            context,
                        )
                        .map(Box::new)
                    })
                    .transpose()?,
            }))
        }
        _ => Ok(None),
    }
}

pub(super) fn compile_optional_static_list_coalesce_slice_comparison_scalar_expression(
    list: &Expression,
    comparison: &StaticListSliceComparison<'_>,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match list {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_coalesce_slice_comparison_scalar_expression(
                inner, comparison, path, plan, context,
            )
        }
        Expression::FunctionCall(function) if is_coalesce_function(function) => {
            let Some(coalesce) = compile_optional_static_list_coalesce_arguments(
                function,
                format!("{path}.list"),
                plan,
                context,
            )?
            else {
                return Ok(None);
            };
            static_list_coalesce_slice_comparison_scalar_expression(
                coalesce, comparison, path, context,
            )
            .map(Some)
        }
        _ => Ok(None),
    }
}

pub(super) fn static_list_case_result_slice_comparison_scalar_expression(
    result: StaticListCaseResult,
    comparison: &StaticListSliceComparison<'_>,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    match result {
        StaticListCaseResult::Null => Ok(ScalarExpression::Literal(Literal::Null)),
        StaticListCaseResult::List(value) => {
            static_list_value_slice_comparison_scalar_expression(value, comparison, path, context)
        }
        StaticListCaseResult::Coalesce(coalesce) => {
            static_list_coalesce_slice_comparison_scalar_expression(
                coalesce, comparison, path, context,
            )
        }
    }
}

pub(super) fn static_list_coalesce_slice_comparison_scalar_expression(
    coalesce: StaticListCoalesceArguments,
    comparison: &StaticListSliceComparison<'_>,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let mut expression = ScalarExpression::Literal(Literal::Null);
    for (argument_index, argument) in coalesce.arguments.into_iter().enumerate().rev() {
        let StaticListCoalesceArgument::List(mut value) = argument else {
            continue;
        };
        let presence_variable = value.presence_variable.take();
        let branch_comparison = static_list_value_slice_comparison_scalar_expression(
            value,
            comparison,
            format!("{path}.arguments[{argument_index}]"),
            context,
        )?;
        expression = match presence_variable {
            Some(variable) => ScalarExpression::Case {
                alternatives: vec![ScalarCaseAlternative {
                    when: PredicateExpression::Presence(PresencePredicate {
                        variable,
                        operator: ComparisonOperator::NotEqual,
                    }),
                    then: branch_comparison,
                }],
                else_expression: Some(Box::new(expression)),
            },
            None => branch_comparison,
        };
    }
    Ok(expression)
}

pub(super) fn static_list_value_slice_comparison_scalar_expression(
    value: StaticListValue,
    comparison: &StaticListSliceComparison<'_>,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let value = slice_static_list_value(
        value,
        comparison.bounds.start,
        comparison.bounds.end,
        path.clone(),
        context,
    )?;
    Ok(ScalarExpression::Predicate(Box::new(
        compile_static_list_predicate(&value, comparison.operator, comparison.expected, &path)?,
    )))
}

pub(super) fn compile_optional_metadata_list_ref(
    expression: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<MetadataListRef>, CoreError> {
    let path = path.into();
    if let Expression::FunctionCall(function) = expression
        && is_labels_function(function)
        && let Some(value) = compile_optional_same_label_undirected_endpoint_function_argument(
            function,
            format!("{path}.arguments"),
            plan,
            context,
        )?
    {
        return Ok(Some(MetadataListRef::UndirectedEndpointLabels { value }));
    }
    if let Expression::FunctionCall(function) = expression
        && is_keys_function(function)
        && let Some(value) = compile_optional_same_label_undirected_endpoint_function_argument(
            function,
            format!("{path}.arguments"),
            plan,
            context,
        )?
    {
        return Ok(Some(MetadataListRef::UndirectedEndpointKeys { value }));
    }
    if let Some((value, label)) =
        compile_optional_labels_ref(expression, path.clone(), plan, context)?
    {
        return Ok(Some(MetadataListRef::Labels { value, label }));
    }
    Ok(compile_optional_keys_ref(expression, path, plan, context)?
        .map(|value| MetadataListRef::Keys { value }))
}

pub(super) fn compile_metadata_list_actual_literals(
    reference: &MetadataListRef,
    graph: Option<&Declaration>,
    plan: &GraphPlan,
    path: impl Into<String>,
) -> Result<Vec<Literal>, CoreError> {
    let path = path.into();
    match reference {
        MetadataListRef::Labels { label, .. } => Ok(vec![Literal::String(label.clone())]),
        MetadataListRef::UndirectedEndpointLabels { value } => {
            Ok(vec![Literal::String(value.label.clone())])
        }
        MetadataListRef::Keys { value } => {
            let graph = graph.ok_or_else(|| {
                unsupported(
                    path.clone(),
                    "keys() requires a graph declaration so mapped property keys can be inspected",
                )
            })?;
            declared_graph_value_property_names(graph, plan, value, &path).map(|properties| {
                properties
                    .into_iter()
                    .map(Literal::String)
                    .collect::<Vec<_>>()
            })
        }
        MetadataListRef::UndirectedEndpointKeys { value } => {
            let graph = graph.ok_or_else(|| {
                unsupported(
                    path.clone(),
                    "keys() requires a graph declaration so mapped property keys can be inspected",
                )
            })?;
            let mapping = graph.node(&value.label).ok_or_else(|| {
                unsupported(
                    path,
                    format!(
                        "keys() metadata expression could not resolve node label '{}'",
                        value.label
                    ),
                )
            })?;
            Ok(mapping
                .properties
                .keys()
                .cloned()
                .map(Literal::String)
                .collect::<Vec<_>>())
        }
    }
}

pub(super) fn compile_optional_metadata_list_value(
    expression: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<MetadataListValue>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_optional_metadata_list_value(inner, path, plan, context)
        }
        Expression::ListSlice {
            list, start, end, ..
        } => {
            let Some(mut value) =
                compile_optional_metadata_list_value(list, format!("{path}.list"), plan, context)?
            else {
                return Ok(None);
            };
            value.literals = compile_list_slice_literals(
                &value.literals,
                start.as_deref(),
                end.as_deref(),
                path,
                context,
                "metadata list slice bounds require integer literals or scalar integer parameters",
            )?;
            Ok(Some(value))
        }
        expression => {
            let Some(reference) =
                compile_optional_metadata_list_ref(expression, path.clone(), plan, context)?
            else {
                return Ok(None);
            };
            let literals = compile_metadata_list_actual_literals(
                &reference,
                context.graph.as_ref(),
                plan,
                path.clone(),
            )?;
            let presence_variable = match &reference {
                MetadataListRef::Labels { value, .. } | MetadataListRef::Keys { value } => {
                    graph_value_metadata_presence_variable(value, plan)?
                }
                MetadataListRef::UndirectedEndpointLabels { value }
                | MetadataListRef::UndirectedEndpointKeys { value } => {
                    Some(value.relationship.clone())
                }
            };
            Ok(Some(MetadataListValue {
                presence_variable,
                literals,
            }))
        }
    }
}

pub(super) fn compile_static_list_predicate(
    actual: &StaticListValue,
    operator: ComparisonOperator,
    expected: &StaticListValue,
    path: &str,
) -> Result<PredicateExpression, CoreError> {
    let matches = evaluate_static_literal_list_comparison(actual, operator, expected, path)?;
    let presence_variables = actual
        .presence_variable
        .iter()
        .chain(expected.presence_variable.iter())
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    Ok(static_boolean_outcome_predicate(
        matches,
        presence_variables,
    ))
}

pub(super) fn compile_static_list_comparison_rhs(
    expression: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<StaticListValue, CoreError> {
    let path = path.into();
    compile_optional_static_list_value(expression, path.clone(), Some(plan), context)?.ok_or_else(
        || {
            unsupported(
                path,
                "static list predicates require a literal list, list parameter, static split(...), range(...), tail(...), or static labels()/keys() metadata list",
            )
        },
    )
}

pub(super) fn compile_static_list_value_source(
    source: &str,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<StaticListValue>, CoreError> {
    let path = path.into();
    let (expression, fragment_context) =
        parse_cypher_expression_fragment(source, path.clone(), context)?;
    compile_optional_static_list_value(&expression, path, plan, &fragment_context)
}

pub(super) fn parse_cypher_expression_fragment(
    source: &str,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<(Expression, CypherCompileContext), CoreError> {
    let path = path.into();
    let fragment = format!("RETURN {source} AS __coral_expr");
    let query = decypher::parse(&fragment).map_err(|error| {
        Diagnostic::new(
            diagnostic_codes::CYPHER_PARSE_ERROR,
            path.clone(),
            format!("could not parse Cypher expression fragment: {error}"),
        )
        .into_core_error()
    })?;
    let expression = single_return_expression(&query, &path)?.clone();
    let fragment_context = CypherCompileContext::from_source_with_parameters_and_graph(
        &fragment,
        context.parameters.clone(),
        context.graph.clone(),
        context.catalog.as_ref(),
        BTreeMap::new(),
    );
    Ok((expression, fragment_context))
}

pub(super) fn compile_static_list_quantifier_ast_predicate(
    expression: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<PredicateExpression, CoreError> {
    match expression {
        Expression::All(filter) => compile_static_list_quantifier_predicate(
            filter,
            StaticListQuantifier::All,
            path,
            mode,
            context,
        ),
        Expression::Any(filter) => compile_static_list_quantifier_predicate(
            filter,
            StaticListQuantifier::Any,
            path,
            mode,
            context,
        ),
        Expression::None(filter) => compile_static_list_quantifier_predicate(
            filter,
            StaticListQuantifier::None,
            path,
            mode,
            context,
        ),
        Expression::Single(filter) => compile_static_list_quantifier_predicate(
            filter,
            StaticListQuantifier::Single,
            path,
            mode,
            context,
        ),
        _ => Err(CoreError::internal(
            "static list quantifier AST helper called with non-quantifier expression",
        )),
    }
}

pub(super) fn compile_static_list_quantifier_predicate(
    filter: &FilterExpression,
    quantifier: StaticListQuantifier,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<PredicateExpression, CoreError> {
    let path = path.into();
    let variable = variable_name(&filter.variable);
    if let Some(predicate) = compile_optional_static_list_quantifier_collection_predicate(
        &filter.collection,
        filter.predicate.as_deref(),
        &variable,
        quantifier,
        path.clone(),
        mode,
        context,
    )? {
        return Ok(predicate);
    }
    let Some(collection) = compile_optional_static_list_value(
        &filter.collection,
        format!("{path}.collection"),
        mode.static_metadata_plan(),
        context,
    )?
    else {
        return Err(unsupported(
            format!("{path}.collection"),
            "collection predicates require a literal list, list parameter, static split(...), range(...), tail(...), or static labels()/keys() metadata list",
        ));
    };
    compile_static_list_quantifier_value_predicate(
        collection,
        filter.predicate.as_deref(),
        &variable,
        quantifier,
        path,
        mode,
        context,
    )
}

pub(super) fn compile_static_list_quantifier_value_predicate(
    collection: StaticListValue,
    predicate: Option<&Expression>,
    variable: &str,
    quantifier: StaticListQuantifier,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<PredicateExpression, CoreError> {
    let path = path.into();
    let outcomes = collection
        .literals
        .iter()
        .enumerate()
        .map(|(index, item)| {
            let evaluation = StaticFilterEvaluation {
                variable,
                item,
                accumulator_variable: None,
                accumulator: None,
                mode,
                context,
            };
            evaluate_static_filter_predicate(
                predicate,
                evaluation,
                format!("{path}.predicate[{index}]"),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(static_boolean_outcome_predicate(
        evaluate_static_list_quantifier(quantifier, outcomes.into_iter()),
        collection.presence_variable.into_iter().collect(),
    ))
}

pub(super) fn compile_static_list_quantifier_function_predicate(
    function: &FunctionInvocation,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<PredicateExpression, CoreError> {
    let path = path.into();
    let quantifier = collection_quantifier_function(function).ok_or_else(|| {
        CoreError::internal("collection quantifier helper called with non-quantifier function")
    })?;
    if let [
        Expression::All(filter)
        | Expression::Any(filter)
        | Expression::None(filter)
        | Expression::Single(filter),
    ] = function.arguments.as_slice()
    {
        return compile_static_list_quantifier_predicate(filter, quantifier, path, mode, context);
    }

    let call = context.collection_filter_call(function).ok_or_else(|| {
        unsupported(
            format!("{path}.arguments"),
            format!(
                "{}() requires an item IN collection filter expression",
                qualified_function_name(function)
            ),
        )
    })?;
    let predicate = if call.has_predicate {
        let [predicate] = function.arguments.as_slice() else {
            return Err(unsupported(
                format!("{path}.arguments"),
                format!(
                    "{}() requires exactly one WHERE predicate expression",
                    qualified_function_name(function)
                ),
            ));
        };
        Some(predicate)
    } else {
        None
    };
    let (collection_expression, fragment_context) = parse_cypher_expression_fragment(
        &call.collection_source,
        format!("{path}.collection"),
        context,
    )?;
    if let Some(compiled) = compile_optional_static_list_quantifier_collection_predicate(
        &collection_expression,
        predicate,
        &call.variable,
        quantifier,
        path.clone(),
        mode,
        &fragment_context,
    )? {
        return Ok(compiled);
    }
    let collection = compile_optional_static_list_value(
        &collection_expression,
        format!("{path}.collection"),
        mode.static_metadata_plan(),
        &fragment_context,
    )?
    .ok_or_else(|| {
        unsupported(
            format!("{path}.collection"),
            "collection predicates require a literal list, list parameter, static split(...), range(...), tail(...), or static labels()/keys() metadata list",
        )
    })?;
    compile_static_list_quantifier_value_predicate(
        collection,
        predicate,
        &call.variable,
        quantifier,
        path,
        mode,
        context,
    )
}

pub(super) fn compile_optional_static_list_quantifier_collection_predicate(
    collection: &Expression,
    predicate: Option<&Expression>,
    variable: &str,
    quantifier: StaticListQuantifier,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<PredicateExpression>, CoreError> {
    let path = path.into();
    if let Some(expression) = compile_optional_static_list_slice_quantifier_scalar_expression(
        collection,
        predicate,
        variable,
        quantifier,
        path.clone(),
        mode,
        context,
    )? {
        return Ok(Some(boolean_scalar_expression_predicate(expression)));
    }
    if let Some(expression) = compile_optional_static_list_case_quantifier_scalar_expression(
        collection,
        predicate,
        variable,
        quantifier,
        path.clone(),
        mode,
        context,
    )? {
        return Ok(Some(boolean_scalar_expression_predicate(expression)));
    }
    if let Some(expression) = compile_optional_static_list_coalesce_quantifier_scalar_expression(
        collection, predicate, variable, quantifier, path, mode, context,
    )? {
        return Ok(Some(boolean_scalar_expression_predicate(expression)));
    }
    Ok(None)
}

#[derive(Clone, Copy)]
pub(super) struct StaticListQuantifierCompile<'a> {
    predicate: Option<&'a Expression>,
    variable: &'a str,
    quantifier: StaticListQuantifier,
    mode: PredicateCompileMode<'a>,
    context: &'a CypherCompileContext,
}

pub(super) fn compile_optional_static_list_slice_quantifier_scalar_expression(
    collection: &Expression,
    predicate: Option<&Expression>,
    variable: &str,
    quantifier: StaticListQuantifier,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    if !is_branch_local_static_list_slice_expression(collection) {
        return Ok(None);
    }
    let compile = StaticListQuantifierCompile {
        predicate,
        variable,
        quantifier,
        mode,
        context,
    };
    compile_optional_static_list_slice_quantifier_inner(collection, path, compile)
}

pub(super) fn compile_optional_static_list_slice_quantifier_inner(
    collection: &Expression,
    path: impl Into<String>,
    compile: StaticListQuantifierCompile<'_>,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match collection {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_slice_quantifier_inner(inner, path, compile)
        }
        Expression::ListSlice {
            list, start, end, ..
        } => {
            let bounds = StaticListSliceBounds {
                start: start.as_deref(),
                end: end.as_deref(),
            };
            if let Some(expression) =
                compile_optional_static_list_case_slice_quantifier_scalar_expression(
                    list,
                    bounds,
                    path.clone(),
                    compile,
                )?
            {
                return Ok(Some(expression));
            }
            compile_optional_static_list_coalesce_slice_quantifier_scalar_expression(
                list, bounds, path, compile,
            )
        }
        _ => Ok(None),
    }
}

pub(super) fn compile_optional_static_list_case_slice_quantifier_scalar_expression(
    list: &Expression,
    bounds: StaticListSliceBounds<'_>,
    path: impl Into<String>,
    compile: StaticListQuantifierCompile<'_>,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match list {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_case_slice_quantifier_scalar_expression(
                inner, bounds, path, compile,
            )
        }
        Expression::Case(case) => {
            let Some(parts) = compile_optional_static_list_case_parts(
                case,
                format!("{path}.collection"),
                compile.mode,
                compile.context,
            )?
            else {
                return Ok(None);
            };
            Ok(Some(ScalarExpression::Case {
                alternatives: parts
                    .alternatives
                    .into_iter()
                    .enumerate()
                    .map(|(index, (when, result))| {
                        Ok(ScalarCaseAlternative {
                            when,
                            then: static_list_case_result_slice_quantifier_scalar_expression(
                                result,
                                bounds,
                                format!("{path}.collection.alternatives[{index}].then"),
                                compile,
                            )?,
                        })
                    })
                    .collect::<Result<Vec<_>, CoreError>>()?,
                else_expression: parts
                    .default
                    .map(|result| {
                        static_list_case_result_slice_quantifier_scalar_expression(
                            result,
                            bounds,
                            format!("{path}.collection.default"),
                            compile,
                        )
                        .map(Box::new)
                    })
                    .transpose()?,
            }))
        }
        _ => Ok(None),
    }
}

pub(super) fn compile_optional_static_list_coalesce_slice_quantifier_scalar_expression(
    list: &Expression,
    bounds: StaticListSliceBounds<'_>,
    path: impl Into<String>,
    compile: StaticListQuantifierCompile<'_>,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match list {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_coalesce_slice_quantifier_scalar_expression(
                inner, bounds, path, compile,
            )
        }
        Expression::FunctionCall(function) if is_coalesce_function(function) => {
            let Some(coalesce) = compile_optional_static_list_coalesce_arguments(
                function,
                format!("{path}.collection"),
                compile.mode.static_metadata_plan(),
                compile.context,
            )?
            else {
                return Ok(None);
            };
            static_list_coalesce_slice_quantifier_scalar_expression(coalesce, bounds, path, compile)
                .map(Some)
        }
        _ => Ok(None),
    }
}

pub(super) fn static_list_case_result_slice_quantifier_scalar_expression(
    result: StaticListCaseResult,
    bounds: StaticListSliceBounds<'_>,
    path: impl Into<String>,
    compile: StaticListQuantifierCompile<'_>,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    match result {
        StaticListCaseResult::Null => Ok(ScalarExpression::Literal(Literal::Null)),
        StaticListCaseResult::List(value) => {
            static_list_value_slice_quantifier_scalar_expression(value, bounds, path, compile)
        }
        StaticListCaseResult::Coalesce(coalesce) => {
            static_list_coalesce_slice_quantifier_scalar_expression(coalesce, bounds, path, compile)
        }
    }
}

pub(super) fn static_list_coalesce_slice_quantifier_scalar_expression(
    coalesce: StaticListCoalesceArguments,
    bounds: StaticListSliceBounds<'_>,
    path: impl Into<String>,
    compile: StaticListQuantifierCompile<'_>,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let mut expression = ScalarExpression::Literal(Literal::Null);
    for (index, argument) in coalesce.arguments.into_iter().enumerate().rev() {
        let StaticListCoalesceArgument::List(mut value) = argument else {
            continue;
        };
        let presence_variable = value.presence_variable.take();
        let quantifier_expression = static_list_value_slice_quantifier_scalar_expression(
            value,
            bounds,
            format!("{path}.arguments[{index}]"),
            compile,
        )?;
        expression = match presence_variable {
            Some(variable) => ScalarExpression::Case {
                alternatives: vec![ScalarCaseAlternative {
                    when: PredicateExpression::Presence(PresencePredicate {
                        variable,
                        operator: ComparisonOperator::NotEqual,
                    }),
                    then: quantifier_expression,
                }],
                else_expression: Some(Box::new(expression)),
            },
            None => quantifier_expression,
        };
    }
    Ok(expression)
}

pub(super) fn static_list_value_slice_quantifier_scalar_expression(
    collection: StaticListValue,
    bounds: StaticListSliceBounds<'_>,
    path: impl Into<String>,
    compile: StaticListQuantifierCompile<'_>,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let collection = slice_static_list_value(
        collection,
        bounds.start,
        bounds.end,
        path.clone(),
        compile.context,
    )?;
    compile_static_list_quantifier_value_scalar_expression(
        collection,
        compile.predicate,
        compile.variable,
        compile.quantifier,
        path,
        compile.mode,
        compile.context,
    )
}

pub(super) fn compile_optional_static_list_case_quantifier_scalar_expression(
    collection: &Expression,
    predicate: Option<&Expression>,
    variable: &str,
    quantifier: StaticListQuantifier,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match collection {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_case_quantifier_scalar_expression(
                inner, predicate, variable, quantifier, path, mode, context,
            )
        }
        Expression::Case(case) => {
            let Some(parts) = compile_optional_static_list_case_parts(
                case,
                format!("{path}.collection"),
                mode,
                context,
            )?
            else {
                return Ok(None);
            };
            Ok(Some(ScalarExpression::Case {
                alternatives: parts
                    .alternatives
                    .into_iter()
                    .enumerate()
                    .map(|(index, (when, result))| {
                        Ok(ScalarCaseAlternative {
                            when,
                            then: static_list_case_result_quantifier_scalar_expression(
                                result,
                                predicate,
                                variable,
                                quantifier,
                                format!("{path}.collection.alternatives[{index}].then"),
                                mode,
                                context,
                            )?,
                        })
                    })
                    .collect::<Result<Vec<_>, CoreError>>()?,
                else_expression: parts
                    .default
                    .map(|result| {
                        static_list_case_result_quantifier_scalar_expression(
                            result,
                            predicate,
                            variable,
                            quantifier,
                            format!("{path}.collection.default"),
                            mode,
                            context,
                        )
                        .map(Box::new)
                    })
                    .transpose()?,
            }))
        }
        _ => Ok(None),
    }
}

pub(super) fn compile_optional_static_list_coalesce_quantifier_scalar_expression(
    collection: &Expression,
    predicate: Option<&Expression>,
    variable: &str,
    quantifier: StaticListQuantifier,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match collection {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_coalesce_quantifier_scalar_expression(
                inner, predicate, variable, quantifier, path, mode, context,
            )
        }
        Expression::FunctionCall(function) if is_coalesce_function(function) => {
            let Some(coalesce) = compile_optional_static_list_coalesce_arguments(
                function,
                format!("{path}.collection"),
                mode.static_metadata_plan(),
                context,
            )?
            else {
                return Ok(None);
            };
            static_list_coalesce_quantifier_scalar_expression(
                coalesce, predicate, variable, quantifier, path, mode, context,
            )
            .map(Some)
        }
        _ => Ok(None),
    }
}

pub(super) fn static_list_case_result_quantifier_scalar_expression(
    result: StaticListCaseResult,
    predicate: Option<&Expression>,
    variable: &str,
    quantifier: StaticListQuantifier,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    match result {
        StaticListCaseResult::Null => Ok(ScalarExpression::Literal(Literal::Null)),
        StaticListCaseResult::List(value) => {
            compile_static_list_quantifier_value_scalar_expression(
                value, predicate, variable, quantifier, path, mode, context,
            )
        }
        StaticListCaseResult::Coalesce(coalesce) => {
            static_list_coalesce_quantifier_scalar_expression(
                coalesce, predicate, variable, quantifier, path, mode, context,
            )
        }
    }
}

pub(super) fn static_list_coalesce_quantifier_scalar_expression(
    coalesce: StaticListCoalesceArguments,
    predicate: Option<&Expression>,
    variable: &str,
    quantifier: StaticListQuantifier,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let mut expression = ScalarExpression::Literal(Literal::Null);
    for (index, argument) in coalesce.arguments.into_iter().enumerate().rev() {
        let StaticListCoalesceArgument::List(mut value) = argument else {
            continue;
        };
        let presence_variable = value.presence_variable.take();
        let quantifier_expression = compile_static_list_quantifier_value_scalar_expression(
            value,
            predicate,
            variable,
            quantifier,
            format!("{path}.arguments[{index}]"),
            mode,
            context,
        )?;
        expression = match presence_variable {
            Some(variable) => ScalarExpression::Case {
                alternatives: vec![ScalarCaseAlternative {
                    when: PredicateExpression::Presence(PresencePredicate {
                        variable,
                        operator: ComparisonOperator::NotEqual,
                    }),
                    then: quantifier_expression,
                }],
                else_expression: Some(Box::new(expression)),
            },
            None => quantifier_expression,
        };
    }
    Ok(expression)
}

pub(super) fn compile_static_list_quantifier_value_scalar_expression(
    collection: StaticListValue,
    predicate: Option<&Expression>,
    variable: &str,
    quantifier: StaticListQuantifier,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Predicate(Box::new(
        compile_static_list_quantifier_value_predicate(
            collection, predicate, variable, quantifier, path, mode, context,
        )?,
    )))
}

pub(super) fn evaluate_static_filter_predicate(
    predicate: Option<&Expression>,
    evaluation: StaticFilterEvaluation<'_>,
    path: impl Into<String>,
) -> Result<StaticBooleanOutcome, CoreError> {
    let path = path.into();
    match predicate {
        Some(predicate) => evaluate_static_filter_predicate_expression(predicate, evaluation, path),
        None => static_boolean_outcome_from_literal(evaluation.item, path),
    }
}

pub(super) fn evaluate_static_filter_predicate_expression(
    expression: &Expression,
    evaluation: StaticFilterEvaluation<'_>,
    path: impl Into<String>,
) -> Result<StaticBooleanOutcome, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            evaluate_static_filter_predicate_expression(inner, evaluation, path)
        }
        Expression::BinaryOp {
            op:
                op @ (CypherBinaryOperator::And | CypherBinaryOperator::Or | CypherBinaryOperator::Xor),
            lhs,
            rhs,
            ..
        } => evaluate_static_filter_boolean_binary(*op, lhs, rhs, evaluation, path),
        Expression::UnaryOp {
            op: UnaryOperator::Not,
            operand,
            ..
        } => Ok(static_boolean_not(
            evaluate_static_filter_predicate_expression(
                operand,
                evaluation,
                format!("{path}.operand"),
            )?,
        )),
        Expression::Comparison { lhs, operators, .. } => {
            evaluate_static_filter_comparison(lhs, operators.as_slice(), evaluation, path)
        }
        Expression::In { lhs, rhs, .. } => {
            evaluate_static_filter_in_predicate(lhs, rhs, evaluation, path)
        }
        Expression::FunctionCall(function) if is_empty_function(function) => {
            evaluate_static_filter_is_empty(function, evaluation, path)
        }
        Expression::IsNull {
            operand, negated, ..
        } => {
            let literal = compile_static_filter_literal_operand(
                operand,
                evaluation,
                format!("{path}.operand"),
            )?;
            let is_null = matches!(literal, Literal::Null);
            Ok(StaticBooleanOutcome::from_bool(if *negated {
                !is_null
            } else {
                is_null
            }))
        }
        Expression::Literal(CypherLiteral::Boolean(value)) => {
            Ok(StaticBooleanOutcome::from_bool(*value))
        }
        Expression::Variable(variable_ref) => {
            let variable = variable_name(variable_ref);
            let literal = evaluation.literal_for_variable(&variable).ok_or_else(|| {
                unsupported(
                    path.clone(),
                    format!(
                        "collection predicate variable '{variable}' is not {}",
                        evaluation.expected_variable_message()
                    ),
                )
            })?;
            static_boolean_outcome_from_literal(literal, path)
        }
        _ => Err(unsupported(
            path,
            "collection predicate item predicates support the filter variable, literals, parameters, folded scalar expressions, comparisons, IN static lists, IS NULL, and AND/OR/XOR/NOT",
        )),
    }
}

pub(super) fn evaluate_static_filter_boolean_binary(
    operator: CypherBinaryOperator,
    lhs: &Expression,
    rhs: &Expression,
    evaluation: StaticFilterEvaluation<'_>,
    path: impl Into<String>,
) -> Result<StaticBooleanOutcome, CoreError> {
    let path = path.into();
    let left = evaluate_static_filter_predicate_expression(lhs, evaluation, format!("{path}.lhs"))?;
    let right =
        evaluate_static_filter_predicate_expression(rhs, evaluation, format!("{path}.rhs"))?;
    match operator {
        CypherBinaryOperator::And => Ok(static_boolean_and(left, right)),
        CypherBinaryOperator::Or => Ok(static_boolean_or(left, right)),
        CypherBinaryOperator::Xor => Ok(static_boolean_xor(left, right)),
        _ => unreachable!("non-boolean operator reached static filter boolean helper"),
    }
}

pub(super) fn evaluate_static_filter_comparison(
    lhs: &Expression,
    operators: &[(CypherComparisonOperator, Box<Expression>)],
    evaluation: StaticFilterEvaluation<'_>,
    path: impl Into<String>,
) -> Result<StaticBooleanOutcome, CoreError> {
    let path = path.into();
    if operators.is_empty() {
        return Err(unsupported(
            path,
            "collection predicate comparison must include an operator",
        ));
    }

    let mut expression = StaticBooleanOutcome::True;
    let mut current_lhs = lhs;
    for (index, (operator, rhs)) in operators.iter().enumerate() {
        let next = evaluate_static_filter_binary_comparison(
            current_lhs,
            compile_comparison_operator(*operator),
            rhs,
            evaluation,
            format!("{path}.operators[{index}]"),
        )?;
        expression = static_boolean_and(expression, next);
        current_lhs = rhs;
    }
    Ok(expression)
}

pub(super) fn evaluate_static_filter_binary_comparison(
    lhs: &Expression,
    operator: ComparisonOperator,
    rhs: &Expression,
    evaluation: StaticFilterEvaluation<'_>,
    path: impl Into<String>,
) -> Result<StaticBooleanOutcome, CoreError> {
    let path = path.into();
    let lhs = compile_static_filter_literal_operand(lhs, evaluation, format!("{path}.lhs"))?;
    let rhs = compile_static_filter_literal_operand(rhs, evaluation, format!("{path}.rhs"))?;
    evaluate_static_literal_comparison(&lhs, operator, &rhs, path)
}

pub(super) fn evaluate_static_filter_in_predicate(
    lhs: &Expression,
    rhs: &Expression,
    evaluation: StaticFilterEvaluation<'_>,
    path: impl Into<String>,
) -> Result<StaticBooleanOutcome, CoreError> {
    let path = path.into();
    let literal = compile_static_filter_literal_operand(lhs, evaluation, format!("{path}.lhs"))?;
    let Some(values) = compile_optional_static_list_value(
        rhs,
        format!("{path}.rhs"),
        evaluation.mode.static_metadata_plan(),
        evaluation.context,
    )?
    else {
        return Err(unsupported(
            format!("{path}.rhs"),
            "collection predicate IN right-hand sides require a literal list, list parameter, static split(...), range(...), tail(...), or static labels()/keys() metadata list",
        ));
    };
    evaluate_static_literal_in_list(&literal, &values.literals, path)
}

pub(super) fn evaluate_static_filter_is_empty(
    function: &FunctionInvocation,
    evaluation: StaticFilterEvaluation<'_>,
    path: impl Into<String>,
) -> Result<StaticBooleanOutcome, CoreError> {
    let path = path.into();
    if let [argument] = function.arguments.as_slice() {
        if let Some(value) = compile_optional_static_list_value(
            argument,
            format!("{path}.arguments[0]"),
            evaluation.mode.static_metadata_plan(),
            evaluation.context,
        )? {
            return Ok(StaticBooleanOutcome::from_bool(value.literals.is_empty()));
        }
        return evaluate_static_is_empty_literal(
            &compile_static_filter_literal_operand(
                argument,
                evaluation,
                format!("{path}.arguments[0]"),
            )?,
            format!("{path}.arguments[0]"),
        );
    }

    let literal = evaluate_static_map_single_function_argument(
        function,
        path.clone(),
        evaluation,
        "isEmpty",
    )?;
    evaluate_static_is_empty_literal(&literal, format!("{path}.arguments[0]"))
}

pub(super) fn compile_static_filter_literal_operand(
    expression: &Expression,
    evaluation: StaticFilterEvaluation<'_>,
    path: impl Into<String>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_static_filter_literal_operand(inner, evaluation, path)
        }
        Expression::Variable(variable_ref) => {
            let variable = variable_name(variable_ref);
            evaluation
                .literal_for_variable(&variable)
                .cloned()
                .ok_or_else(|| {
                    unsupported(
                        path,
                        format!(
                            "collection predicate variable '{variable}' is not {}",
                            evaluation.expected_variable_message()
                        ),
                    )
                })
        }
        Expression::UnaryOp {
            op: UnaryOperator::Negate,
            ..
        }
        | Expression::BinaryOp {
            op:
                CypherBinaryOperator::Add
                | CypherBinaryOperator::Subtract
                | CypherBinaryOperator::Multiply
                | CypherBinaryOperator::Divide
                | CypherBinaryOperator::Modulo
                | CypherBinaryOperator::Power,
            ..
        } => evaluate_static_map_expression(expression, evaluation, path),
        Expression::FunctionCall(function) if is_static_map_operand_function(function) => {
            evaluate_static_map_expression(expression, evaluation, path)
        }
        _ => {
            compile_predicate_literal_in_mode(expression, path, evaluation.mode, evaluation.context)
        }
    }
}

pub(super) fn is_static_map_operand_function(function: &FunctionInvocation) -> bool {
    is_coalesce_function(function)
        || is_null_if_function(function)
        || is_character_length_function(function)
        || is_empty_function(function)
        || is_static_map_cast_function(function)
        || is_static_map_numeric_function(function)
        || is_is_nan_function(function)
        || static_map_string_function_returns_string(function)
}

pub(super) fn is_static_map_cast_function(function: &FunctionInvocation) -> bool {
    is_to_string_function(function)
        || is_to_string_or_null_function(function)
        || is_to_integer_function(function)
        || is_to_integer_or_null_function(function)
        || is_to_float_function(function)
        || is_to_float_or_null_function(function)
        || is_to_boolean_function(function)
        || is_to_boolean_or_null_function(function)
}

pub(super) fn is_static_map_numeric_function(function: &FunctionInvocation) -> bool {
    is_abs_function(function)
        || is_ceil_function(function)
        || is_floor_function(function)
        || is_round_function(function)
        || is_sqrt_function(function)
        || is_sign_function(function)
}

pub(super) fn static_boolean_outcome_predicate(
    outcome: StaticBooleanOutcome,
    presence_variables: Vec<String>,
) -> PredicateExpression {
    match outcome {
        StaticBooleanOutcome::True if presence_variables.is_empty() => {
            PredicateExpression::Boolean(true)
        }
        StaticBooleanOutcome::False if presence_variables.is_empty() => {
            PredicateExpression::Boolean(false)
        }
        StaticBooleanOutcome::True => {
            presence_gated_boolean_predicate_for_variables(presence_variables, true)
        }
        StaticBooleanOutcome::False => {
            presence_gated_boolean_predicate_for_variables(presence_variables, false)
        }
        StaticBooleanOutcome::Unknown => unknown_boolean_predicate(),
    }
}

pub(super) fn unknown_boolean_predicate() -> PredicateExpression {
    PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: ScalarExpression::Literal(Literal::Null),
        operator: ComparisonOperator::Equal,
        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Boolean(true))),
    })
}

pub(super) fn static_boolean_outcome_from_literal(
    literal: &Literal,
    path: impl Into<String>,
) -> Result<StaticBooleanOutcome, CoreError> {
    match literal {
        Literal::Boolean(value) => Ok(StaticBooleanOutcome::from_bool(*value)),
        Literal::Null => Ok(StaticBooleanOutcome::Unknown),
        _ => Err(unsupported(
            path,
            "collection predicate truthiness requires boolean list elements",
        )),
    }
}

impl StaticBooleanOutcome {
    pub(super) fn from_bool(value: bool) -> Self {
        if value { Self::True } else { Self::False }
    }
}

pub(super) fn static_boolean_not(value: StaticBooleanOutcome) -> StaticBooleanOutcome {
    match value {
        StaticBooleanOutcome::True => StaticBooleanOutcome::False,
        StaticBooleanOutcome::False => StaticBooleanOutcome::True,
        StaticBooleanOutcome::Unknown => StaticBooleanOutcome::Unknown,
    }
}

pub(super) fn static_boolean_and(
    left: StaticBooleanOutcome,
    right: StaticBooleanOutcome,
) -> StaticBooleanOutcome {
    match (left, right) {
        (StaticBooleanOutcome::False, _) | (_, StaticBooleanOutcome::False) => {
            StaticBooleanOutcome::False
        }
        (StaticBooleanOutcome::Unknown, _) | (_, StaticBooleanOutcome::Unknown) => {
            StaticBooleanOutcome::Unknown
        }
        (StaticBooleanOutcome::True, StaticBooleanOutcome::True) => StaticBooleanOutcome::True,
    }
}

pub(super) fn static_boolean_or(
    left: StaticBooleanOutcome,
    right: StaticBooleanOutcome,
) -> StaticBooleanOutcome {
    match (left, right) {
        (StaticBooleanOutcome::True, _) | (_, StaticBooleanOutcome::True) => {
            StaticBooleanOutcome::True
        }
        (StaticBooleanOutcome::Unknown, _) | (_, StaticBooleanOutcome::Unknown) => {
            StaticBooleanOutcome::Unknown
        }
        (StaticBooleanOutcome::False, StaticBooleanOutcome::False) => StaticBooleanOutcome::False,
    }
}

pub(super) fn static_boolean_xor(
    left: StaticBooleanOutcome,
    right: StaticBooleanOutcome,
) -> StaticBooleanOutcome {
    match (left, right) {
        (StaticBooleanOutcome::Unknown, _) | (_, StaticBooleanOutcome::Unknown) => {
            StaticBooleanOutcome::Unknown
        }
        (StaticBooleanOutcome::True, StaticBooleanOutcome::False)
        | (StaticBooleanOutcome::False, StaticBooleanOutcome::True) => StaticBooleanOutcome::True,
        (StaticBooleanOutcome::True, StaticBooleanOutcome::True)
        | (StaticBooleanOutcome::False, StaticBooleanOutcome::False) => StaticBooleanOutcome::False,
    }
}

pub(super) fn validate_ordered_static_list_element_family(
    actual: &StaticListValue,
    expected: &StaticListValue,
    path: &str,
) -> Result<(), CoreError> {
    let mut family = None;
    for element_type in [actual.element_type, expected.element_type]
        .into_iter()
        .flatten()
    {
        merge_ordered_static_list_element_family(
            &mut family,
            static_list_element_family(element_type, path)?,
            path,
        )?;
    }
    for literal in actual.literals.iter().chain(expected.literals.iter()) {
        if let Some(next) = literal_static_list_element_family(literal, path)? {
            merge_ordered_static_list_element_family(&mut family, next, path)?;
        }
    }
    Ok(())
}

pub(super) fn static_list_element_family(
    element_type: LiteralListElementType,
    path: &str,
) -> Result<StaticListElementFamily, CoreError> {
    match element_type {
        LiteralListElementType::String => Ok(StaticListElementFamily::String),
        LiteralListElementType::Integer | LiteralListElementType::Float => {
            Ok(StaticListElementFamily::Numeric)
        }
        LiteralListElementType::Boolean => Err(unsupported(
            path.to_string(),
            "ordered static list predicates require string or numeric list elements",
        )),
        LiteralListElementType::StringList
        | LiteralListElementType::IntegerList
        | LiteralListElementType::FloatList
        | LiteralListElementType::BooleanList => Err(unsupported(
            path.to_string(),
            "ordered static list predicates require string or numeric scalar list elements",
        )),
    }
}

pub(super) fn literal_static_list_element_family(
    literal: &Literal,
    path: &str,
) -> Result<Option<StaticListElementFamily>, CoreError> {
    match literal {
        Literal::String(_) => Ok(Some(StaticListElementFamily::String)),
        Literal::Integer(_) | Literal::Float(_) => Ok(Some(StaticListElementFamily::Numeric)),
        Literal::Null => Ok(None),
        Literal::Boolean(_) | Literal::List(_) => Err(unsupported(
            path.to_string(),
            "ordered static list predicates require string or numeric scalar list elements",
        )),
    }
}

pub(super) fn merge_ordered_static_list_element_family(
    family: &mut Option<StaticListElementFamily>,
    next: StaticListElementFamily,
    path: &str,
) -> Result<(), CoreError> {
    match family {
        Some(current) if *current != next => Err(unsupported(
            path.to_string(),
            "ordered static list predicates require both lists to use the same orderable element family",
        )),
        Some(_) => Ok(()),
        None => {
            *family = Some(next);
            Ok(())
        }
    }
}

pub(super) fn compare_static_literal_lists(
    actual: &[Literal],
    expected: &[Literal],
    path: &str,
) -> Result<StaticListOrderingOutcome, CoreError> {
    for (index, (actual, expected)) in actual.iter().zip(expected.iter()).enumerate() {
        match compare_static_list_literals(actual, expected, format!("{path}[{index}]"))? {
            StaticListOrderingOutcome::Known(Ordering::Equal) => {}
            outcome => return Ok(outcome),
        }
    }
    Ok(StaticListOrderingOutcome::Known(
        actual.len().cmp(&expected.len()),
    ))
}

pub(super) fn compare_static_list_literals(
    actual: &Literal,
    expected: &Literal,
    path: impl Into<String>,
) -> Result<StaticListOrderingOutcome, CoreError> {
    let path = path.into();
    if matches!(actual, Literal::Null) || matches!(expected, Literal::Null) {
        return Ok(StaticListOrderingOutcome::Unknown);
    }
    if let Some(ordering) = compare_numeric_literals(actual, expected, path.clone())? {
        return Ok(StaticListOrderingOutcome::Known(ordering));
    }
    match (actual, expected) {
        (Literal::String(actual), Literal::String(expected)) => {
            Ok(StaticListOrderingOutcome::Known(actual.cmp(expected)))
        }
        _ => Err(unsupported(
            path,
            "ordered static list predicates require comparable string or numeric elements",
        )),
    }
}

pub(super) fn evaluate_ordering_comparison(
    ordering: Ordering,
    operator: ComparisonOperator,
) -> bool {
    match operator {
        ComparisonOperator::GreaterThan => ordering == Ordering::Greater,
        ComparisonOperator::GreaterThanOrEqual => {
            matches!(ordering, Ordering::Greater | Ordering::Equal)
        }
        ComparisonOperator::LessThan => ordering == Ordering::Less,
        ComparisonOperator::LessThanOrEqual => {
            matches!(ordering, Ordering::Less | Ordering::Equal)
        }
        _ => unreachable!("non-ordered operator reached ordered list comparison helper"),
    }
}

pub(super) fn is_parameter_expression(expression: &Expression) -> bool {
    match expression {
        Expression::Parenthesized(inner) => is_parameter_expression(inner),
        Expression::Parameter(_) => true,
        _ => false,
    }
}

pub(super) fn compile_optional_metadata_list_index_scalar_expression(
    expression: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_optional_metadata_list_index_scalar_expression(inner, path, plan, context)
        }
        Expression::ListIndex { list, index, .. } => {
            let Some(value) =
                compile_optional_metadata_list_value(list, format!("{path}.list"), plan, context)?
            else {
                return Ok(None);
            };
            let literal = compile_list_index_literal(
                &value.literals,
                index,
                &path,
                context,
                "metadata list indexes require an integer literal or scalar integer parameter",
            )?;
            Ok(Some(presence_gate_scalar_expression(
                value.presence_variable,
                ScalarExpression::Literal(literal),
            )))
        }
        _ => Ok(None),
    }
}

pub(super) fn compile_optional_non_literal_static_list_index_scalar_expression(
    expression: &Expression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    let Expression::ListIndex { list, .. } = expression else {
        return Ok(None);
    };
    if is_literal_list_source_expression(list) {
        return Ok(None);
    }
    compile_optional_static_list_index_scalar_expression(expression, path, plan, context)
}

pub(super) fn compile_optional_static_list_index_scalar_expression(
    expression: &Expression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_index_scalar_expression(inner, path, plan, context)
        }
        Expression::ListIndex { list, index, .. } => {
            if let Some(expression) = compile_optional_static_list_slice_index_scalar_expression(
                list,
                index,
                path.clone(),
                plan,
                context,
            )? {
                return Ok(Some(expression));
            }
            if let Some(expression) = compile_optional_static_list_case_index_scalar_expression(
                list,
                index,
                path.clone(),
                plan,
                context,
            )? {
                return Ok(Some(expression));
            }
            if let Some(expression) = compile_optional_static_list_coalesce_index_scalar_expression(
                list,
                index,
                path.clone(),
                plan,
                context,
            )? {
                return Ok(Some(expression));
            }
            let Some(value) =
                compile_optional_static_list_value(list, format!("{path}.list"), plan, context)?
            else {
                return Ok(None);
            };
            static_list_value_index_scalar_expression(value, index, path, context).map(Some)
        }
        _ => Ok(None),
    }
}

pub(super) fn compile_optional_static_list_slice_index_scalar_expression(
    list: &Expression,
    index: &Expression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match list {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_slice_index_scalar_expression(
                inner, index, path, plan, context,
            )
        }
        Expression::ListSlice {
            list, start, end, ..
        } => {
            if let Some(expression) =
                compile_optional_static_list_case_slice_index_scalar_expression(
                    list,
                    start.as_deref(),
                    end.as_deref(),
                    index,
                    path.clone(),
                    PredicateCompileMode::CaseWhen { plan },
                    context,
                )?
            {
                return Ok(Some(expression));
            }
            compile_optional_static_list_coalesce_slice_index_scalar_expression(
                list,
                start.as_deref(),
                end.as_deref(),
                index,
                path,
                plan,
                context,
            )
        }
        _ => Ok(None),
    }
}

pub(super) fn compile_optional_static_list_case_slice_index_scalar_expression(
    list: &Expression,
    start: Option<&Expression>,
    end: Option<&Expression>,
    index: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match list {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_case_slice_index_scalar_expression(
                inner, start, end, index, path, mode, context,
            )
        }
        Expression::Case(case) => {
            let Some(parts) = compile_optional_static_list_case_parts(
                case,
                format!("{path}.list"),
                mode,
                context,
            )?
            else {
                return Ok(None);
            };
            Ok(Some(ScalarExpression::Case {
                alternatives: parts
                    .alternatives
                    .into_iter()
                    .enumerate()
                    .map(|(alternative_index, (when, result))| {
                        Ok(ScalarCaseAlternative {
                            when,
                            then: static_list_case_result_slice_index_scalar_expression(
                                result,
                                start,
                                end,
                                index,
                                format!("{path}.list.alternatives[{alternative_index}].then"),
                                context,
                            )?,
                        })
                    })
                    .collect::<Result<Vec<_>, CoreError>>()?,
                else_expression: parts
                    .default
                    .map(|result| {
                        static_list_case_result_slice_index_scalar_expression(
                            result,
                            start,
                            end,
                            index,
                            format!("{path}.list.default"),
                            context,
                        )
                        .map(Box::new)
                    })
                    .transpose()?,
            }))
        }
        _ => Ok(None),
    }
}

pub(super) fn compile_optional_static_list_coalesce_slice_index_scalar_expression(
    list: &Expression,
    start: Option<&Expression>,
    end: Option<&Expression>,
    index: &Expression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match list {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_coalesce_slice_index_scalar_expression(
                inner, start, end, index, path, plan, context,
            )
        }
        Expression::FunctionCall(function) if is_coalesce_function(function) => {
            let Some(coalesce) = compile_optional_static_list_coalesce_arguments(
                function,
                format!("{path}.list"),
                plan,
                context,
            )?
            else {
                return Ok(None);
            };
            static_list_coalesce_slice_index_scalar_expression(
                coalesce, start, end, index, path, context,
            )
            .map(Some)
        }
        _ => Ok(None),
    }
}

pub(super) fn static_list_case_result_slice_index_scalar_expression(
    result: StaticListCaseResult,
    start: Option<&Expression>,
    end: Option<&Expression>,
    index: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    match result {
        StaticListCaseResult::Null => Ok(ScalarExpression::Literal(Literal::Null)),
        StaticListCaseResult::List(value) => {
            static_list_value_slice_index_scalar_expression(value, start, end, index, path, context)
        }
        StaticListCaseResult::Coalesce(coalesce) => {
            static_list_coalesce_slice_index_scalar_expression(
                coalesce, start, end, index, path, context,
            )
        }
    }
}

pub(super) fn static_list_coalesce_slice_index_scalar_expression(
    coalesce: StaticListCoalesceArguments,
    start: Option<&Expression>,
    end: Option<&Expression>,
    index: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let mut expression = ScalarExpression::Literal(Literal::Null);
    for (argument_index, argument) in coalesce.arguments.into_iter().enumerate().rev() {
        let StaticListCoalesceArgument::List(mut value) = argument else {
            continue;
        };
        let presence_variable = value.presence_variable.take();
        let indexed = static_list_value_slice_index_scalar_expression(
            value,
            start,
            end,
            index,
            format!("{path}.arguments[{argument_index}]"),
            context,
        )?;
        expression = match presence_variable {
            Some(variable) => ScalarExpression::Case {
                alternatives: vec![ScalarCaseAlternative {
                    when: PredicateExpression::Presence(PresencePredicate {
                        variable,
                        operator: ComparisonOperator::NotEqual,
                    }),
                    then: indexed,
                }],
                else_expression: Some(Box::new(expression)),
            },
            None => indexed,
        };
    }
    Ok(expression)
}

pub(super) fn static_list_value_slice_index_scalar_expression(
    value: StaticListValue,
    start: Option<&Expression>,
    end: Option<&Expression>,
    index: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let value = slice_static_list_value(value, start, end, format!("{path}.slice"), context)?;
    static_list_value_index_scalar_expression(value, index, format!("{path}.index"), context)
}

pub(super) fn compile_optional_static_list_case_index_scalar_expression(
    list: &Expression,
    index: &Expression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match list {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_case_index_scalar_expression(
                inner, index, path, plan, context,
            )
        }
        Expression::Case(case) => {
            let Some(parts) = compile_optional_static_list_case_parts(
                case,
                format!("{path}.list"),
                PredicateCompileMode::CaseWhen { plan },
                context,
            )?
            else {
                return Ok(None);
            };
            Ok(Some(ScalarExpression::Case {
                alternatives: parts
                    .alternatives
                    .into_iter()
                    .enumerate()
                    .map(|(alternative_index, (when, result))| {
                        Ok(ScalarCaseAlternative {
                            when,
                            then: static_list_case_result_index_scalar_expression(
                                result,
                                index,
                                format!("{path}.list.alternatives[{alternative_index}].then"),
                                context,
                            )?,
                        })
                    })
                    .collect::<Result<Vec<_>, CoreError>>()?,
                else_expression: parts
                    .default
                    .map(|result| {
                        static_list_case_result_index_scalar_expression(
                            result,
                            index,
                            format!("{path}.list.default"),
                            context,
                        )
                        .map(Box::new)
                    })
                    .transpose()?,
            }))
        }
        _ => Ok(None),
    }
}

pub(super) fn compile_optional_static_list_coalesce_index_scalar_expression(
    list: &Expression,
    index: &Expression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match list {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_coalesce_index_scalar_expression(
                inner, index, path, plan, context,
            )
        }
        Expression::FunctionCall(function) if is_coalesce_function(function) => {
            let Some(coalesce) = compile_optional_static_list_coalesce_arguments(
                function,
                format!("{path}.list"),
                plan,
                context,
            )?
            else {
                return Ok(None);
            };
            static_list_coalesce_index_scalar_expression(coalesce, index, path, context).map(Some)
        }
        _ => Ok(None),
    }
}

pub(super) fn static_list_case_result_index_scalar_expression(
    result: StaticListCaseResult,
    index: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    match result {
        StaticListCaseResult::Null => Ok(ScalarExpression::Literal(Literal::Null)),
        StaticListCaseResult::List(value) => {
            static_list_value_index_scalar_expression(value, index, path, context)
        }
        StaticListCaseResult::Coalesce(coalesce) => {
            static_list_coalesce_index_scalar_expression(coalesce, index, path, context)
        }
    }
}

pub(super) fn static_list_coalesce_index_scalar_expression(
    coalesce: StaticListCoalesceArguments,
    index: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let mut expression = ScalarExpression::Literal(Literal::Null);
    for (argument_index, argument) in coalesce.arguments.into_iter().enumerate().rev() {
        let StaticListCoalesceArgument::List(mut value) = argument else {
            continue;
        };
        let presence_variable = value.presence_variable.take();
        let indexed = static_list_value_index_scalar_expression(
            value,
            index,
            format!("{path}.arguments[{argument_index}]"),
            context,
        )?;
        expression = match presence_variable {
            Some(variable) => ScalarExpression::Case {
                alternatives: vec![ScalarCaseAlternative {
                    when: PredicateExpression::Presence(PresencePredicate {
                        variable,
                        operator: ComparisonOperator::NotEqual,
                    }),
                    then: indexed,
                }],
                else_expression: Some(Box::new(expression)),
            },
            None => indexed,
        };
    }
    Ok(expression)
}

pub(super) fn static_list_value_index_scalar_expression(
    value: StaticListValue,
    index: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let literal = compile_list_index_literal(
        &value.literals,
        index,
        path,
        context,
        "static list indexes require an integer literal or scalar integer parameter",
    )?;
    Ok(presence_gate_scalar_expression(
        value.presence_variable,
        ScalarExpression::Literal(literal),
    ))
}

pub(super) fn is_literal_list_source_expression(expression: &Expression) -> bool {
    match expression {
        Expression::Parenthesized(inner) => is_literal_list_source_expression(inner),
        Expression::Literal(CypherLiteral::List(_)) | Expression::Parameter(_) => true,
        Expression::ListSlice { list, .. } => is_literal_list_source_expression(list),
        _ => false,
    }
}

pub(super) fn is_literal_list_value_expression(expression: &Expression) -> bool {
    match expression {
        Expression::Parenthesized(inner) => is_literal_list_value_expression(inner),
        Expression::Literal(CypherLiteral::List(_)) => true,
        _ => false,
    }
}

pub(super) fn compile_optional_scalar_binary_comparison(
    lhs: &Expression,
    operator: ComparisonOperator,
    rhs: &Expression,
    path: &str,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<PredicateExpression>, CoreError> {
    if let Some(lhs) =
        compile_optional_path_length_scalar_expression(lhs, format!("{path}.lhs"), mode, context)?
    {
        return Ok(Some(PredicateExpression::ScalarComparison(
            ScalarPredicate {
                lhs,
                operator,
                rhs: compile_scalar_predicate_rhs(rhs, format!("{path}.rhs"), mode, context)?,
            },
        )));
    }
    if let Some(lhs) =
        compile_optional_predicate_scalar_expression(lhs, format!("{path}.lhs"), mode, context)?
    {
        return Ok(Some(PredicateExpression::ScalarComparison(
            ScalarPredicate {
                lhs,
                operator,
                rhs: compile_scalar_predicate_rhs(rhs, format!("{path}.rhs"), mode, context)?,
            },
        )));
    }
    if let Some(rhs) =
        compile_optional_path_length_scalar_expression(rhs, format!("{path}.rhs"), mode, context)?
    {
        return Ok(Some(PredicateExpression::ScalarComparison(
            ScalarPredicate {
                lhs: rhs,
                operator: invert_comparison_operator(operator, format!("{path}.operator"))?,
                rhs: compile_scalar_predicate_rhs(lhs, format!("{path}.lhs"), mode, context)?,
            },
        )));
    }
    let Some(rhs) =
        compile_optional_predicate_scalar_expression(rhs, format!("{path}.rhs"), mode, context)?
    else {
        return Ok(None);
    };
    Ok(Some(PredicateExpression::ScalarComparison(
        ScalarPredicate {
            lhs: rhs,
            operator: invert_comparison_operator(operator, format!("{path}.operator"))?,
            rhs: compile_scalar_predicate_rhs(lhs, format!("{path}.lhs"), mode, context)?,
        },
    )))
}

pub(super) fn compile_left_property_comparison(
    property: PropertyRef,
    operator: ComparisonOperator,
    rhs: &Expression,
    path: &str,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<PredicateExpression, CoreError> {
    if let Some(rhs) =
        compile_optional_path_length_scalar_expression(rhs, format!("{path}.rhs"), mode, context)?
    {
        return Ok(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Property(property),
            operator,
            rhs: ScalarPredicateRhs::Expression(rhs),
        }));
    }
    if let Some(predicate) =
        compile_dynamic_string_property_predicate(&property, operator, rhs, path, mode, context)?
    {
        return Ok(predicate);
    }
    if let Some(predicate) =
        compile_dynamic_scalar_property_predicate(&property, operator, rhs, path, mode, context)?
    {
        return Ok(predicate);
    }
    Ok(PredicateExpression::Comparison(PropertyPredicate {
        property,
        operator,
        rhs: compile_predicate_rhs(rhs, format!("{path}.rhs"), mode, context)?,
    }))
}

pub(super) fn compile_dynamic_string_property_predicate(
    property: &PropertyRef,
    operator: ComparisonOperator,
    rhs: &Expression,
    path: &str,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<PredicateExpression>, CoreError> {
    if !is_string_comparison_operator(operator) || is_literal_expression(rhs) {
        return Ok(None);
    }

    let Some(rhs) =
        compile_optional_predicate_scalar_expression(rhs, format!("{path}.rhs"), mode, context)?
    else {
        return Ok(None);
    };

    Ok(Some(PredicateExpression::ScalarComparison(
        ScalarPredicate {
            lhs: ScalarExpression::Property(property.clone()),
            operator,
            rhs: ScalarPredicateRhs::Expression(rhs),
        },
    )))
}

pub(super) fn compile_dynamic_scalar_property_predicate(
    property: &PropertyRef,
    operator: ComparisonOperator,
    rhs: &Expression,
    path: &str,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<PredicateExpression>, CoreError> {
    if is_string_comparison_operator(operator) || is_literal_expression(rhs) {
        return Ok(None);
    }

    let Some(rhs) =
        compile_optional_predicate_scalar_expression(rhs, format!("{path}.rhs"), mode, context)?
    else {
        return Ok(None);
    };

    Ok(Some(PredicateExpression::ScalarComparison(
        ScalarPredicate {
            lhs: ScalarExpression::Property(property.clone()),
            operator,
            rhs: ScalarPredicateRhs::Expression(rhs),
        },
    )))
}

pub(super) fn compile_in_predicate(
    lhs: &Expression,
    rhs: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<PredicateExpression, CoreError> {
    let path = path.into();
    if is_literal_list_value_expression(lhs) {
        return Err(unsupported(
            format!("{path}.lhs"),
            "only string, numeric, boolean, and null literals are supported",
        ));
    }
    if let Some(plan) = mode.graph_plan() {
        if let Some(predicate) =
            compile_label_membership_predicate(lhs, rhs, path.clone(), plan, context)?
        {
            return Ok(predicate);
        }
        if let Some(predicate) =
            compile_property_key_membership_predicate(lhs, rhs, path.clone(), plan, context)?
        {
            return Ok(predicate);
        }
    }
    if let Some(predicate) =
        compile_optional_static_list_case_in_predicate(lhs, rhs, path.clone(), mode, context)?
    {
        return Ok(predicate);
    }
    if let Some(predicate) = compile_optional_static_list_comprehension_in_predicate(
        lhs,
        rhs,
        path.clone(),
        mode,
        context,
    )? {
        return Ok(predicate);
    }
    if let Some(predicate) =
        compile_optional_static_list_slice_in_predicate(lhs, rhs, path.clone(), mode, context)?
    {
        return Ok(predicate);
    }
    if let Some(predicate) =
        compile_optional_static_list_coalesce_in_predicate(lhs, rhs, path.clone(), mode, context)?
    {
        return Ok(predicate);
    }
    let rhs_value = compile_static_list_in_rhs_value(
        rhs,
        format!("{path}.rhs"),
        mode.static_metadata_plan(),
        context,
    )?;
    compile_static_list_value_in_predicate(lhs, rhs_value, path, mode, context)
}

pub(super) fn compile_optional_static_list_comprehension_in_predicate(
    lhs: &Expression,
    rhs: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<PredicateExpression>, CoreError> {
    let path = path.into();
    match rhs {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_comprehension_in_predicate(lhs, inner, path, mode, context)
        }
        Expression::ListComprehension(comprehension) => {
            let Some(expression) = compile_optional_static_list_comprehension_in_scalar_expression(
                lhs,
                comprehension,
                path,
                mode,
                context,
            )?
            else {
                return Ok(None);
            };
            Ok(Some(boolean_scalar_expression_predicate(expression)))
        }
        _ => Ok(None),
    }
}

pub(super) fn compile_optional_static_list_comprehension_in_scalar_expression(
    lhs: &Expression,
    comprehension: &ListComprehension,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    let source = context
        .list_comprehension_source(comprehension)
        .ok_or_else(|| {
            unsupported(
                path.clone(),
                "list comprehensions require a recoverable `variable IN collection` source",
            )
        })?;
    let variable = variable_name(&comprehension.variable);
    if source.variable != variable {
        return Err(unsupported(
            path,
            "list comprehension variable recovery did not match the parsed AST",
        ));
    }
    let map = if source.has_map {
        Some(comprehension.map.as_ref().ok_or_else(|| {
            unsupported(
                format!("{path}.map"),
                "mapped static list comprehensions require a recoverable map expression",
            )
        })?)
    } else {
        None
    };
    let recovered_filter =
        recover_static_list_comprehension_filter(comprehension, source, &path, context)?;
    let filter = comprehension
        .filter
        .as_deref()
        .or_else(|| recovered_filter.as_ref().map(|(filter, _)| filter));
    let filter_context = recovered_filter
        .as_ref()
        .map_or(context, |(_, filter_context)| filter_context);
    let (collection_expression, collection_context) = parse_cypher_expression_fragment(
        &source.collection_source,
        format!("{path}.rhs.collection"),
        context,
    )?;
    let evaluation = StaticListComprehensionEvaluation {
        variable: &variable,
        filter,
        filter_context,
        map,
        map_context: context,
        mode,
    };
    compile_optional_static_list_comprehension_source_in_scalar_expression(
        lhs,
        &collection_expression,
        path,
        evaluation,
        &collection_context,
        mode,
        context,
    )
}

pub(super) fn compile_optional_static_list_comprehension_source_in_scalar_expression(
    lhs: &Expression,
    collection: &Expression,
    path: impl Into<String>,
    evaluation: StaticListComprehensionEvaluation<'_>,
    collection_context: &CypherCompileContext,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match collection {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_comprehension_source_in_scalar_expression(
                lhs,
                inner,
                path,
                evaluation,
                collection_context,
                mode,
                context,
            )
        }
        Expression::Case(case) => {
            compile_optional_static_list_case_comprehension_in_scalar_expression(
                lhs,
                case,
                path,
                evaluation,
                collection_context,
                mode,
                context,
            )
        }
        Expression::FunctionCall(function) if is_coalesce_function(function) => {
            compile_optional_static_list_coalesce_comprehension_in_scalar_expression(
                lhs,
                function,
                path,
                evaluation,
                collection_context,
                mode,
                context,
            )
        }
        Expression::ListSlice {
            list, start, end, ..
        } => {
            let compile = StaticListComprehensionInCompile {
                lhs,
                evaluation,
                collection_context,
                mode,
                context,
            };
            compile_optional_static_list_slice_comprehension_in_scalar_expression(
                list,
                StaticListSliceBounds {
                    start: start.as_deref(),
                    end: end.as_deref(),
                },
                path,
                compile,
            )
        }
        _ => Ok(None),
    }
}

#[derive(Clone, Copy)]
pub(super) struct StaticListComprehensionInCompile<'a> {
    lhs: &'a Expression,
    evaluation: StaticListComprehensionEvaluation<'a>,
    collection_context: &'a CypherCompileContext,
    mode: PredicateCompileMode<'a>,
    context: &'a CypherCompileContext,
}

pub(super) fn compile_optional_static_list_slice_comprehension_in_scalar_expression(
    list: &Expression,
    bounds: StaticListSliceBounds<'_>,
    path: impl Into<String>,
    compile: StaticListComprehensionInCompile<'_>,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match list {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_slice_comprehension_in_scalar_expression(
                inner, bounds, path, compile,
            )
        }
        Expression::Case(case) => {
            compile_optional_static_list_case_slice_comprehension_in_scalar_expression(
                case, bounds, path, compile,
            )
        }
        Expression::FunctionCall(function) if is_coalesce_function(function) => {
            compile_optional_static_list_coalesce_slice_comprehension_in_scalar_expression(
                function, bounds, path, compile,
            )
        }
        _ => Ok(None),
    }
}

pub(super) fn compile_optional_static_list_case_slice_comprehension_in_scalar_expression(
    case: &CaseExpression,
    bounds: StaticListSliceBounds<'_>,
    path: impl Into<String>,
    compile: StaticListComprehensionInCompile<'_>,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    let Some(parts) = compile_optional_static_list_case_parts(
        case,
        format!("{path}.rhs.list"),
        compile.evaluation.mode,
        compile.collection_context,
    )?
    else {
        return Ok(None);
    };
    Ok(Some(ScalarExpression::Case {
        alternatives: parts
            .alternatives
            .into_iter()
            .enumerate()
            .map(|(index, (when, result))| {
                let result = static_list_case_result_slice_comprehension_result(
                    result,
                    bounds,
                    format!("{path}.rhs.alternatives[{index}].then"),
                    compile.evaluation,
                    compile.collection_context,
                )?;
                Ok(ScalarCaseAlternative {
                    when,
                    then: static_list_case_result_in_scalar_expression(
                        compile.lhs,
                        result,
                        format!("{path}.rhs.alternatives[{index}].then"),
                        compile.mode,
                        compile.context,
                    )?,
                })
            })
            .collect::<Result<Vec<_>, CoreError>>()?,
        else_expression: parts
            .default
            .map(|result| {
                let result = static_list_case_result_slice_comprehension_result(
                    result,
                    bounds,
                    format!("{path}.rhs.default"),
                    compile.evaluation,
                    compile.collection_context,
                )?;
                static_list_case_result_in_scalar_expression(
                    compile.lhs,
                    result,
                    format!("{path}.rhs.default"),
                    compile.mode,
                    compile.context,
                )
                .map(Box::new)
            })
            .transpose()?,
    }))
}

pub(super) fn compile_optional_static_list_coalesce_slice_comprehension_in_scalar_expression(
    function: &FunctionInvocation,
    bounds: StaticListSliceBounds<'_>,
    path: impl Into<String>,
    compile: StaticListComprehensionInCompile<'_>,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    let Some(coalesce) = compile_optional_static_list_coalesce_arguments(
        function,
        format!("{path}.rhs.list"),
        compile.evaluation.mode.static_metadata_plan(),
        compile.collection_context,
    )?
    else {
        return Ok(None);
    };
    let coalesce = static_list_coalesce_slice_comprehension_arguments(
        coalesce,
        bounds,
        path.clone(),
        compile.evaluation,
        compile.collection_context,
    )?;
    static_list_coalesce_in_scalar_expression(
        compile.lhs,
        coalesce,
        path,
        compile.mode,
        compile.context,
    )
    .map(Some)
}

pub(super) fn compile_optional_static_list_case_comprehension_in_scalar_expression(
    lhs: &Expression,
    case: &CaseExpression,
    path: impl Into<String>,
    evaluation: StaticListComprehensionEvaluation<'_>,
    collection_context: &CypherCompileContext,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    let Some(parts) = compile_optional_static_list_case_parts(
        case,
        format!("{path}.rhs"),
        evaluation.mode,
        collection_context,
    )?
    else {
        return Ok(None);
    };
    Ok(Some(ScalarExpression::Case {
        alternatives: parts
            .alternatives
            .into_iter()
            .enumerate()
            .map(|(index, (when, result))| {
                let result = static_list_case_result_comprehension_result(
                    result,
                    format!("{path}.rhs.alternatives[{index}].then"),
                    evaluation,
                )?;
                Ok(ScalarCaseAlternative {
                    when,
                    then: static_list_case_result_in_scalar_expression(
                        lhs,
                        result,
                        format!("{path}.rhs.alternatives[{index}].then"),
                        mode,
                        context,
                    )?,
                })
            })
            .collect::<Result<Vec<_>, CoreError>>()?,
        else_expression: parts
            .default
            .map(|result| {
                let result = static_list_case_result_comprehension_result(
                    result,
                    format!("{path}.rhs.default"),
                    evaluation,
                )?;
                static_list_case_result_in_scalar_expression(
                    lhs,
                    result,
                    format!("{path}.rhs.default"),
                    mode,
                    context,
                )
                .map(Box::new)
            })
            .transpose()?,
    }))
}

pub(super) fn compile_optional_static_list_coalesce_comprehension_in_scalar_expression(
    lhs: &Expression,
    function: &FunctionInvocation,
    path: impl Into<String>,
    evaluation: StaticListComprehensionEvaluation<'_>,
    collection_context: &CypherCompileContext,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    let Some(coalesce) = compile_optional_static_list_coalesce_arguments(
        function,
        format!("{path}.rhs"),
        evaluation.mode.static_metadata_plan(),
        collection_context,
    )?
    else {
        return Ok(None);
    };
    let coalesce =
        static_list_coalesce_comprehension_arguments(coalesce, path.clone(), evaluation)?;
    static_list_coalesce_in_scalar_expression(lhs, coalesce, path, mode, context).map(Some)
}

pub(super) fn compile_optional_static_list_slice_in_predicate(
    lhs: &Expression,
    rhs: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<PredicateExpression>, CoreError> {
    if !is_branch_local_static_list_slice_expression(rhs) {
        return Ok(None);
    }
    let Some(expression) =
        compile_optional_static_list_slice_in_scalar_expression(lhs, rhs, path, mode, context)?
    else {
        return Ok(None);
    };
    Ok(Some(boolean_scalar_expression_predicate(expression)))
}

pub(super) fn compile_optional_static_list_slice_in_scalar_expression(
    lhs: &Expression,
    rhs: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match rhs {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_slice_in_scalar_expression(lhs, inner, path, mode, context)
        }
        Expression::ListSlice {
            list, start, end, ..
        } => {
            let bounds = StaticListSliceBounds {
                start: start.as_deref(),
                end: end.as_deref(),
            };
            if let Some(expression) = compile_optional_static_list_case_slice_in_scalar_expression(
                lhs,
                list,
                bounds,
                path.clone(),
                mode,
                context,
            )? {
                return Ok(Some(expression));
            }
            compile_optional_static_list_coalesce_slice_in_scalar_expression(
                lhs, list, bounds, path, mode, context,
            )
        }
        _ => Ok(None),
    }
}

pub(super) fn compile_optional_static_list_case_slice_in_scalar_expression(
    lhs: &Expression,
    list: &Expression,
    bounds: StaticListSliceBounds<'_>,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match list {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_case_slice_in_scalar_expression(
                lhs, inner, bounds, path, mode, context,
            )
        }
        Expression::Case(case) => {
            let Some(parts) = compile_optional_static_list_case_parts(
                case,
                format!("{path}.rhs"),
                mode,
                context,
            )?
            else {
                return Ok(None);
            };
            Ok(Some(ScalarExpression::Case {
                alternatives: parts
                    .alternatives
                    .into_iter()
                    .enumerate()
                    .map(|(index, (when, result))| {
                        Ok(ScalarCaseAlternative {
                            when,
                            then: static_list_case_result_slice_in_scalar_expression(
                                lhs,
                                result,
                                bounds,
                                format!("{path}.rhs.alternatives[{index}].then"),
                                mode,
                                context,
                            )?,
                        })
                    })
                    .collect::<Result<Vec<_>, CoreError>>()?,
                else_expression: parts
                    .default
                    .map(|result| {
                        static_list_case_result_slice_in_scalar_expression(
                            lhs,
                            result,
                            bounds,
                            format!("{path}.rhs.default"),
                            mode,
                            context,
                        )
                        .map(Box::new)
                    })
                    .transpose()?,
            }))
        }
        _ => Ok(None),
    }
}

pub(super) fn compile_optional_static_list_coalesce_slice_in_scalar_expression(
    lhs: &Expression,
    list: &Expression,
    bounds: StaticListSliceBounds<'_>,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    match list {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_coalesce_slice_in_scalar_expression(
                lhs, inner, bounds, path, mode, context,
            )
        }
        Expression::FunctionCall(function) if is_coalesce_function(function) => {
            let Some(coalesce) = compile_optional_static_list_coalesce_arguments(
                function,
                format!("{path}.rhs"),
                mode.static_metadata_plan(),
                context,
            )?
            else {
                return Ok(None);
            };
            static_list_coalesce_slice_in_scalar_expression(
                lhs, coalesce, bounds, path, mode, context,
            )
            .map(Some)
        }
        _ => Ok(None),
    }
}

pub(super) fn static_list_case_result_slice_in_scalar_expression(
    lhs: &Expression,
    result: StaticListCaseResult,
    bounds: StaticListSliceBounds<'_>,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    match result {
        StaticListCaseResult::Null => Ok(ScalarExpression::Literal(Literal::Null)),
        StaticListCaseResult::List(value) => {
            static_list_value_slice_in_scalar_expression(lhs, value, bounds, path, mode, context)
        }
        StaticListCaseResult::Coalesce(coalesce) => {
            static_list_coalesce_slice_in_scalar_expression(
                lhs, coalesce, bounds, path, mode, context,
            )
        }
    }
}

pub(super) fn static_list_coalesce_slice_in_scalar_expression(
    lhs: &Expression,
    coalesce: StaticListCoalesceArguments,
    bounds: StaticListSliceBounds<'_>,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let mut expression = ScalarExpression::Literal(Literal::Null);
    for (index, argument) in coalesce.arguments.into_iter().enumerate().rev() {
        let StaticListCoalesceArgument::List(mut value) = argument else {
            continue;
        };
        let presence_variable = value.presence_variable.take();
        let membership = static_list_value_slice_in_scalar_expression(
            lhs,
            value,
            bounds,
            format!("{path}.arguments[{index}]"),
            mode,
            context,
        )?;
        expression = match presence_variable {
            Some(variable) => ScalarExpression::Case {
                alternatives: vec![ScalarCaseAlternative {
                    when: PredicateExpression::Presence(PresencePredicate {
                        variable,
                        operator: ComparisonOperator::NotEqual,
                    }),
                    then: membership,
                }],
                else_expression: Some(Box::new(expression)),
            },
            None => membership,
        };
    }
    Ok(expression)
}

pub(super) fn static_list_value_slice_in_scalar_expression(
    lhs: &Expression,
    rhs_value: StaticListValue,
    bounds: StaticListSliceBounds<'_>,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let rhs_value =
        slice_static_list_value(rhs_value, bounds.start, bounds.end, path.clone(), context)?;
    compile_static_list_value_in_scalar_expression(lhs, rhs_value, path, mode, context)
}

pub(super) fn compile_optional_static_list_case_in_predicate(
    lhs: &Expression,
    rhs: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<PredicateExpression>, CoreError> {
    let path = path.into();
    match rhs {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_case_in_predicate(lhs, inner, path, mode, context)
        }
        Expression::Case(case) => {
            let Some(expression) = compile_optional_static_list_case_in_scalar_expression(
                lhs,
                case,
                format!("{path}.rhs"),
                mode,
                context,
            )?
            else {
                return Ok(None);
            };
            Ok(Some(boolean_scalar_expression_predicate(expression)))
        }
        _ => Ok(None),
    }
}

pub(super) fn compile_optional_static_list_coalesce_in_predicate(
    lhs: &Expression,
    rhs: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<PredicateExpression>, CoreError> {
    let path = path.into();
    match rhs {
        Expression::Parenthesized(inner) => {
            compile_optional_static_list_coalesce_in_predicate(lhs, inner, path, mode, context)
        }
        Expression::FunctionCall(function) if is_coalesce_function(function) => {
            let Some(coalesce) = compile_optional_static_list_coalesce_arguments(
                function,
                format!("{path}.rhs"),
                mode.static_metadata_plan(),
                context,
            )?
            else {
                return Ok(None);
            };
            Ok(Some(boolean_scalar_expression_predicate(
                static_list_coalesce_in_scalar_expression(lhs, coalesce, path, mode, context)?,
            )))
        }
        _ => Ok(None),
    }
}

pub(super) fn compile_optional_static_list_case_in_scalar_expression(
    lhs: &Expression,
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

    Ok(Some(ScalarExpression::Case {
        alternatives: parts
            .alternatives
            .into_iter()
            .enumerate()
            .map(|(index, (when, result))| {
                Ok(ScalarCaseAlternative {
                    when,
                    then: static_list_case_result_in_scalar_expression(
                        lhs,
                        result,
                        format!("{path}.alternatives[{index}].then"),
                        mode,
                        context,
                    )?,
                })
            })
            .collect::<Result<Vec<_>, CoreError>>()?,
        else_expression: parts
            .default
            .map(|result| {
                static_list_case_result_in_scalar_expression(
                    lhs,
                    result,
                    format!("{path}.default"),
                    mode,
                    context,
                )
                .map(Box::new)
            })
            .transpose()?,
    }))
}

pub(super) fn static_list_case_result_in_scalar_expression(
    lhs: &Expression,
    result: StaticListCaseResult,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    match result {
        StaticListCaseResult::Null => Ok(ScalarExpression::Literal(Literal::Null)),
        StaticListCaseResult::List(value) => {
            compile_static_list_value_in_scalar_expression(lhs, value, path, mode, context)
        }
        StaticListCaseResult::Coalesce(coalesce) => {
            static_list_coalesce_in_scalar_expression(lhs, coalesce, path, mode, context)
        }
    }
}

pub(super) fn static_list_coalesce_in_scalar_expression(
    lhs: &Expression,
    coalesce: StaticListCoalesceArguments,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let mut expression = ScalarExpression::Literal(Literal::Null);
    for (index, argument) in coalesce.arguments.into_iter().enumerate().rev() {
        let StaticListCoalesceArgument::List(mut value) = argument else {
            continue;
        };
        let presence_variable = value.presence_variable.take();
        let membership = compile_static_list_value_in_scalar_expression(
            lhs,
            value,
            format!("{path}.arguments[{index}]"),
            mode,
            context,
        )?;
        expression = match presence_variable {
            Some(variable) => ScalarExpression::Case {
                alternatives: vec![ScalarCaseAlternative {
                    when: PredicateExpression::Presence(PresencePredicate {
                        variable,
                        operator: ComparisonOperator::NotEqual,
                    }),
                    then: membership,
                }],
                else_expression: Some(Box::new(expression)),
            },
            None => membership,
        };
    }
    Ok(expression)
}

pub(super) fn compile_static_list_value_in_scalar_expression(
    lhs: &Expression,
    rhs_value: StaticListValue,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    Ok(ScalarExpression::Predicate(Box::new(
        compile_static_list_value_in_predicate(lhs, rhs_value, path, mode, context)?,
    )))
}

pub(super) fn compile_static_list_value_in_predicate(
    lhs: &Expression,
    rhs_value: StaticListValue,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<PredicateExpression, CoreError> {
    let path = path.into();
    let presence_variable = rhs_value.presence_variable.clone();
    let literals = rhs_value.literals;
    if let Some(literal) =
        compile_optional_static_literal_scalar_operand(lhs, format!("{path}.lhs"), mode, context)?
    {
        return Ok(presence_gate_static_list_in_literal_predicate(
            evaluate_literal_in_list(&literal, &literals, path)?,
            presence_variable,
        ));
    }
    if let Some(predicate) = compile_dynamic_static_list_in_predicate(
        lhs,
        &literals,
        presence_variable.clone(),
        &path,
        mode,
        context,
    )? {
        return Ok(predicate);
    }
    if let Some(plan) = mode.static_metadata_plan()
        && contains_type_function(lhs)
    {
        let literal = compile_predicate_literal(lhs, format!("{path}.lhs"), plan, context)?;
        return Ok(presence_gate_static_list_in_literal_predicate(
            evaluate_literal_in_list(&literal, &literals, path)?,
            presence_variable,
        ));
    }
    if is_literal_expression(lhs) {
        let literal = compile_literal(lhs, format!("{path}.lhs"), context)?;
        return Ok(presence_gate_static_list_in_literal_predicate(
            evaluate_literal_in_list(&literal, &literals, path)?,
            presence_variable,
        ));
    }
    Err(unsupported(
        format!("{path}.lhs"),
        mode.unsupported_in_message(),
    ))
}

pub(super) fn boolean_scalar_expression_predicate(
    expression: ScalarExpression,
) -> PredicateExpression {
    PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: expression,
        operator: ComparisonOperator::Equal,
        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Boolean(true))),
    })
}

pub(super) fn compile_dynamic_static_list_in_predicate(
    lhs: &Expression,
    literals: &[Literal],
    presence_variable: Option<String>,
    path: &str,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<PredicateExpression>, CoreError> {
    if let Some(lhs) =
        compile_optional_path_length_scalar_expression(lhs, format!("{path}.lhs"), mode, context)?
    {
        return Ok(Some(presence_gate_optional_static_list_in_predicate(
            PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs,
                operator: ComparisonOperator::In,
                rhs: ScalarPredicateRhs::List(literals.to_vec()),
            }),
            presence_variable,
        )));
    }
    if let Some(property) =
        compile_optional_property_ref(lhs, format!("{path}.lhs"), mode.graph_plan(), context)?
    {
        return Ok(Some(presence_gate_optional_static_list_in_predicate(
            PredicateExpression::Comparison(PropertyPredicate {
                property,
                operator: ComparisonOperator::In,
                rhs: PredicateRhs::List(literals.to_vec()),
            }),
            presence_variable,
        )));
    }
    if let Some(plan) = mode.graph_plan() {
        if let Some(variable) = compile_optional_id_ref(lhs, format!("{path}.lhs"), plan, context)?
        {
            return Ok(Some(presence_gate_optional_static_list_in_predicate(
                PredicateExpression::KeyComparison(KeyPredicate {
                    variable,
                    operator: ComparisonOperator::In,
                    rhs: PredicateRhs::List(literals.to_vec()),
                }),
                presence_variable,
            )));
        }
        if let Some(variable) =
            compile_optional_element_id_ref(lhs, format!("{path}.lhs"), plan, context)?
        {
            return Ok(Some(presence_gate_optional_static_list_in_predicate(
                PredicateExpression::ElementIdComparison(ElementIdPredicate {
                    variable,
                    operator: ComparisonOperator::In,
                    rhs: PredicateRhs::List(literals.to_vec()),
                }),
                presence_variable,
            )));
        }
    }
    if let Some(lhs) =
        compile_optional_predicate_scalar_expression(lhs, format!("{path}.lhs"), mode, context)?
    {
        return Ok(Some(presence_gate_optional_static_list_in_predicate(
            PredicateExpression::ScalarComparison(ScalarPredicate {
                lhs,
                operator: ComparisonOperator::In,
                rhs: ScalarPredicateRhs::List(literals.to_vec()),
            }),
            presence_variable,
        )));
    }
    Ok(None)
}

pub(super) fn presence_gate_static_list_in_literal_predicate(
    matches: bool,
    presence_variable: Option<String>,
) -> PredicateExpression {
    match presence_variable {
        Some(presence_variable) => presence_gated_boolean_predicate(presence_variable, matches),
        None => PredicateExpression::Boolean(matches),
    }
}

pub(super) fn presence_gate_optional_static_list_in_predicate(
    predicate: PredicateExpression,
    presence_variable: Option<String>,
) -> PredicateExpression {
    match presence_variable {
        Some(presence_variable) => PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::PresenceGated {
                presence_variable,
                expression: Box::new(ScalarExpression::Predicate(Box::new(predicate))),
            },
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Boolean(true))),
        }),
        None => predicate,
    }
}

pub(super) fn compile_property_key_membership_predicate(
    lhs: &Expression,
    rhs: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<PredicateExpression>, CoreError> {
    let path = path.into();
    if let Expression::FunctionCall(function) = rhs
        && is_keys_function(function)
        && let Some(value) = compile_optional_same_label_undirected_endpoint_function_argument(
            function,
            format!("{path}.rhs.arguments"),
            plan,
            context,
        )?
    {
        if !is_literal_expression(lhs) {
            return Ok(None);
        }
        let literal = compile_predicate_literal(lhs, format!("{path}.lhs"), plan, context)?;
        let Literal::String(key) = literal else {
            return Err(unsupported(
                format!("{path}.lhs"),
                "keys() membership predicates require a string literal or scalar string parameter",
            ));
        };
        let graph = context.graph.as_ref().ok_or_else(|| {
            unsupported(
                format!("{path}.rhs"),
                "keys() requires a graph declaration so mapped property keys can be inspected",
            )
        })?;
        let mapping = graph.node(&value.label).ok_or_else(|| {
            unsupported(
                format!("{path}.rhs"),
                format!(
                    "keys() metadata expression could not resolve node label '{}'",
                    value.label
                ),
            )
        })?;
        return Ok(Some(presence_gated_boolean_predicate(
            value.relationship,
            mapping.properties.contains_key(&key),
        )));
    }
    if let Expression::FunctionCall(function) = rhs
        && is_keys_function(function)
        && matches!(
            function.arguments.as_slice(),
            [Expression::Literal(CypherLiteral::Map(_))]
        )
    {
        return Ok(None);
    }
    let Some(value) = compile_optional_keys_ref(rhs, format!("{path}.rhs"), plan, context)? else {
        return Ok(None);
    };
    if !is_literal_expression(lhs) {
        return Ok(None);
    }
    let literal = compile_predicate_literal(lhs, format!("{path}.lhs"), plan, context)?;
    let Literal::String(key) = literal else {
        return Err(unsupported(
            format!("{path}.lhs"),
            "keys() membership predicates require a string literal or scalar string parameter",
        ));
    };
    Ok(Some(PredicateExpression::PropertyKeyMembership(
        PropertyKeyMembershipPredicate {
            variable: value.variable,
            key,
            presence_variable: value.presence_variable,
        },
    )))
}

pub(super) fn compile_label_membership_predicate(
    lhs: &Expression,
    rhs: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<PredicateExpression>, CoreError> {
    let path = path.into();
    if let Expression::FunctionCall(function) = rhs
        && is_labels_function(function)
        && let Some(value) = compile_optional_same_label_undirected_endpoint_function_argument(
            function,
            format!("{path}.rhs.arguments"),
            plan,
            context,
        )?
    {
        if !is_literal_expression(lhs) {
            return Ok(None);
        }
        let literal = compile_predicate_literal(lhs, format!("{path}.lhs"), plan, context)?;
        let Literal::String(candidate) = literal else {
            return Err(unsupported(
                format!("{path}.lhs"),
                "label membership predicates require a string literal or scalar string parameter",
            ));
        };
        return Ok(Some(presence_gated_boolean_predicate(
            value.relationship,
            candidate == value.label,
        )));
    }
    let Some((value, label)) =
        compile_optional_labels_ref(rhs, format!("{path}.rhs"), plan, context)?
    else {
        return Ok(None);
    };
    if !is_literal_expression(lhs) {
        return Ok(None);
    }
    let literal = compile_predicate_literal(lhs, format!("{path}.lhs"), plan, context)?;
    let Literal::String(candidate) = literal else {
        return Err(unsupported(
            format!("{path}.lhs"),
            "label membership predicates require a string literal or scalar string parameter",
        ));
    };
    let matches = candidate == label;
    if let Some(presence_variable) = value.presence_variable {
        return Ok(Some(presence_gated_boolean_predicate(
            presence_variable,
            matches,
        )));
    }
    Ok(Some(PredicateExpression::Boolean(matches)))
}

pub(super) fn presence_gated_boolean_predicate(
    presence_variable: String,
    value: bool,
) -> PredicateExpression {
    presence_gated_boolean_predicate_for_variables(vec![presence_variable], value)
}

pub(super) fn presence_gated_boolean_predicate_for_variables(
    presence_variables: Vec<String>,
    value: bool,
) -> PredicateExpression {
    let expression = presence_variables.into_iter().fold(
        ScalarExpression::Literal(Literal::Boolean(value)),
        |expression, presence_variable| ScalarExpression::PresenceGated {
            presence_variable,
            expression: Box::new(expression),
        },
    );
    PredicateExpression::ScalarComparison(ScalarPredicate {
        lhs: expression,
        operator: ComparisonOperator::Equal,
        rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Boolean(true))),
    })
}

pub(super) fn compile_graph_label_predicate(
    base: &Expression,
    labels: &[LabelExpression],
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<PredicateExpression, CoreError> {
    let path = path.into();
    let variable = compile_graph_value_expression_variable(
        base,
        format!("{path}.base"),
        "graph label predicates require a node or relationship variable",
        plan,
        context,
    )?;
    let mapped_label = mapped_graph_label_for_variable(plan, &variable).ok_or_else(|| {
        unsupported(
            format!("{path}.base"),
            format!("label predicate variable '{variable}' is not a node or relationship variable"),
        )
    })?;
    if labels.is_empty() {
        return Err(unsupported(
            format!("{path}.labels"),
            "graph label predicates require at least one label or relationship type",
        ));
    }

    let matches = labels.iter().enumerate().try_fold(
        true,
        |matches, (index, label)| -> Result<bool, CoreError> {
            Ok(matches
                && evaluate_label_predicate_expression(
                    label,
                    mapped_label,
                    format!("{path}.labels[{index}]"),
                    context,
                )?)
        },
    )?;
    Ok(PredicateExpression::Boolean(matches))
}

pub(super) fn mapped_graph_label_for_variable<'a>(
    plan: &'a GraphPlan,
    variable: &str,
) -> Option<&'a str> {
    if let Some(node) = plan.nodes.iter().find(|node| node.variable == variable) {
        return Some(node.label.as_str());
    }
    plan.relationships
        .iter()
        .find(|relationship| relationship.variable.as_deref() == Some(variable))
        .map(|relationship| relationship.relationship_type.as_str())
}

#[derive(Clone, Copy)]
pub(super) enum LabelExpressionResolver<'a> {
    StaticOnly,
    CompileTimeDynamic { context: &'a CypherCompileContext },
}

impl LabelExpressionResolver<'_> {
    pub(super) fn resolve_dynamic(
        self,
        expression: &Expression,
        path: impl Into<String>,
    ) -> Result<String, CoreError> {
        match self {
            LabelExpressionResolver::StaticOnly => Err(unsupported(
                path,
                "dynamic label expressions are not supported yet",
            )),
            LabelExpressionResolver::CompileTimeDynamic { context } => {
                compile_dynamic_label_expression(expression, path, context)
            }
        }
    }

    pub(super) fn resolve_dynamic_labels(
        self,
        expression: &Expression,
        path: impl Into<String>,
    ) -> Result<Vec<String>, CoreError> {
        match self {
            LabelExpressionResolver::StaticOnly => Err(unsupported(
                path,
                "dynamic label expressions are not supported yet",
            )),
            LabelExpressionResolver::CompileTimeDynamic { context } => {
                compile_dynamic_label_expressions(expression, path, context)
            }
        }
    }
}

pub(super) fn evaluate_label_predicate_expression(
    expression: &LabelExpression,
    mapped_label: &str,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<bool, CoreError> {
    evaluate_label_expression(
        expression,
        mapped_label,
        path,
        LabelExpressionResolver::CompileTimeDynamic { context },
    )
}

pub(super) fn evaluate_label_expression(
    expression: &LabelExpression,
    mapped_label: &str,
    path: impl Into<String>,
    resolver: LabelExpressionResolver<'_>,
) -> Result<bool, CoreError> {
    let path = path.into();
    match expression {
        LabelExpression::Static(label) => Ok(label.name == mapped_label),
        LabelExpression::Dynamic { expression, .. } => Ok(resolver
            .resolve_dynamic_labels(expression, path)?
            .iter()
            .any(|label| label == mapped_label)),
        LabelExpression::Or { lhs, rhs, .. } => {
            Ok(
                evaluate_label_expression(lhs, mapped_label, format!("{path}.lhs"), resolver)?
                    || evaluate_label_expression(
                        rhs,
                        mapped_label,
                        format!("{path}.rhs"),
                        resolver,
                    )?,
            )
        }
        LabelExpression::And { lhs, rhs, .. } => {
            Ok(
                evaluate_label_expression(lhs, mapped_label, format!("{path}.lhs"), resolver)?
                    && evaluate_label_expression(
                        rhs,
                        mapped_label,
                        format!("{path}.rhs"),
                        resolver,
                    )?,
            )
        }
        LabelExpression::Not { inner, .. } => Ok(!evaluate_label_expression(
            inner,
            mapped_label,
            format!("{path}.inner"),
            resolver,
        )?),
        LabelExpression::Group { inner, .. } => {
            evaluate_label_expression(inner, mapped_label, path, resolver)
        }
    }
}

pub(super) fn compile_dynamic_label_expression(
    expression: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<String, CoreError> {
    let path = path.into();
    let labels = compile_dynamic_label_expressions(expression, path.clone(), context)?;
    match labels.as_slice() {
        [label] => Ok(label.clone()),
        [] => Err(CoreError::internal(
            "dynamic label expression resolver returned an empty label set",
        )),
        _ => Err(unsupported(
            path,
            "dynamic label expressions require exactly one label in this context",
        )),
    }
}

pub(super) fn compile_dynamic_label_expressions(
    expression: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Vec<String>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_dynamic_label_expressions(inner, path, context),
        Expression::Literal(CypherLiteral::String(label)) => Ok(vec![label.value.clone()]),
        Expression::Literal(CypherLiteral::List(list)) => {
            compile_dynamic_label_literal_list(&list.elements, path, context)
        }
        Expression::Case(case) => {
            match compile_optional_static_folded_case_list_value(
                case,
                path.clone(),
                None,
                context,
                "dynamic label CASE expressions require statically foldable WHEN predicates",
            )? {
                Some(value) => compile_dynamic_label_static_list_value(value, path),
                None => Err(unsupported(path, DYNAMIC_LABEL_EXPRESSION_MESSAGE)),
            }
        }
        Expression::Parameter(parameter) => {
            match context.parameter_value(parameter, path.clone())? {
                CypherParameterValue::Literal(Literal::String(label)) => Ok(vec![label.clone()]),
                CypherParameterValue::List(labels) => {
                    compile_dynamic_label_list_parameter(labels, path)
                }
                CypherParameterValue::Literal(_) => {
                    Err(unsupported(path, DYNAMIC_LABEL_EXPRESSION_MESSAGE))
                }
            }
        }
        _ => {
            if let Some(value) =
                compile_optional_static_list_value(expression, path.clone(), None, context)?
            {
                return compile_dynamic_label_static_list_value(value, path);
            }
            Err(unsupported(path, DYNAMIC_LABEL_EXPRESSION_MESSAGE))
        }
    }
}

pub(super) const DYNAMIC_LABEL_EXPRESSION_MESSAGE: &str = "dynamic label expressions require a string literal, scalar string parameter, non-empty literal string list, folded static string-list expression, or non-empty list string parameter";

pub(super) fn compile_dynamic_label_literal_list(
    expressions: &[Expression],
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Vec<String>, CoreError> {
    let path = path.into();
    if expressions.is_empty() {
        return Err(unsupported(
            path,
            "dynamic label literal lists require at least one string",
        ));
    }
    expressions
        .iter()
        .enumerate()
        .map(|(index, expression)| {
            match compile_literal(expression, format!("{path}[{index}]"), context)? {
                Literal::String(label) => Ok(label),
                _ => Err(unsupported(
                    format!("{path}[{index}]"),
                    "dynamic label literal lists require only strings",
                )),
            }
        })
        .collect()
}

pub(super) fn compile_dynamic_label_static_list_value(
    value: StaticListValue,
    path: impl Into<String>,
) -> Result<Vec<String>, CoreError> {
    let path = path.into();
    if value.presence_variable.is_some() {
        return Err(unsupported(
            path,
            "dynamic label list expressions cannot depend on optional graph bindings",
        ));
    }
    if value.literals.is_empty() {
        return Err(unsupported(
            path,
            "dynamic label list expressions require at least one string",
        ));
    }
    value
        .literals
        .into_iter()
        .enumerate()
        .map(|(index, literal)| match literal {
            Literal::String(label) => Ok(label),
            _ => Err(unsupported(
                format!("{path}[{index}]"),
                "dynamic label list expressions require only strings",
            )),
        })
        .collect()
}

pub(super) fn compile_dynamic_label_list_parameter(
    labels: &[Literal],
    path: impl Into<String>,
) -> Result<Vec<String>, CoreError> {
    let path = path.into();
    if labels.is_empty() {
        return Err(unsupported(
            path,
            "dynamic label list parameters require at least one string",
        ));
    }
    labels
        .iter()
        .enumerate()
        .map(|(index, label)| match label {
            Literal::String(label) => Ok(label.clone()),
            _ => Err(unsupported(
                format!("{path}[{index}]"),
                "dynamic label list parameters require only strings",
            )),
        })
        .collect()
}

pub(super) fn compile_optional_keys_ref(
    expression: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<GraphValueRef>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_optional_keys_ref(inner, path, plan, context),
        Expression::FunctionCall(function) if is_keys_function(function) => {
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
            Ok(Some(value))
        }
        _ => Ok(None),
    }
}

pub(super) fn compile_optional_labels_ref(
    expression: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<(GraphValueRef, String)>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_optional_labels_ref(inner, path, plan, context),
        Expression::FunctionCall(function) if is_labels_function(function) => {
            Ok(Some(compile_node_function_target_ref(
                function,
                format!("{path}.arguments"),
                "labels() supports exactly one node variable argument",
                plan,
                context,
            )?))
        }
        _ => Ok(None),
    }
}

pub(super) fn compile_predicate_rhs(
    expression: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<PredicateRhs, CoreError> {
    let path = path.into();
    if let Some(expression) =
        compile_optional_path_length_scalar_expression(expression, path.clone(), mode, context)?
    {
        let ScalarExpression::Literal(literal) = expression else {
            return Err(CoreError::internal(
                "path length scalar expression was not lowered to a literal",
            ));
        };
        return Ok(PredicateRhs::Literal(literal));
    }
    match expression {
        Expression::Parenthesized(inner) => compile_predicate_rhs(inner, path, mode, context),
        Expression::PropertyLookup { .. } => Ok(PredicateRhs::Property(compile_property_ref(
            expression,
            path,
            mode.graph_plan(),
            context,
        )?)),
        Expression::ListIndex { .. } => {
            if let Some(property) =
                compile_optional_property_ref(expression, path.clone(), mode.graph_plan(), context)?
            {
                Ok(PredicateRhs::Property(property))
            } else {
                Ok(PredicateRhs::Literal(compile_predicate_literal_in_mode(
                    expression, path, mode, context,
                )?))
            }
        }
        Expression::FunctionCall(function) if is_id_function(function) => match mode.graph_plan() {
            Some(plan) => Ok(PredicateRhs::Key {
                variable: compile_id_variable(function, path, plan, context)?,
            }),
            None => Err(unsupported(
                path,
                "CASE WHEN property comparisons do not support id() right-hand sides yet",
            )),
        },
        Expression::FunctionCall(function) if is_element_id_function(function) => {
            match mode.graph_plan() {
                Some(plan) => Ok(PredicateRhs::ElementId {
                    variable: compile_element_id_variable(function, path, plan, context)?,
                }),
                None => Err(unsupported(
                    path,
                    "CASE WHEN property comparisons do not support elementId() right-hand sides yet",
                )),
            }
        }
        _ => Ok(PredicateRhs::Literal(compile_predicate_literal_in_mode(
            expression, path, mode, context,
        )?)),
    }
}

pub(super) fn compile_literal_predicate_rhs(
    expression: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<PredicateRhs, CoreError> {
    Ok(PredicateRhs::Literal(compile_predicate_literal_in_mode(
        expression, path, mode, context,
    )?))
}

pub(super) fn compile_null_predicate(
    operand: &Expression,
    negated: bool,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<PredicateExpression, CoreError> {
    let path = path.into();
    let operator = if negated {
        ComparisonOperator::NotEqual
    } else {
        ComparisonOperator::Equal
    };
    if let Some(property) = compile_optional_property_ref(
        operand,
        format!("{path}.operand"),
        mode.graph_plan(),
        context,
    )? {
        return Ok(PredicateExpression::Comparison(PropertyPredicate {
            property,
            operator,
            rhs: PredicateRhs::Literal(Literal::Null),
        }));
    }
    if let Some(plan) = mode.graph_plan() {
        if let Some(variable) =
            compile_optional_id_ref(operand, format!("{path}.operand"), plan, context)?
        {
            return Ok(PredicateExpression::KeyComparison(KeyPredicate {
                variable,
                operator,
                rhs: PredicateRhs::Literal(Literal::Null),
            }));
        }
        if let Some(variable) =
            compile_optional_element_id_ref(operand, format!("{path}.operand"), plan, context)?
        {
            return Ok(PredicateExpression::ElementIdComparison(
                ElementIdPredicate {
                    variable,
                    operator,
                    rhs: PredicateRhs::Literal(Literal::Null),
                },
            ));
        }
    }
    if let Some(literal) = compile_optional_static_literal_scalar_operand(
        operand,
        format!("{path}.operand"),
        mode,
        context,
    )? {
        let is_null = matches!(literal, Literal::Null);
        return Ok(PredicateExpression::Boolean(if negated {
            !is_null
        } else {
            is_null
        }));
    }
    if let Some(lhs) = compile_optional_path_length_scalar_expression(
        operand,
        format!("{path}.operand"),
        mode,
        context,
    )? {
        return Ok(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs,
            operator,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Null)),
        }));
    }
    if let Some(lhs) = compile_optional_predicate_scalar_expression(
        operand,
        format!("{path}.operand"),
        mode,
        context,
    )? {
        return Ok(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs,
            operator,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Null)),
        }));
    }
    if let Some(plan) = mode.graph_plan() {
        if let Some(variable) =
            compile_optional_graph_variable_ref(operand, format!("{path}.operand"), plan, context)?
        {
            if !plan_uses_variable(plan, &variable) {
                return Err(unsupported(
                    format!("{path}.operand"),
                    format!("IS NULL argument '{variable}' is not a bound graph variable"),
                ));
            }
            return Ok(PredicateExpression::Presence(PresencePredicate {
                variable,
                operator,
            }));
        }
        if mode.graph_metadata_plan().is_some() && contains_type_function(operand) {
            return Ok(PredicateExpression::Boolean(negated));
        }
    }
    Err(unsupported(
        format!("{path}.operand"),
        mode.unsupported_null_message(),
    ))
}

pub(super) fn compile_optional_graph_variable_ref(
    expression: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<String>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_optional_graph_variable_ref(inner, path, plan, context)
        }
        Expression::Variable(variable) => Ok(Some(variable_name(variable))),
        Expression::FunctionCall(function)
            if is_start_node_function(function) || is_end_node_function(function) =>
        {
            let value = compile_relationship_endpoint_ref(function, path, plan, context)?;
            if value.presence_variable.is_some() {
                Ok(None)
            } else {
                Ok(Some(value.variable))
            }
        }
        _ => Ok(None),
    }
}

pub(super) fn compile_optional_id_ref(
    expression: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<String>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_optional_id_ref(inner, path, plan, context),
        Expression::FunctionCall(function) if is_id_function(function) => {
            let value = compile_id_graph_value_ref(function, path, plan, context)?;
            if value.presence_variable.is_some() {
                Ok(None)
            } else {
                Ok(Some(value.variable))
            }
        }
        _ => Ok(None),
    }
}

pub(super) fn compile_optional_element_id_ref(
    expression: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<String>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_optional_element_id_ref(inner, path, plan, context)
        }
        Expression::FunctionCall(function) if is_element_id_function(function) => {
            let value = compile_element_id_graph_value_ref(function, path, plan, context)?;
            if value.presence_variable.is_some() {
                Ok(None)
            } else {
                Ok(Some(value.variable))
            }
        }
        _ => Ok(None),
    }
}

pub(super) fn compile_id_variable(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<String, CoreError> {
    let path = path.into();
    let variable = compile_single_graph_value_function_argument(
        function,
        format!("{path}.arguments"),
        "id() supports exactly one graph variable argument",
        plan,
        context,
    )?;
    if !plan_uses_variable(plan, &variable) {
        return Err(unsupported(
            format!("{path}.arguments[0]"),
            format!("id() argument '{variable}' is not a bound graph variable"),
        ));
    }
    Ok(variable)
}

pub(super) fn compile_element_id_variable(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<String, CoreError> {
    let path = path.into();
    let variable = compile_single_graph_value_function_argument(
        function,
        format!("{path}.arguments"),
        "elementId() supports exactly one graph variable argument",
        plan,
        context,
    )?;
    if !plan_uses_variable(plan, &variable) {
        return Err(unsupported(
            format!("{path}.arguments[0]"),
            format!("elementId() argument '{variable}' is not a bound graph variable"),
        ));
    }
    Ok(variable)
}

pub(super) fn compile_predicate_literal(
    expression: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Literal, CoreError> {
    compile_predicate_literal_in_mode(
        expression,
        path,
        PredicateCompileMode::Graph {
            plan,
            path_state: None,
        },
        context,
    )
}

pub(super) fn compile_predicate_literal_in_mode(
    expression: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Literal, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_predicate_literal_in_mode(inner, path, mode, context)
        }
        Expression::FunctionCall(function) if is_type_function(function) => {
            match mode.static_metadata_plan() {
                Some(plan) => compile_type_literal(function, path, plan, context),
                None => Err(unsupported(path, "type() operands require graph context")),
            }
        }
        Expression::FunctionCall(function)
            if matches!(mode, PredicateCompileMode::CaseWhen { .. })
                && (is_id_function(function)
                    || is_element_id_function(function)
                    || is_labels_function(function)
                    || is_keys_function(function)) =>
        {
            Err(unsupported(
                path,
                "CASE WHEN predicates do not support graph identity or metadata functions yet",
            ))
        }
        _ => compile_literal(expression, path, context),
    }
}

pub(super) fn compile_type_literal(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Literal, CoreError> {
    let path = path.into();
    let variable = compile_single_variable_function_argument(
        function,
        format!("{path}.arguments"),
        "type() supports exactly one relationship variable argument",
        context,
    )?;
    let relationship = plan
        .relationships
        .iter()
        .find(|relationship| relationship.variable.as_deref() == Some(variable.as_str()))
        .ok_or_else(|| {
            unsupported(
                format!("{path}.arguments[0]"),
                format!("type() argument '{variable}' is not a named relationship variable"),
            )
        })?;
    Ok(Literal::String(relationship.relationship_type.clone()))
}

pub(super) fn contains_type_function(expression: &Expression) -> bool {
    match expression {
        Expression::Parenthesized(inner) => contains_type_function(inner),
        Expression::FunctionCall(function) => is_type_function(function),
        _ => false,
    }
}

pub(super) fn evaluate_literal_comparison(
    lhs: &Literal,
    operator: ComparisonOperator,
    rhs: &Literal,
    path: impl Into<String>,
) -> Result<bool, CoreError> {
    let path = path.into();
    match operator {
        ComparisonOperator::Equal => match compare_numeric_literals(lhs, rhs, path.clone())? {
            Some(ordering) => Ok(ordering == Ordering::Equal),
            None => Ok(lhs == rhs),
        },
        ComparisonOperator::NotEqual => match compare_numeric_literals(lhs, rhs, path.clone())? {
            Some(ordering) => Ok(ordering != Ordering::Equal),
            None => Ok(lhs != rhs),
        },
        ComparisonOperator::StartsWith => match (lhs, rhs) {
            (Literal::String(lhs), Literal::String(rhs)) => Ok(lhs.starts_with(rhs)),
            _ => Err(unsupported(
                path,
                "STARTS WITH literal comparisons require string operands",
            )),
        },
        ComparisonOperator::EndsWith => match (lhs, rhs) {
            (Literal::String(lhs), Literal::String(rhs)) => Ok(lhs.ends_with(rhs)),
            _ => Err(unsupported(
                path,
                "ENDS WITH literal comparisons require string operands",
            )),
        },
        ComparisonOperator::Contains => match (lhs, rhs) {
            (Literal::String(lhs), Literal::String(rhs)) => Ok(lhs.contains(rhs)),
            _ => Err(unsupported(
                path,
                "CONTAINS literal comparisons require string operands",
            )),
        },
        ComparisonOperator::RegexMatch => match (lhs, rhs) {
            (Literal::String(lhs), Literal::String(rhs)) => Regex::new(rhs)
                .map(|pattern| pattern.is_match(lhs))
                .map_err(|error| {
                    unsupported(
                        path,
                        format!("invalid regex literal for =~ comparison: {error}"),
                    )
                }),
            _ => Err(unsupported(
                path,
                "=~ literal comparisons require string operands",
            )),
        },
        ComparisonOperator::GreaterThan
        | ComparisonOperator::GreaterThanOrEqual
        | ComparisonOperator::LessThan
        | ComparisonOperator::LessThanOrEqual => {
            let ordering = if let Some(ordering) = compare_numeric_literals(lhs, rhs, path.clone())?
            {
                ordering
            } else if let (Literal::String(lhs), Literal::String(rhs)) = (lhs, rhs) {
                lhs.cmp(rhs)
            } else {
                return Err(unsupported(
                    path,
                    "ordered literal comparisons require numeric or string operands",
                ));
            };
            match operator {
                ComparisonOperator::GreaterThan => Ok(ordering == Ordering::Greater),
                ComparisonOperator::GreaterThanOrEqual => {
                    Ok(matches!(ordering, Ordering::Greater | Ordering::Equal))
                }
                ComparisonOperator::LessThan => Ok(ordering == Ordering::Less),
                ComparisonOperator::LessThanOrEqual => {
                    Ok(matches!(ordering, Ordering::Less | Ordering::Equal))
                }
                _ => unreachable!("non-ordered operator reached ordered comparison branch"),
            }
        }
        ComparisonOperator::In => Err(unsupported(
            path,
            "literal comparisons do not use the IN comparison operator",
        )),
    }
}

pub(super) fn evaluate_literal_only_comparison(
    lhs: &Literal,
    operator: ComparisonOperator,
    rhs: &Literal,
    path: impl Into<String>,
) -> Result<bool, CoreError> {
    let path = path.into();
    if matches!(lhs, Literal::Null) || matches!(rhs, Literal::Null) {
        return Err(unsupported(
            path,
            "literal-only null comparisons are not supported because Cypher null comparisons produce unknown",
        ));
    }
    evaluate_literal_comparison(lhs, operator, rhs, path)
}

pub(super) fn evaluate_literal_in_list(
    literal: &Literal,
    literals: &[Literal],
    path: impl Into<String>,
) -> Result<bool, CoreError> {
    let path = path.into();
    if matches!(literal, Literal::Null) {
        return Err(unsupported(
            path,
            "literal IN predicates with a null left-hand side are not supported because Cypher membership produces unknown",
        ));
    }

    for candidate in literals {
        if matches!(candidate, Literal::Null) {
            continue;
        }
        if evaluate_literal_comparison(literal, ComparisonOperator::Equal, candidate, path.clone())?
        {
            return Ok(true);
        }
    }

    Ok(false)
}

pub(super) fn compare_numeric_literals(
    lhs: &Literal,
    rhs: &Literal,
    path: impl Into<String>,
) -> Result<Option<Ordering>, CoreError> {
    let path = path.into();
    match (lhs, rhs) {
        (Literal::Integer(lhs), Literal::Integer(rhs)) => Ok(Some(lhs.cmp(rhs))),
        (Literal::Float(lhs), Literal::Float(rhs)) => lhs
            .into_inner()
            .partial_cmp(&rhs.into_inner())
            .map(Some)
            .ok_or_else(|| unsupported(path, "non-finite numeric literals are not supported")),
        (Literal::Integer(lhs), Literal::Float(rhs)) => {
            compare_integer_float_literals(*lhs, rhs.into_inner(), path).map(Some)
        }
        (Literal::Float(lhs), Literal::Integer(rhs)) => {
            compare_integer_float_literals(*rhs, lhs.into_inner(), path)
                .map(Ordering::reverse)
                .map(Some)
        }
        _ => Ok(None),
    }
}

#[expect(
    clippy::cast_precision_loss,
    reason = "integer is range-checked to f64's exact integer range before casting"
)]
pub(super) fn compare_integer_float_literals(
    integer: i64,
    float: f64,
    path: impl Into<String>,
) -> Result<Ordering, CoreError> {
    const MAX_EXACT_F64_INTEGER: i64 = 9_007_199_254_740_992;
    let path = path.into();
    if !(-MAX_EXACT_F64_INTEGER..=MAX_EXACT_F64_INTEGER).contains(&integer) {
        return Err(unsupported(
            path,
            "mixed integer/float literal comparisons require an integer that can be represented exactly as f64",
        ));
    }
    // The range guard above restricts the integer to f64's exact integer range.
    (integer as f64)
        .partial_cmp(&float)
        .ok_or_else(|| unsupported(path, "non-finite numeric literals are not supported"))
}

pub(super) fn compile_property_ref(
    expression: &Expression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<PropertyRef, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_property_ref(inner, path, plan, context),
        Expression::PropertyLookup { base, property, .. } => Ok(PropertyRef {
            variable: compile_property_base_variable(base, format!("{path}.base"), plan, context)?,
            property: property.name.name.clone(),
        }),
        Expression::ListIndex { list, index, .. } => compile_property_index_ref(
            list,
            index,
            format!("{path}.list"),
            format!("{path}.index"),
            plan,
            context,
        ),
        _ => Err(unsupported(
            path,
            "only variable.property expressions are supported here",
        )),
    }
}

pub(super) fn compile_optional_property_ref(
    expression: &Expression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<PropertyRef>, CoreError> {
    let path = path.into();
    if let Some(property) =
        compile_optional_static_map_lookup_property_ref(expression, path.clone(), plan, context)?
    {
        return Ok(Some(property));
    }
    match expression {
        Expression::Parenthesized(inner) => {
            compile_optional_property_ref(inner, path, plan, context)
        }
        Expression::PropertyLookup { base, .. } => {
            if compile_optional_endpoint_property_scalar_expression(
                expression,
                path.clone(),
                plan,
                context,
            )?
            .is_some()
            {
                return Ok(None);
            }
            if !is_property_index_base_expression(base) {
                return Ok(None);
            }
            compile_property_ref(expression, path, plan, context).map(Some)
        }
        Expression::ListIndex { list, index, .. } => {
            if !is_property_index_base_expression(list) {
                return Ok(None);
            }
            compile_property_index_ref(
                list,
                index,
                format!("{path}.list"),
                format!("{path}.index"),
                plan,
                context,
            )
            .map(Some)
        }
        _ => Ok(None),
    }
}

pub(super) fn compile_property_index_ref(
    base: &Expression,
    index: &Expression,
    base_path: impl Into<String>,
    index_path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<PropertyRef, CoreError> {
    Ok(PropertyRef {
        variable: compile_property_base_variable(base, base_path, plan, context)?,
        property: compile_property_index_name(index, index_path, context)?,
    })
}

pub(super) fn compile_property_index_name(
    index: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<String, CoreError> {
    let path = path.into();
    match compile_literal(index, path.clone(), context)? {
        Literal::String(property) => Ok(property),
        _ => Err(unsupported(
            path,
            "property index lookups require a string literal or scalar string parameter",
        )),
    }
}

pub(super) fn is_property_index_base_expression(expression: &Expression) -> bool {
    match expression {
        Expression::Parenthesized(inner) => is_property_index_base_expression(inner),
        Expression::Variable(_) => true,
        Expression::FunctionCall(function) => {
            is_properties_function(function)
                || is_start_node_function(function)
                || is_end_node_function(function)
        }
        _ => false,
    }
}

pub(super) fn compile_optional_endpoint_property_scalar_expression(
    expression: &Expression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<(ScalarExpression, String)>, CoreError> {
    let path = path.into();
    let Some(plan) = plan else {
        return Ok(None);
    };
    if let Some((expression, output_name)) =
        compile_optional_same_label_undirected_endpoint_property_scalar_expression(
            expression,
            path.clone(),
            plan,
            context,
        )?
    {
        return Ok(Some((expression, output_name)));
    }
    let Some((property, presence_variable, output_name)) =
        compile_optional_endpoint_property_ref(expression, path, plan, context)?
    else {
        return Ok(None);
    };
    Ok(Some((
        presence_gate_scalar_expression(
            Some(presence_variable),
            ScalarExpression::Property(property),
        ),
        output_name,
    )))
}

pub(super) fn compile_optional_same_label_undirected_endpoint_property_scalar_expression(
    expression: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<(ScalarExpression, String)>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_optional_same_label_undirected_endpoint_property_scalar_expression(
                inner, path, plan, context,
            )
        }
        Expression::PropertyLookup { base, property, .. } => {
            let Some(endpoint_ref) = compile_optional_same_label_undirected_relationship_endpoint(
                base,
                format!("{path}.base"),
                plan,
                context,
            )?
            else {
                return Ok(None);
            };
            let property = property.name.name.clone();
            let output_name = format!("{}_{}", endpoint_ref.relationship, property);
            Ok(Some((
                ScalarExpression::UndirectedEndpointProperty {
                    relationship: endpoint_ref.relationship,
                    endpoint: endpoint_ref.endpoint,
                    property,
                },
                output_name,
            )))
        }
        Expression::ListIndex { list, index, .. } => {
            let Some(endpoint_ref) = compile_optional_same_label_undirected_relationship_endpoint(
                list,
                format!("{path}.list"),
                plan,
                context,
            )?
            else {
                return Ok(None);
            };
            let property = compile_property_index_name(index, format!("{path}.index"), context)?;
            let output_name = format!("{}_{}", endpoint_ref.relationship, property);
            Ok(Some((
                ScalarExpression::UndirectedEndpointProperty {
                    relationship: endpoint_ref.relationship,
                    endpoint: endpoint_ref.endpoint,
                    property,
                },
                output_name,
            )))
        }
        _ => Ok(None),
    }
}

pub(super) fn compile_optional_same_label_undirected_relationship_endpoint(
    expression: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<SameLabelUndirectedEndpointRef>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_optional_same_label_undirected_relationship_endpoint(inner, path, plan, context)
        }
        Expression::FunctionCall(function)
            if is_start_node_function(function) || is_end_node_function(function) =>
        {
            let endpoint = match relationship_endpoint_function(function) {
                Some(RelationshipEndpoint::Start) => UndirectedRelationshipEndpoint::Start,
                Some(RelationshipEndpoint::End) => UndirectedRelationshipEndpoint::End,
                None => return Ok(None),
            };
            let relationship_variable = compile_single_variable_function_argument(
                function,
                format!("{path}.arguments"),
                match endpoint {
                    UndirectedRelationshipEndpoint::Start => {
                        "startNode() supports exactly one relationship variable argument"
                    }
                    UndirectedRelationshipEndpoint::End => {
                        "endNode() supports exactly one relationship variable argument"
                    }
                },
                context,
            )?;
            let Some(relationship) = plan.relationships.iter().find(|relationship| {
                relationship.variable.as_deref() == Some(relationship_variable.as_str())
            }) else {
                return Ok(None);
            };
            if relationship.direction != Direction::Undirected {
                return Ok(None);
            }
            let left_label =
                node_label_for_variable(plan, &relationship.left, format!("{path}.left"))?;
            let right_label =
                node_label_for_variable(plan, &relationship.right, format!("{path}.right"))?;
            if left_label != right_label {
                return Ok(None);
            }
            Ok(Some(SameLabelUndirectedEndpointRef {
                relationship: relationship_variable,
                endpoint,
                label: left_label.to_string(),
            }))
        }
        Expression::FunctionCall(function) if is_properties_function(function) => {
            let [argument] = function.arguments.as_slice() else {
                return Ok(None);
            };
            compile_optional_same_label_undirected_relationship_endpoint(
                argument,
                format!("{path}.arguments[0]"),
                plan,
                context,
            )
        }
        _ => Ok(None),
    }
}

pub(super) fn compile_optional_same_label_undirected_endpoint_function_argument(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<SameLabelUndirectedEndpointRef>, CoreError> {
    let path = path.into();
    match function.arguments.as_slice() {
        [argument] => compile_optional_same_label_undirected_relationship_endpoint(
            argument,
            format!("{path}[0]"),
            plan,
            context,
        ),
        [] | [_, ..] => Ok(None),
    }
}

pub(super) fn compile_optional_endpoint_property_ref(
    expression: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<(PropertyRef, String, String)>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_optional_endpoint_property_ref(inner, path, plan, context)
        }
        Expression::PropertyLookup { base, property, .. } => {
            let Some(endpoint) = compile_optional_relationship_endpoint_ref_from_expression(
                base,
                format!("{path}.base"),
                plan,
                context,
            )?
            else {
                return Ok(None);
            };
            let Some(presence_variable) = endpoint.presence_variable else {
                return Ok(None);
            };
            let output_name = format!("{}_{}", endpoint.variable, property.name.name);
            Ok(Some((
                PropertyRef {
                    variable: endpoint.variable,
                    property: property.name.name.clone(),
                },
                presence_variable,
                output_name,
            )))
        }
        Expression::ListIndex { list, index, .. } => {
            let Some(endpoint) = compile_optional_relationship_endpoint_ref_from_expression(
                list,
                format!("{path}.list"),
                plan,
                context,
            )?
            else {
                return Ok(None);
            };
            let Some(presence_variable) = endpoint.presence_variable else {
                return Ok(None);
            };
            let property = compile_property_index_name(index, format!("{path}.index"), context)?;
            let output_name = format!("{}_{}", endpoint.variable, property);
            Ok(Some((
                PropertyRef {
                    variable: endpoint.variable,
                    property,
                },
                presence_variable,
                output_name,
            )))
        }
        _ => Ok(None),
    }
}

pub(super) fn compile_optional_relationship_endpoint_ref_from_expression(
    expression: &Expression,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<Option<GraphValueRef>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_optional_relationship_endpoint_ref_from_expression(inner, path, plan, context)
        }
        Expression::FunctionCall(function)
            if is_start_node_function(function) || is_end_node_function(function) =>
        {
            Ok(Some(compile_relationship_endpoint_ref(
                function, path, plan, context,
            )?))
        }
        Expression::FunctionCall(function) if is_properties_function(function) => {
            match function.arguments.as_slice() {
                [argument] => compile_optional_relationship_endpoint_ref_from_expression(
                    argument,
                    format!("{path}.arguments[0]"),
                    plan,
                    context,
                ),
                [] => {
                    let Some(sources) = context.function_argument_sources(function) else {
                        return Ok(None);
                    };
                    let [source] = sources.arguments.as_slice() else {
                        return Ok(None);
                    };
                    let (argument, fragment_context) = parse_cypher_expression_fragment(
                        source,
                        format!("{path}.arguments[0]"),
                        context,
                    )?;
                    compile_optional_relationship_endpoint_ref_from_expression(
                        &argument,
                        format!("{path}.arguments[0]"),
                        plan,
                        &fragment_context,
                    )
                }
                _ => Ok(None),
            }
        }
        _ => Ok(None),
    }
}

pub(super) fn compile_property_base_variable(
    expression: &Expression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<String, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_property_base_variable(inner, path, plan, context)
        }
        Expression::Variable(variable) => Ok(variable_name(variable)),
        Expression::FunctionCall(function) if is_properties_function(function) => {
            compile_properties_function_base_variable(function, path, plan, context)
        }
        Expression::FunctionCall(function)
            if is_start_node_function(function) || is_end_node_function(function) =>
        {
            let Some(plan) = plan else {
                return Err(unsupported(
                    path,
                    "relationship endpoint property references require graph context",
                ));
            };
            compile_relationship_endpoint_variable(function, path, plan, context)
        }
        _ => Err(unsupported(
            path,
            "property references must be variable.property or startNode()/endNode() relationship endpoint properties",
        )),
    }
}

pub(super) fn compile_properties_function_base_variable(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<String, CoreError> {
    let path = path.into();
    match function.arguments.as_slice() {
        [argument] => compile_property_base_variable(
            argument,
            format!("{path}.arguments[0]"),
            plan,
            context,
        ),
        [] => context
            .variable_function_argument(function)
            .map(str::to_string)
            .ok_or_else(|| {
                unsupported(
                    path,
                    "properties() supports exactly one graph variable or relationship endpoint argument when followed by a property lookup",
                )
            }),
        _ => Err(unsupported(
            path,
            "properties() supports exactly one graph variable or relationship endpoint argument when followed by a property lookup",
        )),
    }
}

pub(super) fn compile_relationship_endpoint_variable(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<String, CoreError> {
    let path = path.into();
    let value = compile_relationship_endpoint_ref(function, path.clone(), plan, context)?;
    reject_optional_graph_value_ref(value, path)
}

pub(super) fn compile_relationship_endpoint_ref(
    function: &FunctionInvocation,
    path: impl Into<String>,
    plan: &GraphPlan,
    context: &CypherCompileContext,
) -> Result<GraphValueRef, CoreError> {
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
    let (relationship_index, relationship) = plan
        .relationships
        .iter()
        .enumerate()
        .find(|(_, relationship)| relationship.variable.as_deref() == Some(variable.as_str()))
        .ok_or_else(|| {
            unsupported(
                format!("{path}.arguments[0]"),
                format!(
                    "{function_name}() argument '{variable}' is not a named relationship variable"
                ),
            )
        })?;
    let endpoint_variable = match relationship.direction {
        Direction::Outgoing => Ok(match endpoint {
            RelationshipEndpoint::Start => relationship.left.clone(),
            RelationshipEndpoint::End => relationship.right.clone(),
        }),
        Direction::Incoming => Ok(match endpoint {
            RelationshipEndpoint::Start => relationship.right.clone(),
            RelationshipEndpoint::End => relationship.left.clone(),
        }),
        Direction::Undirected => resolve_undirected_relationship_endpoint_variable(
            relationship,
            endpoint,
            function_name,
            plan,
            context,
            path,
        ),
    }?;
    Ok(GraphValueRef {
        variable: endpoint_variable,
        presence_variable: plan
            .optional_relationships
            .contains(&relationship_index)
            .then_some(variable),
    })
}

pub(super) fn resolve_undirected_relationship_endpoint_variable(
    relationship: &RelationshipPattern,
    endpoint: RelationshipEndpoint,
    function_name: &str,
    plan: &GraphPlan,
    context: &CypherCompileContext,
    path: impl Into<String>,
) -> Result<String, CoreError> {
    let path = path.into();
    let left_label =
        node_label_for_variable(plan, &relationship.left, format!("{path}.left"))?.to_string();
    let right_label =
        node_label_for_variable(plan, &relationship.right, format!("{path}.right"))?.to_string();
    if left_label == right_label {
        return Err(unsupported(
            path,
            format!(
                "{function_name}() over undirected relationships is not supported yet because endpoint orientation is data-dependent"
            ),
        ));
    }
    let graph = context.graph.as_ref().ok_or_else(|| {
        unsupported(
            path.clone(),
            format!(
                "{function_name}() over cross-label undirected relationships requires a graph declaration so Coral can resolve mapping orientation"
            ),
        )
    })?;
    let matches = graph
        .relationships_for_type(&relationship.relationship_type)
        .filter(|mapping| {
            relationship_mapping_matches_pattern(
                mapping,
                Direction::Undirected,
                &left_label,
                &right_label,
            )
        })
        .collect::<Vec<_>>();
    let [mapping] = matches.as_slice() else {
        return Err(unsupported(
            path,
            format!(
                "{function_name}() over undirected relationship type '{}' for {left_label} and {right_label} requires exactly one graph declaration mapping",
                relationship.relationship_type
            ),
        ));
    };
    if mapping.from.label == left_label && mapping.to.label == right_label {
        return Ok(match endpoint {
            RelationshipEndpoint::Start => relationship.left.clone(),
            RelationshipEndpoint::End => relationship.right.clone(),
        });
    }
    if mapping.from.label == right_label && mapping.to.label == left_label {
        return Ok(match endpoint {
            RelationshipEndpoint::Start => relationship.right.clone(),
            RelationshipEndpoint::End => relationship.left.clone(),
        });
    }
    Err(CoreError::internal(
        "undirected endpoint mapping matched neither pattern orientation",
    ))
}

pub(super) fn node_label_for_variable<'a>(
    plan: &'a GraphPlan,
    variable: &str,
    path: impl Into<String>,
) -> Result<&'a str, CoreError> {
    let path = path.into();
    plan.nodes
        .iter()
        .find(|node| node.variable == variable)
        .map(|node| node.label.as_str())
        .ok_or_else(|| {
            unsupported(
                path,
                format!("relationship endpoint variable '{variable}' is not a named node"),
            )
        })
}

pub(super) fn compile_literal_list(
    expression: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Vec<Literal>, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_literal_list(inner, path, context),
        Expression::Literal(CypherLiteral::List(list)) => list
            .elements
            .iter()
            .enumerate()
            .map(|(index, expression)| {
                compile_literal_list_element(expression, format!("{path}[{index}]"), context)
            })
            .collect(),
        Expression::Parameter(parameter) => {
            match context.parameter_value(parameter, path.clone())? {
                CypherParameterValue::List(values) => Ok(values.clone()),
                CypherParameterValue::Literal(_) => Err(unsupported(
                    path,
                    "IN parameter right-hand sides require a list value",
                )),
            }
        }
        Expression::ListSlice {
            list, start, end, ..
        } => compile_literal_list_slice(list, start.as_deref(), end.as_deref(), path, context),
        _ => Err(unsupported(
            path,
            "IN predicates require a literal list right-hand side",
        )),
    }
}

pub(super) fn compile_static_list_in_rhs_value(
    expression: &Expression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<StaticListValue, CoreError> {
    let path = path.into();
    let Some(value) = compile_optional_static_list_value(expression, path.clone(), plan, context)?
    else {
        let literals = compile_literal_list(expression, path, context)?;
        return Ok(StaticListValue {
            presence_variable: None,
            element_type: infer_literal_list_element_type(&literals),
            literals,
        });
    };
    Ok(value)
}

pub(super) fn compile_static_list_in_rhs_literals(
    expression: &Expression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Vec<Literal>, CoreError> {
    Ok(compile_static_list_in_rhs_value(expression, path, plan, context)?.literals)
}

pub(super) fn compile_literal_list_slice(
    list: &Expression,
    start: Option<&Expression>,
    end: Option<&Expression>,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Vec<Literal>, CoreError> {
    let path = path.into();
    let values = compile_literal_list(list, format!("{path}.list"), context)?;
    compile_list_slice_literals(
        &values,
        start,
        end,
        path,
        context,
        "literal list slice bounds require integer literals or scalar integer parameters",
    )
}

pub(super) fn compile_list_slice_literals(
    values: &[Literal],
    start: Option<&Expression>,
    end: Option<&Expression>,
    path: impl Into<String>,
    context: &CypherCompileContext,
    bound_message: &'static str,
) -> Result<Vec<Literal>, CoreError> {
    let path = path.into();
    let len = i64::try_from(values.len())
        .map_err(|error| CoreError::internal(format!("list length overflow: {error}")))?;
    let start = compile_list_slice_bound(
        start,
        0,
        len,
        format!("{path}.start"),
        context,
        bound_message,
    )?;
    let end =
        compile_list_slice_bound(end, len, len, format!("{path}.end"), context, bound_message)?;
    if start >= end {
        return Ok(Vec::new());
    }
    values
        .get(start..end)
        .map(<[Literal]>::to_vec)
        .ok_or_else(|| CoreError::internal("list slice bounds were invalid after checking"))
}

pub(super) fn compile_list_slice_bound(
    bound: Option<&Expression>,
    default: i64,
    len: i64,
    path: impl Into<String>,
    context: &CypherCompileContext,
    message: &'static str,
) -> Result<usize, CoreError> {
    let path = path.into();
    let Some(bound) = bound else {
        return usize::try_from(default).map_err(|error| {
            CoreError::internal(format!("list default slice bound overflow: {error}"))
        });
    };
    let bound = compile_literal(bound, path.clone(), context)?;
    let Literal::Integer(bound) = bound else {
        return Err(unsupported(path, message));
    };
    let normalized = if bound < 0 { len + bound } else { bound };
    usize::try_from(normalized.clamp(0, len))
        .map_err(|error| CoreError::internal(format!("list slice bound overflow: {error}")))
}

pub(super) fn compile_literal_list_index(
    list: &Expression,
    index: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Literal, CoreError> {
    let path = path.into();
    let values = compile_literal_list(list, format!("{path}.list"), context)?;
    compile_list_index_literal(
        &values,
        index,
        path,
        context,
        "literal list indexes require an integer literal or scalar integer parameter",
    )
}

pub(super) fn compile_list_index_literal(
    values: &[Literal],
    index: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
    message: &'static str,
) -> Result<Literal, CoreError> {
    let path = path.into();
    let index = compile_literal(index, format!("{path}.index"), context)?;
    let Literal::Integer(index) = index else {
        return Err(unsupported(format!("{path}.index"), message));
    };
    let len = i64::try_from(values.len())
        .map_err(|error| CoreError::internal(format!("list length overflow: {error}")))?;
    let normalized = if index < 0 { len + index } else { index };
    if normalized < 0 || normalized >= len {
        return Ok(Literal::Null);
    }
    let index = usize::try_from(normalized).map_err(|error| {
        CoreError::internal(format!("list index normalization failed: {error}"))
    })?;
    values
        .get(index)
        .cloned()
        .ok_or_else(|| CoreError::internal("list index was out of bounds after checking"))
}

pub(super) fn compile_literal(
    expression: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Literal, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_literal(inner, path, context),
        Expression::PropertyLookup { .. } => {
            compile_static_map_lookup_literal(expression, path, context)
        }
        Expression::ListIndex { list, index, .. } => {
            if literal_map_expression(list).is_some() {
                compile_static_map_lookup_literal(expression, path, context)
            } else {
                compile_literal_list_index(list, index, path, context)
            }
        }
        Expression::Literal(CypherLiteral::String(value)) => {
            Ok(Literal::String(value.value.clone()))
        }
        Expression::Literal(CypherLiteral::Number(NumberLiteral::Integer(value))) => {
            Ok(Literal::Integer(*value))
        }
        Expression::Literal(CypherLiteral::Number(NumberLiteral::Float(value))) => {
            compile_float_literal(*value, path)
        }
        Expression::Literal(CypherLiteral::Boolean(value)) => Ok(Literal::Boolean(*value)),
        Expression::Literal(CypherLiteral::Null) => Ok(Literal::Null),
        Expression::UnaryOp {
            op: UnaryOperator::Negate,
            operand,
            ..
        } => match compile_literal(operand, path, context)? {
            Literal::Integer(value) => Ok(Literal::Integer(-value)),
            Literal::Float(value) => Ok(Literal::Float(OrderedFloat(-value.into_inner()))),
            _ => Err(unsupported(
                "literal",
                "only numeric literals can be negated",
            )),
        },
        Expression::Parameter(parameter) => {
            match context.parameter_value(parameter, path.clone())? {
                CypherParameterValue::Literal(value) => Ok(value.clone()),
                CypherParameterValue::List(_) => Err(unsupported(
                    path,
                    "list parameters can only be used as IN right-hand sides",
                )),
            }
        }
        _ => Err(unsupported(
            path,
            "only string, numeric, boolean, and null literals are supported",
        )),
    }
}

pub(super) fn compile_literal_list_element(
    expression: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Literal, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_literal_list_element(inner, path, context),
        Expression::Literal(CypherLiteral::List(_)) => {
            compile_literal_list(expression, path, context).map(Literal::List)
        }
        _ => compile_literal(expression, path, context),
    }
}

pub(super) fn compile_optional_static_map_lookup_literal(
    expression: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Option<Literal>, CoreError> {
    if !is_static_map_lookup_expression(expression) {
        return Ok(None);
    }
    compile_static_map_lookup_literal(expression, path, context).map(Some)
}

pub(super) fn compile_optional_static_map_lookup_scalar_expression(
    expression: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    if !is_static_map_lookup_expression(expression) {
        return Ok(None);
    }
    compile_static_map_lookup_scalar_expression(expression, path, mode, context).map(Some)
}

pub(super) fn compile_optional_non_literal_static_map_lookup_scalar_expression(
    expression: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<Option<ScalarExpression>, CoreError> {
    let path = path.into();
    if !is_static_map_lookup_expression(expression) {
        return Ok(None);
    }
    if compile_static_map_lookup_literal(expression, path.clone(), context).is_ok() {
        return Ok(None);
    }
    compile_static_map_lookup_scalar_expression(expression, path, mode, context).map(Some)
}

pub(super) fn compile_optional_static_map_lookup_property_ref(
    expression: &Expression,
    path: impl Into<String>,
    plan: Option<&GraphPlan>,
    context: &CypherCompileContext,
) -> Result<Option<PropertyRef>, CoreError> {
    let Some(expression) = compile_optional_non_literal_static_map_lookup_scalar_expression(
        expression,
        path,
        PredicateCompileMode::CaseWhen { plan },
        context,
    )?
    else {
        return Ok(None);
    };
    match expression {
        ScalarExpression::Property(property) => Ok(Some(property)),
        _ => Ok(None),
    }
}

pub(super) fn compile_static_map_lookup_scalar_expression(
    expression: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => {
            compile_static_map_lookup_scalar_expression(inner, path, mode, context)
        }
        Expression::PropertyLookup { base, property, .. } => {
            let Some(map) = literal_map_expression(base) else {
                return Err(unsupported(
                    path,
                    "static map property lookups require a literal map base",
                ));
            };
            compile_static_map_key_scalar_expression(map, &property.name.name, path, mode, context)
        }
        Expression::ListIndex { list, index, .. } => {
            let Some(map) = literal_map_expression(list) else {
                return Err(unsupported(
                    path,
                    "static map index lookups require a literal map base",
                ));
            };
            let key = compile_property_index_name(index, format!("{path}.index"), context)?;
            compile_static_map_key_scalar_expression(map, &key, path, mode, context)
        }
        _ => Err(unsupported(
            path,
            "static map lookups must be map.property or map['property'] expressions",
        )),
    }
}

pub(super) fn compile_static_map_key_scalar_expression(
    map: &MapLiteral,
    key: &str,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    let Some((_, value)) = map
        .entries
        .iter()
        .rev()
        .find(|(entry_key, _)| entry_key.name.name == key)
    else {
        return Ok(ScalarExpression::Literal(Literal::Null));
    };
    compile_static_map_value_scalar_expression(value, format!("{path}.value"), mode, context)
}

pub(super) fn compile_static_map_value_scalar_expression(
    value: &Expression,
    path: impl Into<String>,
    mode: PredicateCompileMode<'_>,
    context: &CypherCompileContext,
) -> Result<ScalarExpression, CoreError> {
    let path = path.into();
    if let Some(source) = context.truncated_inline_property_value_source(value) {
        let (expression, fragment_context) =
            parse_cypher_expression_fragment(&source.source, path.clone(), context)?;
        return compile_scalar_expression_in_predicate_mode(
            &expression,
            path,
            mode,
            &fragment_context,
        );
    }
    compile_scalar_expression_in_predicate_mode(value, path, mode, context)
}

pub(super) fn compile_static_map_lookup_literal(
    expression: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Literal, CoreError> {
    let path = path.into();
    match expression {
        Expression::Parenthesized(inner) => compile_static_map_lookup_literal(inner, path, context),
        Expression::PropertyLookup { base, property, .. } => {
            let Some(map) = literal_map_expression(base) else {
                return Err(unsupported(
                    path,
                    "literal map property lookups require a literal map base",
                ));
            };
            compile_static_map_key_literal(map, &property.name.name, path, context)
        }
        Expression::ListIndex { list, index, .. } => {
            let Some(map) = literal_map_expression(list) else {
                return Err(unsupported(
                    path,
                    "literal map index lookups require a literal map base",
                ));
            };
            let key = compile_property_index_name(index, format!("{path}.index"), context)?;
            compile_static_map_key_literal(map, &key, path, context)
        }
        _ => Err(unsupported(
            path,
            "literal map lookups must be map.property or map['property'] expressions",
        )),
    }
}

pub(super) fn compile_static_map_key_literal(
    map: &MapLiteral,
    key: &str,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Literal, CoreError> {
    let path = path.into();
    let Some((_, value)) = map
        .entries
        .iter()
        .rev()
        .find(|(entry_key, _)| entry_key.name.name == key)
    else {
        return Ok(Literal::Null);
    };
    compile_static_map_value_literal(value, format!("{path}.value"), context)
}

pub(super) fn compile_static_map_value_literal(
    value: &Expression,
    path: impl Into<String>,
    context: &CypherCompileContext,
) -> Result<Literal, CoreError> {
    let path = path.into();
    if let Some(source) = context.truncated_inline_property_value_source(value) {
        let (expression, fragment_context) =
            parse_cypher_expression_fragment(&source.source, path.clone(), context)?;
        return compile_literal(&expression, path, &fragment_context);
    }
    compile_literal(value, path, context)
}

pub(super) fn literal_map_expression(expression: &Expression) -> Option<&MapLiteral> {
    match expression {
        Expression::Parenthesized(inner) => literal_map_expression(inner),
        Expression::Literal(CypherLiteral::Map(map)) => Some(map),
        _ => None,
    }
}

pub(super) fn is_static_map_lookup_expression(expression: &Expression) -> bool {
    match expression {
        Expression::Parenthesized(inner) => is_static_map_lookup_expression(inner),
        Expression::PropertyLookup { base, .. } => literal_map_expression(base).is_some(),
        Expression::ListIndex { list, .. } => literal_map_expression(list).is_some(),
        _ => false,
    }
}

pub(super) fn compile_float_literal(
    value: f64,
    path: impl Into<String>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    if value.is_finite() {
        Ok(Literal::Float(OrderedFloat(value)))
    } else {
        Err(unsupported(
            path,
            "non-finite floating-point literals are not supported",
        ))
    }
}

pub(super) fn compile_non_negative_integer(
    expression: &Expression,
    path: impl Into<String>,
    keyword: &str,
    context: &CypherCompileContext,
) -> Result<u64, CoreError> {
    let path = path.into();
    let literal = match compile_optional_static_literal_scalar_operand(
        expression,
        path.clone(),
        PredicateCompileMode::CaseWhen { plan: None },
        context,
    )? {
        Some(literal) => literal,
        None => compile_literal(expression, path.clone(), context)?,
    };
    match literal {
        Literal::Integer(value) => u64::try_from(value).map_err(|conversion_error| {
            unsupported(
                path.clone(),
                format!("{keyword} must be a non-negative integer literal: {conversion_error}"),
            )
        }),
        _ => Err(unsupported(
            path,
            format!("{keyword} must be a non-negative integer literal"),
        )),
    }
}

pub(super) fn compile_comparison_operator(
    operator: CypherComparisonOperator,
) -> ComparisonOperator {
    match operator {
        CypherComparisonOperator::Eq => ComparisonOperator::Equal,
        CypherComparisonOperator::Ne => ComparisonOperator::NotEqual,
        CypherComparisonOperator::Gt => ComparisonOperator::GreaterThan,
        CypherComparisonOperator::Ge => ComparisonOperator::GreaterThanOrEqual,
        CypherComparisonOperator::Lt => ComparisonOperator::LessThan,
        CypherComparisonOperator::Le => ComparisonOperator::LessThanOrEqual,
        CypherComparisonOperator::StartsWith => ComparisonOperator::StartsWith,
        CypherComparisonOperator::EndsWith => ComparisonOperator::EndsWith,
        CypherComparisonOperator::Contains => ComparisonOperator::Contains,
        CypherComparisonOperator::RegexMatch => ComparisonOperator::RegexMatch,
    }
}

pub(super) fn invert_comparison_operator(
    operator: ComparisonOperator,
    path: impl Into<String>,
) -> Result<ComparisonOperator, CoreError> {
    match operator {
        ComparisonOperator::Equal => Ok(ComparisonOperator::Equal),
        ComparisonOperator::NotEqual => Ok(ComparisonOperator::NotEqual),
        ComparisonOperator::GreaterThan => Ok(ComparisonOperator::LessThan),
        ComparisonOperator::GreaterThanOrEqual => Ok(ComparisonOperator::LessThanOrEqual),
        ComparisonOperator::LessThan => Ok(ComparisonOperator::GreaterThan),
        ComparisonOperator::LessThanOrEqual => Ok(ComparisonOperator::GreaterThanOrEqual),
        ComparisonOperator::In
        | ComparisonOperator::StartsWith
        | ComparisonOperator::EndsWith
        | ComparisonOperator::Contains
        | ComparisonOperator::RegexMatch => Err(unsupported(
            path,
            "this comparison operator requires a variable.property left-hand side",
        )),
    }
}

pub(super) fn is_string_comparison_operator(operator: ComparisonOperator) -> bool {
    matches!(
        operator,
        ComparisonOperator::StartsWith
            | ComparisonOperator::EndsWith
            | ComparisonOperator::Contains
            | ComparisonOperator::RegexMatch
    )
}
