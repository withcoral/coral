//! Context-free reference validation: rejects unsupported path-variable and
//! scalar-alias references, and out-of-scope graph-variable references, over the
//! decypher AST and `GraphPlan`, reading only `&CypherCompileState` — no compile context.

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "Reference-validation helpers intentionally inherit parent-private Cypher helpers."
)]
use super::*;

pub(super) fn reject_ignored_path_variable_references(
    plan: &GraphPlan,
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    if state.path_variables.is_empty() && state.out_of_scope_graph_names.is_empty() {
        return Ok(());
    }
    for (index, projection) in plan.projections.iter().enumerate() {
        reject_ignored_path_variable_references_in_projection(
            projection,
            state,
            format!("{path}.projections[{index}]"),
        )?;
    }
    for (index, predicate) in plan.predicates.iter().enumerate() {
        reject_ignored_path_variable_references_in_property_predicate(
            predicate,
            state,
            format!("{path}.predicates[{index}]"),
        )?;
    }
    if let Some(predicate) = &plan.predicate {
        reject_ignored_path_variable_references_in_predicate(
            predicate,
            state,
            format!("{path}.predicate"),
        )?;
    }
    for (index, optional_match) in plan.optional_matches.iter().enumerate() {
        if let Some(predicate) = &optional_match.predicate {
            reject_ignored_path_variable_references_in_predicate(
                predicate,
                state,
                format!("{path}.optional_matches[{index}].predicate"),
            )?;
        }
    }
    for (index, order_key) in plan.order_by.iter().enumerate() {
        reject_ignored_path_variable_references_in_order_expression(
            &order_key.expression,
            state,
            format!("{path}.order_by[{index}]"),
        )?;
    }
    Ok(())
}

pub(super) fn reject_ignored_path_variable_references_in_projection(
    projection: &Projection,
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    match projection {
        Projection::Property { property, .. } => {
            reject_ignored_path_variable_property_ref(property, state, path)
        }
        Projection::Key { variable, .. }
        | Projection::ElementId { variable, .. }
        | Projection::RelationshipType { variable, .. }
        | Projection::NodeLabels { variable, .. }
        | Projection::PropertyKeys { variable, .. } => {
            reject_ignored_path_variable(variable, state, path)
        }
        Projection::Expression { expression, .. } => {
            reject_ignored_path_variable_references_in_scalar_expression(expression, state, path)
        }
        Projection::Aggregate { target, .. } => {
            reject_ignored_path_variable_references_in_aggregate_target(target, state, path)
        }
        Projection::Literal { .. }
        | Projection::LiteralList { .. }
        | Projection::CountAll { .. } => Ok(()),
    }
}

fn reject_ignored_path_variable_references_in_aggregate_target(
    target: &AggregateTarget,
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    match target {
        AggregateTarget::Property(property) => {
            reject_ignored_path_variable_property_ref(property, state, path)
        }
        AggregateTarget::PresenceGatedProperty {
            property,
            presence_variable,
        } => {
            reject_ignored_path_variable_property_ref(property, state, format!("{path}.property"))?;
            reject_ignored_path_variable(
                presence_variable,
                state,
                format!("{path}.presence_variable"),
            )
        }
        AggregateTarget::Expression(expression) => {
            reject_ignored_path_variable_references_in_scalar_expression(expression, state, path)
        }
        AggregateTarget::VariableKey { variable } => {
            reject_ignored_path_variable(variable, state, path)
        }
        AggregateTarget::PresenceGatedVariableKey {
            variable,
            presence_variable,
        } => {
            reject_ignored_path_variable(variable, state, format!("{path}.variable"))?;
            reject_ignored_path_variable(
                presence_variable,
                state,
                format!("{path}.presence_variable"),
            )
        }
    }
}

fn reject_ignored_path_variable_references_in_order_expression(
    expression: &OrderExpression,
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    match expression {
        OrderExpression::Property(property) => {
            reject_ignored_path_variable_property_ref(property, state, path)
        }
        OrderExpression::Key { variable }
        | OrderExpression::ElementId { variable }
        | OrderExpression::RelationshipType { variable, .. }
        | OrderExpression::NodeLabels { variable, .. }
        | OrderExpression::PropertyKeys { variable } => {
            reject_ignored_path_variable(variable, state, path)
        }
        OrderExpression::Aggregate { target, .. } => {
            reject_ignored_path_variable_references_in_aggregate_target(target, state, path)
        }
        OrderExpression::Scalar(expression) => {
            reject_ignored_path_variable_references_in_scalar_expression(expression, state, path)
        }
        OrderExpression::CountAll
        | OrderExpression::Literal(_)
        | OrderExpression::ProjectionAlias(_) => Ok(()),
    }
}

pub(super) fn reject_ignored_path_variable_references_in_predicate(
    expression: &PredicateExpression,
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    match expression {
        PredicateExpression::Boolean(_) => Ok(()),
        PredicateExpression::Comparison(predicate) => {
            reject_ignored_path_variable_references_in_property_predicate(predicate, state, path)
        }
        PredicateExpression::KeyComparison(predicate) => {
            reject_ignored_path_variable(&predicate.variable, state, format!("{path}.variable"))?;
            reject_ignored_path_variable_references_in_predicate_rhs(
                &predicate.rhs,
                state,
                format!("{path}.rhs"),
            )
        }
        PredicateExpression::ElementIdComparison(predicate) => {
            reject_ignored_path_variable(&predicate.variable, state, format!("{path}.variable"))?;
            reject_ignored_path_variable_references_in_predicate_rhs(
                &predicate.rhs,
                state,
                format!("{path}.rhs"),
            )
        }
        PredicateExpression::Presence(predicate) => {
            reject_ignored_path_variable(&predicate.variable, state, format!("{path}.variable"))
        }
        PredicateExpression::PropertyKeyMembership(predicate) => {
            reject_ignored_path_variable(&predicate.variable, state, format!("{path}.variable"))
        }
        PredicateExpression::ExistsPattern(predicate) => {
            for (index, node) in predicate.nodes.iter().enumerate() {
                reject_ignored_path_variable(
                    &node.variable,
                    state,
                    format!("{path}.nodes[{index}].variable"),
                )?;
            }
            for (index, relationship) in predicate.relationships.iter().enumerate() {
                if let Some(variable) = &relationship.variable {
                    reject_ignored_path_variable(
                        variable,
                        state,
                        format!("{path}.relationships[{index}].variable"),
                    )?;
                }
                reject_ignored_path_variable(
                    &relationship.left,
                    state,
                    format!("{path}.relationships[{index}].left"),
                )?;
                reject_ignored_path_variable(
                    &relationship.right,
                    state,
                    format!("{path}.relationships[{index}].right"),
                )?;
            }
            for (index, predicate) in predicate.predicates.iter().enumerate() {
                reject_ignored_path_variable_references_in_property_predicate(
                    predicate,
                    state,
                    format!("{path}.predicates[{index}]"),
                )?;
            }
            Ok(())
        }
        PredicateExpression::ScalarComparison(predicate) => {
            reject_ignored_path_variable_references_in_scalar_expression(
                &predicate.lhs,
                state,
                format!("{path}.lhs"),
            )?;
            reject_ignored_path_variable_references_in_scalar_predicate_rhs(
                &predicate.rhs,
                state,
                format!("{path}.rhs"),
            )
        }
        PredicateExpression::And { left, right }
        | PredicateExpression::Or { left, right }
        | PredicateExpression::Xor { left, right } => {
            reject_ignored_path_variable_references_in_predicate(
                left,
                state,
                format!("{path}.left"),
            )?;
            reject_ignored_path_variable_references_in_predicate(
                right,
                state,
                format!("{path}.right"),
            )
        }
        PredicateExpression::Not { expression } => {
            reject_ignored_path_variable_references_in_predicate(
                expression,
                state,
                format!("{path}.expression"),
            )
        }
    }
}

fn reject_ignored_path_variable_references_in_property_predicate(
    predicate: &PropertyPredicate,
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    reject_ignored_path_variable_property_ref(
        &predicate.property,
        state,
        format!("{path}.property"),
    )?;
    reject_ignored_path_variable_references_in_predicate_rhs(
        &predicate.rhs,
        state,
        format!("{path}.rhs"),
    )
}

fn reject_ignored_path_variable_references_in_predicate_rhs(
    rhs: &PredicateRhs,
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    match rhs {
        PredicateRhs::Property(property) => {
            reject_ignored_path_variable_property_ref(property, state, path)
        }
        PredicateRhs::Key { variable } | PredicateRhs::ElementId { variable } => {
            reject_ignored_path_variable(variable, state, path)
        }
        PredicateRhs::Literal(_)
        | PredicateRhs::TemporalCoercion { .. }
        | PredicateRhs::TemporalCoercionList(_)
        | PredicateRhs::List(_) => Ok(()),
    }
}

fn reject_ignored_path_variable_references_in_scalar_predicate_rhs(
    rhs: &ScalarPredicateRhs,
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    match rhs {
        ScalarPredicateRhs::Expression(expression) => {
            reject_ignored_path_variable_references_in_scalar_expression(expression, state, path)
        }
        ScalarPredicateRhs::List(_) => Ok(()),
    }
}

fn reject_ignored_path_variable_references_in_scalar_expression(
    expression: &ScalarExpression,
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    if let Some(expression) = unary_scalar_expression_operand(expression) {
        return reject_ignored_path_variable_references_in_scalar_expression(
            expression, state, path,
        );
    }

    match expression {
        ScalarExpression::Property(property) => {
            reject_ignored_path_variable_property_ref(property, state, path)
        }
        ScalarExpression::UndirectedEndpointProperty { relationship, .. }
        | ScalarExpression::UndirectedEndpointKey { relationship, .. }
        | ScalarExpression::UndirectedEndpointElementId { relationship, .. }
        | ScalarExpression::UndirectedEndpointLabels { relationship, .. }
        | ScalarExpression::UndirectedEndpointPropertyKeys { relationship, .. } => {
            reject_ignored_path_variable(relationship, state, path)
        }
        ScalarExpression::Literal(_)
        | ScalarExpression::LiteralList { .. }
        | ScalarExpression::TypedLiteralList { .. } => Ok(()),
        ScalarExpression::Predicate(predicate) => {
            reject_ignored_path_variable_references_in_predicate(predicate, state, path)
        }
        ScalarExpression::CountSubquery {
            pattern,
            distinct_target,
        } => {
            reject_ignored_path_variable_references_in_count_subquery(
                pattern,
                state,
                format!("{path}.pattern"),
            )?;
            if let Some(target) = distinct_target {
                reject_ignored_path_variable_references_in_scalar_expression(
                    target,
                    state,
                    format!("{path}.distinct_target"),
                )?;
            }
            Ok(())
        }
        ScalarExpression::CollectSubquery {
            pattern, target, ..
        } => {
            reject_ignored_path_variable_references_in_count_subquery(
                pattern,
                state,
                format!("{path}.pattern"),
            )?;
            reject_ignored_path_variable_references_in_scalar_expression(
                target,
                state,
                format!("{path}.target"),
            )
        }
        ScalarExpression::Key { variable }
        | ScalarExpression::ElementId { variable }
        | ScalarExpression::GraphIdentity { variable }
        | ScalarExpression::GraphPresence { variable }
        | ScalarExpression::PropertyKeys { variable }
        | ScalarExpression::RelationshipType { variable, .. }
        | ScalarExpression::NodeLabels { variable, .. } => {
            reject_ignored_path_variable(variable, state, path)
        }
        ScalarExpression::PresenceGated {
            presence_variable,
            expression,
        } => {
            reject_ignored_path_variable(
                presence_variable,
                state,
                format!("{path}.presence_variable"),
            )?;
            reject_ignored_path_variable_references_in_scalar_expression(
                expression,
                state,
                format!("{path}.expression"),
            )
        }
        _ => reject_ignored_path_variable_references_in_structural_scalar_expression(
            expression, state, path,
        ),
    }
}

fn reject_ignored_path_variable_references_in_structural_scalar_expression(
    expression: &ScalarExpression,
    state: &CypherCompileState,
    path: String,
) -> Result<(), CoreError> {
    if let Some((left, right)) = path_variable_scalar_pair_operands(expression) {
        return reject_path_variables_in_scalar_pair(left, right, state, path);
    }
    if let Some((first, second, third)) = path_variable_scalar_triple_operands(expression) {
        return reject_path_variables_in_scalar_triple(first, second, third, state, path);
    }
    if let Some((name, expression)) = temporal_scalar_single_operand(expression) {
        return reject_ignored_path_variable_references_in_scalar_expression(
            expression,
            state,
            format!("{path}.{name}"),
        );
    }

    match expression {
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
        }) => reject_path_variables_in_temporal_fields(
            [
                ("year", year),
                ("month", month),
                ("day", day),
                ("hour", hour),
                ("minute", minute),
                ("second", second),
                ("millisecond", millisecond),
                ("microsecond", microsecond),
                ("nanosecond", nanosecond),
            ],
            state,
            &path,
        ),
        ScalarExpression::Temporal(TemporalExpr::MakeLocalTime {
            hour,
            minute,
            second,
            millisecond,
            microsecond,
            nanosecond,
        }) => reject_path_variables_in_temporal_fields(
            [
                ("hour", hour),
                ("minute", minute),
                ("second", second),
                ("millisecond", millisecond),
                ("microsecond", microsecond),
                ("nanosecond", nanosecond),
            ],
            state,
            &path,
        ),
        ScalarExpression::Temporal(TemporalExpr::MakeDuration { .. }) => Ok(()),
        ScalarExpression::Temporal(TemporalExpr::DurationInUnits { start, end, .. }) => {
            reject_path_variables_in_temporal_fields([("start", start), ("end", end)], state, &path)
        }
        ScalarExpression::Coalesce { expressions } => {
            reject_path_variables_in_scalar_list(expressions, state, format!("{path}.expressions"))
        }
        ScalarExpression::Round { expression, places } => {
            reject_path_variables_in_scalar_optional_pair(
                ("expression", expression),
                ("places", places.as_deref()),
                state,
                path,
            )
        }
        ScalarExpression::Substring {
            expression,
            start,
            length,
        } => reject_path_variables_in_substring_expression(
            expression,
            start,
            length.as_deref(),
            state,
            path,
        ),
        ScalarExpression::Case {
            alternatives,
            else_expression,
        } => reject_ignored_path_variable_references_in_case_expression(
            alternatives,
            else_expression.as_deref(),
            state,
            path,
        ),
        _ => {
            reject_ignored_path_variable_references_in_non_structural_scalar_expression(expression);
            Ok(())
        }
    }
}

fn temporal_scalar_single_operand(
    expression: &ScalarExpression,
) -> Option<(&'static str, &ScalarExpression)> {
    match expression {
        ScalarExpression::Temporal(
            TemporalExpr::DateFromString { text }
            | TemporalExpr::LocalDateTimeFromString { text }
            | TemporalExpr::LocalTimeFromString { text },
        ) => Some(("text", text)),
        ScalarExpression::Temporal(TemporalExpr::Component { expression, .. }) => {
            Some(("expression", expression))
        }
        _ => None,
    }
}

fn reject_path_variables_in_temporal_fields<const N: usize>(
    fields: [(&'static str, &ScalarExpression); N],
    state: &CypherCompileState,
    path: &str,
) -> Result<(), CoreError> {
    for (name, expression) in fields {
        reject_ignored_path_variable_references_in_scalar_expression(
            expression,
            state,
            format!("{path}.{name}"),
        )?;
    }
    Ok(())
}

fn reject_ignored_path_variable_references_in_non_structural_scalar_expression(
    expression: &ScalarExpression,
) {
    match expression {
        ScalarExpression::Property(_)
        | ScalarExpression::UndirectedEndpointProperty { .. }
        | ScalarExpression::UndirectedEndpointKey { .. }
        | ScalarExpression::UndirectedEndpointElementId { .. }
        | ScalarExpression::UndirectedEndpointLabels { .. }
        | ScalarExpression::UndirectedEndpointPropertyKeys { .. }
        | ScalarExpression::Literal(_)
        | ScalarExpression::LiteralList { .. }
        | ScalarExpression::TypedLiteralList { .. }
        | ScalarExpression::Predicate(_)
        | ScalarExpression::CountSubquery { .. }
        | ScalarExpression::CollectSubquery { .. }
        | ScalarExpression::Key { .. }
        | ScalarExpression::ElementId { .. }
        | ScalarExpression::GraphIdentity { .. }
        | ScalarExpression::GraphPresence { .. }
        | ScalarExpression::NodeLabels { .. }
        | ScalarExpression::PropertyKeys { .. }
        | ScalarExpression::PresenceGated { .. }
        | ScalarExpression::RelationshipType { .. } => {
            unreachable!("simple scalar expressions handled before structural path checks")
        }
        ScalarExpression::GraphKeyList { .. } | ScalarExpression::StageValue { .. } => {}
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
            unreachable!("unary scalar expressions handled before structural path checks")
        }
        ScalarExpression::Coalesce { .. }
        | ScalarExpression::NullIf { .. }
        | ScalarExpression::Round { .. }
        | ScalarExpression::Left { .. }
        | ScalarExpression::Right { .. }
        | ScalarExpression::StringIndices { .. }
        | ScalarExpression::LPad { .. }
        | ScalarExpression::RPad { .. }
        | ScalarExpression::StringContains { .. }
        | ScalarExpression::StringStartsWith { .. }
        | ScalarExpression::StringEndsWith { .. }
        | ScalarExpression::Replace { .. }
        | ScalarExpression::Substring { .. }
        | ScalarExpression::Temporal(_)
        | ScalarExpression::Arithmetic { .. }
        | ScalarExpression::ListConcat { .. }
        | ScalarExpression::Atan2 { .. }
        | ScalarExpression::Case { .. } => {
            unreachable!("structural scalar expressions handled before this path check")
        }
    }
}

fn reject_ignored_path_variable_references_in_count_subquery(
    pattern: &CountSubqueryPattern,
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    match pattern {
        CountSubqueryPattern::Relationships(pattern) => {
            reject_ignored_path_variable_references_in_exists_pattern(pattern, state, path)?;
        }
        CountSubqueryPattern::Nodes {
            nodes,
            predicates,
            predicate,
        } => {
            for (index, node) in nodes.iter().enumerate() {
                reject_ignored_path_variable(
                    &node.variable,
                    state,
                    format!("{path}.nodes[{index}].variable"),
                )?;
            }
            for (index, predicate) in predicates.iter().enumerate() {
                reject_ignored_path_variable_references_in_property_predicate(
                    predicate,
                    state,
                    format!("{path}.predicates[{index}]"),
                )?;
            }
            if let Some(predicate) = predicate {
                reject_ignored_path_variable_references_in_predicate(
                    predicate,
                    state,
                    format!("{path}.predicate"),
                )?;
            }
        }
    }
    Ok(())
}

fn reject_ignored_path_variable_references_in_exists_pattern(
    pattern: &ExistsPatternPredicate,
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    for (index, node) in pattern.nodes.iter().enumerate() {
        reject_ignored_path_variable(
            &node.variable,
            state,
            format!("{path}.nodes[{index}].variable"),
        )?;
    }
    for (index, relationship) in pattern.relationships.iter().enumerate() {
        if let Some(variable) = &relationship.variable {
            reject_ignored_path_variable(
                variable,
                state,
                format!("{path}.relationships[{index}].variable"),
            )?;
        }
        reject_ignored_path_variable(
            &relationship.left,
            state,
            format!("{path}.relationships[{index}].left"),
        )?;
        reject_ignored_path_variable(
            &relationship.right,
            state,
            format!("{path}.relationships[{index}].right"),
        )?;
    }
    for (index, predicate) in pattern.predicates.iter().enumerate() {
        reject_ignored_path_variable_references_in_property_predicate(
            predicate,
            state,
            format!("{path}.predicates[{index}]"),
        )?;
    }
    if let Some(predicate) = &pattern.predicate {
        reject_ignored_path_variable_references_in_predicate(
            predicate,
            state,
            format!("{path}.predicate"),
        )?;
    }
    Ok(())
}

fn reject_path_variables_in_scalar_list(
    expressions: &[ScalarExpression],
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    for (index, expression) in expressions.iter().enumerate() {
        reject_ignored_path_variable_references_in_scalar_expression(
            expression,
            state,
            format!("{path}[{index}]"),
        )?;
    }
    Ok(())
}

fn reject_path_variables_in_scalar_pair(
    left: (&str, &ScalarExpression),
    right: (&str, &ScalarExpression),
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    reject_ignored_path_variable_references_in_scalar_expression(
        left.1,
        state,
        format!("{path}.{}", left.0),
    )?;
    reject_ignored_path_variable_references_in_scalar_expression(
        right.1,
        state,
        format!("{path}.{}", right.0),
    )
}

fn reject_path_variables_in_scalar_triple(
    first: (&str, &ScalarExpression),
    second: (&str, &ScalarExpression),
    third: (&str, &ScalarExpression),
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    reject_ignored_path_variable_references_in_scalar_expression(
        first.1,
        state,
        format!("{path}.{}", first.0),
    )?;
    reject_ignored_path_variable_references_in_scalar_expression(
        second.1,
        state,
        format!("{path}.{}", second.0),
    )?;
    reject_ignored_path_variable_references_in_scalar_expression(
        third.1,
        state,
        format!("{path}.{}", third.0),
    )
}

fn reject_path_variables_in_scalar_optional_pair(
    required: (&str, &ScalarExpression),
    optional: (&str, Option<&ScalarExpression>),
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    reject_ignored_path_variable_references_in_scalar_expression(
        required.1,
        state,
        format!("{path}.{}", required.0),
    )?;
    if let Some(expression) = optional.1 {
        reject_ignored_path_variable_references_in_scalar_expression(
            expression,
            state,
            format!("{path}.{}", optional.0),
        )?;
    }
    Ok(())
}

fn reject_path_variables_in_substring_expression(
    expression: &ScalarExpression,
    start: &ScalarExpression,
    length: Option<&ScalarExpression>,
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    reject_ignored_path_variable_references_in_scalar_expression(
        expression,
        state,
        format!("{path}.expression"),
    )?;
    reject_ignored_path_variable_references_in_scalar_expression(
        start,
        state,
        format!("{path}.start"),
    )?;
    if let Some(length) = length {
        reject_ignored_path_variable_references_in_scalar_expression(
            length,
            state,
            format!("{path}.length"),
        )?;
    }
    Ok(())
}

fn reject_ignored_path_variable_references_in_case_expression(
    alternatives: &[ScalarCaseAlternative],
    else_expression: Option<&ScalarExpression>,
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    for (index, alternative) in alternatives.iter().enumerate() {
        reject_ignored_path_variable_references_in_predicate(
            &alternative.when,
            state,
            format!("{path}.alternatives[{index}].when"),
        )?;
        reject_ignored_path_variable_references_in_scalar_expression(
            &alternative.then,
            state,
            format!("{path}.alternatives[{index}].then"),
        )?;
    }
    if let Some(else_expression) = else_expression {
        reject_ignored_path_variable_references_in_scalar_expression(
            else_expression,
            state,
            format!("{path}.else"),
        )?;
    }
    Ok(())
}

fn reject_ignored_path_variable_property_ref(
    property: &PropertyRef,
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    reject_ignored_path_variable(&property.variable, state, path)
}

pub(super) fn reject_ignored_path_variable(
    variable: &str,
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    if state.out_of_scope_graph_names.contains(variable) {
        return Err(unsupported(
            path,
            format!("graph variable '{variable}' is not in scope after WITH"),
        ));
    }
    if state.path_variables.contains_key(variable) {
        return Err(unsupported(
            path,
            format!(
                "path variable '{variable}' cannot be used as a graph value because Coral does not materialize path values yet"
            ),
        ));
    }
    Ok(())
}

pub(super) fn reject_match_scalar_alias_conflicts(
    match_clause: &Match,
    state: &CypherCompileState,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    let aliases = scalar_alias_names(state);
    if aliases.is_empty() {
        return Ok(());
    }
    let variables = match_clause_bound_variables(match_clause);
    if let Some(conflict) = variables
        .iter()
        .find(|variable| aliases.contains(*variable))
    {
        return Err(unsupported(
            path,
            format!("MATCH variable '{conflict}' conflicts with an in-scope WITH scalar alias"),
        ));
    }
    Ok(())
}

pub(super) fn reject_optional_graph_value_ref(
    value: GraphValueRef,
    path: impl Into<String>,
) -> Result<String, CoreError> {
    let path = path.into();
    if value.presence_variable.is_some() {
        return Err(unsupported(
            path,
            "relationship endpoint values from OPTIONAL MATCH are only supported in scalar projections, predicates, and ordering expressions",
        ));
    }
    Ok(value.variable)
}
