//! IR variable-rename visitors: rewrite variable references across graph-plan IR
//! nodes (and hidden-graph-variable state) by substituting names via a renames map.
//! Self-contained — no compile context; drives only ir.rs AST types + `CypherCompileState`.

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "Cypher variable-rename visitors are split into a child module while preserving parent-private helper access."
)]
use super::*;

pub(super) fn rename_hidden_graph_variables(
    state: &mut CypherCompileState,
    renames: &BTreeMap<String, String>,
) {
    state.hidden_graph_variables = state
        .hidden_graph_variables
        .iter()
        .map(|variable| renames.get(variable).unwrap_or(variable).clone())
        .collect();
}

pub(super) fn rename_graph_plan_variables(
    plan: &mut GraphPlan,
    renames: &BTreeMap<String, String>,
) {
    for node in &mut plan.nodes {
        rename_string(&mut node.variable, renames);
    }
    for relationship in &mut plan.relationships {
        if let Some(variable) = &mut relationship.variable {
            rename_string(variable, renames);
        }
        rename_string(&mut relationship.left, renames);
        rename_string(&mut relationship.right, renames);
    }
    for projection in &mut plan.projections {
        rename_projection_variables(projection, renames);
    }
    for predicate in &mut plan.predicates {
        rename_property_predicate_variables(predicate, renames);
    }
    if let Some(predicate) = &mut plan.predicate {
        rename_predicate_expression_variables(predicate, renames);
    }
    for optional_match in &mut plan.optional_matches {
        if let Some(predicate) = &mut optional_match.predicate {
            rename_predicate_expression_variables(predicate, renames);
        }
    }
    for order_key in &mut plan.order_by {
        rename_order_expression_variables(&mut order_key.expression, renames);
    }
}

pub(super) fn rename_projection_variables(
    projection: &mut Projection,
    renames: &BTreeMap<String, String>,
) {
    match projection {
        Projection::Property { property, .. } => rename_property_ref_variables(property, renames),
        Projection::Key { variable, .. }
        | Projection::ElementId { variable, .. }
        | Projection::RelationshipType { variable, .. }
        | Projection::NodeLabels { variable, .. }
        | Projection::PropertyKeys { variable, .. } => rename_string(variable, renames),
        Projection::Expression { expression, .. } => {
            rename_scalar_expression_variables(expression, renames);
        }
        Projection::Aggregate { target, .. } => rename_aggregate_target_variables(target, renames),
        Projection::Literal { .. }
        | Projection::LiteralList { .. }
        | Projection::CountAll { .. } => {}
    }
}

fn rename_aggregate_target_variables(
    target: &mut AggregateTarget,
    renames: &BTreeMap<String, String>,
) {
    match target {
        AggregateTarget::Property(property) => rename_property_ref_variables(property, renames),
        AggregateTarget::PresenceGatedProperty {
            property,
            presence_variable,
        } => {
            rename_property_ref_variables(property, renames);
            rename_string(presence_variable, renames);
        }
        AggregateTarget::Expression(expression) => {
            rename_scalar_expression_variables(expression, renames);
        }
        AggregateTarget::VariableKey { variable } => rename_string(variable, renames),
        AggregateTarget::PresenceGatedVariableKey {
            variable,
            presence_variable,
        } => {
            rename_string(variable, renames);
            rename_string(presence_variable, renames);
        }
    }
}

fn rename_order_expression_variables(
    expression: &mut OrderExpression,
    renames: &BTreeMap<String, String>,
) {
    match expression {
        OrderExpression::Property(property) => rename_property_ref_variables(property, renames),
        OrderExpression::Key { variable }
        | OrderExpression::ElementId { variable }
        | OrderExpression::RelationshipType { variable, .. }
        | OrderExpression::NodeLabels { variable, .. }
        | OrderExpression::PropertyKeys { variable } => rename_string(variable, renames),
        OrderExpression::Aggregate { target, .. } => {
            rename_aggregate_target_variables(target, renames);
        }
        OrderExpression::Scalar(expression) => {
            rename_scalar_expression_variables(expression, renames);
        }
        OrderExpression::CountAll
        | OrderExpression::Literal(_)
        | OrderExpression::ProjectionAlias(_) => {}
    }
}

fn rename_predicate_expression_variables(
    expression: &mut PredicateExpression,
    renames: &BTreeMap<String, String>,
) {
    match expression {
        PredicateExpression::Boolean(_) => {}
        PredicateExpression::Comparison(predicate) => {
            rename_property_predicate_variables(predicate, renames);
        }
        PredicateExpression::KeyComparison(predicate) => {
            rename_string(&mut predicate.variable, renames);
            rename_predicate_rhs_variables(&mut predicate.rhs, renames);
        }
        PredicateExpression::ElementIdComparison(predicate) => {
            rename_string(&mut predicate.variable, renames);
            rename_predicate_rhs_variables(&mut predicate.rhs, renames);
        }
        PredicateExpression::Presence(predicate) => {
            rename_string(&mut predicate.variable, renames);
        }
        PredicateExpression::PropertyKeyMembership(predicate) => {
            rename_string(&mut predicate.variable, renames);
        }
        PredicateExpression::ExistsPattern(predicate) => {
            for node in &mut predicate.nodes {
                rename_string(&mut node.variable, renames);
            }
            for relationship in &mut predicate.relationships {
                if let Some(variable) = &mut relationship.variable {
                    rename_string(variable, renames);
                }
                rename_string(&mut relationship.left, renames);
                rename_string(&mut relationship.right, renames);
            }
            for predicate in &mut predicate.predicates {
                rename_property_predicate_variables(predicate, renames);
            }
            if let Some(predicate) = &mut predicate.predicate {
                rename_predicate_expression_variables(predicate, renames);
            }
        }
        PredicateExpression::ScalarComparison(predicate) => {
            rename_scalar_expression_variables(&mut predicate.lhs, renames);
            rename_scalar_predicate_rhs_variables(&mut predicate.rhs, renames);
        }
        PredicateExpression::And { left, right }
        | PredicateExpression::Or { left, right }
        | PredicateExpression::Xor { left, right } => {
            rename_predicate_expression_variables(left, renames);
            rename_predicate_expression_variables(right, renames);
        }
        PredicateExpression::Not { expression } => {
            rename_predicate_expression_variables(expression, renames);
        }
    }
}

fn rename_property_predicate_variables(
    predicate: &mut PropertyPredicate,
    renames: &BTreeMap<String, String>,
) {
    rename_property_ref_variables(&mut predicate.property, renames);
    rename_predicate_rhs_variables(&mut predicate.rhs, renames);
}

fn rename_predicate_rhs_variables(rhs: &mut PredicateRhs, renames: &BTreeMap<String, String>) {
    match rhs {
        PredicateRhs::Property(property) => rename_property_ref_variables(property, renames),
        PredicateRhs::Key { variable } | PredicateRhs::ElementId { variable } => {
            rename_string(variable, renames);
        }
        PredicateRhs::Literal(_) | PredicateRhs::List(_) => {}
    }
}

fn rename_scalar_predicate_rhs_variables(
    rhs: &mut ScalarPredicateRhs,
    renames: &BTreeMap<String, String>,
) {
    match rhs {
        ScalarPredicateRhs::Expression(expression) => {
            rename_scalar_expression_variables(expression, renames);
        }
        ScalarPredicateRhs::List(_) => {}
    }
}

fn rename_scalar_expression_variables(
    expression: &mut ScalarExpression,
    renames: &BTreeMap<String, String>,
) {
    if let Some(expression) = unary_scalar_expression_operand_mut(expression) {
        rename_scalar_expression_variables(expression, renames);
        return;
    }

    rename_non_unary_scalar_expression_variables(expression, renames);
}

fn rename_non_unary_scalar_expression_variables(
    expression: &mut ScalarExpression,
    renames: &BTreeMap<String, String>,
) {
    if let Some((left, right)) = binary_scalar_expression_operands_mut(expression) {
        rename_scalar_expression_variables(left, renames);
        rename_scalar_expression_variables(right, renames);
        return;
    }

    match expression {
        ScalarExpression::Property(property) => rename_property_ref_variables(property, renames),
        ScalarExpression::UndirectedEndpointProperty { relationship, .. }
        | ScalarExpression::UndirectedEndpointKey { relationship, .. }
        | ScalarExpression::UndirectedEndpointElementId { relationship, .. }
        | ScalarExpression::UndirectedEndpointLabels { relationship, .. }
        | ScalarExpression::UndirectedEndpointPropertyKeys { relationship, .. } => {
            rename_string(relationship, renames);
        }
        ScalarExpression::Literal(_)
        | ScalarExpression::LiteralList { .. }
        | ScalarExpression::TypedLiteralList { .. } => {}
        ScalarExpression::Predicate(predicate) => {
            rename_predicate_expression_variables(predicate, renames);
        }
        ScalarExpression::Key { variable }
        | ScalarExpression::ElementId { variable }
        | ScalarExpression::GraphIdentity { variable }
        | ScalarExpression::GraphPresence { variable }
        | ScalarExpression::PropertyKeys { variable }
        | ScalarExpression::RelationshipType { variable, .. }
        | ScalarExpression::NodeLabels { variable, .. } => {
            rename_string(variable, renames);
        }
        ScalarExpression::PresenceGated {
            presence_variable,
            expression,
        } => {
            rename_string(presence_variable, renames);
            rename_scalar_expression_variables(expression, renames);
        }
        ScalarExpression::Coalesce { expressions } => {
            rename_scalar_expression_list_variables(expressions, renames);
        }
        ScalarExpression::Round { expression, places } => {
            rename_scalar_expression_variables(expression, renames);
            if let Some(places) = places {
                rename_scalar_expression_variables(places, renames);
            }
        }
        ScalarExpression::Replace {
            expression,
            search,
            replacement,
        } => {
            rename_scalar_expression_variables(expression, renames);
            rename_scalar_expression_variables(search, renames);
            rename_scalar_expression_variables(replacement, renames);
        }
        ScalarExpression::Substring {
            expression,
            start,
            length,
        } => {
            rename_scalar_expression_variables(expression, renames);
            rename_scalar_expression_variables(start, renames);
            if let Some(length) = length {
                rename_scalar_expression_variables(length, renames);
            }
        }
        ScalarExpression::Case {
            alternatives,
            else_expression,
        } => {
            rename_case_expression_variables(alternatives, else_expression.as_deref_mut(), renames);
        }
        _ => {
            unreachable!("unary scalar expressions handled before structural rename")
        }
    }
}

fn binary_scalar_expression_operands_mut(
    expression: &mut ScalarExpression,
) -> Option<(&mut ScalarExpression, &mut ScalarExpression)> {
    match expression {
        ScalarExpression::NullIf { expression, value } => Some((expression, value)),
        ScalarExpression::Left { expression, count }
        | ScalarExpression::Right { expression, count } => Some((expression, count)),
        ScalarExpression::StringContains {
            expression,
            pattern,
        }
        | ScalarExpression::StringStartsWith {
            expression,
            pattern,
        }
        | ScalarExpression::StringEndsWith {
            expression,
            pattern,
        } => Some((expression, pattern)),
        ScalarExpression::Arithmetic { left, right, .. } => Some((left, right)),
        ScalarExpression::Atan2 { y, x } => Some((y, x)),
        _ => None,
    }
}

fn rename_scalar_expression_list_variables(
    expressions: &mut [ScalarExpression],
    renames: &BTreeMap<String, String>,
) {
    for expression in expressions {
        rename_scalar_expression_variables(expression, renames);
    }
}

fn rename_case_expression_variables(
    alternatives: &mut [ScalarCaseAlternative],
    else_expression: Option<&mut ScalarExpression>,
    renames: &BTreeMap<String, String>,
) {
    for alternative in alternatives {
        rename_predicate_expression_variables(&mut alternative.when, renames);
        rename_scalar_expression_variables(&mut alternative.then, renames);
    }
    if let Some(else_expression) = else_expression {
        rename_scalar_expression_variables(else_expression, renames);
    }
}

fn rename_property_ref_variables(property: &mut PropertyRef, renames: &BTreeMap<String, String>) {
    rename_string(&mut property.variable, renames);
}

fn rename_string(value: &mut String, renames: &BTreeMap<String, String>) {
    if let Some(replacement) = renames.get(value.as_str()) {
        *value = replacement.clone();
    }
}
