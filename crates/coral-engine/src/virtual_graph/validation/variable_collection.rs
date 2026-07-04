//! Collection of the graph variables referenced by predicate and scalar expressions.
//!
//! Pure AST traversal helpers (no `GraphPlanValidator` state) used by the validation
//! passes to determine which bound variables each expression depends on.

macro_rules! unary_scalar_expression_pattern {
    () => {
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
            | ScalarExpression::Negate { .. }
    };
}

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "Validation variable collection helpers are split into a child module while preserving parent-private access."
)]
use super::*;

#[allow(
    clippy::allow_attributes,
    clippy::elidable_lifetime_names,
    reason = "Validation child modules use the same explicit GraphPlanValidator lifetime shape as the parent impl."
)]
impl<'a> GraphPlanValidator<'a> {
    pub(super) fn collect_predicate_expression_variables<'b>(
        predicate: &'b PredicateExpression,
        variables: &mut BTreeSet<&'b str>,
    ) {
        match predicate {
            PredicateExpression::Boolean(_) => {}
            PredicateExpression::Comparison(predicate) => {
                Self::collect_property_predicate_variables(predicate, variables);
            }
            PredicateExpression::KeyComparison(predicate) => {
                variables.insert(predicate.variable.as_str());
                Self::collect_predicate_rhs_variables(&predicate.rhs, variables);
            }
            PredicateExpression::ElementIdComparison(predicate) => {
                variables.insert(predicate.variable.as_str());
                Self::collect_predicate_rhs_variables(&predicate.rhs, variables);
            }
            PredicateExpression::Presence(predicate) => {
                variables.insert(predicate.variable.as_str());
            }
            PredicateExpression::PropertyKeyMembership(predicate) => {
                variables.insert(predicate.variable.as_str());
            }
            PredicateExpression::ExistsPattern(predicate) => {
                Self::collect_exists_pattern_outer_variables(predicate, variables);
            }
            PredicateExpression::ScalarComparison(predicate) => {
                Self::collect_scalar_expression_variables(&predicate.lhs, variables);
                Self::collect_scalar_predicate_rhs_variables(&predicate.rhs, variables);
            }
            PredicateExpression::And { left, right }
            | PredicateExpression::Or { left, right }
            | PredicateExpression::Xor { left, right } => {
                Self::collect_predicate_expression_variables(left, variables);
                Self::collect_predicate_expression_variables(right, variables);
            }
            PredicateExpression::Not { expression } => {
                Self::collect_predicate_expression_variables(expression, variables);
            }
        }
    }

    fn collect_exists_pattern_outer_variables<'b>(
        predicate: &'b ExistsPatternPredicate,
        variables: &mut BTreeSet<&'b str>,
    ) {
        let local_variables = Self::exists_pattern_local_variables(predicate);
        for relationship in &predicate.relationships {
            if !local_variables.contains(relationship.left.as_str()) {
                variables.insert(relationship.left.as_str());
            }
            if !local_variables.contains(relationship.right.as_str()) {
                variables.insert(relationship.right.as_str());
            }
        }
        for property_predicate in &predicate.predicates {
            let mut predicate_variables = BTreeSet::new();
            Self::collect_property_predicate_variables(
                property_predicate,
                &mut predicate_variables,
            );
            variables.extend(
                predicate_variables
                    .into_iter()
                    .filter(|variable| !local_variables.contains(*variable)),
            );
        }
        if let Some(predicate) = &predicate.predicate {
            let mut predicate_variables = BTreeSet::new();
            Self::collect_predicate_expression_variables(predicate, &mut predicate_variables);
            variables.extend(
                predicate_variables
                    .into_iter()
                    .filter(|variable| !local_variables.contains(*variable)),
            );
        }
    }

    fn exists_pattern_local_variables(predicate: &ExistsPatternPredicate) -> BTreeSet<&str> {
        predicate
            .nodes
            .iter()
            .map(|node| node.variable.as_str())
            .chain(
                predicate
                    .relationships
                    .iter()
                    .filter_map(|relationship| relationship.variable.as_deref()),
            )
            .collect()
    }

    fn count_subquery_node_local_variables(nodes: &[NodePattern]) -> BTreeSet<&str> {
        nodes.iter().map(|node| node.variable.as_str()).collect()
    }

    fn collect_count_subquery_outer_variables<'b>(
        pattern: &'b CountSubqueryPattern,
        variables: &mut BTreeSet<&'b str>,
    ) {
        match pattern {
            CountSubqueryPattern::Relationships(predicate) => {
                Self::collect_exists_pattern_outer_variables(predicate, variables);
            }
            CountSubqueryPattern::Nodes {
                nodes,
                predicates,
                predicate,
            } => {
                let local_variables = Self::count_subquery_node_local_variables(nodes);
                for predicate in predicates {
                    let mut predicate_variables = BTreeSet::new();
                    Self::collect_property_predicate_variables(predicate, &mut predicate_variables);
                    variables.extend(
                        predicate_variables
                            .into_iter()
                            .filter(|variable| !local_variables.contains(*variable)),
                    );
                }
                if let Some(predicate) = predicate {
                    let mut predicate_variables = BTreeSet::new();
                    Self::collect_predicate_expression_variables(
                        predicate,
                        &mut predicate_variables,
                    );
                    variables.extend(
                        predicate_variables
                            .into_iter()
                            .filter(|variable| !local_variables.contains(*variable)),
                    );
                }
            }
        }
    }

    fn count_subquery_pattern_local_variables(pattern: &CountSubqueryPattern) -> BTreeSet<&str> {
        match pattern {
            CountSubqueryPattern::Relationships(predicate) => {
                Self::exists_pattern_local_variables(predicate)
            }
            CountSubqueryPattern::Nodes { nodes, .. } => {
                Self::count_subquery_node_local_variables(nodes)
            }
        }
    }

    fn collect_collect_subquery_outer_variables<'b>(
        pattern: &'b CountSubqueryPattern,
        target: &'b ScalarExpression,
        variables: &mut BTreeSet<&'b str>,
    ) {
        Self::collect_count_subquery_outer_variables(pattern, variables);
        let local_variables = Self::count_subquery_pattern_local_variables(pattern);
        let mut target_variables = BTreeSet::new();
        Self::collect_scalar_expression_variables(target, &mut target_variables);
        variables.extend(
            target_variables
                .into_iter()
                .filter(|variable| !local_variables.contains(*variable)),
        );
    }

    fn collect_property_predicate_variables<'b>(
        predicate: &'b PropertyPredicate,
        variables: &mut BTreeSet<&'b str>,
    ) {
        variables.insert(predicate.property.variable.as_str());
        Self::collect_predicate_rhs_variables(&predicate.rhs, variables);
    }

    fn collect_predicate_rhs_variables<'b>(
        rhs: &'b PredicateRhs,
        variables: &mut BTreeSet<&'b str>,
    ) {
        match rhs {
            PredicateRhs::Property(property) => {
                variables.insert(property.variable.as_str());
            }
            PredicateRhs::Key { variable } | PredicateRhs::ElementId { variable } => {
                variables.insert(variable.as_str());
            }
            PredicateRhs::Literal(_) | PredicateRhs::List(_) => {}
        }
    }

    fn collect_scalar_predicate_rhs_variables<'b>(
        rhs: &'b ScalarPredicateRhs,
        variables: &mut BTreeSet<&'b str>,
    ) {
        match rhs {
            ScalarPredicateRhs::Expression(expression) => {
                Self::collect_scalar_expression_variables(expression, variables);
            }
            ScalarPredicateRhs::List(_) => {}
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "This exhaustive variable collector stays total over every scalar variant."
    )]
    fn collect_scalar_expression_variables<'b>(
        expression: &'b ScalarExpression,
        variables: &mut BTreeSet<&'b str>,
    ) {
        if let ScalarExpression::GraphKeyList {
            variables: path_variables,
        } = expression
        {
            variables.extend(path_variables.iter().map(String::as_str));
            return;
        }
        if let Some(expression) = Self::unary_scalar_expression_operand(expression) {
            Self::collect_scalar_expression_variables(expression, variables);
            return;
        }
        if let Some(variable) = Self::scalar_expression_direct_variable(expression) {
            variables.insert(variable);
            return;
        }
        if let Some((left, right)) = Self::scalar_expression_binary_operands(expression) {
            Self::collect_scalar_expression_variables(left, variables);
            Self::collect_scalar_expression_variables(right, variables);
            return;
        }
        if let Some((first, second, third)) = Self::scalar_expression_ternary_operands(expression) {
            Self::collect_scalar_expression_variables(first, variables);
            Self::collect_scalar_expression_variables(second, variables);
            Self::collect_scalar_expression_variables(third, variables);
            return;
        }

        match expression {
            ScalarExpression::Literal(_)
            | ScalarExpression::LiteralList { .. }
            | ScalarExpression::TypedLiteralList { .. }
            | ScalarExpression::Temporal(TemporalExpr::MakeDuration { .. }) => {}
            ScalarExpression::Temporal(TemporalExpr::DurationInUnits { start, end, .. }) => {
                Self::collect_scalar_expression_variables(start, variables);
                Self::collect_scalar_expression_variables(end, variables);
            }
            ScalarExpression::Predicate(predicate) => {
                Self::collect_predicate_expression_variables(predicate, variables);
            }
            ScalarExpression::CountSubquery {
                pattern,
                distinct_target,
            } => {
                if let Some(target) = distinct_target {
                    Self::collect_collect_subquery_outer_variables(pattern, target, variables);
                } else {
                    Self::collect_count_subquery_outer_variables(pattern, variables);
                }
            }
            ScalarExpression::CollectSubquery {
                pattern, target, ..
            } => {
                Self::collect_collect_subquery_outer_variables(pattern, target, variables);
            }
            ScalarExpression::PresenceGated {
                presence_variable,
                expression,
            } => {
                variables.insert(presence_variable.as_str());
                Self::collect_scalar_expression_variables(expression, variables);
            }
            ScalarExpression::Coalesce { expressions } => {
                for expression in expressions {
                    Self::collect_scalar_expression_variables(expression, variables);
                }
            }
            unary_scalar_expression_pattern!() => {
                unreachable!("unary scalar expressions handled above")
            }
            ScalarExpression::Round { expression, places } => {
                Self::collect_scalar_expression_variables(expression, variables);
                if let Some(places) = places {
                    Self::collect_scalar_expression_variables(places, variables);
                }
            }
            ScalarExpression::Substring {
                expression,
                start,
                length,
            } => {
                Self::collect_scalar_expression_variables(expression, variables);
                Self::collect_scalar_expression_variables(start, variables);
                if let Some(length) = length {
                    Self::collect_scalar_expression_variables(length, variables);
                }
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
            }) => {
                for expression in [
                    year,
                    month,
                    day,
                    hour,
                    minute,
                    second,
                    millisecond,
                    microsecond,
                    nanosecond,
                ] {
                    Self::collect_scalar_expression_variables(expression, variables);
                }
            }
            ScalarExpression::Temporal(TemporalExpr::MakeLocalTime {
                hour,
                minute,
                second,
                millisecond,
                microsecond,
                nanosecond,
            }) => {
                for expression in [hour, minute, second, millisecond, microsecond, nanosecond] {
                    Self::collect_scalar_expression_variables(expression, variables);
                }
            }
            ScalarExpression::Case {
                alternatives,
                else_expression,
            } => {
                for alternative in alternatives {
                    Self::collect_predicate_expression_variables(&alternative.when, variables);
                    Self::collect_scalar_expression_variables(&alternative.then, variables);
                }
                if let Some(else_expression) = else_expression {
                    Self::collect_scalar_expression_variables(else_expression, variables);
                }
            }
            _ => unreachable!("direct scalar variables handled above"),
        }
    }

    fn scalar_expression_binary_operands(
        expression: &ScalarExpression,
    ) -> Option<(&ScalarExpression, &ScalarExpression)> {
        match expression {
            ScalarExpression::NullIf { expression, value } => Some((expression, value)),
            ScalarExpression::Left { expression, count }
            | ScalarExpression::Right { expression, count } => Some((expression, count)),
            ScalarExpression::StringIndices {
                expression,
                pattern,
            }
            | ScalarExpression::StringContains {
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

    fn scalar_expression_ternary_operands(
        expression: &ScalarExpression,
    ) -> Option<(&ScalarExpression, &ScalarExpression, &ScalarExpression)> {
        match expression {
            ScalarExpression::LPad {
                expression,
                length,
                fill,
            }
            | ScalarExpression::RPad {
                expression,
                length,
                fill,
            } => Some((expression, length, fill)),
            ScalarExpression::Replace {
                expression,
                search,
                replacement,
            } => Some((expression, search, replacement)),
            ScalarExpression::Temporal(TemporalExpr::MakeDate { year, month, day }) => {
                Some((year, month, day))
            }
            _ => None,
        }
    }

    fn scalar_expression_direct_variable(expression: &ScalarExpression) -> Option<&str> {
        match expression {
            ScalarExpression::Property(property) => Some(property.variable.as_str()),
            ScalarExpression::UndirectedEndpointProperty { relationship, .. }
            | ScalarExpression::UndirectedEndpointKey { relationship, .. }
            | ScalarExpression::UndirectedEndpointElementId { relationship, .. }
            | ScalarExpression::UndirectedEndpointLabels { relationship, .. }
            | ScalarExpression::UndirectedEndpointPropertyKeys { relationship, .. } => {
                Some(relationship.as_str())
            }
            ScalarExpression::Key { variable }
            | ScalarExpression::ElementId { variable }
            | ScalarExpression::GraphIdentity { variable }
            | ScalarExpression::GraphPresence { variable }
            | ScalarExpression::PropertyKeys { variable }
            | ScalarExpression::RelationshipType { variable, .. }
            | ScalarExpression::NodeLabels { variable, .. } => Some(variable.as_str()),
            _ => None,
        }
    }

    fn unary_scalar_expression_operand(expression: &ScalarExpression) -> Option<&ScalarExpression> {
        match expression {
            ScalarExpression::ToString { expression }
            | ScalarExpression::ToInteger { expression }
            | ScalarExpression::ToFloat { expression }
            | ScalarExpression::ToBoolean { expression }
            | ScalarExpression::ToStringOrNull { expression }
            | ScalarExpression::ToIntegerOrNull { expression }
            | ScalarExpression::ToFloatOrNull { expression }
            | ScalarExpression::ToBooleanOrNull { expression }
            | ScalarExpression::ToLower { expression }
            | ScalarExpression::ToUpper { expression }
            | ScalarExpression::Trim { expression }
            | ScalarExpression::LTrim { expression }
            | ScalarExpression::RTrim { expression }
            | ScalarExpression::CharacterLength { expression }
            | ScalarExpression::Reverse { expression }
            | ScalarExpression::Abs { expression }
            | ScalarExpression::Ceil { expression }
            | ScalarExpression::Floor { expression }
            | ScalarExpression::Sqrt { expression }
            | ScalarExpression::Sign { expression }
            | ScalarExpression::Exp { expression }
            | ScalarExpression::Log { expression }
            | ScalarExpression::Log10 { expression }
            | ScalarExpression::Sin { expression }
            | ScalarExpression::Cos { expression }
            | ScalarExpression::Tan { expression }
            | ScalarExpression::Cot { expression }
            | ScalarExpression::Asin { expression }
            | ScalarExpression::Acos { expression }
            | ScalarExpression::Atan { expression }
            | ScalarExpression::Degrees { expression }
            | ScalarExpression::Radians { expression }
            | ScalarExpression::IsNaN { expression }
            | ScalarExpression::Temporal(
                TemporalExpr::DateFromString { text: expression }
                | TemporalExpr::LocalDateTimeFromString { text: expression }
                | TemporalExpr::LocalTimeFromString { text: expression }
                | TemporalExpr::Component { expression, .. },
            )
            | ScalarExpression::Negate { expression } => Some(expression),
            _ => None,
        }
    }
}
