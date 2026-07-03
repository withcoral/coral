//! Scoped-predicate engine: validates WHERE/property predicates and infers scalar
//! types inside EXISTS / COUNT / COLLECT subquery scopes, resolving local node and
//! relationship bindings (with nested-scope shadowing) over `ExistsPredicateValidationContext`.
//! Read-only; the primary caller is the `exists_subqueries` sibling.

use super::exists_subqueries::{ExistsPredicateValidationContext, ExistsRelationshipValidation};
#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "Scoped-predicate validation methods are split into a child module while preserving parent-private access."
)]
use super::*;

#[allow(
    clippy::allow_attributes,
    clippy::elidable_lifetime_names,
    reason = "Keep the split validation impl shape aligned with the parent GraphPlanValidator impl."
)]
impl<'a> GraphPlanValidator<'a> {
    pub(super) fn validate_scoped_node_patterns<'b>(
        &self,
        nodes: &'b [NodePattern],
        path: &str,
        scope_name: &str,
    ) -> Result<BTreeMap<&'b str, &'a Node>, CoreError> {
        let mut local_nodes = BTreeMap::new();
        for (index, pattern) in nodes.iter().enumerate() {
            validate_variable(format!("{path}.nodes[{index}].variable"), &pattern.variable)?;
            if self.bindings.contains_key(pattern.variable.as_str()) {
                return Err(Diagnostic::new(
                    diagnostic_codes::DUPLICATE_VARIABLE,
                    format!("{path}.nodes[{index}].variable"),
                    format!(
                        "{scope_name} node variable '{}' shadows an outer graph variable",
                        pattern.variable,
                    ),
                )
                .into_core_error());
            }
            if local_nodes.contains_key(pattern.variable.as_str()) {
                return Err(Diagnostic::new(
                    diagnostic_codes::DUPLICATE_VARIABLE,
                    format!("{path}.nodes[{index}].variable"),
                    format!(
                        "{scope_name} node variable '{}' is bound more than once",
                        pattern.variable,
                    ),
                )
                .into_core_error());
            }
            let node = self.graph.node(&pattern.label).ok_or_else(|| {
                Diagnostic::new(
                    diagnostic_codes::UNKNOWN_NODE_LABEL,
                    format!("{path}.nodes[{index}].label"),
                    format!("unknown node label '{}'", pattern.label),
                )
                .into_core_error()
            })?;
            local_nodes.insert(pattern.variable.as_str(), node);
        }
        Ok(local_nodes)
    }

    fn nested_scoped_exists_node_binding_for_path<'b>(
        &self,
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        parent_scope: ExistsPredicateValidationContext<'a, '_>,
        variable: &str,
        path: impl Into<String>,
    ) -> Result<&'a Node, CoreError> {
        if let Some(node) = local_nodes.get(variable).copied() {
            return Ok(node);
        }
        if let Some(node) = parent_scope.local_nodes.get(variable).copied() {
            return Ok(node);
        }
        let path = path.into();
        match self.bindings.get(variable).map(ValidatedBinding::kind) {
            Some(ValidatedBindingKind::Node(node)) => Ok(*node),
            Some(ValidatedBindingKind::Relationship(_)) => Err(Diagnostic::new(
                diagnostic_codes::INVALID_ENDPOINT_VARIABLE,
                path,
                format!("relationship endpoint '{variable}' is not a node variable"),
            )
            .into_core_error()),
            None => Err(Diagnostic::new(
                diagnostic_codes::UNKNOWN_VARIABLE,
                path,
                format!("relationship references unknown node variable '{variable}'"),
            )
            .into_core_error()),
        }
    }

    pub(super) fn validate_scoped_predicate_expression<'b>(
        &self,
        predicate: &PredicateExpression,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        match predicate {
            PredicateExpression::Boolean(_) => Ok(()),
            PredicateExpression::Comparison(predicate) => {
                self.validate_exists_property_predicate(predicate, scope, path)
            }
            PredicateExpression::KeyComparison(predicate) => {
                self.validate_exists_key_ref(
                    &predicate.variable,
                    scope.relationships,
                    scope.local_nodes,
                    format!("{path}.variable"),
                )?;
                let lhs_type = self.scoped_key_scalar_type(&predicate.variable, scope)?;
                self.validate_scoped_predicate_rhs_operand_types(
                    predicate.operator,
                    lhs_type,
                    &predicate.rhs,
                    scope,
                    &path,
                )
            }
            PredicateExpression::ElementIdComparison(predicate) => {
                self.validate_exists_key_ref(
                    &predicate.variable,
                    scope.relationships,
                    scope.local_nodes,
                    format!("{path}.variable"),
                )?;
                self.validate_scoped_predicate_rhs_operand_types(
                    predicate.operator,
                    ScalarType::String,
                    &predicate.rhs,
                    scope,
                    &path,
                )
            }
            PredicateExpression::Presence(predicate) => {
                self.validate_scoped_presence_predicate(predicate, scope, path)
            }
            PredicateExpression::PropertyKeyMembership(predicate) => {
                self.validate_scoped_property_key_membership_predicate(predicate, scope, path)
            }
            PredicateExpression::ExistsPattern(predicate) => {
                self.validate_nested_scoped_exists_pattern_predicate(predicate, scope, path)
            }
            PredicateExpression::ScalarComparison(predicate) => {
                self.validate_scoped_scalar_predicate(predicate, scope, path)
            }
            PredicateExpression::And { left, right }
            | PredicateExpression::Or { left, right }
            | PredicateExpression::Xor { left, right } => {
                self.validate_scoped_predicate_expression(left, scope, format!("{path}.left"))?;
                self.validate_scoped_predicate_expression(right, scope, format!("{path}.right"))
            }
            PredicateExpression::Not { expression } => self.validate_scoped_predicate_expression(
                expression,
                scope,
                format!("{path}.expression"),
            ),
        }
    }

    fn validate_scoped_presence_predicate<'b>(
        &self,
        predicate: &PresencePredicate,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        self.validate_scoped_variable(&predicate.variable, scope, format!("{path}.variable"))?;
        match predicate.operator {
            ComparisonOperator::Equal | ComparisonOperator::NotEqual => Ok(()),
            _ => Err(Diagnostic::new(
                diagnostic_codes::INVALID_NULL_COMPARISON,
                path,
                "presence predicates only support equality and inequality",
            )
            .into_core_error()),
        }
    }

    fn validate_scoped_property_key_membership_predicate<'b>(
        &self,
        predicate: &PropertyKeyMembershipPredicate,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        self.validate_scoped_variable(&predicate.variable, scope, format!("{path}.variable"))?;
        if let Some(presence_variable) = &predicate.presence_variable {
            self.validate_scoped_variable(
                presence_variable,
                scope,
                format!("{path}.presence_variable"),
            )?;
        }
        Ok(())
    }

    fn validate_nested_scoped_exists_pattern_predicate<'b, 'p>(
        &self,
        predicate: &'b ExistsPatternPredicate,
        parent_scope: ExistsPredicateValidationContext<'a, 'p>,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        let local_nodes = self.validate_exists_pattern_nodes(predicate, &path)?;
        self.validate_exists_relationship_variables(predicate, &local_nodes, &path)?;
        let relationships = self.resolve_nested_scoped_exists_relationship_mappings(
            predicate,
            &local_nodes,
            parent_scope,
            &path,
        )?;
        Self::validate_exists_pattern_not_empty(predicate, &path)?;
        let mut scoped_relationships = relationships.clone();
        scoped_relationships.extend(parent_scope.relationships.iter().copied());
        let mut scoped_local_nodes = parent_scope.local_nodes.clone();
        scoped_local_nodes.extend(
            local_nodes
                .iter()
                .map(|(variable, node)| (*variable, *node)),
        );
        let scope = ExistsPredicateValidationContext {
            relationships: &scoped_relationships,
            local_nodes: &scoped_local_nodes,
        };
        for (index, property_predicate) in predicate.predicates.iter().enumerate() {
            self.validate_exists_property_predicate(
                property_predicate,
                scope,
                format!("{path}.predicates[{index}]"),
            )?;
        }
        if let Some(predicate_expression) = predicate.predicate.as_ref() {
            self.validate_scoped_predicate_expression(
                predicate_expression,
                scope,
                format!("{path}.predicate"),
            )?;
        }
        Ok(())
    }

    fn validate_nested_scoped_count_subquery_pattern<'b>(
        &self,
        pattern: &'b CountSubqueryPattern,
        parent_scope: ExistsPredicateValidationContext<'a, '_>,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        match pattern {
            CountSubqueryPattern::Relationships(predicate) => self
                .validate_nested_scoped_count_relationship_pattern(predicate, parent_scope, path),
            CountSubqueryPattern::Nodes {
                nodes,
                predicates,
                predicate,
            } => {
                if nodes.is_empty() {
                    return Err(Diagnostic::new(
                        diagnostic_codes::UNSUPPORTED_COUNT_SUBQUERY,
                        format!("{path}.nodes"),
                        "COUNT subqueries without relationship patterns must bind at least one local node",
                    )
                    .into_core_error());
                }
                let local_nodes =
                    self.validate_scoped_node_patterns(nodes, &path, "nested COUNT subquery")?;
                let relationships = Vec::new();
                let scope = ExistsPredicateValidationContext {
                    relationships: &relationships,
                    local_nodes: &local_nodes,
                };
                for (index, property_predicate) in predicates.iter().enumerate() {
                    self.validate_exists_property_predicate(
                        property_predicate,
                        scope,
                        format!("{path}.predicates[{index}]"),
                    )?;
                }
                if let Some(predicate_expression) = predicate.as_ref() {
                    self.validate_scoped_predicate_expression(
                        predicate_expression,
                        scope,
                        format!("{path}.predicate"),
                    )?;
                }
                Ok(())
            }
        }
    }

    fn validate_nested_scoped_count_relationship_pattern<'b, 'p>(
        &self,
        predicate: &'b ExistsPatternPredicate,
        parent_scope: ExistsPredicateValidationContext<'a, 'p>,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        let local_nodes = self.validate_exists_pattern_nodes(predicate, &path)?;
        self.validate_exists_relationship_variables(predicate, &local_nodes, &path)?;
        let relationships = self.resolve_nested_scoped_exists_relationship_mappings(
            predicate,
            &local_nodes,
            parent_scope,
            &path,
        )?;
        Self::validate_exists_pattern_not_empty(predicate, &path)?;
        let mut scoped_relationships = relationships.clone();
        scoped_relationships.extend(parent_scope.relationships.iter().copied());
        let mut scoped_local_nodes = parent_scope.local_nodes.clone();
        scoped_local_nodes.extend(
            local_nodes
                .iter()
                .map(|(variable, node)| (*variable, *node)),
        );
        let scope = ExistsPredicateValidationContext {
            relationships: &scoped_relationships,
            local_nodes: &scoped_local_nodes,
        };
        for (index, property_predicate) in predicate.predicates.iter().enumerate() {
            self.validate_exists_property_predicate(
                property_predicate,
                scope,
                format!("{path}.predicates[{index}]"),
            )?;
        }
        if let Some(predicate_expression) = predicate.predicate.as_ref() {
            self.validate_scoped_predicate_expression(
                predicate_expression,
                scope,
                format!("{path}.predicate"),
            )?;
        }
        Ok(())
    }

    fn resolve_nested_scoped_exists_relationship_mappings<'b>(
        &self,
        predicate: &'b ExistsPatternPredicate,
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        parent_scope: ExistsPredicateValidationContext<'a, '_>,
        path: &str,
    ) -> Result<Vec<ExistsRelationshipValidation<'a, 'b>>, CoreError> {
        predicate
            .relationships
            .iter()
            .enumerate()
            .map(|(index, relationship)| {
                self.resolve_nested_scoped_exists_relationship_mapping(
                    relationship,
                    local_nodes,
                    parent_scope,
                    format!("{path}.relationships[{index}]"),
                )
                .map(|mapping| ExistsRelationshipValidation {
                    pattern: relationship,
                    relationship: mapping,
                })
            })
            .collect()
    }

    fn resolve_nested_scoped_exists_relationship_mapping<'b>(
        &self,
        relationship: &'b RelationshipPattern,
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        parent_scope: ExistsPredicateValidationContext<'a, '_>,
        path: impl Into<String>,
    ) -> Result<&'a Relationship, CoreError> {
        let path = path.into();
        let left_node = self.nested_scoped_exists_node_binding_for_path(
            local_nodes,
            parent_scope,
            &relationship.left,
            format!("{path}.left"),
        )?;
        let right_node = self.nested_scoped_exists_node_binding_for_path(
            local_nodes,
            parent_scope,
            &relationship.right,
            format!("{path}.right"),
        )?;
        self.resolve_relationship_mapping_for_nodes(relationship, left_node, right_node, path)
    }

    fn validate_scoped_scalar_predicate<'b>(
        &self,
        predicate: &ScalarPredicate,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        let lhs_type =
            self.infer_scoped_scalar_expression_type(&predicate.lhs, scope, format!("{path}.lhs"))?;
        match &predicate.rhs {
            ScalarPredicateRhs::Expression(expression) => {
                if predicate.operator == ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        diagnostic_codes::INVALID_PREDICATE_OPERAND,
                        path.clone(),
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                let rhs_type = self.infer_scoped_scalar_expression_type(
                    expression,
                    scope,
                    format!("{path}.rhs"),
                )?;
                Self::validate_scalar_predicate_operand_types(
                    predicate.operator,
                    lhs_type,
                    rhs_type,
                    &path,
                )
            }
            ScalarPredicateRhs::List(literals) => {
                if predicate.operator != ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        diagnostic_codes::INVALID_PREDICATE_OPERAND,
                        path.clone(),
                        "literal lists are only supported with IN predicates",
                    )
                    .into_core_error());
                }
                Self::validate_scalar_in_list_operand_types(lhs_type, literals, &path)
            }
        }
    }

    fn validate_scoped_predicate_rhs_operand_types<'b>(
        &self,
        operator: ComparisonOperator,
        lhs_type: ScalarType,
        rhs: &PredicateRhs,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: &str,
    ) -> Result<(), CoreError> {
        match rhs {
            PredicateRhs::Literal(literal) => Self::validate_scalar_predicate_operand_types(
                operator,
                lhs_type,
                literal_scalar_type(literal),
                path,
            ),
            PredicateRhs::Property(property) => {
                let rhs_type = self.exists_property_ref_scalar_type(
                    property,
                    scope.relationships,
                    scope.local_nodes,
                )?;
                Self::validate_scalar_predicate_operand_types(operator, lhs_type, rhs_type, path)
            }
            PredicateRhs::Key { variable } => {
                let rhs_type = self.scoped_key_scalar_type(variable, scope)?;
                Self::validate_scalar_predicate_operand_types(operator, lhs_type, rhs_type, path)
            }
            PredicateRhs::ElementId { .. } => Self::validate_scalar_predicate_operand_types(
                operator,
                lhs_type,
                ScalarType::String,
                path,
            ),
            PredicateRhs::List(literals) => {
                Self::validate_scalar_in_list_operand_types(lhs_type, literals, path)
            }
        }
    }

    pub(super) fn infer_scoped_scalar_expression_type<'b>(
        &self,
        expression: &ScalarExpression,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: impl Into<String>,
    ) -> Result<ScalarType, CoreError> {
        let path = path.into();
        match expression {
            ScalarExpression::UndirectedEndpointProperty { .. }
            | ScalarExpression::UndirectedEndpointKey { .. }
            | ScalarExpression::UndirectedEndpointElementId { .. }
            | ScalarExpression::UndirectedEndpointLabels { .. }
            | ScalarExpression::UndirectedEndpointPropertyKeys { .. } => {
                self.infer_scoped_undirected_endpoint_scalar_type(expression, scope, &path)
            }
            ScalarExpression::Property(_)
            | ScalarExpression::Literal(_)
            | ScalarExpression::LiteralList { .. }
            | ScalarExpression::TypedLiteralList { .. }
            | ScalarExpression::GraphKeyList { .. }
            | ScalarExpression::Predicate(_)
            | ScalarExpression::Key { .. }
            | ScalarExpression::ElementId { .. }
            | ScalarExpression::GraphIdentity { .. }
            | ScalarExpression::GraphPresence { .. }
            | ScalarExpression::NodeLabels { .. }
            | ScalarExpression::PropertyKeys { .. }
            | ScalarExpression::RelationshipType { .. }
            | ScalarExpression::PresenceGated { .. } => {
                self.infer_scoped_atomic_scalar_type(expression, scope, &path)
            }
            ScalarExpression::Coalesce { expressions } => {
                self.infer_scoped_coalesce_scalar_type(expressions, scope, &path)
            }
            ScalarExpression::CountSubquery {
                pattern,
                distinct_target,
            } => {
                self.validate_nested_scoped_count_subquery_pattern(
                    pattern,
                    scope,
                    format!("{path}.pattern"),
                )?;
                if let Some(target) = distinct_target {
                    self.infer_scoped_scalar_expression_type(
                        target,
                        scope,
                        format!("{path}.distinct_target"),
                    )?;
                }
                Ok(ScalarType::Integer)
            }
            ScalarExpression::CollectSubquery { .. } => Err(Diagnostic::new(
                diagnostic_codes::UNSUPPORTED_COLLECT_SUBQUERY,
                path,
                "nested COLLECT subqueries require scoped list-value planning and are not supported yet",
            )
            .into_core_error()),
            ScalarExpression::NullIf { expression, value } => {
                self.infer_scoped_null_if_scalar_type(expression, value, scope, &path)
            }
            ScalarExpression::Case {
                alternatives,
                else_expression,
            } => self.infer_scoped_case_scalar_type(
                alternatives,
                else_expression.as_deref(),
                scope,
                &path,
            ),
            ScalarExpression::Temporal(temporal) => {
                self.infer_scoped_temporal_scalar_type(temporal, scope, &path)
            }
            _ => self.infer_scoped_scalar_function_type(expression, scope, &path),
        }
    }

    fn infer_scoped_undirected_endpoint_scalar_type<'b>(
        &self,
        expression: &ScalarExpression,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        match expression {
            ScalarExpression::UndirectedEndpointProperty {
                relationship,
                endpoint,
                property,
            } => {
                if let Some(expression_type) = self
                    .scoped_undirected_endpoint_property_scalar_type(
                        relationship,
                        *endpoint,
                        property,
                        scope,
                        path,
                    )?
                {
                    Ok(expression_type)
                } else {
                    self.infer_atomic_scalar_type(expression, path)
                }
            }
            ScalarExpression::UndirectedEndpointKey { relationship, .. } => {
                if let Some((left_node, _)) =
                    self.scoped_same_label_undirected_endpoint_nodes(relationship, scope, path)?
                {
                    Ok(self.column_scalar_type(&left_node.table, &left_node.key))
                } else {
                    self.infer_atomic_scalar_type(expression, path)
                }
            }
            ScalarExpression::UndirectedEndpointElementId { relationship, .. } => {
                if self
                    .scoped_same_label_undirected_endpoint_nodes(relationship, scope, path)?
                    .is_some()
                {
                    Ok(ScalarType::String)
                } else {
                    self.infer_atomic_scalar_type(expression, path)
                }
            }
            ScalarExpression::UndirectedEndpointLabels {
                relationship,
                label,
                ..
            } => {
                if let Some((left_node, _)) =
                    self.scoped_same_label_undirected_endpoint_nodes(relationship, scope, path)?
                {
                    if left_node.label != *label {
                        return Err(CoreError::internal(
                            "validated scoped same-label undirected endpoint labels did not match node label",
                        ));
                    }
                    Ok(ScalarType::Other)
                } else {
                    self.infer_atomic_scalar_type(expression, path)
                }
            }
            ScalarExpression::UndirectedEndpointPropertyKeys { relationship, .. } => {
                if self
                    .scoped_same_label_undirected_endpoint_nodes(relationship, scope, path)?
                    .is_some()
                {
                    Ok(ScalarType::Other)
                } else {
                    self.infer_atomic_scalar_type(expression, path)
                }
            }
            _ => unreachable!(
                "non-undirected endpoint scalar expression reached scoped endpoint type inference"
            ),
        }
    }

    fn infer_scoped_atomic_scalar_type<'b>(
        &self,
        expression: &ScalarExpression,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        let relationships = scope.relationships;
        let local_nodes = scope.local_nodes;

        match expression {
            ScalarExpression::Property(property) => {
                self.validate_exists_property_ref(property, relationships, local_nodes, path)?;
                self.exists_property_ref_scalar_type(property, relationships, local_nodes)
            }
            ScalarExpression::Literal(literal) => Ok(literal_scalar_type(literal)),
            ScalarExpression::LiteralList { literals } => {
                Self::validate_literal_list_projection(literals, path)?;
                Ok(ScalarType::Other)
            }
            ScalarExpression::TypedLiteralList {
                literals,
                element_type,
            } => {
                Self::validate_typed_literal_list(literals, *element_type, path)?;
                Ok(ScalarType::Other)
            }
            ScalarExpression::GraphKeyList { variables } => {
                for variable in variables {
                    self.validate_exists_key_ref(variable, relationships, local_nodes, path)?;
                }
                Ok(ScalarType::Other)
            }
            ScalarExpression::Predicate(predicate) => {
                self.validate_scoped_predicate_expression(predicate, scope, path)?;
                Ok(ScalarType::Boolean)
            }
            ScalarExpression::Key { variable } => {
                self.validate_exists_key_ref(variable, relationships, local_nodes, path)?;
                self.scoped_key_scalar_type(variable, scope)
            }
            ScalarExpression::ElementId { variable }
            | ScalarExpression::GraphIdentity { variable } => {
                self.validate_exists_key_ref(variable, relationships, local_nodes, path)?;
                Ok(ScalarType::String)
            }
            ScalarExpression::GraphPresence { variable } => {
                self.validate_scoped_variable(variable, scope, path)?;
                Ok(ScalarType::String)
            }
            ScalarExpression::NodeLabels { variable, label } => {
                self.validate_scoped_node_labels_projection(variable, label, scope, path)?;
                Ok(ScalarType::Other)
            }
            ScalarExpression::PropertyKeys { variable } => {
                self.validate_scoped_variable(variable, scope, path)?;
                Ok(ScalarType::Other)
            }
            ScalarExpression::RelationshipType {
                variable,
                relationship_type,
            } => {
                self.validate_scoped_relationship_type_projection(
                    variable,
                    relationship_type,
                    scope,
                    path,
                )?;
                Ok(ScalarType::String)
            }
            ScalarExpression::PresenceGated {
                presence_variable,
                expression,
            } => {
                self.validate_scoped_variable(presence_variable, scope, path)?;
                self.infer_scoped_scalar_expression_type(
                    expression,
                    scope,
                    format!("{path}.expression"),
                )
            }
            _ => unreachable!("non-atomic scalar expression reached scoped atomic type inference"),
        }
    }

    fn infer_scoped_coalesce_scalar_type<'b>(
        &self,
        expressions: &[ScalarExpression],
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        if expressions.len() < 2 {
            return Err(Diagnostic::new(
                diagnostic_codes::INVALID_SCALAR_EXPRESSION,
                path,
                "coalesce expressions require at least two arguments",
            )
            .into_core_error());
        }
        let mut result_type = ScalarType::Null;
        for (index, expression) in expressions.iter().enumerate() {
            let expression_type = self.infer_scoped_scalar_expression_type(
                expression,
                scope,
                format!("{path}[{index}]"),
            )?;
            result_type = Self::merge_scalar_types(
                result_type,
                expression_type,
                format!("{path}[{index}]"),
                "coalesce arguments",
            )?;
        }
        Ok(result_type)
    }

    fn infer_scoped_null_if_scalar_type<'b>(
        &self,
        expression: &ScalarExpression,
        value: &ScalarExpression,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        let expression_type = self.infer_scoped_scalar_expression_type(
            expression,
            scope,
            format!("{path}.expression"),
        )?;
        let value_type =
            self.infer_scoped_scalar_expression_type(value, scope, format!("{path}.value"))?;
        Self::validate_compatible_scalar_types(
            expression_type,
            value_type,
            path,
            "nullIf arguments",
        )?;
        Ok(expression_type)
    }

    fn infer_scoped_case_scalar_type<'b>(
        &self,
        alternatives: &[ScalarCaseAlternative],
        else_expression: Option<&ScalarExpression>,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        if alternatives.is_empty() {
            return Err(Diagnostic::new(
                diagnostic_codes::INVALID_SCALAR_EXPRESSION,
                path,
                "CASE expressions require at least one WHEN/THEN alternative",
            )
            .into_core_error());
        }
        let mut result_type = ScalarType::Null;
        for (index, alternative) in alternatives.iter().enumerate() {
            self.validate_scoped_predicate_expression(
                &alternative.when,
                scope,
                format!("{path}.alternatives[{index}].when"),
            )?;
            let then_type = self.infer_scoped_scalar_expression_type(
                &alternative.then,
                scope,
                format!("{path}.alternatives[{index}].then"),
            )?;
            result_type = Self::merge_scalar_types(
                result_type,
                then_type,
                format!("{path}.alternatives[{index}].then"),
                "CASE result branches",
            )?;
        }
        if let Some(else_expression) = else_expression {
            let else_type = self.infer_scoped_scalar_expression_type(
                else_expression,
                scope,
                format!("{path}.else"),
            )?;
            result_type = Self::merge_scalar_types(
                result_type,
                else_type,
                format!("{path}.else"),
                "CASE result branches",
            )?;
        }
        Ok(result_type)
    }

    fn infer_scoped_scalar_function_type<'b>(
        &self,
        expression: &ScalarExpression,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        let string_scalar_type =
            self.infer_scoped_string_scalar_function_type(expression, scope, path)?;
        if let Some(scalar_type) = string_scalar_type {
            return Ok(scalar_type);
        }

        match expression {
            ScalarExpression::ToString { expression }
            | ScalarExpression::ToStringOrNull { expression } => {
                self.infer_scoped_scalar_expression_type(
                    expression,
                    scope,
                    format!("{path}.expression"),
                )?;
                Ok(ScalarType::String)
            }
            ScalarExpression::ToInteger { expression }
            | ScalarExpression::ToIntegerOrNull { expression } => {
                self.infer_scoped_scalar_expression_type(
                    expression,
                    scope,
                    format!("{path}.expression"),
                )?;
                Ok(ScalarType::Integer)
            }
            ScalarExpression::ToFloat { expression }
            | ScalarExpression::ToFloatOrNull { expression } => {
                self.infer_scoped_scalar_expression_type(
                    expression,
                    scope,
                    format!("{path}.expression"),
                )?;
                Ok(ScalarType::Float)
            }
            ScalarExpression::ToBoolean { expression }
            | ScalarExpression::ToBooleanOrNull { expression } => {
                self.infer_scoped_scalar_expression_type(
                    expression,
                    scope,
                    format!("{path}.expression"),
                )?;
                Ok(ScalarType::Boolean)
            }
            ScalarExpression::CharacterLength { expression } => {
                let expression_type = self.infer_scoped_scalar_expression_type(
                    expression,
                    scope,
                    format!("{path}.expression"),
                )?;
                Self::require_string_compatible_type(
                    expression_type,
                    format!("{path}.expression"),
                    "character length",
                )?;
                Ok(ScalarType::Integer)
            }
            ScalarExpression::IsNaN { expression } => {
                self.infer_scoped_is_nan_scalar_type(expression, scope, path)
            }
            ScalarExpression::Abs { expression }
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
            | ScalarExpression::Negate { expression } => {
                self.infer_scoped_numeric_unary_scalar_type(expression, scope, path)
            }
            ScalarExpression::Round { expression, places } => {
                self.infer_scoped_round_scalar_type(expression, places.as_deref(), scope, path)
            }
            ScalarExpression::Arithmetic { left, right, .. } => {
                self.infer_scoped_arithmetic_scalar_type(left, right, scope, path)
            }
            ScalarExpression::Atan2 { y, x } => {
                self.infer_scoped_atan2_scalar_type(y, x, scope, path)
            }
            _ => unreachable!("non-function scalar expression reached function type inference"),
        }
    }

    fn infer_scoped_temporal_scalar_type<'b>(
        &self,
        expression: &TemporalExpr,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        match expression {
            TemporalExpr::MakeDate { year, month, day } => {
                for (name, expression) in [("year", year), ("month", month), ("day", day)] {
                    let expression_type = self.infer_scoped_scalar_expression_type(
                        expression,
                        scope,
                        format!("{path}.{name}"),
                    )?;
                    Self::require_integer_compatible_type(
                        expression_type,
                        format!("{path}.{name}"),
                        "date constructor field",
                    )?;
                }
                Ok(ScalarType::Temporal(TemporalKind::Date))
            }
            TemporalExpr::DateFromString { text } => {
                let text_type =
                    self.infer_scoped_scalar_expression_type(text, scope, format!("{path}.text"))?;
                Self::require_string_compatible_type(
                    text_type,
                    format!("{path}.text"),
                    "date string constructor",
                )?;
                Ok(ScalarType::Temporal(TemporalKind::Date))
            }
        }
    }

    fn infer_scoped_string_scalar_function_type<'b>(
        &self,
        expression: &ScalarExpression,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: &str,
    ) -> Result<Option<ScalarType>, CoreError> {
        match expression {
            ScalarExpression::ToLower { expression }
            | ScalarExpression::ToUpper { expression }
            | ScalarExpression::Trim { expression }
            | ScalarExpression::LTrim { expression }
            | ScalarExpression::RTrim { expression }
            | ScalarExpression::Reverse { expression } => self
                .infer_scoped_string_unary_scalar_type(expression, scope, path)
                .map(Some),
            ScalarExpression::Left { expression, count }
            | ScalarExpression::Right { expression, count } => self
                .infer_scoped_sized_string_scalar_type(expression, count, scope, path)
                .map(Some),
            ScalarExpression::StringIndices {
                expression,
                pattern,
            } => self
                .infer_scoped_string_indices_scalar_type(expression, pattern, scope, path)
                .map(Some),
            ScalarExpression::LPad {
                expression,
                length,
                fill,
            }
            | ScalarExpression::RPad {
                expression,
                length,
                fill,
            } => self
                .infer_scoped_padding_scalar_type(expression, length, fill, scope, path)
                .map(Some),
            ScalarExpression::StringContains {
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
            } => self
                .infer_scoped_string_predicate_function_scalar_type(
                    expression, operand, scope, path,
                )
                .map(Some),
            ScalarExpression::Replace {
                expression,
                search,
                replacement,
            } => self
                .infer_scoped_replace_scalar_type(expression, search, replacement, scope, path)
                .map(Some),
            ScalarExpression::Substring {
                expression,
                start,
                length,
            } => self
                .infer_scoped_substring_scalar_type(
                    expression,
                    start,
                    length.as_deref(),
                    scope,
                    path,
                )
                .map(Some),
            _ => Ok(None),
        }
    }

    fn infer_scoped_string_unary_scalar_type<'b>(
        &self,
        expression: &ScalarExpression,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        let expression_type = self.infer_scoped_scalar_expression_type(
            expression,
            scope,
            format!("{path}.expression"),
        )?;
        Self::require_string_compatible_type(
            expression_type,
            format!("{path}.expression"),
            "string function",
        )?;
        Ok(ScalarType::String)
    }

    fn infer_scoped_sized_string_scalar_type<'b>(
        &self,
        expression: &ScalarExpression,
        count: &ScalarExpression,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        let expression_type = self.infer_scoped_scalar_expression_type(
            expression,
            scope,
            format!("{path}.expression"),
        )?;
        Self::require_string_compatible_type(
            expression_type,
            format!("{path}.expression"),
            "sized string function",
        )?;
        let count_type =
            self.infer_scoped_scalar_expression_type(count, scope, format!("{path}.count"))?;
        Self::require_integer_compatible_type(
            count_type,
            format!("{path}.count"),
            "sized string count",
        )?;
        Ok(ScalarType::String)
    }

    fn infer_scoped_string_indices_scalar_type<'b>(
        &self,
        expression: &ScalarExpression,
        pattern: &ScalarExpression,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        for (name, expression) in [("expression", expression), ("pattern", pattern)] {
            let expression_type = self.infer_scoped_scalar_expression_type(
                expression,
                scope,
                format!("{path}.{name}"),
            )?;
            Self::require_string_compatible_type(
                expression_type,
                format!("{path}.{name}"),
                "indices",
            )?;
        }
        Ok(ScalarType::Other)
    }

    fn infer_scoped_padding_scalar_type<'b>(
        &self,
        expression: &ScalarExpression,
        length: &ScalarExpression,
        fill: &ScalarExpression,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        for (name, expression) in [("expression", expression), ("fill", fill)] {
            let expression_type = self.infer_scoped_scalar_expression_type(
                expression,
                scope,
                format!("{path}.{name}"),
            )?;
            Self::require_string_compatible_type(
                expression_type,
                format!("{path}.{name}"),
                "padding string function",
            )?;
        }
        let length_type =
            self.infer_scoped_scalar_expression_type(length, scope, format!("{path}.length"))?;
        Self::require_integer_compatible_type(
            length_type,
            format!("{path}.length"),
            "padding length",
        )?;
        Ok(ScalarType::String)
    }

    fn infer_scoped_string_predicate_function_scalar_type<'b>(
        &self,
        expression: &ScalarExpression,
        pattern: &ScalarExpression,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        for (name, expression) in [("expression", expression), ("pattern", pattern)] {
            let expression_type = self.infer_scoped_scalar_expression_type(
                expression,
                scope,
                format!("{path}.{name}"),
            )?;
            Self::require_string_compatible_type(
                expression_type,
                format!("{path}.{name}"),
                "string predicate function",
            )?;
        }
        Ok(ScalarType::Boolean)
    }

    fn infer_scoped_replace_scalar_type<'b>(
        &self,
        expression: &ScalarExpression,
        search: &ScalarExpression,
        replacement: &ScalarExpression,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        for (name, expression) in [
            ("expression", expression),
            ("search", search),
            ("replacement", replacement),
        ] {
            let expression_type = self.infer_scoped_scalar_expression_type(
                expression,
                scope,
                format!("{path}.{name}"),
            )?;
            Self::require_string_compatible_type(
                expression_type,
                format!("{path}.{name}"),
                "replace",
            )?;
        }
        Ok(ScalarType::String)
    }

    fn infer_scoped_substring_scalar_type<'b>(
        &self,
        expression: &ScalarExpression,
        start: &ScalarExpression,
        length: Option<&ScalarExpression>,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        let expression_type = self.infer_scoped_scalar_expression_type(
            expression,
            scope,
            format!("{path}.expression"),
        )?;
        Self::require_string_compatible_type(
            expression_type,
            format!("{path}.expression"),
            "substring",
        )?;
        let start_type =
            self.infer_scoped_scalar_expression_type(start, scope, format!("{path}.start"))?;
        Self::require_integer_compatible_type(
            start_type,
            format!("{path}.start"),
            "substring start",
        )?;
        if let Some(length) = length {
            let length_type =
                self.infer_scoped_scalar_expression_type(length, scope, format!("{path}.length"))?;
            Self::require_integer_compatible_type(
                length_type,
                format!("{path}.length"),
                "substring length",
            )?;
        }
        Ok(ScalarType::String)
    }

    fn infer_scoped_is_nan_scalar_type<'b>(
        &self,
        expression: &ScalarExpression,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        let expression_type = self.infer_scoped_scalar_expression_type(
            expression,
            scope,
            format!("{path}.expression"),
        )?;
        Self::require_numeric_compatible_type(
            expression_type,
            format!("{path}.expression"),
            "isNaN",
        )?;
        Ok(ScalarType::Boolean)
    }

    fn infer_scoped_numeric_unary_scalar_type<'b>(
        &self,
        expression: &ScalarExpression,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        let expression_type = self.infer_scoped_scalar_expression_type(
            expression,
            scope,
            format!("{path}.expression"),
        )?;
        Self::require_numeric_compatible_type(
            expression_type,
            format!("{path}.expression"),
            "numeric function",
        )?;
        numeric_result_type(expression_type, path, "numeric function")
    }

    fn infer_scoped_round_scalar_type<'b>(
        &self,
        expression: &ScalarExpression,
        places: Option<&ScalarExpression>,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        let expression_type = self.infer_scoped_scalar_expression_type(
            expression,
            scope,
            format!("{path}.expression"),
        )?;
        Self::require_numeric_compatible_type(
            expression_type,
            format!("{path}.expression"),
            "round",
        )?;
        if let Some(places) = places {
            let places_type =
                self.infer_scoped_scalar_expression_type(places, scope, format!("{path}.places"))?;
            Self::require_integer_compatible_type(
                places_type,
                format!("{path}.places"),
                "round precision",
            )?;
        }
        numeric_result_type(expression_type, path, "round")
    }

    fn infer_scoped_arithmetic_scalar_type<'b>(
        &self,
        left: &ScalarExpression,
        right: &ScalarExpression,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        let left_type =
            self.infer_scoped_scalar_expression_type(left, scope, format!("{path}.left"))?;
        let right_type =
            self.infer_scoped_scalar_expression_type(right, scope, format!("{path}.right"))?;
        Self::require_numeric_compatible_type(left_type, format!("{path}.left"), "arithmetic")?;
        Self::require_numeric_compatible_type(right_type, format!("{path}.right"), "arithmetic")?;
        numeric_binary_result_type(left_type, right_type, path, "arithmetic")
    }

    fn infer_scoped_atan2_scalar_type<'b>(
        &self,
        y: &ScalarExpression,
        x: &ScalarExpression,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        let y_type = self.infer_scoped_scalar_expression_type(y, scope, format!("{path}.y"))?;
        let x_type = self.infer_scoped_scalar_expression_type(x, scope, format!("{path}.x"))?;
        Self::require_numeric_compatible_type(y_type, format!("{path}.y"), "atan2")?;
        Self::require_numeric_compatible_type(x_type, format!("{path}.x"), "atan2")?;
        Ok(ScalarType::Float)
    }

    fn validate_scoped_variable<'b>(
        &self,
        variable: &str,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        validate_variable(path.clone(), variable)?;
        if scope.local_nodes.contains_key(variable)
            || Self::exists_relationship_for_variable(scope.relationships, variable).is_some()
            || self.bindings.contains_key(variable)
        {
            return Ok(());
        }
        Err(Diagnostic::new(
            diagnostic_codes::UNKNOWN_VARIABLE,
            path,
            format!("unknown graph variable '{variable}'"),
        )
        .into_core_error())
    }

    fn validate_scoped_node_labels_projection<'b>(
        &self,
        variable: &str,
        label: &str,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        if Self::exists_relationship_for_variable(scope.relationships, variable).is_some() {
            return Err(Diagnostic::new(
                diagnostic_codes::INVALID_LABELS_PROJECTION,
                path,
                format!("labels({variable}) requires a node variable"),
            )
            .into_core_error());
        }
        if let Some(node) = scope.local_nodes.get(variable).copied() {
            if node.label == label {
                return Ok(());
            }
            return Err(Diagnostic::new(
                diagnostic_codes::INVALID_LABELS_PROJECTION,
                path,
                format!(
                    "labels({variable}) expected node label '{}', got '{label}'",
                    node.label
                ),
            )
            .into_core_error());
        }
        self.validate_node_labels_projection(variable, label, path)
    }

    fn validate_scoped_relationship_type_projection<'b>(
        &self,
        variable: &str,
        relationship_type: &str,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        if let Some(relationship) =
            Self::exists_relationship_for_variable(scope.relationships, variable)
        {
            if relationship.relationship_type == relationship_type {
                return Ok(());
            }
            return Err(Diagnostic::new(
                diagnostic_codes::INVALID_TYPE_PROJECTION,
                path,
                format!(
                    "type({variable}) expected relationship type '{}', got '{relationship_type}'",
                    relationship.relationship_type
                ),
            )
            .into_core_error());
        }
        if scope.local_nodes.contains_key(variable) {
            return Err(Diagnostic::new(
                diagnostic_codes::INVALID_TYPE_PROJECTION,
                path,
                format!("type({variable}) requires a relationship variable"),
            )
            .into_core_error());
        }
        self.validate_relationship_type_projection(variable, relationship_type, path)
    }

    fn scoped_key_scalar_type<'b>(
        &self,
        variable: &str,
        scope: ExistsPredicateValidationContext<'a, 'b>,
    ) -> Result<ScalarType, CoreError> {
        if let Some(relationship) =
            Self::exists_relationship_for_variable(scope.relationships, variable)
        {
            let Some(key) = relationship.key.as_deref() else {
                return Ok(ScalarType::Unknown);
            };
            return Ok(self.column_scalar_type(&relationship.table, key));
        }
        if let Some(node) = scope.local_nodes.get(variable).copied() {
            return Ok(self.column_scalar_type(&node.table, &node.key));
        }
        self.key_scalar_type(variable)
    }

    fn scoped_same_label_undirected_endpoint_nodes<'b>(
        &self,
        relationship_variable: &str,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: &str,
    ) -> Result<Option<(&'a Node, &'a Node)>, CoreError> {
        let Some(scoped_relationship) = scope.relationships.iter().find(|relationship| {
            relationship.pattern.variable.as_deref() == Some(relationship_variable)
        }) else {
            return Ok(None);
        };
        if scoped_relationship.pattern.direction != Direction::Undirected {
            return Err(CoreError::internal(
                "scoped undirected endpoint scalar referenced a directed relationship",
            ));
        }
        let left_node = self.scoped_node_binding_for_path(
            scope,
            &scoped_relationship.pattern.left,
            format!("{path}.left"),
        )?;
        let right_node = self.scoped_node_binding_for_path(
            scope,
            &scoped_relationship.pattern.right,
            format!("{path}.right"),
        )?;
        if left_node.label != right_node.label {
            return Err(CoreError::internal(
                "scoped undirected endpoint scalar referenced a cross-label relationship",
            ));
        }
        if scoped_relationship.relationship.from.label != left_node.label
            || scoped_relationship.relationship.to.label != right_node.label
        {
            return Err(CoreError::internal(
                "validated scoped same-label undirected relationship mapping did not match endpoint labels",
            ));
        }
        Ok(Some((left_node, right_node)))
    }

    fn scoped_node_binding_for_path<'b>(
        &self,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        variable: &str,
        path: impl Into<String>,
    ) -> Result<&'a Node, CoreError> {
        if let Some(node) = scope.local_nodes.get(variable).copied() {
            return Ok(node);
        }
        let path = path.into();
        match self.bindings.get(variable).map(ValidatedBinding::kind) {
            Some(ValidatedBindingKind::Node(node)) => Ok(*node),
            Some(ValidatedBindingKind::Relationship(_)) => Err(Diagnostic::new(
                diagnostic_codes::INVALID_ENDPOINT_VARIABLE,
                path,
                format!("relationship endpoint '{variable}' is not a node variable"),
            )
            .into_core_error()),
            None => Err(Diagnostic::new(
                diagnostic_codes::UNKNOWN_VARIABLE,
                path,
                format!("relationship references unknown node variable '{variable}'"),
            )
            .into_core_error()),
        }
    }

    fn scoped_undirected_endpoint_property_scalar_type<'b>(
        &self,
        relationship_variable: &str,
        endpoint: UndirectedRelationshipEndpoint,
        property: &str,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: &str,
    ) -> Result<Option<ScalarType>, CoreError> {
        let Some((left_node, right_node)) =
            self.scoped_same_label_undirected_endpoint_nodes(relationship_variable, scope, path)?
        else {
            return Ok(None);
        };
        let Some(left_column) = left_node.column_for_property(property) else {
            let function = match endpoint {
                UndirectedRelationshipEndpoint::Start => "startNode",
                UndirectedRelationshipEndpoint::End => "endNode",
            };
            return Err(Diagnostic::new(
                diagnostic_codes::UNKNOWN_PROPERTY,
                path,
                format!(
                    "{function}({relationship_variable}) does not expose property '{property}'"
                ),
            )
            .into_core_error());
        };
        if right_node.column_for_property(property).is_none() {
            return Err(CoreError::internal(
                "scoped same-label undirected relationship endpoints exposed different property sets",
            ));
        }
        Ok(Some(self.column_scalar_type(&left_node.table, left_column)))
    }
}
