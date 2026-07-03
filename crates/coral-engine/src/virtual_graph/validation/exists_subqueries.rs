//! EXISTS / COUNT / COLLECT subquery-pattern validation: binds local node and
//! relationship variables, resolves pattern endpoints to graph relationships, and
//! type-checks per-scope property predicates over `ExistsPredicateValidationContext`.

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "EXISTS subquery validation methods are split into a child module while preserving parent-private access."
)]
use super::*;

#[derive(Debug, Clone, Copy)]
pub(super) struct ExistsPredicateValidationContext<'a, 'b> {
    pub(super) relationships: &'b [ExistsRelationshipValidation<'a, 'b>],
    pub(super) local_nodes: &'b BTreeMap<&'b str, &'a Node>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ExistsRelationshipValidation<'a, 'b> {
    pub(super) pattern: &'b RelationshipPattern,
    pub(super) relationship: &'a Relationship,
}

#[allow(
    clippy::allow_attributes,
    clippy::elidable_lifetime_names,
    reason = "Keep the split validation impl shape aligned with the parent GraphPlanValidator impl."
)]
impl<'a> GraphPlanValidator<'a> {
    pub(super) fn validate_exists_pattern_predicate(
        &self,
        predicate: &ExistsPatternPredicate,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        let local_nodes = self.validate_exists_pattern_nodes(predicate, &path)?;
        self.validate_exists_relationship_variables(predicate, &local_nodes, &path)?;
        let relationships =
            self.resolve_exists_relationship_mappings(predicate, &local_nodes, &path)?;
        Self::validate_exists_pattern_not_empty(predicate, &path)?;
        let scope = ExistsPredicateValidationContext {
            relationships: &relationships,
            local_nodes: &local_nodes,
        };
        for (index, property_predicate) in predicate.predicates.iter().enumerate() {
            self.validate_exists_property_predicate(
                property_predicate,
                scope,
                format!("{path}.predicates[{index}]"),
            )?;
        }
        if let Some(predicate) = &predicate.predicate {
            self.validate_scoped_predicate_expression(
                predicate,
                scope,
                format!("{path}.predicate"),
            )?;
        }
        Ok(())
    }

    pub(super) fn validate_count_subquery_pattern(
        &self,
        pattern: &CountSubqueryPattern,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        match pattern {
            CountSubqueryPattern::Relationships(predicate) => {
                self.validate_count_relationship_pattern(predicate, path)
            }
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
                    self.validate_scoped_node_patterns(nodes, &path, "COUNT subquery")?;
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
                if let Some(predicate) = predicate {
                    self.validate_scoped_predicate_expression(
                        predicate,
                        scope,
                        format!("{path}.predicate"),
                    )?;
                }
                Ok(())
            }
        }
    }

    pub(super) fn validate_collect_subquery_pattern(
        &self,
        pattern: &CountSubqueryPattern,
        target: &ScalarExpression,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        match pattern {
            CountSubqueryPattern::Relationships(predicate) => {
                self.validate_collect_relationship_pattern(predicate, target, path)
            }
            CountSubqueryPattern::Nodes {
                nodes,
                predicates,
                predicate,
            } => {
                if nodes.is_empty() {
                    return Err(Diagnostic::new(
                        diagnostic_codes::UNSUPPORTED_COLLECT_SUBQUERY,
                        format!("{path}.nodes"),
                        "COLLECT subqueries without relationship patterns must bind at least one local node",
                    )
                    .into_core_error());
                }
                let local_nodes =
                    self.validate_scoped_node_patterns(nodes, &path, "COLLECT subquery")?;
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
                if let Some(predicate) = predicate {
                    self.validate_scoped_predicate_expression(
                        predicate,
                        scope,
                        format!("{path}.predicate"),
                    )?;
                }
                self.infer_scoped_scalar_expression_type(target, scope, format!("{path}.target"))?;
                Ok(())
            }
        }
    }

    fn validate_collect_relationship_pattern(
        &self,
        predicate: &ExistsPatternPredicate,
        target: &ScalarExpression,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        let local_nodes = self.validate_exists_pattern_nodes(predicate, &path)?;
        self.validate_exists_relationship_variables(predicate, &local_nodes, &path)?;
        let relationships =
            self.resolve_exists_relationship_mappings(predicate, &local_nodes, &path)?;
        Self::validate_exists_pattern_not_empty(predicate, &path)?;
        let scope = ExistsPredicateValidationContext {
            relationships: &relationships,
            local_nodes: &local_nodes,
        };
        for (index, property_predicate) in predicate.predicates.iter().enumerate() {
            self.validate_exists_property_predicate(
                property_predicate,
                scope,
                format!("{path}.predicates[{index}]"),
            )?;
        }
        if let Some(predicate) = &predicate.predicate {
            self.validate_scoped_predicate_expression(
                predicate,
                scope,
                format!("{path}.predicate"),
            )?;
        }
        self.infer_scoped_scalar_expression_type(target, scope, format!("{path}.target"))?;
        Ok(())
    }

    fn validate_count_relationship_pattern(
        &self,
        predicate: &ExistsPatternPredicate,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        let local_nodes = self.validate_exists_pattern_nodes(predicate, &path)?;
        self.validate_exists_relationship_variables(predicate, &local_nodes, &path)?;
        let relationships =
            self.resolve_exists_relationship_mappings(predicate, &local_nodes, &path)?;
        let scope = ExistsPredicateValidationContext {
            relationships: &relationships,
            local_nodes: &local_nodes,
        };
        for (index, property_predicate) in predicate.predicates.iter().enumerate() {
            self.validate_exists_property_predicate(
                property_predicate,
                scope,
                format!("{path}.predicates[{index}]"),
            )?;
        }
        if let Some(predicate) = &predicate.predicate {
            self.validate_scoped_predicate_expression(
                predicate,
                scope,
                format!("{path}.predicate"),
            )?;
        }
        Ok(())
    }

    pub(super) fn validate_exists_pattern_nodes<'b>(
        &self,
        predicate: &'b ExistsPatternPredicate,
        path: &str,
    ) -> Result<BTreeMap<&'b str, &'a Node>, CoreError> {
        self.validate_scoped_node_patterns(&predicate.nodes, path, "EXISTS pattern")
    }

    pub(super) fn validate_exists_relationship_variables(
        &self,
        predicate: &ExistsPatternPredicate,
        local_nodes: &BTreeMap<&str, &Node>,
        path: &str,
    ) -> Result<(), CoreError> {
        let mut relationship_variables = BTreeSet::new();
        for (index, relationship) in predicate.relationships.iter().enumerate() {
            let Some(variable) = relationship.variable.as_deref() else {
                continue;
            };
            validate_variable(format!("{path}.relationships[{index}].variable"), variable)?;
            if self.bindings.contains_key(variable) || local_nodes.contains_key(variable) {
                return Err(Diagnostic::new(
                    diagnostic_codes::DUPLICATE_VARIABLE,
                    format!("{path}.relationships[{index}].variable"),
                    format!("EXISTS pattern relationship variable '{variable}' shadows another graph variable"),
                )
                .into_core_error());
            }
            if !relationship_variables.insert(variable) {
                return Err(Diagnostic::new(
                    diagnostic_codes::DUPLICATE_VARIABLE,
                    format!("{path}.relationships[{index}].variable"),
                    format!(
                        "EXISTS pattern relationship variable '{variable}' is bound more than once"
                    ),
                )
                .into_core_error());
            }
        }
        Ok(())
    }

    fn resolve_exists_relationship_mappings<'b>(
        &self,
        predicate: &'b ExistsPatternPredicate,
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        path: &str,
    ) -> Result<Vec<ExistsRelationshipValidation<'a, 'b>>, CoreError> {
        predicate
            .relationships
            .iter()
            .enumerate()
            .map(|(index, relationship)| {
                self.resolve_exists_relationship_mapping(
                    relationship,
                    local_nodes,
                    format!("{path}.relationships[{index}]"),
                )
                .map(|mapping| ExistsRelationshipValidation {
                    pattern: relationship,
                    relationship: mapping,
                })
            })
            .collect()
    }

    fn resolve_exists_relationship_mapping<'b>(
        &self,
        relationship: &'b RelationshipPattern,
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        path: impl Into<String>,
    ) -> Result<&'a Relationship, CoreError> {
        let path = path.into();
        let left_node = self.exists_node_binding_for_path(
            local_nodes,
            &relationship.left,
            format!("{path}.left"),
        )?;
        let right_node = self.exists_node_binding_for_path(
            local_nodes,
            &relationship.right,
            format!("{path}.right"),
        )?;
        self.resolve_relationship_mapping_for_nodes(relationship, left_node, right_node, path)
    }

    pub(super) fn resolve_relationship_mapping_for_nodes(
        &self,
        relationship: &RelationshipPattern,
        left_node: &Node,
        right_node: &Node,
        path: String,
    ) -> Result<&'a Relationship, CoreError> {
        let candidates = self
            .graph
            .relationships_for_type(&relationship.relationship_type)
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Err(Diagnostic::new(
                diagnostic_codes::UNKNOWN_RELATIONSHIP_TYPE,
                format!("{path}.type"),
                format!(
                    "unknown relationship type '{}'",
                    relationship.relationship_type
                ),
            )
            .into_core_error());
        }

        let matches = candidates
            .iter()
            .copied()
            .filter(|candidate| {
                Self::relationship_matches_pattern(
                    candidate,
                    relationship.direction,
                    &left_node.label,
                    &right_node.label,
                )
            })
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [relationship] => Ok(*relationship),
            [] => {
                let available = candidates
                    .iter()
                    .map(|relationship| {
                        format!(
                            "{} -> {}",
                            relationship.from.label, relationship.to.label
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                Err(Diagnostic::new(
                    diagnostic_codes::RELATIONSHIP_ENDPOINT_MISMATCH,
                    path.clone(),
                    format!(
                        "relationship type '{}' has no mapping for {} -> {}; available endpoint mappings: {}",
                        relationship.relationship_type, left_node.label, right_node.label, available
                    ),
                )
                .into_core_error())
            }
            _ => Err(Diagnostic::new(
                diagnostic_codes::AMBIGUOUS_RELATIONSHIP_MAPPING,
                path,
                format!(
                    "relationship type '{}' with endpoints {} -> {} matches {} mappings; add direction or use distinct relationship types",
                    relationship.relationship_type,
                    left_node.label,
                    right_node.label,
                    matches.len()
                ),
            )
            .into_core_error()),
        }
    }

    pub(super) fn validate_exists_pattern_not_empty(
        predicate: &ExistsPatternPredicate,
        path: &str,
    ) -> Result<(), CoreError> {
        if predicate.relationships.is_empty() && predicate.nodes.is_empty() {
            return Err(Diagnostic::new(
                diagnostic_codes::UNSUPPORTED_EXISTS_PATTERN,
                format!("{path}.pattern"),
                "EXISTS pattern predicates require at least one local node or relationship pattern",
            )
            .into_core_error());
        }
        Ok(())
    }

    fn exists_node_binding_for_path<'b>(
        &self,
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        variable: &str,
        path: impl Into<String>,
    ) -> Result<&'a Node, CoreError> {
        if let Some(node) = local_nodes.get(variable).copied() {
            return Ok(node);
        }
        let path = path.into();
        match self.bindings.get(variable).map(ValidatedBinding::kind) {
            Some(
                ValidatedBindingKind::Node(node) | ValidatedBindingKind::StageColumn { node, .. },
            ) => Ok(*node),
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

    pub(super) fn validate_exists_property_predicate<'b>(
        &self,
        predicate: &PropertyPredicate,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        self.validate_exists_property_ref(
            &predicate.property,
            scope.relationships,
            scope.local_nodes,
            format!("{path}.property"),
        )?;
        match &predicate.rhs {
            PredicateRhs::Literal(literal) => {
                if predicate.operator == ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        diagnostic_codes::INVALID_PREDICATE_OPERAND,
                        path.clone(),
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                Self::validate_string_predicate(path.clone(), predicate.operator, literal)?;
                Self::validate_literal_predicate(path.clone(), predicate.operator, literal)
            }
            PredicateRhs::Property(property) => {
                if predicate.operator == ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        diagnostic_codes::INVALID_PREDICATE_OPERAND,
                        path.clone(),
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                Self::validate_non_literal_string_predicate_operand(
                    path.clone(),
                    predicate.operator,
                )?;
                self.validate_exists_property_ref(
                    property,
                    scope.relationships,
                    scope.local_nodes,
                    format!("{path}.rhs"),
                )
            }
            PredicateRhs::Key { variable } | PredicateRhs::ElementId { variable } => {
                if predicate.operator == ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        diagnostic_codes::INVALID_PREDICATE_OPERAND,
                        path.clone(),
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                Self::validate_non_literal_string_predicate_operand(
                    path.clone(),
                    predicate.operator,
                )?;
                self.validate_exists_key_ref(
                    variable,
                    scope.relationships,
                    scope.local_nodes,
                    format!("{path}.rhs"),
                )
            }
            PredicateRhs::List(_) => {
                if predicate.operator != ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        diagnostic_codes::INVALID_PREDICATE_OPERAND,
                        path.clone(),
                        "literal lists are only supported with IN predicates",
                    )
                    .into_core_error());
                }
                Ok(())
            }
        }?;
        let lhs_type = self.exists_property_ref_scalar_type(
            &predicate.property,
            scope.relationships,
            scope.local_nodes,
        )?;
        self.validate_exists_predicate_rhs_operand_types(predicate, lhs_type, scope, &path)
    }

    pub(super) fn validate_exists_property_ref<'b>(
        &self,
        property: &PropertyRef,
        relationships: &[ExistsRelationshipValidation<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        let column = self.exists_column_for_property(property, relationships, local_nodes)?;
        if column.is_none() {
            return Err(Diagnostic::new(
                diagnostic_codes::UNKNOWN_PROPERTY,
                path,
                format!(
                    "variable '{}' does not expose property '{}'",
                    property.variable, property.property
                ),
            )
            .into_core_error());
        }
        Ok(())
    }

    pub(super) fn validate_exists_key_ref<'b>(
        &self,
        variable: &str,
        relationships: &[ExistsRelationshipValidation<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        if let Some(relationship) = Self::exists_relationship_for_variable(relationships, variable)
        {
            if relationship.key.is_some() {
                return Ok(());
            }
            return Err(Diagnostic::new(
                diagnostic_codes::INVALID_KEY_PROJECTION,
                path,
                format!(
                    "id({variable}) requires relationship type '{}' to declare a key column",
                    relationship.relationship_type
                ),
            )
            .into_core_error());
        }
        if local_nodes.contains_key(variable) {
            return Ok(());
        }
        self.validate_key_projection(variable, path)
    }

    fn exists_column_for_property<'b>(
        &self,
        property: &PropertyRef,
        relationships: &[ExistsRelationshipValidation<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
    ) -> Result<Option<&'a str>, CoreError> {
        if let Some(relationship) =
            Self::exists_relationship_for_variable(relationships, &property.variable)
        {
            return Ok(relationship.column_for_property(&property.property));
        }
        if let Some(node) = local_nodes.get(property.variable.as_str()).copied() {
            return Ok(node.column_for_property(&property.property));
        }
        let binding = self
            .bindings
            .get(property.variable.as_str())
            .ok_or_else(|| {
                Diagnostic::new(
                    diagnostic_codes::UNKNOWN_VARIABLE,
                    "property.variable",
                    format!("unknown graph variable '{}'", property.variable),
                )
                .into_core_error()
            })?;
        Ok(match binding.kind() {
            ValidatedBindingKind::Node(node) | ValidatedBindingKind::StageColumn { node, .. } => {
                node.column_for_property(&property.property)
            }
            ValidatedBindingKind::Relationship(relationship) => {
                relationship.column_for_property(&property.property)
            }
        })
    }

    pub(super) fn exists_property_ref_scalar_type<'b>(
        &self,
        property: &PropertyRef,
        relationships: &[ExistsRelationshipValidation<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
    ) -> Result<ScalarType, CoreError> {
        if let Some(relationship) =
            Self::exists_relationship_for_variable(relationships, &property.variable)
        {
            let Some(column) = relationship.column_for_property(&property.property) else {
                return Ok(ScalarType::Unknown);
            };
            return Ok(self.column_scalar_type(&relationship.table, column));
        }
        if let Some(node) = local_nodes.get(property.variable.as_str()).copied() {
            let Some(column) = node.column_for_property(&property.property) else {
                return Ok(ScalarType::Unknown);
            };
            return Ok(self.column_scalar_type(&node.table, column));
        }
        self.property_ref_scalar_type(property)
    }

    fn validate_exists_predicate_rhs_operand_types<'b>(
        &self,
        predicate: &PropertyPredicate,
        lhs_type: ScalarType,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: &str,
    ) -> Result<(), CoreError> {
        match &predicate.rhs {
            PredicateRhs::Literal(literal) => Self::validate_scalar_predicate_operand_types(
                predicate.operator,
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
                Self::validate_scalar_predicate_operand_types(
                    predicate.operator,
                    lhs_type,
                    rhs_type,
                    path,
                )
            }
            PredicateRhs::Key { variable } => {
                let rhs_type = if let Some(relationship) =
                    Self::exists_relationship_for_variable(scope.relationships, variable)
                {
                    relationship
                        .key
                        .as_deref()
                        .map_or(ScalarType::Unknown, |column| {
                            self.column_scalar_type(&relationship.table, column)
                        })
                } else if let Some(node) = scope.local_nodes.get(variable.as_str()).copied() {
                    self.column_scalar_type(&node.table, &node.key)
                } else {
                    self.key_scalar_type(variable)?
                };
                Self::validate_scalar_predicate_operand_types(
                    predicate.operator,
                    lhs_type,
                    rhs_type,
                    path,
                )
            }
            PredicateRhs::ElementId { .. } => Self::validate_scalar_predicate_operand_types(
                predicate.operator,
                lhs_type,
                ScalarType::String,
                path,
            ),
            PredicateRhs::List(literals) => {
                Self::validate_scalar_in_list_operand_types(lhs_type, literals, path)
            }
        }
    }

    pub(super) fn exists_relationship_for_variable<'b>(
        relationships: &[ExistsRelationshipValidation<'a, 'b>],
        variable: &str,
    ) -> Option<&'a Relationship> {
        relationships.iter().find_map(|candidate| {
            (candidate.pattern.variable.as_deref() == Some(variable))
                .then_some(candidate.relationship)
        })
    }
}
