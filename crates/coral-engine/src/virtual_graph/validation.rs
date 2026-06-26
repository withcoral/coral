use std::collections::{BTreeMap, BTreeSet};

use super::declaration::{Declaration, Node, Relationship};
use super::diagnostic::Diagnostic;
use super::ir::{
    AggregateFunction, AggregateTarget, ComparisonOperator, Direction, GraphPlan, KeyPredicate,
    Literal, OrderExpression, PredicateExpression, PredicateRhs, Projection, ProjectionPredicate,
    ProjectionPredicateExpression, ProjectionPredicateRhs, PropertyPredicate, PropertyRef,
    RelationshipPattern,
};
use crate::CoreError;

/// Graph plan validated against a specific declaration.
#[derive(Debug, Clone)]
pub(crate) struct ValidatedGraphPlan<'a> {
    plan: &'a GraphPlan,
    bindings: BTreeMap<&'a str, ValidatedBinding<'a>>,
    relationship_mappings: Vec<&'a Relationship>,
}

/// Resolved query variable binding.
#[derive(Debug, Clone)]
pub(crate) struct ValidatedBinding<'a> {
    alias: String,
    kind: ValidatedBindingKind<'a>,
}

/// Resolved binding target.
#[derive(Debug, Clone)]
pub(crate) enum ValidatedBindingKind<'a> {
    Node(&'a Node),
    Relationship(&'a Relationship),
}

impl Declaration {
    /// Validates a shared graph query plan against this declaration.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidInput`] when the graph plan references
    /// unknown labels, relationship types, variables, or properties, or when
    /// the plan shape is not supported by the current deterministic lowerer.
    pub(crate) fn validate_graph_plan<'a>(
        &'a self,
        plan: &'a GraphPlan,
    ) -> Result<ValidatedGraphPlan<'a>, CoreError> {
        GraphPlanValidator::new(self, plan).validate()
    }
}

impl<'a> ValidatedGraphPlan<'a> {
    pub(crate) fn plan(&self) -> &'a GraphPlan {
        self.plan
    }

    pub(crate) fn binding(&self, variable: &str) -> Result<&ValidatedBinding<'a>, CoreError> {
        self.bindings.get(variable).ok_or_else(|| {
            Diagnostic::new(
                "UNKNOWN_VARIABLE",
                "variable",
                format!("unknown graph variable '{variable}'"),
            )
            .into_core_error()
        })
    }

    pub(crate) fn node_binding(&self, variable: &str) -> Result<&Node, CoreError> {
        let binding = self.binding(variable)?;
        let ValidatedBindingKind::Node(node) = binding.kind() else {
            return Err(Diagnostic::new(
                "INVALID_ENDPOINT_VARIABLE",
                "variable",
                format!("relationship endpoint '{variable}' is not a node variable"),
            )
            .into_core_error());
        };
        Ok(node)
    }

    pub(crate) fn relationship_mapping(&self, index: usize) -> Result<&Relationship, CoreError> {
        self.relationship_mappings
            .get(index)
            .copied()
            .ok_or_else(|| CoreError::internal("validated relationship mapping missing"))
    }

    pub(crate) fn relationship_is_optional(&self, index: usize) -> bool {
        self.plan
            .optional_relationships
            .binary_search(&index)
            .is_ok()
    }

    pub(crate) fn relationship_alias(&self, index: usize, pattern: &RelationshipPattern) -> String {
        pattern
            .variable
            .as_deref()
            .and_then(|variable| self.bindings.get(variable))
            .map_or_else(
                || format!("r{index}"),
                |binding| binding.alias().to_string(),
            )
    }
}

impl<'a> ValidatedBinding<'a> {
    pub(crate) fn alias(&self) -> &str {
        &self.alias
    }

    pub(crate) fn kind(&self) -> &ValidatedBindingKind<'a> {
        &self.kind
    }

    fn column_for_property(&self, property: &str) -> Option<&str> {
        match self.kind {
            ValidatedBindingKind::Node(node) => node.column_for_property(property),
            ValidatedBindingKind::Relationship(relationship) => {
                relationship.column_for_property(property)
            }
        }
    }
}

struct GraphPlanValidator<'a> {
    graph: &'a Declaration,
    plan: &'a GraphPlan,
    bindings: BTreeMap<&'a str, ValidatedBinding<'a>>,
    relationship_mappings: Vec<&'a Relationship>,
}

impl<'a> GraphPlanValidator<'a> {
    fn new(graph: &'a Declaration, plan: &'a GraphPlan) -> Self {
        Self {
            graph,
            plan,
            bindings: BTreeMap::new(),
            relationship_mappings: Vec::with_capacity(plan.relationships.len()),
        }
    }

    fn validate(mut self) -> Result<ValidatedGraphPlan<'a>, CoreError> {
        self.bind_nodes()?;
        self.bind_relationships()?;
        self.validate_optional_relationship_indices()?;
        self.validate_projection_shape()?;
        self.validate_aggregation()?;
        self.validate_property_references()?;
        self.validate_optional_predicates()?;
        self.validate_distinct_ordering()?;
        self.validate_connectivity()?;

        Ok(ValidatedGraphPlan {
            plan: self.plan,
            bindings: self.bindings,
            relationship_mappings: self.relationship_mappings,
        })
    }

    fn bind_nodes(&mut self) -> Result<(), CoreError> {
        if self.plan.nodes.is_empty() {
            return Err(Diagnostic::new(
                "EMPTY_PLAN",
                "nodes",
                "at least one node pattern is required",
            )
            .into_core_error());
        }

        for (index, pattern) in self.plan.nodes.iter().enumerate() {
            validate_variable(format!("nodes[{index}].variable"), &pattern.variable)?;
            if self.bindings.contains_key(pattern.variable.as_str()) {
                return Err(Diagnostic::new(
                    "DUPLICATE_VARIABLE",
                    format!("nodes[{index}].variable"),
                    format!("variable '{}' is bound more than once", pattern.variable),
                )
                .into_core_error());
            }
            let node = self.graph.node(&pattern.label).ok_or_else(|| {
                Diagnostic::new(
                    "UNKNOWN_NODE_LABEL",
                    format!("nodes[{index}].label"),
                    format!("unknown node label '{}'", pattern.label),
                )
                .into_core_error()
            })?;
            self.bindings.insert(
                pattern.variable.as_str(),
                ValidatedBinding {
                    alias: format!("n{index}"),
                    kind: ValidatedBindingKind::Node(node),
                },
            );
        }
        Ok(())
    }

    fn bind_relationships(&mut self) -> Result<(), CoreError> {
        for (index, pattern) in self.plan.relationships.iter().enumerate() {
            let relationship = self.resolve_relationship_mapping(index, pattern)?;
            if let Some(variable) = &pattern.variable {
                validate_variable(format!("relationships[{index}].variable"), variable)?;
                if self.bindings.contains_key(variable.as_str()) {
                    return Err(Diagnostic::new(
                        "DUPLICATE_VARIABLE",
                        format!("relationships[{index}].variable"),
                        format!("variable '{variable}' is bound more than once"),
                    )
                    .into_core_error());
                }
                self.bindings.insert(
                    variable.as_str(),
                    ValidatedBinding {
                        alias: format!("r{index}"),
                        kind: ValidatedBindingKind::Relationship(relationship),
                    },
                );
            }
            self.relationship_mappings.push(relationship);
        }
        Ok(())
    }

    fn resolve_relationship_mapping(
        &self,
        index: usize,
        pattern: &RelationshipPattern,
    ) -> Result<&'a Relationship, CoreError> {
        let left_node =
            self.node_binding_for_path(&pattern.left, format!("relationships[{index}].left"))?;
        let right_node =
            self.node_binding_for_path(&pattern.right, format!("relationships[{index}].right"))?;
        let candidates = self
            .graph
            .relationships_for_type(&pattern.relationship_type)
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Err(Diagnostic::new(
                "UNKNOWN_RELATIONSHIP_TYPE",
                format!("relationships[{index}].type"),
                format!("unknown relationship type '{}'", pattern.relationship_type),
            )
            .into_core_error());
        }

        let matches = candidates
            .iter()
            .copied()
            .filter(|relationship| {
                Self::relationship_matches_pattern(
                    relationship,
                    pattern.direction,
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
                    "RELATIONSHIP_ENDPOINT_MISMATCH",
                    format!("relationships[{index}]"),
                    format!(
                        "relationship type '{}' has no mapping for {} -> {}; available endpoint mappings: {}",
                        pattern.relationship_type, left_node.label, right_node.label, available
                    ),
                )
                .into_core_error())
            }
            _ => Err(Diagnostic::new(
                "AMBIGUOUS_RELATIONSHIP_MAPPING",
                format!("relationships[{index}]"),
                format!(
                    "relationship type '{}' with endpoints {} -> {} matches {} mappings; add direction or use distinct relationship types",
                    pattern.relationship_type,
                    left_node.label,
                    right_node.label,
                    matches.len()
                ),
            )
            .into_core_error()),
        }
    }

    fn relationship_matches_pattern(
        relationship: &Relationship,
        direction: Direction,
        left_label: &str,
        right_label: &str,
    ) -> bool {
        let matches_forward =
            left_label == relationship.from.label && right_label == relationship.to.label;
        let matches_reverse =
            left_label == relationship.to.label && right_label == relationship.from.label;
        match direction {
            Direction::Outgoing => matches_forward,
            Direction::Incoming => matches_reverse,
            Direction::Undirected => matches_forward || matches_reverse,
        }
    }

    fn validate_projection_shape(&self) -> Result<(), CoreError> {
        if self.plan.projections.is_empty() {
            return Err(Diagnostic::new(
                "EMPTY_PROJECTION",
                "projections",
                "at least one projection is required",
            )
            .into_core_error());
        }
        Ok(())
    }

    fn validate_optional_relationship_indices(&self) -> Result<(), CoreError> {
        let mut seen = BTreeSet::new();
        for (position, index) in self.plan.optional_relationships.iter().copied().enumerate() {
            if index >= self.plan.relationships.len() {
                return Err(Diagnostic::new(
                    "INVALID_OPTIONAL_RELATIONSHIP",
                    format!("optional_relationships[{position}]"),
                    format!(
                        "optional relationship index {index} is out of bounds for {} relationships",
                        self.plan.relationships.len()
                    ),
                )
                .into_core_error());
            }
            if !seen.insert(index) {
                return Err(Diagnostic::new(
                    "DUPLICATE_OPTIONAL_RELATIONSHIP",
                    format!("optional_relationships[{position}]"),
                    format!("optional relationship index {index} is listed more than once"),
                )
                .into_core_error());
            }
        }
        if self
            .plan
            .optional_relationships
            .windows(2)
            .any(|pair| matches!(pair, [left, right] if left > right))
        {
            return Err(Diagnostic::new(
                "UNSORTED_OPTIONAL_RELATIONSHIPS",
                "optional_relationships",
                "optional relationship indices must be sorted in ascending order",
            )
            .into_core_error());
        }
        Ok(())
    }

    fn validate_aggregation(&self) -> Result<(), CoreError> {
        let aggregate_count = self
            .plan
            .projections
            .iter()
            .filter(|projection| projection.is_aggregate())
            .count();
        if aggregate_count == 0 {
            return Ok(());
        }
        let projected_properties = self.projected_properties();
        for (index, order_key) in self.plan.order_by.iter().enumerate() {
            if !self.order_expression_is_projected_property_or_alias(
                &order_key.expression,
                &projected_properties,
            ) {
                return Err(Diagnostic::new(
                    "UNSUPPORTED_AGGREGATION",
                    format!("order_by[{index}]"),
                    "ORDER BY with aggregate projections must use a projected property or projection alias",
                )
                .into_core_error());
            }
        }
        Ok(())
    }

    fn validate_property_references(&self) -> Result<(), CoreError> {
        for (index, projection) in self.plan.projections.iter().enumerate() {
            match projection {
                Projection::Property { property, .. } => {
                    self.validate_property_ref(property, format!("projections[{index}].property"))?;
                }
                Projection::Key { variable, .. } => {
                    self.validate_key_projection(
                        variable,
                        format!("projections[{index}].variable"),
                    )?;
                }
                Projection::Literal { .. } | Projection::CountAll { .. } => {}
                Projection::Aggregate {
                    function, target, ..
                } => {
                    self.validate_aggregate_target(
                        *function,
                        target,
                        format!("projections[{index}].target"),
                    )?;
                }
            }
        }
        for (index, predicate) in self.plan.predicates.iter().enumerate() {
            self.validate_predicate(index, predicate)?;
        }
        if let Some(predicate) = &self.plan.predicate {
            self.validate_predicate_expression(predicate, "predicate")?;
        }
        if let Some(predicate) = &self.plan.post_projection_predicate {
            self.validate_projection_predicate_expression(predicate, "post_projection_predicate")?;
        }
        for (index, key) in self.plan.order_by.iter().enumerate() {
            self.validate_order_expression(&key.expression, format!("order_by[{index}]"))?;
        }
        Ok(())
    }

    fn validate_optional_predicates(&self) -> Result<(), CoreError> {
        if self.plan.optional_relationships.is_empty() {
            return Ok(());
        }
        let mandatory_nodes = self.mandatory_reachable_nodes()?;
        let optional_variables = self.optional_variables(&mandatory_nodes);

        for (index, predicate) in self.plan.predicates.iter().enumerate() {
            Self::validate_property_predicate_not_optional(
                predicate,
                &optional_variables,
                format!("predicates[{index}]"),
            )?;
        }
        if let Some(predicate) = &self.plan.predicate {
            Self::validate_predicate_expression_not_optional(
                predicate,
                &optional_variables,
                "predicate",
            )?;
        }
        if self.plan.post_projection_predicate.is_some() {
            return Err(Diagnostic::new(
                "UNSUPPORTED_OPTIONAL_PREDICATE",
                "post_projection_predicate",
                "post-projection predicates with optional matches require null-preserving predicate placement",
            )
            .into_core_error());
        }
        Ok(())
    }

    fn validate_predicate_expression_not_optional(
        predicate: &PredicateExpression,
        optional_variables: &BTreeSet<&str>,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        match predicate {
            PredicateExpression::Comparison(predicate) => {
                Self::validate_property_predicate_not_optional(predicate, optional_variables, path)
            }
            PredicateExpression::KeyComparison(predicate) => {
                Self::validate_key_predicate_not_optional(predicate, optional_variables, path)
            }
            PredicateExpression::And { left, right } | PredicateExpression::Or { left, right } => {
                Self::validate_predicate_expression_not_optional(
                    left,
                    optional_variables,
                    format!("{path}.left"),
                )?;
                Self::validate_predicate_expression_not_optional(
                    right,
                    optional_variables,
                    format!("{path}.right"),
                )
            }
            PredicateExpression::Not { expression } => {
                Self::validate_predicate_expression_not_optional(
                    expression,
                    optional_variables,
                    format!("{path}.expression"),
                )
            }
            PredicateExpression::Boolean(_) => Ok(()),
        }
    }

    fn validate_property_predicate_not_optional(
        predicate: &PropertyPredicate,
        optional_variables: &BTreeSet<&str>,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        Self::validate_variable_not_optional(
            &predicate.property.variable,
            optional_variables,
            format!("{path}.property.variable"),
        )?;
        Self::validate_predicate_rhs_not_optional(&predicate.rhs, optional_variables, path)
    }

    fn validate_key_predicate_not_optional(
        predicate: &KeyPredicate,
        optional_variables: &BTreeSet<&str>,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        Self::validate_variable_not_optional(
            &predicate.variable,
            optional_variables,
            format!("{path}.variable"),
        )?;
        Self::validate_predicate_rhs_not_optional(&predicate.rhs, optional_variables, path)
    }

    fn validate_predicate_rhs_not_optional(
        rhs: &PredicateRhs,
        optional_variables: &BTreeSet<&str>,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        match rhs {
            PredicateRhs::Property(property) => Self::validate_variable_not_optional(
                &property.variable,
                optional_variables,
                format!("{path}.rhs.variable"),
            ),
            PredicateRhs::Key { variable } => Self::validate_variable_not_optional(
                variable,
                optional_variables,
                format!("{path}.rhs.variable"),
            ),
            PredicateRhs::Literal(_) | PredicateRhs::List(_) => Ok(()),
        }
    }

    fn validate_variable_not_optional(
        variable: &str,
        optional_variables: &BTreeSet<&str>,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        if optional_variables.contains(variable) {
            return Err(Diagnostic::new(
                "UNSUPPORTED_OPTIONAL_PREDICATE",
                path,
                format!(
                    "predicate references optional binding '{variable}', which requires null-preserving predicate placement"
                ),
            )
            .into_core_error());
        }
        Ok(())
    }

    fn validate_distinct_ordering(&self) -> Result<(), CoreError> {
        if !self.plan.distinct || self.plan.order_by.is_empty() {
            return Ok(());
        }

        let projected_properties = self.projected_properties();
        for (index, order_key) in self.plan.order_by.iter().enumerate() {
            if !self.order_expression_is_projected_property_or_alias(
                &order_key.expression,
                &projected_properties,
            ) {
                return Err(Diagnostic::new(
                    "UNSUPPORTED_DISTINCT_ORDERING",
                    format!("order_by[{index}]"),
                    "ORDER BY with DISTINCT must use a projected property or projection alias",
                )
                .into_core_error());
            }
        }
        Ok(())
    }

    fn projected_properties(&self) -> Vec<&PropertyRef> {
        self.plan
            .projections
            .iter()
            .filter_map(|projection| match projection {
                Projection::Property { property, .. } => Some(property),
                Projection::Key { .. }
                | Projection::Literal { .. }
                | Projection::CountAll { .. }
                | Projection::Aggregate { .. } => None,
            })
            .collect()
    }

    fn validate_predicate(
        &self,
        index: usize,
        predicate: &PropertyPredicate,
    ) -> Result<(), CoreError> {
        self.validate_property_predicate(predicate, format!("predicates[{index}]"))
    }

    fn validate_predicate_expression(
        &self,
        expression: &PredicateExpression,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        match expression {
            PredicateExpression::Boolean(_) => Ok(()),
            PredicateExpression::Comparison(predicate) => {
                self.validate_property_predicate(predicate, path)
            }
            PredicateExpression::KeyComparison(predicate) => {
                self.validate_key_predicate(predicate, path)
            }
            PredicateExpression::And { left, right } | PredicateExpression::Or { left, right } => {
                self.validate_predicate_expression(left, format!("{path}.left"))?;
                self.validate_predicate_expression(right, format!("{path}.right"))
            }
            PredicateExpression::Not { expression } => {
                self.validate_predicate_expression(expression, format!("{path}.expression"))
            }
        }
    }

    fn validate_projection_predicate_expression(
        &self,
        expression: &ProjectionPredicateExpression,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        match expression {
            ProjectionPredicateExpression::Boolean(_) => Ok(()),
            ProjectionPredicateExpression::Comparison(predicate) => {
                self.validate_projection_predicate(predicate, path)
            }
            ProjectionPredicateExpression::And { left, right }
            | ProjectionPredicateExpression::Or { left, right } => {
                self.validate_projection_predicate_expression(left, format!("{path}.left"))?;
                self.validate_projection_predicate_expression(right, format!("{path}.right"))
            }
            ProjectionPredicateExpression::Not { expression } => self
                .validate_projection_predicate_expression(expression, format!("{path}.expression")),
        }
    }

    fn validate_order_expression(
        &self,
        expression: &OrderExpression,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        match expression {
            OrderExpression::Property(property) => {
                self.validate_property_ref(property, format!("{path}.property"))
            }
            OrderExpression::Key { variable } => {
                self.validate_key_projection(variable, format!("{path}.variable"))
            }
            OrderExpression::Literal(_) => Ok(()),
            OrderExpression::ProjectionAlias(alias) => {
                if self.projection_alias_exists(alias) {
                    Ok(())
                } else {
                    Err(Diagnostic::new(
                        "UNKNOWN_PROJECTION_ALIAS",
                        path,
                        format!("unknown projection alias '{alias}'"),
                    )
                    .into_core_error())
                }
            }
        }
    }

    fn order_expression_is_projected_property_or_alias(
        &self,
        expression: &OrderExpression,
        projected_properties: &[&PropertyRef],
    ) -> bool {
        match expression {
            OrderExpression::Property(property) => projected_properties.contains(&property),
            OrderExpression::Key { variable } => {
                self.plan.projections.iter().any(|projection| {
                    matches!(projection, Projection::Key { variable: projected, .. } if projected == variable)
                })
            }
            OrderExpression::Literal(literal) => {
                self.plan.projections.iter().any(|projection| {
                    matches!(projection, Projection::Literal { literal: projected, .. } if projected == literal)
                })
            }
            OrderExpression::ProjectionAlias(alias) => self.projection_alias_exists(alias),
        }
    }

    fn projection_alias_exists(&self, alias: &str) -> bool {
        self.plan
            .projections
            .iter()
            .any(|projection| match projection {
                Projection::Property {
                    alias: Some(projection_alias),
                    ..
                }
                | Projection::CountAll {
                    alias: projection_alias,
                }
                | Projection::Key {
                    alias: projection_alias,
                    ..
                }
                | Projection::Literal {
                    alias: projection_alias,
                    ..
                }
                | Projection::Aggregate {
                    alias: projection_alias,
                    ..
                } => projection_alias == alias,
                Projection::Property { alias: None, .. } => false,
            })
    }

    fn validate_property_predicate(
        &self,
        predicate: &PropertyPredicate,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        self.validate_property_ref(&predicate.property, format!("{path}.property"))?;
        match &predicate.rhs {
            PredicateRhs::Literal(literal) => {
                if predicate.operator == ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        "INVALID_PREDICATE_OPERAND",
                        path,
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                Self::validate_string_predicate(path.clone(), predicate.operator, literal)?;
                Self::validate_literal_predicate(path, predicate.operator, literal)
            }
            PredicateRhs::Property(property) => {
                if predicate.operator == ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        "INVALID_PREDICATE_OPERAND",
                        path,
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                if matches!(
                    predicate.operator,
                    ComparisonOperator::StartsWith
                        | ComparisonOperator::EndsWith
                        | ComparisonOperator::Contains
                ) {
                    return Err(Diagnostic::new(
                        "INVALID_PREDICATE_OPERAND",
                        path,
                        "string predicates require a string literal right-hand side",
                    )
                    .into_core_error());
                }
                self.validate_property_ref(property, format!("{path}.rhs"))
            }
            PredicateRhs::Key { variable } => {
                if predicate.operator == ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        "INVALID_PREDICATE_OPERAND",
                        path,
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                if matches!(
                    predicate.operator,
                    ComparisonOperator::StartsWith
                        | ComparisonOperator::EndsWith
                        | ComparisonOperator::Contains
                ) {
                    return Err(Diagnostic::new(
                        "INVALID_PREDICATE_OPERAND",
                        path,
                        "string predicates require a string literal right-hand side",
                    )
                    .into_core_error());
                }
                self.validate_key_projection(variable, format!("{path}.rhs"))
            }
            PredicateRhs::List(literals) => {
                if predicate.operator != ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        "INVALID_PREDICATE_OPERAND",
                        path,
                        "literal lists are only supported with IN predicates",
                    )
                    .into_core_error());
                }
                Self::validate_in_list(path, literals)
            }
        }
    }

    fn validate_key_predicate(
        &self,
        predicate: &KeyPredicate,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        self.validate_key_projection(&predicate.variable, format!("{path}.variable"))?;
        match &predicate.rhs {
            PredicateRhs::Literal(literal) => {
                if predicate.operator == ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        "INVALID_PREDICATE_OPERAND",
                        path,
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                Self::validate_string_predicate(path.clone(), predicate.operator, literal)?;
                Self::validate_literal_predicate(path, predicate.operator, literal)
            }
            PredicateRhs::Property(property) => {
                if predicate.operator == ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        "INVALID_PREDICATE_OPERAND",
                        path,
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                if matches!(
                    predicate.operator,
                    ComparisonOperator::StartsWith
                        | ComparisonOperator::EndsWith
                        | ComparisonOperator::Contains
                ) {
                    return Err(Diagnostic::new(
                        "INVALID_PREDICATE_OPERAND",
                        path,
                        "string predicates require a string literal right-hand side",
                    )
                    .into_core_error());
                }
                self.validate_property_ref(property, format!("{path}.rhs"))
            }
            PredicateRhs::Key { variable } => {
                if predicate.operator == ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        "INVALID_PREDICATE_OPERAND",
                        path,
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                if matches!(
                    predicate.operator,
                    ComparisonOperator::StartsWith
                        | ComparisonOperator::EndsWith
                        | ComparisonOperator::Contains
                ) {
                    return Err(Diagnostic::new(
                        "INVALID_PREDICATE_OPERAND",
                        path,
                        "string predicates require a string literal right-hand side",
                    )
                    .into_core_error());
                }
                self.validate_key_projection(variable, format!("{path}.rhs"))
            }
            PredicateRhs::List(literals) => {
                if predicate.operator != ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        "INVALID_PREDICATE_OPERAND",
                        path,
                        "literal lists are only supported with IN predicates",
                    )
                    .into_core_error());
                }
                Self::validate_in_list(path, literals)
            }
        }
    }

    fn validate_projection_predicate(
        &self,
        predicate: &ProjectionPredicate,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        self.validate_projection_alias(&predicate.alias, format!("{path}.alias"))?;
        match &predicate.rhs {
            ProjectionPredicateRhs::Literal(literal) => {
                if predicate.operator == ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        "INVALID_PREDICATE_OPERAND",
                        path,
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                Self::validate_string_predicate(path.clone(), predicate.operator, literal)?;
                Self::validate_literal_predicate(path, predicate.operator, literal)
            }
            ProjectionPredicateRhs::Alias(alias) => {
                if predicate.operator == ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        "INVALID_PREDICATE_OPERAND",
                        path,
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                if matches!(
                    predicate.operator,
                    ComparisonOperator::StartsWith
                        | ComparisonOperator::EndsWith
                        | ComparisonOperator::Contains
                ) {
                    return Err(Diagnostic::new(
                        "INVALID_PREDICATE_OPERAND",
                        path,
                        "string predicates require a string literal right-hand side",
                    )
                    .into_core_error());
                }
                self.validate_projection_alias(alias, format!("{path}.rhs"))
            }
            ProjectionPredicateRhs::List(literals) => {
                if predicate.operator != ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        "INVALID_PREDICATE_OPERAND",
                        path,
                        "literal lists are only supported with IN predicates",
                    )
                    .into_core_error());
                }
                Self::validate_in_list(path, literals)
            }
        }
    }

    fn validate_projection_alias(
        &self,
        alias: &str,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        if self.projection_alias_exists(alias) {
            Ok(())
        } else {
            Err(Diagnostic::new(
                "UNKNOWN_PROJECTION_ALIAS",
                path,
                format!("unknown projection alias '{alias}'"),
            )
            .into_core_error())
        }
    }

    fn validate_aggregate_target(
        &self,
        function: AggregateFunction,
        target: &AggregateTarget,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        match target {
            AggregateTarget::Property(property) => self.validate_property_ref(property, path),
            AggregateTarget::VariableKey { variable } => {
                if function != AggregateFunction::Count {
                    return Err(Diagnostic::new(
                        "INVALID_AGGREGATE_TARGET",
                        path,
                        format!(
                            "{}({variable}) requires a graph property argument; only count(variable) can aggregate a graph variable key",
                            aggregate_function_name(function)
                        ),
                    )
                    .into_core_error());
                }
                validate_variable(path.clone(), variable)?;
                let binding = self.bindings.get(variable.as_str()).ok_or_else(|| {
                    Diagnostic::new(
                        "UNKNOWN_VARIABLE",
                        path.clone(),
                        format!("unknown graph variable '{variable}'"),
                    )
                    .into_core_error()
                })?;
                match binding.kind() {
                    ValidatedBindingKind::Node(_) => Ok(()),
                    ValidatedBindingKind::Relationship(relationship) => {
                        if relationship.key.is_some() {
                            Ok(())
                        } else {
                            Err(Diagnostic::new(
                                "INVALID_AGGREGATE_TARGET",
                                path,
                                format!(
                                    "count({variable}) requires relationship mapping '{}' to declare a key",
                                    relationship.relationship_type
                                ),
                            )
                            .into_core_error())
                        }
                    }
                }
            }
        }
    }

    fn validate_key_projection(
        &self,
        variable: &str,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        let binding = self.bindings.get(variable).ok_or_else(|| {
            Diagnostic::new(
                "UNKNOWN_VARIABLE",
                path.clone(),
                format!("unknown graph variable '{variable}'"),
            )
            .into_core_error()
        })?;
        match binding.kind() {
            ValidatedBindingKind::Node(_) => Ok(()),
            ValidatedBindingKind::Relationship(relationship) => {
                if relationship.key.is_some() {
                    Ok(())
                } else {
                    Err(Diagnostic::new(
                        "INVALID_KEY_PROJECTION",
                        path,
                        format!(
                            "id({variable}) requires relationship type '{}' to declare a key column",
                            relationship.relationship_type
                        ),
                    )
                    .into_core_error())
                }
            }
        }
    }

    fn validate_string_predicate(
        path: impl Into<String>,
        operator: ComparisonOperator,
        literal: &Literal,
    ) -> Result<(), CoreError> {
        if !matches!(
            operator,
            ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains
        ) {
            return Ok(());
        }
        if !matches!(literal, Literal::String(_)) {
            return Err(Diagnostic::new(
                "INVALID_PREDICATE_OPERAND",
                path,
                "string predicates require a string literal right-hand side",
            )
            .into_core_error());
        }
        Ok(())
    }

    fn validate_in_list(path: impl Into<String>, literals: &[Literal]) -> Result<(), CoreError> {
        let path = path.into();
        for (index, literal) in literals.iter().enumerate() {
            if matches!(literal, Literal::Null) {
                return Err(Diagnostic::new(
                    "UNSUPPORTED_IN_LIST",
                    format!("{path}.rhs[{index}]"),
                    "null values in IN lists are not supported yet",
                )
                .into_core_error());
            }
        }
        Ok(())
    }

    fn validate_literal_predicate(
        path: impl Into<String>,
        operator: ComparisonOperator,
        literal: &Literal,
    ) -> Result<(), CoreError> {
        let path = path.into();
        match (operator, literal) {
            (
                ComparisonOperator::GreaterThan
                | ComparisonOperator::GreaterThanOrEqual
                | ComparisonOperator::LessThan
                | ComparisonOperator::LessThanOrEqual,
                Literal::Null,
            ) => Err(Diagnostic::new(
                "INVALID_NULL_COMPARISON",
                path,
                "null can only be compared with equality or inequality",
            )
            .into_core_error()),
            _ => Ok(()),
        }
    }

    fn validate_property_ref(
        &self,
        property: &PropertyRef,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        let binding = self
            .bindings
            .get(property.variable.as_str())
            .ok_or_else(|| {
                Diagnostic::new(
                    "UNKNOWN_VARIABLE",
                    path.clone(),
                    format!("unknown graph variable '{}'", property.variable),
                )
                .into_core_error()
            })?;
        if binding.column_for_property(&property.property).is_none() {
            return Err(Diagnostic::new(
                "UNKNOWN_PROPERTY",
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

    fn validate_connectivity(&self) -> Result<(), CoreError> {
        let mandatory_nodes = self.mandatory_reachable_nodes()?;
        for (index, relationship) in self.plan.relationships.iter().enumerate() {
            if self
                .plan
                .optional_relationships
                .binary_search(&index)
                .is_ok()
            {
                continue;
            }
            if !mandatory_nodes.contains(relationship.left.as_str())
                || !mandatory_nodes.contains(relationship.right.as_str())
            {
                return Err(Diagnostic::new(
                    "MANDATORY_RELATIONSHIP_DEPENDS_ON_OPTIONAL_BINDING",
                    format!("relationships[{index}]"),
                    "mandatory relationships cannot depend on bindings introduced only by OPTIONAL MATCH",
                )
                .into_core_error());
            }
        }

        let all_joined_nodes = self.optional_reachable_nodes(&mandatory_nodes)?;
        for node in &self.plan.nodes {
            if !all_joined_nodes.contains(node.variable.as_str()) {
                return Err(Diagnostic::new(
                    "DISCONNECTED_PATTERN",
                    "nodes",
                    format!(
                        "node variable '{}' is not connected to the first node pattern",
                        node.variable
                    ),
                )
                .into_core_error());
            }
        }
        Ok(())
    }

    fn mandatory_reachable_nodes(&self) -> Result<BTreeSet<&'a str>, CoreError> {
        let first_node = self.plan.nodes.first().ok_or_else(|| {
            Diagnostic::new(
                "EMPTY_PLAN",
                "nodes",
                "at least one node pattern is required",
            )
            .into_core_error()
        })?;
        let mut joined_nodes = BTreeSet::new();
        joined_nodes.insert(first_node.variable.as_str());

        let mut remaining_relationships = (0..self.plan.relationships.len())
            .filter(|index| {
                self.plan
                    .optional_relationships
                    .binary_search(index)
                    .is_err()
            })
            .collect::<BTreeSet<_>>();
        while !remaining_relationships.is_empty() {
            let mut progressed = false;
            for index in remaining_relationships.iter().copied().collect::<Vec<_>>() {
                let pattern = self.plan.relationships.get(index).ok_or_else(|| {
                    CoreError::internal("validated relationship index was out of bounds")
                })?;
                let left_joined = joined_nodes.contains(pattern.left.as_str());
                let right_joined = joined_nodes.contains(pattern.right.as_str());
                if left_joined || right_joined {
                    joined_nodes.insert(pattern.left.as_str());
                    joined_nodes.insert(pattern.right.as_str());
                    remaining_relationships.remove(&index);
                    progressed = true;
                }
            }
            if !progressed {
                let index = *remaining_relationships
                    .first()
                    .ok_or_else(|| CoreError::internal("remaining relationship set was empty"))?;
                return Err(Diagnostic::new(
                    "DISCONNECTED_PATTERN",
                    format!("relationships[{index}]"),
                    "relationship is not connected to the first node pattern",
                )
                .into_core_error());
            }
        }
        Ok(joined_nodes)
    }

    fn optional_reachable_nodes(
        &self,
        mandatory_nodes: &BTreeSet<&'a str>,
    ) -> Result<BTreeSet<&'a str>, CoreError> {
        let mut joined_nodes = mandatory_nodes.clone();
        let mut remaining_relationships = self
            .plan
            .optional_relationships
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        while !remaining_relationships.is_empty() {
            let mut progressed = false;
            for index in remaining_relationships.iter().copied().collect::<Vec<_>>() {
                let pattern = self.plan.relationships.get(index).ok_or_else(|| {
                    CoreError::internal("validated relationship index was out of bounds")
                })?;
                let left_joined = joined_nodes.contains(pattern.left.as_str());
                let right_joined = joined_nodes.contains(pattern.right.as_str());
                if left_joined || right_joined {
                    joined_nodes.insert(pattern.left.as_str());
                    joined_nodes.insert(pattern.right.as_str());
                    remaining_relationships.remove(&index);
                    progressed = true;
                }
            }
            if !progressed {
                let index = *remaining_relationships.first().ok_or_else(|| {
                    CoreError::internal("remaining optional relationship set was empty")
                })?;
                return Err(Diagnostic::new(
                    "OPTIONAL_MATCH_NOT_ANCHORED",
                    format!("optional_relationships[{index}]"),
                    "optional relationship is not anchored to a mandatory or earlier optional binding",
                )
                .into_core_error());
            }
        }
        Ok(joined_nodes)
    }

    fn optional_variables(&self, mandatory_nodes: &BTreeSet<&'a str>) -> BTreeSet<&'a str> {
        let optional_relationships = self
            .plan
            .optional_relationships
            .iter()
            .filter_map(|index| self.plan.relationships.get(*index))
            .filter_map(|relationship| relationship.variable.as_deref());
        self.plan
            .nodes
            .iter()
            .map(|node| node.variable.as_str())
            .filter(|variable| !mandatory_nodes.contains(variable))
            .chain(optional_relationships)
            .collect()
    }

    fn node_binding_for_path(
        &self,
        variable: &str,
        path: impl Into<String>,
    ) -> Result<&Node, CoreError> {
        let path = path.into();
        match self.bindings.get(variable).map(ValidatedBinding::kind) {
            Some(ValidatedBindingKind::Node(node)) => Ok(node),
            Some(ValidatedBindingKind::Relationship(_)) => Err(Diagnostic::new(
                "INVALID_ENDPOINT_VARIABLE",
                path,
                format!("relationship endpoint '{variable}' is not a node variable"),
            )
            .into_core_error()),
            None => Err(Diagnostic::new(
                "UNKNOWN_VARIABLE",
                path,
                format!("relationship references unknown node variable '{variable}'"),
            )
            .into_core_error()),
        }
    }
}

fn validate_variable(path: impl Into<String>, variable: &str) -> Result<(), CoreError> {
    let path = path.into();
    if variable.trim().is_empty() {
        return Err(
            Diagnostic::new("EMPTY_VARIABLE", path, "variable must not be empty").into_core_error(),
        );
    }
    Ok(())
}

fn aggregate_function_name(function: AggregateFunction) -> &'static str {
    match function {
        AggregateFunction::Count => "count",
        AggregateFunction::Sum => "sum",
        AggregateFunction::Avg => "avg",
        AggregateFunction::Min => "min",
        AggregateFunction::Max => "max",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::virtual_graph::ir::{
        AggregateFunction, AggregateTarget, Direction, KeyPredicate, NodePattern, OrderDirection,
        OrderExpression, OrderKey, PredicateExpression, PredicateRhs, Projection,
        PropertyPredicate, PropertyRef, RelationshipPattern,
    };

    const GRAPH: &str = r"
version: 1
name: ownership
nodes:
  - label: Person
    table: { schema: ops, name: people }
    key: id
    properties:
      name: full_name
  - label: Service
    table: { schema: ops, name: services }
    key: id
    properties:
      name: service_name
      tier: tier
relationships:
  - type: OWNS
    table: { schema: ops, name: ownerships }
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
    properties:
      since: since
";

    #[test]
    fn validate_graph_plan_resolves_bindings_and_relationships() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let plan = ownership_plan();

        let validated = graph
            .validate_graph_plan(&plan)
            .expect("plan should validate");

        assert_eq!(validated.binding("person").unwrap().alias(), "n0");
        assert_eq!(validated.binding("owns").unwrap().alias(), "r0");
        assert_eq!(
            validated
                .relationship_mapping(0)
                .expect("relationship mapping")
                .relationship_type,
            "OWNS"
        );
    }

    #[test]
    fn validate_graph_plan_selects_relationship_type_overload_by_endpoint_labels() {
        let graph = Declaration::from_yaml(
            r"
version: 1
name: overloaded-ownership
nodes:
  - label: Person
    table: { schema: ops, name: people }
    key: id
    properties:
      name: full_name
  - label: Team
    table: { schema: ops, name: teams }
    key: id
    properties:
      name: team_name
  - label: Service
    table: { schema: ops, name: services }
    key: id
    properties:
      name: service_name
relationships:
  - type: OWNS
    table: { schema: ops, name: person_ownerships }
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
  - type: OWNS
    table: { schema: ops, name: team_ownerships }
    from: { label: Team, key: team_id }
    to: { label: Service, key: service_id }
",
        )
        .expect("graph should parse");
        let plan = GraphPlan {
            nodes: vec![
                NodePattern {
                    variable: "team".to_string(),
                    label: "Team".to_string(),
                },
                NodePattern {
                    variable: "service".to_string(),
                    label: "Service".to_string(),
                },
            ],
            relationships: vec![RelationshipPattern {
                variable: Some("owns".to_string()),
                relationship_type: "OWNS".to_string(),
                left: "team".to_string(),
                direction: Direction::Outgoing,
                right: "service".to_string(),
            }],
            optional_relationships: Vec::new(),
            distinct: false,
            projections: vec![Projection::Property {
                property: PropertyRef {
                    variable: "team".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("owner".to_string()),
            }],
            predicates: Vec::new(),
            predicate: None,
            post_projection_predicate: None,
            order_by: Vec::new(),
            skip: None,
            limit: None,
        };

        let validated = graph
            .validate_graph_plan(&plan)
            .expect("plan should validate");

        assert_eq!(
            validated
                .relationship_mapping(0)
                .expect("relationship mapping")
                .table
                .name,
            "team_ownerships"
        );
    }

    #[test]
    fn validate_graph_plan_rejects_ambiguous_undirected_relationship_overloads() {
        let graph = Declaration::from_yaml(
            r"
version: 1
name: inverse-ownership
nodes:
  - label: Person
    table: { schema: ops, name: people }
    key: id
    properties:
      name: full_name
  - label: Service
    table: { schema: ops, name: services }
    key: id
    properties:
      name: service_name
relationships:
  - type: OWNS
    table: { schema: ops, name: person_ownerships }
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
  - type: OWNS
    table: { schema: ops, name: service_owner_edges }
    from: { label: Service, key: service_id }
    to: { label: Person, key: person_id }
",
        )
        .expect("graph should parse");
        let mut plan = ownership_plan();
        plan.relationships
            .first_mut()
            .expect("ownership plan should have a relationship")
            .direction = Direction::Undirected;

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("undirected inverse overloads should be ambiguous");

        assert!(
            error.to_string().contains("AMBIGUOUS_RELATIONSHIP_MAPPING"),
            "{error:?}"
        );
    }

    #[test]
    fn validate_graph_plan_accepts_undirected_reversed_relationship_labels() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let plan = GraphPlan {
            nodes: vec![
                NodePattern {
                    variable: "service".to_string(),
                    label: "Service".to_string(),
                },
                NodePattern {
                    variable: "person".to_string(),
                    label: "Person".to_string(),
                },
            ],
            relationships: vec![RelationshipPattern {
                variable: None,
                relationship_type: "OWNS".to_string(),
                left: "service".to_string(),
                direction: Direction::Undirected,
                right: "person".to_string(),
            }],
            optional_relationships: Vec::new(),
            distinct: false,
            projections: vec![Projection::Property {
                property: PropertyRef {
                    variable: "person".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("owner".to_string()),
            }],
            predicates: Vec::new(),
            predicate: None,
            post_projection_predicate: None,
            order_by: Vec::new(),
            skip: None,
            limit: None,
        };

        graph
            .validate_graph_plan(&plan)
            .expect("undirected relationship should validate in either endpoint order");
    }

    #[test]
    fn validate_graph_plan_rejects_unknown_properties_before_lowering() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.order_by = vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "missing".to_string(),
            }),
            direction: OrderDirection::Ascending,
        }];

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("unknown property should fail validation");

        assert!(error.to_string().contains("UNKNOWN_PROPERTY"), "{error:?}");
    }

    #[test]
    fn validate_graph_plan_rejects_unknown_aggregate_properties_before_lowering() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.projections.push(Projection::Aggregate {
            function: AggregateFunction::Count,
            target: AggregateTarget::Property(PropertyRef {
                variable: "service".to_string(),
                property: "missing".to_string(),
            }),
            distinct: false,
            alias: "missing_count".to_string(),
        });

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("unknown aggregate property should fail validation");

        assert!(error.to_string().contains("UNKNOWN_PROPERTY"), "{error:?}");
    }

    #[test]
    fn validate_graph_plan_rejects_keyless_relationship_aggregate_targets() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.projections = vec![Projection::Aggregate {
            function: AggregateFunction::Count,
            target: AggregateTarget::VariableKey {
                variable: "owns".to_string(),
            },
            distinct: true,
            alias: "ownership_count".to_string(),
        }];

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("relationship aggregate target should fail validation");

        assert!(
            error.to_string().contains("INVALID_AGGREGATE_TARGET"),
            "{error:?}"
        );
    }

    #[test]
    fn validate_graph_plan_accepts_keyed_relationship_aggregate_targets() {
        let keyed_graph = GRAPH.replace(
            "table: { schema: ops, name: ownerships }\n    from:",
            "table: { schema: ops, name: ownerships }\n    key: ownership_id\n    from:",
        );
        let graph = Declaration::from_yaml(&keyed_graph).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.projections = vec![Projection::Aggregate {
            function: AggregateFunction::Count,
            target: AggregateTarget::VariableKey {
                variable: "owns".to_string(),
            },
            distinct: true,
            alias: "ownership_count".to_string(),
        }];

        graph
            .validate_graph_plan(&plan)
            .expect("keyed relationship aggregate target should validate");
    }

    #[test]
    fn validate_graph_plan_rejects_keyless_relationship_key_projections() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.projections = vec![Projection::Key {
            variable: "owns".to_string(),
            alias: "ownership_id".to_string(),
        }];

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("keyless relationship id projection should fail validation");

        assert!(
            error.to_string().contains("INVALID_KEY_PROJECTION"),
            "{error:?}"
        );
    }

    #[test]
    fn validate_graph_plan_rejects_keyless_relationship_key_predicates() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.predicate = Some(PredicateExpression::KeyComparison(KeyPredicate {
            variable: "owns".to_string(),
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Literal(Literal::Integer(100)),
        }));

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("keyless relationship id predicate should fail validation");

        assert!(
            error.to_string().contains("INVALID_KEY_PROJECTION"),
            "{error:?}"
        );
    }

    #[test]
    fn validate_graph_plan_rejects_predicates_on_optional_bindings() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.optional_relationships = vec![0];
        plan.predicates = vec![PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
        }];

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("optional binding predicate should fail validation");

        assert!(
            error.to_string().contains("UNSUPPORTED_OPTIONAL_PREDICATE"),
            "{error:?}"
        );
    }

    #[test]
    fn validate_graph_plan_rejects_non_count_node_aggregate_targets() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.projections = vec![Projection::Aggregate {
            function: AggregateFunction::Sum,
            target: AggregateTarget::VariableKey {
                variable: "service".to_string(),
            },
            distinct: false,
            alias: "service_sum".to_string(),
        }];

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("non-count node aggregate target should fail validation");

        assert!(
            error.to_string().contains("INVALID_AGGREGATE_TARGET"),
            "{error:?}"
        );
    }

    #[test]
    fn validate_graph_plan_rejects_unknown_post_projection_aliases() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.post_projection_predicate = Some(ProjectionPredicateExpression::Comparison(
            ProjectionPredicate {
                alias: "missing".to_string(),
                operator: ComparisonOperator::Equal,
                rhs: ProjectionPredicateRhs::Literal(Literal::String("Ada".to_string())),
            },
        ));

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("unknown projected alias should fail validation");

        assert!(
            error.to_string().contains("UNKNOWN_PROJECTION_ALIAS"),
            "{error:?}"
        );
    }

    #[test]
    fn validate_graph_plan_rejects_unknown_rhs_properties_before_lowering() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.predicates = vec![PropertyPredicate {
            property: PropertyRef {
                variable: "person".to_string(),
                property: "name".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Property(PropertyRef {
                variable: "service".to_string(),
                property: "missing".to_string(),
            }),
        }];

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("unknown RHS property should fail validation");

        assert!(error.to_string().contains("UNKNOWN_PROPERTY"), "{error:?}");
    }

    #[test]
    fn validate_graph_plan_rejects_unknown_properties_inside_predicate_expression() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.predicate = Some(PredicateExpression::Or {
            left: Box::new(PredicateExpression::Comparison(PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
            })),
            right: Box::new(PredicateExpression::Comparison(PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "missing".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("dev".to_string())),
            })),
        });

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("unknown property inside predicate tree should fail validation");

        assert!(error.to_string().contains("UNKNOWN_PROPERTY"), "{error:?}");
    }

    #[test]
    fn validate_graph_plan_rejects_invalid_null_comparisons_inside_predicate_expression() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.predicate = Some(PredicateExpression::Not {
            expression: Box::new(PredicateExpression::Comparison(PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                operator: ComparisonOperator::GreaterThan,
                rhs: PredicateRhs::Literal(Literal::Null),
            })),
        });

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("ordered null comparison inside predicate tree should fail validation");

        assert!(
            error.to_string().contains("INVALID_NULL_COMPARISON"),
            "{error:?}"
        );
    }

    #[test]
    fn validate_graph_plan_rejects_null_values_in_in_lists() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.predicates = vec![PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            },
            operator: ComparisonOperator::In,
            rhs: PredicateRhs::List(vec![Literal::String("prod".to_string()), Literal::Null]),
        }];

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("null values in IN list should fail validation");

        assert!(
            error.to_string().contains("UNSUPPORTED_IN_LIST"),
            "{error:?}"
        );
    }

    #[test]
    fn validate_graph_plan_rejects_list_rhs_without_in_operator() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.predicates = vec![PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::List(vec![Literal::String("prod".to_string())]),
        }];

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("literal list without IN should fail validation");

        assert!(
            error.to_string().contains("INVALID_PREDICATE_OPERAND"),
            "{error:?}"
        );
    }

    #[test]
    fn validate_graph_plan_rejects_non_string_rhs_for_string_predicates() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.predicates = vec![PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            },
            operator: ComparisonOperator::StartsWith,
            rhs: PredicateRhs::Literal(Literal::Integer(10)),
        }];

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("non-string RHS for string predicate should fail validation");

        assert!(
            error.to_string().contains("INVALID_PREDICATE_OPERAND"),
            "{error:?}"
        );
    }

    #[test]
    fn validate_graph_plan_rejects_property_rhs_for_string_predicates() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.predicates = vec![PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            },
            operator: ComparisonOperator::Contains,
            rhs: PredicateRhs::Property(PropertyRef {
                variable: "person".to_string(),
                property: "name".to_string(),
            }),
        }];

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("property RHS for string predicate should fail validation");

        assert!(
            error.to_string().contains("INVALID_PREDICATE_OPERAND"),
            "{error:?}"
        );
    }

    #[test]
    fn validate_graph_plan_rejects_distinct_ordering_by_unprojected_properties() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.distinct = true;
        plan.order_by = vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            }),
            direction: OrderDirection::Ascending,
        }];

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("DISTINCT should not order by unprojected properties");

        assert!(
            error.to_string().contains("UNSUPPORTED_DISTINCT_ORDERING"),
            "{error:?}"
        );
    }

    #[test]
    fn validate_graph_plan_accepts_out_of_order_connected_relationships() {
        let graph = Declaration::from_yaml(
            r"
version: 1
name: dependencies
nodes:
  - label: Service
    table: { schema: ops, name: services }
    key: id
    properties:
      name: service_name
relationships:
  - type: DEPENDS_ON
    table: { schema: ops, name: service_dependencies }
    from: { label: Service, key: from_service_id }
    to: { label: Service, key: to_service_id }
",
        )
        .expect("graph should parse");
        let plan = GraphPlan {
            nodes: vec![
                NodePattern {
                    variable: "source".to_string(),
                    label: "Service".to_string(),
                },
                NodePattern {
                    variable: "middle".to_string(),
                    label: "Service".to_string(),
                },
                NodePattern {
                    variable: "target".to_string(),
                    label: "Service".to_string(),
                },
            ],
            relationships: vec![
                RelationshipPattern {
                    variable: None,
                    relationship_type: "DEPENDS_ON".to_string(),
                    left: "middle".to_string(),
                    direction: Direction::Outgoing,
                    right: "target".to_string(),
                },
                RelationshipPattern {
                    variable: None,
                    relationship_type: "DEPENDS_ON".to_string(),
                    left: "source".to_string(),
                    direction: Direction::Outgoing,
                    right: "middle".to_string(),
                },
            ],
            optional_relationships: Vec::new(),
            distinct: false,
            projections: vec![Projection::Property {
                property: PropertyRef {
                    variable: "target".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("target".to_string()),
            }],
            predicates: Vec::new(),
            predicate: None,
            post_projection_predicate: None,
            order_by: Vec::new(),
            skip: None,
            limit: None,
        };

        graph
            .validate_graph_plan(&plan)
            .expect("connected relationships should validate independent of order");
    }

    #[test]
    fn validate_graph_plan_rejects_disconnected_patterns_before_lowering() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.nodes.push(NodePattern {
            variable: "orphan".to_string(),
            label: "Service".to_string(),
        });

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("disconnected node should fail validation");

        assert!(
            error.to_string().contains("DISCONNECTED_PATTERN"),
            "{error:?}"
        );
    }

    fn ownership_plan() -> GraphPlan {
        GraphPlan {
            nodes: vec![
                NodePattern {
                    variable: "person".to_string(),
                    label: "Person".to_string(),
                },
                NodePattern {
                    variable: "service".to_string(),
                    label: "Service".to_string(),
                },
            ],
            relationships: vec![RelationshipPattern {
                variable: Some("owns".to_string()),
                relationship_type: "OWNS".to_string(),
                left: "person".to_string(),
                direction: Direction::Outgoing,
                right: "service".to_string(),
            }],
            optional_relationships: Vec::new(),
            distinct: false,
            projections: vec![Projection::Property {
                property: PropertyRef {
                    variable: "person".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("owner".to_string()),
            }],
            predicates: Vec::new(),
            predicate: None,
            post_projection_predicate: None,
            order_by: Vec::new(),
            skip: None,
            limit: None,
        }
    }
}
