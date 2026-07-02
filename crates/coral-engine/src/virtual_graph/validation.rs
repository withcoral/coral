use std::collections::{BTreeMap, BTreeSet};

use super::declaration::{Declaration, Node, Relationship, TableRef};
use super::diagnostic::Diagnostic;
use super::ir::{
    AggregateFunction, AggregateTarget, ComparisonOperator, CountSubqueryPattern, Direction,
    ElementIdPredicate, ExistsPatternPredicate, GraphPlan, GraphQuery, GraphUnionOuterProjection,
    GraphUnionOuterProjectionItem, KeyPredicate, Literal, LiteralListElementType, NodePattern,
    OptionalMatchScope, OrderExpression, PredicateExpression, PredicateRhs, PresencePredicate,
    Projection, ProjectionPredicate, ProjectionPredicateExpression, ProjectionPredicateRhs,
    PropertyKeyMembershipPredicate, PropertyPredicate, PropertyRef, RelationshipPattern,
    ScalarCaseAlternative, ScalarExpression, ScalarPredicate, ScalarPredicateRhs,
    UndirectedRelationshipEndpoint,
};
use crate::{CatalogInfo, CoreError};

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

mod aggregation;
mod projection;
mod scalar_types;
mod type_checks;
mod type_classifiers;
mod variable_collection;

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "Validation type classifiers are split into a child module while preserving parent call sites."
)]
use self::type_classifiers::*;

/// Graph plan validated against a specific declaration.
#[derive(Debug, Clone)]
pub(crate) struct ValidatedGraphPlan<'a> {
    graph: &'a Declaration,
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
        GraphPlanValidator::new(self, plan, None).validate()
    }

    pub(crate) fn validate_graph_plan_against_catalog(
        &self,
        plan: &GraphPlan,
        catalog: &CatalogInfo,
    ) -> Result<(), CoreError> {
        self.validate_against_catalog(catalog)?;
        GraphPlanValidator::new(self, plan, Some(catalog))
            .validate()
            .map(|_| ())
    }

    pub(crate) fn validate_graph_query(&self, query: &GraphQuery) -> Result<(), CoreError> {
        match query {
            GraphQuery::Plan(plan) => self.validate_graph_plan(plan).map(|_| ()),
            GraphQuery::Union(union) => {
                if union.branches.is_empty() {
                    return Err(CoreError::internal("graph union had no union branches"));
                }

                let expected_names = union.first.projection_output_names();
                let mut merged_types = GraphPlanValidator::new(self, &union.first, None)
                    .validate_and_infer_projection_scalar_types()?;
                for (index, branch) in union.branches.iter().enumerate() {
                    let branch_names = branch.plan.projection_output_names();
                    validate_union_projection_names(&expected_names, &branch_names, index)?;
                    let branch_types = GraphPlanValidator::new(self, &branch.plan, None)
                        .validate_and_infer_projection_scalar_types()?;
                    validate_union_projection_types(&mut merged_types, &branch_types, index)?;
                }
                if let Some(outer_projection) = &union.outer_projection {
                    validate_union_outer_projection(
                        outer_projection,
                        &expected_names,
                        &merged_types,
                    )?;
                }
                Ok(())
            }
        }
    }

    pub(crate) fn validate_graph_query_against_catalog(
        &self,
        query: &GraphQuery,
        catalog: &CatalogInfo,
    ) -> Result<(), CoreError> {
        self.validate_against_catalog(catalog)?;
        match query {
            GraphQuery::Plan(plan) => GraphPlanValidator::new(self, plan, Some(catalog))
                .validate()
                .map(|_| ()),
            GraphQuery::Union(union) => {
                if union.branches.is_empty() {
                    return Err(CoreError::internal("graph union had no union branches"));
                }

                let expected_names = union.first.projection_output_names();
                let mut merged_types = GraphPlanValidator::new(self, &union.first, Some(catalog))
                    .validate_and_infer_projection_scalar_types()?;
                for (index, branch) in union.branches.iter().enumerate() {
                    let branch_names = branch.plan.projection_output_names();
                    validate_union_projection_names(&expected_names, &branch_names, index)?;
                    let branch_types = GraphPlanValidator::new(self, &branch.plan, Some(catalog))
                        .validate_and_infer_projection_scalar_types()?;
                    validate_union_projection_types(&mut merged_types, &branch_types, index)?;
                }
                if let Some(outer_projection) = &union.outer_projection {
                    validate_union_outer_projection(
                        outer_projection,
                        &expected_names,
                        &merged_types,
                    )?;
                }
                Ok(())
            }
        }
    }
}

impl<'a> ValidatedGraphPlan<'a> {
    pub(crate) fn graph(&self) -> &'a Declaration {
        self.graph
    }

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
    catalog: Option<&'a CatalogInfo>,
    bindings: BTreeMap<&'a str, ValidatedBinding<'a>>,
    relationship_mappings: Vec<&'a Relationship>,
}

#[derive(Debug, Clone, Copy)]
struct ExistsPredicateValidationContext<'a, 'b> {
    relationships: &'b [ExistsRelationshipValidation<'a, 'b>],
    local_nodes: &'b BTreeMap<&'b str, &'a Node>,
}

#[derive(Debug, Clone, Copy)]
struct ExistsRelationshipValidation<'a, 'b> {
    pattern: &'b RelationshipPattern,
    relationship: &'a Relationship,
}

impl<'a> GraphPlanValidator<'a> {
    fn new(graph: &'a Declaration, plan: &'a GraphPlan, catalog: Option<&'a CatalogInfo>) -> Self {
        Self {
            graph,
            plan,
            catalog,
            bindings: BTreeMap::new(),
            relationship_mappings: Vec::with_capacity(plan.relationships.len()),
        }
    }

    fn validate(mut self) -> Result<ValidatedGraphPlan<'a>, CoreError> {
        self.validate_plan()?;

        Ok(ValidatedGraphPlan {
            graph: self.graph,
            plan: self.plan,
            bindings: self.bindings,
            relationship_mappings: self.relationship_mappings,
        })
    }

    fn validate_and_infer_projection_scalar_types(mut self) -> Result<Vec<ScalarType>, CoreError> {
        self.validate_plan()?;
        self.projection_scalar_types()
    }

    fn validate_plan(&mut self) -> Result<(), CoreError> {
        self.bind_nodes()?;
        self.bind_relationships()?;
        self.validate_optional_relationship_indices()?;
        self.validate_projection_shape()?;
        self.validate_aggregation()?;
        self.validate_property_references()?;
        self.validate_optional_predicates()?;
        self.validate_distinct_ordering()?;
        self.validate_connectivity()?;
        Ok(())
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
                Projection::ElementId { variable, .. } => {
                    self.validate_element_id_projection(
                        variable,
                        format!("projections[{index}].variable"),
                    )?;
                }
                Projection::NodeLabels {
                    variable, label, ..
                } => {
                    self.validate_node_labels_projection(
                        variable,
                        label,
                        format!("projections[{index}].variable"),
                    )?;
                }
                Projection::PropertyKeys { variable, .. } => {
                    self.validate_property_keys_projection(
                        variable,
                        format!("projections[{index}].variable"),
                    )?;
                }
                Projection::RelationshipType {
                    variable,
                    relationship_type,
                    ..
                } => {
                    self.validate_relationship_type_projection(
                        variable,
                        relationship_type,
                        format!("projections[{index}].variable"),
                    )?;
                }
                Projection::Literal { .. } | Projection::CountAll { .. } => {}
                Projection::LiteralList { literals, .. } => {
                    Self::validate_literal_list_projection(
                        literals,
                        format!("projections[{index}].literals"),
                    )?;
                }
                Projection::Expression { expression, .. } => {
                    self.validate_scalar_expression(
                        expression,
                        format!("projections[{index}].expression"),
                    )?;
                }
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
        for (index, optional_match) in self.plan.optional_matches.iter().enumerate() {
            if let Some(predicate) = &optional_match.predicate {
                self.validate_predicate_expression(
                    predicate,
                    format!("optional_matches[{index}].predicate"),
                )?;
            }
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
        if self.plan.post_projection_predicate.is_some() {
            return Err(Diagnostic::new(
                "UNSUPPORTED_OPTIONAL_PREDICATE",
                "post_projection_predicate",
                "post-projection predicates with optional matches require explicit projection-boundary planning",
            )
            .into_core_error());
        }
        self.validate_optional_match_scopes()?;
        Ok(())
    }

    fn validate_optional_match_scopes(&self) -> Result<(), CoreError> {
        for (index, optional_match) in self.plan.optional_matches.iter().enumerate() {
            self.validate_optional_match_scope(index, optional_match)?;
        }
        Ok(())
    }

    fn validate_optional_match_scope(
        &self,
        index: usize,
        optional_match: &OptionalMatchScope,
    ) -> Result<(), CoreError> {
        let allowed_variables = self.optional_match_scope_variables(index, optional_match)?;
        self.validate_optional_match_node_indices(index, optional_match)?;
        self.validate_optional_match_scope_shape(index, optional_match)?;
        let Some(predicate) = &optional_match.predicate else {
            return Ok(());
        };
        self.validate_optional_match_predicate_shape(index, optional_match)?;
        let mut referenced_variables = BTreeSet::new();
        Self::collect_predicate_expression_variables(predicate, &mut referenced_variables);
        if let Some(variable) = referenced_variables
            .iter()
            .find(|variable| !allowed_variables.contains(**variable))
        {
            return Err(Diagnostic::new(
                "UNSUPPORTED_OPTIONAL_PREDICATE",
                format!("optional_matches[{index}].predicate"),
                format!(
                    "optional predicate references '{variable}', which is outside the optional relationship scope"
                ),
            )
            .into_core_error());
        }
        Ok(())
    }

    fn optional_match_scope_variables(
        &self,
        index: usize,
        optional_match: &OptionalMatchScope,
    ) -> Result<BTreeSet<&str>, CoreError> {
        self.validate_optional_match_relationship_indices(index, optional_match)?;
        let mut allowed_variables = BTreeSet::new();
        for relationship_index in optional_match.relationship_indices.iter().copied() {
            let relationship = self
                .plan
                .relationships
                .get(relationship_index)
                .ok_or_else(|| CoreError::internal("validated optional index was invalid"))?;
            allowed_variables.insert(relationship.left.as_str());
            allowed_variables.insert(relationship.right.as_str());
            if let Some(variable) = relationship.variable.as_deref() {
                allowed_variables.insert(variable);
            }
        }
        Ok(allowed_variables)
    }

    fn validate_optional_match_node_indices(
        &self,
        index: usize,
        optional_match: &OptionalMatchScope,
    ) -> Result<(), CoreError> {
        if optional_match
            .node_indices
            .windows(2)
            .any(|pair| matches!(pair, [left, right] if left >= right))
        {
            return Err(Diagnostic::new(
                "INVALID_OPTIONAL_MATCH_SCOPE",
                format!("optional_matches[{index}].node_indices"),
                "optional match node indices must be unique and sorted in ascending order",
            )
            .into_core_error());
        }
        let scoped_node_variables = optional_match
            .relationship_indices
            .iter()
            .copied()
            .filter_map(|relationship_index| self.plan.relationships.get(relationship_index))
            .flat_map(|relationship| [relationship.left.as_str(), relationship.right.as_str()])
            .collect::<BTreeSet<_>>();
        for (position, node_index) in optional_match.node_indices.iter().copied().enumerate() {
            let Some(node) = self.plan.nodes.get(node_index) else {
                return Err(Diagnostic::new(
                    "INVALID_OPTIONAL_MATCH_SCOPE",
                    format!("optional_matches[{index}].node_indices[{position}]"),
                    format!(
                        "optional match scope references node index {node_index}, but only {} nodes exist",
                        self.plan.nodes.len()
                    ),
                )
                .into_core_error());
            };
            if !scoped_node_variables.contains(node.variable.as_str()) {
                return Err(Diagnostic::new(
                    "INVALID_OPTIONAL_MATCH_SCOPE",
                    format!("optional_matches[{index}].node_indices[{position}]"),
                    format!(
                        "optional match scope marks node '{}' as nullable, but that node is not part of the optional relationship scope",
                        node.variable
                    ),
                )
                .into_core_error());
            }
        }
        Ok(())
    }

    fn validate_optional_match_relationship_indices(
        &self,
        index: usize,
        optional_match: &OptionalMatchScope,
    ) -> Result<(), CoreError> {
        if optional_match.relationship_indices.is_empty() {
            return Err(Diagnostic::new(
                "INVALID_OPTIONAL_MATCH_SCOPE",
                format!("optional_matches[{index}].relationship_indices"),
                "optional match scopes must contain at least one relationship index",
            )
            .into_core_error());
        }
        if optional_match
            .relationship_indices
            .windows(2)
            .any(|pair| matches!(pair, [left, right] if left >= right))
        {
            return Err(Diagnostic::new(
                "INVALID_OPTIONAL_MATCH_SCOPE",
                format!("optional_matches[{index}].relationship_indices"),
                "optional match relationship indices must be unique and sorted in ascending order",
            )
            .into_core_error());
        }
        for (position, relationship_index) in optional_match
            .relationship_indices
            .iter()
            .copied()
            .enumerate()
        {
            self.validate_optional_match_relationship_index(index, position, relationship_index)?;
        }
        Ok(())
    }

    fn validate_optional_match_relationship_index(
        &self,
        index: usize,
        position: usize,
        relationship_index: usize,
    ) -> Result<(), CoreError> {
        if self.plan.relationships.get(relationship_index).is_none() {
            return Err(Diagnostic::new(
                "INVALID_OPTIONAL_MATCH_SCOPE",
                format!("optional_matches[{index}].relationship_indices[{position}]"),
                format!(
                    "optional match scope references relationship index {relationship_index}, but only {} relationships exist",
                    self.plan.relationships.len()
                ),
            )
            .into_core_error());
        }
        if self
            .plan
            .optional_relationships
            .binary_search(&relationship_index)
            .is_err()
        {
            return Err(Diagnostic::new(
                "INVALID_OPTIONAL_MATCH_SCOPE",
                format!("optional_matches[{index}].relationship_indices[{position}]"),
                "optional match scopes must reference optional relationships",
            )
            .into_core_error());
        }
        Ok(())
    }

    fn validate_optional_match_predicate_shape(
        &self,
        index: usize,
        optional_match: &OptionalMatchScope,
    ) -> Result<(), CoreError> {
        self.validate_optional_match_scope_shape(index, optional_match)?;
        let relationship_index = *optional_match
            .relationship_indices
            .first()
            .ok_or_else(|| CoreError::internal("validated optional match scope was empty"))?;
        self.plan
            .relationships
            .get(relationship_index)
            .ok_or_else(|| CoreError::internal("validated relationship index was invalid"))?;
        Ok(())
    }

    fn validate_optional_match_scope_shape(
        &self,
        index: usize,
        optional_match: &OptionalMatchScope,
    ) -> Result<(), CoreError> {
        if optional_match.relationship_indices.len() == 1 {
            return Ok(());
        }
        if optional_match.node_indices.is_empty() {
            return Err(Diagnostic::new(
                "UNSUPPORTED_OPTIONAL_MATCH_SCOPE",
                format!("optional_matches[{index}].node_indices"),
                "multi-hop optional match scopes must introduce at least one nullable node",
            )
            .into_core_error());
        }

        let mandatory_nodes = self.mandatory_reachable_nodes()?;
        let optional_nodes = optional_match
            .node_indices
            .iter()
            .copied()
            .filter_map(|node_index| self.plan.nodes.get(node_index))
            .map(|node| node.variable.as_str())
            .collect::<BTreeSet<_>>();
        let mut degree_by_node = BTreeMap::new();
        let mut boundary_relationships = 0;
        let mut reachable_nodes = BTreeSet::new();
        let mut remaining_relationships = optional_match
            .relationship_indices
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();

        for relationship_index in optional_match.relationship_indices.iter().copied() {
            let relationship = self
                .plan
                .relationships
                .get(relationship_index)
                .ok_or_else(|| CoreError::internal("validated optional index was invalid"))?;
            *degree_by_node
                .entry(relationship.left.as_str())
                .or_insert(0) += 1;
            *degree_by_node
                .entry(relationship.right.as_str())
                .or_insert(0) += 1;

            let left_is_mandatory = mandatory_nodes.contains(relationship.left.as_str())
                || !optional_nodes.contains(relationship.left.as_str());
            let right_is_mandatory = mandatory_nodes.contains(relationship.right.as_str())
                || !optional_nodes.contains(relationship.right.as_str());
            if left_is_mandatory || right_is_mandatory {
                boundary_relationships += 1;
                reachable_nodes.insert(relationship.left.as_str());
                reachable_nodes.insert(relationship.right.as_str());
            }
        }

        if !(1..=2).contains(&boundary_relationships) {
            return Err(Diagnostic::new(
                "UNSUPPORTED_OPTIONAL_MATCH_SCOPE",
                format!("optional_matches[{index}].relationship_indices"),
                "multi-hop optional match scopes require one connected chain with one or two previously-bound boundary relationships",
            )
            .into_core_error());
        }

        if degree_by_node.values().any(|degree| *degree > 2) {
            return Err(Diagnostic::new(
                "UNSUPPORTED_OPTIONAL_MATCH_SCOPE",
                format!("optional_matches[{index}].relationship_indices"),
                "multi-hop optional match scopes currently require one connected chain, not a branching pattern",
            )
            .into_core_error());
        }

        while !remaining_relationships.is_empty() {
            let mut progressed = false;
            for relationship_index in remaining_relationships.iter().copied().collect::<Vec<_>>() {
                let relationship = self
                    .plan
                    .relationships
                    .get(relationship_index)
                    .ok_or_else(|| CoreError::internal("validated optional index was invalid"))?;
                if reachable_nodes.contains(relationship.left.as_str())
                    || reachable_nodes.contains(relationship.right.as_str())
                {
                    reachable_nodes.insert(relationship.left.as_str());
                    reachable_nodes.insert(relationship.right.as_str());
                    remaining_relationships.remove(&relationship_index);
                    progressed = true;
                }
            }
            if !progressed {
                return Err(Diagnostic::new(
                    "UNSUPPORTED_OPTIONAL_MATCH_SCOPE",
                    format!("optional_matches[{index}].relationship_indices"),
                    "multi-hop optional match scopes must be one connected chain",
                )
                .into_core_error());
            }
        }

        Ok(())
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
            PredicateExpression::ElementIdComparison(predicate) => {
                self.validate_element_id_predicate(predicate, path)
            }
            PredicateExpression::Presence(predicate) => {
                self.validate_presence_predicate(predicate, path)
            }
            PredicateExpression::PropertyKeyMembership(predicate) => {
                self.validate_property_key_membership_predicate(predicate, path)
            }
            PredicateExpression::ExistsPattern(predicate) => {
                self.validate_exists_pattern_predicate(predicate, path)
            }
            PredicateExpression::ScalarComparison(predicate) => {
                self.validate_scalar_predicate(predicate, path)
            }
            PredicateExpression::And { left, right }
            | PredicateExpression::Or { left, right }
            | PredicateExpression::Xor { left, right } => {
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
            | ProjectionPredicateExpression::Or { left, right }
            | ProjectionPredicateExpression::Xor { left, right } => {
                self.validate_projection_predicate_expression(left, format!("{path}.left"))?;
                self.validate_projection_predicate_expression(right, format!("{path}.right"))
            }
            ProjectionPredicateExpression::Not { expression } => self
                .validate_projection_predicate_expression(expression, format!("{path}.expression")),
        }
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
                        "INVALID_PREDICATE_OPERAND",
                        path.clone(),
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                Self::validate_non_literal_string_predicate_operand(
                    path.clone(),
                    predicate.operator,
                )?;
                self.validate_property_ref(property, format!("{path}.rhs"))
            }
            PredicateRhs::Key { variable } => {
                if predicate.operator == ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        "INVALID_PREDICATE_OPERAND",
                        path.clone(),
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                Self::validate_non_literal_string_predicate_operand(
                    path.clone(),
                    predicate.operator,
                )?;
                self.validate_key_projection(variable, format!("{path}.rhs"))
            }
            PredicateRhs::ElementId { variable } => {
                if predicate.operator == ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        "INVALID_PREDICATE_OPERAND",
                        path.clone(),
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                Self::validate_non_literal_string_predicate_operand(
                    path.clone(),
                    predicate.operator,
                )?;
                self.validate_element_id_projection(variable, format!("{path}.rhs"))
            }
            PredicateRhs::List(_) => {
                if predicate.operator != ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        "INVALID_PREDICATE_OPERAND",
                        path.clone(),
                        "literal lists are only supported with IN predicates",
                    )
                    .into_core_error());
                }
                Ok(())
            }
        }?;
        let lhs_type = self.property_ref_scalar_type(&predicate.property)?;
        self.validate_predicate_rhs_operand_types(
            predicate.operator,
            lhs_type,
            &predicate.rhs,
            &path,
        )
    }

    fn validate_exists_pattern_predicate(
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

    fn validate_count_subquery_pattern(
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
                        "UNSUPPORTED_COUNT_SUBQUERY",
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

    fn validate_collect_subquery_pattern(
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
                        "UNSUPPORTED_COLLECT_SUBQUERY",
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

    fn validate_exists_pattern_nodes<'b>(
        &self,
        predicate: &'b ExistsPatternPredicate,
        path: &str,
    ) -> Result<BTreeMap<&'b str, &'a Node>, CoreError> {
        self.validate_scoped_node_patterns(&predicate.nodes, path, "EXISTS pattern")
    }

    fn validate_scoped_node_patterns<'b>(
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
                    "DUPLICATE_VARIABLE",
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
                    "DUPLICATE_VARIABLE",
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
                    "UNKNOWN_NODE_LABEL",
                    format!("{path}.nodes[{index}].label"),
                    format!("unknown node label '{}'", pattern.label),
                )
                .into_core_error()
            })?;
            local_nodes.insert(pattern.variable.as_str(), node);
        }
        Ok(local_nodes)
    }

    fn validate_exists_relationship_variables(
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
                    "DUPLICATE_VARIABLE",
                    format!("{path}.relationships[{index}].variable"),
                    format!("EXISTS pattern relationship variable '{variable}' shadows another graph variable"),
                )
                .into_core_error());
            }
            if !relationship_variables.insert(variable) {
                return Err(Diagnostic::new(
                    "DUPLICATE_VARIABLE",
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

    fn resolve_relationship_mapping_for_nodes(
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
                "UNKNOWN_RELATIONSHIP_TYPE",
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
                    "RELATIONSHIP_ENDPOINT_MISMATCH",
                    path.clone(),
                    format!(
                        "relationship type '{}' has no mapping for {} -> {}; available endpoint mappings: {}",
                        relationship.relationship_type, left_node.label, right_node.label, available
                    ),
                )
                .into_core_error())
            }
            _ => Err(Diagnostic::new(
                "AMBIGUOUS_RELATIONSHIP_MAPPING",
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

    fn validate_exists_pattern_not_empty(
        predicate: &ExistsPatternPredicate,
        path: &str,
    ) -> Result<(), CoreError> {
        if predicate.relationships.is_empty() && predicate.nodes.is_empty() {
            return Err(Diagnostic::new(
                "UNSUPPORTED_EXISTS_PATTERN",
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
            Some(ValidatedBindingKind::Node(node)) => Ok(*node),
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

    fn validate_exists_property_predicate<'b>(
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
                        "INVALID_PREDICATE_OPERAND",
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
                        "INVALID_PREDICATE_OPERAND",
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
                        "INVALID_PREDICATE_OPERAND",
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
                        "INVALID_PREDICATE_OPERAND",
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

    fn validate_scoped_predicate_expression<'b>(
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
                "INVALID_NULL_COMPARISON",
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
                        "UNSUPPORTED_COUNT_SUBQUERY",
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
                        "INVALID_PREDICATE_OPERAND",
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
                        "INVALID_PREDICATE_OPERAND",
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

    #[expect(
        clippy::too_many_lines,
        reason = "Scoped scalar validation mirrors the top-level scalar type dispatcher while resolving local aliases"
    )]
    fn infer_scoped_scalar_expression_type<'b>(
        &self,
        expression: &ScalarExpression,
        scope: ExistsPredicateValidationContext<'a, 'b>,
        path: impl Into<String>,
    ) -> Result<ScalarType, CoreError> {
        let path = path.into();
        match expression {
            ScalarExpression::Property(property) => {
                self.validate_exists_property_ref(
                    property,
                    scope.relationships,
                    scope.local_nodes,
                    path.clone(),
                )?;
                self.exists_property_ref_scalar_type(
                    property,
                    scope.relationships,
                    scope.local_nodes,
                )
            }
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
                        &path,
                    )?
                {
                    Ok(expression_type)
                } else {
                    self.infer_atomic_scalar_type(expression, &path)
                }
            }
            ScalarExpression::UndirectedEndpointKey { relationship, .. } => {
                if let Some((left_node, _)) =
                    self.scoped_same_label_undirected_endpoint_nodes(relationship, scope, &path)?
                {
                    Ok(self.column_scalar_type(&left_node.table, &left_node.key))
                } else {
                    self.infer_atomic_scalar_type(expression, &path)
                }
            }
            ScalarExpression::UndirectedEndpointElementId { relationship, .. } => {
                if self
                    .scoped_same_label_undirected_endpoint_nodes(relationship, scope, &path)?
                    .is_some()
                {
                    Ok(ScalarType::String)
                } else {
                    self.infer_atomic_scalar_type(expression, &path)
                }
            }
            ScalarExpression::UndirectedEndpointLabels {
                relationship,
                label,
                ..
            } => {
                if let Some((left_node, _)) =
                    self.scoped_same_label_undirected_endpoint_nodes(relationship, scope, &path)?
                {
                    if left_node.label != *label {
                        return Err(CoreError::internal(
                            "validated scoped same-label undirected endpoint labels did not match node label",
                        ));
                    }
                    Ok(ScalarType::Other)
                } else {
                    self.infer_atomic_scalar_type(expression, &path)
                }
            }
            ScalarExpression::UndirectedEndpointPropertyKeys { relationship, .. } => {
                if self
                    .scoped_same_label_undirected_endpoint_nodes(relationship, scope, &path)?
                    .is_some()
                {
                    Ok(ScalarType::Other)
                } else {
                    self.infer_atomic_scalar_type(expression, &path)
                }
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
                    self.validate_exists_key_ref(
                        variable,
                        scope.relationships,
                        scope.local_nodes,
                        path.clone(),
                    )?;
                }
                Ok(ScalarType::Other)
            }
            ScalarExpression::Predicate(predicate) => {
                self.validate_scoped_predicate_expression(predicate, scope, path)?;
                Ok(ScalarType::Boolean)
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
                "UNSUPPORTED_COLLECT_SUBQUERY",
                path,
                "nested COLLECT subqueries require scoped list-value planning and are not supported yet",
            )
            .into_core_error()),
            ScalarExpression::Key { variable } => {
                self.validate_exists_key_ref(
                    variable,
                    scope.relationships,
                    scope.local_nodes,
                    path.clone(),
                )?;
                self.scoped_key_scalar_type(variable, scope)
            }
            ScalarExpression::ElementId { variable }
            | ScalarExpression::GraphIdentity { variable } => {
                self.validate_exists_key_ref(
                    variable,
                    scope.relationships,
                    scope.local_nodes,
                    path,
                )?;
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
                self.validate_scoped_variable(presence_variable, scope, path.clone())?;
                self.infer_scoped_scalar_expression_type(
                    expression,
                    scope,
                    format!("{path}.expression"),
                )
            }
            ScalarExpression::Coalesce { expressions } => {
                if expressions.len() < 2 {
                    return Err(Diagnostic::new(
                        "INVALID_SCALAR_EXPRESSION",
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
            ScalarExpression::NullIf { expression, value } => {
                let expression_type = self.infer_scoped_scalar_expression_type(
                    expression,
                    scope,
                    format!("{path}.expression"),
                )?;
                let value_type = self.infer_scoped_scalar_expression_type(
                    value,
                    scope,
                    format!("{path}.value"),
                )?;
                Self::validate_compatible_scalar_types(
                    expression_type,
                    value_type,
                    &path,
                    "nullIf arguments",
                )?;
                Ok(expression_type)
            }
            ScalarExpression::Case {
                alternatives,
                else_expression,
            } => {
                if alternatives.is_empty() {
                    return Err(Diagnostic::new(
                        "INVALID_SCALAR_EXPRESSION",
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
            ScalarExpression::ToLower { expression }
            | ScalarExpression::ToUpper { expression }
            | ScalarExpression::Trim { expression }
            | ScalarExpression::LTrim { expression }
            | ScalarExpression::RTrim { expression }
            | ScalarExpression::Reverse { expression } => {
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
                Ok(numeric_result_type(expression_type))
            }
            ScalarExpression::Round { expression, places } => {
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
                    let places_type = self.infer_scoped_scalar_expression_type(
                        places,
                        scope,
                        format!("{path}.places"),
                    )?;
                    Self::require_integer_compatible_type(
                        places_type,
                        format!("{path}.places"),
                        "round precision",
                    )?;
                }
                Ok(numeric_result_type(expression_type))
            }
            ScalarExpression::Left { expression, count }
            | ScalarExpression::Right { expression, count } => {
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
                let count_type = self.infer_scoped_scalar_expression_type(
                    count,
                    scope,
                    format!("{path}.count"),
                )?;
                Self::require_integer_compatible_type(
                    count_type,
                    format!("{path}.count"),
                    "sized string count",
                )?;
                Ok(ScalarType::String)
            }
            ScalarExpression::StringIndices {
                expression,
                pattern,
            } => {
                for (name, expression) in [
                    ("expression", expression.as_ref()),
                    ("pattern", pattern.as_ref()),
                ] {
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
                for (name, expression) in [
                    ("expression", expression.as_ref()),
                    ("fill", fill.as_ref()),
                ] {
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
                let length_type = self.infer_scoped_scalar_expression_type(
                    length,
                    scope,
                    format!("{path}.length"),
                )?;
                Self::require_integer_compatible_type(
                    length_type,
                    format!("{path}.length"),
                    "padding length",
                )?;
                Ok(ScalarType::String)
            }
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
            } => {
                for (name, expression) in [
                    ("expression", expression.as_ref()),
                    ("pattern", operand.as_ref()),
                ] {
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
            ScalarExpression::Replace {
                expression,
                search,
                replacement,
            } => {
                for (name, expression) in [
                    ("expression", expression.as_ref()),
                    ("search", search.as_ref()),
                    ("replacement", replacement.as_ref()),
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
            ScalarExpression::Substring {
                expression,
                start,
                length,
            } => {
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
                let start_type = self.infer_scoped_scalar_expression_type(
                    start,
                    scope,
                    format!("{path}.start"),
                )?;
                Self::require_integer_compatible_type(
                    start_type,
                    format!("{path}.start"),
                    "substring start",
                )?;
                if let Some(length) = length {
                    let length_type = self.infer_scoped_scalar_expression_type(
                        length,
                        scope,
                        format!("{path}.length"),
                    )?;
                    Self::require_integer_compatible_type(
                        length_type,
                        format!("{path}.length"),
                        "substring length",
                    )?;
                }
                Ok(ScalarType::String)
            }
            ScalarExpression::Arithmetic { left, right, .. } => {
                let left_type =
                    self.infer_scoped_scalar_expression_type(left, scope, format!("{path}.left"))?;
                let right_type = self.infer_scoped_scalar_expression_type(
                    right,
                    scope,
                    format!("{path}.right"),
                )?;
                Self::require_numeric_compatible_type(
                    left_type,
                    format!("{path}.left"),
                    "arithmetic",
                )?;
                Self::require_numeric_compatible_type(
                    right_type,
                    format!("{path}.right"),
                    "arithmetic",
                )?;
                Ok(numeric_binary_result_type(left_type, right_type))
            }
            ScalarExpression::Atan2 { y, x } => {
                let y_type =
                    self.infer_scoped_scalar_expression_type(y, scope, format!("{path}.y"))?;
                let x_type =
                    self.infer_scoped_scalar_expression_type(x, scope, format!("{path}.x"))?;
                Self::require_numeric_compatible_type(y_type, format!("{path}.y"), "atan2")?;
                Self::require_numeric_compatible_type(x_type, format!("{path}.x"), "atan2")?;
                Ok(ScalarType::Float)
            }
        }
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
            "UNKNOWN_VARIABLE",
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
                "INVALID_LABELS_PROJECTION",
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
                "INVALID_LABELS_PROJECTION",
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
                "INVALID_TYPE_PROJECTION",
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
                "INVALID_TYPE_PROJECTION",
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

    fn validate_exists_property_ref<'b>(
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

    fn validate_exists_key_ref<'b>(
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
                "INVALID_KEY_PROJECTION",
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
                    "UNKNOWN_VARIABLE",
                    "property.variable",
                    format!("unknown graph variable '{}'", property.variable),
                )
                .into_core_error()
            })?;
        Ok(match binding.kind() {
            ValidatedBindingKind::Node(node) => node.column_for_property(&property.property),
            ValidatedBindingKind::Relationship(relationship) => {
                relationship.column_for_property(&property.property)
            }
        })
    }

    fn exists_property_ref_scalar_type<'b>(
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

    fn exists_relationship_for_variable<'b>(
        relationships: &[ExistsRelationshipValidation<'a, 'b>],
        variable: &str,
    ) -> Option<&'a Relationship> {
        relationships.iter().find_map(|candidate| {
            (candidate.pattern.variable.as_deref() == Some(variable))
                .then_some(candidate.relationship)
        })
    }

    fn validate_scalar_predicate(
        &self,
        predicate: &ScalarPredicate,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        let lhs_type = self.infer_scalar_expression_type(&predicate.lhs, format!("{path}.lhs"))?;
        match &predicate.rhs {
            ScalarPredicateRhs::Expression(expression) => {
                if predicate.operator == ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        "INVALID_PREDICATE_OPERAND",
                        path.clone(),
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                let rhs_type =
                    self.infer_scalar_expression_type(expression, format!("{path}.rhs"))?;
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
                        "INVALID_PREDICATE_OPERAND",
                        path.clone(),
                        "literal lists are only supported with IN predicates",
                    )
                    .into_core_error());
                }
                Self::validate_scalar_in_list_operand_types(lhs_type, literals, &path)
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
                        "INVALID_PREDICATE_OPERAND",
                        path.clone(),
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                if matches!(
                    predicate.operator,
                    ComparisonOperator::StartsWith
                        | ComparisonOperator::EndsWith
                        | ComparisonOperator::Contains
                        | ComparisonOperator::RegexMatch
                ) {
                    return Err(Diagnostic::new(
                        "INVALID_PREDICATE_OPERAND",
                        path.clone(),
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
                        path.clone(),
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                if matches!(
                    predicate.operator,
                    ComparisonOperator::StartsWith
                        | ComparisonOperator::EndsWith
                        | ComparisonOperator::Contains
                        | ComparisonOperator::RegexMatch
                ) {
                    return Err(Diagnostic::new(
                        "INVALID_PREDICATE_OPERAND",
                        path.clone(),
                        "string predicates require a string literal right-hand side",
                    )
                    .into_core_error());
                }
                self.validate_key_projection(variable, format!("{path}.rhs"))
            }
            PredicateRhs::ElementId { .. } => Err(Diagnostic::new(
                "INVALID_PREDICATE_OPERAND",
                path.clone(),
                "id() predicates cannot compare against elementId(); compare id() to mapped keys or elementId() to string values",
            )
            .into_core_error()),
            PredicateRhs::List(_) => {
                if predicate.operator != ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        "INVALID_PREDICATE_OPERAND",
                        path.clone(),
                        "literal lists are only supported with IN predicates",
                    )
                    .into_core_error());
                }
                Ok(())
            }
        }?;
        let lhs_type = self.key_scalar_type(&predicate.variable)?;
        self.validate_predicate_rhs_operand_types(
            predicate.operator,
            lhs_type,
            &predicate.rhs,
            &path,
        )
    }

    fn validate_element_id_predicate(
        &self,
        predicate: &ElementIdPredicate,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        self.validate_element_id_projection(&predicate.variable, format!("{path}.variable"))?;
        match &predicate.rhs {
            PredicateRhs::Literal(literal) => {
                if predicate.operator == ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        "INVALID_PREDICATE_OPERAND",
                        path.clone(),
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                Self::validate_element_id_literal(path.clone(), literal)?;
                Self::validate_string_predicate(path.clone(), predicate.operator, literal)?;
                Self::validate_literal_predicate(path.clone(), predicate.operator, literal)
            }
            PredicateRhs::ElementId { variable } => {
                if predicate.operator == ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        "INVALID_PREDICATE_OPERAND",
                        path.clone(),
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                if matches!(
                    predicate.operator,
                    ComparisonOperator::StartsWith
                        | ComparisonOperator::EndsWith
                        | ComparisonOperator::Contains
                        | ComparisonOperator::RegexMatch
                ) {
                    return Err(Diagnostic::new(
                        "INVALID_PREDICATE_OPERAND",
                        path.clone(),
                        "string predicates require a string literal right-hand side",
                    )
                    .into_core_error());
                }
                self.validate_element_id_projection(variable, format!("{path}.rhs"))
            }
            PredicateRhs::List(literals) => {
                if predicate.operator != ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        "INVALID_PREDICATE_OPERAND",
                        path.clone(),
                        "literal lists are only supported with IN predicates",
                    )
                    .into_core_error());
                }
                for (index, literal) in literals.iter().enumerate() {
                    Self::validate_element_id_literal(format!("{path}.rhs[{index}]"), literal)?;
                }
                Ok(())
            }
            PredicateRhs::Property(property) => {
                if predicate.operator == ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        "INVALID_PREDICATE_OPERAND",
                        path.clone(),
                        "IN predicates require a literal list right-hand side",
                    )
                    .into_core_error());
                }
                if matches!(
                    predicate.operator,
                    ComparisonOperator::StartsWith
                        | ComparisonOperator::EndsWith
                        | ComparisonOperator::Contains
                        | ComparisonOperator::RegexMatch
                ) {
                    return Err(Diagnostic::new(
                        "INVALID_PREDICATE_OPERAND",
                        path.clone(),
                        "string predicates require a string literal right-hand side",
                    )
                    .into_core_error());
                }
                self.validate_property_ref(property, format!("{path}.rhs"))
            }
            PredicateRhs::Key { .. } => Err(Diagnostic::new(
                "INVALID_PREDICATE_OPERAND",
                path.clone(),
                "elementId() predicates cannot compare against id(); compare elementId() to string values or id() to mapped keys",
            )
            .into_core_error()),
        }?;
        self.validate_predicate_rhs_operand_types(
            predicate.operator,
            ScalarType::String,
            &predicate.rhs,
            &path,
        )
    }

    fn validate_element_id_literal(
        path: impl Into<String>,
        literal: &Literal,
    ) -> Result<(), CoreError> {
        match literal {
            Literal::String(_) | Literal::Null => Ok(()),
            Literal::Integer(_) | Literal::Float(_) | Literal::Boolean(_) => Err(Diagnostic::new(
                "INVALID_PREDICATE_OPERAND",
                path,
                "elementId() predicates require string or null literal operands",
            )
            .into_core_error()),
        }
    }

    fn validate_presence_predicate(
        &self,
        predicate: &PresencePredicate,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        validate_variable(format!("{path}.variable"), &predicate.variable)?;
        if !self.bindings.contains_key(predicate.variable.as_str()) {
            return Err(Diagnostic::new(
                "UNKNOWN_VARIABLE",
                format!("{path}.variable"),
                format!("unknown graph variable '{}'", predicate.variable),
            )
            .into_core_error());
        }
        match predicate.operator {
            ComparisonOperator::Equal | ComparisonOperator::NotEqual => Ok(()),
            ComparisonOperator::GreaterThan
            | ComparisonOperator::GreaterThanOrEqual
            | ComparisonOperator::LessThan
            | ComparisonOperator::LessThanOrEqual
            | ComparisonOperator::In
            | ComparisonOperator::StartsWith
            | ComparisonOperator::EndsWith
            | ComparisonOperator::Contains
            | ComparisonOperator::RegexMatch => Err(Diagnostic::new(
                "INVALID_PRESENCE_PREDICATE",
                path,
                "graph variable presence predicates only support IS NULL and IS NOT NULL",
            )
            .into_core_error()),
        }
    }

    fn validate_property_key_membership_predicate(
        &self,
        predicate: &PropertyKeyMembershipPredicate,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        if !self.bindings.contains_key(predicate.variable.as_str()) {
            return Err(Diagnostic::new(
                "UNKNOWN_VARIABLE",
                format!("{path}.variable"),
                format!("unknown graph variable '{}'", predicate.variable),
            )
            .into_core_error());
        }
        if let Some(presence_variable) = &predicate.presence_variable
            && !self.bindings.contains_key(presence_variable.as_str())
        {
            return Err(Diagnostic::new(
                "UNKNOWN_VARIABLE",
                format!("{path}.presence_variable"),
                format!("unknown graph variable '{presence_variable}'"),
            )
            .into_core_error());
        }
        Ok(())
    }

    fn validate_projection_predicate(
        &self,
        predicate: &ProjectionPredicate,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        let lhs_type =
            self.projection_alias_scalar_type(&predicate.alias, format!("{path}.alias"))?;
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
                Self::validate_literal_predicate(path.clone(), predicate.operator, literal)?;
                Self::validate_scalar_predicate_operand_types(
                    predicate.operator,
                    lhs_type,
                    literal_scalar_type(literal),
                    &path,
                )
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
                        | ComparisonOperator::RegexMatch
                ) {
                    return Err(Diagnostic::new(
                        "INVALID_PREDICATE_OPERAND",
                        path,
                        "string predicates require a string literal right-hand side",
                    )
                    .into_core_error());
                }
                let rhs_type = self.projection_alias_scalar_type(alias, format!("{path}.rhs"))?;
                Self::validate_scalar_predicate_operand_types(
                    predicate.operator,
                    lhs_type,
                    rhs_type,
                    &path,
                )
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
                Self::validate_scalar_in_list_operand_types(lhs_type, literals, &path)
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
                | ComparisonOperator::RegexMatch
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

    fn validate_non_literal_string_predicate_operand(
        path: impl Into<String>,
        operator: ComparisonOperator,
    ) -> Result<(), CoreError> {
        if !matches!(
            operator,
            ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains
                | ComparisonOperator::RegexMatch
        ) {
            return Ok(());
        }
        Err(Diagnostic::new(
            "INVALID_PREDICATE_OPERAND",
            path,
            "string predicates require a string literal right-hand side",
        )
        .into_core_error())
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
                "UNKNOWN_PROPERTY",
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

    fn validate_connectivity(&self) -> Result<(), CoreError> {
        if self.plan.optional_relationships.is_empty() {
            return Ok(());
        }

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
                let pattern = self.plan.relationships.get(index).ok_or_else(|| {
                    CoreError::internal("validated relationship index was out of bounds")
                })?;
                joined_nodes.insert(pattern.left.as_str());
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
                let anchor = self.optional_relationship_component_anchor(index)?;
                joined_nodes.insert(anchor);
            }
        }
        Ok(joined_nodes)
    }

    fn optional_relationship_component_anchor(
        &self,
        relationship_index: usize,
    ) -> Result<&'a str, CoreError> {
        let pattern = self
            .plan
            .relationships
            .get(relationship_index)
            .ok_or_else(|| CoreError::internal("validated relationship index was out of bounds"))?;
        let left_position = self.node_position(pattern.left.as_str())?;
        let right_position = self.node_position(pattern.right.as_str())?;
        if left_position <= right_position {
            Ok(pattern.left.as_str())
        } else {
            Ok(pattern.right.as_str())
        }
    }

    fn node_position(&self, variable: &str) -> Result<usize, CoreError> {
        self.plan
            .nodes
            .iter()
            .position(|node| node.variable == variable)
            .ok_or_else(|| CoreError::internal("validated node variable was missing"))
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

fn validate_union_projection_names(
    expected: &[String],
    actual: &[String],
    branch_index: usize,
) -> Result<(), CoreError> {
    if expected == actual {
        return Ok(());
    }

    Err(Diagnostic::new(
        "UNION_SCHEMA_MISMATCH",
        format!("union.branches[{branch_index}].projections"),
        format!(
            "UNION branch projections must match the first branch; expected [{}], got [{}]",
            expected.join(", "),
            actual.join(", ")
        ),
    )
    .into_core_error())
}

fn validate_union_projection_types(
    merged_types: &mut [ScalarType],
    branch_types: &[ScalarType],
    branch_index: usize,
) -> Result<(), CoreError> {
    if merged_types.len() != branch_types.len() {
        return Err(Diagnostic::new(
            "UNION_SCHEMA_MISMATCH",
            format!("union.branches[{branch_index}].projections"),
            format!(
                "UNION branch projection count must match the first branch; expected {}, got {}",
                merged_types.len(),
                branch_types.len()
            ),
        )
        .into_core_error());
    }

    for (index, (merged_type, branch_type)) in
        merged_types.iter_mut().zip(branch_types.iter()).enumerate()
    {
        *merged_type = GraphPlanValidator::merge_scalar_types(
            *merged_type,
            *branch_type,
            format!("union.branches[{branch_index}].projections[{index}]"),
            "UNION branch projection types",
        )?;
    }
    Ok(())
}

fn validate_union_outer_projection(
    outer_projection: &GraphUnionOuterProjection,
    branch_projection_names: &[String],
    branch_projection_types: &[ScalarType],
) -> Result<(), CoreError> {
    if branch_projection_names.len() != branch_projection_types.len() {
        return Err(CoreError::internal(
            "union branch projection names and scalar types were not aligned",
        ));
    }
    for (index, item) in outer_projection.items.iter().enumerate() {
        match item {
            GraphUnionOuterProjectionItem::Column { name } => {
                validate_union_outer_projection_source(
                    branch_projection_names,
                    branch_projection_types,
                    name,
                    format!("outer_projection.items[{index}].name"),
                )?;
            }
            GraphUnionOuterProjectionItem::CountAll { .. } => {}
            GraphUnionOuterProjectionItem::Aggregate {
                function, source, ..
            } => {
                let source_type = validate_union_outer_projection_source(
                    branch_projection_names,
                    branch_projection_types,
                    source,
                    format!("outer_projection.items[{index}].source"),
                )?;
                aggregation::validate_aggregate_scalar_type(
                    *function,
                    source_type,
                    format!("outer_projection.items[{index}].source"),
                )?;
            }
        }
    }
    for (index, source) in outer_projection.group_by.iter().enumerate() {
        validate_union_outer_projection_source(
            branch_projection_names,
            branch_projection_types,
            source,
            format!("outer_projection.group_by[{index}]"),
        )?;
    }
    Ok(())
}

fn validate_union_outer_projection_source(
    branch_projection_names: &[String],
    branch_projection_types: &[ScalarType],
    source: &str,
    path: impl Into<String>,
) -> Result<ScalarType, CoreError> {
    let path = path.into();
    let position = branch_projection_names
        .iter()
        .position(|name| name == source)
        .ok_or_else(|| {
            Diagnostic::new(
                "UNKNOWN_PROJECTION",
                path.clone(),
                format!("outer union projection references unknown branch column '{source}'"),
            )
            .into_core_error()
        })?;
    branch_projection_types
        .get(position)
        .copied()
        .ok_or_else(|| CoreError::internal("union branch projection type index was out of bounds"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScalarType {
    Unknown,
    Null,
    String,
    Integer,
    Float,
    Boolean,
    Other,
}

impl ScalarType {
    fn is_numeric(self) -> bool {
        matches!(self, Self::Integer | Self::Float)
    }

    fn name(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Null => "null",
            Self::String => "string",
            Self::Integer => "integer",
            Self::Float => "float",
            Self::Boolean => "boolean",
            Self::Other => "non-scalar",
        }
    }
}

#[path = "validation_tests.rs"]
#[cfg(test)]
mod tests;
