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
            | ScalarExpression::Negate { .. }
    };
}

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

    fn projection_scalar_types(&self) -> Result<Vec<ScalarType>, CoreError> {
        self.plan
            .projections
            .iter()
            .enumerate()
            .map(|(index, projection)| {
                self.infer_projection_scalar_type(projection, format!("projections[{index}]"))
            })
            .collect()
    }

    fn infer_projection_scalar_type(
        &self,
        projection: &Projection,
        path: impl Into<String>,
    ) -> Result<ScalarType, CoreError> {
        let path = path.into();
        match projection {
            Projection::Property { property, .. } => {
                self.validate_property_ref(property, &path)?;
                self.property_ref_scalar_type(property)
            }
            Projection::Key { variable, .. } => {
                self.validate_key_projection(variable, &path)?;
                self.key_scalar_type(variable)
            }
            Projection::ElementId { variable, .. } => {
                self.validate_element_id_projection(variable, &path)?;
                Ok(ScalarType::String)
            }
            Projection::NodeLabels {
                variable, label, ..
            } => {
                self.validate_node_labels_projection(variable, label, &path)?;
                Ok(ScalarType::Other)
            }
            Projection::PropertyKeys { variable, .. } => {
                self.validate_property_keys_projection(variable, &path)?;
                Ok(ScalarType::Other)
            }
            Projection::RelationshipType {
                variable,
                relationship_type,
                ..
            } => {
                self.validate_relationship_type_projection(variable, relationship_type, &path)?;
                Ok(ScalarType::String)
            }
            Projection::Literal { literal, .. } => Ok(literal_scalar_type(literal)),
            Projection::LiteralList { literals, .. } => {
                Self::validate_literal_list_projection(literals, &path)?;
                Ok(ScalarType::Other)
            }
            Projection::Expression { expression, .. } => {
                self.infer_scalar_expression_type(expression, &path)
            }
            Projection::CountAll { .. } => Ok(ScalarType::Integer),
            Projection::Aggregate {
                function, target, ..
            } => self.infer_aggregate_projection_type(*function, target, &path),
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
        self.validate_distinct_keyless_relationship_counts()?;
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

    fn validate_distinct_keyless_relationship_counts(&self) -> Result<(), CoreError> {
        for (index, projection) in self.plan.projections.iter().enumerate() {
            let Projection::Aggregate {
                function: AggregateFunction::Count,
                target: AggregateTarget::VariableKey { variable },
                distinct: true,
                ..
            } = projection
            else {
                continue;
            };
            let Some(ValidatedBindingKind::Relationship(relationship)) = self
                .bindings
                .get(variable.as_str())
                .map(ValidatedBinding::kind)
            else {
                continue;
            };
            if relationship.key.is_none() {
                return Err(Diagnostic::new(
                    "INVALID_AGGREGATE_TARGET",
                    format!("projections[{index}].target"),
                    format!(
                        "count(DISTINCT {variable}) requires relationship mapping '{}' to declare a key",
                        relationship.relationship_type
                    ),
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
                    function,
                    target,
                    distinct,
                    ..
                } => {
                    Self::validate_aggregate_distinct_support(
                        *function,
                        *distinct,
                        format!("projections[{index}].distinct"),
                    )?;
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

        let mandatory_nodes = self.mandatory_reachable_nodes()?;
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

            let left_is_mandatory = mandatory_nodes.contains(relationship.left.as_str());
            let right_is_mandatory = mandatory_nodes.contains(relationship.right.as_str());
            if left_is_mandatory ^ right_is_mandatory {
                boundary_relationships += 1;
                reachable_nodes.insert(relationship.left.as_str());
                reachable_nodes.insert(relationship.right.as_str());
            } else if left_is_mandatory && right_is_mandatory {
                return Err(Diagnostic::new(
                    "UNSUPPORTED_OPTIONAL_MATCH_SCOPE",
                    format!("optional_matches[{index}].relationship_indices"),
                    "multi-hop optional match scopes cannot connect two already-bound nodes yet",
                )
                .into_core_error());
            }
        }

        if boundary_relationships != 1 {
            return Err(Diagnostic::new(
                "UNSUPPORTED_OPTIONAL_MATCH_SCOPE",
                format!("optional_matches[{index}].relationship_indices"),
                "multi-hop optional match scopes require exactly one relationship connected to a previously-bound anchor node",
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

    fn collect_predicate_expression_variables<'b>(
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

    fn collect_scalar_expression_variables<'b>(
        expression: &'b ScalarExpression,
        variables: &mut BTreeSet<&'b str>,
    ) {
        if let Some(expression) = Self::unary_scalar_expression_operand(expression) {
            Self::collect_scalar_expression_variables(expression, variables);
            return;
        }

        match expression {
            ScalarExpression::Property(property) => {
                variables.insert(property.variable.as_str());
            }
            ScalarExpression::Literal(_)
            | ScalarExpression::LiteralList { .. }
            | ScalarExpression::TypedLiteralList { .. } => {}
            ScalarExpression::Predicate(predicate) => {
                Self::collect_predicate_expression_variables(predicate, variables);
            }
            ScalarExpression::CountSubquery { pattern } => {
                Self::collect_count_subquery_outer_variables(pattern, variables);
            }
            ScalarExpression::Key { variable }
            | ScalarExpression::ElementId { variable }
            | ScalarExpression::GraphIdentity { variable }
            | ScalarExpression::GraphPresence { variable }
            | ScalarExpression::PropertyKeys { variable }
            | ScalarExpression::RelationshipType { variable, .. }
            | ScalarExpression::NodeLabels { variable, .. } => {
                variables.insert(variable.as_str());
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
            ScalarExpression::NullIf { expression, value } => {
                Self::collect_scalar_expression_variables(expression, variables);
                Self::collect_scalar_expression_variables(value, variables);
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
            ScalarExpression::Left { expression, count }
            | ScalarExpression::Right { expression, count } => {
                Self::collect_scalar_expression_variables(expression, variables);
                Self::collect_scalar_expression_variables(count, variables);
            }
            ScalarExpression::Replace {
                expression,
                search,
                replacement,
            } => {
                Self::collect_scalar_expression_variables(expression, variables);
                Self::collect_scalar_expression_variables(search, variables);
                Self::collect_scalar_expression_variables(replacement, variables);
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
            ScalarExpression::Arithmetic { left, right, .. } => {
                Self::collect_scalar_expression_variables(left, variables);
                Self::collect_scalar_expression_variables(right, variables);
            }
            ScalarExpression::Atan2 { y, x } => {
                Self::collect_scalar_expression_variables(y, variables);
                Self::collect_scalar_expression_variables(x, variables);
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
            | ScalarExpression::Negate { expression } => Some(expression),
            _ => None,
        }
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
                | Projection::ElementId { .. }
                | Projection::NodeLabels { .. }
                | Projection::PropertyKeys { .. }
                | Projection::RelationshipType { .. }
                | Projection::Literal { .. }
                | Projection::LiteralList { .. }
                | Projection::Expression { .. }
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
            OrderExpression::ElementId { variable } => {
                self.validate_element_id_projection(variable, format!("{path}.variable"))
            }
            OrderExpression::NodeLabels { variable, label } => {
                self.validate_node_labels_projection(variable, label, format!("{path}.variable"))
            }
            OrderExpression::PropertyKeys { variable } => {
                self.validate_property_keys_projection(variable, format!("{path}.variable"))
            }
            OrderExpression::RelationshipType {
                variable,
                relationship_type,
            } => self.validate_relationship_type_projection(
                variable,
                relationship_type,
                format!("{path}.variable"),
            ),
            OrderExpression::Scalar(expression) => {
                self.validate_scalar_expression(expression, format!("{path}.expression"))
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
            OrderExpression::ElementId { variable } => self.plan.projections.iter().any(|projection| {
                matches!(projection, Projection::ElementId { variable: projected, .. } if projected == variable)
            }),
            OrderExpression::RelationshipType {
                variable,
                relationship_type,
            } => self.plan.projections.iter().any(|projection| {
                matches!(
                    projection,
                    Projection::RelationshipType {
                        variable: projected,
                        relationship_type: projected_type,
                        ..
                    } if projected == variable && projected_type == relationship_type
                )
            }),
            OrderExpression::NodeLabels { variable, label } => {
                self.plan.projections.iter().any(|projection| {
                    matches!(
                        projection,
                        Projection::NodeLabels {
                            variable: projected,
                            label: projected_label,
                            ..
                        } if projected == variable && projected_label == label
                    )
                })
            }
            OrderExpression::PropertyKeys { variable } => {
                self.plan.projections.iter().any(|projection| {
                    matches!(
                        projection,
                        Projection::PropertyKeys {
                            variable: projected,
                            ..
                        } if projected == variable
                    )
                })
            }
            OrderExpression::Literal(literal) => {
                self.plan.projections.iter().any(|projection| {
                    matches!(projection, Projection::Literal { literal: projected, .. } if projected == literal)
                })
            }
            OrderExpression::Scalar(expression) => {
                self.plan.projections.iter().any(|projection| {
                    matches!(
                        projection,
                        Projection::Expression {
                            expression: projected,
                            ..
                        } if projected == expression
                    )
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
                | Projection::ElementId {
                    alias: projection_alias,
                    ..
                }
                | Projection::RelationshipType {
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
            ScalarExpression::Predicate(predicate) => {
                self.validate_scoped_predicate_expression(predicate, scope, path)?;
                Ok(ScalarType::Boolean)
            }
            ScalarExpression::CountSubquery { pattern } => {
                self.validate_nested_scoped_count_subquery_pattern(
                    pattern,
                    scope,
                    format!("{path}.pattern"),
                )?;
                Ok(ScalarType::Integer)
            }
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
                        | ComparisonOperator::RegexMatch
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
            ProjectionPredicateRhs::List(_) => {
                if predicate.operator != ComparisonOperator::In {
                    return Err(Diagnostic::new(
                        "INVALID_PREDICATE_OPERAND",
                        path,
                        "literal lists are only supported with IN predicates",
                    )
                    .into_core_error());
                }
                Ok(())
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

    fn validate_aggregate_distinct_support(
        function: AggregateFunction,
        distinct: bool,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        if distinct
            && matches!(
                function,
                AggregateFunction::StdDev | AggregateFunction::StdDevP
            )
        {
            return Err(Diagnostic::new(
                "UNSUPPORTED_AGGREGATION",
                path,
                format!(
                    "{}(DISTINCT property) is not supported because DataFusion does not execute distinct standard-deviation aggregates",
                    aggregate_function_name(function)
                ),
            )
            .into_core_error());
        }
        Ok(())
    }

    fn validate_aggregate_target(
        &self,
        function: AggregateFunction,
        target: &AggregateTarget,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        match target {
            AggregateTarget::Property(property) => {
                self.validate_property_ref(property, path.clone())?;
                self.validate_aggregate_property_type(function, property, path)
            }
            AggregateTarget::PresenceGatedProperty {
                property,
                presence_variable,
            } => {
                self.validate_property_ref(property, format!("{path}.property"))?;
                validate_variable(format!("{path}.presence_variable"), presence_variable)?;
                self.bindings
                    .get(presence_variable.as_str())
                    .ok_or_else(|| {
                        Diagnostic::new(
                            "UNKNOWN_VARIABLE",
                            format!("{path}.presence_variable"),
                            format!("unknown graph variable '{presence_variable}'"),
                        )
                        .into_core_error()
                    })?;
                self.validate_aggregate_property_type(function, property, path)
            }
            AggregateTarget::Expression(expression) => {
                self.validate_scalar_expression(expression, format!("{path}.expression"))?;
                self.validate_aggregate_expression_type(function, expression, path)
            }
            AggregateTarget::VariableKey { variable } => {
                self.validate_graph_variable_aggregate_target(function, variable, path)
            }
            AggregateTarget::PresenceGatedVariableKey {
                variable,
                presence_variable,
            } => {
                validate_variable(format!("{path}.presence_variable"), presence_variable)?;
                self.bindings
                    .get(presence_variable.as_str())
                    .ok_or_else(|| {
                        Diagnostic::new(
                            "UNKNOWN_VARIABLE",
                            format!("{path}.presence_variable"),
                            format!("unknown graph variable '{presence_variable}'"),
                        )
                        .into_core_error()
                    })?;
                self.validate_graph_variable_aggregate_target(
                    function,
                    variable,
                    format!("{path}.variable"),
                )
            }
        }
    }

    fn validate_graph_variable_aggregate_target(
        &self,
        function: AggregateFunction,
        variable: &str,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        if !aggregate_function_accepts_graph_variable_key(function) {
            return Err(Diagnostic::new(
                "INVALID_AGGREGATE_TARGET",
                path.clone(),
                format!(
                    "{}({variable}) requires a graph property argument; only count(variable) and collect(variable) can aggregate a graph variable key",
                    aggregate_function_name(function)
                ),
            )
            .into_core_error());
        }
        validate_variable(path.clone(), variable)?;
        let binding = self.bindings.get(variable).ok_or_else(|| {
            Diagnostic::new(
                "UNKNOWN_VARIABLE",
                path.clone(),
                format!("unknown graph variable '{variable}'"),
            )
            .into_core_error()
        })?;
        validate_collect_graph_variable_aggregate_binding(function, variable, binding.kind(), path)
    }

    fn validate_aggregate_property_type(
        &self,
        function: AggregateFunction,
        property: &PropertyRef,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        if self.catalog.is_none() || !aggregate_function_requires_numeric_target(function) {
            return Ok(());
        }
        let scalar_type = self.property_ref_scalar_type(property)?;
        validate_aggregate_scalar_type(function, scalar_type, path)
    }

    fn validate_aggregate_expression_type(
        &self,
        function: AggregateFunction,
        expression: &ScalarExpression,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        if !aggregate_function_requires_numeric_target(function) {
            return Ok(());
        }
        let scalar_type =
            self.infer_scalar_expression_type(expression, format!("{path}.expression"))?;
        validate_aggregate_scalar_type(function, scalar_type, path)
    }

    fn infer_aggregate_projection_type(
        &self,
        function: AggregateFunction,
        target: &AggregateTarget,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        self.validate_aggregate_target(function, target, path)?;
        match function {
            AggregateFunction::Count => Ok(ScalarType::Integer),
            AggregateFunction::Collect => Ok(ScalarType::Other),
            AggregateFunction::Sum
            | AggregateFunction::Avg
            | AggregateFunction::Median
            | AggregateFunction::StdDev
            | AggregateFunction::StdDevP => Ok(ScalarType::Float),
            AggregateFunction::Min | AggregateFunction::Max => match target {
                AggregateTarget::Property(property)
                | AggregateTarget::PresenceGatedProperty { property, .. } => {
                    self.property_ref_scalar_type(property)
                }
                AggregateTarget::Expression(expression) => {
                    self.infer_scalar_expression_type(expression, "expression")
                }
                AggregateTarget::VariableKey { .. }
                | AggregateTarget::PresenceGatedVariableKey { .. } => Ok(ScalarType::Unknown),
            },
        }
    }

    fn validate_node_labels_projection(
        &self,
        variable: &str,
        label: &str,
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
        let ValidatedBindingKind::Node(node) = binding.kind() else {
            return Err(Diagnostic::new(
                "INVALID_LABELS_PROJECTION",
                path,
                format!("labels({variable}) requires a node variable"),
            )
            .into_core_error());
        };
        if node.label != label {
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
        Ok(())
    }

    fn validate_property_keys_projection(
        &self,
        variable: &str,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        validate_variable(path.clone(), variable)?;
        self.bindings.get(variable).map(|_| ()).ok_or_else(|| {
            Diagnostic::new(
                "UNKNOWN_VARIABLE",
                path,
                format!("unknown graph variable '{variable}'"),
            )
            .into_core_error()
        })
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

    fn validate_element_id_projection(
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
                        "INVALID_ELEMENT_ID_PROJECTION",
                        path,
                        format!(
                            "elementId({variable}) requires relationship type '{}' to declare a key column",
                            relationship.relationship_type
                        ),
                    )
                    .into_core_error())
                }
            }
        }
    }

    fn validate_graph_identity_projection(
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
                        "INVALID_GRAPH_IDENTITY_PROJECTION",
                        path,
                        format!(
                            "graph identity for relationship variable '{variable}' requires relationship type '{}' to declare a key column",
                            relationship.relationship_type
                        ),
                    )
                    .into_core_error())
                }
            }
        }
    }

    fn validate_graph_presence_projection(
        &self,
        variable: &str,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        validate_variable(path.clone(), variable)?;
        self.bindings.get(variable).map(|_| ()).ok_or_else(|| {
            Diagnostic::new(
                "UNKNOWN_VARIABLE",
                path,
                format!("unknown graph variable '{variable}'"),
            )
            .into_core_error()
        })
    }

    fn validate_relationship_type_projection(
        &self,
        variable: &str,
        relationship_type: &str,
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
        let ValidatedBindingKind::Relationship(relationship) = binding.kind() else {
            return Err(Diagnostic::new(
                "INVALID_TYPE_PROJECTION",
                path,
                format!("type({variable}) requires a relationship variable"),
            )
            .into_core_error());
        };
        if relationship.relationship_type != relationship_type {
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
        Ok(())
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

    fn validate_literal_list_projection(
        literals: &[Literal],
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        if literals.is_empty() {
            return Err(Diagnostic::new(
                "INVALID_LITERAL_LIST_PROJECTION",
                path,
                "literal list projections require at least one element",
            )
            .into_core_error());
        }

        let mut expected = None;
        for literal in literals {
            let Some(kind) = literal_list_element_kind(literal) else {
                continue;
            };
            match expected {
                Some(expected) if expected != kind => {
                    return Err(Diagnostic::new(
                        "INVALID_LITERAL_LIST_PROJECTION",
                        path,
                        "literal list projections require all non-null elements to have the same type",
                    )
                    .into_core_error());
                }
                Some(_) => {}
                None => expected = Some(kind),
            }
        }

        if expected.is_none() {
            return Err(Diagnostic::new(
                "INVALID_LITERAL_LIST_PROJECTION",
                path,
                "literal list projections require at least one non-null element",
            )
            .into_core_error());
        }

        Ok(())
    }

    fn validate_typed_literal_list(
        literals: &[Literal],
        element_type: LiteralListElementType,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        for literal in literals {
            let Some(kind) = literal_list_element_kind(literal) else {
                continue;
            };
            if kind != element_type {
                return Err(Diagnostic::new(
                    "INVALID_TYPED_LITERAL_LIST",
                    path,
                    "typed literal lists require all non-null elements to match the declared element type",
                )
                .into_core_error());
            }
        }
        Ok(())
    }

    fn validate_scalar_expression(
        &self,
        expression: &ScalarExpression,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        self.infer_scalar_expression_type(expression, path)
            .map(|_| ())
    }

    fn infer_scalar_expression_type(
        &self,
        expression: &ScalarExpression,
        path: impl Into<String>,
    ) -> Result<ScalarType, CoreError> {
        let path = path.into();
        match expression {
            ScalarExpression::Property(_)
            | ScalarExpression::Literal(_)
            | ScalarExpression::LiteralList { .. }
            | ScalarExpression::TypedLiteralList { .. }
            | ScalarExpression::Predicate(_)
            | ScalarExpression::CountSubquery { .. }
            | ScalarExpression::Key { .. }
            | ScalarExpression::ElementId { .. }
            | ScalarExpression::GraphIdentity { .. }
            | ScalarExpression::GraphPresence { .. }
            | ScalarExpression::NodeLabels { .. }
            | ScalarExpression::PropertyKeys { .. }
            | ScalarExpression::PresenceGated { .. }
            | ScalarExpression::RelationshipType { .. } => {
                self.infer_atomic_scalar_type(expression, &path)
            }
            ScalarExpression::Coalesce { expressions } => {
                self.infer_coalesce_scalar_type(expressions, &path)
            }
            ScalarExpression::NullIf { expression, value } => {
                self.infer_null_if_scalar_type(expression, value, &path)
            }
            ScalarExpression::Case {
                alternatives,
                else_expression,
            } => self.infer_case_scalar_type(alternatives, else_expression.as_deref(), &path),
            _ => self.infer_scalar_function_type(expression, &path),
        }
    }

    fn infer_atomic_scalar_type(
        &self,
        expression: &ScalarExpression,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        match expression {
            ScalarExpression::Property(property) => {
                self.validate_property_ref(property, path)?;
                self.property_ref_scalar_type(property)
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
            ScalarExpression::Predicate(predicate) => {
                self.validate_predicate_expression(predicate, path)?;
                Ok(ScalarType::Boolean)
            }
            ScalarExpression::CountSubquery { pattern } => {
                self.validate_count_subquery_pattern(pattern, format!("{path}.pattern"))?;
                Ok(ScalarType::Integer)
            }
            ScalarExpression::Key { variable } => {
                self.validate_key_projection(variable, path)?;
                self.key_scalar_type(variable)
            }
            ScalarExpression::ElementId { variable } => {
                self.validate_element_id_projection(variable, path)?;
                Ok(ScalarType::String)
            }
            ScalarExpression::GraphIdentity { variable } => {
                self.validate_graph_identity_projection(variable, path)?;
                Ok(ScalarType::String)
            }
            ScalarExpression::GraphPresence { variable } => {
                self.validate_graph_presence_projection(variable, path)?;
                Ok(ScalarType::String)
            }
            ScalarExpression::NodeLabels { variable, label } => {
                self.validate_node_labels_projection(variable, label, path)?;
                Ok(ScalarType::Other)
            }
            ScalarExpression::PropertyKeys { variable } => {
                self.validate_property_keys_projection(variable, path)?;
                Ok(ScalarType::Other)
            }
            ScalarExpression::PresenceGated {
                presence_variable,
                expression,
            } => {
                self.validate_graph_presence_projection(presence_variable, path)?;
                self.infer_scalar_expression_type(expression, format!("{path}.expression"))
            }
            ScalarExpression::RelationshipType {
                variable,
                relationship_type,
            } => {
                self.validate_relationship_type_projection(variable, relationship_type, path)?;
                Ok(ScalarType::String)
            }
            _ => unreachable!("non-atomic scalar expression reached atomic type inference"),
        }
    }

    fn infer_null_if_scalar_type(
        &self,
        expression: &ScalarExpression,
        value: &ScalarExpression,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        let expression_type =
            self.infer_scalar_expression_type(expression, format!("{path}.expression"))?;
        let value_type = self.infer_scalar_expression_type(value, format!("{path}.value"))?;
        Self::validate_compatible_scalar_types(
            expression_type,
            value_type,
            path,
            "nullIf arguments",
        )?;
        Ok(expression_type)
    }

    fn infer_scalar_function_type(
        &self,
        expression: &ScalarExpression,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        match expression {
            ScalarExpression::ToString { expression }
            | ScalarExpression::ToStringOrNull { expression } => {
                self.infer_scalar_expression_type(expression, format!("{path}.expression"))?;
                Ok(ScalarType::String)
            }
            ScalarExpression::ToInteger { expression }
            | ScalarExpression::ToIntegerOrNull { expression } => {
                self.infer_scalar_expression_type(expression, format!("{path}.expression"))?;
                Ok(ScalarType::Integer)
            }
            ScalarExpression::ToFloat { expression }
            | ScalarExpression::ToFloatOrNull { expression } => {
                self.infer_scalar_expression_type(expression, format!("{path}.expression"))?;
                Ok(ScalarType::Float)
            }
            ScalarExpression::ToBoolean { expression }
            | ScalarExpression::ToBooleanOrNull { expression } => {
                self.infer_scalar_expression_type(expression, format!("{path}.expression"))?;
                Ok(ScalarType::Boolean)
            }
            ScalarExpression::ToLower { expression }
            | ScalarExpression::ToUpper { expression }
            | ScalarExpression::Trim { expression }
            | ScalarExpression::LTrim { expression }
            | ScalarExpression::RTrim { expression }
            | ScalarExpression::Reverse { expression } => {
                self.infer_string_unary_scalar_type(expression, path)
            }
            ScalarExpression::CharacterLength { expression } => {
                let expression_type =
                    self.infer_scalar_expression_type(expression, format!("{path}.expression"))?;
                Self::require_string_compatible_type(
                    expression_type,
                    format!("{path}.expression"),
                    "character length",
                )?;
                Ok(ScalarType::Integer)
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
                self.infer_numeric_unary_scalar_type(expression, path)
            }
            ScalarExpression::Round { expression, places } => {
                self.infer_round_scalar_type(expression, places.as_deref(), path)
            }
            ScalarExpression::Left { expression, count }
            | ScalarExpression::Right { expression, count } => {
                self.infer_sized_string_scalar_type(expression, count, path)
            }
            ScalarExpression::Replace {
                expression,
                search,
                replacement,
            } => self.infer_replace_scalar_type(expression, search, replacement, path),
            ScalarExpression::Substring {
                expression,
                start,
                length,
            } => self.infer_substring_scalar_type(expression, start, length.as_deref(), path),
            ScalarExpression::Arithmetic { left, right, .. } => {
                self.infer_arithmetic_scalar_type(left, right, path)
            }
            ScalarExpression::Atan2 { y, x } => self.infer_atan2_scalar_type(y, x, path),
            _ => unreachable!("non-function scalar expression reached function type inference"),
        }
    }

    fn infer_case_scalar_type(
        &self,
        alternatives: &[ScalarCaseAlternative],
        else_expression: Option<&ScalarExpression>,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
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
            self.validate_predicate_expression(
                &alternative.when,
                format!("{path}.alternatives[{index}].when"),
            )?;
            let then_type = self.infer_scalar_expression_type(
                &alternative.then,
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
            let else_type =
                self.infer_scalar_expression_type(else_expression, format!("{path}.else"))?;
            result_type = Self::merge_scalar_types(
                result_type,
                else_type,
                format!("{path}.else"),
                "CASE result branches",
            )?;
        }
        Ok(result_type)
    }

    fn infer_coalesce_scalar_type(
        &self,
        expressions: &[ScalarExpression],
        path: &str,
    ) -> Result<ScalarType, CoreError> {
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
            let expression_type =
                self.infer_scalar_expression_type(expression, format!("{path}[{index}]"))?;
            result_type = Self::merge_scalar_types(
                result_type,
                expression_type,
                format!("{path}[{index}]"),
                "coalesce arguments",
            )?;
        }
        Ok(result_type)
    }

    fn infer_string_unary_scalar_type(
        &self,
        expression: &ScalarExpression,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        let expression_type =
            self.infer_scalar_expression_type(expression, format!("{path}.expression"))?;
        Self::require_string_compatible_type(
            expression_type,
            format!("{path}.expression"),
            "string function",
        )?;
        Ok(ScalarType::String)
    }

    fn infer_numeric_unary_scalar_type(
        &self,
        expression: &ScalarExpression,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        let expression_type =
            self.infer_scalar_expression_type(expression, format!("{path}.expression"))?;
        Self::require_numeric_compatible_type(
            expression_type,
            format!("{path}.expression"),
            "numeric function",
        )?;
        Ok(numeric_result_type(expression_type))
    }

    fn infer_round_scalar_type(
        &self,
        expression: &ScalarExpression,
        places: Option<&ScalarExpression>,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        let expression_type =
            self.infer_scalar_expression_type(expression, format!("{path}.expression"))?;
        Self::require_numeric_compatible_type(
            expression_type,
            format!("{path}.expression"),
            "round",
        )?;
        if let Some(places) = places {
            let places_type =
                self.infer_scalar_expression_type(places, format!("{path}.places"))?;
            Self::require_integer_compatible_type(
                places_type,
                format!("{path}.places"),
                "round precision",
            )?;
        }
        Ok(numeric_result_type(expression_type))
    }

    fn infer_sized_string_scalar_type(
        &self,
        expression: &ScalarExpression,
        count: &ScalarExpression,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        let expression_type =
            self.infer_scalar_expression_type(expression, format!("{path}.expression"))?;
        Self::require_string_compatible_type(
            expression_type,
            format!("{path}.expression"),
            "sized string function",
        )?;
        let count_type = self.infer_scalar_expression_type(count, format!("{path}.count"))?;
        Self::require_integer_compatible_type(
            count_type,
            format!("{path}.count"),
            "sized string count",
        )?;
        Ok(ScalarType::String)
    }

    fn infer_replace_scalar_type(
        &self,
        expression: &ScalarExpression,
        search: &ScalarExpression,
        replacement: &ScalarExpression,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        for (name, expression) in [
            ("expression", expression),
            ("search", search),
            ("replacement", replacement),
        ] {
            let expression_type =
                self.infer_scalar_expression_type(expression, format!("{path}.{name}"))?;
            Self::require_string_compatible_type(
                expression_type,
                format!("{path}.{name}"),
                "replace",
            )?;
        }
        Ok(ScalarType::String)
    }

    fn infer_substring_scalar_type(
        &self,
        expression: &ScalarExpression,
        start: &ScalarExpression,
        length: Option<&ScalarExpression>,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        let expression_type =
            self.infer_scalar_expression_type(expression, format!("{path}.expression"))?;
        Self::require_string_compatible_type(
            expression_type,
            format!("{path}.expression"),
            "substring",
        )?;
        let start_type = self.infer_scalar_expression_type(start, format!("{path}.start"))?;
        Self::require_integer_compatible_type(
            start_type,
            format!("{path}.start"),
            "substring start",
        )?;
        if let Some(length) = length {
            let length_type =
                self.infer_scalar_expression_type(length, format!("{path}.length"))?;
            Self::require_integer_compatible_type(
                length_type,
                format!("{path}.length"),
                "substring length",
            )?;
        }
        Ok(ScalarType::String)
    }

    fn infer_atan2_scalar_type(
        &self,
        y: &ScalarExpression,
        x: &ScalarExpression,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        let y_type = self.infer_scalar_expression_type(y, format!("{path}.y"))?;
        let x_type = self.infer_scalar_expression_type(x, format!("{path}.x"))?;
        Self::require_numeric_compatible_type(y_type, format!("{path}.y"), "atan2")?;
        Self::require_numeric_compatible_type(x_type, format!("{path}.x"), "atan2")?;
        Ok(ScalarType::Float)
    }

    fn infer_arithmetic_scalar_type(
        &self,
        left: &ScalarExpression,
        right: &ScalarExpression,
        path: &str,
    ) -> Result<ScalarType, CoreError> {
        let left_type = self.infer_scalar_expression_type(left, format!("{path}.left"))?;
        let right_type = self.infer_scalar_expression_type(right, format!("{path}.right"))?;
        Self::require_numeric_compatible_type(left_type, format!("{path}.left"), "arithmetic")?;
        Self::require_numeric_compatible_type(right_type, format!("{path}.right"), "arithmetic")?;
        Ok(numeric_binary_result_type(left_type, right_type))
    }

    fn property_ref_scalar_type(&self, property: &PropertyRef) -> Result<ScalarType, CoreError> {
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
        let Some(column) = binding.column_for_property(&property.property) else {
            return Ok(ScalarType::Unknown);
        };
        let table = match binding.kind() {
            ValidatedBindingKind::Node(node) => &node.table,
            ValidatedBindingKind::Relationship(relationship) => &relationship.table,
        };
        Ok(self.column_scalar_type(table, column))
    }

    fn key_scalar_type(&self, variable: &str) -> Result<ScalarType, CoreError> {
        let binding = self.bindings.get(variable).ok_or_else(|| {
            Diagnostic::new(
                "UNKNOWN_VARIABLE",
                "variable",
                format!("unknown graph variable '{variable}'"),
            )
            .into_core_error()
        })?;
        let (table, column) = match binding.kind() {
            ValidatedBindingKind::Node(node) => (&node.table, node.key.as_str()),
            ValidatedBindingKind::Relationship(relationship) => {
                let Some(key) = relationship.key.as_deref() else {
                    return Ok(ScalarType::Unknown);
                };
                (&relationship.table, key)
            }
        };
        Ok(self.column_scalar_type(table, column))
    }

    fn column_scalar_type(&self, table: &TableRef, column: &str) -> ScalarType {
        self.catalog
            .and_then(|catalog| {
                catalog.tables.iter().find(|candidate| {
                    candidate.schema_name == table.schema && candidate.table_name == table.name
                })
            })
            .and_then(|table| {
                table
                    .columns
                    .iter()
                    .find(|candidate| candidate.name == column)
            })
            .map_or(ScalarType::Unknown, |column| {
                scalar_type_for_data_type(&column.data_type)
            })
    }

    fn validate_scalar_predicate_operand_types(
        operator: ComparisonOperator,
        lhs_type: ScalarType,
        rhs_type: ScalarType,
        path: &str,
    ) -> Result<(), CoreError> {
        if matches!(
            operator,
            ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains
                | ComparisonOperator::RegexMatch
        ) {
            Self::require_string_compatible_type(
                lhs_type,
                format!("{path}.lhs"),
                "string predicate",
            )?;
            Self::require_string_compatible_type(
                rhs_type,
                format!("{path}.rhs"),
                "string predicate",
            )?;
            return Ok(());
        }

        if matches!(
            operator,
            ComparisonOperator::GreaterThan
                | ComparisonOperator::GreaterThanOrEqual
                | ComparisonOperator::LessThan
                | ComparisonOperator::LessThanOrEqual
        ) {
            if matches!(rhs_type, ScalarType::Null) {
                return Err(Diagnostic::new(
                    "INVALID_NULL_COMPARISON",
                    path,
                    "null can only be compared with equality or inequality",
                )
                .into_core_error());
            }
            Self::validate_orderable_scalar_type(lhs_type, format!("{path}.lhs"))?;
            Self::validate_orderable_scalar_type(rhs_type, format!("{path}.rhs"))?;
        }

        Self::validate_compatible_scalar_types(
            lhs_type,
            rhs_type,
            path,
            "scalar predicate operands",
        )
    }

    fn validate_predicate_rhs_operand_types(
        &self,
        operator: ComparisonOperator,
        lhs_type: ScalarType,
        rhs: &PredicateRhs,
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
                let rhs_type = self.property_ref_scalar_type(property)?;
                Self::validate_scalar_predicate_operand_types(operator, lhs_type, rhs_type, path)
            }
            PredicateRhs::Key { variable } => {
                let rhs_type = self.key_scalar_type(variable)?;
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

    fn validate_scalar_in_list_operand_types(
        lhs_type: ScalarType,
        literals: &[Literal],
        path: &str,
    ) -> Result<(), CoreError> {
        let list_type = literal_list_scalar_type(literals)?;
        Self::validate_compatible_scalar_types(lhs_type, list_type, path, "IN predicate operands")
    }

    fn merge_scalar_types(
        left: ScalarType,
        right: ScalarType,
        path: impl Into<String>,
        context: &str,
    ) -> Result<ScalarType, CoreError> {
        if matches!(left, ScalarType::Null) {
            return Ok(right);
        }
        if matches!(right, ScalarType::Null) {
            return Ok(left);
        }
        if matches!(left, ScalarType::Unknown) || matches!(right, ScalarType::Unknown) {
            return Ok(if matches!(left, ScalarType::Unknown) {
                right
            } else {
                left
            });
        }
        if left == right {
            return Ok(left);
        }
        if left.is_numeric() && right.is_numeric() {
            return Ok(ScalarType::Float);
        }
        Err(Self::scalar_type_error(path, context, left, right))
    }

    fn validate_compatible_scalar_types(
        left: ScalarType,
        right: ScalarType,
        path: impl Into<String>,
        context: &str,
    ) -> Result<(), CoreError> {
        Self::merge_scalar_types(left, right, path, context).map(|_| ())
    }

    fn require_string_compatible_type(
        scalar_type: ScalarType,
        path: impl Into<String>,
        context: &str,
    ) -> Result<(), CoreError> {
        if matches!(
            scalar_type,
            ScalarType::Unknown | ScalarType::Null | ScalarType::String
        ) {
            return Ok(());
        }
        Err(Self::expected_type_error(
            path,
            context,
            "string",
            scalar_type,
        ))
    }

    fn require_integer_compatible_type(
        scalar_type: ScalarType,
        path: impl Into<String>,
        context: &str,
    ) -> Result<(), CoreError> {
        if matches!(
            scalar_type,
            ScalarType::Unknown | ScalarType::Null | ScalarType::Integer
        ) {
            return Ok(());
        }
        Err(Self::expected_type_error(
            path,
            context,
            "integer",
            scalar_type,
        ))
    }

    fn require_numeric_compatible_type(
        scalar_type: ScalarType,
        path: impl Into<String>,
        context: &str,
    ) -> Result<(), CoreError> {
        if scalar_type.is_numeric() || matches!(scalar_type, ScalarType::Unknown | ScalarType::Null)
        {
            return Ok(());
        }
        Err(Self::expected_type_error(
            path,
            context,
            "numeric",
            scalar_type,
        ))
    }

    fn validate_orderable_scalar_type(
        scalar_type: ScalarType,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        if matches!(scalar_type, ScalarType::Boolean) {
            return Err(Self::expected_type_error(
                path,
                "range predicate",
                "orderable",
                scalar_type,
            ));
        }
        Ok(())
    }

    fn scalar_type_error(
        path: impl Into<String>,
        context: &str,
        left: ScalarType,
        right: ScalarType,
    ) -> CoreError {
        Diagnostic::new(
            "INVALID_SCALAR_TYPE",
            path,
            format!(
                "{context} require compatible scalar types, got {} and {}",
                left.name(),
                right.name()
            ),
        )
        .into_core_error()
    }

    fn expected_type_error(
        path: impl Into<String>,
        context: &str,
        expected: &str,
        actual: ScalarType,
    ) -> CoreError {
        Diagnostic::new(
            "INVALID_SCALAR_TYPE",
            path,
            format!(
                "{context} requires a {expected} scalar expression, got {}",
                actual.name()
            ),
        )
        .into_core_error()
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
                function,
                source,
                distinct,
                ..
            } => {
                GraphPlanValidator::validate_aggregate_distinct_support(
                    *function,
                    *distinct,
                    format!("outer_projection.items[{index}].distinct"),
                )?;
                let source_type = validate_union_outer_projection_source(
                    branch_projection_names,
                    branch_projection_types,
                    source,
                    format!("outer_projection.items[{index}].source"),
                )?;
                validate_aggregate_scalar_type(
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

fn literal_scalar_type(literal: &Literal) -> ScalarType {
    match literal {
        Literal::String(_) => ScalarType::String,
        Literal::Integer(_) => ScalarType::Integer,
        Literal::Float(_) => ScalarType::Float,
        Literal::Boolean(_) => ScalarType::Boolean,
        Literal::Null => ScalarType::Null,
    }
}

fn literal_list_scalar_type(literals: &[Literal]) -> Result<ScalarType, CoreError> {
    let mut result_type = ScalarType::Null;
    for literal in literals {
        result_type = GraphPlanValidator::merge_scalar_types(
            result_type,
            literal_scalar_type(literal),
            "rhs",
            "literal list elements",
        )?;
    }
    Ok(result_type)
}

fn numeric_result_type(scalar_type: ScalarType) -> ScalarType {
    match scalar_type {
        ScalarType::Integer => ScalarType::Integer,
        ScalarType::Float => ScalarType::Float,
        ScalarType::Unknown | ScalarType::Null => ScalarType::Unknown,
        ScalarType::String | ScalarType::Boolean | ScalarType::Other => {
            unreachable!("numeric result requested for non-numeric type")
        }
    }
}

fn numeric_binary_result_type(left: ScalarType, right: ScalarType) -> ScalarType {
    match (left, right) {
        (ScalarType::Float, _) | (_, ScalarType::Float) => ScalarType::Float,
        (ScalarType::Integer, ScalarType::Integer) => ScalarType::Integer,
        _ => ScalarType::Unknown,
    }
}

fn scalar_type_for_data_type(data_type: &str) -> ScalarType {
    let data_type = data_type.trim();
    if data_type.is_empty() {
        return ScalarType::Unknown;
    }
    if data_type.contains("Utf8") {
        return ScalarType::String;
    }
    if data_type.starts_with("Int") || data_type.starts_with("UInt") {
        return ScalarType::Integer;
    }
    if data_type.starts_with("Float") || data_type.starts_with("Decimal") {
        return ScalarType::Float;
    }
    if data_type == "Boolean" {
        return ScalarType::Boolean;
    }
    if data_type.starts_with("Dictionary") {
        return scalar_type_for_dictionary_data_type(data_type);
    }
    if matches!(data_type, "Null" | "NullType") {
        return ScalarType::Null;
    }
    ScalarType::Other
}

fn scalar_type_for_dictionary_data_type(data_type: &str) -> ScalarType {
    if data_type.contains("Utf8") {
        ScalarType::String
    } else if data_type.contains("Float") || data_type.contains("Decimal") {
        ScalarType::Float
    } else if data_type.contains("Int") || data_type.contains("UInt") {
        ScalarType::Integer
    } else if data_type.contains("Boolean") {
        ScalarType::Boolean
    } else {
        ScalarType::Other
    }
}

fn literal_list_element_kind(literal: &Literal) -> Option<LiteralListElementType> {
    match literal {
        Literal::String(_) => Some(LiteralListElementType::String),
        Literal::Integer(_) => Some(LiteralListElementType::Integer),
        Literal::Float(_) => Some(LiteralListElementType::Float),
        Literal::Boolean(_) => Some(LiteralListElementType::Boolean),
        Literal::Null => None,
    }
}

fn aggregate_function_name(function: AggregateFunction) -> &'static str {
    match function {
        AggregateFunction::Count => "count",
        AggregateFunction::Collect => "collect",
        AggregateFunction::Sum => "sum",
        AggregateFunction::Avg => "avg",
        AggregateFunction::Median => "median",
        AggregateFunction::StdDev => "stDev",
        AggregateFunction::StdDevP => "stDevP",
        AggregateFunction::Min => "min",
        AggregateFunction::Max => "max",
    }
}

fn aggregate_function_requires_numeric_target(function: AggregateFunction) -> bool {
    matches!(
        function,
        AggregateFunction::Sum
            | AggregateFunction::Avg
            | AggregateFunction::Median
            | AggregateFunction::StdDev
            | AggregateFunction::StdDevP
    )
}

fn aggregate_function_accepts_graph_variable_key(function: AggregateFunction) -> bool {
    matches!(
        function,
        AggregateFunction::Count | AggregateFunction::Collect
    )
}

fn validate_collect_graph_variable_aggregate_binding(
    function: AggregateFunction,
    variable: &str,
    binding: &ValidatedBindingKind<'_>,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    let ValidatedBindingKind::Relationship(relationship) = binding else {
        return Ok(());
    };
    if function == AggregateFunction::Collect && relationship.key.is_none() {
        return Err(Diagnostic::new(
            "INVALID_AGGREGATE_TARGET",
            path,
            format!(
                "collect({variable}) requires relationship mapping '{}' to declare a key",
                relationship.relationship_type
            ),
        )
        .into_core_error());
    }
    Ok(())
}

fn validate_aggregate_scalar_type(
    function: AggregateFunction,
    scalar_type: ScalarType,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    if !aggregate_function_requires_numeric_target(function)
        || scalar_type.is_numeric()
        || matches!(scalar_type, ScalarType::Unknown | ScalarType::Null)
    {
        return Ok(());
    }
    Err(Diagnostic::new(
        "INVALID_AGGREGATE_TARGET",
        path,
        format!(
            "{}(property) requires a numeric property, got {}",
            aggregate_function_name(function),
            scalar_type.name()
        ),
    )
    .into_core_error())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::virtual_graph::ir::{
        AggregateFunction, AggregateTarget, Direction, KeyPredicate, NodePattern,
        OptionalMatchScope, OrderDirection, OrderExpression, OrderKey, PredicateExpression,
        PredicateRhs, Projection, PropertyPredicate, PropertyRef, RelationshipPattern,
        ScalarExpression, ScalarPredicate, ScalarPredicateRhs,
    };
    use crate::{ColumnInfo, TableInfo};

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
            optional_matches: Vec::new(),
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
            optional_matches: Vec::new(),
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
            nulls: None,
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
    fn validate_graph_plan_accepts_catalog_typed_numeric_aggregate_targets() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.projections = vec![Projection::Aggregate {
            function: AggregateFunction::Sum,
            target: AggregateTarget::Property(PropertyRef {
                variable: "service".to_string(),
                property: "id".to_string(),
            }),
            distinct: false,
            alias: "service_id_sum".to_string(),
        }];

        graph
            .validate_graph_plan_against_catalog(&plan, &typed_ownership_catalog())
            .expect("numeric aggregate target should validate against catalog types");
    }

    #[test]
    fn validate_graph_plan_rejects_catalog_typed_non_numeric_aggregate_targets() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.projections = vec![Projection::Aggregate {
            function: AggregateFunction::Sum,
            target: AggregateTarget::Property(PropertyRef {
                variable: "person".to_string(),
                property: "name".to_string(),
            }),
            distinct: false,
            alias: "bad_sum".to_string(),
        }];

        let error = graph
            .validate_graph_plan_against_catalog(&plan, &typed_ownership_catalog())
            .expect_err("string aggregate target should fail catalog-aware validation");

        assert!(
            error.to_string().contains("INVALID_AGGREGATE_TARGET"),
            "{error:?}"
        );
        assert!(error.to_string().contains("numeric"), "{error:?}");
    }

    #[test]
    fn validate_graph_plan_accepts_keyless_relationship_count_targets() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.projections = vec![Projection::Aggregate {
            function: AggregateFunction::Count,
            target: AggregateTarget::VariableKey {
                variable: "owns".to_string(),
            },
            distinct: false,
            alias: "ownership_count".to_string(),
        }];

        graph
            .validate_graph_plan(&plan)
            .expect("keyless relationship count target should validate");
    }

    #[test]
    fn validate_graph_plan_rejects_keyless_relationship_collect_targets() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.projections = vec![Projection::Aggregate {
            function: AggregateFunction::Collect,
            target: AggregateTarget::VariableKey {
                variable: "owns".to_string(),
            },
            distinct: false,
            alias: "ownerships".to_string(),
        }];

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("keyless relationship collect target should fail validation");

        assert!(
            error.to_string().contains("INVALID_AGGREGATE_TARGET"),
            "{error:?}"
        );
        assert!(error.to_string().contains("declare a key"), "{error:?}");
    }

    #[test]
    fn validate_graph_plan_rejects_keyless_relationship_element_id_projections() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.projections = vec![Projection::ElementId {
            variable: "owns".to_string(),
            alias: "ownership_element_id".to_string(),
        }];

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("keyless relationship element id should fail validation");

        assert!(
            error.to_string().contains("INVALID_ELEMENT_ID_PROJECTION"),
            "{error:?}"
        );
    }

    #[test]
    fn validate_graph_plan_rejects_non_string_element_id_predicate_literals() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.predicate = Some(PredicateExpression::ElementIdComparison(
            ElementIdPredicate {
                variable: "person".to_string(),
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::Integer(1)),
            },
        ));

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("non-string element id literal should fail validation");

        assert!(
            error.to_string().contains("INVALID_PREDICATE_OPERAND"),
            "{error:?}"
        );
    }

    #[test]
    fn validate_graph_plan_rejects_distinct_keyless_relationship_count_targets() {
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
            .expect_err("distinct keyless relationship aggregate target should fail validation");

        assert!(
            error.to_string().contains("INVALID_AGGREGATE_TARGET"),
            "{error:?}"
        );
    }

    #[test]
    fn validate_graph_plan_rejects_distinct_standard_deviation_aggregates() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.projections = vec![Projection::Aggregate {
            function: AggregateFunction::StdDevP,
            target: AggregateTarget::Property(PropertyRef {
                variable: "service".to_string(),
                property: "risk".to_string(),
            }),
            distinct: true,
            alias: "population_risk".to_string(),
        }];

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("distinct standard deviation aggregate should fail validation");

        assert!(
            error.to_string().contains("UNSUPPORTED_AGGREGATION"),
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
    fn validate_graph_plan_rejects_ambiguous_literal_list_projections() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");

        for literals in [
            Vec::new(),
            vec![Literal::Null],
            vec![Literal::Integer(1), Literal::String("prod".to_string())],
        ] {
            let mut plan = ownership_plan();
            plan.projections = vec![Projection::LiteralList {
                literals,
                alias: "values".to_string(),
            }];

            let error = graph
                .validate_graph_plan(&plan)
                .expect_err("ambiguous literal list projection should fail validation");

            assert!(
                error
                    .to_string()
                    .contains("INVALID_LITERAL_LIST_PROJECTION"),
                "{error:?}"
            );
        }
    }

    #[test]
    fn validate_graph_plan_accepts_empty_typed_literal_list_expressions() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.projections = vec![Projection::Expression {
            expression: ScalarExpression::TypedLiteralList {
                literals: Vec::new(),
                element_type: LiteralListElementType::String,
            },
            alias: "values".to_string(),
        }];

        graph
            .validate_graph_plan(&plan)
            .expect("typed empty list expression should validate");
    }

    #[test]
    fn validate_graph_plan_rejects_typed_literal_list_type_mismatches() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.projections = vec![Projection::Expression {
            expression: ScalarExpression::TypedLiteralList {
                literals: vec![Literal::Integer(1)],
                element_type: LiteralListElementType::String,
            },
            alias: "values".to_string(),
        }];

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("typed literal list mismatch should fail validation");

        assert!(
            error.to_string().contains("INVALID_TYPED_LITERAL_LIST"),
            "{error:?}"
        );
    }

    #[test]
    fn validate_graph_plan_accepts_scalar_expression_projections() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.projections = vec![Projection::Expression {
            expression: ScalarExpression::Coalesce {
                expressions: vec![
                    ScalarExpression::Property(PropertyRef {
                        variable: "person".to_string(),
                        property: "name".to_string(),
                    }),
                    ScalarExpression::Literal(Literal::String("unknown".to_string())),
                ],
            },
            alias: "owner_name".to_string(),
        }];

        graph
            .validate_graph_plan(&plan)
            .expect("scalar expression projection should validate");
    }

    #[test]
    fn validate_graph_plan_accepts_relationship_type_scalar_expression_projections() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.projections = vec![Projection::Expression {
            expression: ScalarExpression::Coalesce {
                expressions: vec![
                    ScalarExpression::RelationshipType {
                        variable: "owns".to_string(),
                        relationship_type: "OWNS".to_string(),
                    },
                    ScalarExpression::Literal(Literal::String("missing".to_string())),
                ],
            },
            alias: "relationship_type".to_string(),
        }];

        graph
            .validate_graph_plan(&plan)
            .expect("relationship type scalar expression projection should validate");
    }

    #[test]
    fn validate_graph_plan_accepts_identity_scalar_expression_projections() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.projections = vec![
            Projection::Expression {
                expression: ScalarExpression::Coalesce {
                    expressions: vec![
                        ScalarExpression::Key {
                            variable: "person".to_string(),
                        },
                        ScalarExpression::Literal(Literal::Integer(0)),
                    ],
                },
                alias: "person_id".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::ToString {
                    expression: Box::new(ScalarExpression::ElementId {
                        variable: "person".to_string(),
                    }),
                },
                alias: "person_element_id".to_string(),
            },
        ];

        graph
            .validate_graph_plan(&plan)
            .expect("identity scalar expression projections should validate");
    }

    #[test]
    fn validate_graph_plan_rejects_catalog_typed_coalesce_mismatches() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.projections = vec![Projection::Expression {
            expression: ScalarExpression::Coalesce {
                expressions: vec![
                    ScalarExpression::Key {
                        variable: "service".to_string(),
                    },
                    ScalarExpression::Literal(Literal::String("unknown".to_string())),
                ],
            },
            alias: "service_id".to_string(),
        }];

        let error = graph
            .validate_graph_plan_against_catalog(&plan, &typed_ownership_catalog())
            .expect_err("catalog-typed coalesce mismatch should fail validation");

        assert!(
            error.to_string().contains("INVALID_SCALAR_TYPE"),
            "{error:?}"
        );
        assert!(error.to_string().contains("coalesce"), "{error:?}");
    }

    #[test]
    fn validate_graph_plan_rejects_catalog_typed_case_branch_mismatches() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.projections = vec![Projection::Expression {
            expression: ScalarExpression::Case {
                alternatives: vec![ScalarCaseAlternative {
                    when: PredicateExpression::Boolean(true),
                    then: ScalarExpression::RelationshipType {
                        variable: "owns".to_string(),
                        relationship_type: "OWNS".to_string(),
                    },
                }],
                else_expression: Some(Box::new(ScalarExpression::Literal(Literal::Integer(1)))),
            },
            alias: "kind".to_string(),
        }];

        let error = graph
            .validate_graph_plan_against_catalog(&plan, &typed_ownership_catalog())
            .expect_err("catalog-typed CASE mismatch should fail validation");

        assert!(
            error.to_string().contains("INVALID_SCALAR_TYPE"),
            "{error:?}"
        );
        assert!(error.to_string().contains("CASE"), "{error:?}");
    }

    #[test]
    fn validate_graph_plan_rejects_catalog_typed_string_functions_over_numeric_values() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.projections = vec![Projection::Expression {
            expression: ScalarExpression::ToLower {
                expression: Box::new(ScalarExpression::Key {
                    variable: "service".to_string(),
                }),
            },
            alias: "lower_id".to_string(),
        }];

        let error = graph
            .validate_graph_plan_against_catalog(&plan, &typed_ownership_catalog())
            .expect_err("catalog-typed string function mismatch should fail validation");

        assert!(
            error.to_string().contains("INVALID_SCALAR_TYPE"),
            "{error:?}"
        );
        assert!(error.to_string().contains("string"), "{error:?}");
    }

    #[test]
    fn validate_graph_plan_rejects_catalog_typed_numeric_functions_over_string_values() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.projections = vec![Projection::Expression {
            expression: ScalarExpression::Abs {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "person".to_string(),
                    property: "name".to_string(),
                })),
            },
            alias: "abs_name".to_string(),
        }];

        let error = graph
            .validate_graph_plan_against_catalog(&plan, &typed_ownership_catalog())
            .expect_err("catalog-typed numeric function mismatch should fail validation");

        assert!(
            error.to_string().contains("INVALID_SCALAR_TYPE"),
            "{error:?}"
        );
        assert!(error.to_string().contains("numeric"), "{error:?}");
    }

    #[test]
    fn validate_graph_plan_rejects_catalog_typed_scalar_predicate_mismatches() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Key {
                variable: "service".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
                "10".to_string(),
            ))),
        }));

        let error = graph
            .validate_graph_plan_against_catalog(&plan, &typed_ownership_catalog())
            .expect_err("catalog-typed scalar predicate mismatch should fail validation");

        assert!(
            error.to_string().contains("INVALID_SCALAR_TYPE"),
            "{error:?}"
        );
        assert!(error.to_string().contains("predicate"), "{error:?}");
    }

    #[test]
    fn validate_graph_plan_rejects_catalog_typed_property_predicate_mismatches() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.predicate = Some(PredicateExpression::Comparison(PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Literal(Literal::Integer(10)),
        }));

        let error = graph
            .validate_graph_plan_against_catalog(&plan, &typed_ownership_catalog())
            .expect_err("catalog-typed property predicate mismatch should fail validation");

        assert!(
            error.to_string().contains("INVALID_SCALAR_TYPE"),
            "{error:?}"
        );
        assert!(error.to_string().contains("predicate"), "{error:?}");
    }

    #[test]
    fn validate_graph_plan_rejects_keyless_relationship_element_id_scalar_expressions() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.projections = vec![Projection::Expression {
            expression: ScalarExpression::ElementId {
                variable: "owns".to_string(),
            },
            alias: "ownership_element_id".to_string(),
        }];

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("keyless relationship elementId scalar should fail validation");

        assert!(
            error.to_string().contains("INVALID_ELEMENT_ID_PROJECTION"),
            "{error:?}"
        );
    }

    #[test]
    fn validate_graph_plan_rejects_invalid_relationship_type_scalar_expression_projections() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.projections = vec![Projection::Expression {
            expression: ScalarExpression::RelationshipType {
                variable: "person".to_string(),
                relationship_type: "OWNS".to_string(),
            },
            alias: "relationship_type".to_string(),
        }];

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("relationship type scalar over a node should fail validation");

        assert!(
            error.to_string().contains("INVALID_TYPE_PROJECTION"),
            "{error:?}"
        );
    }

    #[test]
    fn validate_graph_plan_rejects_unknown_properties_in_scalar_expression_projections() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.projections = vec![Projection::Expression {
            expression: ScalarExpression::Coalesce {
                expressions: vec![
                    ScalarExpression::Property(PropertyRef {
                        variable: "person".to_string(),
                        property: "missing".to_string(),
                    }),
                    ScalarExpression::Literal(Literal::String("unknown".to_string())),
                ],
            },
            alias: "owner_name".to_string(),
        }];

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("unknown scalar expression property should fail validation");

        assert!(error.to_string().contains("UNKNOWN_PROPERTY"), "{error:?}");
    }

    #[test]
    fn validate_graph_plan_rejects_unknown_properties_in_scalar_predicates() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Coalesce {
                expressions: vec![
                    ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "missing".to_string(),
                    }),
                    ScalarExpression::Literal(Literal::String("unknown".to_string())),
                ],
            },
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
                "prod".to_string(),
            ))),
        }));

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("unknown scalar predicate property should fail validation");

        assert!(error.to_string().contains("UNKNOWN_PROPERTY"), "{error:?}");
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
    fn validate_graph_plan_accepts_node_labels_projections() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.projections = vec![Projection::NodeLabels {
            variable: "person".to_string(),
            label: "Person".to_string(),
            alias: "labels".to_string(),
        }];

        graph
            .validate_graph_plan(&plan)
            .expect("node labels projection should validate");
    }

    #[test]
    fn validate_graph_plan_rejects_labels_projection_on_relationships() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.projections = vec![Projection::NodeLabels {
            variable: "owns".to_string(),
            label: "OWNS".to_string(),
            alias: "labels".to_string(),
        }];

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("relationship labels projection should fail validation");

        assert!(
            error.to_string().contains("INVALID_LABELS_PROJECTION"),
            "{error:?}"
        );
    }

    #[test]
    fn validate_graph_plan_accepts_property_keys_projections() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.projections = vec![
            Projection::PropertyKeys {
                variable: "person".to_string(),
                alias: "person_keys".to_string(),
            },
            Projection::PropertyKeys {
                variable: "owns".to_string(),
                alias: "ownership_keys".to_string(),
            },
        ];

        graph
            .validate_graph_plan(&plan)
            .expect("property keys projections should validate");
    }

    #[test]
    fn validate_graph_plan_accepts_property_key_membership_predicates() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.predicate = Some(PredicateExpression::PropertyKeyMembership(
            PropertyKeyMembershipPredicate {
                variable: "person".to_string(),
                key: "name".to_string(),
                presence_variable: None,
            },
        ));

        graph
            .validate_graph_plan(&plan)
            .expect("property key membership predicate should validate");
    }

    #[test]
    fn validate_graph_plan_rejects_property_key_membership_on_unknown_variables() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.predicate = Some(PredicateExpression::PropertyKeyMembership(
            PropertyKeyMembershipPredicate {
                variable: "unknown".to_string(),
                key: "name".to_string(),
                presence_variable: None,
            },
        ));

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("unknown property key membership variable should fail validation");

        assert!(error.to_string().contains("UNKNOWN_VARIABLE"), "{error:?}");
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
    fn validate_graph_plan_accepts_global_predicates_on_optional_bindings() {
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

        graph
            .validate_graph_plan(&plan)
            .expect("global optional binding predicate should validate");
    }

    #[test]
    fn validate_graph_plan_accepts_global_scalar_predicates_on_optional_bindings() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.optional_relationships = vec![0];
        plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Coalesce {
                expressions: vec![
                    ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    }),
                    ScalarExpression::Literal(Literal::String("unassigned".to_string())),
                ],
            },
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
                "prod".to_string(),
            ))),
        }));

        graph
            .validate_graph_plan(&plan)
            .expect("global optional scalar predicate should validate");
    }

    #[test]
    fn validate_graph_plan_accepts_optional_match_scoped_predicates() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.optional_relationships = vec![0];
        plan.optional_matches = vec![OptionalMatchScope {
            relationship_indices: vec![0],
            predicate: Some(PredicateExpression::Comparison(PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
            })),
        }];

        graph
            .validate_graph_plan(&plan)
            .expect("scoped optional predicate should validate");
    }

    #[test]
    fn validate_graph_plan_accepts_multihop_optional_match_scope() {
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
                    left: "source".to_string(),
                    direction: Direction::Outgoing,
                    right: "middle".to_string(),
                },
                RelationshipPattern {
                    variable: None,
                    relationship_type: "DEPENDS_ON".to_string(),
                    left: "middle".to_string(),
                    direction: Direction::Outgoing,
                    right: "target".to_string(),
                },
            ],
            optional_relationships: vec![0, 1],
            optional_matches: vec![OptionalMatchScope {
                relationship_indices: vec![0, 1],
                predicate: None,
            }],
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
            .expect("multi-hop optional scope should validate");
    }

    #[test]
    fn validate_graph_plan_rejects_multi_relationship_optional_match_scopes() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        let mut second_relationship = plan
            .relationships
            .first()
            .expect("ownership plan should contain a relationship")
            .clone();
        second_relationship.variable = Some("second_owns".to_string());
        plan.relationships.push(second_relationship);
        plan.optional_relationships = vec![0, 1];
        plan.optional_matches = vec![OptionalMatchScope {
            relationship_indices: vec![0, 1],
            predicate: None,
        }];

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("multi-relationship optional scope should fail validation");

        assert!(
            error
                .to_string()
                .contains("UNSUPPORTED_OPTIONAL_MATCH_SCOPE"),
            "{error:?}"
        );
    }

    #[test]
    fn validate_graph_plan_rejects_optional_match_predicates_outside_scope() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.nodes.push(NodePattern {
            variable: "other".to_string(),
            label: "Person".to_string(),
        });
        plan.optional_relationships = vec![0];
        plan.optional_matches = vec![OptionalMatchScope {
            relationship_indices: vec![0],
            predicate: Some(PredicateExpression::Comparison(PropertyPredicate {
                property: PropertyRef {
                    variable: "other".to_string(),
                    property: "name".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("platform".to_string())),
            })),
        }];

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("out-of-scope optional predicate should fail validation");

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
    fn validate_graph_plan_accepts_collect_node_aggregate_targets() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.projections = vec![Projection::Aggregate {
            function: AggregateFunction::Collect,
            target: AggregateTarget::VariableKey {
                variable: "service".to_string(),
            },
            distinct: false,
            alias: "services".to_string(),
        }];

        graph
            .validate_graph_plan(&plan)
            .expect("collect node aggregate target should validate");
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
    fn validate_graph_plan_accepts_null_values_in_in_lists() {
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

        graph
            .validate_graph_plan(&plan)
            .expect("null values in IN lists should validate");
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
    fn validate_graph_plan_accepts_presence_predicates_for_keyless_relationships() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.predicate = Some(PredicateExpression::Presence(PresencePredicate {
            variable: "owns".to_string(),
            operator: ComparisonOperator::Equal,
        }));

        graph
            .validate_graph_plan(&plan)
            .expect("presence predicates should validate for keyless relationships");
    }

    #[test]
    fn validate_graph_plan_rejects_invalid_presence_predicate_operator() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.predicate = Some(PredicateExpression::Presence(PresencePredicate {
            variable: "owns".to_string(),
            operator: ComparisonOperator::GreaterThan,
        }));

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("invalid presence predicate operator should fail validation");

        assert!(
            error.to_string().contains("INVALID_PRESENCE_PREDICATE"),
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
    fn validate_graph_plan_rejects_non_string_rhs_for_regex_predicates() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.predicates = vec![PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            },
            operator: ComparisonOperator::RegexMatch,
            rhs: PredicateRhs::Literal(Literal::Integer(10)),
        }];

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("non-string RHS for regex predicate should fail validation");

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
            nulls: None,
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
            optional_matches: Vec::new(),
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
    fn validate_graph_plan_accepts_disconnected_mandatory_patterns() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.nodes.push(NodePattern {
            variable: "orphan".to_string(),
            label: "Service".to_string(),
        });

        graph
            .validate_graph_plan(&plan)
            .expect("disconnected mandatory nodes should validate for CROSS JOIN lowering");
    }

    #[test]
    fn validate_graph_plan_accepts_optional_match_from_disconnected_component() {
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
                NodePattern {
                    variable: "owned".to_string(),
                    label: "Service".to_string(),
                },
            ],
            relationships: vec![RelationshipPattern {
                variable: Some("owns".to_string()),
                relationship_type: "OWNS".to_string(),
                left: "person".to_string(),
                direction: Direction::Outgoing,
                right: "owned".to_string(),
            }],
            optional_relationships: vec![0],
            optional_matches: Vec::new(),
            distinct: false,
            projections: vec![Projection::Property {
                property: PropertyRef {
                    variable: "owned".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("owned".to_string()),
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
            .expect("optional match should anchor to disconnected mandatory component");
    }

    fn typed_ownership_catalog() -> CatalogInfo {
        CatalogInfo {
            tables: vec![
                typed_table("ops", "people", &[("id", "Int64"), ("full_name", "Utf8")]),
                typed_table(
                    "ops",
                    "services",
                    &[("id", "Int64"), ("service_name", "Utf8"), ("tier", "Utf8")],
                ),
                typed_table(
                    "ops",
                    "ownerships",
                    &[
                        ("person_id", "Int64"),
                        ("service_id", "Int64"),
                        ("since", "Utf8"),
                    ],
                ),
            ],
            table_functions: Vec::new(),
        }
    }

    fn typed_table(schema: &str, name: &str, columns: &[(&str, &str)]) -> TableInfo {
        TableInfo {
            schema_name: schema.to_string(),
            table_name: name.to_string(),
            description: String::new(),
            guide: String::new(),
            columns: columns
                .iter()
                .enumerate()
                .map(|(position, (column, data_type))| ColumnInfo {
                    name: (*column).to_string(),
                    data_type: (*data_type).to_string(),
                    nullable: true,
                    is_virtual: false,
                    is_required_filter: false,
                    description: String::new(),
                    ordinal_position: u32::try_from(position).unwrap_or(u32::MAX),
                })
                .collect(),
            required_filters: Vec::new(),
        }
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
            optional_matches: Vec::new(),
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
