//! Graph-plan validation hub: the stateful `GraphPlanValidator` core that drives
//! the read-only `validation/*` analysis submodules. Owns plan-validation
//! orchestration (`validate`, `validate_and_infer_projection_scalar_types`,
//! `validate_plan`) and the `Declaration` query/union entry points, the mutable
//! bind phase that resolves node and relationship variables into
//! `ValidatedBinding`s (the only `&mut self` surface, over `bindings` and
//! `relationship_mappings`), the plan-wide property-reference walker, and
//! connectivity/reachability across mandatory and OPTIONAL MATCH components.

use std::collections::{BTreeMap, BTreeSet};

use super::declaration::{Declaration, Node, Relationship, TableRef};
use super::diagnostic::Diagnostic;
use super::diagnostic_codes;
use super::ir::{
    AggregateFunction, AggregateTarget, ArithmeticOperator, ComparisonOperator,
    CountSubqueryPattern, Direction, ElementIdPredicate, ExistsPatternPredicate, GraphPlan,
    GraphQuery, GraphStageExport, GraphStagedQuery, GraphStagedUnwindBinding,
    GraphStagedUnwindQuery, GraphUnionOuterProjection, GraphUnionOuterProjectionItem, GraphUnwind,
    GraphUnwindInputProjection, GraphUnwindPipeline, GraphUnwindProjection, KeyPredicate, Literal,
    LiteralListElementType, NodePattern, OptionalMatchScope, OrderExpression, PredicateExpression,
    PredicateRhs, PresencePredicate, Projection, ProjectionPredicate,
    ProjectionPredicateExpression, ProjectionPredicateRhs, PropertyKeyMembershipPredicate,
    PropertyPredicate, PropertyRef, RelationshipPattern, ScalarCaseAlternative, ScalarExpression,
    ScalarPredicate, ScalarPredicateRhs, TemporalComponentUnit, TemporalExpr, TemporalKind,
    UndirectedRelationshipEndpoint,
};
use crate::{CatalogInfo, CoreError};

mod aggregation;
mod exists_subqueries;
mod optional_match;
mod predicates;
mod projection;
mod scalar_types;
mod scoped;
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
    catalog: Option<&'a CatalogInfo>,
    bindings: BTreeMap<&'a str, ValidatedBinding<'a>>,
    stage_columns: StageColumnBindings,
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
    StageColumn {
        node: &'a Node,
        stage_alias: String,
        key_column: String,
    },
    Relationship(&'a Relationship),
}

#[derive(Debug, Clone, Default)]
pub(crate) struct StageColumnBindings {
    node_keys: BTreeMap<String, StageNodeColumnBinding>,
    relationship_keys: BTreeMap<String, StageRelationshipColumnBinding>,
    scalar_values: BTreeMap<String, StageScalarColumnBinding>,
}

#[derive(Debug, Clone)]
struct StageNodeColumnBinding {
    stage_alias: String,
    key_column: String,
}

#[derive(Debug, Clone)]
struct StageRelationshipColumnBinding {
    stage_alias: String,
    key_column: String,
}

#[derive(Debug, Clone)]
struct StageScalarColumnBinding {
    stage_alias: String,
    value_column: String,
    scalar_type: ScalarType,
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

    pub(crate) fn validate_graph_plan_with_stage_columns<'a>(
        &'a self,
        plan: &'a GraphPlan,
        stage_columns: StageColumnBindings,
    ) -> Result<ValidatedGraphPlan<'a>, CoreError> {
        GraphPlanValidator::new_with_stage_columns(self, plan, None, stage_columns).validate()
    }

    #[cfg(test)]
    pub(crate) fn validate_graph_plan_against_catalog(
        &self,
        plan: &GraphPlan,
        catalog: &CatalogInfo,
    ) -> Result<(), CoreError> {
        self.validated_graph_plan_against_catalog(plan, catalog)
            .map(|_| ())
    }

    pub(crate) fn validated_graph_plan_against_catalog<'a>(
        &'a self,
        plan: &'a GraphPlan,
        catalog: &'a CatalogInfo,
    ) -> Result<ValidatedGraphPlan<'a>, CoreError> {
        self.validate_against_catalog(catalog)?;
        GraphPlanValidator::new(self, plan, Some(catalog)).validate()
    }

    pub(crate) fn validate_graph_plan_with_stage_columns_against_catalog<'a>(
        &'a self,
        plan: &'a GraphPlan,
        stage_columns: StageColumnBindings,
        catalog: &'a CatalogInfo,
    ) -> Result<ValidatedGraphPlan<'a>, CoreError> {
        self.validate_against_catalog(catalog)?;
        GraphPlanValidator::new_with_stage_columns(self, plan, Some(catalog), stage_columns)
            .validate()
    }

    pub(crate) fn stage_column_bindings_against_catalog(
        &self,
        staged: &GraphStagedQuery,
        catalog: &CatalogInfo,
    ) -> Result<StageColumnBindings, CoreError> {
        self.validate_against_catalog(catalog)?;
        stage_column_bindings_with_catalog(self, staged, Some(catalog))
    }

    pub(crate) fn validate_graph_query(&self, query: &GraphQuery) -> Result<(), CoreError> {
        match query {
            GraphQuery::Plan(plan) => self.validate_graph_plan(plan).map(|_| ()),
            GraphQuery::Unwind(unwind) => Self::validate_graph_unwind(unwind),
            GraphQuery::UnwindPipeline(pipeline) => self.validate_graph_unwind_pipeline(pipeline),
            GraphQuery::Staged(staged) => self.validate_graph_staged_query(staged, None),
            GraphQuery::StagedUnwind(staged) => {
                self.validate_graph_staged_unwind_query(staged, None)
            }
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
            GraphQuery::Unwind(unwind) => Self::validate_graph_unwind(unwind),
            GraphQuery::UnwindPipeline(pipeline) => {
                self.validate_graph_unwind_pipeline_against_catalog(pipeline, catalog)
            }
            GraphQuery::Staged(staged) => self.validate_graph_staged_query(staged, Some(catalog)),
            GraphQuery::StagedUnwind(staged) => {
                self.validate_graph_staged_unwind_query(staged, Some(catalog))
            }
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

    pub(crate) fn validate_graph_unwind(unwind: &GraphUnwind) -> Result<(), CoreError> {
        validate_variable("unwind.variable", &unwind.variable)?;
        let input_aliases = validate_graph_unwind_input(unwind)?;
        validate_graph_unwind_list_expression(&unwind.list, "unwind.list", &input_aliases)?;
        validate_graph_unwind_element_type(
            &unwind.list,
            unwind.element_type,
            "unwind.list",
            &input_aliases,
        )?;

        if input_aliases.contains_key(unwind.variable.as_str()) {
            return Err(CoreError::internal(format!(
                "UNWIND variable '{}' conflicts with an input alias",
                unwind.variable
            )));
        }

        let [projection] = unwind.projections.as_slice() else {
            return Err(CoreError::internal(
                "UNWIND row source requires exactly one projection",
            ));
        };
        match projection {
            GraphUnwindProjection::Variable { alias } => {
                validate_variable("unwind.projections[0].alias", alias)
            }
        }
    }

    fn validate_graph_unwind_pipeline(
        &self,
        pipeline: &GraphUnwindPipeline,
    ) -> Result<(), CoreError> {
        Self::validate_graph_unwind(&pipeline.unwind)?;
        let stage_columns = unwind_stage_column_bindings(&pipeline.unwind, "stage0")?;
        GraphPlanValidator::new_with_stage_columns(self, &pipeline.final_plan, None, stage_columns)
            .validate()
            .map(|_| ())
    }

    fn validate_graph_unwind_pipeline_against_catalog(
        &self,
        pipeline: &GraphUnwindPipeline,
        catalog: &CatalogInfo,
    ) -> Result<(), CoreError> {
        Self::validate_graph_unwind(&pipeline.unwind)?;
        let stage_columns = unwind_stage_column_bindings(&pipeline.unwind, "stage0")?;
        GraphPlanValidator::new_with_stage_columns(
            self,
            &pipeline.final_plan,
            Some(catalog),
            stage_columns,
        )
        .validate()
        .map(|_| ())
    }

    fn validate_graph_staged_query(
        &self,
        staged: &super::ir::GraphStagedQuery,
        catalog: Option<&CatalogInfo>,
    ) -> Result<(), CoreError> {
        if staged.stages.is_empty() {
            return Err(CoreError::internal("staged graph query had no stages"));
        }

        let stage_columns = stage_column_bindings_with_catalog(self, staged, catalog)?;
        GraphPlanValidator::new_with_stage_columns(self, &staged.final_plan, catalog, stage_columns)
            .validate()
            .map(|_| ())
    }

    fn validate_graph_staged_unwind_query(
        &self,
        staged: &GraphStagedUnwindQuery,
        catalog: Option<&CatalogInfo>,
    ) -> Result<(), CoreError> {
        validate_staged_unwind_source_export(staged)?;
        GraphPlanValidator::new(self, &staged.stage.plan, catalog)
            .validate_and_infer_projection_scalar_types()?;
        let stage_columns = staged_unwind_stage_column_bindings(staged)?;
        GraphPlanValidator::new_with_stage_columns(self, &staged.final_plan, catalog, stage_columns)
            .validate()
            .map(|_| ())
    }
}

impl<'a> ValidatedGraphPlan<'a> {
    pub(crate) fn graph(&self) -> &'a Declaration {
        self.graph
    }

    pub(crate) fn with_alias_prefix(&self, prefix: &str) -> Self {
        let mut cloned = self.clone();
        for binding in cloned.bindings.values_mut() {
            binding.alias = format!("{prefix}{}", binding.alias);
        }
        cloned
    }

    pub(crate) fn plan(&self) -> &'a GraphPlan {
        self.plan
    }

    pub(super) fn property_ref_temporal_kind(
        &self,
        property: &PropertyRef,
    ) -> Result<Option<TemporalKind>, CoreError> {
        if self.catalog.is_none() {
            return Ok(None);
        }
        let binding = self.binding(&property.variable)?;
        let Some(column) = binding.column_for_property(&property.property) else {
            return Ok(None);
        };
        let table = match binding.kind() {
            ValidatedBindingKind::Node(node) | ValidatedBindingKind::StageColumn { node, .. } => {
                &node.table
            }
            ValidatedBindingKind::Relationship(relationship) => &relationship.table,
        };
        Ok(self.column_temporal_kind(table, column))
    }

    pub(super) fn column_temporal_kind(
        &self,
        table: &TableRef,
        column: &str,
    ) -> Option<TemporalKind> {
        let catalog = self.catalog?;
        let scalar_type = catalog
            .tables
            .iter()
            .find(|candidate| {
                candidate.schema_name == table.schema && candidate.table_name == table.name
            })
            .and_then(|table| {
                table
                    .columns
                    .iter()
                    .find(|candidate| candidate.name == column)
            })
            .map_or(ScalarType::Unknown, |column| {
                scalar_type_for_data_type(&column.data_type)
            });
        match scalar_type {
            ScalarType::Temporal(
                kind @ (TemporalKind::Date | TemporalKind::LocalDateTime | TemporalKind::LocalTime),
            ) => Some(kind),
            ScalarType::Unknown
            | ScalarType::Null
            | ScalarType::String
            | ScalarType::Integer
            | ScalarType::Float
            | ScalarType::Boolean
            | ScalarType::Temporal(TemporalKind::ZonedDateTime | TemporalKind::Duration)
            | ScalarType::Other => None,
        }
    }

    pub(crate) fn binding(&self, variable: &str) -> Result<&ValidatedBinding<'a>, CoreError> {
        self.bindings.get(variable).ok_or_else(|| {
            Diagnostic::new(
                diagnostic_codes::UNKNOWN_VARIABLE,
                "variable",
                format!("unknown graph variable '{variable}'"),
            )
            .into_core_error()
        })
    }

    pub(crate) fn node_binding(&self, variable: &str) -> Result<&Node, CoreError> {
        let binding = self.binding(variable)?;
        let ValidatedBindingKind::Node(node) = binding.kind() else {
            if let ValidatedBindingKind::StageColumn { node, .. } = binding.kind() {
                return Ok(node);
            }
            return Err(Diagnostic::new(
                diagnostic_codes::INVALID_ENDPOINT_VARIABLE,
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

    pub(crate) fn stage_scalar_column_ref(&self, alias: &str) -> Result<(&str, &str), CoreError> {
        self.stage_columns
            .scalar_values
            .get(alias)
            .map(|binding| (binding.stage_alias.as_str(), binding.value_column.as_str()))
            .ok_or_else(|| {
                Diagnostic::new(
                    diagnostic_codes::UNKNOWN_VARIABLE,
                    "stage_column",
                    format!("unknown staged scalar value '{alias}'"),
                )
                .into_core_error()
            })
    }

    pub(crate) fn stage_relationship_column_ref(&self, variable: &str) -> Option<(&str, &str)> {
        self.stage_columns
            .relationship_keys
            .get(variable)
            .map(|binding| (binding.stage_alias.as_str(), binding.key_column.as_str()))
    }

    pub(crate) fn has_stage_node_keys(&self) -> bool {
        !self.stage_columns.node_keys.is_empty()
    }

    pub(crate) fn scalar_stage_aliases(&self) -> BTreeSet<&str> {
        self.stage_columns
            .scalar_values
            .values()
            .map(|binding| binding.stage_alias.as_str())
            .collect()
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
            ValidatedBindingKind::Node(node) | ValidatedBindingKind::StageColumn { node, .. } => {
                node.column_for_property(property)
            }
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
    stage_columns: StageColumnBindings,
    bindings: BTreeMap<&'a str, ValidatedBinding<'a>>,
    relationship_mappings: Vec<&'a Relationship>,
}

impl<'a> GraphPlanValidator<'a> {
    fn new(graph: &'a Declaration, plan: &'a GraphPlan, catalog: Option<&'a CatalogInfo>) -> Self {
        Self {
            graph,
            plan,
            catalog,
            stage_columns: StageColumnBindings::default(),
            bindings: BTreeMap::new(),
            relationship_mappings: Vec::with_capacity(plan.relationships.len()),
        }
    }

    fn new_with_stage_columns(
        graph: &'a Declaration,
        plan: &'a GraphPlan,
        catalog: Option<&'a CatalogInfo>,
        stage_columns: StageColumnBindings,
    ) -> Self {
        Self {
            graph,
            plan,
            catalog,
            stage_columns,
            bindings: BTreeMap::new(),
            relationship_mappings: Vec::with_capacity(plan.relationships.len()),
        }
    }

    fn validate(mut self) -> Result<ValidatedGraphPlan<'a>, CoreError> {
        self.validate_plan()?;

        Ok(ValidatedGraphPlan {
            graph: self.graph,
            plan: self.plan,
            catalog: self.catalog,
            bindings: self.bindings,
            stage_columns: self.stage_columns,
            relationship_mappings: self.relationship_mappings,
        })
    }

    fn validate_and_infer_projection_scalar_types(mut self) -> Result<Vec<ScalarType>, CoreError> {
        self.validate_plan()?;
        self.projection_scalar_types()
    }

    fn stage_scalar_column_type(
        &self,
        alias: &str,
        path: impl Into<String>,
    ) -> Result<ScalarType, CoreError> {
        let path = path.into();
        self.stage_columns
            .scalar_values
            .get(alias)
            .map(|binding| binding.scalar_type)
            .ok_or_else(|| {
                Diagnostic::new(
                    diagnostic_codes::UNKNOWN_VARIABLE,
                    path,
                    format!("unknown staged scalar value '{alias}'"),
                )
                .into_core_error()
            })
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
            if self.plan.relationships.is_empty() && !self.stage_columns.scalar_values.is_empty() {
                return Ok(());
            }
            return Err(Diagnostic::new(
                diagnostic_codes::EMPTY_PLAN,
                "nodes",
                "at least one node pattern is required",
            )
            .into_core_error());
        }

        for (index, pattern) in self.plan.nodes.iter().enumerate() {
            validate_variable(format!("nodes[{index}].variable"), &pattern.variable)?;
            if self.bindings.contains_key(pattern.variable.as_str()) {
                return Err(Diagnostic::new(
                    diagnostic_codes::DUPLICATE_VARIABLE,
                    format!("nodes[{index}].variable"),
                    format!("variable '{}' is bound more than once", pattern.variable),
                )
                .into_core_error());
            }
            let node = self.graph.node(&pattern.label).ok_or_else(|| {
                Diagnostic::new(
                    diagnostic_codes::UNKNOWN_NODE_LABEL,
                    format!("nodes[{index}].label"),
                    format!("unknown node label '{}'", pattern.label),
                )
                .into_core_error()
            })?;
            self.bindings.insert(
                pattern.variable.as_str(),
                ValidatedBinding {
                    alias: format!("n{index}"),
                    kind: if let Some(stage_column) =
                        self.stage_columns.node_keys.get(pattern.variable.as_str())
                    {
                        ValidatedBindingKind::StageColumn {
                            node,
                            stage_alias: stage_column.stage_alias.clone(),
                            key_column: stage_column.key_column.clone(),
                        }
                    } else {
                        ValidatedBindingKind::Node(node)
                    },
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
                        diagnostic_codes::DUPLICATE_VARIABLE,
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
                diagnostic_codes::UNKNOWN_RELATIONSHIP_TYPE,
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
                    diagnostic_codes::RELATIONSHIP_ENDPOINT_MISMATCH,
                    format!("relationships[{index}]"),
                    format!(
                        "relationship type '{}' has no mapping for {} -> {}; available endpoint mappings: {}",
                        pattern.relationship_type, left_node.label, right_node.label, available
                    ),
                )
                .into_core_error())
            }
            _ => Err(Diagnostic::new(
                diagnostic_codes::AMBIGUOUS_RELATIONSHIP_MAPPING,
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
                    diagnostic_codes::MANDATORY_RELATIONSHIP_DEPENDS_ON_OPTIONAL_BINDING,
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
                    diagnostic_codes::DISCONNECTED_PATTERN,
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
                diagnostic_codes::EMPTY_PLAN,
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
            Some(
                ValidatedBindingKind::Node(node) | ValidatedBindingKind::StageColumn { node, .. },
            ) => Ok(node),
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
}

fn validate_graph_unwind_input(
    unwind: &GraphUnwind,
) -> Result<BTreeMap<String, LiteralListElementType>, CoreError> {
    let mut aliases = BTreeMap::new();
    let Some(input) = &unwind.input else {
        return Ok(aliases);
    };
    if input.projections.is_empty() {
        return Err(CoreError::internal("UNWIND input stage had no projections"));
    }
    for (index, projection) in input.projections.iter().enumerate() {
        validate_graph_unwind_input_projection(projection, index)?;
        if aliases
            .insert(projection.alias.clone(), projection.element_type)
            .is_some()
        {
            return Err(CoreError::internal(format!(
                "UNWIND input alias '{}' was projected more than once",
                projection.alias
            )));
        }
    }
    Ok(aliases)
}

fn validate_graph_unwind_input_projection(
    projection: &GraphUnwindInputProjection,
    index: usize,
) -> Result<(), CoreError> {
    validate_variable(
        format!("unwind.input.projections[{index}].alias"),
        &projection.alias,
    )?;
    let aliases = BTreeMap::new();
    let path = format!("unwind.input.projections[{index}].expression");
    validate_graph_unwind_list_expression(&projection.expression, &path, &aliases)?;
    validate_graph_unwind_element_type(
        &projection.expression,
        projection.element_type,
        &path,
        &aliases,
    )
}

fn validate_graph_unwind_list_expression(
    expression: &ScalarExpression,
    path: &str,
    input_aliases: &BTreeMap<String, LiteralListElementType>,
) -> Result<(), CoreError> {
    match expression {
        ScalarExpression::TypedLiteralList {
            literals,
            element_type,
        } => GraphPlanValidator::validate_typed_literal_list(literals, *element_type, path),
        ScalarExpression::StageValue { alias } => {
            if input_aliases.contains_key(alias) {
                Ok(())
            } else {
                Err(CoreError::internal(format!(
                    "UNWIND list expression references unknown input alias '{alias}'",
                )))
            }
        }
        ScalarExpression::ListConcat { left, right } => {
            validate_graph_unwind_list_expression(left, &format!("{path}.left"), input_aliases)?;
            validate_graph_unwind_list_expression(right, &format!("{path}.right"), input_aliases)
        }
        _ => Err(CoreError::internal(
            "UNWIND row source requires a list expression",
        )),
    }
}

fn validate_graph_unwind_element_type(
    expression: &ScalarExpression,
    expected: LiteralListElementType,
    path: &str,
    input_aliases: &BTreeMap<String, LiteralListElementType>,
) -> Result<(), CoreError> {
    match expression {
        ScalarExpression::TypedLiteralList {
            literals,
            element_type,
        } => {
            if *element_type != expected {
                return Err(CoreError::internal(format!(
                    "UNWIND list element type mismatch at {path}",
                )));
            }
            GraphPlanValidator::validate_typed_literal_list(literals, *element_type, path)
        }
        ScalarExpression::StageValue { alias } => {
            let Some(actual) = input_aliases.get(alias) else {
                return Err(CoreError::internal(format!(
                    "UNWIND list expression references unknown input alias '{alias}'",
                )));
            };
            if *actual != expected {
                return Err(CoreError::internal(format!(
                    "UNWIND input alias '{alias}' has incompatible element type at {path}",
                )));
            }
            Ok(())
        }
        ScalarExpression::ListConcat { left, right } => {
            validate_graph_unwind_element_type(
                left,
                expected,
                &format!("{path}.left"),
                input_aliases,
            )?;
            validate_graph_unwind_element_type(
                right,
                expected,
                &format!("{path}.right"),
                input_aliases,
            )
        }
        _ => Err(CoreError::internal(
            "UNWIND row source requires a list expression",
        )),
    }
}

pub(crate) fn unwind_stage_column_bindings(
    unwind: &GraphUnwind,
    stage_alias: &str,
) -> Result<StageColumnBindings, CoreError> {
    let [projection] = unwind.projections.as_slice() else {
        return Err(CoreError::internal(
            "UNWIND row source requires exactly one projection",
        ));
    };
    let GraphUnwindProjection::Variable { alias } = projection;
    let mut bindings = StageColumnBindings::default();
    bindings.scalar_values.insert(
        unwind.variable.clone(),
        StageScalarColumnBinding {
            stage_alias: stage_alias.to_string(),
            value_column: alias.clone(),
            scalar_type: scalar_type_for_literal_list_element(unwind.element_type),
        },
    );
    Ok(bindings)
}

pub(crate) fn staged_unwind_stage_column_bindings(
    staged: &GraphStagedUnwindQuery,
) -> Result<StageColumnBindings, CoreError> {
    validate_staged_unwind_source_export(staged)?;
    let mut bindings = StageColumnBindings::default();
    let stage_alias = "stage1";
    for export in &staged.stage.exports {
        match export {
            GraphStageExport::NodeKey { variable, column } => {
                if bindings
                    .node_keys
                    .insert(
                        variable.clone(),
                        StageNodeColumnBinding {
                            stage_alias: stage_alias.to_string(),
                            key_column: column.clone(),
                        },
                    )
                    .is_some()
                {
                    return Err(CoreError::internal(format!(
                        "staged UNWIND exported node variable '{variable}' more than once",
                    )));
                }
            }
            GraphStageExport::RelationshipKey { .. } => {
                return Err(CoreError::internal(
                    "staged UNWIND does not support carried relationship keys yet",
                ));
            }
            GraphStageExport::ScalarValue { alias, source } => {
                if bindings
                    .scalar_values
                    .insert(
                        alias.clone(),
                        StageScalarColumnBinding {
                            stage_alias: stage_alias.to_string(),
                            value_column: source.clone(),
                            scalar_type: ScalarType::Unknown,
                        },
                    )
                    .is_some()
                {
                    return Err(CoreError::internal(format!(
                        "staged UNWIND exported scalar value '{alias}' more than once",
                    )));
                }
            }
            GraphStageExport::AggregateValue { alias, .. } => {
                if alias != &staged.unwind.source_alias {
                    return Err(CoreError::internal(format!(
                        "staged UNWIND cannot carry aggregate value '{alias}'",
                    )));
                }
            }
        }
    }

    match &staged.unwind.binding {
        GraphStagedUnwindBinding::Scalar { element_type } => {
            if bindings
                .scalar_values
                .insert(
                    staged.unwind.variable.clone(),
                    StageScalarColumnBinding {
                        stage_alias: stage_alias.to_string(),
                        value_column: staged.unwind.variable.clone(),
                        scalar_type: scalar_type_for_literal_list_element(*element_type),
                    },
                )
                .is_some()
            {
                return Err(CoreError::internal(format!(
                    "staged UNWIND scalar '{}' conflicts with a carried scalar",
                    staged.unwind.variable
                )));
            }
        }
        GraphStagedUnwindBinding::NodeKey { .. } => {
            if bindings
                .node_keys
                .insert(
                    staged.unwind.variable.clone(),
                    StageNodeColumnBinding {
                        stage_alias: stage_alias.to_string(),
                        key_column: staged.unwind.variable.clone(),
                    },
                )
                .is_some()
            {
                return Err(CoreError::internal(format!(
                    "staged UNWIND node '{}' conflicts with a carried node",
                    staged.unwind.variable
                )));
            }
        }
    }
    Ok(bindings)
}

fn validate_staged_unwind_source_export(staged: &GraphStagedUnwindQuery) -> Result<(), CoreError> {
    validate_variable(
        "staged_unwind.unwind.source_alias",
        &staged.unwind.source_alias,
    )?;
    validate_variable("staged_unwind.unwind.variable", &staged.unwind.variable)?;
    let source_column = staged_unwind_source_column(staged)?;
    let Some(projection) = staged
        .stage
        .plan
        .projections
        .iter()
        .find(|projection| projection.output_name() == source_column)
    else {
        return Err(CoreError::internal(format!(
            "staged UNWIND source column '{source_column}' was not projected by the stage",
        )));
    };
    let Projection::Aggregate {
        function: AggregateFunction::Collect,
        ..
    } = projection
    else {
        return Err(CoreError::internal(format!(
            "staged UNWIND source column '{source_column}' must be a collect aggregate",
        )));
    };
    if let GraphStagedUnwindBinding::NodeKey { label } = &staged.unwind.binding {
        let Some(node) = staged
            .final_plan
            .nodes
            .iter()
            .find(|node| node.variable == staged.unwind.variable)
        else {
            return Err(CoreError::internal(format!(
                "staged UNWIND node '{}' is not bound in the final plan",
                staged.unwind.variable
            )));
        };
        if &node.label != label {
            return Err(CoreError::internal(format!(
                "staged UNWIND node '{}' expected label '{label}' but final plan used '{}'",
                staged.unwind.variable, node.label
            )));
        }
    }
    Ok(())
}

pub(crate) fn staged_unwind_source_column(
    staged: &GraphStagedUnwindQuery,
) -> Result<&str, CoreError> {
    staged
        .stage
        .exports
        .iter()
        .find_map(|export| match export {
            GraphStageExport::AggregateValue { alias, column }
                if alias == &staged.unwind.source_alias =>
            {
                Some(column.as_str())
            }
            _ => None,
        })
        .ok_or_else(|| {
            CoreError::internal(format!(
                "staged UNWIND source alias '{}' was not exported",
                staged.unwind.source_alias
            ))
        })
}

fn scalar_type_for_literal_list_element(element_type: LiteralListElementType) -> ScalarType {
    match element_type {
        LiteralListElementType::String => ScalarType::String,
        LiteralListElementType::Integer => ScalarType::Integer,
        LiteralListElementType::Float => ScalarType::Float,
        LiteralListElementType::Boolean => ScalarType::Boolean,
        LiteralListElementType::StringList
        | LiteralListElementType::IntegerList
        | LiteralListElementType::FloatList
        | LiteralListElementType::BooleanList => ScalarType::Other,
    }
}

pub(crate) fn stage_column_bindings(
    graph: &Declaration,
    staged: &super::ir::GraphStagedQuery,
) -> Result<StageColumnBindings, CoreError> {
    stage_column_bindings_with_catalog(graph, staged, None)
}

fn stage_column_bindings_with_catalog(
    graph: &Declaration,
    staged: &super::ir::GraphStagedQuery,
    catalog: Option<&CatalogInfo>,
) -> Result<StageColumnBindings, CoreError> {
    let mut bindings = StageColumnBindings::default();
    for (index, stage) in staged.stages.iter().enumerate() {
        let stage_alias = format!("stage{index}");
        let validated_stage = GraphPlanValidator::new(graph, &stage.plan, catalog).validate()?;
        let projection_types = GraphPlanValidator::new(graph, &stage.plan, catalog)
            .validate_and_infer_projection_scalar_types()?;
        for export in &stage.exports {
            let column = export.column();
            let Some((projection_index, projection)) = stage
                .plan
                .projections
                .iter()
                .enumerate()
                .find(|(_, projection)| projection.output_name() == column)
            else {
                return Err(CoreError::internal(format!(
                    "staged graph query exported missing column '{column}'",
                )));
            };
            match export {
                GraphStageExport::NodeKey { variable, column } => {
                    if bindings
                        .node_keys
                        .insert(
                            variable.clone(),
                            StageNodeColumnBinding {
                                stage_alias: stage_alias.clone(),
                                key_column: column.clone(),
                            },
                        )
                        .is_some()
                    {
                        return Err(CoreError::internal(format!(
                            "staged graph query exported variable '{variable}' more than once",
                        )));
                    }
                }
                GraphStageExport::RelationshipKey { variable, column } => {
                    bind_stage_relationship_key_export(
                        &mut bindings,
                        &stage_alias,
                        &validated_stage,
                        projection,
                        variable,
                        column,
                    )?;
                }
                GraphStageExport::AggregateValue { alias, column } => {
                    if !projection.is_aggregate() {
                        return Err(CoreError::internal(format!(
                            "staged graph query exported non-aggregate column '{column}' as aggregate value",
                        )));
                    }
                    let scalar_type = stage_projection_type(&projection_types, projection_index)?;
                    if bindings
                        .scalar_values
                        .insert(
                            alias.clone(),
                            StageScalarColumnBinding {
                                stage_alias: stage_alias.clone(),
                                value_column: column.clone(),
                                scalar_type,
                            },
                        )
                        .is_some()
                    {
                        return Err(CoreError::internal(format!(
                            "staged graph query exported scalar value '{alias}' more than once",
                        )));
                    }
                }
                GraphStageExport::ScalarValue { alias, source } => {
                    if projection.is_aggregate() {
                        return Err(CoreError::internal(format!(
                            "staged graph query exported aggregate column '{source}' as scalar value",
                        )));
                    }
                    let scalar_type = stage_projection_type(&projection_types, projection_index)?;
                    if bindings
                        .scalar_values
                        .insert(
                            alias.clone(),
                            StageScalarColumnBinding {
                                stage_alias: stage_alias.clone(),
                                value_column: source.clone(),
                                scalar_type,
                            },
                        )
                        .is_some()
                    {
                        return Err(CoreError::internal(format!(
                            "staged graph query exported scalar value '{alias}' more than once",
                        )));
                    }
                }
            }
        }
    }
    Ok(bindings)
}

fn bind_stage_relationship_key_export(
    bindings: &mut StageColumnBindings,
    stage_alias: &str,
    validated_stage: &ValidatedGraphPlan<'_>,
    projection: &Projection,
    variable: &str,
    column: &str,
) -> Result<(), CoreError> {
    match projection {
        Projection::Key {
            variable: projected_variable,
            ..
        } if projected_variable == variable => {}
        _ => {
            return Err(CoreError::internal(format!(
                "staged graph query exported non-key column '{column}' as relationship key",
            )));
        }
    }
    let binding = validated_stage.binding(variable)?;
    let ValidatedBindingKind::Relationship(relationship) = binding.kind() else {
        return Err(CoreError::internal(format!(
            "staged graph query exported non-relationship variable '{variable}' as relationship key",
        )));
    };
    if relationship.key.is_none() {
        return Err(CoreError::internal(format!(
            "staged graph query exported keyless relationship variable '{variable}'",
        )));
    }
    if bindings
        .relationship_keys
        .insert(
            variable.to_string(),
            StageRelationshipColumnBinding {
                stage_alias: stage_alias.to_string(),
                key_column: column.to_string(),
            },
        )
        .is_some()
    {
        return Err(CoreError::internal(format!(
            "staged graph query exported relationship variable '{variable}' more than once",
        )));
    }
    Ok(())
}

fn stage_projection_type(
    projection_types: &[ScalarType],
    projection_index: usize,
) -> Result<ScalarType, CoreError> {
    projection_types
        .get(projection_index)
        .copied()
        .ok_or_else(|| {
            CoreError::internal("staged graph query projection type index was out of bounds")
        })
}

fn validate_variable(path: impl Into<String>, variable: &str) -> Result<(), CoreError> {
    let path = path.into();
    if variable.trim().is_empty() {
        return Err(Diagnostic::new(
            diagnostic_codes::EMPTY_VARIABLE,
            path,
            "variable must not be empty",
        )
        .into_core_error());
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
        diagnostic_codes::UNION_SCHEMA_MISMATCH,
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
            diagnostic_codes::UNION_SCHEMA_MISMATCH,
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
                diagnostic_codes::UNKNOWN_PROJECTION,
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
    Temporal(TemporalKind),
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
            Self::Temporal(TemporalKind::Date) => "date",
            Self::Temporal(TemporalKind::LocalDateTime) => "localdatetime",
            Self::Temporal(TemporalKind::ZonedDateTime) => "datetime",
            Self::Temporal(TemporalKind::LocalTime) => "localtime",
            Self::Temporal(TemporalKind::Duration) => "duration",
            Self::Other => "non-scalar",
        }
    }
}

#[path = "validation_tests.rs"]
#[cfg(test)]
mod tests;
