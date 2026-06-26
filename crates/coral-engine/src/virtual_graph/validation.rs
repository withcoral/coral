use std::collections::{BTreeMap, BTreeSet};

use super::declaration::{Declaration, Node, Relationship};
use super::diagnostic::Diagnostic;
use super::ir::{
    ComparisonOperator, GraphPlan, Literal, Projection, PropertyPredicate, PropertyRef,
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
        self.validate_projection_shape()?;
        self.validate_aggregation()?;
        self.validate_property_references()?;
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
            let relationship = self
                .graph
                .relationship(&pattern.relationship_type)
                .ok_or_else(|| {
                    Diagnostic::new(
                        "UNKNOWN_RELATIONSHIP_TYPE",
                        format!("relationships[{index}].type"),
                        format!("unknown relationship type '{}'", pattern.relationship_type),
                    )
                    .into_core_error()
                })?;
            self.validate_relationship_endpoint_nodes(index, relationship, pattern)?;
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

    fn validate_relationship_endpoint_nodes(
        &self,
        index: usize,
        relationship: &Relationship,
        pattern: &RelationshipPattern,
    ) -> Result<(), CoreError> {
        let left_node =
            self.node_binding_for_path(&pattern.left, format!("relationships[{index}].left"))?;
        let right_node =
            self.node_binding_for_path(&pattern.right, format!("relationships[{index}].right"))?;

        let (expected_left, expected_right) = match pattern.direction {
            super::ir::Direction::Outgoing => (&relationship.from.label, &relationship.to.label),
            super::ir::Direction::Incoming => (&relationship.to.label, &relationship.from.label),
        };
        if left_node.label != *expected_left || right_node.label != *expected_right {
            return Err(Diagnostic::new(
                "RELATIONSHIP_ENDPOINT_MISMATCH",
                format!("relationships[{index}]"),
                format!(
                    "relationship type '{}' expects {} -> {}, got {} -> {}",
                    relationship.relationship_type,
                    relationship.from.label,
                    relationship.to.label,
                    left_node.label,
                    right_node.label
                ),
            )
            .into_core_error());
        }

        Ok(())
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

    fn validate_aggregation(&self) -> Result<(), CoreError> {
        let count_all_count = self
            .plan
            .projections
            .iter()
            .filter(|projection| matches!(projection, Projection::CountAll { .. }))
            .count();
        if count_all_count == 0 {
            return Ok(());
        }
        if self.plan.projections.len() != 1 {
            return Err(Diagnostic::new(
                "UNSUPPORTED_AGGREGATION",
                "projections",
                "COUNT(*) cannot be mixed with property projections until grouping is supported",
            )
            .into_core_error());
        }
        if !self.plan.order_by.is_empty() {
            return Err(Diagnostic::new(
                "UNSUPPORTED_AGGREGATION",
                "order_by",
                "ORDER BY with COUNT(*) is not supported until aggregate ordering is supported",
            )
            .into_core_error());
        }
        Ok(())
    }

    fn validate_property_references(&self) -> Result<(), CoreError> {
        for (index, projection) in self.plan.projections.iter().enumerate() {
            if let Projection::Property { property, .. } = projection {
                self.validate_property_ref(property, format!("projections[{index}].property"))?;
            }
        }
        for (index, predicate) in self.plan.predicates.iter().enumerate() {
            self.validate_predicate(index, predicate)?;
        }
        for (index, key) in self.plan.order_by.iter().enumerate() {
            self.validate_property_ref(&key.property, format!("order_by[{index}].property"))?;
        }
        Ok(())
    }

    fn validate_predicate(
        &self,
        index: usize,
        predicate: &PropertyPredicate,
    ) -> Result<(), CoreError> {
        self.validate_property_ref(&predicate.property, format!("predicates[{index}].property"))?;
        match (&predicate.operator, &predicate.literal) {
            (
                ComparisonOperator::GreaterThan
                | ComparisonOperator::GreaterThanOrEqual
                | ComparisonOperator::LessThan
                | ComparisonOperator::LessThanOrEqual,
                Literal::Null,
            ) => Err(Diagnostic::new(
                "INVALID_NULL_COMPARISON",
                format!("predicates[{index}]"),
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

        for (index, pattern) in self.plan.relationships.iter().enumerate() {
            let left_joined = joined_nodes.contains(pattern.left.as_str());
            let right_joined = joined_nodes.contains(pattern.right.as_str());
            if !left_joined && !right_joined {
                return Err(Diagnostic::new(
                    "DISCONNECTED_PATTERN",
                    format!("relationships[{index}]"),
                    "relationship does not connect to an already joined node",
                )
                .into_core_error());
            }
            joined_nodes.insert(pattern.left.as_str());
            joined_nodes.insert(pattern.right.as_str());
        }

        for node in &self.plan.nodes {
            if !joined_nodes.contains(node.variable.as_str()) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::virtual_graph::ir::{
        Direction, NodePattern, OrderDirection, OrderKey, Projection, PropertyRef,
        RelationshipPattern,
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
    fn validate_graph_plan_rejects_unknown_properties_before_lowering() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan();
        plan.order_by = vec![OrderKey {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "missing".to_string(),
            },
            direction: OrderDirection::Ascending,
        }];

        let error = graph
            .validate_graph_plan(&plan)
            .expect_err("unknown property should fail validation");

        assert!(error.to_string().contains("UNKNOWN_PROPERTY"), "{error:?}");
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
            projections: vec![Projection::Property {
                property: PropertyRef {
                    variable: "person".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("owner".to_string()),
            }],
            predicates: Vec::new(),
            order_by: Vec::new(),
            limit: None,
        }
    }
}
