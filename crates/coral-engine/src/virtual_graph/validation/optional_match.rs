//! OPTIONAL MATCH shape validation.
//!
//! Structural validation of the plan's OPTIONAL MATCH feature: the
//! `optional_relationships` index list (bounds, uniqueness, ascending order)
//! and each `OptionalMatchScope` (node/relationship index membership, scope
//! variables, and single- vs multi-hop predicate/chain shape). Read-only
//! `&self` checks over `self.plan`; connectivity/reachability lives in the
//! parent hub, not here.

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "OPTIONAL MATCH validation methods are split into a child module while preserving parent-private access."
)]
use super::*;

#[allow(
    clippy::allow_attributes,
    clippy::elidable_lifetime_names,
    reason = "Keep the split validation impl shape aligned with the parent GraphPlanValidator impl."
)]
impl<'a> GraphPlanValidator<'a> {
    pub(super) fn validate_optional_relationship_indices(&self) -> Result<(), CoreError> {
        let mut seen = BTreeSet::new();
        for (position, index) in self.plan.optional_relationships.iter().copied().enumerate() {
            if index >= self.plan.relationships.len() {
                return Err(Diagnostic::new(
                    diagnostic_codes::INVALID_OPTIONAL_RELATIONSHIP,
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
                    diagnostic_codes::DUPLICATE_OPTIONAL_RELATIONSHIP,
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
                diagnostic_codes::UNSORTED_OPTIONAL_RELATIONSHIPS,
                "optional_relationships",
                "optional relationship indices must be sorted in ascending order",
            )
            .into_core_error());
        }
        Ok(())
    }

    pub(super) fn validate_optional_predicates(&self) -> Result<(), CoreError> {
        if self.plan.optional_relationships.is_empty() {
            return Ok(());
        }
        if self.plan.post_projection_predicate.is_some() {
            return Err(Diagnostic::new(
                diagnostic_codes::UNSUPPORTED_OPTIONAL_PREDICATE,
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
                diagnostic_codes::UNSUPPORTED_OPTIONAL_PREDICATE,
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
                diagnostic_codes::INVALID_OPTIONAL_MATCH_SCOPE,
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
                    diagnostic_codes::INVALID_OPTIONAL_MATCH_SCOPE,
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
                    diagnostic_codes::INVALID_OPTIONAL_MATCH_SCOPE,
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
                diagnostic_codes::INVALID_OPTIONAL_MATCH_SCOPE,
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
                diagnostic_codes::INVALID_OPTIONAL_MATCH_SCOPE,
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
                diagnostic_codes::INVALID_OPTIONAL_MATCH_SCOPE,
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
                diagnostic_codes::INVALID_OPTIONAL_MATCH_SCOPE,
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
                diagnostic_codes::UNSUPPORTED_OPTIONAL_MATCH_SCOPE,
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
                diagnostic_codes::UNSUPPORTED_OPTIONAL_MATCH_SCOPE,
                format!("optional_matches[{index}].relationship_indices"),
                "multi-hop optional match scopes require one connected chain with one or two previously-bound boundary relationships",
            )
            .into_core_error());
        }

        if degree_by_node.values().any(|degree| *degree > 2) {
            return Err(Diagnostic::new(
                diagnostic_codes::UNSUPPORTED_OPTIONAL_MATCH_SCOPE,
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
                    diagnostic_codes::UNSUPPORTED_OPTIONAL_MATCH_SCOPE,
                    format!("optional_matches[{index}].relationship_indices"),
                    "multi-hop optional match scopes must be one connected chain",
                )
                .into_core_error());
            }
        }

        Ok(())
    }
}
