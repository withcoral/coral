#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "Relationship join helpers are split into a child module while preserving parent-private access."
)]
use super::*;

impl<'a> Lowerer<'a> {
    pub(super) fn render_optional_join_predicate(
        &self,
        relationship_index: usize,
    ) -> Result<Option<String>, CoreError> {
        let predicates = self
            .validated
            .plan()
            .optional_matches
            .iter()
            .filter(|optional_match| {
                optional_match.relationship_indices.as_slice() == [relationship_index]
            })
            .filter_map(|optional_match| optional_match.predicate.as_ref())
            .map(|predicate| self.render_predicate_expression(predicate))
            .collect::<Result<Vec<_>, _>>()?;
        if predicates.is_empty() {
            Ok(None)
        } else {
            Ok(Some(predicates.join(" AND ")))
        }
    }

    pub(super) fn render_optional_match_predicate(
        &self,
        optional_match: &OptionalMatchScope,
    ) -> Result<Option<String>, CoreError> {
        optional_match
            .predicate
            .as_ref()
            .map(|predicate| self.render_predicate_expression(predicate))
            .transpose()
    }

    pub(super) fn join_condition_with_predicate(
        condition: String,
        optional_predicate: Option<&str>,
    ) -> String {
        match optional_predicate {
            Some(predicate) => format!("({condition}) AND ({predicate})"),
            None => condition,
        }
    }

    pub(super) fn relationship_outer_condition_for_known_node(
        validated: &ValidatedGraphPlan<'a>,
        relationship: &Relationship,
        pattern: &'a RelationshipPattern,
        relationship_alias: &str,
        relationship_join: String,
    ) -> Result<String, CoreError> {
        if pattern.direction == Direction::Undirected
            && Self::relationship_orientations(validated, relationship, pattern)?.len() > 1
        {
            Self::relationship_pair_condition(validated, relationship, relationship_alias, pattern)
        } else {
            Ok(relationship_join)
        }
    }

    pub(super) fn relationship_inner_unknown_condition_for_known_node(
        validated: &ValidatedGraphPlan<'a>,
        relationship: &Relationship,
        pattern: &'a RelationshipPattern,
        relationship_alias: &str,
        unknown_variable: &str,
        unknown_is_left: bool,
    ) -> Result<String, CoreError> {
        if pattern.direction == Direction::Undirected
            && Self::relationship_orientations(validated, relationship, pattern)?.len() > 1
        {
            Self::relationship_pair_condition(validated, relationship, relationship_alias, pattern)
        } else {
            Self::relationship_known_node_condition(
                validated,
                relationship,
                pattern,
                relationship_alias,
                unknown_variable,
                unknown_is_left,
            )
        }
    }

    pub(super) fn relationship_pair_condition(
        validated: &ValidatedGraphPlan<'a>,
        relationship: &Relationship,
        relationship_alias: &str,
        pattern: &'a RelationshipPattern,
    ) -> Result<String, CoreError> {
        let orientations = Self::relationship_orientations(validated, relationship, pattern)?;
        let left_binding = validated.binding(&pattern.left)?;
        let right_binding = validated.binding(&pattern.right)?;
        let left_node = validated.node_binding(&pattern.left)?;
        let right_node = validated.node_binding(&pattern.right)?;

        let has_multiple_orientations = orientations.len() > 1;
        let conditions = orientations
            .iter()
            .map(|orientation| {
                let condition = format!(
                    "{}.{} = {}.{} AND {}.{} = {}.{}",
                    quote_ident(relationship_alias),
                    quote_ident(&orientation.left_relationship_key),
                    quote_ident(left_binding.alias()),
                    quote_ident(&left_node.key),
                    quote_ident(relationship_alias),
                    quote_ident(&orientation.right_relationship_key),
                    quote_ident(right_binding.alias()),
                    quote_ident(&right_node.key)
                );
                if has_multiple_orientations {
                    format!("({condition})")
                } else {
                    condition
                }
            })
            .collect::<Vec<_>>();
        Self::render_condition_disjunction(&conditions)
    }

    pub(super) fn relationship_known_node_condition(
        validated: &ValidatedGraphPlan<'a>,
        relationship: &Relationship,
        pattern: &'a RelationshipPattern,
        relationship_alias: &str,
        node_variable: &str,
        node_is_left: bool,
    ) -> Result<String, CoreError> {
        let orientations = Self::relationship_orientations(validated, relationship, pattern)?;
        let node_binding = validated.binding(node_variable)?;
        let node = validated.node_binding(node_variable)?;

        let conditions = orientations
            .iter()
            .map(|orientation| {
                let relationship_key = if node_is_left {
                    orientation.left_relationship_key.as_str()
                } else {
                    orientation.right_relationship_key.as_str()
                };
                format!(
                    "{}.{} = {}.{}",
                    quote_ident(relationship_alias),
                    quote_ident(relationship_key),
                    quote_ident(node_binding.alias()),
                    quote_ident(&node.key)
                )
            })
            .collect::<Vec<_>>();
        Self::render_condition_disjunction(&conditions)
    }

    pub(super) fn relationship_orientations(
        validated: &ValidatedGraphPlan<'a>,
        relationship: &Relationship,
        pattern: &'a RelationshipPattern,
    ) -> Result<Vec<RelationshipOrientation>, CoreError> {
        let left_node = validated.node_binding(&pattern.left)?;
        let right_node = validated.node_binding(&pattern.right)?;
        Self::relationship_orientations_for_labels(
            relationship,
            pattern.direction,
            &left_node.label,
            &right_node.label,
        )
    }

    pub(super) fn relationship_matches_labels(
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

    pub(super) fn relationship_orientations_for_labels(
        relationship: &Relationship,
        direction: Direction,
        left_label: &str,
        right_label: &str,
    ) -> Result<Vec<RelationshipOrientation>, CoreError> {
        match direction {
            Direction::Outgoing => Ok(vec![RelationshipOrientation {
                left_relationship_key: relationship.from.key.clone(),
                right_relationship_key: relationship.to.key.clone(),
            }]),
            Direction::Incoming => Ok(vec![RelationshipOrientation {
                left_relationship_key: relationship.to.key.clone(),
                right_relationship_key: relationship.from.key.clone(),
            }]),
            Direction::Undirected => {
                let mut orientations = Vec::with_capacity(2);
                if left_label == relationship.from.label && right_label == relationship.to.label {
                    orientations.push(RelationshipOrientation {
                        left_relationship_key: relationship.from.key.clone(),
                        right_relationship_key: relationship.to.key.clone(),
                    });
                }
                if left_label == relationship.to.label && right_label == relationship.from.label {
                    orientations.push(RelationshipOrientation {
                        left_relationship_key: relationship.to.key.clone(),
                        right_relationship_key: relationship.from.key.clone(),
                    });
                }
                if orientations.is_empty() {
                    return Err(CoreError::internal(
                        "validated undirected relationship had no endpoint orientation",
                    ));
                }
                Ok(orientations)
            }
        }
    }

    pub(super) fn render_condition_disjunction(conditions: &[String]) -> Result<String, CoreError> {
        match conditions {
            [] => Err(CoreError::internal(
                "relationship join had no endpoint condition",
            )),
            [condition] => Ok(condition.clone()),
            _ => Ok(format!("({})", conditions.join(" OR "))),
        }
    }
}
