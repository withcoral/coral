//! FROM-clause construction for the SQL `SqlRenderer`: assembles the graph plan's join tree —
//! start and cross-joined nodes, mandatory and OPTIONAL relationship joins, OPTIONAL MATCH
//! scope grouping and anchoring, and relationship orientation/label matching with the
//! resulting join conditions. `FromClauseBuilder` owns the mutable FROM workspace while
//! borrowing the render-capable `SqlRenderer`.

use std::collections::BTreeSet;

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "Relationship join helpers are split into a child module while preserving parent-private access."
)]
use super::*;

pub(super) struct FromClauseBuilder<'a, 'r> {
    lowerer: &'r SqlRenderer<'a>,
    joined_nodes: BTreeSet<&'a str>,
    joined_relationship_indices: BTreeSet<usize>,
    joined_stage_aliases: BTreeSet<String>,
    optional_relationships_joined: bool,
    from_clause: String,
}

impl<'a> SqlRenderer<'a> {
    fn render_optional_join_predicate(
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

    fn render_optional_match_predicate(
        &self,
        optional_match: &OptionalMatchScope,
    ) -> Result<Option<String>, CoreError> {
        optional_match
            .predicate
            .as_ref()
            .map(|predicate| self.render_predicate_expression(predicate))
            .transpose()
    }

    fn join_condition_with_predicate(
        condition: String,
        optional_predicate: Option<&str>,
    ) -> String {
        match optional_predicate {
            Some(predicate) => format!("({condition}) AND ({predicate})"),
            None => condition,
        }
    }

    fn relationship_outer_condition_for_known_node(
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

    fn relationship_inner_unknown_condition_for_known_node(
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

    fn relationship_pair_condition(
        validated: &ValidatedGraphPlan<'a>,
        relationship: &Relationship,
        relationship_alias: &str,
        pattern: &'a RelationshipPattern,
    ) -> Result<String, CoreError> {
        let orientations = Self::relationship_orientations(validated, relationship, pattern)?;
        let left_key = Self::node_key_ref(validated, &pattern.left)?;
        let right_key = Self::node_key_ref(validated, &pattern.right)?;

        let has_multiple_orientations = orientations.len() > 1;
        let conditions = orientations
            .iter()
            .map(|orientation| {
                let condition = format!(
                    "{}.{} = {} AND {}.{} = {}",
                    quote_ident(relationship_alias),
                    quote_ident(&orientation.left_relationship_key),
                    left_key,
                    quote_ident(relationship_alias),
                    quote_ident(&orientation.right_relationship_key),
                    right_key
                );
                if has_multiple_orientations {
                    format!("({condition})")
                } else {
                    condition
                }
            })
            .collect::<Vec<_>>();
        let condition = Self::render_condition_disjunction(&conditions)?;
        Self::condition_with_stage_relationship_key(
            validated,
            relationship,
            relationship_alias,
            pattern,
            condition,
        )
    }

    fn relationship_known_node_condition(
        validated: &ValidatedGraphPlan<'a>,
        relationship: &Relationship,
        pattern: &'a RelationshipPattern,
        relationship_alias: &str,
        node_variable: &str,
        node_is_left: bool,
    ) -> Result<String, CoreError> {
        let orientations = Self::relationship_orientations(validated, relationship, pattern)?;
        let node_key = Self::node_key_ref(validated, node_variable)?;

        let conditions = orientations
            .iter()
            .map(|orientation| {
                let relationship_key = if node_is_left {
                    orientation.left_relationship_key.as_str()
                } else {
                    orientation.right_relationship_key.as_str()
                };
                format!(
                    "{}.{} = {}",
                    quote_ident(relationship_alias),
                    quote_ident(relationship_key),
                    node_key
                )
            })
            .collect::<Vec<_>>();
        let condition = Self::render_condition_disjunction(&conditions)?;
        Self::condition_with_stage_relationship_key(
            validated,
            relationship,
            relationship_alias,
            pattern,
            condition,
        )
    }

    fn condition_with_stage_relationship_key(
        validated: &ValidatedGraphPlan<'a>,
        relationship: &Relationship,
        relationship_alias: &str,
        pattern: &'a RelationshipPattern,
        condition: String,
    ) -> Result<String, CoreError> {
        let Some(variable) = pattern.variable.as_deref() else {
            return Ok(condition);
        };
        let Some((stage_alias, key_column)) = validated.stage_relationship_column_ref(variable)
        else {
            return Ok(condition);
        };
        let relationship_key = relationship.key.as_deref().ok_or_else(|| {
            CoreError::internal("validated staged relationship did not have a key")
        })?;
        Ok(format!(
            "({condition}) AND ({}.{} = {}.{})",
            quote_ident(relationship_alias),
            quote_ident(relationship_key),
            quote_ident(stage_alias),
            quote_ident(key_column)
        ))
    }

    fn node_key_ref(
        validated: &ValidatedGraphPlan<'a>,
        variable: &str,
    ) -> Result<String, CoreError> {
        let binding = validated.binding(variable)?;
        match binding.kind() {
            ValidatedBindingKind::Node(node) => Ok(format!(
                "{}.{}",
                quote_ident(binding.alias()),
                quote_ident(&node.key)
            )),
            ValidatedBindingKind::StageColumn {
                stage_alias,
                key_column,
                ..
            } => Ok(format!(
                "{}.{}",
                quote_ident(stage_alias),
                quote_ident(key_column)
            )),
            ValidatedBindingKind::Relationship(_) => Err(CoreError::internal(
                "validated relationship endpoint was not a node binding",
            )),
        }
    }

    fn stage_column_node_rehydration_condition(
        validated: &ValidatedGraphPlan<'a>,
        variable: &str,
    ) -> Result<Option<String>, CoreError> {
        let binding = validated.binding(variable)?;
        match binding.kind() {
            ValidatedBindingKind::StageColumn {
                node,
                stage_alias,
                key_column,
            } => Ok(Some(format!(
                "{}.{} = {}.{}",
                quote_ident(binding.alias()),
                quote_ident(&node.key),
                quote_ident(stage_alias),
                quote_ident(key_column)
            ))),
            ValidatedBindingKind::Node(_) => Ok(None),
            ValidatedBindingKind::Relationship(_) => Err(CoreError::internal(
                "validated relationship endpoint was not a node binding",
            )),
        }
    }

    fn relationship_orientations(
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

impl<'a, 'r> FromClauseBuilder<'a, 'r> {
    pub(super) fn new(lowerer: &'r SqlRenderer<'a>) -> Self {
        Self {
            lowerer,
            joined_nodes: BTreeSet::new(),
            joined_relationship_indices: BTreeSet::new(),
            joined_stage_aliases: BTreeSet::new(),
            optional_relationships_joined: false,
            from_clause: String::new(),
        }
    }

    pub(super) fn build(mut self) -> Result<String, CoreError> {
        let plan = self.lowerer.validated.plan();
        if !self.try_start_from_staged_relationship()? {
            if let Some(first_node) = plan.nodes.first() {
                self.start_from_node(first_node.variable.as_str())?;
            } else {
                self.start_from_scalar_stage()?;
            }
        }

        self.join_mandatory_relationships()?;
        self.cross_join_isolated_nodes()?;
        self.ensure_optional_relationships_joined()?;
        self.cross_join_scalar_stages()?;

        Ok(self.from_clause)
    }

    fn start_from_scalar_stage(&mut self) -> Result<(), CoreError> {
        let stage_alias = self
            .lowerer
            .validated
            .scalar_stage_aliases()
            .into_iter()
            .next()
            .ok_or_else(|| CoreError::internal("validated graph plan had no nodes"))?;
        self.from_clause = format!(
            "FROM {} AS {}",
            quote_ident(stage_alias),
            quote_ident(stage_alias)
        );
        self.joined_stage_aliases.insert(stage_alias.to_string());
        Ok(())
    }

    fn try_start_from_staged_relationship(&mut self) -> Result<bool, CoreError> {
        if self.lowerer.validated.has_stage_node_keys() {
            return Ok(false);
        }
        let Some((index, pattern)) = self
            .lowerer
            .validated
            .plan()
            .relationships
            .iter()
            .enumerate()
            .find(|(_, pattern)| {
                pattern.variable.as_deref().is_some_and(|variable| {
                    self.lowerer
                        .validated
                        .stage_relationship_column_ref(variable)
                        .is_some()
                })
            })
        else {
            return Ok(false);
        };
        let variable = pattern
            .variable
            .as_deref()
            .ok_or_else(|| CoreError::internal("staged relationship variable was missing"))?;
        let (stage_alias, key_column) = self
            .lowerer
            .validated
            .stage_relationship_column_ref(variable)
            .ok_or_else(|| CoreError::internal("staged relationship key binding was missing"))?;
        let relationship = self.lowerer.validated.relationship_mapping(index)?;
        let relationship_key = relationship.key.as_deref().ok_or_else(|| {
            CoreError::internal("validated staged relationship did not have a key")
        })?;
        let relationship_alias = self.lowerer.validated.relationship_alias(index, pattern);
        self.joined_stage_aliases.insert(stage_alias.to_string());
        self.from_clause = format!(
            "FROM {} AS {} JOIN {} AS {} ON {}.{} = {}.{}",
            quote_ident(stage_alias),
            quote_ident(stage_alias),
            render_table_ref(&relationship.table),
            quote_ident(&relationship_alias),
            quote_ident(&relationship_alias),
            quote_ident(relationship_key),
            quote_ident(stage_alias),
            quote_ident(key_column)
        );
        self.joined_relationship_indices.insert(index);
        self.join_endpoint_nodes_from_staged_relationship(
            pattern,
            relationship,
            &relationship_alias,
        )?;
        Ok(true)
    }

    fn join_endpoint_nodes_from_staged_relationship(
        &mut self,
        pattern: &'a RelationshipPattern,
        relationship: &Relationship,
        relationship_alias: &str,
    ) -> Result<(), CoreError> {
        let orientations =
            SqlRenderer::relationship_orientations(&self.lowerer.validated, relationship, pattern)?;
        let [orientation] = orientations.as_slice() else {
            return Err(CoreError::internal(
                "staged relationship carry supports one deterministic orientation",
            ));
        };
        let optional = self
            .lowerer
            .validated
            .plan()
            .relationships
            .iter()
            .position(|candidate| candidate == pattern)
            .is_some_and(|index| self.lowerer.validated.relationship_is_optional(index));
        self.join_endpoint_node_from_staged_relationship(
            pattern.left.as_str(),
            relationship_alias,
            &orientation.left_relationship_key,
            optional,
        )?;
        self.join_endpoint_node_from_staged_relationship(
            pattern.right.as_str(),
            relationship_alias,
            &orientation.right_relationship_key,
            optional,
        )
    }

    fn join_endpoint_node_from_staged_relationship(
        &mut self,
        variable: &'a str,
        relationship_alias: &str,
        relationship_key: &str,
        optional: bool,
    ) -> Result<(), CoreError> {
        if self.joined_nodes.contains(variable) {
            return Ok(());
        }
        let node = self.lowerer.validated.node_binding(variable)?;
        let binding = self.lowerer.validated.binding(variable)?;
        let join_operator = if optional { " LEFT JOIN " } else { " JOIN " };
        write!(
            self.from_clause,
            "{}{} AS {} ON {}.{} = {}.{}",
            join_operator,
            render_table_ref(&node.table),
            quote_ident(binding.alias()),
            quote_ident(relationship_alias),
            quote_ident(relationship_key),
            quote_ident(binding.alias()),
            quote_ident(&node.key)
        )
        .map_err(|_| CoreError::internal("failed to render graph SQL"))?;
        self.joined_nodes.insert(variable);
        Ok(())
    }

    fn start_from_node(&mut self, variable: &'a str) -> Result<(), CoreError> {
        let binding = self.lowerer.validated.binding(variable)?;
        match binding.kind() {
            ValidatedBindingKind::Node(node_mapping) => {
                self.from_clause = format!(
                    "FROM {} AS {}",
                    render_table_ref(&node_mapping.table),
                    quote_ident(binding.alias())
                );
            }
            ValidatedBindingKind::StageColumn {
                node,
                stage_alias,
                key_column,
            } => {
                self.joined_stage_aliases.insert(stage_alias.clone());
                self.from_clause = format!(
                    "FROM {} AS {} JOIN {} AS {} ON {}.{} = {}.{}",
                    quote_ident(stage_alias),
                    quote_ident(stage_alias),
                    render_table_ref(&node.table),
                    quote_ident(binding.alias()),
                    quote_ident(binding.alias()),
                    quote_ident(&node.key),
                    quote_ident(stage_alias),
                    quote_ident(key_column)
                );
            }
            ValidatedBindingKind::Relationship(_) => {
                return Err(CoreError::internal("graph component root was not a node"));
            }
        }
        self.joined_nodes.insert(variable);
        Ok(())
    }

    fn cross_join_node(&mut self, variable: &'a str) -> Result<(), CoreError> {
        if self.joined_nodes.contains(variable) {
            return Ok(());
        }
        let binding = self.lowerer.validated.binding(variable)?;
        match binding.kind() {
            ValidatedBindingKind::Node(node_mapping) => {
                write!(
                    self.from_clause,
                    " CROSS JOIN {} AS {}",
                    render_table_ref(&node_mapping.table),
                    quote_ident(binding.alias())
                )
                .map_err(|_| CoreError::internal("failed to render graph SQL"))?;
            }
            ValidatedBindingKind::StageColumn {
                node,
                stage_alias,
                key_column,
            } => {
                self.joined_stage_aliases.insert(stage_alias.clone());
                write!(
                    self.from_clause,
                    " CROSS JOIN {} AS {} JOIN {} AS {} ON {}.{} = {}.{}",
                    quote_ident(stage_alias),
                    quote_ident(stage_alias),
                    render_table_ref(&node.table),
                    quote_ident(binding.alias()),
                    quote_ident(binding.alias()),
                    quote_ident(&node.key),
                    quote_ident(stage_alias),
                    quote_ident(key_column)
                )
                .map_err(|_| CoreError::internal("failed to render graph SQL"))?;
            }
            ValidatedBindingKind::Relationship(_) => {
                return Err(CoreError::internal("graph component root was not a node"));
            }
        }
        self.joined_nodes.insert(variable);
        Ok(())
    }

    fn cross_join_scalar_stages(&mut self) -> Result<(), CoreError> {
        for stage_alias in self.lowerer.validated.scalar_stage_aliases() {
            if self.joined_stage_aliases.contains(stage_alias) {
                continue;
            }
            write!(
                self.from_clause,
                " CROSS JOIN {} AS {}",
                quote_ident(stage_alias),
                quote_ident(stage_alias),
            )
            .map_err(|_| CoreError::internal("failed to render graph SQL"))?;
            self.joined_stage_aliases.insert(stage_alias.to_string());
        }
        Ok(())
    }

    fn cross_join_isolated_nodes(&mut self) -> Result<(), CoreError> {
        let scoped_relationships = self.lowerer.optional_match_scope_relationships();
        let optional_match_nodes = self
            .lowerer
            .validated
            .plan()
            .optional_matches
            .iter()
            .flat_map(|optional_match| {
                optional_match
                    .node_indices
                    .iter()
                    .copied()
                    .filter_map(|index| self.lowerer.validated.plan().nodes.get(index))
                    .map(|node| node.variable.as_str())
            });
        let unscoped_optional_relationship_nodes = self
            .lowerer
            .validated
            .plan()
            .optional_relationships
            .iter()
            .filter(|index| !scoped_relationships.contains(index))
            .filter_map(|index| self.lowerer.validated.plan().relationships.get(*index))
            .flat_map(|relationship| [relationship.left.as_str(), relationship.right.as_str()]);
        let optional_nodes = optional_match_nodes
            .chain(unscoped_optional_relationship_nodes)
            .collect::<BTreeSet<_>>();
        for node in &self.lowerer.validated.plan().nodes {
            if !self.joined_nodes.contains(node.variable.as_str())
                && !optional_nodes.contains(node.variable.as_str())
            {
                self.cross_join_node(node.variable.as_str())?;
            }
        }
        Ok(())
    }

    fn join_mandatory_relationships(&mut self) -> Result<(), CoreError> {
        let plan = self.lowerer.validated.plan();
        let validated = &self.lowerer.validated;
        let optional_nodes = self.lowerer.optional_relationship_node_variables();
        let mut remaining_relationships = (0..plan.relationships.len())
            .filter(|index| !validated.relationship_is_optional(*index))
            .filter(|index| !self.joined_relationship_indices.contains(index))
            .collect::<BTreeSet<_>>();
        while !remaining_relationships.is_empty() {
            let progressed =
                self.join_available_relationships(&mut remaining_relationships, false)?;
            if progressed {
                continue;
            }
            let index = *remaining_relationships
                .first()
                .ok_or_else(|| CoreError::internal("remaining relationship set was empty"))?;
            let pattern = plan.relationships.get(index).ok_or_else(|| {
                CoreError::internal("validated relationship index was out of bounds")
            })?;
            if !self.optional_relationships_joined
                && [pattern.left.as_str(), pattern.right.as_str()]
                    .iter()
                    .any(|variable| optional_nodes.contains(*variable))
            {
                self.ensure_optional_relationships_joined()?;
                continue;
            }
            self.cross_join_node(pattern.left.as_str())?;
        }
        Ok(())
    }
}

impl<'a> SqlRenderer<'a> {
    fn optional_relationship_node_variables(&self) -> BTreeSet<&'a str> {
        self.validated
            .plan()
            .optional_relationships
            .iter()
            .filter_map(|index| self.validated.plan().relationships.get(*index))
            .flat_map(|relationship| [relationship.left.as_str(), relationship.right.as_str()])
            .collect()
    }
}

impl FromClauseBuilder<'_, '_> {
    fn join_relationship_index_set(
        &mut self,
        remaining_relationships: &mut BTreeSet<usize>,
        optional: bool,
    ) -> Result<(), CoreError> {
        while !remaining_relationships.is_empty() {
            let progressed =
                self.join_available_relationships(&mut *remaining_relationships, optional)?;
            if !progressed {
                if optional {
                    let index = *remaining_relationships.first().ok_or_else(|| {
                        CoreError::internal("remaining optional relationship set was empty")
                    })?;
                    let anchor = self.lowerer.optional_relationship_component_anchor(index)?;
                    self.cross_join_node(anchor)?;
                    continue;
                }
                return Err(CoreError::internal(
                    "validated graph plan contained an unjoinable relationship",
                ));
            }
        }
        Ok(())
    }

    fn join_optional_relationships(&mut self) -> Result<(), CoreError> {
        let scoped_relationships = self.lowerer.optional_match_scope_relationships();
        self.join_optional_match_scopes()?;

        let mut remaining_relationships = self
            .lowerer
            .validated
            .plan()
            .optional_relationships
            .iter()
            .copied()
            .filter(|index| !scoped_relationships.contains(index))
            .filter(|index| !self.joined_relationship_indices.contains(index))
            .collect::<BTreeSet<_>>();
        self.join_relationship_index_set(&mut remaining_relationships, true)
    }

    fn ensure_optional_relationships_joined(&mut self) -> Result<(), CoreError> {
        if self.optional_relationships_joined {
            return Ok(());
        }
        self.join_optional_relationships()?;
        self.optional_relationships_joined = true;
        Ok(())
    }
}

impl SqlRenderer<'_> {
    fn optional_match_scope_relationships(&self) -> BTreeSet<usize> {
        self.validated
            .plan()
            .optional_matches
            .iter()
            .flat_map(|optional_match| optional_match.relationship_indices.iter().copied())
            .collect()
    }
}

impl FromClauseBuilder<'_, '_> {
    fn join_optional_match_scopes(&mut self) -> Result<(), CoreError> {
        let mut remaining_scopes =
            (0..self.lowerer.validated.plan().optional_matches.len()).collect::<BTreeSet<_>>();
        remaining_scopes.retain(|scope_index| {
            self.lowerer
                .validated
                .plan()
                .optional_matches
                .get(*scope_index)
                .is_none_or(|optional_match| {
                    !optional_match
                        .relationship_indices
                        .iter()
                        .all(|index| self.joined_relationship_indices.contains(index))
                })
        });
        while !remaining_scopes.is_empty() {
            let mut progressed = false;
            for index in remaining_scopes.iter().copied().collect::<Vec<_>>() {
                let optional_match = self
                    .lowerer
                    .validated
                    .plan()
                    .optional_matches
                    .get(index)
                    .ok_or_else(|| CoreError::internal("optional match scope index missing"))?
                    .clone();
                if self.try_join_optional_match_scope(&optional_match)? {
                    remaining_scopes.remove(&index);
                    progressed = true;
                }
            }
            if progressed {
                continue;
            }

            let index = *remaining_scopes
                .first()
                .ok_or_else(|| CoreError::internal("remaining optional scope set was empty"))?;
            let optional_match = self
                .lowerer
                .validated
                .plan()
                .optional_matches
                .get(index)
                .ok_or_else(|| CoreError::internal("optional match scope index missing"))?;
            let anchor = self
                .lowerer
                .optional_match_scope_component_anchor(optional_match)?;
            self.cross_join_node(anchor)?;
        }
        Ok(())
    }

    fn try_join_optional_match_scope(
        &mut self,
        optional_match: &OptionalMatchScope,
    ) -> Result<bool, CoreError> {
        let relationship_indices = optional_match.relationship_indices.as_slice();
        let [relationship_index] = relationship_indices else {
            let Some(anchor) = self.optional_match_scope_join_anchor(optional_match)? else {
                return Ok(false);
            };
            self.join_optional_match_group(optional_match, anchor)?;
            return Ok(true);
        };

        let pattern = self
            .lowerer
            .validated
            .plan()
            .relationships
            .get(*relationship_index)
            .ok_or_else(|| CoreError::internal("validated relationship index was out of bounds"))?;
        let left_joined = self.joined_nodes.contains(pattern.left.as_str());
        let right_joined = self.joined_nodes.contains(pattern.right.as_str());
        if !left_joined && !right_joined {
            return Ok(false);
        }

        let relationship = self
            .lowerer
            .validated
            .relationship_mapping(*relationship_index)?;
        let optional_predicate = self
            .lowerer
            .render_optional_match_predicate(optional_match)?;
        SqlRenderer::join_relationship(
            &self.lowerer.validated,
            &mut self.joined_nodes,
            &mut self.from_clause,
            *relationship_index,
            pattern,
            relationship,
            JoinRelationshipOptions {
                optional: true,
                optional_predicate: optional_predicate.as_deref(),
            },
        )?;
        Ok(true)
    }
}

impl<'a> SqlRenderer<'a> {
    fn optional_relationship_component_anchor(
        &self,
        relationship_index: usize,
    ) -> Result<&'a str, CoreError> {
        let pattern = self
            .validated
            .plan()
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

    fn optional_match_scope_component_anchor(
        &self,
        optional_match: &OptionalMatchScope,
    ) -> Result<&'a str, CoreError> {
        optional_match
            .relationship_indices
            .iter()
            .copied()
            .flat_map(|relationship_index| {
                self.validated
                    .plan()
                    .relationships
                    .get(relationship_index)
                    .into_iter()
                    .flat_map(|relationship| {
                        [relationship.left.as_str(), relationship.right.as_str()]
                    })
            })
            .min_by_key(|variable| self.node_position(variable).unwrap_or(usize::MAX))
            .ok_or_else(|| CoreError::internal("optional match scope had no anchor candidates"))
    }
}

impl<'a> FromClauseBuilder<'a, '_> {
    fn optional_match_scope_join_anchor(
        &self,
        optional_match: &OptionalMatchScope,
    ) -> Result<Option<OptionalScopeAnchor<'a>>, CoreError> {
        let mut anchor = None;
        for relationship_index in optional_match.relationship_indices.iter().copied() {
            let pattern = self
                .lowerer
                .validated
                .plan()
                .relationships
                .get(relationship_index)
                .ok_or_else(|| {
                    CoreError::internal("validated relationship index was out of bounds")
                })?;
            let left_joined = self.joined_nodes.contains(pattern.left.as_str());
            let right_joined = self.joined_nodes.contains(pattern.right.as_str());
            if left_joined ^ right_joined {
                anchor.get_or_insert(OptionalScopeAnchor {
                    relationship_index,
                    anchor_variable: if left_joined {
                        pattern.left.as_str()
                    } else {
                        pattern.right.as_str()
                    },
                    anchor_is_left: left_joined,
                });
            }
        }
        Ok(anchor)
    }

    fn join_optional_match_group(
        &mut self,
        optional_match: &OptionalMatchScope,
        anchor: OptionalScopeAnchor<'a>,
    ) -> Result<(), CoreError> {
        let mut remaining_relationships = optional_match
            .relationship_indices
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let mut inner_joined_nodes = BTreeSet::new();
        let (mut join_group, outer_condition) = self
            .lowerer
            .render_optional_match_group_anchor(anchor, &mut inner_joined_nodes)?;
        let mut outer_conditions = vec![outer_condition];
        remaining_relationships.remove(&anchor.relationship_index);

        while !remaining_relationships.is_empty() {
            let mut progressed = false;
            for relationship_index in remaining_relationships.iter().copied().collect::<Vec<_>>() {
                let pattern = self
                    .lowerer
                    .validated
                    .plan()
                    .relationships
                    .get(relationship_index)
                    .ok_or_else(|| {
                        CoreError::internal("validated relationship index was out of bounds")
                    })?;
                let left_joined = inner_joined_nodes.contains(pattern.left.as_str());
                let right_joined = inner_joined_nodes.contains(pattern.right.as_str());
                if !left_joined && !right_joined {
                    continue;
                }

                if let Some(outer_condition) = self.render_optional_match_group_relationship(
                    &mut join_group,
                    &mut inner_joined_nodes,
                    relationship_index,
                    left_joined,
                    right_joined,
                )? {
                    outer_conditions.push(outer_condition);
                }
                remaining_relationships.remove(&relationship_index);
                progressed = true;
            }
            if !progressed {
                return Err(CoreError::internal(
                    "validated optional match scope was not joinable",
                ));
            }
        }

        let optional_predicate = self
            .lowerer
            .render_optional_match_predicate(optional_match)?;
        let outer_condition = match outer_conditions.as_slice() {
            [] => {
                return Err(CoreError::internal(
                    "optional match scope had no outer condition",
                ));
            }
            [condition] => condition.clone(),
            _ => outer_conditions
                .into_iter()
                .map(|condition| format!("({condition})"))
                .collect::<Vec<_>>()
                .join(" AND "),
        };
        let outer_condition = SqlRenderer::join_condition_with_predicate(
            outer_condition,
            optional_predicate.as_deref(),
        );
        write!(
            self.from_clause,
            " LEFT JOIN ({join_group}) ON {outer_condition}"
        )
        .map_err(|_| CoreError::internal("failed to render graph SQL"))?;

        for relationship_index in optional_match.relationship_indices.iter().copied() {
            let pattern = self
                .lowerer
                .validated
                .plan()
                .relationships
                .get(relationship_index)
                .ok_or_else(|| {
                    CoreError::internal("validated relationship index was out of bounds")
                })?;
            self.joined_nodes.insert(pattern.left.as_str());
            self.joined_nodes.insert(pattern.right.as_str());
        }
        Ok(())
    }
}

impl<'a> SqlRenderer<'a> {
    fn render_optional_match_group_anchor(
        &self,
        anchor: OptionalScopeAnchor<'a>,
        inner_joined_nodes: &mut BTreeSet<&'a str>,
    ) -> Result<(String, String), CoreError> {
        let pattern = self
            .validated
            .plan()
            .relationships
            .get(anchor.relationship_index)
            .ok_or_else(|| CoreError::internal("validated relationship index was out of bounds"))?;
        let relationship = self
            .validated
            .relationship_mapping(anchor.relationship_index)?;
        let relationship_alias = self
            .validated
            .relationship_alias(anchor.relationship_index, pattern);
        let relationship_join = Self::relationship_known_node_condition(
            &self.validated,
            relationship,
            pattern,
            &relationship_alias,
            anchor.anchor_variable,
            anchor.anchor_is_left,
        )?;
        let outer_condition = Self::relationship_outer_condition_for_known_node(
            &self.validated,
            relationship,
            pattern,
            &relationship_alias,
            relationship_join,
        )?;
        let unknown_variable = if anchor.anchor_is_left {
            pattern.right.as_str()
        } else {
            pattern.left.as_str()
        };
        let unknown_join = Self::relationship_inner_unknown_condition_for_known_node(
            &self.validated,
            relationship,
            pattern,
            &relationship_alias,
            unknown_variable,
            !anchor.anchor_is_left,
        )?;
        let unknown_node = self.validated.node_binding(unknown_variable)?;
        let unknown_alias = self.validated.binding(unknown_variable)?.alias();
        inner_joined_nodes.insert(unknown_variable);
        Ok((
            format!(
                "{} AS {} JOIN {} AS {} ON {}",
                render_table_ref(&relationship.table),
                quote_ident(&relationship_alias),
                render_table_ref(&unknown_node.table),
                quote_ident(unknown_alias),
                unknown_join
            ),
            outer_condition,
        ))
    }
}

impl<'a> FromClauseBuilder<'a, '_> {
    fn render_optional_match_group_relationship(
        &self,
        join_group: &mut String,
        inner_joined_nodes: &mut BTreeSet<&'a str>,
        relationship_index: usize,
        left_joined: bool,
        right_joined: bool,
    ) -> Result<Option<String>, CoreError> {
        let pattern = self
            .lowerer
            .validated
            .plan()
            .relationships
            .get(relationship_index)
            .ok_or_else(|| CoreError::internal("validated relationship index was out of bounds"))?;
        let relationship = self
            .lowerer
            .validated
            .relationship_mapping(relationship_index)?;
        let relationship_alias = self
            .lowerer
            .validated
            .relationship_alias(relationship_index, pattern);
        if left_joined && right_joined {
            let condition = SqlRenderer::relationship_pair_condition(
                &self.lowerer.validated,
                relationship,
                &relationship_alias,
                pattern,
            )?;
            write!(
                join_group,
                " JOIN {} AS {} ON {}",
                render_table_ref(&relationship.table),
                quote_ident(&relationship_alias),
                condition
            )
            .map_err(|_| CoreError::internal("failed to render graph SQL"))?;
            return Ok(None);
        }

        let (known_variable, unknown_variable, known_is_left) = if left_joined {
            (pattern.left.as_str(), pattern.right.as_str(), true)
        } else {
            (pattern.right.as_str(), pattern.left.as_str(), false)
        };
        let relationship_join = SqlRenderer::relationship_known_node_condition(
            &self.lowerer.validated,
            relationship,
            pattern,
            &relationship_alias,
            known_variable,
            known_is_left,
        )?;
        write!(
            join_group,
            " JOIN {} AS {} ON {}",
            render_table_ref(&relationship.table),
            quote_ident(&relationship_alias),
            relationship_join
        )
        .map_err(|_| CoreError::internal("failed to render graph SQL"))?;

        if self.joined_nodes.contains(unknown_variable) {
            let outer_join = SqlRenderer::relationship_known_node_condition(
                &self.lowerer.validated,
                relationship,
                pattern,
                &relationship_alias,
                unknown_variable,
                !known_is_left,
            )?;
            return SqlRenderer::relationship_outer_condition_for_known_node(
                &self.lowerer.validated,
                relationship,
                pattern,
                &relationship_alias,
                outer_join,
            )
            .map(Some);
        }

        let unknown_join = SqlRenderer::relationship_inner_unknown_condition_for_known_node(
            &self.lowerer.validated,
            relationship,
            pattern,
            &relationship_alias,
            unknown_variable,
            !known_is_left,
        )?;
        let unknown_node = self.lowerer.validated.node_binding(unknown_variable)?;
        let unknown_alias = self.lowerer.validated.binding(unknown_variable)?.alias();
        write!(
            join_group,
            " JOIN {} AS {} ON {}",
            render_table_ref(&unknown_node.table),
            quote_ident(unknown_alias),
            unknown_join
        )
        .map_err(|_| CoreError::internal("failed to render graph SQL"))?;
        inner_joined_nodes.insert(unknown_variable);
        Ok(None)
    }
}

impl SqlRenderer<'_> {
    fn node_position(&self, variable: &str) -> Result<usize, CoreError> {
        self.validated
            .plan()
            .nodes
            .iter()
            .position(|node| node.variable == variable)
            .ok_or_else(|| CoreError::internal("validated node variable was missing"))
    }
}

impl FromClauseBuilder<'_, '_> {
    fn join_available_relationships(
        &mut self,
        remaining_relationships: &mut BTreeSet<usize>,
        optional: bool,
    ) -> Result<bool, CoreError> {
        let plan = self.lowerer.validated.plan();
        let validated = &self.lowerer.validated;
        let mut progressed = false;
        for index in remaining_relationships.iter().copied().collect::<Vec<_>>() {
            let pattern = plan.relationships.get(index).ok_or_else(|| {
                CoreError::internal("validated relationship index was out of bounds")
            })?;
            let left_joined = self.joined_nodes.contains(pattern.left.as_str());
            let right_joined = self.joined_nodes.contains(pattern.right.as_str());
            if left_joined || right_joined {
                let relationship = validated.relationship_mapping(index)?;
                let optional_predicate = if optional {
                    self.lowerer.render_optional_join_predicate(index)?
                } else {
                    None
                };
                SqlRenderer::join_relationship(
                    validated,
                    &mut self.joined_nodes,
                    &mut self.from_clause,
                    index,
                    pattern,
                    relationship,
                    JoinRelationshipOptions {
                        optional,
                        optional_predicate: optional_predicate.as_deref(),
                    },
                )?;
                self.joined_relationship_indices.insert(index);
                remaining_relationships.remove(&index);
                progressed = true;
            }
        }
        Ok(progressed)
    }
}

impl<'a> SqlRenderer<'a> {
    fn join_relationship(
        validated: &ValidatedGraphPlan<'a>,
        joined_nodes: &mut BTreeSet<&'a str>,
        from_clause: &mut String,
        index: usize,
        pattern: &'a RelationshipPattern,
        relationship: &Relationship,
        options: JoinRelationshipOptions<'_>,
    ) -> Result<(), CoreError> {
        let left_joined = joined_nodes.contains(pattern.left.as_str());
        let right_joined = joined_nodes.contains(pattern.right.as_str());
        if !left_joined && !right_joined {
            return Err(CoreError::internal(
                "validated graph relationship was not joinable",
            ));
        }

        let relationship_alias = validated.relationship_alias(index, pattern);
        let quoted_relationship_alias = quote_ident(&relationship_alias);
        let join_operator = if options.optional {
            " LEFT JOIN "
        } else {
            " JOIN "
        };

        if left_joined && right_joined {
            let condition = Self::relationship_pair_condition(
                validated,
                relationship,
                &relationship_alias,
                pattern,
            )?;
            let condition =
                Self::join_condition_with_predicate(condition, options.optional_predicate);
            write!(
                from_clause,
                "{}{} AS {} ON {}",
                join_operator,
                render_table_ref(&relationship.table),
                quoted_relationship_alias,
                condition
            )
            .map_err(|_| CoreError::internal("failed to render graph SQL"))?;
        } else if left_joined {
            Self::join_from_known_node(
                validated,
                joined_nodes,
                from_clause,
                relationship,
                pattern,
                &relationship_alias,
                JoinFromKnownNodeOptions {
                    left_is_known: true,
                    optional: options.optional,
                    optional_predicate: options.optional_predicate,
                },
            )?;
        } else {
            Self::join_from_known_node(
                validated,
                joined_nodes,
                from_clause,
                relationship,
                pattern,
                &relationship_alias,
                JoinFromKnownNodeOptions {
                    left_is_known: false,
                    optional: options.optional,
                    optional_predicate: options.optional_predicate,
                },
            )?;
        }

        Ok(())
    }

    fn join_from_known_node(
        validated: &ValidatedGraphPlan<'a>,
        joined_nodes: &mut BTreeSet<&'a str>,
        from_clause: &mut String,
        relationship: &Relationship,
        pattern: &'a RelationshipPattern,
        relationship_alias: &str,
        options: JoinFromKnownNodeOptions<'_>,
    ) -> Result<(), CoreError> {
        let (known_variable, unknown_variable, known_is_left) = if options.left_is_known {
            (pattern.left.as_str(), pattern.right.as_str(), true)
        } else {
            (pattern.right.as_str(), pattern.left.as_str(), false)
        };
        let relationship_join = Self::relationship_known_node_condition(
            validated,
            relationship,
            pattern,
            relationship_alias,
            known_variable,
            known_is_left,
        )?;
        if options.optional
            && Self::try_join_stage_relationship_optional_unknown_node(
                validated,
                joined_nodes,
                from_clause,
                StageRelationshipOptionalUnknownOptions {
                    relationship,
                    pattern,
                    relationship_alias,
                    unknown_variable,
                    optional_predicate: options.optional_predicate,
                },
            )?
        {
            return Ok(());
        }
        if options.optional
            && let Some(optional_predicate) = options.optional_predicate
        {
            Self::join_optional_predicate_unknown_node(
                validated,
                joined_nodes,
                from_clause,
                OptionalPredicateUnknownJoinOptions {
                    relationship,
                    pattern,
                    relationship_alias,
                    unknown_variable,
                    known_is_left,
                    relationship_join: &relationship_join,
                    optional_predicate,
                },
            )?;
            return Ok(());
        }

        let unknown_node = validated.node_binding(unknown_variable)?;
        let unknown_table_ref = render_table_ref(&unknown_node.table);
        let unknown_alias = validated.binding(unknown_variable)?.alias().to_string();
        let join_operator = if options.optional {
            " LEFT JOIN "
        } else {
            " JOIN "
        };
        write!(
            from_clause,
            "{}{} AS {} ON {}",
            join_operator,
            render_table_ref(&relationship.table),
            quote_ident(relationship_alias),
            relationship_join
        )
        .map_err(|_| CoreError::internal("failed to render graph SQL"))?;
        let unknown_join = Self::unknown_node_join_condition(
            validated,
            relationship,
            pattern,
            relationship_alias,
            unknown_variable,
            known_is_left,
        )?;
        write!(
            from_clause,
            "{}{} AS {} ON {}",
            join_operator,
            unknown_table_ref,
            quote_ident(&unknown_alias),
            unknown_join
        )
        .map_err(|_| CoreError::internal("failed to render graph SQL"))?;
        joined_nodes.insert(unknown_variable);
        Ok(())
    }

    fn join_optional_predicate_unknown_node(
        validated: &ValidatedGraphPlan<'a>,
        joined_nodes: &mut BTreeSet<&'a str>,
        from_clause: &mut String,
        options: OptionalPredicateUnknownJoinOptions<'_, 'a>,
    ) -> Result<(), CoreError> {
        let mut unknown_join = Self::relationship_known_node_condition(
            validated,
            options.relationship,
            options.pattern,
            options.relationship_alias,
            options.unknown_variable,
            !options.known_is_left,
        )?;
        if let Some(rehydration) =
            Self::stage_column_node_rehydration_condition(validated, options.unknown_variable)?
        {
            unknown_join = format!("({unknown_join}) AND ({rehydration})");
        }
        let relationship_condition = if options.pattern.direction == Direction::Undirected
            && Self::relationship_orientations(validated, options.relationship, options.pattern)?
                .len()
                > 1
        {
            Self::relationship_pair_condition(
                validated,
                options.relationship,
                options.relationship_alias,
                options.pattern,
            )?
        } else {
            options.relationship_join.to_string()
        };
        let outer_condition = Self::join_condition_with_predicate(
            relationship_condition,
            Some(options.optional_predicate),
        );
        let unknown_node = validated.node_binding(options.unknown_variable)?;
        let unknown_alias = validated.binding(options.unknown_variable)?.alias();
        write!(
            from_clause,
            " LEFT JOIN ({} AS {} JOIN {} AS {} ON {}) ON {}",
            render_table_ref(&options.relationship.table),
            quote_ident(options.relationship_alias),
            render_table_ref(&unknown_node.table),
            quote_ident(unknown_alias),
            unknown_join,
            outer_condition
        )
        .map_err(|_| CoreError::internal("failed to render graph SQL"))?;
        joined_nodes.insert(options.unknown_variable);
        Ok(())
    }

    fn try_join_stage_relationship_optional_unknown_node(
        validated: &ValidatedGraphPlan<'a>,
        joined_nodes: &mut BTreeSet<&'a str>,
        from_clause: &mut String,
        options: StageRelationshipOptionalUnknownOptions<'_, 'a>,
    ) -> Result<bool, CoreError> {
        let Some(variable) = options.pattern.variable.as_deref() else {
            return Ok(false);
        };
        let Some((stage_alias, key_column)) = validated.stage_relationship_column_ref(variable)
        else {
            return Ok(false);
        };
        if !matches!(
            validated.binding(options.unknown_variable)?.kind(),
            ValidatedBindingKind::Node(_)
        ) {
            return Ok(false);
        }

        let relationship_key = options.relationship.key.as_deref().ok_or_else(|| {
            CoreError::internal("validated staged relationship did not have a key")
        })?;
        let relationship_condition = format!(
            "{}.{} = {}.{}",
            quote_ident(options.relationship_alias),
            quote_ident(relationship_key),
            quote_ident(stage_alias),
            quote_ident(key_column)
        );
        let unknown_join = Self::relationship_pair_condition(
            validated,
            options.relationship,
            options.relationship_alias,
            options.pattern,
        )?;
        let unknown_join =
            Self::join_condition_with_predicate(unknown_join, options.optional_predicate);
        let unknown_node = validated.node_binding(options.unknown_variable)?;
        let unknown_alias = validated.binding(options.unknown_variable)?.alias();
        write!(
            from_clause,
            " JOIN {} AS {} ON {} LEFT JOIN {} AS {} ON {}",
            render_table_ref(&options.relationship.table),
            quote_ident(options.relationship_alias),
            relationship_condition,
            render_table_ref(&unknown_node.table),
            quote_ident(unknown_alias),
            unknown_join
        )
        .map_err(|_| CoreError::internal("failed to render graph SQL"))?;
        joined_nodes.insert(options.unknown_variable);
        Ok(true)
    }

    fn unknown_node_join_condition(
        validated: &ValidatedGraphPlan<'a>,
        relationship: &Relationship,
        pattern: &'a RelationshipPattern,
        relationship_alias: &str,
        unknown_variable: &str,
        known_is_left: bool,
    ) -> Result<String, CoreError> {
        let mut condition = if pattern.direction == Direction::Undirected
            && Self::relationship_orientations(validated, relationship, pattern)?.len() > 1
        {
            Self::relationship_pair_condition(validated, relationship, relationship_alias, pattern)?
        } else {
            Self::relationship_known_node_condition(
                validated,
                relationship,
                pattern,
                relationship_alias,
                unknown_variable,
                !known_is_left,
            )?
        };
        if let Some(rehydration) =
            Self::stage_column_node_rehydration_condition(validated, unknown_variable)?
        {
            condition = format!("({condition}) AND ({rehydration})");
        }
        Ok(condition)
    }
}
