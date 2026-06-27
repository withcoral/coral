use std::collections::BTreeSet;
use std::fmt::Write as _;

use super::declaration::{Declaration, Relationship, TableRef};
use super::diagnostic::Diagnostic;
use super::ir::{
    AggregateFunction, AggregateTarget, ArithmeticOperator, ComparisonOperator, Direction,
    ElementIdPredicate, GraphPlan, KeyPredicate, Literal, OrderDirection, OrderExpression,
    PredicateExpression, PredicateRhs, PresencePredicate, Projection, ProjectionPredicate,
    ProjectionPredicateExpression, ProjectionPredicateRhs, PropertyKeyMembershipPredicate,
    PropertyRef, ScalarCaseAlternative, ScalarExpression, ScalarPredicate, ScalarPredicateRhs,
};
use super::validation::{ValidatedBindingKind, ValidatedGraphPlan};
use crate::CoreError;

/// Result of lowering a graph query plan to `DataFusion` SQL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlTranslation {
    sql: String,
    diagnostics: Vec<Diagnostic>,
}

impl SqlTranslation {
    /// Builds a SQL translation result.
    #[must_use]
    pub fn new(sql: String, diagnostics: Vec<Diagnostic>) -> Self {
        Self { sql, diagnostics }
    }

    /// Returns the translated `DataFusion` SQL.
    #[must_use]
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Returns non-fatal translation diagnostics.
    #[must_use]
    pub fn diagnostics(&self) -> &[Diagnostic] {
        &self.diagnostics
    }
}

impl Declaration {
    /// Lowers a shared graph query plan into `DataFusion` SQL.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidInput`] when the graph plan references
    /// unknown labels, relationship types, variables, or properties, or when
    /// the plan uses a relationship shape not yet supported by the lowerer.
    pub fn lower_graph_plan(&self, plan: &GraphPlan) -> Result<SqlTranslation, CoreError> {
        let validated = self.validate_graph_plan(plan)?;
        Lowerer::new(validated).lower()
    }
}

struct Lowerer<'a> {
    validated: ValidatedGraphPlan<'a>,
    joined_nodes: BTreeSet<&'a str>,
    from_clause: String,
}

#[derive(Debug, Clone)]
struct RelationshipOrientation {
    left_relationship_key: String,
    right_relationship_key: String,
}

#[derive(Debug, Clone, Copy)]
struct JoinFromKnownNodeOptions<'p> {
    left_is_known: bool,
    optional: bool,
    optional_predicate: Option<&'p str>,
}

#[derive(Debug, Clone, Copy)]
struct JoinRelationshipOptions<'p> {
    optional: bool,
    optional_predicate: Option<&'p str>,
}

impl<'a> Lowerer<'a> {
    fn new(validated: ValidatedGraphPlan<'a>) -> Self {
        Self {
            validated,
            joined_nodes: BTreeSet::new(),
            from_clause: String::new(),
        }
    }

    fn lower(mut self) -> Result<SqlTranslation, CoreError> {
        self.build_from_clause()?;

        let select = self.render_select()?;
        let where_clause = self.render_where()?;
        let group_by = self.render_group_by()?;
        let having = self.render_having()?;
        let order_by = self.render_order_by()?;
        let limit = self
            .validated
            .plan()
            .limit
            .map(|limit| format!(" LIMIT {limit}"))
            .unwrap_or_default();
        let offset = self
            .validated
            .plan()
            .skip
            .map(|skip| format!(" OFFSET {skip}"))
            .unwrap_or_default();

        Ok(SqlTranslation::new(
            format!(
                "{select} {}{where_clause}{group_by}{having}{order_by}{limit}{offset}",
                self.from_clause
            ),
            Vec::new(),
        ))
    }

    fn build_from_clause(&mut self) -> Result<(), CoreError> {
        let plan = self.validated.plan();
        let first_node = plan
            .nodes
            .first()
            .ok_or_else(|| CoreError::internal("validated graph plan had no nodes"))?;
        self.start_from_node(first_node.variable.as_str())?;

        self.join_mandatory_relationships()?;
        self.cross_join_isolated_nodes()?;
        self.join_relationships(true)?;

        Ok(())
    }

    fn start_from_node(&mut self, variable: &'a str) -> Result<(), CoreError> {
        let binding = self.validated.binding(variable)?;
        let ValidatedBindingKind::Node(node_mapping) = binding.kind() else {
            return Err(CoreError::internal("graph component root was not a node"));
        };
        self.from_clause = format!(
            "FROM {} AS {}",
            render_table_ref(&node_mapping.table),
            quote_ident(binding.alias())
        );
        self.joined_nodes.insert(variable);
        Ok(())
    }

    fn cross_join_node(&mut self, variable: &'a str) -> Result<(), CoreError> {
        if self.joined_nodes.contains(variable) {
            return Ok(());
        }
        let binding = self.validated.binding(variable)?;
        let ValidatedBindingKind::Node(node_mapping) = binding.kind() else {
            return Err(CoreError::internal("graph component root was not a node"));
        };
        write!(
            self.from_clause,
            " CROSS JOIN {} AS {}",
            render_table_ref(&node_mapping.table),
            quote_ident(binding.alias())
        )
        .map_err(|_| CoreError::internal("failed to render graph SQL"))?;
        self.joined_nodes.insert(variable);
        Ok(())
    }

    fn cross_join_isolated_nodes(&mut self) -> Result<(), CoreError> {
        let optional_nodes = self
            .validated
            .plan()
            .optional_relationships
            .iter()
            .filter_map(|index| self.validated.plan().relationships.get(*index))
            .flat_map(|relationship| [relationship.left.as_str(), relationship.right.as_str()])
            .collect::<BTreeSet<_>>();
        for node in &self.validated.plan().nodes {
            if !self.joined_nodes.contains(node.variable.as_str())
                && !optional_nodes.contains(node.variable.as_str())
            {
                self.cross_join_node(node.variable.as_str())?;
            }
        }
        Ok(())
    }

    fn join_mandatory_relationships(&mut self) -> Result<(), CoreError> {
        let plan = self.validated.plan();
        let validated = &self.validated;
        let mut remaining_relationships = (0..plan.relationships.len())
            .filter(|index| !validated.relationship_is_optional(*index))
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
            self.cross_join_node(pattern.left.as_str())?;
        }
        Ok(())
    }

    fn join_relationships(&mut self, optional: bool) -> Result<(), CoreError> {
        let plan = self.validated.plan();
        let validated = &self.validated;
        let mut remaining_relationships = (0..plan.relationships.len())
            .filter(|index| validated.relationship_is_optional(*index) == optional)
            .collect::<BTreeSet<_>>();
        while !remaining_relationships.is_empty() {
            let progressed =
                self.join_available_relationships(&mut remaining_relationships, optional)?;
            if !progressed {
                return Err(CoreError::internal(
                    "validated graph plan contained an unjoinable relationship",
                ));
            }
        }
        Ok(())
    }

    fn join_available_relationships(
        &mut self,
        remaining_relationships: &mut BTreeSet<usize>,
        optional: bool,
    ) -> Result<bool, CoreError> {
        let plan = self.validated.plan();
        let validated = &self.validated;
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
                    self.render_optional_join_predicate(index)?
                } else {
                    None
                };
                Self::join_relationship(
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
                remaining_relationships.remove(&index);
                progressed = true;
            }
        }
        Ok(progressed)
    }

    fn join_relationship(
        validated: &ValidatedGraphPlan<'a>,
        joined_nodes: &mut BTreeSet<&'a str>,
        from_clause: &mut String,
        index: usize,
        pattern: &'a super::ir::RelationshipPattern,
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
        pattern: &'a super::ir::RelationshipPattern,
        relationship_alias: &str,
        options: JoinFromKnownNodeOptions<'_>,
    ) -> Result<(), CoreError> {
        let (known_variable, unknown_variable, known_is_left) = if options.left_is_known {
            (pattern.left.as_str(), pattern.right.as_str(), true)
        } else {
            (pattern.right.as_str(), pattern.left.as_str(), false)
        };
        let unknown_node = validated.node_binding(unknown_variable)?;
        let relationship_join = Self::relationship_known_node_condition(
            validated,
            relationship,
            pattern,
            relationship_alias,
            known_variable,
            known_is_left,
        )?;
        let unknown_table_ref = render_table_ref(&unknown_node.table);
        let unknown_alias = validated.binding(unknown_variable)?.alias().to_string();
        let join_operator = if options.optional {
            " LEFT JOIN "
        } else {
            " JOIN "
        };
        if options.optional
            && let Some(optional_predicate) = options.optional_predicate
        {
            let unknown_join = Self::relationship_known_node_condition(
                validated,
                relationship,
                pattern,
                relationship_alias,
                unknown_variable,
                !known_is_left,
            )?;
            let relationship_condition = if pattern.direction == Direction::Undirected
                && Self::relationship_orientations(validated, relationship, pattern)?.len() > 1
            {
                Self::relationship_pair_condition(
                    validated,
                    relationship,
                    relationship_alias,
                    pattern,
                )?
            } else {
                relationship_join
            };
            let outer_condition = Self::join_condition_with_predicate(
                relationship_condition,
                Some(optional_predicate),
            );
            write!(
                from_clause,
                " LEFT JOIN ({} AS {} JOIN {} AS {} ON {}) ON {}",
                render_table_ref(&relationship.table),
                quote_ident(relationship_alias),
                unknown_table_ref,
                quote_ident(&unknown_alias),
                unknown_join,
                outer_condition
            )
            .map_err(|_| CoreError::internal("failed to render graph SQL"))?;
            joined_nodes.insert(unknown_variable);
            return Ok(());
        }

        write!(
            from_clause,
            "{}{} AS {} ON {}",
            join_operator,
            render_table_ref(&relationship.table),
            quote_ident(relationship_alias),
            relationship_join
        )
        .map_err(|_| CoreError::internal("failed to render graph SQL"))?;
        let unknown_join = if pattern.direction == Direction::Undirected
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

    fn join_condition_with_predicate(
        condition: String,
        optional_predicate: Option<&str>,
    ) -> String {
        match optional_predicate {
            Some(predicate) => format!("({condition}) AND ({predicate})"),
            None => condition,
        }
    }

    fn relationship_pair_condition(
        validated: &ValidatedGraphPlan<'a>,
        relationship: &Relationship,
        relationship_alias: &str,
        pattern: &'a super::ir::RelationshipPattern,
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

    fn relationship_known_node_condition(
        validated: &ValidatedGraphPlan<'a>,
        relationship: &Relationship,
        pattern: &'a super::ir::RelationshipPattern,
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

    fn relationship_orientations(
        validated: &ValidatedGraphPlan<'a>,
        relationship: &Relationship,
        pattern: &'a super::ir::RelationshipPattern,
    ) -> Result<Vec<RelationshipOrientation>, CoreError> {
        match pattern.direction {
            Direction::Outgoing => Ok(vec![RelationshipOrientation {
                left_relationship_key: relationship.from.key.clone(),
                right_relationship_key: relationship.to.key.clone(),
            }]),
            Direction::Incoming => Ok(vec![RelationshipOrientation {
                left_relationship_key: relationship.to.key.clone(),
                right_relationship_key: relationship.from.key.clone(),
            }]),
            Direction::Undirected => {
                let left_node = validated.node_binding(&pattern.left)?;
                let right_node = validated.node_binding(&pattern.right)?;
                let mut orientations = Vec::with_capacity(2);
                if left_node.label == relationship.from.label
                    && right_node.label == relationship.to.label
                {
                    orientations.push(RelationshipOrientation {
                        left_relationship_key: relationship.from.key.clone(),
                        right_relationship_key: relationship.to.key.clone(),
                    });
                }
                if left_node.label == relationship.to.label
                    && right_node.label == relationship.from.label
                {
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

    fn render_condition_disjunction(conditions: &[String]) -> Result<String, CoreError> {
        match conditions {
            [] => Err(CoreError::internal(
                "relationship join had no endpoint condition",
            )),
            [condition] => Ok(condition.clone()),
            _ => Ok(format!("({})", conditions.join(" OR "))),
        }
    }

    fn render_select(&self) -> Result<String, CoreError> {
        let mut rendered = Vec::with_capacity(self.validated.plan().projections.len());
        for projection in &self.validated.plan().projections {
            rendered.push(self.render_projection_select_item(projection)?);
        }
        Ok(format!(
            "SELECT {}{}",
            if self.validated.plan().distinct {
                "DISTINCT "
            } else {
                ""
            },
            rendered.join(", ")
        ))
    }

    fn render_projection_select_item(&self, projection: &Projection) -> Result<String, CoreError> {
        match projection {
            Projection::Property { property, alias } => {
                let expression = self.render_property_ref(property)?;
                let alias = alias
                    .clone()
                    .unwrap_or_else(|| format!("{}_{}", property.variable, property.property));
                Ok(format!("{expression} AS {}", quote_ident(&alias)))
            }
            Projection::Key { variable, alias } => Ok(format!(
                "{} AS {}",
                self.render_binding_key_ref(variable)?,
                quote_ident(alias)
            )),
            Projection::ElementId { variable, alias } => Ok(format!(
                "{} AS {}",
                self.render_binding_element_id_ref(variable)?,
                quote_ident(alias)
            )),
            Projection::NodeLabels {
                variable,
                label,
                alias,
            } => Ok(format!(
                "{} AS {}",
                self.render_node_labels_ref(variable, label)?,
                quote_ident(alias)
            )),
            Projection::PropertyKeys { variable, alias } => Ok(format!(
                "{} AS {}",
                self.render_property_keys_ref(variable)?,
                quote_ident(alias)
            )),
            Projection::RelationshipType {
                variable,
                relationship_type,
                alias,
            } => Ok(format!(
                "{} AS {}",
                self.render_relationship_type_ref(variable, relationship_type)?,
                quote_ident(alias)
            )),
            Projection::Literal { literal, alias } => Ok(format!(
                "{} AS {}",
                render_literal(literal),
                quote_ident(alias)
            )),
            Projection::LiteralList { literals, alias } => Ok(format!(
                "{} AS {}",
                render_literal_list(literals),
                quote_ident(alias)
            )),
            Projection::Expression { expression, alias } => Ok(format!(
                "{} AS {}",
                self.render_scalar_expression(expression)?,
                quote_ident(alias)
            )),
            Projection::CountAll { alias } => Ok(format!("COUNT(*) AS {}", quote_ident(alias))),
            Projection::Aggregate {
                function,
                target,
                distinct,
                alias,
            } => Ok(format!(
                "{}({}{}) AS {}",
                render_aggregate_function(*function),
                if *distinct { "DISTINCT " } else { "" },
                self.render_aggregate_target(*function, target)?,
                quote_ident(alias)
            )),
        }
    }

    fn render_where(&self) -> Result<String, CoreError> {
        let mut predicates = self.render_pre_projection_predicates()?;
        if !self.plan_has_aggregate_projection()
            && let Some(predicate) = &self.validated.plan().post_projection_predicate
        {
            predicates.push(self.render_projection_predicate_expression(predicate)?);
        }
        if predicates.is_empty() {
            return Ok(String::new());
        }
        Ok(format!(" WHERE {}", predicates.join(" AND ")))
    }

    fn render_pre_projection_predicates(&self) -> Result<Vec<String>, CoreError> {
        let mut predicates = Vec::with_capacity(
            self.validated.plan().predicates.len()
                + usize::from(self.validated.plan().predicate.is_some()),
        );
        for predicate in &self.validated.plan().predicates {
            predicates.push(self.render_predicate(predicate)?);
        }
        if let Some(predicate) = &self.validated.plan().predicate {
            predicates.push(self.render_predicate_expression(predicate)?);
        }
        Ok(predicates)
    }

    fn render_having(&self) -> Result<String, CoreError> {
        if !self.plan_has_aggregate_projection() {
            return Ok(String::new());
        }
        let Some(predicate) = &self.validated.plan().post_projection_predicate else {
            return Ok(String::new());
        };
        Ok(format!(
            " HAVING {}",
            self.render_projection_predicate_expression(predicate)?
        ))
    }

    fn plan_has_aggregate_projection(&self) -> bool {
        self.validated
            .plan()
            .projections
            .iter()
            .any(Projection::is_aggregate)
    }

    fn render_group_by(&self) -> Result<String, CoreError> {
        if !self.plan_has_aggregate_projection() {
            return Ok(String::new());
        }

        let expressions = self.render_group_by_expressions()?;
        if expressions.is_empty() {
            Ok(String::new())
        } else {
            Ok(format!(" GROUP BY {}", expressions.join(", ")))
        }
    }

    fn render_group_by_expressions(&self) -> Result<Vec<String>, CoreError> {
        let mut expressions = Vec::new();
        for projection in &self.validated.plan().projections {
            match projection {
                Projection::Property { property, .. } => {
                    expressions.push(self.render_property_ref(property)?);
                }
                Projection::Key { variable, .. } => {
                    expressions.push(self.render_binding_key_ref(variable)?);
                }
                Projection::ElementId { variable, .. } => {
                    expressions.push(self.render_binding_element_id_ref(variable)?);
                }
                Projection::RelationshipType {
                    variable,
                    relationship_type,
                    ..
                } => {
                    expressions
                        .push(self.render_relationship_type_ref(variable, relationship_type)?);
                }
                Projection::NodeLabels {
                    variable, label, ..
                } => {
                    expressions.push(self.render_node_labels_ref(variable, label)?);
                }
                Projection::PropertyKeys { variable, .. } => {
                    expressions.push(self.render_property_keys_ref(variable)?);
                }
                Projection::Expression { expression, .. } => {
                    expressions.push(self.render_scalar_expression(expression)?);
                }
                Projection::Literal { .. }
                | Projection::LiteralList { .. }
                | Projection::CountAll { .. }
                | Projection::Aggregate { .. } => {}
            }
        }
        Ok(expressions)
    }

    fn render_predicate_expression(
        &self,
        predicate: &PredicateExpression,
    ) -> Result<String, CoreError> {
        match predicate {
            PredicateExpression::Boolean(value) => Ok(value.to_string().to_uppercase()),
            PredicateExpression::Comparison(predicate) => self.render_predicate(predicate),
            PredicateExpression::KeyComparison(predicate) => self.render_key_predicate(predicate),
            PredicateExpression::ElementIdComparison(predicate) => {
                self.render_element_id_predicate(predicate)
            }
            PredicateExpression::Presence(predicate) => self.render_presence_predicate(predicate),
            PredicateExpression::PropertyKeyMembership(predicate) => {
                self.render_property_key_membership_predicate(predicate)
            }
            PredicateExpression::ScalarComparison(predicate) => {
                self.render_scalar_predicate(predicate)
            }
            PredicateExpression::And { left, right } => Ok(format!(
                "({} AND {})",
                self.render_predicate_expression(left)?,
                self.render_predicate_expression(right)?
            )),
            PredicateExpression::Or { left, right } => Ok(format!(
                "({} OR {})",
                self.render_predicate_expression(left)?,
                self.render_predicate_expression(right)?
            )),
            PredicateExpression::Xor { left, right } => {
                let left = self.render_predicate_expression(left)?;
                let right = self.render_predicate_expression(right)?;
                Ok(render_xor_predicate(&left, &right))
            }
            PredicateExpression::Not { expression } => Ok(format!(
                "NOT ({})",
                self.render_predicate_expression(expression)?
            )),
        }
    }

    fn render_projection_predicate_expression(
        &self,
        predicate: &ProjectionPredicateExpression,
    ) -> Result<String, CoreError> {
        match predicate {
            ProjectionPredicateExpression::Boolean(value) => Ok(value.to_string().to_uppercase()),
            ProjectionPredicateExpression::Comparison(predicate) => {
                self.render_projection_predicate(predicate)
            }
            ProjectionPredicateExpression::And { left, right } => Ok(format!(
                "({} AND {})",
                self.render_projection_predicate_expression(left)?,
                self.render_projection_predicate_expression(right)?
            )),
            ProjectionPredicateExpression::Or { left, right } => Ok(format!(
                "({} OR {})",
                self.render_projection_predicate_expression(left)?,
                self.render_projection_predicate_expression(right)?
            )),
            ProjectionPredicateExpression::Xor { left, right } => {
                let left = self.render_projection_predicate_expression(left)?;
                let right = self.render_projection_predicate_expression(right)?;
                Ok(render_xor_predicate(&left, &right))
            }
            ProjectionPredicateExpression::Not { expression } => Ok(format!(
                "NOT ({})",
                self.render_projection_predicate_expression(expression)?
            )),
        }
    }

    fn render_projection_predicate(
        &self,
        predicate: &ProjectionPredicate,
    ) -> Result<String, CoreError> {
        let alias = self.render_projection_alias_ref(&predicate.alias)?;
        match (&predicate.operator, &predicate.rhs) {
            (ComparisonOperator::In, ProjectionPredicateRhs::List(literals)) => {
                if literals.is_empty() {
                    return Ok("FALSE".to_string());
                }
                let rendered = literals
                    .iter()
                    .map(render_literal)
                    .collect::<Vec<_>>()
                    .join(", ");
                Ok(format!("{alias} IN ({rendered})"))
            }
            (ComparisonOperator::In, _) => Err(CoreError::internal(
                "validated projected IN predicate did not contain a literal list",
            )),
            (
                ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains,
                ProjectionPredicateRhs::Literal(Literal::String(value)),
            ) => Ok(format!(
                "{alias} LIKE {} ESCAPE '\\'",
                render_like_pattern(predicate.operator, value)
            )),
            (
                ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains,
                _,
            ) => Err(CoreError::internal(
                "validated projected string predicate did not contain a string literal",
            )),
            (
                ComparisonOperator::RegexMatch,
                ProjectionPredicateRhs::Literal(Literal::String(value)),
            ) => Ok(render_regex_predicate(&alias, &quote_string_literal(value))),
            (ComparisonOperator::RegexMatch, _) => Err(CoreError::internal(
                "validated projected regex predicate did not contain a string literal",
            )),
            (ComparisonOperator::Equal, ProjectionPredicateRhs::Literal(Literal::Null)) => {
                Ok(format!("{alias} IS NULL"))
            }
            (ComparisonOperator::NotEqual, ProjectionPredicateRhs::Literal(Literal::Null)) => {
                Ok(format!("{alias} IS NOT NULL"))
            }
            (
                ComparisonOperator::GreaterThan
                | ComparisonOperator::GreaterThanOrEqual
                | ComparisonOperator::LessThan
                | ComparisonOperator::LessThanOrEqual,
                ProjectionPredicateRhs::Literal(Literal::Null),
            ) => Err(CoreError::internal(
                "validated projected predicate contained an invalid null comparison",
            )),
            _ => Ok(format!(
                "{alias} {} {}",
                render_operator(predicate.operator),
                self.render_projection_predicate_rhs(&predicate.rhs)?
            )),
        }
    }

    fn render_predicate(
        &self,
        predicate: &super::ir::PropertyPredicate,
    ) -> Result<String, CoreError> {
        let property = self.render_property_ref(&predicate.property)?;
        match (&predicate.operator, &predicate.rhs) {
            (ComparisonOperator::In, PredicateRhs::List(literals)) => {
                if literals.is_empty() {
                    return Ok("FALSE".to_string());
                }
                let rendered = literals
                    .iter()
                    .map(render_literal)
                    .collect::<Vec<_>>()
                    .join(", ");
                Ok(format!("{property} IN ({rendered})"))
            }
            (ComparisonOperator::In, _) => Err(CoreError::internal(
                "validated IN predicate did not contain a literal list",
            )),
            (
                ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains,
                PredicateRhs::Literal(Literal::String(value)),
            ) => Ok(format!(
                "{property} LIKE {} ESCAPE '\\'",
                render_like_pattern(predicate.operator, value)
            )),
            (
                ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains,
                _,
            ) => Err(CoreError::internal(
                "validated string predicate did not contain a string literal",
            )),
            (ComparisonOperator::RegexMatch, PredicateRhs::List(_)) => Err(CoreError::internal(
                "validated regex predicate did not contain a scalar RHS",
            )),
            (ComparisonOperator::RegexMatch, rhs) => Ok(render_regex_predicate(
                &property,
                &self.render_predicate_rhs(rhs)?,
            )),
            (ComparisonOperator::Equal, PredicateRhs::Literal(Literal::Null)) => {
                Ok(format!("{property} IS NULL"))
            }
            (ComparisonOperator::NotEqual, PredicateRhs::Literal(Literal::Null)) => {
                Ok(format!("{property} IS NOT NULL"))
            }
            (
                ComparisonOperator::GreaterThan
                | ComparisonOperator::GreaterThanOrEqual
                | ComparisonOperator::LessThan
                | ComparisonOperator::LessThanOrEqual,
                PredicateRhs::Literal(Literal::Null),
            ) => Err(CoreError::internal(
                "validated graph predicate contained an invalid null comparison",
            )),
            _ => Ok(format!(
                "{property} {} {}",
                render_operator(predicate.operator),
                self.render_predicate_rhs(&predicate.rhs)?
            )),
        }
    }

    fn render_scalar_predicate(&self, predicate: &ScalarPredicate) -> Result<String, CoreError> {
        let lhs = self.render_scalar_expression(&predicate.lhs)?;
        match (&predicate.operator, &predicate.rhs) {
            (ComparisonOperator::In, ScalarPredicateRhs::List(literals)) => {
                if literals.is_empty() {
                    return Ok("FALSE".to_string());
                }
                let rendered = literals
                    .iter()
                    .map(render_literal)
                    .collect::<Vec<_>>()
                    .join(", ");
                Ok(format!("{lhs} IN ({rendered})"))
            }
            (ComparisonOperator::In, _) => Err(CoreError::internal(
                "validated scalar IN predicate did not contain a literal list",
            )),
            (
                ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains,
                ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(value))),
            ) => Ok(format!(
                "{lhs} LIKE {} ESCAPE '\\'",
                render_like_pattern(predicate.operator, value)
            )),
            (
                ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains,
                ScalarPredicateRhs::Expression(expression),
            ) => {
                let rhs = self.render_scalar_expression(expression)?;
                Ok(render_string_function_predicate(
                    predicate.operator,
                    &lhs,
                    &rhs,
                ))
            }
            (
                ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains,
                _,
            ) => Err(CoreError::internal(
                "validated scalar string predicate did not contain a string literal",
            )),
            (ComparisonOperator::RegexMatch, ScalarPredicateRhs::List(_)) => {
                Err(CoreError::internal(
                    "validated scalar regex predicate did not contain a scalar RHS",
                ))
            }
            (ComparisonOperator::RegexMatch, ScalarPredicateRhs::Expression(expression)) => {
                let rhs = self.render_scalar_expression(expression)?;
                Ok(render_regex_predicate(&lhs, &rhs))
            }
            (
                ComparisonOperator::Equal,
                ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Null)),
            ) => Ok(format!("{lhs} IS NULL")),
            (
                ComparisonOperator::NotEqual,
                ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Null)),
            ) => Ok(format!("{lhs} IS NOT NULL")),
            (
                ComparisonOperator::GreaterThan
                | ComparisonOperator::GreaterThanOrEqual
                | ComparisonOperator::LessThan
                | ComparisonOperator::LessThanOrEqual,
                ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Null)),
            ) => Err(CoreError::internal(
                "validated scalar predicate contained an invalid null comparison",
            )),
            _ => Ok(format!(
                "{lhs} {} {}",
                render_operator(predicate.operator),
                self.render_scalar_predicate_rhs(&predicate.rhs)?
            )),
        }
    }

    fn render_key_predicate(&self, predicate: &KeyPredicate) -> Result<String, CoreError> {
        let key = self.render_binding_key_ref(&predicate.variable)?;
        match (&predicate.operator, &predicate.rhs) {
            (ComparisonOperator::In, PredicateRhs::List(literals)) => {
                if literals.is_empty() {
                    return Ok("FALSE".to_string());
                }
                let rendered = literals
                    .iter()
                    .map(render_literal)
                    .collect::<Vec<_>>()
                    .join(", ");
                Ok(format!("{key} IN ({rendered})"))
            }
            (ComparisonOperator::In, _) => Err(CoreError::internal(
                "validated id() IN predicate did not contain a literal list",
            )),
            (
                ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains,
                PredicateRhs::Literal(Literal::String(value)),
            ) => Ok(format!(
                "{key} LIKE {} ESCAPE '\\'",
                render_like_pattern(predicate.operator, value)
            )),
            (
                ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains,
                _,
            ) => Err(CoreError::internal(
                "validated id() string predicate did not contain a string literal",
            )),
            (ComparisonOperator::RegexMatch, PredicateRhs::List(_)) => Err(CoreError::internal(
                "validated id() regex predicate did not contain a scalar RHS",
            )),
            (ComparisonOperator::RegexMatch, rhs) => Ok(render_regex_predicate(
                &key,
                &self.render_predicate_rhs(rhs)?,
            )),
            (ComparisonOperator::Equal, PredicateRhs::Literal(Literal::Null)) => {
                Ok(format!("{key} IS NULL"))
            }
            (ComparisonOperator::NotEqual, PredicateRhs::Literal(Literal::Null)) => {
                Ok(format!("{key} IS NOT NULL"))
            }
            (
                ComparisonOperator::GreaterThan
                | ComparisonOperator::GreaterThanOrEqual
                | ComparisonOperator::LessThan
                | ComparisonOperator::LessThanOrEqual,
                PredicateRhs::Literal(Literal::Null),
            ) => Err(CoreError::internal(
                "validated id() predicate contained an invalid null comparison",
            )),
            _ => Ok(format!(
                "{key} {} {}",
                render_operator(predicate.operator),
                self.render_predicate_rhs(&predicate.rhs)?
            )),
        }
    }

    fn render_element_id_predicate(
        &self,
        predicate: &ElementIdPredicate,
    ) -> Result<String, CoreError> {
        let element_id = self.render_binding_element_id_ref(&predicate.variable)?;
        match (&predicate.operator, &predicate.rhs) {
            (ComparisonOperator::In, PredicateRhs::List(literals)) => {
                if literals.is_empty() {
                    return Ok("FALSE".to_string());
                }
                let rendered = literals
                    .iter()
                    .map(render_literal)
                    .collect::<Vec<_>>()
                    .join(", ");
                Ok(format!("{element_id} IN ({rendered})"))
            }
            (ComparisonOperator::In, _) => Err(CoreError::internal(
                "validated elementId() IN predicate did not contain a literal list",
            )),
            (
                ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains,
                PredicateRhs::Literal(Literal::String(value)),
            ) => Ok(format!(
                "{element_id} LIKE {} ESCAPE '\\'",
                render_like_pattern(predicate.operator, value)
            )),
            (
                ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains,
                _,
            ) => Err(CoreError::internal(
                "validated elementId() string predicate did not contain a string literal",
            )),
            (ComparisonOperator::RegexMatch, PredicateRhs::List(_)) => Err(CoreError::internal(
                "validated elementId() regex predicate did not contain a scalar RHS",
            )),
            (ComparisonOperator::RegexMatch, rhs) => Ok(render_regex_predicate(
                &element_id,
                &self.render_predicate_rhs(rhs)?,
            )),
            (ComparisonOperator::Equal, PredicateRhs::Literal(Literal::Null)) => {
                Ok(format!("{element_id} IS NULL"))
            }
            (ComparisonOperator::NotEqual, PredicateRhs::Literal(Literal::Null)) => {
                Ok(format!("{element_id} IS NOT NULL"))
            }
            (
                ComparisonOperator::GreaterThan
                | ComparisonOperator::GreaterThanOrEqual
                | ComparisonOperator::LessThan
                | ComparisonOperator::LessThanOrEqual,
                PredicateRhs::Literal(Literal::Null),
            ) => Err(CoreError::internal(
                "validated elementId() predicate contained an invalid null comparison",
            )),
            _ => Ok(format!(
                "{element_id} {} {}",
                render_operator(predicate.operator),
                self.render_predicate_rhs(&predicate.rhs)?
            )),
        }
    }

    fn render_presence_predicate(
        &self,
        predicate: &PresencePredicate,
    ) -> Result<String, CoreError> {
        let presence = self.render_binding_presence_ref(&predicate.variable)?;
        match predicate.operator {
            ComparisonOperator::Equal => Ok(format!("{presence} IS NULL")),
            ComparisonOperator::NotEqual => Ok(format!("{presence} IS NOT NULL")),
            ComparisonOperator::GreaterThan
            | ComparisonOperator::GreaterThanOrEqual
            | ComparisonOperator::LessThan
            | ComparisonOperator::LessThanOrEqual
            | ComparisonOperator::In
            | ComparisonOperator::StartsWith
            | ComparisonOperator::EndsWith
            | ComparisonOperator::Contains
            | ComparisonOperator::RegexMatch => Err(CoreError::internal(
                "validated presence predicate contained an invalid operator",
            )),
        }
    }

    fn render_property_key_membership_predicate(
        &self,
        predicate: &PropertyKeyMembershipPredicate,
    ) -> Result<String, CoreError> {
        let binding = self.validated.binding(&predicate.variable)?;
        let has_key = match binding.kind() {
            ValidatedBindingKind::Node(node) => node.properties.contains_key(&predicate.key),
            ValidatedBindingKind::Relationship(relationship) => {
                relationship.properties.contains_key(&predicate.key)
            }
        };
        let presence = self.render_binding_presence_ref(&predicate.variable)?;
        let value = if has_key { "TRUE" } else { "FALSE" };
        Ok(format!(
            "CASE WHEN {presence} IS NULL THEN NULL ELSE {value} END"
        ))
    }

    fn render_projection_predicate_rhs(
        &self,
        rhs: &ProjectionPredicateRhs,
    ) -> Result<String, CoreError> {
        match rhs {
            ProjectionPredicateRhs::Literal(literal) => Ok(render_literal(literal)),
            ProjectionPredicateRhs::Alias(alias) => self.render_projection_alias_ref(alias),
            ProjectionPredicateRhs::List(_) => Err(CoreError::internal(
                "validated projected literal list predicate reached generic RHS renderer",
            )),
        }
    }

    fn render_predicate_rhs(&self, rhs: &PredicateRhs) -> Result<String, CoreError> {
        match rhs {
            PredicateRhs::Literal(literal) => Ok(render_literal(literal)),
            PredicateRhs::Property(property) => self.render_property_ref(property),
            PredicateRhs::Key { variable } => self.render_binding_key_ref(variable),
            PredicateRhs::ElementId { variable } => self.render_binding_element_id_ref(variable),
            PredicateRhs::List(_) => Err(CoreError::internal(
                "validated literal list predicate reached generic RHS renderer",
            )),
        }
    }

    fn render_scalar_predicate_rhs(&self, rhs: &ScalarPredicateRhs) -> Result<String, CoreError> {
        match rhs {
            ScalarPredicateRhs::Expression(expression) => self.render_scalar_expression(expression),
            ScalarPredicateRhs::List(_) => Err(CoreError::internal(
                "validated scalar literal list predicate reached generic RHS renderer",
            )),
        }
    }

    fn render_order_by(&self) -> Result<String, CoreError> {
        if self.validated.plan().order_by.is_empty() {
            return Ok(String::new());
        }

        let mut keys = Vec::with_capacity(self.validated.plan().order_by.len());
        for key in &self.validated.plan().order_by {
            keys.push(format!(
                "{} {}",
                self.render_order_expression(&key.expression)?,
                match key.direction {
                    OrderDirection::Ascending => "ASC",
                    OrderDirection::Descending => "DESC",
                }
            ));
        }
        Ok(format!(" ORDER BY {}", keys.join(", ")))
    }

    fn render_order_expression(&self, expression: &OrderExpression) -> Result<String, CoreError> {
        match expression {
            OrderExpression::Property(property) => self.render_property_ref(property),
            OrderExpression::Key { variable } => self.render_binding_key_ref(variable),
            OrderExpression::ElementId { variable } => self.render_binding_element_id_ref(variable),
            OrderExpression::NodeLabels { variable, label } => {
                self.render_node_labels_ref(variable, label)
            }
            OrderExpression::PropertyKeys { variable } => self.render_property_keys_ref(variable),
            OrderExpression::RelationshipType {
                variable,
                relationship_type,
            } => self.render_relationship_type_ref(variable, relationship_type),
            OrderExpression::Scalar(expression) => self.render_scalar_expression(expression),
            OrderExpression::Literal(literal) => Ok(render_literal(literal)),
            OrderExpression::ProjectionAlias(alias) => Ok(quote_ident(alias)),
        }
    }

    fn render_aggregate_target(
        &self,
        function: AggregateFunction,
        target: &AggregateTarget,
    ) -> Result<String, CoreError> {
        match target {
            AggregateTarget::Property(property) => self.render_property_ref(property),
            AggregateTarget::VariableKey { variable } => {
                if function == AggregateFunction::Count {
                    self.render_binding_presence_ref(variable)
                } else {
                    self.render_binding_key_ref(variable)
                }
            }
        }
    }

    fn render_binding_presence_ref(&self, variable: &str) -> Result<String, CoreError> {
        let binding = self.validated.binding(variable)?;
        let column = match binding.kind() {
            ValidatedBindingKind::Node(node) => node.key.as_str(),
            ValidatedBindingKind::Relationship(relationship) => relationship
                .key
                .as_deref()
                .unwrap_or(&relationship.from.key),
        };
        Ok(format!(
            "{}.{}",
            quote_ident(binding.alias()),
            quote_ident(column)
        ))
    }

    fn render_relationship_type_ref(
        &self,
        variable: &str,
        relationship_type: &str,
    ) -> Result<String, CoreError> {
        let presence = self.render_relationship_presence_ref(variable)?;
        Ok(format!(
            "CASE WHEN {presence} IS NULL THEN NULL ELSE {} END",
            quote_string_literal(relationship_type)
        ))
    }

    fn render_node_labels_ref(&self, variable: &str, label: &str) -> Result<String, CoreError> {
        let binding = self.validated.binding(variable)?;
        let ValidatedBindingKind::Node(node) = binding.kind() else {
            return Err(CoreError::internal(
                "validated labels expression did not reference a node",
            ));
        };
        if node.label != label {
            return Err(CoreError::internal(
                "validated labels expression did not match the node label",
            ));
        }
        let presence = self.render_binding_presence_ref(variable)?;
        Ok(format!(
            "CASE WHEN {presence} IS NULL THEN NULL ELSE make_array({}) END",
            quote_string_literal(label)
        ))
    }

    fn render_property_keys_ref(&self, variable: &str) -> Result<String, CoreError> {
        let binding = self.validated.binding(variable)?;
        let property_names = match binding.kind() {
            ValidatedBindingKind::Node(node) => node.properties.keys(),
            ValidatedBindingKind::Relationship(relationship) => relationship.properties.keys(),
        }
        .map(|property| quote_string_literal(property))
        .collect::<Vec<_>>()
        .join(", ");
        let presence = self.render_binding_presence_ref(variable)?;
        Ok(format!(
            "CASE WHEN {presence} IS NULL THEN NULL ELSE make_array({property_names}) END"
        ))
    }

    fn render_relationship_presence_ref(&self, variable: &str) -> Result<String, CoreError> {
        let binding = self.validated.binding(variable)?;
        let ValidatedBindingKind::Relationship(relationship) = binding.kind() else {
            return Err(CoreError::internal(
                "validated relationship type expression did not reference a relationship",
            ));
        };
        let column = relationship
            .key
            .as_deref()
            .unwrap_or(&relationship.from.key);
        Ok(format!(
            "{}.{}",
            quote_ident(binding.alias()),
            quote_ident(column)
        ))
    }

    fn render_projection_alias_ref(&self, alias: &str) -> Result<String, CoreError> {
        let projection = self
            .validated
            .plan()
            .projections
            .iter()
            .find(|projection| projection_output_alias(projection) == Some(alias))
            .ok_or_else(|| {
                CoreError::internal(format!(
                    "validated projected predicate referenced unknown alias '{alias}'"
                ))
            })?;
        match projection {
            Projection::Property { property, .. } => self.render_property_ref(property),
            Projection::Key { variable, .. } => self.render_binding_key_ref(variable),
            Projection::ElementId { variable, .. } => self.render_binding_element_id_ref(variable),
            Projection::NodeLabels {
                variable, label, ..
            } => self.render_node_labels_ref(variable, label),
            Projection::PropertyKeys { variable, .. } => self.render_property_keys_ref(variable),
            Projection::RelationshipType {
                variable,
                relationship_type,
                ..
            } => self.render_relationship_type_ref(variable, relationship_type),
            Projection::Literal { literal, .. } => Ok(render_literal(literal)),
            Projection::LiteralList { literals, .. } => Ok(render_literal_list(literals)),
            Projection::Expression { expression, .. } => self.render_scalar_expression(expression),
            Projection::CountAll { .. } => Ok("COUNT(*)".to_string()),
            Projection::Aggregate {
                function,
                target,
                distinct,
                ..
            } => Ok(format!(
                "{}({}{})",
                render_aggregate_function(*function),
                if *distinct { "DISTINCT " } else { "" },
                self.render_aggregate_target(*function, target)?
            )),
        }
    }

    fn render_binding_key_ref(&self, variable: &str) -> Result<String, CoreError> {
        let binding = self.validated.binding(variable)?;
        let key = match binding.kind() {
            ValidatedBindingKind::Node(node) => node.key.as_str(),
            ValidatedBindingKind::Relationship(relationship) => {
                relationship.key.as_deref().ok_or_else(|| {
                    CoreError::internal(
                        "validated aggregate relationship target did not have a key",
                    )
                })?
            }
        };
        Ok(format!(
            "{}.{}",
            quote_ident(binding.alias()),
            quote_ident(key)
        ))
    }

    fn render_binding_element_id_ref(&self, variable: &str) -> Result<String, CoreError> {
        Ok(format!(
            "CAST({} AS VARCHAR)",
            self.render_binding_key_ref(variable)?
        ))
    }

    fn render_property_ref(&self, property: &PropertyRef) -> Result<String, CoreError> {
        let binding = self.validated.binding(&property.variable)?;
        let column = match binding.kind() {
            ValidatedBindingKind::Node(node) => node.column_for_property(&property.property),
            ValidatedBindingKind::Relationship(relationship) => {
                relationship.column_for_property(&property.property)
            }
        }
        .ok_or_else(|| {
            CoreError::internal("validated graph property reference was not resolvable")
        })?;

        Ok(format!(
            "{}.{}",
            quote_ident(binding.alias()),
            quote_ident(column)
        ))
    }

    fn render_scalar_expression(&self, expression: &ScalarExpression) -> Result<String, CoreError> {
        if let Some(rendered) = self.render_simple_scalar_expression(expression)? {
            return Ok(rendered);
        }

        match expression {
            ScalarExpression::Property(property) => self.render_property_ref(property),
            ScalarExpression::Literal(literal) => Ok(render_literal(literal)),
            ScalarExpression::Predicate(predicate) => self.render_predicate_expression(predicate),
            ScalarExpression::RelationshipType {
                variable,
                relationship_type,
            } => self.render_relationship_type_ref(variable, relationship_type),
            ScalarExpression::Coalesce { expressions } => {
                let rendered = expressions
                    .iter()
                    .map(|expression| self.render_scalar_expression(expression))
                    .collect::<Result<Vec<_>, _>>()?
                    .join(", ");
                Ok(format!("COALESCE({rendered})"))
            }
            ScalarExpression::NullIf { expression, value } => Ok(format!(
                "NULLIF({}, {})",
                self.render_scalar_expression(expression)?,
                self.render_scalar_expression(value)?
            )),
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
            | ScalarExpression::Left { .. }
            | ScalarExpression::Right { .. }
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
            | ScalarExpression::Atan2 { .. }
            | ScalarExpression::Degrees { .. }
            | ScalarExpression::Radians { .. }
            | ScalarExpression::Negate { .. } => {
                unreachable!("simple scalar expressions handled above")
            }
            ScalarExpression::Replace {
                expression,
                search,
                replacement,
            } => self.render_replace_expression(expression, search, replacement),
            ScalarExpression::Substring {
                expression,
                start,
                length,
            } => self.render_substring_expression(expression, start, length.as_deref()),
            ScalarExpression::Round { expression, places } => {
                self.render_round_expression(expression, places.as_deref())
            }
            ScalarExpression::Arithmetic {
                operator,
                left,
                right,
            } => self.render_arithmetic_expression(*operator, left, right),
            ScalarExpression::Case {
                alternatives,
                else_expression,
            } => self.render_case_expression(alternatives, else_expression.as_deref()),
        }
    }

    fn render_simple_scalar_expression(
        &self,
        expression: &ScalarExpression,
    ) -> Result<Option<String>, CoreError> {
        if let Some(rendered) = self.render_scalar_cast_expression(expression)? {
            return Ok(Some(rendered));
        }

        match expression {
            ScalarExpression::ToLower { expression } => self
                .render_unary_function_expression("LOWER", expression)
                .map(Some),
            ScalarExpression::ToUpper { expression } => self
                .render_unary_function_expression("UPPER", expression)
                .map(Some),
            ScalarExpression::Trim { expression } => self
                .render_unary_function_expression("TRIM", expression)
                .map(Some),
            ScalarExpression::LTrim { expression } => self
                .render_unary_function_expression("LTRIM", expression)
                .map(Some),
            ScalarExpression::RTrim { expression } => self
                .render_unary_function_expression("RTRIM", expression)
                .map(Some),
            ScalarExpression::CharacterLength { expression } => self
                .render_unary_function_expression("character_length", expression)
                .map(Some),
            ScalarExpression::Left { expression, count } => self
                .render_binary_function_expression("left", expression, count)
                .map(Some),
            ScalarExpression::Right { expression, count } => self
                .render_binary_function_expression("right", expression, count)
                .map(Some),
            ScalarExpression::Reverse { expression } => self
                .render_unary_function_expression("reverse", expression)
                .map(Some),
            ScalarExpression::Abs { expression } => self
                .render_unary_function_expression("abs", expression)
                .map(Some),
            ScalarExpression::Ceil { expression } => self
                .render_unary_function_expression("ceil", expression)
                .map(Some),
            ScalarExpression::Floor { expression } => self
                .render_unary_function_expression("floor", expression)
                .map(Some),
            ScalarExpression::Sqrt { expression } => self
                .render_unary_function_expression("sqrt", expression)
                .map(Some),
            ScalarExpression::Sign { expression } => self
                .render_unary_function_expression("signum", expression)
                .map(Some),
            ScalarExpression::Exp { expression } => self
                .render_unary_function_expression("exp", expression)
                .map(Some),
            ScalarExpression::Log { expression } => self
                .render_unary_function_expression("ln", expression)
                .map(Some),
            ScalarExpression::Log10 { expression } => self
                .render_unary_function_expression("log10", expression)
                .map(Some),
            ScalarExpression::Sin { expression } => self
                .render_unary_function_expression("sin", expression)
                .map(Some),
            ScalarExpression::Cos { expression } => self
                .render_unary_function_expression("cos", expression)
                .map(Some),
            ScalarExpression::Tan { expression } => self
                .render_unary_function_expression("tan", expression)
                .map(Some),
            ScalarExpression::Cot { expression } => self
                .render_unary_function_expression("cot", expression)
                .map(Some),
            ScalarExpression::Asin { expression } => self
                .render_unary_function_expression("asin", expression)
                .map(Some),
            ScalarExpression::Acos { expression } => self
                .render_unary_function_expression("acos", expression)
                .map(Some),
            ScalarExpression::Atan { expression } => self
                .render_unary_function_expression("atan", expression)
                .map(Some),
            ScalarExpression::Atan2 { y, x } => self
                .render_binary_function_expression("atan2", y, x)
                .map(Some),
            ScalarExpression::Degrees { expression } => self
                .render_unary_function_expression("degrees", expression)
                .map(Some),
            ScalarExpression::Radians { expression } => self
                .render_unary_function_expression("radians", expression)
                .map(Some),
            ScalarExpression::Negate { expression } => Ok(Some(format!(
                "-({})",
                self.render_scalar_expression(expression)?
            ))),
            _ => Ok(None),
        }
    }

    fn render_scalar_cast_expression(
        &self,
        expression: &ScalarExpression,
    ) -> Result<Option<String>, CoreError> {
        match expression {
            ScalarExpression::ToString { expression } => {
                self.render_cast_expression(expression, "VARCHAR").map(Some)
            }
            ScalarExpression::ToInteger { expression } => {
                self.render_cast_expression(expression, "BIGINT").map(Some)
            }
            ScalarExpression::ToFloat { expression } => {
                self.render_cast_expression(expression, "DOUBLE").map(Some)
            }
            ScalarExpression::ToBoolean { expression } => {
                self.render_cast_expression(expression, "BOOLEAN").map(Some)
            }
            ScalarExpression::ToStringOrNull { expression } => self
                .render_try_cast_expression(expression, "VARCHAR")
                .map(Some),
            ScalarExpression::ToIntegerOrNull { expression } => self
                .render_try_cast_expression(expression, "BIGINT")
                .map(Some),
            ScalarExpression::ToFloatOrNull { expression } => self
                .render_try_cast_expression(expression, "DOUBLE")
                .map(Some),
            ScalarExpression::ToBooleanOrNull { expression } => self
                .render_try_cast_expression(expression, "BOOLEAN")
                .map(Some),
            _ => Ok(None),
        }
    }

    fn render_cast_expression(
        &self,
        expression: &ScalarExpression,
        target_type: &str,
    ) -> Result<String, CoreError> {
        Ok(format!(
            "CAST({} AS {target_type})",
            self.render_scalar_expression(expression)?
        ))
    }

    fn render_try_cast_expression(
        &self,
        expression: &ScalarExpression,
        target_type: &str,
    ) -> Result<String, CoreError> {
        Ok(format!(
            "TRY_CAST({} AS {target_type})",
            self.render_scalar_expression(expression)?
        ))
    }

    fn render_unary_function_expression(
        &self,
        function_name: &str,
        expression: &ScalarExpression,
    ) -> Result<String, CoreError> {
        Ok(format!(
            "{function_name}({})",
            self.render_scalar_expression(expression)?
        ))
    }

    fn render_replace_expression(
        &self,
        expression: &ScalarExpression,
        search: &ScalarExpression,
        replacement: &ScalarExpression,
    ) -> Result<String, CoreError> {
        Ok(format!(
            "REPLACE({}, {}, {})",
            self.render_scalar_expression(expression)?,
            self.render_scalar_expression(search)?,
            self.render_scalar_expression(replacement)?
        ))
    }

    fn render_binary_function_expression(
        &self,
        function_name: &str,
        left: &ScalarExpression,
        right: &ScalarExpression,
    ) -> Result<String, CoreError> {
        Ok(format!(
            "{function_name}({}, {})",
            self.render_scalar_expression(left)?,
            self.render_scalar_expression(right)?
        ))
    }

    fn render_round_expression(
        &self,
        expression: &ScalarExpression,
        places: Option<&ScalarExpression>,
    ) -> Result<String, CoreError> {
        let expression_sql = self.render_scalar_expression(expression)?;
        let Some(places) = places else {
            return Ok(format!("round({expression_sql})"));
        };
        Ok(format!(
            "round({expression_sql}, {})",
            self.render_scalar_expression(places)?
        ))
    }

    fn render_arithmetic_expression(
        &self,
        operator: ArithmeticOperator,
        left: &ScalarExpression,
        right: &ScalarExpression,
    ) -> Result<String, CoreError> {
        let left = self.render_scalar_expression(left)?;
        let right = self.render_scalar_expression(right)?;
        if operator == ArithmeticOperator::Power {
            return Ok(format!("power({left}, {right})"));
        }
        Ok(format!(
            "({left} {} {right})",
            render_arithmetic_operator(operator)
        ))
    }

    fn render_substring_expression(
        &self,
        expression: &ScalarExpression,
        start: &ScalarExpression,
        length: Option<&ScalarExpression>,
    ) -> Result<String, CoreError> {
        let mut sql = format!(
            "SUBSTRING({} FROM ({} + 1)",
            self.render_scalar_expression(expression)?,
            self.render_scalar_expression(start)?
        );
        if let Some(length) = length {
            write!(&mut sql, " FOR {}", self.render_scalar_expression(length)?)
                .map_err(|error| CoreError::internal(error.to_string()))?;
        }
        sql.push(')');
        Ok(sql)
    }

    fn render_case_expression(
        &self,
        alternatives: &[ScalarCaseAlternative],
        else_expression: Option<&ScalarExpression>,
    ) -> Result<String, CoreError> {
        let mut sql = String::from("CASE");
        for alternative in alternatives {
            write!(
                &mut sql,
                " WHEN {} THEN {}",
                self.render_predicate_expression(&alternative.when)?,
                self.render_scalar_expression(&alternative.then)?
            )
            .map_err(|error| CoreError::internal(error.to_string()))?;
        }
        if let Some(else_expression) = else_expression {
            write!(
                &mut sql,
                " ELSE {}",
                self.render_scalar_expression(else_expression)?
            )
            .map_err(|error| CoreError::internal(error.to_string()))?;
        }
        sql.push_str(" END");
        Ok(sql)
    }
}

fn render_table_ref(table: &TableRef) -> String {
    format!(
        "{}.{}",
        quote_ident(&table.schema),
        quote_ident(&table.name)
    )
}

fn render_operator(operator: ComparisonOperator) -> &'static str {
    match operator {
        ComparisonOperator::Equal => "=",
        ComparisonOperator::NotEqual => "<>",
        ComparisonOperator::GreaterThan => ">",
        ComparisonOperator::GreaterThanOrEqual => ">=",
        ComparisonOperator::LessThan => "<",
        ComparisonOperator::LessThanOrEqual => "<=",
        ComparisonOperator::In => "IN",
        ComparisonOperator::StartsWith => "STARTS WITH",
        ComparisonOperator::EndsWith => "ENDS WITH",
        ComparisonOperator::Contains => "CONTAINS",
        ComparisonOperator::RegexMatch => {
            unreachable!("regex predicates lower through regexp_like")
        }
    }
}

fn render_aggregate_function(function: AggregateFunction) -> &'static str {
    match function {
        AggregateFunction::Count => "COUNT",
        AggregateFunction::Collect => "ARRAY_AGG",
        AggregateFunction::Sum => "SUM",
        AggregateFunction::Avg => "AVG",
        AggregateFunction::Median => "MEDIAN",
        AggregateFunction::StdDev => "STDDEV_SAMP",
        AggregateFunction::StdDevP => "STDDEV_POP",
        AggregateFunction::Min => "MIN",
        AggregateFunction::Max => "MAX",
    }
}

fn projection_output_alias(projection: &Projection) -> Option<&str> {
    match projection {
        Projection::Property { alias, .. } => alias.as_deref(),
        Projection::Key { alias, .. }
        | Projection::ElementId { alias, .. }
        | Projection::NodeLabels { alias, .. }
        | Projection::PropertyKeys { alias, .. }
        | Projection::RelationshipType { alias, .. }
        | Projection::Literal { alias, .. }
        | Projection::LiteralList { alias, .. }
        | Projection::Expression { alias, .. }
        | Projection::CountAll { alias }
        | Projection::Aggregate { alias, .. } => Some(alias),
    }
}

fn render_arithmetic_operator(operator: ArithmeticOperator) -> &'static str {
    match operator {
        ArithmeticOperator::Add => "+",
        ArithmeticOperator::Subtract => "-",
        ArithmeticOperator::Multiply => "*",
        ArithmeticOperator::Divide => "/",
        ArithmeticOperator::Modulo => "%",
        ArithmeticOperator::Power => unreachable!("power arithmetic lowers as a function"),
    }
}

fn render_literal(literal: &Literal) -> String {
    match literal {
        Literal::String(value) => quote_string_literal(value),
        Literal::Integer(value) => value.to_string(),
        Literal::Float(value) => (*value).into_inner().to_string(),
        Literal::Boolean(value) => value.to_string(),
        Literal::Null => "NULL".to_string(),
    }
}

fn render_literal_list(literals: &[Literal]) -> String {
    let values = literals
        .iter()
        .map(render_literal)
        .collect::<Vec<_>>()
        .join(", ");
    format!("make_array({values})")
}

fn render_like_pattern(operator: ComparisonOperator, value: &str) -> String {
    let escaped = escape_like_literal(value);
    let pattern = match operator {
        ComparisonOperator::StartsWith => format!("{escaped}%"),
        ComparisonOperator::EndsWith => format!("%{escaped}"),
        ComparisonOperator::Contains => format!("%{escaped}%"),
        _ => unreachable!("LIKE pattern requested for non-string predicate operator"),
    };
    quote_string_literal(&pattern)
}

fn render_string_function_predicate(operator: ComparisonOperator, lhs: &str, rhs: &str) -> String {
    let function_name = match operator {
        ComparisonOperator::StartsWith => "starts_with",
        ComparisonOperator::EndsWith => "ends_with",
        ComparisonOperator::Contains => "contains",
        _ => unreachable!("string function requested for non-string predicate operator"),
    };
    format!("{function_name}({lhs}, {rhs})")
}

fn render_regex_predicate(lhs: &str, rhs: &str) -> String {
    format!("regexp_like({lhs}, {rhs})")
}

fn render_xor_predicate(left: &str, right: &str) -> String {
    format!("(({left} AND NOT ({right})) OR (NOT ({left}) AND {right}))")
}

fn escape_like_literal(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

fn quote_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn quote_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::virtual_graph::ir::{
        AggregateFunction, AggregateTarget, ComparisonOperator, Direction, GraphPlan, KeyPredicate,
        Literal, NodePattern, OptionalMatchScope, OrderDirection, OrderExpression, OrderKey,
        PredicateExpression, PredicateRhs, Projection, ProjectionPredicate,
        ProjectionPredicateExpression, ProjectionPredicateRhs, PropertyPredicate, PropertyRef,
        RelationshipPattern, ScalarExpression, ScalarPredicate, ScalarPredicateRhs,
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
      team: team
  - label: Service
    table: { schema: ops, name: services }
    key: id
    properties:
      name: service_name
      tier: tier
      risk: risk_score
relationships:
  - type: OWNS
    table: { schema: ops, name: ownerships }
    key: ownership_id
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
    properties:
      since: since
  - type: DEPENDS_ON
    table: { schema: ops, name: service_dependencies }
    from: { label: Service, key: from_service_id }
    to: { label: Service, key: to_service_id }
    properties:
      criticality: criticality
";

    #[test]
    fn lower_graph_plan_renders_forward_relationship_sql() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let plan = ownership_plan(Direction::Outgoing);

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("plan should lower to SQL");

        assert_eq!(
            translation.sql(),
            "SELECT \"n0\".\"full_name\" AS \"owner\", \"n1\".\"service_name\" AS \"service\" \
             FROM \"ops\".\"people\" AS \"n0\" \
             JOIN \"ops\".\"ownerships\" AS \"r0\" ON \"r0\".\"person_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n1\" ON \"r0\".\"service_id\" = \"n1\".\"id\" \
             WHERE \"n1\".\"tier\" = 'prod' ORDER BY \"n0\".\"full_name\" ASC LIMIT 25"
        );
    }

    #[test]
    fn lower_graph_plan_renders_disconnected_components_as_cross_joins() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let plan = GraphPlan {
            nodes: vec![
                NodePattern {
                    variable: "source".to_string(),
                    label: "Service".to_string(),
                },
                NodePattern {
                    variable: "target".to_string(),
                    label: "Service".to_string(),
                },
                NodePattern {
                    variable: "person".to_string(),
                    label: "Person".to_string(),
                },
            ],
            relationships: vec![RelationshipPattern {
                variable: None,
                relationship_type: "DEPENDS_ON".to_string(),
                left: "source".to_string(),
                direction: Direction::Outgoing,
                right: "target".to_string(),
            }],
            optional_relationships: Vec::new(),
            optional_matches: Vec::new(),
            distinct: false,
            projections: vec![
                Projection::Property {
                    property: PropertyRef {
                        variable: "source".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("source".to_string()),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "target".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("target".to_string()),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "person".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("person".to_string()),
                },
            ],
            predicates: Vec::new(),
            predicate: None,
            post_projection_predicate: None,
            order_by: Vec::new(),
            skip: None,
            limit: None,
        };

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("disconnected mandatory components should lower");

        assert_eq!(
            translation.sql(),
            "SELECT \"n0\".\"service_name\" AS \"source\", \"n1\".\"service_name\" AS \"target\", \"n2\".\"full_name\" AS \"person\" \
             FROM \"ops\".\"services\" AS \"n0\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r0\" ON \"r0\".\"from_service_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n1\" ON \"r0\".\"to_service_id\" = \"n1\".\"id\" \
             CROSS JOIN \"ops\".\"people\" AS \"n2\""
        );
    }

    #[test]
    fn lower_graph_plan_renders_reverse_relationship_sql() {
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
                direction: Direction::Incoming,
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

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("reverse relationship should lower");

        assert_eq!(
            translation.sql(),
            "SELECT \"n1\".\"full_name\" AS \"owner\" FROM \"ops\".\"services\" AS \"n0\" \
             JOIN \"ops\".\"ownerships\" AS \"r0\" ON \"r0\".\"service_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"people\" AS \"n1\" ON \"r0\".\"person_id\" = \"n1\".\"id\""
        );
    }

    #[test]
    fn lower_graph_plan_renders_optional_relationship_sql() {
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
                left: "person".to_string(),
                direction: Direction::Outgoing,
                right: "service".to_string(),
            }],
            optional_relationships: vec![0],
            optional_matches: Vec::new(),
            distinct: false,
            projections: vec![
                Projection::Property {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("service".to_string()),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "person".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("owner".to_string()),
                },
            ],
            predicates: Vec::new(),
            predicate: None,
            post_projection_predicate: None,
            order_by: Vec::new(),
            skip: None,
            limit: None,
        };

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("optional relationship should lower");

        assert_eq!(
            translation.sql(),
            "SELECT \"n0\".\"service_name\" AS \"service\", \"n1\".\"full_name\" AS \"owner\" \
             FROM \"ops\".\"services\" AS \"n0\" \
             LEFT JOIN \"ops\".\"ownerships\" AS \"r0\" ON \"r0\".\"service_id\" = \"n0\".\"id\" \
             LEFT JOIN \"ops\".\"people\" AS \"n1\" ON \"r0\".\"person_id\" = \"n1\".\"id\""
        );
    }

    #[test]
    fn lower_graph_plan_renders_optional_predicates_inside_join_scope() {
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
                variable: Some("owns".to_string()),
                relationship_type: "OWNS".to_string(),
                left: "person".to_string(),
                direction: Direction::Outgoing,
                right: "service".to_string(),
            }],
            optional_relationships: vec![0],
            optional_matches: vec![OptionalMatchScope {
                relationship_indices: vec![0],
                predicate: Some(PredicateExpression::Comparison(PropertyPredicate {
                    property: PropertyRef {
                        variable: "person".to_string(),
                        property: "team".to_string(),
                    },
                    operator: ComparisonOperator::Equal,
                    rhs: PredicateRhs::Literal(Literal::String("platform".to_string())),
                })),
            }],
            distinct: false,
            projections: vec![
                Projection::Property {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("service".to_string()),
                },
                Projection::Key {
                    variable: "owns".to_string(),
                    alias: "ownership_id".to_string(),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "person".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("owner".to_string()),
                },
            ],
            predicates: Vec::new(),
            predicate: None,
            post_projection_predicate: None,
            order_by: Vec::new(),
            skip: None,
            limit: None,
        };

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("optional predicate should lower");

        assert_eq!(
            translation.sql(),
            "SELECT \"n0\".\"service_name\" AS \"service\", \"r0\".\"ownership_id\" AS \"ownership_id\", \"n1\".\"full_name\" AS \"owner\" \
             FROM \"ops\".\"services\" AS \"n0\" \
             LEFT JOIN (\"ops\".\"ownerships\" AS \"r0\" JOIN \"ops\".\"people\" AS \"n1\" ON \"r0\".\"person_id\" = \"n1\".\"id\") \
             ON (\"r0\".\"service_id\" = \"n0\".\"id\") AND (\"n1\".\"team\" = 'platform')"
        );
    }

    #[test]
    fn lower_graph_plan_renders_undirected_optional_predicates_inside_join_scope() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let plan = GraphPlan {
            nodes: vec![
                NodePattern {
                    variable: "service".to_string(),
                    label: "Service".to_string(),
                },
                NodePattern {
                    variable: "dependency".to_string(),
                    label: "Service".to_string(),
                },
            ],
            relationships: vec![RelationshipPattern {
                variable: Some("dependency_edge".to_string()),
                relationship_type: "DEPENDS_ON".to_string(),
                left: "service".to_string(),
                direction: Direction::Undirected,
                right: "dependency".to_string(),
            }],
            optional_relationships: vec![0],
            optional_matches: vec![OptionalMatchScope {
                relationship_indices: vec![0],
                predicate: Some(PredicateExpression::Comparison(PropertyPredicate {
                    property: PropertyRef {
                        variable: "dependency".to_string(),
                        property: "tier".to_string(),
                    },
                    operator: ComparisonOperator::Equal,
                    rhs: PredicateRhs::Literal(Literal::String("dev".to_string())),
                })),
            }],
            distinct: false,
            projections: vec![
                Projection::Property {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("service".to_string()),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "dependency".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("dependency".to_string()),
                },
            ],
            predicates: Vec::new(),
            predicate: None,
            post_projection_predicate: None,
            order_by: Vec::new(),
            skip: None,
            limit: None,
        };

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("undirected optional predicate should lower");

        assert_eq!(
            translation.sql(),
            "SELECT \"n0\".\"service_name\" AS \"service\", \"n1\".\"service_name\" AS \"dependency\" \
             FROM \"ops\".\"services\" AS \"n0\" \
             LEFT JOIN (\"ops\".\"service_dependencies\" AS \"r0\" JOIN \"ops\".\"services\" AS \"n1\" ON (\"r0\".\"to_service_id\" = \"n1\".\"id\" OR \"r0\".\"from_service_id\" = \"n1\".\"id\")) \
             ON (((\"r0\".\"from_service_id\" = \"n0\".\"id\" AND \"r0\".\"to_service_id\" = \"n1\".\"id\") OR (\"r0\".\"to_service_id\" = \"n0\".\"id\" AND \"r0\".\"from_service_id\" = \"n1\".\"id\"))) AND (\"n1\".\"tier\" = 'dev')"
        );
    }

    #[test]
    fn lower_graph_plan_renders_undirected_distinct_label_relationship_sql() {
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

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("undirected relationship should lower");

        assert_eq!(
            translation.sql(),
            "SELECT \"n1\".\"full_name\" AS \"owner\" FROM \"ops\".\"services\" AS \"n0\" \
             JOIN \"ops\".\"ownerships\" AS \"r0\" ON \"r0\".\"service_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"people\" AS \"n1\" ON \"r0\".\"person_id\" = \"n1\".\"id\""
        );
    }

    #[test]
    fn lower_graph_plan_renders_undirected_same_label_relationship_sql() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let plan = GraphPlan {
            nodes: vec![
                NodePattern {
                    variable: "source".to_string(),
                    label: "Service".to_string(),
                },
                NodePattern {
                    variable: "neighbor".to_string(),
                    label: "Service".to_string(),
                },
            ],
            relationships: vec![RelationshipPattern {
                variable: None,
                relationship_type: "DEPENDS_ON".to_string(),
                left: "source".to_string(),
                direction: Direction::Undirected,
                right: "neighbor".to_string(),
            }],
            optional_relationships: Vec::new(),
            optional_matches: Vec::new(),
            distinct: false,
            projections: vec![Projection::Property {
                property: PropertyRef {
                    variable: "neighbor".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("neighbor".to_string()),
            }],
            predicates: Vec::new(),
            predicate: None,
            post_projection_predicate: None,
            order_by: Vec::new(),
            skip: None,
            limit: None,
        };

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("undirected same-label relationship should lower");

        assert_eq!(
            translation.sql(),
            "SELECT \"n1\".\"service_name\" AS \"neighbor\" FROM \"ops\".\"services\" AS \"n0\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r0\" ON (\"r0\".\"from_service_id\" = \"n0\".\"id\" OR \"r0\".\"to_service_id\" = \"n0\".\"id\") \
             JOIN \"ops\".\"services\" AS \"n1\" ON ((\"r0\".\"from_service_id\" = \"n0\".\"id\" AND \"r0\".\"to_service_id\" = \"n1\".\"id\") OR (\"r0\".\"to_service_id\" = \"n0\".\"id\" AND \"r0\".\"from_service_id\" = \"n1\".\"id\"))"
        );
    }

    #[test]
    fn lower_graph_plan_renders_identity_and_static_function_projections() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.relationships
            .get_mut(0)
            .expect("ownership plan should include one relationship")
            .variable = Some("owns".to_string());
        plan.projections = vec![
            Projection::Key {
                variable: "person".to_string(),
                alias: "person_id".to_string(),
            },
            Projection::NodeLabels {
                variable: "person".to_string(),
                label: "Person".to_string(),
                alias: "person_labels".to_string(),
            },
            Projection::PropertyKeys {
                variable: "person".to_string(),
                alias: "person_keys".to_string(),
            },
            Projection::Key {
                variable: "owns".to_string(),
                alias: "ownership_id".to_string(),
            },
            Projection::RelationshipType {
                variable: "owns".to_string(),
                relationship_type: "OWNS".to_string(),
                alias: "relationship_type".to_string(),
            },
            Projection::PropertyKeys {
                variable: "owns".to_string(),
                alias: "relationship_keys".to_string(),
            },
        ];

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("identity and static function projections should lower");

        assert_eq!(
            translation.sql(),
            "SELECT \"n0\".\"id\" AS \"person_id\", CASE WHEN \"n0\".\"id\" IS NULL THEN NULL ELSE make_array('Person') END AS \"person_labels\", CASE WHEN \"n0\".\"id\" IS NULL THEN NULL ELSE make_array('name', 'team') END AS \"person_keys\", \"r0\".\"ownership_id\" AS \"ownership_id\", CASE WHEN \"r0\".\"ownership_id\" IS NULL THEN NULL ELSE 'OWNS' END AS \"relationship_type\", CASE WHEN \"r0\".\"ownership_id\" IS NULL THEN NULL ELSE make_array('since') END AS \"relationship_keys\" \
             FROM \"ops\".\"people\" AS \"n0\" \
             JOIN \"ops\".\"ownerships\" AS \"r0\" ON \"r0\".\"person_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n1\" ON \"r0\".\"service_id\" = \"n1\".\"id\" \
             WHERE \"n1\".\"tier\" = 'prod' ORDER BY \"n0\".\"full_name\" ASC LIMIT 25"
        );
    }

    #[test]
    fn lower_graph_plan_renders_property_keys_ordering() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.order_by = vec![OrderKey {
            expression: OrderExpression::PropertyKeys {
                variable: "service".to_string(),
            },
            direction: OrderDirection::Descending,
        }];

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("property key ordering should lower");

        assert!(
            translation.sql().contains(
                "ORDER BY CASE WHEN \"n1\".\"id\" IS NULL THEN NULL ELSE \
                 make_array('name', 'risk', 'tier') END DESC"
            ),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_scalar_post_projection_predicates_as_where() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.post_projection_predicate = Some(ProjectionPredicateExpression::Comparison(
            ProjectionPredicate {
                alias: "owner".to_string(),
                operator: ComparisonOperator::StartsWith,
                rhs: ProjectionPredicateRhs::Literal(Literal::String("Ada".to_string())),
            },
        ));

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("post-projection predicate should lower");

        assert_eq!(
            translation.sql(),
            "SELECT \"n0\".\"full_name\" AS \"owner\", \"n1\".\"service_name\" AS \"service\" \
             FROM \"ops\".\"people\" AS \"n0\" \
             JOIN \"ops\".\"ownerships\" AS \"r0\" ON \"r0\".\"person_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n1\" ON \"r0\".\"service_id\" = \"n1\".\"id\" \
             WHERE \"n1\".\"tier\" = 'prod' AND \"n0\".\"full_name\" LIKE 'Ada%' ESCAPE '\\' \
             ORDER BY \"n0\".\"full_name\" ASC LIMIT 25"
        );
    }

    #[test]
    fn lower_graph_plan_renders_xor_post_projection_predicates() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.post_projection_predicate = Some(ProjectionPredicateExpression::Xor {
            left: Box::new(ProjectionPredicateExpression::Comparison(
                ProjectionPredicate {
                    alias: "owner".to_string(),
                    operator: ComparisonOperator::StartsWith,
                    rhs: ProjectionPredicateRhs::Literal(Literal::String("Ada".to_string())),
                },
            )),
            right: Box::new(ProjectionPredicateExpression::Comparison(
                ProjectionPredicate {
                    alias: "service".to_string(),
                    operator: ComparisonOperator::Contains,
                    rhs: ProjectionPredicateRhs::Literal(Literal::String("api".to_string())),
                },
            )),
        });

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("post-projection XOR predicate should lower");

        assert!(
            translation.sql().contains(
                "WHERE \"n1\".\"tier\" = 'prod' AND ((\"n0\".\"full_name\" LIKE 'Ada%' ESCAPE '\\' AND NOT (\"n1\".\"service_name\" LIKE '%api%' ESCAPE '\\')) OR (NOT (\"n0\".\"full_name\" LIKE 'Ada%' ESCAPE '\\') AND \"n1\".\"service_name\" LIKE '%api%' ESCAPE '\\'))"
            ),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_aggregate_post_projection_predicates_as_having() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.projections = vec![
            Projection::Property {
                property: PropertyRef {
                    variable: "person".to_string(),
                    property: "team".to_string(),
                },
                alias: Some("team".to_string()),
            },
            Projection::CountAll {
                alias: "service_count".to_string(),
            },
        ];
        plan.post_projection_predicate = Some(ProjectionPredicateExpression::Comparison(
            ProjectionPredicate {
                alias: "service_count".to_string(),
                operator: ComparisonOperator::GreaterThan,
                rhs: ProjectionPredicateRhs::Literal(Literal::Integer(1)),
            },
        ));
        plan.order_by = vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("service_count".to_string()),
            direction: OrderDirection::Descending,
        }];

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("aggregate post-projection predicate should lower");

        assert_eq!(
            translation.sql(),
            "SELECT \"n0\".\"team\" AS \"team\", COUNT(*) AS \"service_count\" \
             FROM \"ops\".\"people\" AS \"n0\" \
             JOIN \"ops\".\"ownerships\" AS \"r0\" ON \"r0\".\"person_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n1\" ON \"r0\".\"service_id\" = \"n1\".\"id\" \
             WHERE \"n1\".\"tier\" = 'prod' GROUP BY \"n0\".\"team\" \
             HAVING COUNT(*) > 1 ORDER BY \"service_count\" DESC LIMIT 25"
        );
    }

    #[test]
    fn lower_graph_plan_renders_relationship_between_joined_nodes() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
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
                RelationshipPattern {
                    variable: None,
                    relationship_type: "DEPENDS_ON".to_string(),
                    left: "source".to_string(),
                    direction: Direction::Outgoing,
                    right: "target".to_string(),
                },
            ],
            optional_relationships: Vec::new(),
            optional_matches: Vec::new(),
            distinct: false,
            projections: vec![
                Projection::Property {
                    property: PropertyRef {
                        variable: "source".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("source".to_string()),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "middle".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("middle".to_string()),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "target".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("target".to_string()),
                },
            ],
            predicates: Vec::new(),
            predicate: None,
            post_projection_predicate: None,
            order_by: Vec::new(),
            skip: None,
            limit: None,
        };

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("closed service dependency path should lower");

        assert_eq!(
            translation.sql(),
            "SELECT \"n0\".\"service_name\" AS \"source\", \"n1\".\"service_name\" AS \"middle\", \"n2\".\"service_name\" AS \"target\" \
             FROM \"ops\".\"services\" AS \"n0\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r0\" ON \"r0\".\"from_service_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n1\" ON \"r0\".\"to_service_id\" = \"n1\".\"id\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r1\" ON \"r1\".\"from_service_id\" = \"n1\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n2\" ON \"r1\".\"to_service_id\" = \"n2\".\"id\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r2\" ON \"r2\".\"from_service_id\" = \"n0\".\"id\" AND \"r2\".\"to_service_id\" = \"n2\".\"id\""
        );
    }

    #[test]
    fn lower_graph_plan_reorders_connected_relationships() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
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
            projections: vec![
                Projection::Property {
                    property: PropertyRef {
                        variable: "source".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("source".to_string()),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "target".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("target".to_string()),
                },
            ],
            predicates: Vec::new(),
            predicate: None,
            post_projection_predicate: None,
            order_by: Vec::new(),
            skip: None,
            limit: None,
        };

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("connected out-of-order relationship plan should lower");

        assert_eq!(
            translation.sql(),
            "SELECT \"n0\".\"service_name\" AS \"source\", \"n2\".\"service_name\" AS \"target\" \
             FROM \"ops\".\"services\" AS \"n0\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r1\" ON \"r1\".\"from_service_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n1\" ON \"r1\".\"to_service_id\" = \"n1\".\"id\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r0\" ON \"r0\".\"from_service_id\" = \"n1\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n2\" ON \"r0\".\"to_service_id\" = \"n2\".\"id\""
        );
    }

    #[test]
    fn lower_graph_plan_quotes_identifiers_and_literals() {
        let graph = Declaration::from_yaml(
            r#"
version: 1
name: quoting
nodes:
  - label: Weird
    table: { schema: weird-schema, name: table"name }
    key: id"key
    properties:
      display: display"name
relationships: []
"#,
        )
        .expect("graph should parse");
        let plan = GraphPlan {
            nodes: vec![NodePattern {
                variable: "weird".to_string(),
                label: "Weird".to_string(),
            }],
            relationships: Vec::new(),
            optional_relationships: Vec::new(),
            optional_matches: Vec::new(),
            distinct: false,
            projections: vec![Projection::Property {
                property: PropertyRef {
                    variable: "weird".to_string(),
                    property: "display".to_string(),
                },
                alias: Some("value".to_string()),
            }],
            predicates: vec![PropertyPredicate {
                property: PropertyRef {
                    variable: "weird".to_string(),
                    property: "display".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("Ada's laptop".to_string())),
            }],
            predicate: None,
            post_projection_predicate: None,
            order_by: Vec::new(),
            skip: None,
            limit: None,
        };

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("quoted plan should lower");

        assert_eq!(
            translation.sql(),
            "SELECT \"n0\".\"display\"\"name\" AS \"value\" \
             FROM \"weird-schema\".\"table\"\"name\" AS \"n0\" \
             WHERE \"n0\".\"display\"\"name\" = 'Ada''s laptop'"
        );
    }

    #[test]
    fn lower_graph_plan_renders_key_predicates() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let plan = GraphPlan {
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
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("service".to_string()),
            }],
            predicates: Vec::new(),
            predicate: Some(PredicateExpression::And {
                left: Box::new(PredicateExpression::KeyComparison(KeyPredicate {
                    variable: "person".to_string(),
                    operator: ComparisonOperator::Equal,
                    rhs: PredicateRhs::Literal(Literal::Integer(1)),
                })),
                right: Box::new(PredicateExpression::KeyComparison(KeyPredicate {
                    variable: "owns".to_string(),
                    operator: ComparisonOperator::In,
                    rhs: PredicateRhs::List(vec![Literal::Integer(100), Literal::Integer(200)]),
                })),
            }),
            post_projection_predicate: None,
            order_by: Vec::new(),
            skip: None,
            limit: None,
        };

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("key predicates should lower");

        assert_eq!(
            translation.sql(),
            "SELECT \"n1\".\"service_name\" AS \"service\" FROM \"ops\".\"people\" AS \"n0\" \
             JOIN \"ops\".\"ownerships\" AS \"r0\" ON \"r0\".\"person_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n1\" ON \"r0\".\"service_id\" = \"n1\".\"id\" \
             WHERE (\"n0\".\"id\" = 1 AND \"r0\".\"ownership_id\" IN (100, 200))"
        );
    }

    #[test]
    fn lower_graph_plan_renders_element_id_projection_predicate_and_ordering() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.relationships
            .first_mut()
            .expect("ownership plan should contain a relationship")
            .variable = Some("owns".to_string());
        plan.projections = vec![
            Projection::ElementId {
                variable: "person".to_string(),
                alias: "person_element_id".to_string(),
            },
            Projection::ElementId {
                variable: "owns".to_string(),
                alias: "ownership_element_id".to_string(),
            },
        ];
        plan.predicates.clear();
        plan.predicate = Some(PredicateExpression::ElementIdComparison(
            ElementIdPredicate {
                variable: "person".to_string(),
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("1".to_string())),
            },
        ));
        plan.order_by = vec![OrderKey {
            expression: OrderExpression::ElementId {
                variable: "owns".to_string(),
            },
            direction: OrderDirection::Descending,
        }];

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("elementId() projection and predicate should lower");

        assert!(
            translation
                .sql()
                .contains("CAST(\"n0\".\"id\" AS VARCHAR) AS \"person_element_id\""),
            "{}",
            translation.sql()
        );
        assert!(
            translation
                .sql()
                .contains("CAST(\"r0\".\"ownership_id\" AS VARCHAR) AS \"ownership_element_id\""),
            "{}",
            translation.sql()
        );
        assert!(
            translation
                .sql()
                .contains("WHERE CAST(\"n0\".\"id\" AS VARCHAR) = '1'"),
            "{}",
            translation.sql()
        );
        assert!(
            translation
                .sql()
                .contains("ORDER BY CAST(\"r0\".\"ownership_id\" AS VARCHAR) DESC"),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_key_rhs_predicates() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let plan = GraphPlan {
            nodes: vec![
                NodePattern {
                    variable: "source".to_string(),
                    label: "Service".to_string(),
                },
                NodePattern {
                    variable: "target".to_string(),
                    label: "Service".to_string(),
                },
            ],
            relationships: vec![RelationshipPattern {
                variable: None,
                relationship_type: "DEPENDS_ON".to_string(),
                left: "source".to_string(),
                direction: Direction::Outgoing,
                right: "target".to_string(),
            }],
            optional_relationships: Vec::new(),
            optional_matches: Vec::new(),
            distinct: false,
            projections: vec![Projection::Property {
                property: PropertyRef {
                    variable: "source".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("source".to_string()),
            }],
            predicates: Vec::new(),
            predicate: Some(PredicateExpression::KeyComparison(KeyPredicate {
                variable: "source".to_string(),
                operator: ComparisonOperator::NotEqual,
                rhs: PredicateRhs::Key {
                    variable: "target".to_string(),
                },
            })),
            post_projection_predicate: None,
            order_by: Vec::new(),
            skip: None,
            limit: None,
        };

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("key RHS predicate should lower");

        assert_eq!(
            translation.sql(),
            "SELECT \"n0\".\"service_name\" AS \"source\" FROM \"ops\".\"services\" AS \"n0\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r0\" ON \"r0\".\"from_service_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n1\" ON \"r0\".\"to_service_id\" = \"n1\".\"id\" \
             WHERE \"n0\".\"id\" <> \"n1\".\"id\""
        );
    }

    #[test]
    fn lower_graph_plan_renders_null_predicates() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let plan = GraphPlan {
            nodes: vec![NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            }],
            relationships: Vec::new(),
            optional_relationships: Vec::new(),
            optional_matches: Vec::new(),
            distinct: false,
            projections: vec![Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("service".to_string()),
            }],
            predicates: vec![
                PropertyPredicate {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    },
                    operator: ComparisonOperator::Equal,
                    rhs: PredicateRhs::Literal(Literal::Null),
                },
                PropertyPredicate {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    },
                    operator: ComparisonOperator::NotEqual,
                    rhs: PredicateRhs::Literal(Literal::Null),
                },
            ],
            predicate: None,
            post_projection_predicate: None,
            order_by: Vec::new(),
            skip: None,
            limit: None,
        };

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("null predicate plan should lower");

        assert_eq!(
            translation.sql(),
            "SELECT \"n0\".\"service_name\" AS \"service\" FROM \"ops\".\"services\" AS \"n0\" \
             WHERE \"n0\".\"tier\" IS NULL AND \"n0\".\"service_name\" IS NOT NULL"
        );
    }

    #[test]
    fn lower_graph_plan_renders_property_rhs_predicates() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.predicates = vec![PropertyPredicate {
            property: PropertyRef {
                variable: "person".to_string(),
                property: "team".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Property(PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            }),
        }];

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("property comparison should lower");

        assert!(
            translation
                .sql()
                .contains("WHERE \"n0\".\"team\" = \"n1\".\"tier\""),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_in_predicates() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.predicates = vec![PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            },
            operator: ComparisonOperator::In,
            rhs: PredicateRhs::List(vec![
                Literal::String("prod".to_string()),
                Literal::String("dev".to_string()),
            ]),
        }];

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("IN predicate should lower");

        assert!(
            translation
                .sql()
                .contains("WHERE \"n1\".\"tier\" IN ('prod', 'dev')"),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_float_predicates() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.predicates = vec![
            PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                },
                operator: ComparisonOperator::GreaterThanOrEqual,
                rhs: PredicateRhs::Literal(Literal::Float(ordered_float::OrderedFloat(0.75_f64))),
            },
            PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                },
                operator: ComparisonOperator::In,
                rhs: PredicateRhs::List(vec![
                    Literal::Float(ordered_float::OrderedFloat(0.5_f64)),
                    Literal::Float(ordered_float::OrderedFloat(0.75_f64)),
                ]),
            },
        ];

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("float predicates should lower");

        assert!(
            translation.sql().contains(
                "WHERE \"n1\".\"risk_score\" >= 0.75 AND \"n1\".\"risk_score\" IN (0.5, 0.75)"
            ),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_empty_in_lists_as_false() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.predicates = vec![PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            },
            operator: ComparisonOperator::In,
            rhs: PredicateRhs::List(Vec::new()),
        }];

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("empty IN predicate should lower");

        assert!(
            translation.sql().contains("WHERE FALSE"),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_string_predicates_as_escaped_like() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.predicates = vec![
            PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                operator: ComparisonOperator::StartsWith,
                rhs: PredicateRhs::Literal(Literal::String("bill_%".to_string())),
            },
            PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                operator: ComparisonOperator::Contains,
                rhs: PredicateRhs::Literal(Literal::String("api".to_string())),
            },
        ];

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("string predicates should lower");

        assert!(
            translation.sql().contains(
                "WHERE \"n1\".\"service_name\" LIKE 'bill\\_\\%%' ESCAPE '\\' AND \"n1\".\"service_name\" LIKE '%api%' ESCAPE '\\'"
            ),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_dynamic_string_predicates_as_functions() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.predicates.clear();
        plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: service_name_expression(),
            operator: ComparisonOperator::StartsWith,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Left {
                expression: Box::new(service_name_expression()),
                count: Box::new(ScalarExpression::Literal(Literal::Integer(4))),
            }),
        }));

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("dynamic string predicate should lower");

        assert!(
            translation.sql().contains(
                "WHERE starts_with(\"n1\".\"service_name\", left(\"n1\".\"service_name\", 4))"
            ),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_regex_predicates() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.predicates = vec![PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            },
            operator: ComparisonOperator::RegexMatch,
            rhs: PredicateRhs::Literal(Literal::String("^bill.*".to_string())),
        }];

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("regex predicate should lower");

        assert!(
            translation
                .sql()
                .contains("WHERE regexp_like(\"n1\".\"service_name\", '^bill.*')"),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_dynamic_regex_predicates() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.predicates.clear();
        plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: service_name_expression(),
            operator: ComparisonOperator::RegexMatch,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Left {
                expression: Box::new(service_name_expression()),
                count: Box::new(ScalarExpression::Literal(Literal::Integer(4))),
            }),
        }));

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("dynamic regex predicate should lower");

        assert!(
            translation.sql().contains(
                "WHERE regexp_like(\"n1\".\"service_name\", left(\"n1\".\"service_name\", 4))"
            ),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_scalar_predicates() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.predicates.clear();
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
            operator: ComparisonOperator::In,
            rhs: ScalarPredicateRhs::List(vec![
                Literal::String("prod".to_string()),
                Literal::String("dev".to_string()),
            ]),
        }));

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("scalar predicate should lower");

        assert!(
            translation
                .sql()
                .contains("WHERE COALESCE(\"n1\".\"tier\", 'unassigned') IN ('prod', 'dev')"),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_relationship_type_scalar_expressions() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.relationships
            .first_mut()
            .expect("ownership plan should have a relationship")
            .variable = Some("owns".to_string());
        plan.predicates.clear();
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
        plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::RelationshipType {
                variable: "owns".to_string(),
                relationship_type: "OWNS".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
                "OWNS".to_string(),
            ))),
        }));
        plan.order_by = vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::RelationshipType {
                variable: "owns".to_string(),
                relationship_type: "OWNS".to_string(),
            }),
            direction: OrderDirection::Ascending,
        }];

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("relationship type scalar expression should lower");

        assert!(
            translation.sql().contains(
                "COALESCE(CASE WHEN \"r0\".\"ownership_id\" IS NULL THEN NULL ELSE 'OWNS' END, 'missing') AS \"relationship_type\""
            ),
            "{}",
            translation.sql()
        );
        assert!(
            translation.sql().contains(
                "WHERE CASE WHEN \"r0\".\"ownership_id\" IS NULL THEN NULL ELSE 'OWNS' END = 'OWNS'"
            ),
            "{}",
            translation.sql()
        );
        assert!(
            translation.sql().contains(
                "ORDER BY CASE WHEN \"r0\".\"ownership_id\" IS NULL THEN NULL ELSE 'OWNS' END ASC"
            ),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_character_length_and_substring_expressions() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.predicates.clear();
        plan.projections = vec![
            Projection::Expression {
                expression: ScalarExpression::Substring {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    })),
                    start: Box::new(ScalarExpression::Literal(Literal::Integer(0))),
                    length: Some(Box::new(ScalarExpression::Literal(Literal::Integer(7)))),
                },
                alias: "prefix".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::CharacterLength {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    })),
                },
                alias: "tier_length".to_string(),
            },
        ];
        plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::CharacterLength {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                })),
            },
            operator: ComparisonOperator::GreaterThan,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(10))),
        }));
        plan.order_by = vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Substring {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                })),
                start: Box::new(ScalarExpression::Literal(Literal::Integer(0))),
                length: None,
            }),
            direction: OrderDirection::Ascending,
        }];

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("string scalar expressions should lower");

        assert!(
            translation.sql().contains(
                "SELECT SUBSTRING(\"n1\".\"service_name\" FROM (0 + 1) FOR 7) AS \"prefix\", \
                 character_length(\"n1\".\"tier\") AS \"tier_length\""
            ),
            "{}",
            translation.sql()
        );
        assert!(
            translation
                .sql()
                .contains("WHERE character_length(\"n1\".\"service_name\") > 10"),
            "{}",
            translation.sql()
        );
        assert!(
            translation
                .sql()
                .contains("ORDER BY SUBSTRING(\"n1\".\"service_name\" FROM (0 + 1)) ASC"),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_null_if_expressions() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.predicates.clear();
        plan.projections = vec![Projection::Expression {
            expression: ScalarExpression::NullIf {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                })),
                value: Box::new(ScalarExpression::Literal(Literal::String(
                    "prod".to_string(),
                ))),
            },
            alias: "normalized_tier".to_string(),
        }];
        plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::NullIf {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                })),
                value: Box::new(ScalarExpression::Literal(Literal::String(
                    "dev".to_string(),
                ))),
            },
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Null)),
        }));
        plan.order_by = vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::NullIf {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                })),
                value: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                })),
            }),
            direction: OrderDirection::Ascending,
        }];

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("nullIf scalar expressions should lower");

        assert!(
            translation
                .sql()
                .contains("NULLIF(\"n1\".\"tier\", 'prod') AS \"normalized_tier\""),
            "{}",
            translation.sql()
        );
        assert!(
            translation
                .sql()
                .contains("WHERE NULLIF(\"n1\".\"tier\", 'dev') IS NULL"),
            "{}",
            translation.sql()
        );
        assert!(
            translation
                .sql()
                .contains("ORDER BY NULLIF(\"n1\".\"service_name\", \"n1\".\"tier\") ASC"),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_left_right_and_reverse_expressions() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.predicates.clear();
        plan.projections = vec![
            Projection::Expression {
                expression: ScalarExpression::Right {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    })),
                    count: Box::new(ScalarExpression::Literal(Literal::Integer(3))),
                },
                alias: "suffix".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Reverse {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    })),
                },
                alias: "reversed_tier".to_string(),
            },
        ];
        plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Left {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                })),
                count: Box::new(ScalarExpression::Literal(Literal::Integer(7))),
            },
            operator: ComparisonOperator::Equal,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
                "billing".to_string(),
            ))),
        }));
        plan.order_by = vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Reverse {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                })),
            }),
            direction: OrderDirection::Ascending,
        }];

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("left, right, and reverse expressions should lower");

        assert!(
            translation
                .sql()
                .contains("SELECT right(\"n1\".\"service_name\", 3) AS \"suffix\", reverse(\"n1\".\"tier\") AS \"reversed_tier\""),
            "{}",
            translation.sql()
        );
        assert!(
            translation
                .sql()
                .contains("WHERE left(\"n1\".\"service_name\", 7) = 'billing'"),
            "{}",
            translation.sql()
        );
        assert!(
            translation
                .sql()
                .contains("ORDER BY reverse(\"n1\".\"service_name\") ASC"),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_nullable_scalar_cast_expressions() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.predicates.clear();
        plan.projections = vec![
            expression_projection(
                "id_text",
                ScalarExpression::ToStringOrNull {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "id".to_string(),
                    })),
                },
            ),
            expression_projection(
                "risk_float",
                ScalarExpression::ToFloatOrNull {
                    expression: Box::new(service_risk_expression()),
                },
            ),
            expression_projection(
                "active_bool",
                ScalarExpression::ToBooleanOrNull {
                    expression: Box::new(ScalarExpression::Literal(Literal::String(
                        "true".to_string(),
                    ))),
                },
            ),
        ];
        plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::ToIntegerOrNull {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "id".to_string(),
                })),
            },
            operator: ComparisonOperator::GreaterThan,
            rhs: ScalarPredicateRhs::Expression(integer_literal(0)),
        }));
        plan.order_by = vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::ToIntegerOrNull {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "id".to_string(),
                })),
            }),
            direction: OrderDirection::Ascending,
        }];

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("nullable scalar casts should lower");

        for expected in [
            "TRY_CAST(\"n1\".\"id\" AS VARCHAR) AS \"id_text\"",
            "TRY_CAST(\"n1\".\"risk_score\" AS DOUBLE) AS \"risk_float\"",
            "TRY_CAST('true' AS BOOLEAN) AS \"active_bool\"",
            "WHERE TRY_CAST(\"n1\".\"id\" AS BIGINT) > 0",
            "ORDER BY TRY_CAST(\"n1\".\"id\" AS BIGINT) ASC",
        ] {
            assert!(
                translation.sql().contains(expected),
                "{}",
                translation.sql()
            );
        }
    }

    #[test]
    fn lower_graph_plan_renders_numeric_scalar_expressions() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.predicates.clear();
        plan.projections = vec![
            Projection::Expression {
                expression: ScalarExpression::Ceil {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    })),
                },
                alias: "risk_ceiling".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Floor {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    })),
                },
                alias: "risk_floor".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Round {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    })),
                    places: Some(Box::new(ScalarExpression::Literal(Literal::Integer(1)))),
                },
                alias: "risk_rounded".to_string(),
            },
        ];
        plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Abs {
                expression: Box::new(ScalarExpression::Arithmetic {
                    operator: ArithmeticOperator::Subtract,
                    left: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    })),
                    right: Box::new(ScalarExpression::Literal(Literal::Integer(1))),
                }),
            },
            operator: ComparisonOperator::LessThan,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(1))),
        }));
        plan.order_by = vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Round {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                })),
                places: None,
            }),
            direction: OrderDirection::Ascending,
        }];

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("numeric scalar expressions should lower");

        assert!(
            translation
                .sql()
                .contains("SELECT ceil(\"n1\".\"risk_score\") AS \"risk_ceiling\", floor(\"n1\".\"risk_score\") AS \"risk_floor\", round(\"n1\".\"risk_score\", 1) AS \"risk_rounded\""),
            "{}",
            translation.sql()
        );
        assert!(
            translation
                .sql()
                .contains("WHERE abs((\"n1\".\"risk_score\" - 1)) < 1"),
            "{}",
            translation.sql()
        );
        assert!(
            translation
                .sql()
                .contains("ORDER BY round(\"n1\".\"risk_score\") ASC"),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_more_numeric_scalar_expressions() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.predicates.clear();
        plan.projections = vec![
            Projection::Expression {
                expression: ScalarExpression::Sqrt {
                    expression: Box::new(service_risk_expression()),
                },
                alias: "risk_root".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Sign {
                    expression: Box::new(ScalarExpression::Arithmetic {
                        operator: ArithmeticOperator::Subtract,
                        left: Box::new(service_risk_expression()),
                        right: Box::new(ScalarExpression::Literal(Literal::Float(
                            ordered_float::OrderedFloat(0.5),
                        ))),
                    }),
                },
                alias: "risk_sign".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Exp {
                    expression: Box::new(service_risk_expression()),
                },
                alias: "risk_exp".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Log10 {
                    expression: Box::new(service_risk_expression()),
                },
                alias: "risk_log10".to_string(),
            },
        ];
        plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Log {
                expression: Box::new(service_risk_expression()),
            },
            operator: ComparisonOperator::LessThan,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(0))),
        }));
        plan.order_by = vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Sqrt {
                expression: Box::new(service_risk_expression()),
            }),
            direction: OrderDirection::Ascending,
        }];

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("additional numeric scalar expressions should lower");

        assert!(
            translation
                .sql()
                .contains("sqrt(\"n1\".\"risk_score\") AS \"risk_root\""),
            "{}",
            translation.sql()
        );
        assert!(
            translation
                .sql()
                .contains("signum((\"n1\".\"risk_score\" - 0.5)) AS \"risk_sign\""),
            "{}",
            translation.sql()
        );
        assert!(
            translation
                .sql()
                .contains("exp(\"n1\".\"risk_score\") AS \"risk_exp\""),
            "{}",
            translation.sql()
        );
        assert!(
            translation
                .sql()
                .contains("log10(\"n1\".\"risk_score\") AS \"risk_log10\""),
            "{}",
            translation.sql()
        );
        assert!(
            translation
                .sql()
                .contains("WHERE ln(\"n1\".\"risk_score\") < 0"),
            "{}",
            translation.sql()
        );
        assert!(
            translation
                .sql()
                .contains("ORDER BY sqrt(\"n1\".\"risk_score\") ASC"),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_unary_trigonometric_scalar_expressions() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.predicates.clear();
        plan.projections = vec![
            expression_projection(
                "risk_sin",
                ScalarExpression::Sin {
                    expression: Box::new(service_risk_expression()),
                },
            ),
            expression_projection(
                "risk_cos",
                ScalarExpression::Cos {
                    expression: Box::new(service_risk_expression()),
                },
            ),
            expression_projection(
                "risk_tan",
                ScalarExpression::Tan {
                    expression: Box::new(service_risk_expression()),
                },
            ),
            expression_projection(
                "risk_cot",
                ScalarExpression::Cot {
                    expression: Box::new(service_risk_expression()),
                },
            ),
            expression_projection(
                "half_asin",
                ScalarExpression::Asin {
                    expression: Box::new(float_literal(0.5)),
                },
            ),
            expression_projection(
                "one_acos",
                ScalarExpression::Acos {
                    expression: Box::new(float_literal(1.0)),
                },
            ),
            expression_projection(
                "risk_atan",
                ScalarExpression::Atan {
                    expression: Box::new(service_risk_expression()),
                },
            ),
        ];
        plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Sin {
                expression: Box::new(service_risk_expression()),
            },
            operator: ComparisonOperator::GreaterThanOrEqual,
            rhs: ScalarPredicateRhs::Expression(integer_literal(0)),
        }));

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("unary trigonometric scalar expressions should lower");

        for expected in [
            "sin(\"n1\".\"risk_score\") AS \"risk_sin\"",
            "cos(\"n1\".\"risk_score\") AS \"risk_cos\"",
            "tan(\"n1\".\"risk_score\") AS \"risk_tan\"",
            "cot(\"n1\".\"risk_score\") AS \"risk_cot\"",
            "asin(0.5) AS \"half_asin\"",
            "acos(1) AS \"one_acos\"",
            "atan(\"n1\".\"risk_score\") AS \"risk_atan\"",
            "WHERE sin(\"n1\".\"risk_score\") >= 0",
        ] {
            assert!(
                translation.sql().contains(expected),
                "{}",
                translation.sql()
            );
        }
    }

    #[test]
    fn lower_graph_plan_renders_atan2_and_angle_conversion_scalar_expressions() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.predicates.clear();
        plan.projections = vec![
            expression_projection(
                "risk_atan2",
                ScalarExpression::Atan2 {
                    y: Box::new(service_risk_expression()),
                    x: Box::new(integer_literal(1)),
                },
            ),
            expression_projection(
                "risk_degrees",
                ScalarExpression::Degrees {
                    expression: Box::new(service_risk_expression()),
                },
            ),
            expression_projection(
                "pi_radians",
                ScalarExpression::Radians {
                    expression: Box::new(float_literal(180.0)),
                },
            ),
        ];
        plan.order_by = vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Atan2 {
                y: Box::new(service_risk_expression()),
                x: Box::new(integer_literal(1)),
            }),
            direction: OrderDirection::Ascending,
        }];

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("angle conversion scalar expressions should lower");

        for expected in [
            "atan2(\"n1\".\"risk_score\", 1) AS \"risk_atan2\"",
            "degrees(\"n1\".\"risk_score\") AS \"risk_degrees\"",
            "radians(180) AS \"pi_radians\"",
            "ORDER BY atan2(\"n1\".\"risk_score\", 1) ASC",
        ] {
            assert!(
                translation.sql().contains(expected),
                "{}",
                translation.sql()
            );
        }
    }

    #[test]
    fn lower_graph_plan_renders_unary_negation_scalar_expressions() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.predicates.clear();
        plan.projections = vec![
            Projection::Expression {
                expression: ScalarExpression::Negate {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    })),
                },
                alias: "inverse_risk".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Negate {
                    expression: Box::new(ScalarExpression::Arithmetic {
                        operator: ArithmeticOperator::Multiply,
                        left: Box::new(ScalarExpression::Property(PropertyRef {
                            variable: "service".to_string(),
                            property: "risk".to_string(),
                        })),
                        right: Box::new(ScalarExpression::Literal(Literal::Integer(100))),
                    }),
                },
                alias: "inverse_points".to_string(),
            },
        ];
        plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Negate {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                })),
            },
            operator: ComparisonOperator::LessThan,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(0))),
        }));
        plan.order_by = vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Negate {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                })),
            }),
            direction: OrderDirection::Ascending,
        }];

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("unary negation scalar expressions should lower");

        assert!(
            translation.sql().contains(
                "SELECT -(\"n1\".\"risk_score\") AS \"inverse_risk\", \
                 -((\"n1\".\"risk_score\" * 100)) AS \"inverse_points\""
            ),
            "{}",
            translation.sql()
        );
        assert!(
            translation
                .sql()
                .contains("WHERE -(\"n1\".\"risk_score\") < 0"),
            "{}",
            translation.sql()
        );
        assert!(
            translation
                .sql()
                .contains("ORDER BY -(\"n1\".\"risk_score\") ASC"),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_power_arithmetic_expressions() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.predicates.clear();
        plan.projections = vec![Projection::Expression {
            expression: ScalarExpression::Arithmetic {
                operator: ArithmeticOperator::Power,
                left: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                })),
                right: Box::new(ScalarExpression::Literal(Literal::Integer(2))),
            },
            alias: "risk_squared".to_string(),
        }];
        plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::Arithmetic {
                operator: ArithmeticOperator::Power,
                left: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                })),
                right: Box::new(ScalarExpression::Literal(Literal::Integer(2))),
            },
            operator: ComparisonOperator::GreaterThan,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Float(
                ordered_float::OrderedFloat(0.5),
            ))),
        }));
        plan.order_by = vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::Arithmetic {
                operator: ArithmeticOperator::Power,
                left: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "risk".to_string(),
                })),
                right: Box::new(ScalarExpression::Literal(Literal::Integer(2))),
            }),
            direction: OrderDirection::Descending,
        }];

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("power arithmetic expressions should lower");

        assert!(
            translation
                .sql()
                .contains("SELECT power(\"n1\".\"risk_score\", 2) AS \"risk_squared\""),
            "{}",
            translation.sql()
        );
        assert!(
            translation
                .sql()
                .contains("WHERE power(\"n1\".\"risk_score\", 2) > 0.5"),
            "{}",
            translation.sql()
        );
        assert!(
            translation
                .sql()
                .contains("ORDER BY power(\"n1\".\"risk_score\", 2) DESC"),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_or_predicate_expressions() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.predicates.clear();
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
                    property: "tier".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::Null),
            })),
        });

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("OR predicate expression should lower");

        assert!(
            translation
                .sql()
                .contains("WHERE (\"n1\".\"tier\" = 'prod' OR \"n1\".\"tier\" IS NULL)"),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_xor_predicate_expressions() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.predicates.clear();
        plan.predicate = Some(PredicateExpression::Xor {
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
                    property: "tier".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::Null),
            })),
        });

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("XOR predicate expression should lower");

        assert!(
            translation.sql().contains(
                "WHERE ((\"n1\".\"tier\" = 'prod' AND NOT (\"n1\".\"tier\" IS NULL)) OR (NOT (\"n1\".\"tier\" = 'prod') AND \"n1\".\"tier\" IS NULL))"
            ),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_not_predicate_expressions() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.predicates.clear();
        plan.predicate = Some(PredicateExpression::Not {
            expression: Box::new(PredicateExpression::Comparison(PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
            })),
        });

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("NOT predicate expression should lower");

        assert!(
            translation
                .sql()
                .contains("WHERE NOT (\"n1\".\"tier\" = 'prod')"),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_boolean_constant_predicates() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.predicates.clear();
        plan.predicate = Some(PredicateExpression::Or {
            left: Box::new(PredicateExpression::Comparison(PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
            })),
            right: Box::new(PredicateExpression::Boolean(false)),
        });

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("constant boolean predicate expression should lower");

        assert!(
            translation
                .sql()
                .contains("WHERE (\"n1\".\"tier\" = 'prod' OR FALSE)"),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_combines_conjunctive_vector_and_predicate_expression() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.predicate = Some(PredicateExpression::Or {
            left: Box::new(PredicateExpression::Comparison(PropertyPredicate {
                property: PropertyRef {
                    variable: "person".to_string(),
                    property: "team".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("platform".to_string())),
            })),
            right: Box::new(PredicateExpression::Comparison(PropertyPredicate {
                property: PropertyRef {
                    variable: "person".to_string(),
                    property: "team".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("infra".to_string())),
            })),
        });

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("conjunctive vector plus predicate expression should lower");

        assert!(
            translation.sql().contains(
                "WHERE \"n1\".\"tier\" = 'prod' AND (\"n0\".\"team\" = 'platform' OR \"n0\".\"team\" = 'infra')"
            ),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_distinct_projection() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.distinct = true;
        plan.projections = vec![Projection::Property {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            },
            alias: Some("tier".to_string()),
        }];
        plan.order_by = vec![OrderKey {
            expression: OrderExpression::Property(PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            }),
            direction: OrderDirection::Ascending,
        }];
        plan.limit = None;

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("distinct plan should lower");

        assert!(
            translation
                .sql()
                .starts_with("SELECT DISTINCT \"n1\".\"tier\" AS \"tier\""),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_offset() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.skip = Some(5);

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("offset plan should lower");

        assert!(
            translation.sql().ends_with(" LIMIT 25 OFFSET 5"),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_grouped_count_projection() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.projections.push(Projection::CountAll {
            alias: "ownership_count".to_string(),
        });

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("grouped aggregate projection should lower");

        assert!(
            translation.sql().contains(
                " GROUP BY \"n0\".\"full_name\", \"n1\".\"service_name\" ORDER BY \"n0\".\"full_name\" ASC"
            ),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_orders_by_count_alias() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.projections.push(Projection::CountAll {
            alias: "ownership_count".to_string(),
        });
        plan.order_by = vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("ownership_count".to_string()),
            direction: OrderDirection::Descending,
        }];

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("aggregate alias ordering should lower");

        assert!(
            translation
                .sql()
                .contains(" ORDER BY \"ownership_count\" DESC"),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_count_property_projection() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.projections.push(Projection::Aggregate {
            function: AggregateFunction::Count,
            target: AggregateTarget::Property(PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            }),
            distinct: true,
            alias: "tier_count".to_string(),
        });

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("count property projection should lower");

        assert!(
            translation
                .sql()
                .contains("COUNT(DISTINCT \"n1\".\"tier\") AS \"tier_count\""),
            "{}",
            translation.sql()
        );
        assert!(
            translation.sql().contains(" GROUP BY "),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_collect_property_projection() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.projections.push(Projection::Aggregate {
            function: AggregateFunction::Collect,
            target: AggregateTarget::Property(PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            }),
            distinct: true,
            alias: "services".to_string(),
        });

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("collect property projection should lower");

        assert!(
            translation
                .sql()
                .contains("ARRAY_AGG(DISTINCT \"n1\".\"service_name\") AS \"services\""),
            "{}",
            translation.sql()
        );
        assert!(
            translation.sql().contains(" GROUP BY "),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_numeric_aggregate_projections() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.projections.push(Projection::Aggregate {
            function: AggregateFunction::Sum,
            target: AggregateTarget::Property(PropertyRef {
                variable: "service".to_string(),
                property: "risk".to_string(),
            }),
            distinct: false,
            alias: "total_risk".to_string(),
        });
        plan.projections.push(Projection::Aggregate {
            function: AggregateFunction::Avg,
            target: AggregateTarget::Property(PropertyRef {
                variable: "service".to_string(),
                property: "risk".to_string(),
            }),
            distinct: false,
            alias: "average_risk".to_string(),
        });
        plan.projections.push(Projection::Aggregate {
            function: AggregateFunction::Min,
            target: AggregateTarget::Property(PropertyRef {
                variable: "service".to_string(),
                property: "risk".to_string(),
            }),
            distinct: false,
            alias: "lowest_risk".to_string(),
        });
        plan.projections.push(Projection::Aggregate {
            function: AggregateFunction::Max,
            target: AggregateTarget::Property(PropertyRef {
                variable: "service".to_string(),
                property: "risk".to_string(),
            }),
            distinct: true,
            alias: "highest_risk".to_string(),
        });

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("numeric aggregate projections should lower");

        assert!(
            translation.sql().contains(
                "SUM(\"n1\".\"risk_score\") AS \"total_risk\", \
                 AVG(\"n1\".\"risk_score\") AS \"average_risk\", \
                 MIN(\"n1\".\"risk_score\") AS \"lowest_risk\", \
                 MAX(DISTINCT \"n1\".\"risk_score\") AS \"highest_risk\""
            ),
            "{}",
            translation.sql()
        );
        assert!(
            translation.sql().contains(" GROUP BY "),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_statistical_aggregate_projections() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.projections.push(Projection::Aggregate {
            function: AggregateFunction::Median,
            target: AggregateTarget::Property(PropertyRef {
                variable: "service".to_string(),
                property: "risk".to_string(),
            }),
            distinct: false,
            alias: "median_risk".to_string(),
        });
        plan.projections.push(Projection::Aggregate {
            function: AggregateFunction::StdDev,
            target: AggregateTarget::Property(PropertyRef {
                variable: "service".to_string(),
                property: "risk".to_string(),
            }),
            distinct: false,
            alias: "sample_risk".to_string(),
        });
        plan.projections.push(Projection::Aggregate {
            function: AggregateFunction::StdDevP,
            target: AggregateTarget::Property(PropertyRef {
                variable: "service".to_string(),
                property: "risk".to_string(),
            }),
            distinct: false,
            alias: "population_risk".to_string(),
        });
        plan.projections.push(Projection::Aggregate {
            function: AggregateFunction::Median,
            target: AggregateTarget::Property(PropertyRef {
                variable: "service".to_string(),
                property: "risk".to_string(),
            }),
            distinct: true,
            alias: "distinct_median_risk".to_string(),
        });

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("statistical aggregate projections should lower");

        assert!(
            translation.sql().contains(
                "MEDIAN(\"n1\".\"risk_score\") AS \"median_risk\", \
                 STDDEV_SAMP(\"n1\".\"risk_score\") AS \"sample_risk\", \
                 STDDEV_POP(\"n1\".\"risk_score\") AS \"population_risk\", \
                 MEDIAN(DISTINCT \"n1\".\"risk_score\") AS \"distinct_median_risk\""
            ),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_count_node_projection() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.projections = vec![Projection::Aggregate {
            function: AggregateFunction::Count,
            target: AggregateTarget::VariableKey {
                variable: "service".to_string(),
            },
            distinct: true,
            alias: "service_count".to_string(),
        }];
        plan.order_by = vec![OrderKey {
            expression: OrderExpression::ProjectionAlias("service_count".to_string()),
            direction: OrderDirection::Descending,
        }];

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("count node projection should lower");

        assert!(
            translation
                .sql()
                .contains("COUNT(DISTINCT \"n1\".\"id\") AS \"service_count\""),
            "{}",
            translation.sql()
        );
        assert!(
            translation
                .sql()
                .contains(" ORDER BY \"service_count\" DESC"),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_count_keyed_relationship_projection() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.relationships
            .first_mut()
            .expect("ownership plan should include a relationship")
            .variable = Some("owns".to_string());
        plan.projections = vec![Projection::Aggregate {
            function: AggregateFunction::Count,
            target: AggregateTarget::VariableKey {
                variable: "owns".to_string(),
            },
            distinct: true,
            alias: "ownership_count".to_string(),
        }];
        plan.order_by.clear();

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("count keyed relationship projection should lower");

        assert!(
            translation
                .sql()
                .contains("COUNT(DISTINCT \"r0\".\"ownership_id\") AS \"ownership_count\""),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_count_keyless_relationship_projection() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let plan = GraphPlan {
            nodes: vec![
                NodePattern {
                    variable: "source".to_string(),
                    label: "Service".to_string(),
                },
                NodePattern {
                    variable: "target".to_string(),
                    label: "Service".to_string(),
                },
            ],
            relationships: vec![RelationshipPattern {
                variable: Some("dependency".to_string()),
                relationship_type: "DEPENDS_ON".to_string(),
                left: "source".to_string(),
                direction: Direction::Outgoing,
                right: "target".to_string(),
            }],
            optional_relationships: Vec::new(),
            optional_matches: Vec::new(),
            distinct: false,
            projections: vec![Projection::Aggregate {
                function: AggregateFunction::Count,
                target: AggregateTarget::VariableKey {
                    variable: "dependency".to_string(),
                },
                distinct: false,
                alias: "dependency_count".to_string(),
            }],
            predicates: Vec::new(),
            predicate: None,
            post_projection_predicate: None,
            order_by: Vec::new(),
            skip: None,
            limit: None,
        };

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("count keyless relationship projection should lower");

        assert!(
            translation
                .sql()
                .contains("COUNT(\"r0\".\"from_service_id\") AS \"dependency_count\""),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_renders_presence_predicate_for_keyless_relationship() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let plan = GraphPlan {
            nodes: vec![
                NodePattern {
                    variable: "source".to_string(),
                    label: "Service".to_string(),
                },
                NodePattern {
                    variable: "target".to_string(),
                    label: "Service".to_string(),
                },
            ],
            relationships: vec![RelationshipPattern {
                variable: Some("dependency".to_string()),
                relationship_type: "DEPENDS_ON".to_string(),
                left: "source".to_string(),
                direction: Direction::Outgoing,
                right: "target".to_string(),
            }],
            optional_relationships: Vec::new(),
            optional_matches: Vec::new(),
            distinct: false,
            projections: vec![Projection::Property {
                property: PropertyRef {
                    variable: "source".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("source".to_string()),
            }],
            predicates: Vec::new(),
            predicate: Some(PredicateExpression::Presence(PresencePredicate {
                variable: "dependency".to_string(),
                operator: ComparisonOperator::NotEqual,
            })),
            post_projection_predicate: None,
            order_by: Vec::new(),
            skip: None,
            limit: None,
        };

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("keyless relationship presence predicate should lower");

        assert!(
            translation
                .sql()
                .contains("\"r0\".\"from_service_id\" IS NOT NULL"),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_rejects_count_with_property_ordering() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.projections = vec![Projection::CountAll {
            alias: "ownership_count".to_string(),
        }];

        let error = graph
            .lower_graph_plan(&plan)
            .expect_err("count with property ordering should fail");

        assert!(
            error.to_string().contains("UNSUPPORTED_AGGREGATION"),
            "{error:?}"
        );
    }

    #[test]
    fn lower_graph_plan_rejects_ordered_null_comparisons() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        let predicate = plan
            .predicates
            .get_mut(0)
            .expect("ownership fixture should include a predicate");
        predicate.operator = ComparisonOperator::GreaterThan;
        predicate.rhs = PredicateRhs::Literal(Literal::Null);

        let error = graph
            .lower_graph_plan(&plan)
            .expect_err("ordered null comparison should fail");

        assert!(
            error.to_string().contains("INVALID_NULL_COMPARISON"),
            "{error:?}"
        );
    }

    #[test]
    fn lower_graph_plan_rejects_endpoint_mismatch() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        let service_node = plan
            .nodes
            .get_mut(1)
            .expect("ownership fixture should include a service node");
        service_node.label = "Person".to_string();

        let error = graph
            .lower_graph_plan(&plan)
            .expect_err("endpoint mismatch should fail");

        assert!(
            error.to_string().contains("RELATIONSHIP_ENDPOINT_MISMATCH"),
            "{error:?}"
        );
    }

    fn ownership_plan(direction: Direction) -> GraphPlan {
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
                variable: None,
                relationship_type: "OWNS".to_string(),
                left: "person".to_string(),
                direction,
                right: "service".to_string(),
            }],
            optional_relationships: Vec::new(),
            optional_matches: Vec::new(),
            distinct: false,
            projections: vec![
                Projection::Property {
                    property: PropertyRef {
                        variable: "person".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("owner".to_string()),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("service".to_string()),
                },
            ],
            predicates: vec![PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
            }],
            predicate: None,
            post_projection_predicate: None,
            order_by: vec![OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "person".to_string(),
                    property: "name".to_string(),
                }),
                direction: OrderDirection::Ascending,
            }],
            skip: None,
            limit: Some(25),
        }
    }

    fn service_risk_expression() -> ScalarExpression {
        ScalarExpression::Property(PropertyRef {
            variable: "service".to_string(),
            property: "risk".to_string(),
        })
    }

    fn service_name_expression() -> ScalarExpression {
        ScalarExpression::Property(PropertyRef {
            variable: "service".to_string(),
            property: "name".to_string(),
        })
    }

    fn integer_literal(value: i64) -> ScalarExpression {
        ScalarExpression::Literal(Literal::Integer(value))
    }

    fn float_literal(value: f64) -> ScalarExpression {
        ScalarExpression::Literal(Literal::Float(ordered_float::OrderedFloat(value)))
    }

    fn expression_projection(alias: &str, expression: ScalarExpression) -> Projection {
        Projection::Expression {
            expression,
            alias: alias.to_string(),
        }
    }
}
