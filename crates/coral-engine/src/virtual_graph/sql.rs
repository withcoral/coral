use std::collections::BTreeSet;
use std::fmt::Write as _;

use super::declaration::{Declaration, Relationship, TableRef};
use super::diagnostic::Diagnostic;
use super::ir::{
    AggregateFunction, AggregateTarget, ComparisonOperator, Direction, GraphPlan, KeyPredicate,
    Literal, OrderDirection, OrderExpression, PredicateExpression, PredicateRhs, Projection,
    ProjectionPredicate, ProjectionPredicateExpression, ProjectionPredicateRhs, PropertyRef,
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
        let validated = &self.validated;
        let first_node = plan
            .nodes
            .first()
            .ok_or_else(|| CoreError::internal("validated graph plan had no nodes"))?;
        let first_binding = validated.binding(first_node.variable.as_str())?;
        let ValidatedBindingKind::Node(first_node_mapping) = first_binding.kind() else {
            return Err(CoreError::internal("first graph binding was not a node"));
        };
        self.from_clause = format!(
            "FROM {} AS {}",
            render_table_ref(&first_node_mapping.table),
            quote_ident(first_binding.alias())
        );
        self.joined_nodes.insert(first_node.variable.as_str());

        self.join_relationships(false)?;
        self.join_relationships(true)?;

        for node in &plan.nodes {
            if !self.joined_nodes.contains(node.variable.as_str()) {
                return Err(CoreError::internal(
                    "validated graph plan contained a disconnected node",
                ));
            }
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
            if !progressed {
                return Err(CoreError::internal(
                    "validated graph plan contained an unjoinable relationship",
                ));
            }
        }
        Ok(())
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
            let outer_condition =
                Self::join_condition_with_predicate(relationship_join, Some(optional_predicate));
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
            match projection {
                Projection::Property { property, alias } => {
                    let expression = self.render_property_ref(property)?;
                    let alias = alias
                        .clone()
                        .unwrap_or_else(|| format!("{}_{}", property.variable, property.property));
                    rendered.push(format!("{expression} AS {}", quote_ident(&alias)));
                }
                Projection::Key { variable, alias } => {
                    rendered.push(format!(
                        "{} AS {}",
                        self.render_binding_key_ref(variable)?,
                        quote_ident(alias)
                    ));
                }
                Projection::Literal { literal, alias } => {
                    rendered.push(format!(
                        "{} AS {}",
                        render_literal(literal),
                        quote_ident(alias)
                    ));
                }
                Projection::CountAll { alias } => {
                    rendered.push(format!("COUNT(*) AS {}", quote_ident(alias)));
                }
                Projection::Aggregate {
                    function,
                    target,
                    distinct,
                    alias,
                } => {
                    rendered.push(format!(
                        "{}({}{}) AS {}",
                        render_aggregate_function(*function),
                        if *distinct { "DISTINCT " } else { "" },
                        self.render_aggregate_target(target)?,
                        quote_ident(alias)
                    ));
                }
            }
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
        if !self
            .validated
            .plan()
            .projections
            .iter()
            .any(Projection::is_aggregate)
        {
            return Ok(String::new());
        }

        let properties = self
            .validated
            .plan()
            .projections
            .iter()
            .filter_map(|projection| match projection {
                Projection::Property { property, .. } => Some(property),
                Projection::Key { .. }
                | Projection::Literal { .. }
                | Projection::CountAll { .. }
                | Projection::Aggregate { .. } => None,
            })
            .map(|property| self.render_property_ref(property))
            .chain(
                self.validated
                    .plan()
                    .projections
                    .iter()
                    .filter_map(|projection| match projection {
                        Projection::Key { variable, .. } => Some(variable),
                        Projection::Property { .. }
                        | Projection::Literal { .. }
                        | Projection::CountAll { .. }
                        | Projection::Aggregate { .. } => None,
                    })
                    .map(|variable| self.render_binding_key_ref(variable)),
            )
            .collect::<Result<Vec<_>, _>>()?;
        if properties.is_empty() {
            Ok(String::new())
        } else {
            Ok(format!(" GROUP BY {}", properties.join(", ")))
        }
    }

    fn render_predicate_expression(
        &self,
        predicate: &PredicateExpression,
    ) -> Result<String, CoreError> {
        match predicate {
            PredicateExpression::Boolean(value) => Ok(value.to_string().to_uppercase()),
            PredicateExpression::Comparison(predicate) => self.render_predicate(predicate),
            PredicateExpression::KeyComparison(predicate) => self.render_key_predicate(predicate),
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
            PredicateRhs::List(_) => Err(CoreError::internal(
                "validated literal list predicate reached generic RHS renderer",
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
            OrderExpression::Literal(literal) => Ok(render_literal(literal)),
            OrderExpression::ProjectionAlias(alias) => Ok(quote_ident(alias)),
        }
    }

    fn render_aggregate_target(&self, target: &AggregateTarget) -> Result<String, CoreError> {
        match target {
            AggregateTarget::Property(property) => self.render_property_ref(property),
            AggregateTarget::VariableKey { variable } => self.render_binding_key_ref(variable),
        }
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
            Projection::Literal { literal, .. } => Ok(render_literal(literal)),
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
                self.render_aggregate_target(target)?
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
    }
}

fn render_aggregate_function(function: AggregateFunction) -> &'static str {
    match function {
        AggregateFunction::Count => "COUNT",
        AggregateFunction::Sum => "SUM",
        AggregateFunction::Avg => "AVG",
        AggregateFunction::Min => "MIN",
        AggregateFunction::Max => "MAX",
    }
}

fn projection_output_alias(projection: &Projection) -> Option<&str> {
    match projection {
        Projection::Property { alias, .. } => alias.as_deref(),
        Projection::Key { alias, .. }
        | Projection::Literal { alias, .. }
        | Projection::CountAll { alias }
        | Projection::Aggregate { alias, .. } => Some(alias),
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
    fn lower_graph_plan_renders_key_and_literal_projections() {
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
            Projection::Key {
                variable: "owns".to_string(),
                alias: "ownership_id".to_string(),
            },
            Projection::Literal {
                literal: Literal::String("OWNS".to_string()),
                alias: "relationship_type".to_string(),
            },
        ];

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("key and literal projections should lower");

        assert_eq!(
            translation.sql(),
            "SELECT \"n0\".\"id\" AS \"person_id\", \"r0\".\"ownership_id\" AS \"ownership_id\", 'OWNS' AS \"relationship_type\" \
             FROM \"ops\".\"people\" AS \"n0\" \
             JOIN \"ops\".\"ownerships\" AS \"r0\" ON \"r0\".\"person_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n1\" ON \"r0\".\"service_id\" = \"n1\".\"id\" \
             WHERE \"n1\".\"tier\" = 'prod' ORDER BY \"n0\".\"full_name\" ASC LIMIT 25"
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
}
