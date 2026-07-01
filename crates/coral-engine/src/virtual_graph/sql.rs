use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use super::declaration::{Declaration, Node, Relationship, TableRef};
use super::diagnostic::Diagnostic;
use super::ir::{
    AggregateFunction, AggregateTarget, ArithmeticOperator, ComparisonOperator,
    CountSubqueryPattern, Direction, ElementIdPredicate, ExistsPatternPredicate, GraphPlan,
    GraphQuery, GraphUnion, GraphUnionOuterProjectionItem, KeyPredicate, Literal,
    LiteralListElementType, NodePattern, NullOrder, OptionalMatchScope, OrderDirection,
    OrderExpression, PredicateExpression, PredicateRhs, PresencePredicate, Projection,
    ProjectionPredicate, ProjectionPredicateExpression, ProjectionPredicateRhs,
    PropertyKeyMembershipPredicate, PropertyPredicate, PropertyRef, RelationshipPattern,
    ScalarCaseAlternative, ScalarExpression, ScalarPredicate, ScalarPredicateRhs,
    UndirectedRelationshipEndpoint,
};
use super::validation::{ValidatedBindingKind, ValidatedGraphPlan};
use crate::CoreError;

mod joins;
mod metadata;
mod render;
mod scalar;
mod subqueries;

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "SQL render helpers are split into a child module while preserving parent call sites."
)]
use self::render::*;

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

    /// Lowers a read-only virtual graph query into `DataFusion` SQL.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidInput`] when any branch plan is invalid for
    /// this declaration or when the query shape is not supported by the lowerer.
    pub fn lower_graph_query(&self, query: &GraphQuery) -> Result<SqlTranslation, CoreError> {
        match query {
            GraphQuery::Plan(plan) => self.lower_graph_plan(plan),
            GraphQuery::Union(union) => self.lower_graph_union(union),
        }
    }

    fn lower_graph_union(&self, union: &GraphUnion) -> Result<SqlTranslation, CoreError> {
        if union.branches.is_empty() {
            return Err(CoreError::internal("graph union had no union branches"));
        }

        let expected_names = union.first.projection_output_names();
        let mut diagnostics = Vec::new();
        let first = self.lower_graph_plan(&union.first)?;
        diagnostics.extend(first.diagnostics().iter().cloned());
        let mut sql = render_union_branch_sql(first.sql(), 0);

        for (index, branch) in union.branches.iter().enumerate() {
            validate_union_branch_output_names(
                &expected_names,
                &branch.plan.projection_output_names(),
                index,
            )?;
            let translation = self.lower_graph_plan(&branch.plan)?;
            diagnostics.extend(translation.diagnostics().iter().cloned());
            write!(
                sql,
                " {} {}",
                if branch.all { "UNION ALL" } else { "UNION" },
                render_union_branch_sql(translation.sql(), index + 1)
            )
            .map_err(|_| CoreError::internal("failed to render graph union SQL"))?;
        }

        let sql = render_union_outer_sql(sql, union)?;
        Ok(SqlTranslation::new(sql, diagnostics))
    }
}

struct Lowerer<'a> {
    validated: ValidatedGraphPlan<'a>,
    joined_nodes: BTreeSet<&'a str>,
    optional_relationships_joined: bool,
    from_clause: String,
    precomputed_scalar_subqueries: Vec<PrecomputedScalarSubquery>,
    next_scalar_subquery_alias: Cell<usize>,
}

#[derive(Debug, Clone)]
struct ExistsRelationshipSqlBinding<'a, 'b> {
    pattern: &'b RelationshipPattern,
    relationship: &'a Relationship,
    alias: String,
}

#[derive(Debug, Clone)]
struct RelationshipOrientation {
    left_relationship_key: String,
    right_relationship_key: String,
}

#[derive(Debug, Clone)]
struct UndirectedEndpointSelection {
    presence: String,
    left_matches_endpoint: String,
    left_key: String,
    right_key: String,
    left_variable: String,
    right_variable: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ScalarSubqueryCandidate {
    Count {
        pattern: CountSubqueryPattern,
        distinct_target: Option<ScalarExpression>,
    },
    Exists(ExistsPatternPredicate),
    Collect {
        pattern: ExistsPatternPredicate,
        target: ScalarExpression,
        distinct: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CountExistencePredicate {
    Exists,
    NotExists,
    AlwaysTrue,
    AlwaysFalse,
}

#[derive(Debug, Clone)]
struct PrecomputedScalarSubquery {
    candidate: ScalarSubqueryCandidate,
    table_alias: String,
    outer_key_alias: String,
    value_alias: String,
}

#[derive(Debug, Clone)]
struct PrecomputedNodeCorrelation {
    predicate_index: usize,
    local_expression: String,
    outer_expression: String,
}

#[derive(Debug, Clone)]
enum NodeCorrelationOperand {
    Local(String),
    Outer(String),
}

#[derive(Debug, Clone)]
struct ScalarSubqueryCandidateUse {
    candidate: ScalarSubqueryCandidate,
    required: bool,
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

#[derive(Debug, Clone, Copy)]
struct OptionalScopeAnchor<'a> {
    relationship_index: usize,
    anchor_variable: &'a str,
    anchor_is_left: bool,
}

impl<'a> Lowerer<'a> {
    fn new(validated: ValidatedGraphPlan<'a>) -> Self {
        Self {
            validated,
            joined_nodes: BTreeSet::new(),
            optional_relationships_joined: false,
            from_clause: String::new(),
            precomputed_scalar_subqueries: Vec::new(),
            next_scalar_subquery_alias: Cell::new(0),
        }
    }

    fn lower(mut self) -> Result<SqlTranslation, CoreError> {
        self.build_from_clause()?;
        self.join_precomputed_scalar_subqueries()?;

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

    fn scoped_predicates_are_precomputable<'b>(
        predicate: &ExistsPatternPredicate,
        relationship_bindings: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
    ) -> bool {
        predicate.predicates.iter().all(|predicate| {
            Self::scoped_property_predicate_is_inner(predicate, relationship_bindings, local_nodes)
        }) && predicate.predicate.as_deref().is_none_or(|predicate| {
            Self::scoped_predicate_expression_is_inner(
                predicate,
                relationship_bindings,
                local_nodes,
            )
        })
    }

    fn scoped_property_predicate_is_inner<'b>(
        predicate: &PropertyPredicate,
        relationship_bindings: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
    ) -> bool {
        Self::scoped_variable_is_inner(
            &predicate.property.variable,
            relationship_bindings,
            local_nodes,
        ) && Self::scoped_predicate_rhs_is_inner(&predicate.rhs, relationship_bindings, local_nodes)
    }

    fn scoped_predicate_rhs_is_inner<'b>(
        rhs: &PredicateRhs,
        relationship_bindings: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
    ) -> bool {
        match rhs {
            PredicateRhs::Property(property) => Self::scoped_variable_is_inner(
                &property.variable,
                relationship_bindings,
                local_nodes,
            ),
            PredicateRhs::Key { variable } | PredicateRhs::ElementId { variable } => {
                Self::scoped_variable_is_inner(variable, relationship_bindings, local_nodes)
            }
            PredicateRhs::Literal(_) | PredicateRhs::List(_) => true,
        }
    }

    fn scoped_predicate_expression_is_inner<'b>(
        predicate: &PredicateExpression,
        relationship_bindings: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
    ) -> bool {
        match predicate {
            PredicateExpression::Boolean(_) => true,
            PredicateExpression::Comparison(predicate) => Self::scoped_property_predicate_is_inner(
                predicate,
                relationship_bindings,
                local_nodes,
            ),
            PredicateExpression::KeyComparison(predicate) => {
                Self::scoped_variable_is_inner(
                    &predicate.variable,
                    relationship_bindings,
                    local_nodes,
                ) && Self::scoped_predicate_rhs_is_inner(
                    &predicate.rhs,
                    relationship_bindings,
                    local_nodes,
                )
            }
            PredicateExpression::ElementIdComparison(predicate) => {
                Self::scoped_variable_is_inner(
                    &predicate.variable,
                    relationship_bindings,
                    local_nodes,
                ) && Self::scoped_predicate_rhs_is_inner(
                    &predicate.rhs,
                    relationship_bindings,
                    local_nodes,
                )
            }
            PredicateExpression::Presence(predicate) => Self::scoped_variable_is_inner(
                &predicate.variable,
                relationship_bindings,
                local_nodes,
            ),
            PredicateExpression::PropertyKeyMembership(predicate) => {
                Self::scoped_variable_is_inner(
                    &predicate.variable,
                    relationship_bindings,
                    local_nodes,
                )
            }
            PredicateExpression::ScalarComparison(predicate) => {
                Self::scoped_scalar_expression_is_inner(
                    &predicate.lhs,
                    relationship_bindings,
                    local_nodes,
                ) && match &predicate.rhs {
                    ScalarPredicateRhs::Expression(expression) => {
                        Self::scoped_scalar_expression_is_inner(
                            expression,
                            relationship_bindings,
                            local_nodes,
                        )
                    }
                    ScalarPredicateRhs::List(_) => true,
                }
            }
            PredicateExpression::And { left, right }
            | PredicateExpression::Or { left, right }
            | PredicateExpression::Xor { left, right } => {
                Self::scoped_predicate_expression_is_inner(left, relationship_bindings, local_nodes)
                    && Self::scoped_predicate_expression_is_inner(
                        right,
                        relationship_bindings,
                        local_nodes,
                    )
            }
            PredicateExpression::Not { expression } => Self::scoped_predicate_expression_is_inner(
                expression,
                relationship_bindings,
                local_nodes,
            ),
            PredicateExpression::ExistsPattern(_) => false,
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "This exhaustive scoped scalar IR dispatcher keeps inner/outer binding checks total over every scalar variant"
    )]
    fn scoped_scalar_expression_is_inner<'b>(
        expression: &ScalarExpression,
        relationship_bindings: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
    ) -> bool {
        if let Some(expression) = scalar_expression_unary_operand(expression) {
            return Self::scoped_scalar_expression_is_inner(
                expression,
                relationship_bindings,
                local_nodes,
            );
        }
        match expression {
            ScalarExpression::Property(property) => Self::scoped_variable_is_inner(
                &property.variable,
                relationship_bindings,
                local_nodes,
            ),
            ScalarExpression::UndirectedEndpointProperty { relationship, .. }
            | ScalarExpression::UndirectedEndpointKey { relationship, .. }
            | ScalarExpression::UndirectedEndpointElementId { relationship, .. }
            | ScalarExpression::UndirectedEndpointLabels { relationship, .. }
            | ScalarExpression::UndirectedEndpointPropertyKeys { relationship, .. } => {
                Self::scoped_variable_is_inner(relationship, relationship_bindings, local_nodes)
            }
            ScalarExpression::Predicate(predicate) => Self::scoped_predicate_expression_is_inner(
                predicate,
                relationship_bindings,
                local_nodes,
            ),
            ScalarExpression::Key { variable }
            | ScalarExpression::ElementId { variable }
            | ScalarExpression::GraphIdentity { variable }
            | ScalarExpression::GraphPresence { variable }
            | ScalarExpression::NodeLabels { variable, .. }
            | ScalarExpression::PropertyKeys { variable }
            | ScalarExpression::RelationshipType { variable, .. } => {
                Self::scoped_variable_is_inner(variable, relationship_bindings, local_nodes)
            }
            ScalarExpression::GraphKeyList { variables } => variables.iter().all(|variable| {
                Self::scoped_variable_is_inner(variable, relationship_bindings, local_nodes)
            }),
            ScalarExpression::PresenceGated { .. }
            | ScalarExpression::Coalesce { .. }
            | ScalarExpression::NullIf { .. }
            | ScalarExpression::Round { .. }
            | ScalarExpression::Left { .. }
            | ScalarExpression::Right { .. }
            | ScalarExpression::StringIndices { .. }
            | ScalarExpression::LPad { .. }
            | ScalarExpression::RPad { .. }
            | ScalarExpression::StringContains { .. }
            | ScalarExpression::StringStartsWith { .. }
            | ScalarExpression::StringEndsWith { .. }
            | ScalarExpression::Replace { .. }
            | ScalarExpression::Substring { .. }
            | ScalarExpression::Arithmetic { .. }
            | ScalarExpression::Case { .. }
            | ScalarExpression::Atan2 { .. } => Self::scoped_structural_scalar_expression_is_inner(
                expression,
                relationship_bindings,
                local_nodes,
            ),
            ScalarExpression::Literal(_)
            | ScalarExpression::LiteralList { .. }
            | ScalarExpression::TypedLiteralList { .. } => true,
            ScalarExpression::CountSubquery { .. } | ScalarExpression::CollectSubquery { .. } => {
                false
            }
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
            | ScalarExpression::Negate { .. } => {
                unreachable!("unary scalar expressions handled before scoped check")
            }
        }
    }

    fn scoped_structural_scalar_expression_is_inner<'b>(
        expression: &ScalarExpression,
        relationship_bindings: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
    ) -> bool {
        if let Some((left, right)) = Self::structural_scalar_binary_operands(expression) {
            return Self::scoped_scalar_pair_is_inner(
                left,
                right,
                relationship_bindings,
                local_nodes,
            );
        }
        if let Some((first, second, third)) = Self::structural_scalar_ternary_operands(expression) {
            return Self::scoped_scalar_triple_is_inner(
                first,
                second,
                third,
                relationship_bindings,
                local_nodes,
            );
        }

        match expression {
            ScalarExpression::PresenceGated {
                presence_variable,
                expression,
            } => {
                Self::scoped_variable_is_inner(
                    presence_variable,
                    relationship_bindings,
                    local_nodes,
                ) && Self::scoped_scalar_expression_is_inner(
                    expression,
                    relationship_bindings,
                    local_nodes,
                )
            }
            ScalarExpression::Coalesce { expressions } => expressions.iter().all(|expression| {
                Self::scoped_scalar_expression_is_inner(
                    expression,
                    relationship_bindings,
                    local_nodes,
                )
            }),
            ScalarExpression::Round { expression, places } => {
                Self::scoped_scalar_expression_is_inner(
                    expression,
                    relationship_bindings,
                    local_nodes,
                ) && places.as_deref().is_none_or(|places| {
                    Self::scoped_scalar_expression_is_inner(
                        places,
                        relationship_bindings,
                        local_nodes,
                    )
                })
            }
            ScalarExpression::Substring {
                expression,
                start,
                length,
            } => {
                Self::scoped_scalar_pair_is_inner(
                    expression,
                    start,
                    relationship_bindings,
                    local_nodes,
                ) && length.as_deref().is_none_or(|length| {
                    Self::scoped_scalar_expression_is_inner(
                        length,
                        relationship_bindings,
                        local_nodes,
                    )
                })
            }
            ScalarExpression::Case {
                alternatives,
                else_expression,
            } => Self::scoped_case_expression_is_inner(
                alternatives,
                else_expression.as_deref(),
                relationship_bindings,
                local_nodes,
            ),
            _ => unreachable!("non-structural scalar expression reached structural scoped check"),
        }
    }

    fn scoped_scalar_pair_is_inner<'b>(
        left: &ScalarExpression,
        right: &ScalarExpression,
        relationship_bindings: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
    ) -> bool {
        Self::scoped_scalar_expression_is_inner(left, relationship_bindings, local_nodes)
            && Self::scoped_scalar_expression_is_inner(right, relationship_bindings, local_nodes)
    }

    fn scoped_scalar_triple_is_inner<'b>(
        first: &ScalarExpression,
        second: &ScalarExpression,
        third: &ScalarExpression,
        relationship_bindings: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
    ) -> bool {
        Self::scoped_scalar_pair_is_inner(first, second, relationship_bindings, local_nodes)
            && Self::scoped_scalar_expression_is_inner(third, relationship_bindings, local_nodes)
    }

    fn scoped_case_expression_is_inner<'b>(
        alternatives: &[ScalarCaseAlternative],
        else_expression: Option<&ScalarExpression>,
        relationship_bindings: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
    ) -> bool {
        alternatives.iter().all(|alternative| {
            Self::scoped_predicate_expression_is_inner(
                &alternative.when,
                relationship_bindings,
                local_nodes,
            ) && Self::scoped_scalar_expression_is_inner(
                &alternative.then,
                relationship_bindings,
                local_nodes,
            )
        }) && else_expression.is_none_or(|else_expression| {
            Self::scoped_scalar_expression_is_inner(
                else_expression,
                relationship_bindings,
                local_nodes,
            )
        })
    }

    fn scoped_variable_is_inner<'b>(
        variable: &str,
        relationship_bindings: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
    ) -> bool {
        local_nodes.contains_key(variable)
            || relationship_bindings
                .iter()
                .any(|relationship| relationship.pattern.variable.as_deref() == Some(variable))
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
                    .as_deref()
                    .map_or_else(|| projection.output_name(), ToString::to_string);
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
                self.render_projection_scalar_expression(expression)?,
                quote_ident(alias)
            )),
            Projection::CountAll { alias } => Ok(format!("COUNT(*) AS {}", quote_ident(alias))),
            Projection::Aggregate {
                function,
                target,
                distinct,
                alias,
            } => Ok(format!(
                "{} AS {}",
                self.render_aggregate_invocation(*function, target, *distinct)?,
                quote_ident(alias)
            )),
        }
    }

    fn render_where(&self) -> Result<String, CoreError> {
        let mut predicates = self.render_pre_projection_predicates()?;
        if !self.plan_has_aggregation()
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
        if !self.plan_has_aggregation() {
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

    fn plan_has_aggregation(&self) -> bool {
        self.validated
            .plan()
            .projections
            .iter()
            .any(Projection::is_aggregate)
            || self.validated.plan().order_by.iter().any(|key| {
                matches!(
                    &key.expression,
                    OrderExpression::CountAll | OrderExpression::Aggregate { .. }
                )
            })
    }

    fn render_group_by(&self) -> Result<String, CoreError> {
        if !self.plan_has_aggregation() {
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

    pub(super) fn render_predicate_expression(
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
            PredicateExpression::ExistsPattern(predicate) => {
                self.render_exists_pattern_predicate(predicate)
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

    fn render_scalar_predicate_expression(
        &self,
        predicate: &PredicateExpression,
    ) -> Result<String, CoreError> {
        match predicate {
            PredicateExpression::ExistsPattern(predicate) => {
                if let Some(rendered) = self.render_precomputed_exists_pattern_ref(predicate) {
                    return Ok(rendered);
                }
                let alias = self.next_scalar_subquery_alias("__coral_exists_count");
                Ok(format!(
                    "{} > 0",
                    self.render_scoped_pattern_select(predicate, &format!("COUNT(*) AS {alias}"))?
                ))
            }
            PredicateExpression::And { left, right } => Ok(format!(
                "({} AND {})",
                self.render_scalar_predicate_expression(left)?,
                self.render_scalar_predicate_expression(right)?
            )),
            PredicateExpression::Or { left, right } => Ok(format!(
                "({} OR {})",
                self.render_scalar_predicate_expression(left)?,
                self.render_scalar_predicate_expression(right)?
            )),
            PredicateExpression::Xor { left, right } => {
                let left = self.render_scalar_predicate_expression(left)?;
                let right = self.render_scalar_predicate_expression(right)?;
                Ok(render_xor_predicate(&left, &right))
            }
            PredicateExpression::Not { expression } => Ok(format!(
                "NOT ({})",
                self.render_scalar_predicate_expression(expression)?
            )),
            PredicateExpression::Boolean(_)
            | PredicateExpression::Comparison(_)
            | PredicateExpression::KeyComparison(_)
            | PredicateExpression::ElementIdComparison(_)
            | PredicateExpression::Presence(_)
            | PredicateExpression::PropertyKeyMembership(_)
            | PredicateExpression::ScalarComparison(_) => {
                self.render_predicate_expression(predicate)
            }
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
        if let Some(rendered) = self.try_render_count_existence_predicate(predicate)? {
            return Ok(rendered);
        }

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

    fn try_render_count_existence_predicate(
        &self,
        predicate: &ScalarPredicate,
    ) -> Result<Option<String>, CoreError> {
        let ScalarExpression::CountSubquery {
            pattern,
            distinct_target: None,
        } = &predicate.lhs
        else {
            return Ok(None);
        };
        let Some(existence) = Self::count_existence_predicate(predicate.operator, &predicate.rhs)
        else {
            return Ok(None);
        };
        self.render_count_existence_predicate(pattern, existence)
            .map(Some)
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
        let presence = self.render_binding_presence_ref(
            predicate
                .presence_variable
                .as_deref()
                .unwrap_or(&predicate.variable),
        )?;
        let value = if has_key { "TRUE" } else { "FALSE" };
        Ok(format!(
            "CASE WHEN {presence} IS NULL THEN NULL ELSE {value} END"
        ))
    }

    fn render_exists_pattern_predicate(
        &self,
        predicate: &ExistsPatternPredicate,
    ) -> Result<String, CoreError> {
        Ok(format!(
            "EXISTS {}",
            self.render_scoped_pattern_select(predicate, "1")?
        ))
    }

    fn render_count_subquery_expression(
        &self,
        pattern: &CountSubqueryPattern,
        distinct_target: Option<&ScalarExpression>,
    ) -> Result<String, CoreError> {
        if let Some(rendered) = self.render_precomputed_count_subquery_ref(pattern, distinct_target)
        {
            return Ok(rendered);
        }
        match pattern {
            CountSubqueryPattern::Relationships(predicate) => {
                if let Some(target) = distinct_target {
                    self.render_count_distinct_scoped_pattern_select(predicate, target)
                } else {
                    self.render_scoped_pattern_select(predicate, "COUNT(*)")
                }
            }
            CountSubqueryPattern::Nodes {
                nodes,
                predicates,
                predicate,
            } => self.render_count_node_subquery(
                nodes,
                predicates,
                predicate.as_deref(),
                distinct_target,
            ),
        }
    }

    fn render_collect_subquery_expression(
        &self,
        pattern: &CountSubqueryPattern,
        target: &ScalarExpression,
        distinct: bool,
    ) -> Result<String, CoreError> {
        match pattern {
            CountSubqueryPattern::Relationships(predicate) => {
                if let Some(rendered) =
                    self.render_precomputed_collect_subquery_ref(predicate, target, distinct)
                {
                    return Ok(rendered);
                }
                self.render_collect_scoped_pattern_select(predicate, target, distinct)
            }
            CountSubqueryPattern::Nodes {
                nodes,
                predicates,
                predicate,
            } => self.render_collect_node_subquery(
                nodes,
                predicates,
                predicate.as_deref(),
                target,
                distinct,
            ),
        }
    }

    fn render_collect_target_select_expression(target_sql: &str, distinct: bool) -> String {
        let distinct = if distinct { "DISTINCT " } else { "" };
        format!("COALESCE(ARRAY_AGG({distinct}{target_sql}), make_array())")
    }

    fn render_count_distinct_rows_select(row_select: &str) -> String {
        format!(
            "(SELECT COUNT(*) FROM {row_select} AS {})",
            quote_ident("__coral_count_distinct")
        )
    }

    fn render_projection_scalar_expression(
        &self,
        expression: &ScalarExpression,
    ) -> Result<String, CoreError> {
        self.reject_unprecomputed_projection_scalar_subqueries(expression)?;
        self.render_scalar_expression(expression)
    }

    #[expect(
        clippy::too_many_lines,
        reason = "This exhaustive scalar IR dispatcher keeps projection subquery checks total over every scalar variant"
    )]
    fn reject_unprecomputed_projection_scalar_subqueries(
        &self,
        expression: &ScalarExpression,
    ) -> Result<(), CoreError> {
        if let Some(expression) = scalar_expression_unary_operand(expression) {
            return self.reject_unprecomputed_projection_scalar_subqueries(expression);
        }

        match expression {
            ScalarExpression::CountSubquery {
                pattern,
                distinct_target,
            } => {
                if let CountSubqueryPattern::Relationships(predicate) = pattern.as_ref()
                    && predicate.references_outer_variables()
                    && self
                        .render_precomputed_count_subquery_ref(pattern, distinct_target.as_deref())
                        .is_none()
                {
                    return Err(CoreError::InvalidInput(
                        "correlated relationship COUNT subqueries in projections must be precomputable through a single outer node anchor; move complex outer-dependent predicates to WHERE EXISTS or simplify the COUNT pattern"
                            .to_string(),
                    ));
                }
            }
            ScalarExpression::CollectSubquery {
                pattern,
                target,
                distinct,
            } => {
                self.reject_unprecomputed_projection_collect_subquery(pattern, target, *distinct)?;
            }
            ScalarExpression::Predicate(predicate) => {
                self.reject_unprecomputed_projection_predicate_subqueries(predicate)?;
            }
            ScalarExpression::PresenceGated { .. }
            | ScalarExpression::Coalesce { .. }
            | ScalarExpression::NullIf { .. }
            | ScalarExpression::Round { .. }
            | ScalarExpression::Left { .. }
            | ScalarExpression::Right { .. }
            | ScalarExpression::StringIndices { .. }
            | ScalarExpression::LPad { .. }
            | ScalarExpression::RPad { .. }
            | ScalarExpression::StringContains { .. }
            | ScalarExpression::StringStartsWith { .. }
            | ScalarExpression::StringEndsWith { .. }
            | ScalarExpression::Replace { .. }
            | ScalarExpression::Substring { .. }
            | ScalarExpression::Arithmetic { .. }
            | ScalarExpression::Case { .. }
            | ScalarExpression::Atan2 { .. } => {
                self.reject_unprecomputed_projection_structural_subqueries(expression)?;
            }
            ScalarExpression::Property(_)
            | ScalarExpression::UndirectedEndpointProperty { .. }
            | ScalarExpression::UndirectedEndpointKey { .. }
            | ScalarExpression::UndirectedEndpointElementId { .. }
            | ScalarExpression::UndirectedEndpointLabels { .. }
            | ScalarExpression::UndirectedEndpointPropertyKeys { .. }
            | ScalarExpression::Literal(_)
            | ScalarExpression::LiteralList { .. }
            | ScalarExpression::TypedLiteralList { .. }
            | ScalarExpression::GraphKeyList { .. }
            | ScalarExpression::Key { .. }
            | ScalarExpression::ElementId { .. }
            | ScalarExpression::GraphIdentity { .. }
            | ScalarExpression::GraphPresence { .. }
            | ScalarExpression::NodeLabels { .. }
            | ScalarExpression::PropertyKeys { .. }
            | ScalarExpression::RelationshipType { .. } => {}
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
            | ScalarExpression::Negate { .. } => {
                unreachable!("unary scalar expressions handled before projection subquery checks")
            }
        }
        Ok(())
    }

    fn reject_unprecomputed_projection_structural_subqueries(
        &self,
        expression: &ScalarExpression,
    ) -> Result<(), CoreError> {
        if let Some((left, right)) = Self::structural_scalar_binary_operands(expression) {
            self.reject_unprecomputed_projection_scalar_subqueries(left)?;
            self.reject_unprecomputed_projection_scalar_subqueries(right)?;
            return Ok(());
        }
        if let Some((first, second, third)) = Self::structural_scalar_ternary_operands(expression) {
            self.reject_unprecomputed_projection_scalar_subqueries(first)?;
            self.reject_unprecomputed_projection_scalar_subqueries(second)?;
            self.reject_unprecomputed_projection_scalar_subqueries(third)?;
            return Ok(());
        }

        match expression {
            ScalarExpression::PresenceGated { expression, .. } => {
                self.reject_unprecomputed_projection_scalar_subqueries(expression)?;
            }
            ScalarExpression::Coalesce { expressions } => {
                for expression in expressions {
                    self.reject_unprecomputed_projection_scalar_subqueries(expression)?;
                }
            }
            ScalarExpression::Round { expression, places } => {
                self.reject_unprecomputed_projection_scalar_subqueries(expression)?;
                if let Some(places) = places {
                    self.reject_unprecomputed_projection_scalar_subqueries(places)?;
                }
            }
            ScalarExpression::Substring {
                expression,
                start,
                length,
            } => {
                self.reject_unprecomputed_projection_scalar_subqueries(expression)?;
                self.reject_unprecomputed_projection_scalar_subqueries(start)?;
                if let Some(length) = length {
                    self.reject_unprecomputed_projection_scalar_subqueries(length)?;
                }
            }
            ScalarExpression::Case {
                alternatives,
                else_expression,
            } => {
                for alternative in alternatives {
                    self.reject_unprecomputed_projection_predicate_subqueries(&alternative.when)?;
                    self.reject_unprecomputed_projection_scalar_subqueries(&alternative.then)?;
                }
                if let Some(else_expression) = else_expression {
                    self.reject_unprecomputed_projection_scalar_subqueries(else_expression)?;
                }
            }
            _ => {
                unreachable!("projection subquery dispatcher called structural helper incorrectly")
            }
        }
        Ok(())
    }

    fn reject_unprecomputed_projection_collect_subquery(
        &self,
        pattern: &CountSubqueryPattern,
        target: &ScalarExpression,
        distinct: bool,
    ) -> Result<(), CoreError> {
        if let CountSubqueryPattern::Relationships(predicate) = pattern
            && predicate.references_outer_variables()
            && self
                .render_precomputed_collect_subquery_ref(predicate, target, distinct)
                .is_none()
        {
            return Err(CoreError::InvalidInput(
                "correlated relationship COLLECT subqueries in projections must be precomputable through a single outer node anchor and an inner-only return target; move complex outer-dependent logic to the scoped WHERE predicate or simplify the COLLECT pattern"
                    .to_string(),
            ));
        }
        Ok(())
    }

    fn reject_unprecomputed_projection_predicate_subqueries(
        &self,
        predicate: &PredicateExpression,
    ) -> Result<(), CoreError> {
        match predicate {
            PredicateExpression::ScalarComparison(predicate) => {
                self.reject_unprecomputed_projection_scalar_subqueries(&predicate.lhs)?;
                if let ScalarPredicateRhs::Expression(expression) = &predicate.rhs {
                    self.reject_unprecomputed_projection_scalar_subqueries(expression)?;
                }
            }
            PredicateExpression::And { left, right }
            | PredicateExpression::Or { left, right }
            | PredicateExpression::Xor { left, right } => {
                self.reject_unprecomputed_projection_predicate_subqueries(left)?;
                self.reject_unprecomputed_projection_predicate_subqueries(right)?;
            }
            PredicateExpression::Not { expression } => {
                self.reject_unprecomputed_projection_predicate_subqueries(expression)?;
            }
            PredicateExpression::Boolean(_)
            | PredicateExpression::Comparison(_)
            | PredicateExpression::KeyComparison(_)
            | PredicateExpression::ElementIdComparison(_)
            | PredicateExpression::Presence(_)
            | PredicateExpression::PropertyKeyMembership(_)
            | PredicateExpression::ExistsPattern(_) => {}
        }
        Ok(())
    }

    fn render_count_exists_select(
        &self,
        pattern: &CountSubqueryPattern,
    ) -> Result<String, CoreError> {
        match pattern {
            CountSubqueryPattern::Relationships(predicate) => {
                self.render_scoped_pattern_select(predicate, "1")
            }
            CountSubqueryPattern::Nodes {
                nodes,
                predicates,
                predicate,
            } => self.render_count_node_select(nodes, predicates, predicate.as_deref(), "1"),
        }
    }

    fn scoped_condition_capacity(
        relationship_count: usize,
        property_predicate_count: usize,
        predicate: Option<&PredicateExpression>,
    ) -> usize {
        relationship_count
            .saturating_add(property_predicate_count)
            .saturating_add(usize::from(predicate.is_some()))
    }

    fn render_scoped_conditions<'b>(
        &self,
        predicates: &[PropertyPredicate],
        predicate: Option<&PredicateExpression>,
        relationship_bindings: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<Vec<String>, CoreError> {
        let mut conditions = Vec::with_capacity(Self::scoped_condition_capacity(
            0,
            predicates.len(),
            predicate,
        ));
        for property_predicate in predicates {
            conditions.push(self.render_exists_property_predicate(
                property_predicate,
                relationship_bindings,
                local_nodes,
                local_aliases,
            )?);
        }
        if let Some(predicate) = predicate {
            conditions.push(self.render_scoped_predicate_expression(
                predicate,
                relationship_bindings,
                local_nodes,
                local_aliases,
            )?);
        }
        Ok(conditions)
    }

    fn render_scoped_pattern_select(
        &self,
        predicate: &ExistsPatternPredicate,
        select_expression: &str,
    ) -> Result<String, CoreError> {
        let local_nodes = self.exists_local_node_map(predicate)?;
        let relationship_bindings = self.exists_relationship_bindings(predicate, &local_nodes)?;
        let local_aliases = Self::exists_local_node_aliases(predicate);
        if relationship_bindings.is_empty() {
            return self.render_scoped_node_select(
                &predicate.nodes,
                &predicate.predicates,
                predicate.predicate.as_deref(),
                select_expression,
                &local_nodes,
                &local_aliases,
                "EXISTS",
            );
        }
        let mut from_clause = relationship_bindings
            .iter()
            .enumerate()
            .map(|(index, binding)| {
                let table_ref = format!(
                    "{} AS {}",
                    render_table_ref(&binding.relationship.table),
                    quote_ident(&binding.alias)
                );
                if index == 0 {
                    table_ref
                } else {
                    format!("JOIN {table_ref} ON TRUE")
                }
            })
            .collect::<Vec<_>>()
            .join(" ");
        for node in &predicate.nodes {
            let node_mapping = local_nodes.get(node.variable.as_str()).ok_or_else(|| {
                CoreError::internal("validated EXISTS local node mapping was missing")
            })?;
            let alias = local_aliases.get(node.variable.as_str()).ok_or_else(|| {
                CoreError::internal("validated EXISTS local node alias was missing")
            })?;
            write!(
                from_clause,
                " JOIN {} AS {} ON TRUE",
                render_table_ref(&node_mapping.table),
                quote_ident(alias)
            )
            .map_err(|_| CoreError::internal("failed to render EXISTS pattern SQL"))?;
        }

        let mut conditions = Vec::with_capacity(Self::scoped_condition_capacity(
            relationship_bindings.len(),
            predicate.predicates.len(),
            predicate.predicate.as_deref(),
        ));
        for binding in &relationship_bindings {
            conditions.push(self.exists_relationship_condition(
                binding.pattern,
                binding.relationship,
                &binding.alias,
                &local_nodes,
                &local_aliases,
            )?);
        }
        conditions.extend(self.render_scoped_conditions(
            &predicate.predicates,
            predicate.predicate.as_deref(),
            &relationship_bindings,
            &local_nodes,
            &local_aliases,
        )?);
        Ok(format!(
            "(SELECT {select_expression} FROM {from_clause} WHERE {})",
            conditions.join(" AND ")
        ))
    }

    fn render_collect_scoped_pattern_select(
        &self,
        predicate: &ExistsPatternPredicate,
        target: &ScalarExpression,
        distinct: bool,
    ) -> Result<String, CoreError> {
        let local_nodes = self.exists_local_node_map(predicate)?;
        let relationship_bindings = self.exists_relationship_bindings(predicate, &local_nodes)?;
        let local_aliases = Self::exists_local_node_aliases(predicate);
        let target_sql = self.render_scoped_scalar_expression(
            target,
            &relationship_bindings,
            &local_nodes,
            &local_aliases,
        )?;
        let select_expression =
            Self::render_collect_target_select_expression(&target_sql, distinct);
        if relationship_bindings.is_empty() {
            return self.render_scoped_node_select(
                &predicate.nodes,
                &predicate.predicates,
                predicate.predicate.as_deref(),
                &select_expression,
                &local_nodes,
                &local_aliases,
                "COLLECT",
            );
        }

        let mut from_clause = relationship_bindings
            .iter()
            .enumerate()
            .map(|(index, binding)| {
                let table_ref = format!(
                    "{} AS {}",
                    render_table_ref(&binding.relationship.table),
                    quote_ident(&binding.alias)
                );
                if index == 0 {
                    table_ref
                } else {
                    format!("JOIN {table_ref} ON TRUE")
                }
            })
            .collect::<Vec<_>>()
            .join(" ");
        for node in &predicate.nodes {
            let node_mapping = local_nodes.get(node.variable.as_str()).ok_or_else(|| {
                CoreError::internal("validated COLLECT local node mapping was missing")
            })?;
            let alias = local_aliases.get(node.variable.as_str()).ok_or_else(|| {
                CoreError::internal("validated COLLECT local node alias was missing")
            })?;
            write!(
                from_clause,
                " JOIN {} AS {} ON TRUE",
                render_table_ref(&node_mapping.table),
                quote_ident(alias)
            )
            .map_err(|_| CoreError::internal("failed to render COLLECT pattern SQL"))?;
        }

        let mut conditions = Vec::with_capacity(Self::scoped_condition_capacity(
            relationship_bindings.len(),
            predicate.predicates.len(),
            predicate.predicate.as_deref(),
        ));
        for binding in &relationship_bindings {
            conditions.push(self.exists_relationship_condition(
                binding.pattern,
                binding.relationship,
                &binding.alias,
                &local_nodes,
                &local_aliases,
            )?);
        }
        conditions.extend(self.render_scoped_conditions(
            &predicate.predicates,
            predicate.predicate.as_deref(),
            &relationship_bindings,
            &local_nodes,
            &local_aliases,
        )?);
        Ok(format!(
            "(SELECT {select_expression} FROM {from_clause} WHERE {})",
            conditions.join(" AND ")
        ))
    }

    fn render_count_distinct_scoped_pattern_select(
        &self,
        predicate: &ExistsPatternPredicate,
        target: &ScalarExpression,
    ) -> Result<String, CoreError> {
        let local_nodes = self.exists_local_node_map(predicate)?;
        let relationship_bindings = self.exists_relationship_bindings(predicate, &local_nodes)?;
        let local_aliases = Self::exists_local_node_aliases(predicate);
        let target_sql = self.render_scoped_scalar_expression(
            target,
            &relationship_bindings,
            &local_nodes,
            &local_aliases,
        )?;
        let select_expression = format!(
            "DISTINCT {target_sql} AS {}",
            quote_ident("__coral_count_value")
        );
        if relationship_bindings.is_empty() {
            let row_select = self.render_scoped_node_select(
                &predicate.nodes,
                &predicate.predicates,
                predicate.predicate.as_deref(),
                &select_expression,
                &local_nodes,
                &local_aliases,
                "COUNT DISTINCT",
            )?;
            return Ok(Self::render_count_distinct_rows_select(&row_select));
        }

        let mut from_clause = relationship_bindings
            .iter()
            .enumerate()
            .map(|(index, binding)| {
                let table_ref = format!(
                    "{} AS {}",
                    render_table_ref(&binding.relationship.table),
                    quote_ident(&binding.alias)
                );
                if index == 0 {
                    table_ref
                } else {
                    format!("JOIN {table_ref} ON TRUE")
                }
            })
            .collect::<Vec<_>>()
            .join(" ");
        for node in &predicate.nodes {
            let node_mapping = local_nodes.get(node.variable.as_str()).ok_or_else(|| {
                CoreError::internal("validated COUNT DISTINCT local node mapping was missing")
            })?;
            let alias = local_aliases.get(node.variable.as_str()).ok_or_else(|| {
                CoreError::internal("validated COUNT DISTINCT local node alias was missing")
            })?;
            write!(
                from_clause,
                " JOIN {} AS {} ON TRUE",
                render_table_ref(&node_mapping.table),
                quote_ident(alias)
            )
            .map_err(|_| CoreError::internal("failed to render COUNT DISTINCT pattern SQL"))?;
        }

        let mut conditions = Vec::with_capacity(Self::scoped_condition_capacity(
            relationship_bindings.len(),
            predicate.predicates.len(),
            predicate.predicate.as_deref(),
        ));
        for binding in &relationship_bindings {
            conditions.push(self.exists_relationship_condition(
                binding.pattern,
                binding.relationship,
                &binding.alias,
                &local_nodes,
                &local_aliases,
            )?);
        }
        conditions.extend(self.render_scoped_conditions(
            &predicate.predicates,
            predicate.predicate.as_deref(),
            &relationship_bindings,
            &local_nodes,
            &local_aliases,
        )?);
        let row_select = format!(
            "(SELECT {select_expression} FROM {from_clause} WHERE {})",
            conditions.join(" AND ")
        );
        Ok(Self::render_count_distinct_rows_select(&row_select))
    }

    fn render_count_node_subquery(
        &self,
        nodes: &[NodePattern],
        predicates: &[PropertyPredicate],
        predicate: Option<&PredicateExpression>,
        distinct_target: Option<&ScalarExpression>,
    ) -> Result<String, CoreError> {
        if let Some(target) = distinct_target {
            return self.render_count_distinct_node_subquery(nodes, predicates, predicate, target);
        }
        self.render_count_node_select(nodes, predicates, predicate, "COUNT(*)")
    }

    fn render_count_distinct_node_subquery(
        &self,
        nodes: &[NodePattern],
        predicates: &[PropertyPredicate],
        predicate: Option<&PredicateExpression>,
        target: &ScalarExpression,
    ) -> Result<String, CoreError> {
        if nodes.is_empty() {
            return Err(CoreError::internal(
                "validated COUNT DISTINCT node subquery had no node bindings",
            ));
        }
        let local_nodes = self.scoped_local_node_map(nodes)?;
        let local_aliases = Self::count_local_node_aliases(nodes);
        let relationships = Vec::new();
        let target_sql = self.render_scoped_scalar_expression(
            target,
            &relationships,
            &local_nodes,
            &local_aliases,
        )?;
        let select_expression = format!(
            "DISTINCT {target_sql} AS {}",
            quote_ident("__coral_count_value")
        );
        let row_select = self.render_scoped_node_select(
            nodes,
            predicates,
            predicate,
            &select_expression,
            &local_nodes,
            &local_aliases,
            "COUNT DISTINCT",
        )?;
        Ok(Self::render_count_distinct_rows_select(&row_select))
    }

    fn render_collect_node_subquery(
        &self,
        nodes: &[NodePattern],
        predicates: &[PropertyPredicate],
        predicate: Option<&PredicateExpression>,
        target: &ScalarExpression,
        distinct: bool,
    ) -> Result<String, CoreError> {
        if nodes.is_empty() {
            return Err(CoreError::internal(
                "validated COLLECT node subquery had no node bindings",
            ));
        }
        let local_nodes = self.scoped_local_node_map(nodes)?;
        let local_aliases = Self::count_local_node_aliases(nodes);
        let relationships = Vec::new();
        let target_sql = self.render_scoped_scalar_expression(
            target,
            &relationships,
            &local_nodes,
            &local_aliases,
        )?;
        let select_expression =
            Self::render_collect_target_select_expression(&target_sql, distinct);
        self.render_scoped_node_select(
            nodes,
            predicates,
            predicate,
            &select_expression,
            &local_nodes,
            &local_aliases,
            "COLLECT",
        )
    }

    fn render_count_node_select(
        &self,
        nodes: &[NodePattern],
        predicates: &[PropertyPredicate],
        predicate: Option<&PredicateExpression>,
        select_expression: &str,
    ) -> Result<String, CoreError> {
        if nodes.is_empty() {
            return Err(CoreError::internal(
                "validated COUNT node subquery had no node bindings",
            ));
        }
        let local_nodes = self.scoped_local_node_map(nodes)?;
        let local_aliases = Self::count_local_node_aliases(nodes);
        self.render_scoped_node_select(
            nodes,
            predicates,
            predicate,
            select_expression,
            &local_nodes,
            &local_aliases,
            "COUNT",
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Scoped subquery rendering needs the local node bindings, aliases, and SQL context together"
    )]
    fn render_scoped_node_select<'b>(
        &self,
        nodes: &'b [NodePattern],
        predicates: &[PropertyPredicate],
        predicate: Option<&PredicateExpression>,
        select_expression: &str,
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
        context: &str,
    ) -> Result<String, CoreError> {
        if nodes.is_empty() {
            return Err(CoreError::internal(format!(
                "validated {context} node subquery had no node bindings"
            )));
        }
        let mut from_clause = String::new();
        for (index, node) in nodes.iter().enumerate() {
            let node_mapping = local_nodes.get(node.variable.as_str()).ok_or_else(|| {
                CoreError::internal(format!(
                    "validated {context} local node mapping was missing"
                ))
            })?;
            let alias = local_aliases.get(node.variable.as_str()).ok_or_else(|| {
                CoreError::internal(format!("validated {context} local node alias was missing"))
            })?;
            if index > 0 {
                from_clause.push_str(" JOIN ");
            }
            write!(
                from_clause,
                "{} AS {}",
                render_table_ref(&node_mapping.table),
                quote_ident(alias)
            )
            .map_err(|_| {
                CoreError::internal(format!("failed to render {context} node subquery SQL"))
            })?;
            if index > 0 {
                from_clause.push_str(" ON TRUE");
            }
        }

        let relationships = Vec::new();
        let conditions = self.render_scoped_conditions(
            predicates,
            predicate,
            &relationships,
            local_nodes,
            local_aliases,
        )?;
        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conditions.join(" AND "))
        };
        Ok(format!(
            "(SELECT {select_expression} FROM {from_clause}{where_clause})"
        ))
    }

    fn scoped_local_node_map<'b>(
        &self,
        nodes: &'b [NodePattern],
    ) -> Result<BTreeMap<&'b str, &'a Node>, CoreError> {
        let mut local_nodes = BTreeMap::new();
        for node in nodes {
            let mapping = self.validated.graph().node(&node.label).ok_or_else(|| {
                CoreError::internal("validated scoped node label was not resolvable")
            })?;
            local_nodes.insert(node.variable.as_str(), mapping);
        }
        Ok(local_nodes)
    }

    fn render_exists_property_predicate<'b>(
        &self,
        predicate: &PropertyPredicate,
        relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        let property = self.render_exists_property_ref(
            &predicate.property,
            relationships,
            local_nodes,
            local_aliases,
        )?;
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
                "validated EXISTS IN predicate did not contain a literal list",
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
                "validated EXISTS string predicate did not contain a string literal",
            )),
            (ComparisonOperator::RegexMatch, PredicateRhs::List(_)) => Err(CoreError::internal(
                "validated EXISTS regex predicate did not contain a scalar RHS",
            )),
            (ComparisonOperator::RegexMatch, rhs) => Ok(render_regex_predicate(
                &property,
                &self.render_exists_predicate_rhs(
                    rhs,
                    relationships,
                    local_nodes,
                    local_aliases,
                )?,
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
                "validated EXISTS predicate contained an invalid null comparison",
            )),
            _ => Ok(format!(
                "{property} {} {}",
                render_operator(predicate.operator),
                self.render_exists_predicate_rhs(
                    &predicate.rhs,
                    relationships,
                    local_nodes,
                    local_aliases,
                )?
            )),
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "This exhaustive predicate IR dispatcher mirrors the top-level predicate renderer for scoped aliases"
    )]
    fn render_scoped_predicate_expression<'b>(
        &self,
        predicate: &PredicateExpression,
        relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        match predicate {
            PredicateExpression::Boolean(value) => Ok(value.to_string().to_uppercase()),
            PredicateExpression::Comparison(predicate) => self.render_exists_property_predicate(
                predicate,
                relationships,
                local_nodes,
                local_aliases,
            ),
            PredicateExpression::KeyComparison(predicate) => {
                let lhs = self.render_scoped_binding_key_ref(
                    &predicate.variable,
                    relationships,
                    local_nodes,
                    local_aliases,
                )?;
                self.render_scoped_simple_predicate(
                    &lhs,
                    predicate.operator,
                    &predicate.rhs,
                    relationships,
                    local_nodes,
                    local_aliases,
                )
            }
            PredicateExpression::ElementIdComparison(predicate) => {
                let lhs = format!(
                    "CAST({} AS VARCHAR)",
                    self.render_scoped_binding_key_ref(
                        &predicate.variable,
                        relationships,
                        local_nodes,
                        local_aliases,
                    )?
                );
                self.render_scoped_simple_predicate(
                    &lhs,
                    predicate.operator,
                    &predicate.rhs,
                    relationships,
                    local_nodes,
                    local_aliases,
                )
            }
            PredicateExpression::Presence(predicate) => {
                let presence = self.render_scoped_binding_presence_ref(
                    &predicate.variable,
                    relationships,
                    local_nodes,
                    local_aliases,
                )?;
                match predicate.operator {
                    ComparisonOperator::Equal => Ok(format!("{presence} IS NULL")),
                    ComparisonOperator::NotEqual => Ok(format!("{presence} IS NOT NULL")),
                    _ => Err(CoreError::internal(
                        "validated scoped presence predicate contained invalid operator",
                    )),
                }
            }
            PredicateExpression::PropertyKeyMembership(predicate) => self
                .render_scoped_property_key_membership_predicate(
                    predicate,
                    relationships,
                    local_nodes,
                    local_aliases,
                ),
            PredicateExpression::ExistsPattern(predicate) => self
                .render_nested_scoped_exists_pattern_predicate(
                    predicate,
                    relationships,
                    local_nodes,
                    local_aliases,
                ),
            PredicateExpression::ScalarComparison(predicate) => self
                .render_scoped_scalar_predicate(
                    predicate,
                    relationships,
                    local_nodes,
                    local_aliases,
                ),
            PredicateExpression::And { left, right } => Ok(format!(
                "({} AND {})",
                self.render_scoped_predicate_expression(
                    left,
                    relationships,
                    local_nodes,
                    local_aliases
                )?,
                self.render_scoped_predicate_expression(
                    right,
                    relationships,
                    local_nodes,
                    local_aliases
                )?
            )),
            PredicateExpression::Or { left, right } => Ok(format!(
                "({} OR {})",
                self.render_scoped_predicate_expression(
                    left,
                    relationships,
                    local_nodes,
                    local_aliases
                )?,
                self.render_scoped_predicate_expression(
                    right,
                    relationships,
                    local_nodes,
                    local_aliases
                )?
            )),
            PredicateExpression::Xor { left, right } => {
                let left = self.render_scoped_predicate_expression(
                    left,
                    relationships,
                    local_nodes,
                    local_aliases,
                )?;
                let right = self.render_scoped_predicate_expression(
                    right,
                    relationships,
                    local_nodes,
                    local_aliases,
                )?;
                Ok(render_xor_predicate(&left, &right))
            }
            PredicateExpression::Not { expression } => Ok(format!(
                "NOT ({})",
                self.render_scoped_predicate_expression(
                    expression,
                    relationships,
                    local_nodes,
                    local_aliases
                )?
            )),
        }
    }

    fn render_nested_scoped_exists_pattern_predicate<'b, 'c>(
        &self,
        predicate: &'c ExistsPatternPredicate,
        parent_relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        parent_local_nodes: &BTreeMap<&'b str, &'a Node>,
        parent_local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        Ok(format!(
            "EXISTS {}",
            self.render_nested_scoped_pattern_select(
                predicate,
                "1",
                parent_relationships,
                parent_local_nodes,
                parent_local_aliases,
                "__coral_nested_exists_n",
                "__coral_nested_exists_r",
                "nested EXISTS",
            )?
        ))
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Nested scoped pattern rendering needs select SQL plus child and parent alias scopes"
    )]
    fn render_nested_scoped_pattern_select<'b, 'c>(
        &self,
        predicate: &'c ExistsPatternPredicate,
        select_expression: &str,
        parent_relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        parent_local_nodes: &BTreeMap<&'b str, &'a Node>,
        parent_local_aliases: &BTreeMap<&'b str, String>,
        node_alias_prefix: &str,
        relationship_alias_prefix: &str,
        context: &str,
    ) -> Result<String, CoreError> {
        let local_nodes = self.scoped_local_node_map(&predicate.nodes)?;
        let relationship_bindings = self.nested_scoped_exists_relationship_bindings(
            predicate,
            &local_nodes,
            parent_local_nodes,
            relationship_alias_prefix,
        )?;
        let local_aliases =
            Self::nested_scoped_local_node_aliases(&predicate.nodes, node_alias_prefix);
        if relationship_bindings.is_empty() {
            return self.render_scoped_node_select(
                &predicate.nodes,
                &predicate.predicates,
                predicate.predicate.as_deref(),
                select_expression,
                &local_nodes,
                &local_aliases,
                context,
            );
        }

        let mut from_clause = relationship_bindings
            .iter()
            .enumerate()
            .map(|(index, binding)| {
                let table_ref = format!(
                    "{} AS {}",
                    render_table_ref(&binding.relationship.table),
                    quote_ident(&binding.alias)
                );
                if index == 0 {
                    table_ref
                } else {
                    format!("JOIN {table_ref} ON TRUE")
                }
            })
            .collect::<Vec<_>>()
            .join(" ");
        for node in &predicate.nodes {
            let node_mapping = local_nodes.get(node.variable.as_str()).ok_or_else(|| {
                CoreError::internal("validated nested EXISTS local node mapping was missing")
            })?;
            let alias = local_aliases.get(node.variable.as_str()).ok_or_else(|| {
                CoreError::internal("validated nested EXISTS local node alias was missing")
            })?;
            write!(
                from_clause,
                " JOIN {} AS {} ON TRUE",
                render_table_ref(&node_mapping.table),
                quote_ident(alias)
            )
            .map_err(|_| CoreError::internal("failed to render nested EXISTS pattern SQL"))?;
        }

        let mut conditions = Vec::with_capacity(
            relationship_bindings
                .len()
                .saturating_add(predicate.predicates.len())
                .saturating_add(usize::from(predicate.predicate.is_some())),
        );
        for binding in &relationship_bindings {
            conditions.push(self.nested_scoped_exists_relationship_condition(
                binding,
                &local_nodes,
                &local_aliases,
                parent_relationships,
                parent_local_nodes,
                parent_local_aliases,
            )?);
        }
        let mut scoped_relationships = relationship_bindings.clone();
        scoped_relationships.extend(parent_relationships.iter().cloned());
        let mut scoped_local_nodes = parent_local_nodes.clone();
        scoped_local_nodes.extend(
            local_nodes
                .iter()
                .map(|(variable, node)| (*variable, *node)),
        );
        let mut scoped_local_aliases = parent_local_aliases.clone();
        scoped_local_aliases.extend(
            local_aliases
                .iter()
                .map(|(variable, alias)| (*variable, alias.clone())),
        );
        conditions.extend(self.render_scoped_conditions(
            &predicate.predicates,
            predicate.predicate.as_deref(),
            &scoped_relationships,
            &scoped_local_nodes,
            &scoped_local_aliases,
        )?);
        Ok(format!(
            "(SELECT {select_expression} FROM {from_clause} WHERE {})",
            conditions.join(" AND ")
        ))
    }

    #[expect(
        clippy::too_many_lines,
        reason = "Nested distinct COUNT rendering mirrors nested scoped pattern rendering while adding parent-scope target projection"
    )]
    fn render_nested_scoped_count_distinct_pattern_select<'b, 'c>(
        &self,
        predicate: &'c ExistsPatternPredicate,
        target: &ScalarExpression,
        parent_relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        parent_local_nodes: &BTreeMap<&'b str, &'a Node>,
        parent_local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        let local_nodes = self.scoped_local_node_map(&predicate.nodes)?;
        let relationship_bindings = self.nested_scoped_exists_relationship_bindings(
            predicate,
            &local_nodes,
            parent_local_nodes,
            "__coral_nested_count_r",
        )?;
        let local_aliases =
            Self::nested_scoped_local_node_aliases(&predicate.nodes, "__coral_nested_count_n");

        let mut scoped_relationships = relationship_bindings.clone();
        scoped_relationships.extend(parent_relationships.iter().cloned());
        let mut scoped_local_nodes = parent_local_nodes.clone();
        scoped_local_nodes.extend(
            local_nodes
                .iter()
                .map(|(variable, node)| (*variable, *node)),
        );
        let mut scoped_local_aliases = parent_local_aliases.clone();
        scoped_local_aliases.extend(
            local_aliases
                .iter()
                .map(|(variable, alias)| (*variable, alias.clone())),
        );
        let target_sql = self.render_scoped_scalar_expression(
            target,
            &scoped_relationships,
            &scoped_local_nodes,
            &scoped_local_aliases,
        )?;
        let select_expression = format!(
            "DISTINCT {target_sql} AS {}",
            quote_ident("__coral_count_value")
        );

        if relationship_bindings.is_empty() {
            let row_select = self.render_scoped_node_select(
                &predicate.nodes,
                &predicate.predicates,
                predicate.predicate.as_deref(),
                &select_expression,
                &local_nodes,
                &local_aliases,
                "nested COUNT DISTINCT",
            )?;
            return Ok(Self::render_count_distinct_rows_select(&row_select));
        }

        let mut from_clause = relationship_bindings
            .iter()
            .enumerate()
            .map(|(index, binding)| {
                let table_ref = format!(
                    "{} AS {}",
                    render_table_ref(&binding.relationship.table),
                    quote_ident(&binding.alias)
                );
                if index == 0 {
                    table_ref
                } else {
                    format!("JOIN {table_ref} ON TRUE")
                }
            })
            .collect::<Vec<_>>()
            .join(" ");
        for node in &predicate.nodes {
            let node_mapping = local_nodes.get(node.variable.as_str()).ok_or_else(|| {
                CoreError::internal(
                    "validated nested COUNT DISTINCT local node mapping was missing",
                )
            })?;
            let alias = local_aliases.get(node.variable.as_str()).ok_or_else(|| {
                CoreError::internal("validated nested COUNT DISTINCT local node alias was missing")
            })?;
            write!(
                from_clause,
                " JOIN {} AS {} ON TRUE",
                render_table_ref(&node_mapping.table),
                quote_ident(alias)
            )
            .map_err(|_| {
                CoreError::internal("failed to render nested COUNT DISTINCT pattern SQL")
            })?;
        }

        let mut conditions = Vec::with_capacity(
            relationship_bindings
                .len()
                .saturating_add(predicate.predicates.len())
                .saturating_add(usize::from(predicate.predicate.is_some())),
        );
        for binding in &relationship_bindings {
            conditions.push(self.nested_scoped_exists_relationship_condition(
                binding,
                &local_nodes,
                &local_aliases,
                parent_relationships,
                parent_local_nodes,
                parent_local_aliases,
            )?);
        }
        conditions.extend(self.render_scoped_conditions(
            &predicate.predicates,
            predicate.predicate.as_deref(),
            &scoped_relationships,
            &scoped_local_nodes,
            &scoped_local_aliases,
        )?);
        let row_select = format!(
            "(SELECT {select_expression} FROM {from_clause} WHERE {})",
            conditions.join(" AND ")
        );
        Ok(Self::render_count_distinct_rows_select(&row_select))
    }

    fn render_nested_scoped_count_subquery_expression<'b>(
        &self,
        pattern: &CountSubqueryPattern,
        distinct_target: Option<&ScalarExpression>,
        parent_relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        parent_local_nodes: &BTreeMap<&'b str, &'a Node>,
        parent_local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        match pattern {
            CountSubqueryPattern::Relationships(predicate) => {
                if let Some(target) = distinct_target {
                    self.render_nested_scoped_count_distinct_pattern_select(
                        predicate,
                        target,
                        parent_relationships,
                        parent_local_nodes,
                        parent_local_aliases,
                    )
                } else {
                    self.render_nested_scoped_pattern_select(
                        predicate,
                        "COUNT(*)",
                        parent_relationships,
                        parent_local_nodes,
                        parent_local_aliases,
                        "__coral_nested_count_n",
                        "__coral_nested_count_r",
                        "nested COUNT",
                    )
                }
            }
            CountSubqueryPattern::Nodes {
                nodes,
                predicates,
                predicate,
            } => {
                if let Some(target) = distinct_target {
                    self.render_nested_scoped_count_distinct_node_subquery(
                        nodes,
                        predicates,
                        predicate.as_deref(),
                        target,
                        parent_relationships,
                        parent_local_nodes,
                        parent_local_aliases,
                    )
                } else {
                    self.render_nested_scoped_count_node_subquery(
                        nodes,
                        predicates,
                        predicate.as_deref(),
                    )
                }
            }
        }
    }

    fn render_nested_scoped_count_exists_select<'b>(
        &self,
        pattern: &CountSubqueryPattern,
        parent_relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        parent_local_nodes: &BTreeMap<&'b str, &'a Node>,
        parent_local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        match pattern {
            CountSubqueryPattern::Relationships(predicate) => self
                .render_nested_scoped_pattern_select(
                    predicate,
                    "1",
                    parent_relationships,
                    parent_local_nodes,
                    parent_local_aliases,
                    "__coral_nested_count_n",
                    "__coral_nested_count_r",
                    "nested COUNT",
                ),
            CountSubqueryPattern::Nodes {
                nodes,
                predicates,
                predicate,
            } => self.render_nested_scoped_count_node_select(
                nodes,
                predicates,
                predicate.as_deref(),
                "1",
            ),
        }
    }

    fn render_nested_scoped_count_node_subquery(
        &self,
        nodes: &[NodePattern],
        predicates: &[PropertyPredicate],
        predicate: Option<&PredicateExpression>,
    ) -> Result<String, CoreError> {
        self.render_nested_scoped_count_node_select(nodes, predicates, predicate, "COUNT(*)")
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Nested node distinct COUNT rendering needs local pattern inputs plus parent scoped SQL context"
    )]
    fn render_nested_scoped_count_distinct_node_subquery<'b>(
        &self,
        nodes: &[NodePattern],
        predicates: &[PropertyPredicate],
        predicate: Option<&PredicateExpression>,
        target: &ScalarExpression,
        parent_relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        parent_local_nodes: &BTreeMap<&'b str, &'a Node>,
        parent_local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        if nodes.is_empty() {
            return Err(CoreError::internal(
                "validated nested COUNT DISTINCT node subquery had no node bindings",
            ));
        }
        let local_nodes = self.scoped_local_node_map(nodes)?;
        let local_aliases = Self::nested_scoped_local_node_aliases(nodes, "__coral_nested_count_n");
        let mut scoped_local_nodes = parent_local_nodes.clone();
        scoped_local_nodes.extend(
            local_nodes
                .iter()
                .map(|(variable, node)| (*variable, *node)),
        );
        let mut scoped_local_aliases = parent_local_aliases.clone();
        scoped_local_aliases.extend(
            local_aliases
                .iter()
                .map(|(variable, alias)| (*variable, alias.clone())),
        );
        let target_sql = self.render_scoped_scalar_expression(
            target,
            parent_relationships,
            &scoped_local_nodes,
            &scoped_local_aliases,
        )?;
        let select_expression = format!(
            "DISTINCT {target_sql} AS {}",
            quote_ident("__coral_count_value")
        );
        let row_select = self.render_scoped_node_select(
            nodes,
            predicates,
            predicate,
            &select_expression,
            &scoped_local_nodes,
            &scoped_local_aliases,
            "nested COUNT DISTINCT",
        )?;
        Ok(Self::render_count_distinct_rows_select(&row_select))
    }

    fn render_nested_scoped_count_node_select(
        &self,
        nodes: &[NodePattern],
        predicates: &[PropertyPredicate],
        predicate: Option<&PredicateExpression>,
        select_expression: &str,
    ) -> Result<String, CoreError> {
        let local_nodes = self.scoped_local_node_map(nodes)?;
        let local_aliases = Self::nested_scoped_local_node_aliases(nodes, "__coral_nested_count_n");
        self.render_scoped_node_select(
            nodes,
            predicates,
            predicate,
            select_expression,
            &local_nodes,
            &local_aliases,
            "nested COUNT",
        )
    }

    fn nested_scoped_local_node_aliases<'b>(
        nodes: &'b [NodePattern],
        alias_prefix: &str,
    ) -> BTreeMap<&'b str, String> {
        nodes
            .iter()
            .enumerate()
            .map(|(index, node)| (node.variable.as_str(), format!("{alias_prefix}{index}")))
            .collect()
    }

    fn nested_scoped_exists_relationship_bindings<'b, 'c>(
        &self,
        predicate: &'c ExistsPatternPredicate,
        local_nodes: &BTreeMap<&'c str, &'a Node>,
        parent_local_nodes: &BTreeMap<&'b str, &'a Node>,
        relationship_alias_prefix: &str,
    ) -> Result<Vec<ExistsRelationshipSqlBinding<'a, 'c>>, CoreError> {
        predicate
            .relationships
            .iter()
            .enumerate()
            .map(|(index, pattern)| {
                self.nested_scoped_exists_relationship_mapping(
                    pattern,
                    local_nodes,
                    parent_local_nodes,
                )
                .map(|relationship| ExistsRelationshipSqlBinding {
                    pattern,
                    relationship,
                    alias: format!("{relationship_alias_prefix}{index}"),
                })
            })
            .collect()
    }

    fn nested_scoped_exists_relationship_mapping<'b, 'c>(
        &self,
        pattern: &'c RelationshipPattern,
        local_nodes: &BTreeMap<&'c str, &'a Node>,
        parent_local_nodes: &BTreeMap<&'b str, &'a Node>,
    ) -> Result<&'a Relationship, CoreError> {
        let left_node =
            self.nested_scoped_exists_node_mapping(&pattern.left, local_nodes, parent_local_nodes)?;
        let right_node = self.nested_scoped_exists_node_mapping(
            &pattern.right,
            local_nodes,
            parent_local_nodes,
        )?;
        let matches = self
            .validated
            .graph()
            .relationships_for_type(&pattern.relationship_type)
            .filter(|relationship| {
                Self::relationship_matches_labels(
                    relationship,
                    pattern.direction,
                    &left_node.label,
                    &right_node.label,
                )
            })
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [relationship] => Ok(*relationship),
            [] => Err(CoreError::internal(
                "validated nested EXISTS relationship mapping was not resolvable",
            )),
            _ => Err(CoreError::internal(
                "validated nested EXISTS relationship mapping was ambiguous",
            )),
        }
    }

    fn nested_scoped_exists_relationship_condition<'b, 'c>(
        &self,
        binding: &ExistsRelationshipSqlBinding<'a, 'c>,
        local_nodes: &BTreeMap<&'c str, &'a Node>,
        local_aliases: &BTreeMap<&'c str, String>,
        parent_relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        parent_local_nodes: &BTreeMap<&'b str, &'a Node>,
        parent_local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        let left_node = self.nested_scoped_exists_node_mapping(
            &binding.pattern.left,
            local_nodes,
            parent_local_nodes,
        )?;
        let right_node = self.nested_scoped_exists_node_mapping(
            &binding.pattern.right,
            local_nodes,
            parent_local_nodes,
        )?;
        let orientations = Self::relationship_orientations_for_labels(
            binding.relationship,
            binding.pattern.direction,
            &left_node.label,
            &right_node.label,
        )?;
        let has_multiple_orientations = orientations.len() > 1;
        let conditions = orientations
            .iter()
            .map(|orientation| {
                let left_ref = self.nested_scoped_exists_node_key_ref(
                    &binding.pattern.left,
                    left_node,
                    local_nodes,
                    local_aliases,
                    parent_relationships,
                    parent_local_nodes,
                    parent_local_aliases,
                )?;
                let right_ref = self.nested_scoped_exists_node_key_ref(
                    &binding.pattern.right,
                    right_node,
                    local_nodes,
                    local_aliases,
                    parent_relationships,
                    parent_local_nodes,
                    parent_local_aliases,
                )?;
                let condition = format!(
                    "{}.{} = {} AND {}.{} = {}",
                    quote_ident(&binding.alias),
                    quote_ident(&orientation.left_relationship_key),
                    left_ref,
                    quote_ident(&binding.alias),
                    quote_ident(&orientation.right_relationship_key),
                    right_ref
                );
                if has_multiple_orientations {
                    Ok(format!("({condition})"))
                } else {
                    Ok(condition)
                }
            })
            .collect::<Result<Vec<_>, CoreError>>()?;
        Self::render_condition_disjunction(&conditions)
    }

    fn nested_scoped_exists_node_mapping<'b, 'c>(
        &self,
        variable: &str,
        local_nodes: &BTreeMap<&'c str, &'a Node>,
        parent_local_nodes: &BTreeMap<&'b str, &'a Node>,
    ) -> Result<&'a Node, CoreError> {
        if let Some(node) = local_nodes.get(variable).copied() {
            return Ok(node);
        }
        if let Some(node) = parent_local_nodes.get(variable).copied() {
            return Ok(node);
        }
        let binding = self.validated.binding(variable)?;
        let ValidatedBindingKind::Node(node) = binding.kind() else {
            return Err(CoreError::internal(
                "validated nested EXISTS endpoint resolved to a non-node top-level binding",
            ));
        };
        Ok(*node)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Nested EXISTS key resolution needs child, parent scoped, and top-level alias contexts"
    )]
    fn nested_scoped_exists_node_key_ref<'b, 'c>(
        &self,
        variable: &str,
        node: &Node,
        local_nodes: &BTreeMap<&'c str, &'a Node>,
        local_aliases: &BTreeMap<&'c str, String>,
        parent_relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        parent_local_nodes: &BTreeMap<&'b str, &'a Node>,
        parent_local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        if local_nodes.contains_key(variable) {
            let alias = local_aliases.get(variable).ok_or_else(|| {
                CoreError::internal("validated nested EXISTS node alias was missing")
            })?;
            return Ok(format!("{}.{}", quote_ident(alias), quote_ident(&node.key)));
        }
        if parent_local_nodes.contains_key(variable) {
            let alias = parent_local_aliases.get(variable).ok_or_else(|| {
                CoreError::internal("validated parent EXISTS node alias was missing")
            })?;
            return Ok(format!("{}.{}", quote_ident(alias), quote_ident(&node.key)));
        }
        if Self::exists_relationship_for_variable(parent_relationships, variable).is_some() {
            return Err(CoreError::internal(
                "validated nested EXISTS endpoint resolved to a parent relationship variable",
            ));
        }
        self.render_binding_key_ref(variable)
    }

    fn render_scoped_simple_predicate<'b>(
        &self,
        lhs: &str,
        operator: ComparisonOperator,
        rhs: &PredicateRhs,
        relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        match (operator, rhs) {
            (ComparisonOperator::In, PredicateRhs::List(literals)) => {
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
                "validated scoped IN predicate did not contain a literal list",
            )),
            (
                ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains,
                PredicateRhs::Literal(Literal::String(value)),
            ) => Ok(format!(
                "{lhs} LIKE {} ESCAPE '\\'",
                render_like_pattern(operator, value)
            )),
            (
                ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains,
                _,
            ) => Err(CoreError::internal(
                "validated scoped string predicate did not contain a string literal",
            )),
            (ComparisonOperator::RegexMatch, PredicateRhs::List(_)) => Err(CoreError::internal(
                "validated scoped regex predicate did not contain a scalar RHS",
            )),
            (ComparisonOperator::RegexMatch, rhs) => Ok(render_regex_predicate(
                lhs,
                &self.render_exists_predicate_rhs(
                    rhs,
                    relationships,
                    local_nodes,
                    local_aliases,
                )?,
            )),
            (ComparisonOperator::Equal, PredicateRhs::Literal(Literal::Null)) => {
                Ok(format!("{lhs} IS NULL"))
            }
            (ComparisonOperator::NotEqual, PredicateRhs::Literal(Literal::Null)) => {
                Ok(format!("{lhs} IS NOT NULL"))
            }
            (
                ComparisonOperator::GreaterThan
                | ComparisonOperator::GreaterThanOrEqual
                | ComparisonOperator::LessThan
                | ComparisonOperator::LessThanOrEqual,
                PredicateRhs::Literal(Literal::Null),
            ) => Err(CoreError::internal(
                "validated scoped predicate contained an invalid null comparison",
            )),
            _ => Ok(format!(
                "{lhs} {} {}",
                render_operator(operator),
                self.render_exists_predicate_rhs(rhs, relationships, local_nodes, local_aliases)?
            )),
        }
    }

    fn render_scoped_scalar_predicate<'b>(
        &self,
        predicate: &ScalarPredicate,
        relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        if let Some(rendered) = self.try_render_scoped_count_existence_predicate(
            predicate,
            relationships,
            local_nodes,
            local_aliases,
        )? {
            return Ok(rendered);
        }

        let lhs = self.render_scoped_scalar_expression(
            &predicate.lhs,
            relationships,
            local_nodes,
            local_aliases,
        )?;
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
                "validated scoped scalar IN predicate did not contain a literal list",
            )),
            (
                ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains,
                ScalarPredicateRhs::Expression(rhs),
            ) => Ok(render_string_function_predicate(
                predicate.operator,
                &lhs,
                &self.render_scoped_scalar_expression(
                    rhs,
                    relationships,
                    local_nodes,
                    local_aliases,
                )?,
            )),
            (
                ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains,
                ScalarPredicateRhs::List(_),
            ) => Err(CoreError::internal(
                "validated scoped scalar string predicate did not contain a scalar RHS",
            )),
            (ComparisonOperator::RegexMatch, ScalarPredicateRhs::List(_)) => {
                Err(CoreError::internal(
                    "validated scoped scalar regex predicate did not contain a scalar RHS",
                ))
            }
            (ComparisonOperator::RegexMatch, ScalarPredicateRhs::Expression(rhs)) => {
                Ok(render_regex_predicate(
                    &lhs,
                    &self.render_scoped_scalar_expression(
                        rhs,
                        relationships,
                        local_nodes,
                        local_aliases,
                    )?,
                ))
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
                "validated scoped scalar predicate contained an invalid null comparison",
            )),
            (_, ScalarPredicateRhs::Expression(rhs)) => Ok(format!(
                "{lhs} {} {}",
                render_operator(predicate.operator),
                self.render_scoped_scalar_expression(
                    rhs,
                    relationships,
                    local_nodes,
                    local_aliases
                )?
            )),
            (_, ScalarPredicateRhs::List(_)) => Err(CoreError::internal(
                "validated scoped scalar literal list predicate reached generic RHS renderer",
            )),
        }
    }

    fn try_render_scoped_count_existence_predicate<'b>(
        &self,
        predicate: &ScalarPredicate,
        relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<Option<String>, CoreError> {
        let ScalarExpression::CountSubquery {
            pattern,
            distinct_target: None,
        } = &predicate.lhs
        else {
            return Ok(None);
        };
        let Some(existence) = Self::count_existence_predicate(predicate.operator, &predicate.rhs)
        else {
            return Ok(None);
        };
        self.render_scoped_count_existence_predicate(
            pattern,
            existence,
            relationships,
            local_nodes,
            local_aliases,
        )
        .map(Some)
    }

    fn count_existence_predicate(
        operator: ComparisonOperator,
        rhs: &ScalarPredicateRhs,
    ) -> Option<CountExistencePredicate> {
        let ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::Integer(value))) =
            rhs
        else {
            return None;
        };
        match operator {
            ComparisonOperator::Equal => match *value {
                0 => Some(CountExistencePredicate::NotExists),
                value if value < 0 => Some(CountExistencePredicate::AlwaysFalse),
                _ => None,
            },
            ComparisonOperator::NotEqual => match *value {
                0 => Some(CountExistencePredicate::Exists),
                value if value < 0 => Some(CountExistencePredicate::AlwaysTrue),
                _ => None,
            },
            ComparisonOperator::GreaterThan => match *value {
                value if value < 0 => Some(CountExistencePredicate::AlwaysTrue),
                0 => Some(CountExistencePredicate::Exists),
                _ => None,
            },
            ComparisonOperator::GreaterThanOrEqual => match *value {
                value if value <= 0 => Some(CountExistencePredicate::AlwaysTrue),
                1 => Some(CountExistencePredicate::Exists),
                _ => None,
            },
            ComparisonOperator::LessThan => match *value {
                value if value <= 0 => Some(CountExistencePredicate::AlwaysFalse),
                1 => Some(CountExistencePredicate::NotExists),
                _ => None,
            },
            ComparisonOperator::LessThanOrEqual => match *value {
                value if value < 0 => Some(CountExistencePredicate::AlwaysFalse),
                0 => Some(CountExistencePredicate::NotExists),
                _ => None,
            },
            ComparisonOperator::In
            | ComparisonOperator::StartsWith
            | ComparisonOperator::EndsWith
            | ComparisonOperator::Contains
            | ComparisonOperator::RegexMatch => None,
        }
    }

    fn render_count_existence_predicate(
        &self,
        pattern: &CountSubqueryPattern,
        predicate: CountExistencePredicate,
    ) -> Result<String, CoreError> {
        match predicate {
            CountExistencePredicate::AlwaysTrue => Ok("TRUE".to_string()),
            CountExistencePredicate::AlwaysFalse => Ok("FALSE".to_string()),
            CountExistencePredicate::Exists | CountExistencePredicate::NotExists => {
                let select = self.render_count_exists_select(pattern)?;
                Ok(match predicate {
                    CountExistencePredicate::Exists => format!("EXISTS {select}"),
                    CountExistencePredicate::NotExists => format!("NOT EXISTS {select}"),
                    CountExistencePredicate::AlwaysTrue | CountExistencePredicate::AlwaysFalse => {
                        unreachable!("constant count predicates handled before EXISTS rendering")
                    }
                })
            }
        }
    }

    fn render_scoped_count_existence_predicate<'b>(
        &self,
        pattern: &CountSubqueryPattern,
        predicate: CountExistencePredicate,
        relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        match predicate {
            CountExistencePredicate::AlwaysTrue => Ok("TRUE".to_string()),
            CountExistencePredicate::AlwaysFalse => Ok("FALSE".to_string()),
            CountExistencePredicate::Exists | CountExistencePredicate::NotExists => {
                let select = self.render_nested_scoped_count_exists_select(
                    pattern,
                    relationships,
                    local_nodes,
                    local_aliases,
                )?;
                Ok(match predicate {
                    CountExistencePredicate::Exists => format!("EXISTS {select}"),
                    CountExistencePredicate::NotExists => format!("NOT EXISTS {select}"),
                    CountExistencePredicate::AlwaysTrue | CountExistencePredicate::AlwaysFalse => {
                        unreachable!("constant count predicates handled before EXISTS rendering")
                    }
                })
            }
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "This exhaustive scalar IR dispatcher mirrors the top-level scalar renderer for scoped aliases"
    )]
    fn render_scoped_scalar_expression<'b>(
        &self,
        expression: &ScalarExpression,
        relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        if let Some(rendered) = self.render_scoped_simple_scalar_expression(
            expression,
            relationships,
            local_nodes,
            local_aliases,
        )? {
            return Ok(rendered);
        }
        if let Some(rendered) = self.render_scoped_undirected_endpoint_scalar_expression(
            expression,
            relationships,
            local_nodes,
            local_aliases,
        )? {
            return Ok(rendered);
        }
        if let Some(rendered) = self.render_scoped_graph_metadata_scalar_expression(
            expression,
            relationships,
            local_nodes,
            local_aliases,
        )? {
            return Ok(rendered);
        }

        match expression {
            ScalarExpression::Property(property) => {
                self.render_exists_property_ref(property, relationships, local_nodes, local_aliases)
            }
            ScalarExpression::UndirectedEndpointProperty { .. }
            | ScalarExpression::UndirectedEndpointKey { .. }
            | ScalarExpression::UndirectedEndpointElementId { .. }
            | ScalarExpression::UndirectedEndpointLabels { .. }
            | ScalarExpression::UndirectedEndpointPropertyKeys { .. } => Err(CoreError::internal(
                "validated scoped undirected endpoint scalar reached SQL renderer",
            )),
            ScalarExpression::Literal(literal) => Ok(render_literal(literal)),
            ScalarExpression::LiteralList { literals } => Ok(render_literal_list(literals)),
            ScalarExpression::TypedLiteralList {
                literals,
                element_type,
            } => Ok(render_typed_literal_list(literals, *element_type)),
            ScalarExpression::GraphKeyList { variables } => self.render_scoped_graph_key_list_ref(
                variables,
                relationships,
                local_nodes,
                local_aliases,
            ),
            ScalarExpression::Predicate(predicate) => self.render_scoped_predicate_expression(
                predicate,
                relationships,
                local_nodes,
                local_aliases,
            ),
            ScalarExpression::CountSubquery {
                pattern,
                distinct_target,
            } => self
                .render_nested_scoped_count_subquery_expression(
                    pattern,
                    distinct_target.as_deref(),
                    relationships,
                    local_nodes,
                    local_aliases,
                ),
            ScalarExpression::CollectSubquery { .. } => Err(CoreError::InvalidInput(
                "nested COLLECT subqueries require scoped list-value planning and are not supported yet"
                    .to_string(),
            )),
            ScalarExpression::PresenceGated {
                presence_variable,
                expression,
            } => {
                let presence = self.render_scoped_binding_presence_ref(
                    presence_variable,
                    relationships,
                    local_nodes,
                    local_aliases,
                )?;
                let expression = self.render_scoped_scalar_expression(
                    expression,
                    relationships,
                    local_nodes,
                    local_aliases,
                )?;
                Ok(format!(
                    "CASE WHEN {presence} IS NULL THEN NULL ELSE {expression} END"
                ))
            }
            ScalarExpression::Coalesce { expressions } => {
                let rendered = expressions
                    .iter()
                    .map(|expression| {
                        self.render_scoped_scalar_expression(
                            expression,
                            relationships,
                            local_nodes,
                            local_aliases,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .join(", ");
                Ok(format!("COALESCE({rendered})"))
            }
            ScalarExpression::NullIf { expression, value } => Ok(format!(
                "NULLIF({}, {})",
                self.render_scoped_scalar_expression(
                    expression,
                    relationships,
                    local_nodes,
                    local_aliases,
                )?,
                self.render_scoped_scalar_expression(
                    value,
                    relationships,
                    local_nodes,
                    local_aliases
                )?
            )),
            ScalarExpression::Replace {
                expression,
                search,
                replacement,
            } => Ok(format!(
                "REPLACE({}, {}, {})",
                self.render_scoped_scalar_expression(
                    expression,
                    relationships,
                    local_nodes,
                    local_aliases,
                )?,
                self.render_scoped_scalar_expression(
                    search,
                    relationships,
                    local_nodes,
                    local_aliases
                )?,
                self.render_scoped_scalar_expression(
                    replacement,
                    relationships,
                    local_nodes,
                    local_aliases,
                )?
            )),
            ScalarExpression::Substring {
                expression,
                start,
                length,
            } => {
                let mut sql = format!(
                    "SUBSTRING({} FROM ({} + 1)",
                    self.render_scoped_scalar_expression(
                        expression,
                        relationships,
                        local_nodes,
                        local_aliases,
                    )?,
                    self.render_scoped_scalar_expression(
                        start,
                        relationships,
                        local_nodes,
                        local_aliases
                    )?
                );
                if let Some(length) = length {
                    write!(
                        &mut sql,
                        " FOR {}",
                        self.render_scoped_scalar_expression(
                            length,
                            relationships,
                            local_nodes,
                            local_aliases,
                        )?
                    )
                    .map_err(|error| CoreError::internal(error.to_string()))?;
                }
                sql.push(')');
                Ok(sql)
            }
            ScalarExpression::Round { expression, places } => {
                let expression_sql = self.render_scoped_scalar_expression(
                    expression,
                    relationships,
                    local_nodes,
                    local_aliases,
                )?;
                let Some(places) = places else {
                    return Ok(format!("round({expression_sql})"));
                };
                Ok(format!(
                    "round({expression_sql}, {})",
                    self.render_scoped_scalar_expression(
                        places,
                        relationships,
                        local_nodes,
                        local_aliases,
                    )?
                ))
            }
            ScalarExpression::Arithmetic {
                operator,
                left,
                right,
            } => {
                let left = self.render_scoped_scalar_expression(
                    left,
                    relationships,
                    local_nodes,
                    local_aliases,
                )?;
                let right = self.render_scoped_scalar_expression(
                    right,
                    relationships,
                    local_nodes,
                    local_aliases,
                )?;
                if *operator == ArithmeticOperator::Power {
                    return Ok(format!("power({left}, {right})"));
                }
                Ok(format!(
                    "({left} {} {right})",
                    render_arithmetic_operator(*operator)
                ))
            }
            ScalarExpression::Case {
                alternatives,
                else_expression,
            } => {
                let mut sql = String::from("CASE");
                for alternative in alternatives {
                    write!(
                        &mut sql,
                        " WHEN {} THEN {}",
                        self.render_scoped_predicate_expression(
                            &alternative.when,
                            relationships,
                            local_nodes,
                            local_aliases,
                        )?,
                        self.render_scoped_scalar_expression(
                            &alternative.then,
                            relationships,
                            local_nodes,
                            local_aliases,
                        )?
                    )
                    .map_err(|error| CoreError::internal(error.to_string()))?;
                }
                if let Some(else_expression) = else_expression {
                    write!(
                        &mut sql,
                        " ELSE {}",
                        self.render_scoped_scalar_expression(
                            else_expression,
                            relationships,
                            local_nodes,
                            local_aliases,
                        )?
                    )
                    .map_err(|error| CoreError::internal(error.to_string()))?;
                }
                sql.push_str(" END");
                Ok(sql)
            }
            ScalarExpression::Atan2 { y, x } => Ok(format!(
                "atan2({}, {})",
                self.render_scoped_scalar_expression(y, relationships, local_nodes, local_aliases)?,
                self.render_scoped_scalar_expression(x, relationships, local_nodes, local_aliases)?
            )),
            _ => unreachable!("scoped scalar expression handled above"),
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "This exhaustive scoped scalar IR renderer keeps function rendering total over every scalar variant"
    )]
    fn render_scoped_simple_scalar_expression<'b>(
        &self,
        expression: &ScalarExpression,
        relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<Option<String>, CoreError> {
        if let Some(rendered) = self.render_scoped_scalar_cast_expression(
            expression,
            relationships,
            local_nodes,
            local_aliases,
        )? {
            return Ok(Some(rendered));
        }

        let unary = |function_name: &str, expression: &ScalarExpression| {
            Ok(Some(format!(
                "{function_name}({})",
                self.render_scoped_scalar_expression(
                    expression,
                    relationships,
                    local_nodes,
                    local_aliases,
                )?
            )))
        };
        let binary = |function_name: &str, left: &ScalarExpression, right: &ScalarExpression| {
            Ok(Some(format!(
                "{function_name}({}, {})",
                self.render_scoped_scalar_expression(
                    left,
                    relationships,
                    local_nodes,
                    local_aliases,
                )?,
                self.render_scoped_scalar_expression(
                    right,
                    relationships,
                    local_nodes,
                    local_aliases,
                )?
            )))
        };
        if let Some((function_name, expression, pattern)) =
            Self::string_predicate_function_expression(expression)
        {
            return binary(function_name, expression, pattern);
        }

        match expression {
            ScalarExpression::ToLower { expression } => unary("LOWER", expression),
            ScalarExpression::ToUpper { expression } => unary("UPPER", expression),
            ScalarExpression::Trim { expression } => unary("TRIM", expression),
            ScalarExpression::LTrim { expression } => unary("LTRIM", expression),
            ScalarExpression::RTrim { expression } => unary("RTRIM", expression),
            ScalarExpression::CharacterLength { expression } => {
                unary("character_length", expression)
            }
            ScalarExpression::Left { expression, count } => binary("left", expression, count),
            ScalarExpression::Right { expression, count } => binary("right", expression, count),
            ScalarExpression::StringIndices {
                expression,
                pattern,
            } => binary("coral_string_indices", expression, pattern),
            ScalarExpression::LPad {
                expression,
                length,
                fill,
            } => self.render_scoped_ternary_function_expression(
                "lpad",
                expression,
                length,
                fill,
                relationships,
                local_nodes,
                local_aliases,
            ),
            ScalarExpression::RPad {
                expression,
                length,
                fill,
            } => self.render_scoped_ternary_function_expression(
                "rpad",
                expression,
                length,
                fill,
                relationships,
                local_nodes,
                local_aliases,
            ),
            ScalarExpression::Reverse { expression } => unary("reverse", expression),
            ScalarExpression::Abs { expression } => unary("abs", expression),
            ScalarExpression::Ceil { expression } => unary("ceil", expression),
            ScalarExpression::Floor { expression } => unary("floor", expression),
            ScalarExpression::Sqrt { expression } => unary("sqrt", expression),
            ScalarExpression::Sign { expression } => unary("signum", expression),
            ScalarExpression::Exp { expression } => unary("exp", expression),
            ScalarExpression::Log { expression } => unary("ln", expression),
            ScalarExpression::Log10 { expression } => unary("log10", expression),
            ScalarExpression::Sin { expression } => unary("sin", expression),
            ScalarExpression::Cos { expression } => unary("cos", expression),
            ScalarExpression::Tan { expression } => unary("tan", expression),
            ScalarExpression::Cot { expression } => unary("cot", expression),
            ScalarExpression::Asin { expression } => unary("asin", expression),
            ScalarExpression::Acos { expression } => unary("acos", expression),
            ScalarExpression::Atan { expression } => unary("atan", expression),
            ScalarExpression::Atan2 { y, x } => binary("atan2", y, x),
            ScalarExpression::Degrees { expression } => unary("degrees", expression),
            ScalarExpression::Radians { expression } => unary("radians", expression),
            ScalarExpression::IsNaN { expression } => unary("isnan", expression),
            ScalarExpression::Negate { expression } => Ok(Some(format!(
                "-({})",
                self.render_scoped_scalar_expression(
                    expression,
                    relationships,
                    local_nodes,
                    local_aliases,
                )?
            ))),
            _ => Ok(None),
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Scoped rendering helpers pass the same relationship, node, and alias context as neighboring renderers"
    )]
    fn render_scoped_ternary_function_expression<'b>(
        &self,
        function_name: &str,
        first: &ScalarExpression,
        second: &ScalarExpression,
        third: &ScalarExpression,
        relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<Option<String>, CoreError> {
        Ok(Some(format!(
            "{function_name}({}, {}, {})",
            self.render_scoped_scalar_expression(first, relationships, local_nodes, local_aliases)?,
            self.render_scoped_scalar_expression(
                second,
                relationships,
                local_nodes,
                local_aliases,
            )?,
            self.render_scoped_scalar_expression(third, relationships, local_nodes, local_aliases)?
        )))
    }

    fn render_scoped_scalar_cast_expression<'b>(
        &self,
        expression: &ScalarExpression,
        relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<Option<String>, CoreError> {
        let cast = |expression: &ScalarExpression, target_type: &str| {
            Ok(Some(format!(
                "CAST({} AS {target_type})",
                self.render_scoped_scalar_expression(
                    expression,
                    relationships,
                    local_nodes,
                    local_aliases,
                )?
            )))
        };
        let try_cast = |expression: &ScalarExpression, target_type: &str| {
            Ok(Some(format!(
                "TRY_CAST({} AS {target_type})",
                self.render_scoped_scalar_expression(
                    expression,
                    relationships,
                    local_nodes,
                    local_aliases,
                )?
            )))
        };
        match expression {
            ScalarExpression::ToString { expression } => cast(expression, "VARCHAR"),
            ScalarExpression::ToInteger { expression } => cast(expression, "BIGINT"),
            ScalarExpression::ToFloat { expression } => cast(expression, "DOUBLE"),
            ScalarExpression::ToBoolean { expression } => cast(expression, "BOOLEAN"),
            ScalarExpression::ToStringOrNull { expression } => try_cast(expression, "VARCHAR"),
            ScalarExpression::ToIntegerOrNull { expression } => try_cast(expression, "BIGINT"),
            ScalarExpression::ToFloatOrNull { expression } => try_cast(expression, "DOUBLE"),
            ScalarExpression::ToBooleanOrNull { expression } => try_cast(expression, "BOOLEAN"),
            _ => Ok(None),
        }
    }

    fn render_scoped_graph_metadata_scalar_expression<'b>(
        &self,
        expression: &ScalarExpression,
        relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<Option<String>, CoreError> {
        match expression {
            ScalarExpression::Key { variable } => self
                .render_scoped_binding_key_ref(variable, relationships, local_nodes, local_aliases)
                .map(Some),
            ScalarExpression::GraphKeyList { variables } => self
                .render_scoped_graph_key_list_ref(
                    variables,
                    relationships,
                    local_nodes,
                    local_aliases,
                )
                .map(Some),
            ScalarExpression::ElementId { variable } => Ok(Some(format!(
                "CAST({} AS VARCHAR)",
                self.render_scoped_binding_key_ref(
                    variable,
                    relationships,
                    local_nodes,
                    local_aliases,
                )?
            ))),
            ScalarExpression::GraphIdentity { variable } => self
                .render_scoped_graph_identity_ref(
                    variable,
                    relationships,
                    local_nodes,
                    local_aliases,
                )
                .map(Some),
            ScalarExpression::GraphPresence { variable } => Ok(Some(format!(
                "CAST({} AS VARCHAR)",
                self.render_scoped_binding_presence_ref(
                    variable,
                    relationships,
                    local_nodes,
                    local_aliases,
                )?
            ))),
            ScalarExpression::NodeLabels { variable, label } => self
                .render_scoped_node_labels_ref(
                    variable,
                    label,
                    relationships,
                    local_nodes,
                    local_aliases,
                )
                .map(Some),
            ScalarExpression::PropertyKeys { variable } => self
                .render_scoped_property_keys_ref(
                    variable,
                    relationships,
                    local_nodes,
                    local_aliases,
                )
                .map(Some),
            ScalarExpression::RelationshipType {
                variable,
                relationship_type,
            } => self
                .render_scoped_relationship_type_ref(
                    variable,
                    relationship_type,
                    relationships,
                    local_nodes,
                )
                .map(Some),
            _ => Ok(None),
        }
    }

    fn render_scoped_undirected_endpoint_scalar_expression<'b>(
        &self,
        expression: &ScalarExpression,
        relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<Option<String>, CoreError> {
        match expression {
            ScalarExpression::UndirectedEndpointProperty {
                relationship,
                endpoint,
                property,
            } => self
                .render_scoped_undirected_endpoint_property_ref(
                    relationship,
                    *endpoint,
                    property,
                    relationships,
                    local_nodes,
                    local_aliases,
                )
                .map(Some),
            ScalarExpression::UndirectedEndpointKey {
                relationship,
                endpoint,
            } => self
                .render_scoped_undirected_endpoint_key_ref(
                    relationship,
                    *endpoint,
                    relationships,
                    local_nodes,
                    local_aliases,
                )
                .map(Some),
            ScalarExpression::UndirectedEndpointElementId {
                relationship,
                endpoint,
            } => self
                .render_scoped_undirected_endpoint_element_id_ref(
                    relationship,
                    *endpoint,
                    relationships,
                    local_nodes,
                    local_aliases,
                )
                .map(Some),
            ScalarExpression::UndirectedEndpointLabels {
                relationship,
                label,
                ..
            } => self
                .render_scoped_undirected_endpoint_labels_ref(
                    relationship,
                    label,
                    relationships,
                    local_nodes,
                    local_aliases,
                )
                .map(Some),
            ScalarExpression::UndirectedEndpointPropertyKeys { relationship, .. } => self
                .render_scoped_undirected_endpoint_property_keys_ref(
                    relationship,
                    relationships,
                    local_nodes,
                    local_aliases,
                )
                .map(Some),
            _ => Ok(None),
        }
    }

    fn render_scoped_binding_key_ref<'b>(
        &self,
        variable: &str,
        relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        self.render_exists_key_ref(variable, relationships, local_nodes, local_aliases)
    }

    fn render_scoped_graph_key_list_ref<'b>(
        &self,
        variables: &[String],
        relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        let values = variables
            .iter()
            .map(|variable| {
                self.render_scoped_binding_key_ref(
                    variable,
                    relationships,
                    local_nodes,
                    local_aliases,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(render_sql_array(&values))
    }

    fn render_scoped_binding_presence_ref<'b>(
        &self,
        variable: &str,
        relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        if let Some(relationship) = Self::exists_relationship_for_variable(relationships, variable)
        {
            let column = relationship
                .relationship
                .key
                .as_deref()
                .unwrap_or(&relationship.relationship.from.key);
            return Ok(format!(
                "{}.{}",
                quote_ident(&relationship.alias),
                quote_ident(column)
            ));
        }
        if let Some(node) = local_nodes.get(variable).copied() {
            let alias = local_aliases
                .get(variable)
                .ok_or_else(|| CoreError::internal("validated scoped node alias was missing"))?;
            return Ok(format!("{}.{}", quote_ident(alias), quote_ident(&node.key)));
        }
        self.render_binding_presence_ref(variable)
    }

    fn render_scoped_graph_identity_ref<'b>(
        &self,
        variable: &str,
        relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        let prefix = if let Some(relationship) =
            Self::exists_relationship_for_variable(relationships, variable)
        {
            format!(
                "relationship:{}:",
                relationship.relationship.relationship_type
            )
        } else if let Some(node) = local_nodes.get(variable).copied() {
            format!("node:{}:", node.label)
        } else {
            return self.render_binding_graph_identity_ref(variable);
        };
        let key = self.render_scoped_binding_key_ref(
            variable,
            relationships,
            local_nodes,
            local_aliases,
        )?;
        Ok(format!(
            "CASE WHEN {key} IS NULL THEN NULL ELSE concat({}, CAST({key} AS VARCHAR)) END",
            quote_string_literal(&prefix)
        ))
    }

    fn render_scoped_relationship_type_ref<'b>(
        &self,
        variable: &str,
        relationship_type: &str,
        relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
    ) -> Result<String, CoreError> {
        if let Some(relationship) = Self::exists_relationship_for_variable(relationships, variable)
        {
            if relationship.relationship.relationship_type != relationship_type {
                return Err(CoreError::internal(
                    "validated scoped relationship type expression did not match relationship type",
                ));
            }
            let column = relationship
                .relationship
                .key
                .as_deref()
                .unwrap_or(&relationship.relationship.from.key);
            let presence = format!(
                "{}.{}",
                quote_ident(&relationship.alias),
                quote_ident(column)
            );
            return Ok(format!(
                "CASE WHEN {presence} IS NULL THEN NULL ELSE {} END",
                quote_string_literal(relationship_type)
            ));
        }
        if local_nodes.contains_key(variable) {
            return Err(CoreError::internal(
                "validated scoped relationship type expression referenced a node",
            ));
        }
        self.render_relationship_type_ref(variable, relationship_type)
    }

    fn render_scoped_node_labels_ref<'b>(
        &self,
        variable: &str,
        label: &str,
        relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        if Self::exists_relationship_for_variable(relationships, variable).is_some() {
            return Err(CoreError::internal(
                "validated scoped labels expression referenced a relationship",
            ));
        }
        if let Some(node) = local_nodes.get(variable).copied() {
            if node.label != label {
                return Err(CoreError::internal(
                    "validated scoped labels expression did not match node label",
                ));
            }
            let presence = self.render_scoped_binding_presence_ref(
                variable,
                relationships,
                local_nodes,
                local_aliases,
            )?;
            return Ok(format!(
                "CASE WHEN {presence} IS NULL THEN NULL ELSE make_array({}) END",
                quote_string_literal(label)
            ));
        }
        self.render_node_labels_ref(variable, label)
    }

    fn render_scoped_property_keys_ref<'b>(
        &self,
        variable: &str,
        relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        let property_names = if let Some(relationship) =
            Self::exists_relationship_for_variable(relationships, variable)
        {
            Some(relationship.relationship.properties.keys())
        } else {
            local_nodes.get(variable).map(|node| node.properties.keys())
        };
        let Some(property_names) = property_names else {
            return self.render_property_keys_ref(variable);
        };
        let property_names = property_names
            .map(|property| quote_string_literal(property))
            .collect::<Vec<_>>()
            .join(", ");
        let presence = self.render_scoped_binding_presence_ref(
            variable,
            relationships,
            local_nodes,
            local_aliases,
        )?;
        Ok(format!(
            "CASE WHEN {presence} IS NULL THEN NULL ELSE make_array({property_names}) END"
        ))
    }

    fn render_scoped_undirected_endpoint_property_ref<'b>(
        &self,
        relationship_variable: &str,
        endpoint: UndirectedRelationshipEndpoint,
        property: &str,
        relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        if Self::exists_relationship_for_variable(relationships, relationship_variable).is_none() {
            return self.render_undirected_endpoint_property_ref(
                relationship_variable,
                endpoint,
                property,
            );
        }

        let selection = self.render_scoped_undirected_endpoint_selection(
            relationship_variable,
            endpoint,
            relationships,
            local_nodes,
            local_aliases,
        )?;
        let left_property = self.render_exists_property_ref(
            &PropertyRef {
                variable: selection.left_variable,
                property: property.to_string(),
            },
            relationships,
            local_nodes,
            local_aliases,
        )?;
        let right_property = self.render_exists_property_ref(
            &PropertyRef {
                variable: selection.right_variable,
                property: property.to_string(),
            },
            relationships,
            local_nodes,
            local_aliases,
        )?;
        let presence = selection.presence;
        let left_matches_endpoint = selection.left_matches_endpoint;
        Ok(format!(
            "CASE WHEN {presence} IS NULL THEN NULL ELSE CASE WHEN {left_matches_endpoint} THEN {left_property} ELSE {right_property} END END"
        ))
    }

    fn render_scoped_undirected_endpoint_key_ref<'b>(
        &self,
        relationship_variable: &str,
        endpoint: UndirectedRelationshipEndpoint,
        relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        if Self::exists_relationship_for_variable(relationships, relationship_variable).is_none() {
            return self.render_undirected_endpoint_key_ref(relationship_variable, endpoint);
        }

        let selection = self.render_scoped_undirected_endpoint_selection(
            relationship_variable,
            endpoint,
            relationships,
            local_nodes,
            local_aliases,
        )?;
        let presence = selection.presence;
        let left_matches_endpoint = selection.left_matches_endpoint;
        let left_key = selection.left_key;
        let right_key = selection.right_key;
        Ok(format!(
            "CASE WHEN {presence} IS NULL THEN NULL ELSE CASE WHEN {left_matches_endpoint} THEN {left_key} ELSE {right_key} END END"
        ))
    }

    fn render_scoped_undirected_endpoint_element_id_ref<'b>(
        &self,
        relationship_variable: &str,
        endpoint: UndirectedRelationshipEndpoint,
        relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        Ok(format!(
            "CAST({} AS VARCHAR)",
            self.render_scoped_undirected_endpoint_key_ref(
                relationship_variable,
                endpoint,
                relationships,
                local_nodes,
                local_aliases,
            )?
        ))
    }

    fn render_scoped_undirected_endpoint_labels_ref<'b>(
        &self,
        relationship_variable: &str,
        label: &str,
        relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        if Self::exists_relationship_for_variable(relationships, relationship_variable).is_none() {
            return self.render_undirected_endpoint_labels_ref(relationship_variable, label);
        }

        let presence = self.render_scoped_binding_presence_ref(
            relationship_variable,
            relationships,
            local_nodes,
            local_aliases,
        )?;
        Ok(format!(
            "CASE WHEN {presence} IS NULL THEN NULL ELSE make_array({}) END",
            quote_string_literal(label)
        ))
    }

    fn render_scoped_undirected_endpoint_property_keys_ref<'b>(
        &self,
        relationship_variable: &str,
        relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        let Some(relationship) =
            Self::exists_relationship_for_variable(relationships, relationship_variable)
        else {
            return self.render_undirected_endpoint_property_keys_ref(relationship_variable);
        };

        let node = self.exists_node_mapping(local_nodes, &relationship.pattern.left)?;
        let property_names = node
            .properties
            .keys()
            .map(|property| quote_string_literal(property))
            .collect::<Vec<_>>()
            .join(", ");
        let presence = self.render_scoped_binding_presence_ref(
            relationship_variable,
            relationships,
            local_nodes,
            local_aliases,
        )?;
        Ok(format!(
            "CASE WHEN {presence} IS NULL THEN NULL ELSE make_array({property_names}) END"
        ))
    }

    fn render_scoped_undirected_endpoint_selection<'b>(
        &self,
        relationship_variable: &str,
        endpoint: UndirectedRelationshipEndpoint,
        relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<UndirectedEndpointSelection, CoreError> {
        let relationship =
            Self::exists_relationship_for_variable(relationships, relationship_variable)
                .ok_or_else(|| {
                    CoreError::internal(
                        "validated scoped undirected endpoint referenced unknown relationship",
                    )
                })?;
        let endpoint_column = match endpoint {
            UndirectedRelationshipEndpoint::Start => &relationship.relationship.from.key,
            UndirectedRelationshipEndpoint::End => &relationship.relationship.to.key,
        };
        let selector = format!(
            "{}.{}",
            quote_ident(&relationship.alias),
            quote_ident(endpoint_column)
        );
        let presence = self.render_scoped_binding_presence_ref(
            relationship_variable,
            relationships,
            local_nodes,
            local_aliases,
        )?;
        let left_key = self.render_scoped_binding_key_ref(
            &relationship.pattern.left,
            relationships,
            local_nodes,
            local_aliases,
        )?;
        let right_key = self.render_scoped_binding_key_ref(
            &relationship.pattern.right,
            relationships,
            local_nodes,
            local_aliases,
        )?;
        Ok(UndirectedEndpointSelection {
            presence,
            left_matches_endpoint: format!("{left_key} = {selector}"),
            left_key,
            right_key,
            left_variable: relationship.pattern.left.clone(),
            right_variable: relationship.pattern.right.clone(),
        })
    }

    fn render_scoped_property_key_membership_predicate<'b>(
        &self,
        predicate: &PropertyKeyMembershipPredicate,
        relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        let has_key = if let Some(relationship) =
            Self::exists_relationship_for_variable(relationships, &predicate.variable)
        {
            Some(
                relationship
                    .relationship
                    .properties
                    .contains_key(&predicate.key),
            )
        } else {
            local_nodes
                .get(predicate.variable.as_str())
                .map(|node| node.properties.contains_key(&predicate.key))
        };
        let Some(has_key) = has_key else {
            return self.render_property_key_membership_predicate(predicate);
        };
        let presence_variable = predicate
            .presence_variable
            .as_deref()
            .unwrap_or(&predicate.variable);
        let presence = self.render_scoped_binding_presence_ref(
            presence_variable,
            relationships,
            local_nodes,
            local_aliases,
        )?;
        let value = if has_key { "TRUE" } else { "FALSE" };
        Ok(format!(
            "CASE WHEN {presence} IS NULL THEN NULL ELSE {value} END"
        ))
    }

    fn render_exists_predicate_rhs<'b>(
        &self,
        rhs: &PredicateRhs,
        relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        match rhs {
            PredicateRhs::Literal(literal) => Ok(render_literal(literal)),
            PredicateRhs::Property(property) => {
                self.render_exists_property_ref(property, relationships, local_nodes, local_aliases)
            }
            PredicateRhs::Key { variable } => {
                self.render_exists_key_ref(variable, relationships, local_nodes, local_aliases)
            }
            PredicateRhs::ElementId { variable } => Ok(format!(
                "CAST({} AS VARCHAR)",
                self.render_exists_key_ref(variable, relationships, local_nodes, local_aliases,)?
            )),
            PredicateRhs::List(_) => Err(CoreError::internal(
                "validated EXISTS literal list predicate reached generic RHS renderer",
            )),
        }
    }

    fn render_exists_property_ref<'b>(
        &self,
        property: &PropertyRef,
        relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        if let Some(relationship) =
            Self::exists_relationship_for_variable(relationships, property.variable.as_str())
        {
            let column = relationship
                .relationship
                .column_for_property(&property.property)
                .ok_or_else(|| {
                    CoreError::internal("validated EXISTS relationship property was not resolvable")
                })?;
            return Ok(format!(
                "{}.{}",
                quote_ident(&relationship.alias),
                quote_ident(column)
            ));
        }
        if let Some(node) = local_nodes.get(property.variable.as_str()).copied() {
            let alias = local_aliases
                .get(property.variable.as_str())
                .ok_or_else(|| CoreError::internal("validated EXISTS node alias was missing"))?;
            let column = node
                .column_for_property(&property.property)
                .ok_or_else(|| {
                    CoreError::internal("validated EXISTS node property was not resolvable")
                })?;
            return Ok(format!("{}.{}", quote_ident(alias), quote_ident(column)));
        }
        self.render_property_ref(property)
    }

    fn render_exists_key_ref<'b>(
        &self,
        variable: &str,
        relationships: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        if let Some(relationship) = Self::exists_relationship_for_variable(relationships, variable)
        {
            let key = relationship.relationship.key.as_deref().ok_or_else(|| {
                CoreError::internal("validated EXISTS relationship key was not resolvable")
            })?;
            return Ok(format!(
                "{}.{}",
                quote_ident(&relationship.alias),
                quote_ident(key)
            ));
        }
        if let Some(node) = local_nodes.get(variable).copied() {
            let alias = local_aliases
                .get(variable)
                .ok_or_else(|| CoreError::internal("validated EXISTS node alias was missing"))?;
            return Ok(format!("{}.{}", quote_ident(alias), quote_ident(&node.key)));
        }
        self.render_binding_key_ref(variable)
    }

    fn exists_relationship_for_variable<'b, 'c>(
        relationships: &'c [ExistsRelationshipSqlBinding<'a, 'b>],
        variable: &str,
    ) -> Option<&'c ExistsRelationshipSqlBinding<'a, 'b>> {
        relationships
            .iter()
            .find(|relationship| relationship.pattern.variable.as_deref() == Some(variable))
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
            let nulls = render_null_order(key.nulls);
            keys.push(format!(
                "{} {}{}",
                self.render_order_expression(&key.expression)?,
                match key.direction {
                    OrderDirection::Ascending => "ASC",
                    OrderDirection::Descending => "DESC",
                },
                nulls,
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
            OrderExpression::CountAll => Ok("COUNT(*)".to_string()),
            OrderExpression::Aggregate {
                function,
                target,
                distinct,
            } => self.render_aggregate_invocation(*function, target, *distinct),
            OrderExpression::Scalar(ScalarExpression::Literal(literal)) => {
                Ok(render_order_literal(literal))
            }
            OrderExpression::Scalar(ScalarExpression::Predicate(predicate)) => {
                self.render_order_predicate_expression(predicate)
            }
            OrderExpression::Scalar(expression) => self.render_scalar_expression(expression),
            OrderExpression::Literal(literal) => Ok(render_order_literal(literal)),
            OrderExpression::ProjectionAlias(alias) => Ok(quote_ident(alias)),
        }
    }

    fn render_order_predicate_expression(
        &self,
        predicate: &PredicateExpression,
    ) -> Result<String, CoreError> {
        if let PredicateExpression::ExistsPattern(pattern) = predicate
            && let Some(precomputed) =
                self.precomputed_scalar_subqueries
                    .iter()
                    .find(|precomputed| {
                        precomputed.candidate == ScalarSubqueryCandidate::Exists(pattern.clone())
                    })
        {
            return Ok(Self::render_precomputed_exists_ref(precomputed));
        }
        self.render_scalar_predicate_expression(predicate)
    }

    fn render_aggregate_target(
        &self,
        function: AggregateFunction,
        target: &AggregateTarget,
    ) -> Result<String, CoreError> {
        match target {
            AggregateTarget::Property(property) => self.render_property_ref(property),
            AggregateTarget::PresenceGatedProperty {
                property,
                presence_variable,
            } => {
                let presence = self.render_binding_presence_ref(presence_variable)?;
                let property = self.render_property_ref(property)?;
                Ok(format!(
                    "CASE WHEN {presence} IS NULL THEN NULL ELSE {property} END"
                ))
            }
            AggregateTarget::Expression(expression) => self.render_scalar_expression(expression),
            AggregateTarget::VariableKey { variable } => {
                if function == AggregateFunction::Count {
                    self.render_binding_presence_ref(variable)
                } else {
                    self.render_binding_key_ref(variable)
                }
            }
            AggregateTarget::PresenceGatedVariableKey {
                variable,
                presence_variable,
            } => {
                let presence = self.render_binding_presence_ref(presence_variable)?;
                let key = self.render_binding_key_ref(variable)?;
                Ok(format!(
                    "CASE WHEN {presence} IS NULL THEN NULL ELSE {key} END"
                ))
            }
        }
    }

    fn render_aggregate_invocation(
        &self,
        function: AggregateFunction,
        target: &AggregateTarget,
        distinct: bool,
    ) -> Result<String, CoreError> {
        let target = self.render_aggregate_target(function, target)?;
        Ok(render_aggregate_invocation_sql(function, &target, distinct))
    }
}

#[path = "sql_tests.rs"]
#[cfg(test)]
mod tests;
