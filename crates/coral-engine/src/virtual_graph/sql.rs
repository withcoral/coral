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

    fn build_from_clause(&mut self) -> Result<(), CoreError> {
        let plan = self.validated.plan();
        let first_node = plan
            .nodes
            .first()
            .ok_or_else(|| CoreError::internal("validated graph plan had no nodes"))?;
        self.start_from_node(first_node.variable.as_str())?;

        self.join_mandatory_relationships()?;
        self.cross_join_isolated_nodes()?;
        self.ensure_optional_relationships_joined()?;

        Ok(())
    }

    fn join_precomputed_scalar_subqueries(&mut self) -> Result<(), CoreError> {
        let candidates = self.scalar_subquery_candidates();
        if candidates.is_empty() {
            return Ok(());
        }

        let mut unsupported = 0usize;
        for candidate_use in candidates {
            let candidate = candidate_use.candidate;
            let required = candidate_use.required;
            if self
                .precomputed_scalar_subqueries
                .iter()
                .any(|precomputed| precomputed.candidate == candidate)
            {
                continue;
            }
            let index = self.precomputed_scalar_subqueries.len();
            let precomputed = PrecomputedScalarSubquery {
                candidate,
                table_alias: format!("__coral_scalar_subquery_{index}"),
                outer_key_alias: "__coral_outer_key".to_string(),
                value_alias: "__coral_value".to_string(),
            };
            let Some(join_sql) = self.render_precomputed_scalar_subquery_join(&precomputed)? else {
                if required {
                    return Err(CoreError::InvalidInput(
                        "hidden ORDER BY over correlated scalar subqueries requires a precomputable single-anchor relationship or node pattern"
                            .to_string(),
                    ));
                }
                unsupported += 1;
                continue;
            };
            write!(self.from_clause, " {join_sql}")
                .map_err(|_| CoreError::internal("failed to render graph SQL"))?;
            self.precomputed_scalar_subqueries.push(precomputed);
        }

        if unsupported > 1 {
            return Err(CoreError::InvalidInput(
                "multiple correlated scalar subqueries in one projection require relationship-pattern COUNT { ... } / EXISTS { MATCH ... } subqueries with a single outer node anchor or node-only COUNT/EXISTS subqueries with one equality correlation"
                    .to_string(),
            ));
        }
        Ok(())
    }

    fn scalar_subquery_candidates(&self) -> Vec<ScalarSubqueryCandidateUse> {
        let mut candidates = Vec::new();
        for projection in &self.validated.plan().projections {
            if let Projection::Expression { expression, .. } = projection {
                self.collect_scalar_expression_subquery_candidates(
                    expression,
                    false,
                    &mut candidates,
                );
            }
        }
        if let Some(predicate) = &self.validated.plan().predicate {
            self.collect_predicate_expression_subquery_candidates(
                predicate,
                false,
                &mut candidates,
            );
        }
        for order_key in &self.validated.plan().order_by {
            if let OrderExpression::Scalar(expression) = &order_key.expression {
                self.collect_scalar_expression_subquery_candidates(
                    expression,
                    true,
                    &mut candidates,
                );
            }
        }
        candidates
    }

    fn collect_scalar_expression_subquery_candidates(
        &self,
        expression: &ScalarExpression,
        required: bool,
        candidates: &mut Vec<ScalarSubqueryCandidateUse>,
    ) {
        if let Some(expression) = scalar_expression_unary_operand(expression) {
            self.collect_scalar_expression_subquery_candidates(expression, required, candidates);
            return;
        }

        match expression {
            ScalarExpression::Predicate(predicate) => {
                self.collect_predicate_expression_subquery_candidates(
                    predicate, required, candidates,
                );
            }
            ScalarExpression::CountSubquery {
                pattern,
                distinct_target,
            } => Self::collect_count_subquery_candidate(
                pattern,
                distinct_target.as_deref(),
                required,
                candidates,
            ),
            ScalarExpression::CollectSubquery {
                pattern,
                target,
                distinct,
            } => {
                Self::collect_collect_subquery_candidate(
                    pattern, target, *distinct, required, candidates,
                );
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
            | ScalarExpression::Key { .. }
            | ScalarExpression::ElementId { .. }
            | ScalarExpression::GraphIdentity { .. }
            | ScalarExpression::GraphPresence { .. }
            | ScalarExpression::GraphKeyList { .. }
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
                unreachable!("unary scalar expressions handled before candidate collection")
            }
            _ => self.collect_structural_scalar_expression_subquery_candidates(
                expression, required, candidates,
            ),
        }
    }

    fn collect_count_subquery_candidate(
        pattern: &CountSubqueryPattern,
        distinct_target: Option<&ScalarExpression>,
        required: bool,
        candidates: &mut Vec<ScalarSubqueryCandidateUse>,
    ) {
        let should_precompute = match pattern {
            CountSubqueryPattern::Relationships(predicate) => {
                required || predicate.references_outer_variables()
            }
            CountSubqueryPattern::Nodes { .. } => required || pattern.references_outer_variables(),
        };
        if should_precompute {
            candidates.push(ScalarSubqueryCandidateUse {
                candidate: ScalarSubqueryCandidate::Count {
                    pattern: pattern.clone(),
                    distinct_target: distinct_target.cloned(),
                },
                required,
            });
        }
    }

    fn collect_collect_subquery_candidate(
        pattern: &CountSubqueryPattern,
        target: &ScalarExpression,
        distinct: bool,
        required: bool,
        candidates: &mut Vec<ScalarSubqueryCandidateUse>,
    ) {
        if let CountSubqueryPattern::Relationships(predicate) = pattern
            && predicate.references_outer_variables()
        {
            candidates.push(ScalarSubqueryCandidateUse {
                candidate: ScalarSubqueryCandidate::Collect {
                    pattern: predicate.clone(),
                    target: target.clone(),
                    distinct,
                },
                required,
            });
        }
    }

    fn collect_structural_scalar_expression_subquery_candidates(
        &self,
        expression: &ScalarExpression,
        required: bool,
        candidates: &mut Vec<ScalarSubqueryCandidateUse>,
    ) {
        if let Some((left, right)) = Self::structural_scalar_binary_operands(expression) {
            self.collect_scalar_expression_subquery_candidates(left, required, candidates);
            self.collect_scalar_expression_subquery_candidates(right, required, candidates);
            return;
        }

        match expression {
            ScalarExpression::PresenceGated { expression, .. } => {
                self.collect_scalar_expression_subquery_candidates(
                    expression, required, candidates,
                );
            }
            ScalarExpression::Coalesce { expressions } => {
                for expression in expressions {
                    self.collect_scalar_expression_subquery_candidates(
                        expression, required, candidates,
                    );
                }
            }
            ScalarExpression::Round { expression, places } => {
                self.collect_scalar_expression_subquery_candidates(
                    expression, required, candidates,
                );
                if let Some(places) = places {
                    self.collect_scalar_expression_subquery_candidates(
                        places, required, candidates,
                    );
                }
            }
            ScalarExpression::Replace {
                expression,
                search,
                replacement,
            } => {
                self.collect_scalar_expression_subquery_candidates(
                    expression, required, candidates,
                );
                self.collect_scalar_expression_subquery_candidates(search, required, candidates);
                self.collect_scalar_expression_subquery_candidates(
                    replacement,
                    required,
                    candidates,
                );
            }
            ScalarExpression::Substring {
                expression,
                start,
                length,
            } => {
                self.collect_scalar_expression_subquery_candidates(
                    expression, required, candidates,
                );
                self.collect_scalar_expression_subquery_candidates(start, required, candidates);
                if let Some(length) = length {
                    self.collect_scalar_expression_subquery_candidates(
                        length, required, candidates,
                    );
                }
            }
            ScalarExpression::Case {
                alternatives,
                else_expression,
            } => {
                self.collect_case_scalar_expression_subquery_candidates(
                    alternatives,
                    else_expression.as_deref(),
                    required,
                    candidates,
                );
            }
            _ => {
                unreachable!("unary scalar expressions handled before candidate collection")
            }
        }
    }

    fn structural_scalar_binary_operands(
        expression: &ScalarExpression,
    ) -> Option<(&ScalarExpression, &ScalarExpression)> {
        match expression {
            ScalarExpression::NullIf { expression, value } => Some((expression, value)),
            ScalarExpression::Left { expression, count }
            | ScalarExpression::Right { expression, count } => Some((expression, count)),
            ScalarExpression::StringContains {
                expression,
                pattern,
            }
            | ScalarExpression::StringStartsWith {
                expression,
                pattern,
            }
            | ScalarExpression::StringEndsWith {
                expression,
                pattern,
            } => Some((expression, pattern)),
            ScalarExpression::Arithmetic { left, right, .. } => Some((left, right)),
            ScalarExpression::Atan2 { y, x } => Some((y, x)),
            _ => None,
        }
    }

    fn collect_case_scalar_expression_subquery_candidates(
        &self,
        alternatives: &[ScalarCaseAlternative],
        else_expression: Option<&ScalarExpression>,
        required: bool,
        candidates: &mut Vec<ScalarSubqueryCandidateUse>,
    ) {
        for alternative in alternatives {
            self.collect_predicate_expression_subquery_candidates(
                &alternative.when,
                required,
                candidates,
            );
            self.collect_scalar_expression_subquery_candidates(
                &alternative.then,
                required,
                candidates,
            );
        }
        if let Some(else_expression) = else_expression {
            self.collect_scalar_expression_subquery_candidates(
                else_expression,
                required,
                candidates,
            );
        }
    }

    fn collect_predicate_expression_subquery_candidates(
        &self,
        predicate: &PredicateExpression,
        required: bool,
        candidates: &mut Vec<ScalarSubqueryCandidateUse>,
    ) {
        match predicate {
            PredicateExpression::ExistsPattern(predicate) => {
                if required || predicate.references_outer_variables() {
                    candidates.push(ScalarSubqueryCandidateUse {
                        candidate: ScalarSubqueryCandidate::Exists(predicate.clone()),
                        required,
                    });
                }
            }
            PredicateExpression::ScalarComparison(predicate) => {
                if !required && Self::scalar_predicate_renders_as_count_existence(predicate) {
                    return;
                }
                self.collect_scalar_expression_subquery_candidates(
                    &predicate.lhs,
                    required,
                    candidates,
                );
                if let ScalarPredicateRhs::Expression(expression) = &predicate.rhs {
                    self.collect_scalar_expression_subquery_candidates(
                        expression, required, candidates,
                    );
                }
            }
            PredicateExpression::And { left, right }
            | PredicateExpression::Or { left, right }
            | PredicateExpression::Xor { left, right } => {
                self.collect_predicate_expression_subquery_candidates(left, required, candidates);
                self.collect_predicate_expression_subquery_candidates(right, required, candidates);
            }
            PredicateExpression::Not { expression } => {
                self.collect_predicate_expression_subquery_candidates(
                    expression, required, candidates,
                );
            }
            PredicateExpression::Boolean(_)
            | PredicateExpression::Comparison(_)
            | PredicateExpression::KeyComparison(_)
            | PredicateExpression::ElementIdComparison(_)
            | PredicateExpression::Presence(_)
            | PredicateExpression::PropertyKeyMembership(_) => {}
        }
    }

    fn scalar_predicate_renders_as_count_existence(predicate: &ScalarPredicate) -> bool {
        let ScalarExpression::CountSubquery {
            distinct_target: None,
            ..
        } = &predicate.lhs
        else {
            return false;
        };
        Self::count_existence_predicate(predicate.operator, &predicate.rhs).is_some()
    }

    fn render_precomputed_scalar_subquery_join(
        &self,
        precomputed: &PrecomputedScalarSubquery,
    ) -> Result<Option<String>, CoreError> {
        match &precomputed.candidate {
            ScalarSubqueryCandidate::Count {
                pattern: CountSubqueryPattern::Relationships(predicate),
                ..
            } => {
                if predicate.references_outer_variables() {
                    self.render_precomputed_relationship_scalar_subquery_join(
                        predicate,
                        precomputed,
                    )
                } else {
                    self.render_precomputed_uncorrelated_relationship_scalar_subquery_join(
                        predicate,
                        precomputed,
                    )
                    .map(Some)
                }
            }
            ScalarSubqueryCandidate::Exists(predicate) => {
                if predicate.references_outer_variables() {
                    self.render_precomputed_exists_scalar_subquery_join(predicate, precomputed)
                } else {
                    self.render_precomputed_uncorrelated_relationship_scalar_subquery_join(
                        predicate,
                        precomputed,
                    )
                    .map(Some)
                }
            }
            ScalarSubqueryCandidate::Collect { pattern, .. } => {
                self.render_precomputed_relationship_scalar_subquery_join(pattern, precomputed)
            }
            ScalarSubqueryCandidate::Count {
                pattern: pattern @ CountSubqueryPattern::Nodes { .. },
                ..
            } => self.render_precomputed_node_count_scalar_subquery_join(pattern, precomputed),
        }
    }

    fn render_precomputed_uncorrelated_relationship_scalar_subquery_join(
        &self,
        predicate: &ExistsPatternPredicate,
        precomputed: &PrecomputedScalarSubquery,
    ) -> Result<String, CoreError> {
        let value_expression = match &precomputed.candidate {
            ScalarSubqueryCandidate::Exists(_) => "COUNT(*) > 0",
            ScalarSubqueryCandidate::Count {
                distinct_target: None,
                ..
            } => "COUNT(*)",
            ScalarSubqueryCandidate::Count {
                distinct_target: Some(target),
                ..
            } => {
                let value_expression =
                    self.render_count_distinct_scoped_pattern_select(predicate, target)?;
                let select_expression = format!(
                    "{value_expression} AS {}",
                    quote_ident(&precomputed.value_alias)
                );
                return Ok(format!(
                    "CROSS JOIN (SELECT {select_expression}) AS {}",
                    quote_ident(&precomputed.table_alias)
                ));
            }
            ScalarSubqueryCandidate::Collect { .. } => {
                return Err(CoreError::internal(
                    "uncorrelated collect subqueries are not precomputed",
                ));
            }
        };
        let select_expression = format!(
            "{value_expression} AS {}",
            quote_ident(&precomputed.value_alias)
        );
        Ok(format!(
            "CROSS JOIN {} AS {}",
            self.render_scoped_pattern_select(predicate, &select_expression)?,
            quote_ident(&precomputed.table_alias)
        ))
    }

    fn render_precomputed_node_count_scalar_subquery_join(
        &self,
        pattern: &CountSubqueryPattern,
        precomputed: &PrecomputedScalarSubquery,
    ) -> Result<Option<String>, CoreError> {
        let distinct_target = match &precomputed.candidate {
            ScalarSubqueryCandidate::Count {
                distinct_target, ..
            } => distinct_target.as_ref(),
            _ => None,
        };
        let CountSubqueryPattern::Nodes {
            nodes,
            predicates,
            predicate,
        } = pattern
        else {
            return Err(CoreError::internal(
                "precomputed node count renderer received a relationship pattern",
            ));
        };
        if pattern.references_outer_variables() {
            let local_aliases = Self::count_local_node_aliases(nodes);
            if let Some(target) = distinct_target {
                return self.render_precomputed_correlated_node_distinct_count_subquery_join(
                    nodes,
                    predicates,
                    predicate.as_deref(),
                    &local_aliases,
                    precomputed,
                    target,
                );
            }
            return self.render_precomputed_correlated_node_scalar_subquery_join(
                nodes,
                predicates,
                predicate.as_deref(),
                &local_aliases,
                precomputed,
                "COUNT(*)",
            );
        }
        if let Some(target) = distinct_target {
            let value_expression = self.render_count_distinct_node_subquery(
                nodes,
                predicates,
                predicate.as_deref(),
                target,
            )?;
            let select_expression = format!(
                "{value_expression} AS {}",
                quote_ident(&precomputed.value_alias)
            );
            return Ok(Some(format!(
                "CROSS JOIN (SELECT {select_expression}) AS {}",
                quote_ident(&precomputed.table_alias)
            )));
        }
        let select_expression = format!("COUNT(*) AS {}", quote_ident(&precomputed.value_alias));
        Ok(Some(format!(
            "CROSS JOIN {} AS {}",
            self.render_count_node_select(
                nodes,
                predicates,
                predicate.as_deref(),
                &select_expression,
            )?,
            quote_ident(&precomputed.table_alias)
        )))
    }

    fn render_precomputed_exists_scalar_subquery_join(
        &self,
        predicate: &ExistsPatternPredicate,
        precomputed: &PrecomputedScalarSubquery,
    ) -> Result<Option<String>, CoreError> {
        let local_nodes = self.exists_local_node_map(predicate)?;
        if self
            .exists_relationship_bindings(predicate, &local_nodes)?
            .is_empty()
        {
            let local_aliases = Self::exists_local_node_aliases(predicate);
            return self.render_precomputed_correlated_node_scalar_subquery_join(
                &predicate.nodes,
                &predicate.predicates,
                predicate.predicate.as_deref(),
                &local_aliases,
                precomputed,
                "COUNT(*) > 0",
            );
        }
        self.render_precomputed_relationship_scalar_subquery_join(predicate, precomputed)
    }

    fn render_precomputed_correlated_node_distinct_count_subquery_join(
        &self,
        nodes: &[NodePattern],
        predicates: &[PropertyPredicate],
        predicate: Option<&PredicateExpression>,
        local_aliases: &BTreeMap<&str, String>,
        precomputed: &PrecomputedScalarSubquery,
        target: &ScalarExpression,
    ) -> Result<Option<String>, CoreError> {
        let local_nodes = self.scoped_local_node_map(nodes)?;
        let Some(correlation) =
            self.precomputed_node_correlation(predicates, predicate, &local_nodes, local_aliases)?
        else {
            return Ok(None);
        };
        let relationship_bindings = Vec::new();
        if !Self::scoped_scalar_expression_is_inner(target, &relationship_bindings, &local_nodes) {
            return Ok(None);
        }

        let mut conditions = Vec::with_capacity(
            predicates
                .len()
                .saturating_sub(1)
                .saturating_add(usize::from(predicate.is_some())),
        );
        for (index, property_predicate) in predicates.iter().enumerate() {
            if index == correlation.predicate_index {
                continue;
            }
            conditions.push(self.render_exists_property_predicate(
                property_predicate,
                &relationship_bindings,
                &local_nodes,
                local_aliases,
            )?);
        }
        if let Some(predicate) = predicate {
            conditions.push(self.render_scoped_predicate_expression(
                predicate,
                &relationship_bindings,
                &local_nodes,
                local_aliases,
            )?);
        }
        let target_sql = self.render_scoped_scalar_expression(
            target,
            &relationship_bindings,
            &local_nodes,
            local_aliases,
        )?;
        let from_clause =
            Self::render_precomputed_node_from_clause(nodes, &local_nodes, local_aliases)?;
        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conditions.join(" AND "))
        };
        let outer_key_alias = quote_ident(&precomputed.outer_key_alias);
        let value_alias = quote_ident(&precomputed.value_alias);
        let distinct_alias = quote_ident("__coral_count_distinct");
        let distinct_value_alias = quote_ident("__coral_count_value");
        let distinct_rows = format!(
            "SELECT DISTINCT {} AS {outer_key_alias}, {target_sql} AS {distinct_value_alias} FROM {from_clause}{where_clause}",
            correlation.local_expression
        );
        let subquery = format!(
            "SELECT {outer_key_alias}, COUNT(*) AS {value_alias} FROM ({distinct_rows}) AS {distinct_alias} GROUP BY {outer_key_alias}"
        );
        Ok(Some(format!(
            "LEFT JOIN ({subquery}) AS {} ON {}.{} = {}",
            quote_ident(&precomputed.table_alias),
            quote_ident(&precomputed.table_alias),
            outer_key_alias,
            correlation.outer_expression
        )))
    }

    fn render_precomputed_correlated_node_scalar_subquery_join(
        &self,
        nodes: &[NodePattern],
        predicates: &[PropertyPredicate],
        predicate: Option<&PredicateExpression>,
        local_aliases: &BTreeMap<&str, String>,
        precomputed: &PrecomputedScalarSubquery,
        value_expression: &str,
    ) -> Result<Option<String>, CoreError> {
        let local_nodes = self.scoped_local_node_map(nodes)?;
        let Some(correlation) =
            self.precomputed_node_correlation(predicates, predicate, &local_nodes, local_aliases)?
        else {
            return Ok(None);
        };

        let relationship_bindings = Vec::new();
        let mut conditions = Vec::with_capacity(
            predicates
                .len()
                .saturating_sub(1)
                .saturating_add(usize::from(predicate.is_some())),
        );
        for (index, property_predicate) in predicates.iter().enumerate() {
            if index == correlation.predicate_index {
                continue;
            }
            conditions.push(self.render_exists_property_predicate(
                property_predicate,
                &relationship_bindings,
                &local_nodes,
                local_aliases,
            )?);
        }
        if let Some(predicate) = predicate {
            conditions.push(self.render_scoped_predicate_expression(
                predicate,
                &relationship_bindings,
                &local_nodes,
                local_aliases,
            )?);
        }
        let from_clause =
            Self::render_precomputed_node_from_clause(nodes, &local_nodes, local_aliases)?;
        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conditions.join(" AND "))
        };
        let subquery = format!(
            "SELECT {} AS {}, {value_expression} AS {} FROM {from_clause}{where_clause} GROUP BY {}",
            correlation.local_expression,
            quote_ident(&precomputed.outer_key_alias),
            quote_ident(&precomputed.value_alias),
            correlation.local_expression
        );
        Ok(Some(format!(
            "LEFT JOIN ({subquery}) AS {} ON {}.{} = {}",
            quote_ident(&precomputed.table_alias),
            quote_ident(&precomputed.table_alias),
            quote_ident(&precomputed.outer_key_alias),
            correlation.outer_expression
        )))
    }

    fn precomputed_node_correlation<'b>(
        &self,
        predicates: &[PropertyPredicate],
        predicate: Option<&PredicateExpression>,
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<Option<PrecomputedNodeCorrelation>, CoreError> {
        let relationship_bindings = Vec::new();
        if predicate.is_some_and(|predicate| {
            !Self::scoped_predicate_expression_is_inner(
                predicate,
                &relationship_bindings,
                local_nodes,
            )
        }) {
            return Ok(None);
        }

        let mut correlation = None;
        for (index, property_predicate) in predicates.iter().enumerate() {
            if let Some(candidate) = self.precomputed_node_property_correlation(
                index,
                property_predicate,
                local_nodes,
                local_aliases,
            )? {
                if correlation.is_some() {
                    return Ok(None);
                }
                correlation = Some(candidate);
                continue;
            }
            if !Self::scoped_property_predicate_is_inner(
                property_predicate,
                &relationship_bindings,
                local_nodes,
            ) {
                return Ok(None);
            }
        }
        Ok(correlation)
    }

    fn precomputed_node_property_correlation<'b>(
        &self,
        predicate_index: usize,
        predicate: &PropertyPredicate,
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<Option<PrecomputedNodeCorrelation>, CoreError> {
        if predicate.operator != ComparisonOperator::Equal {
            return Ok(None);
        }
        let lhs = self.node_property_correlation_operand(
            &predicate.property,
            local_nodes,
            local_aliases,
        )?;
        let Some(rhs) =
            self.node_rhs_correlation_operand(&predicate.rhs, local_nodes, local_aliases)?
        else {
            return Ok(None);
        };
        Ok(match (lhs, rhs) {
            (
                NodeCorrelationOperand::Local(local_expression),
                NodeCorrelationOperand::Outer(outer_expression),
            )
            | (
                NodeCorrelationOperand::Outer(outer_expression),
                NodeCorrelationOperand::Local(local_expression),
            ) => Some(PrecomputedNodeCorrelation {
                predicate_index,
                local_expression,
                outer_expression,
            }),
            _ => None,
        })
    }

    fn node_rhs_correlation_operand<'b>(
        &self,
        rhs: &PredicateRhs,
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<Option<NodeCorrelationOperand>, CoreError> {
        match rhs {
            PredicateRhs::Property(property) => self
                .node_property_correlation_operand(property, local_nodes, local_aliases)
                .map(Some),
            PredicateRhs::Key { variable } => self
                .node_key_correlation_operand(variable, local_nodes, local_aliases)
                .map(Some),
            PredicateRhs::ElementId { variable } => self
                .node_element_id_correlation_operand(variable, local_nodes, local_aliases)
                .map(Some),
            PredicateRhs::Literal(_) | PredicateRhs::List(_) => Ok(None),
        }
    }

    fn node_property_correlation_operand<'b>(
        &self,
        property: &PropertyRef,
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<NodeCorrelationOperand, CoreError> {
        if local_nodes.contains_key(property.variable.as_str()) {
            let relationship_bindings = Vec::new();
            return Ok(NodeCorrelationOperand::Local(
                self.render_exists_property_ref(
                    property,
                    &relationship_bindings,
                    local_nodes,
                    local_aliases,
                )?,
            ));
        }
        let binding = self.validated.binding(&property.variable)?;
        if !matches!(binding.kind(), ValidatedBindingKind::Node(_)) {
            return Err(CoreError::InvalidInput(
                "hidden ORDER BY node precompute supports correlations to one outer node binding"
                    .to_string(),
            ));
        }
        Ok(NodeCorrelationOperand::Outer(
            self.render_property_ref(property)?,
        ))
    }

    fn node_key_correlation_operand<'b>(
        &self,
        variable: &str,
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<NodeCorrelationOperand, CoreError> {
        if local_nodes.contains_key(variable) {
            let relationship_bindings = Vec::new();
            return Ok(NodeCorrelationOperand::Local(self.render_exists_key_ref(
                variable,
                &relationship_bindings,
                local_nodes,
                local_aliases,
            )?));
        }
        let binding = self.validated.binding(variable)?;
        if !matches!(binding.kind(), ValidatedBindingKind::Node(_)) {
            return Err(CoreError::InvalidInput(
                "hidden ORDER BY node precompute supports correlations to one outer node binding"
                    .to_string(),
            ));
        }
        Ok(NodeCorrelationOperand::Outer(
            self.render_binding_key_ref(variable)?,
        ))
    }

    fn node_element_id_correlation_operand<'b>(
        &self,
        variable: &str,
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<NodeCorrelationOperand, CoreError> {
        let operand = self.node_key_correlation_operand(variable, local_nodes, local_aliases)?;
        Ok(match operand {
            NodeCorrelationOperand::Local(expression) => {
                NodeCorrelationOperand::Local(format!("CAST({expression} AS VARCHAR)"))
            }
            NodeCorrelationOperand::Outer(expression) => {
                NodeCorrelationOperand::Outer(format!("CAST({expression} AS VARCHAR)"))
            }
        })
    }

    fn render_precomputed_node_from_clause<'b>(
        nodes: &'b [NodePattern],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        let mut from_clause = String::new();
        for (index, node) in nodes.iter().enumerate() {
            let node_mapping = local_nodes.get(node.variable.as_str()).ok_or_else(|| {
                CoreError::internal("validated precomputed node local mapping was missing")
            })?;
            let alias = local_aliases.get(node.variable.as_str()).ok_or_else(|| {
                CoreError::internal("validated precomputed node local alias was missing")
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
            .map_err(|_| CoreError::internal("failed to render precomputed node SQL"))?;
            if index > 0 {
                from_clause.push_str(" ON TRUE");
            }
        }
        Ok(from_clause)
    }

    #[expect(
        clippy::too_many_lines,
        reason = "Precomputed scalar subquery joins carry scoped bindings, predicates, and aggregate rendering in one SQL shape"
    )]
    fn render_precomputed_relationship_scalar_subquery_join(
        &self,
        predicate: &ExistsPatternPredicate,
        precomputed: &PrecomputedScalarSubquery,
    ) -> Result<Option<String>, CoreError> {
        let local_nodes = self.exists_local_node_map(predicate)?;
        let Some(outer_variable) = self.precomputed_outer_anchor(predicate, &local_nodes)? else {
            return Ok(None);
        };
        let relationship_bindings = self.exists_relationship_bindings(predicate, &local_nodes)?;
        if relationship_bindings.is_empty()
            || !Self::scoped_predicates_are_precomputable(
                predicate,
                &relationship_bindings,
                &local_nodes,
            )
        {
            return Ok(None);
        }
        let local_aliases = Self::exists_local_node_aliases(predicate);
        let collect_target_sql = match &precomputed.candidate {
            ScalarSubqueryCandidate::Collect { target, .. } => {
                if !Self::scoped_scalar_expression_is_inner(
                    target,
                    &relationship_bindings,
                    &local_nodes,
                ) {
                    return Ok(None);
                }
                Some(self.render_scoped_scalar_expression(
                    target,
                    &relationship_bindings,
                    &local_nodes,
                    &local_aliases,
                )?)
            }
            _ => None,
        };
        let Some((outer_key_ref, mut conditions)) = self
            .render_precomputed_relationship_conditions(
                &relationship_bindings,
                &local_nodes,
                &local_aliases,
                &outer_variable,
            )?
        else {
            return Ok(None);
        };
        conditions.extend(self.render_scoped_conditions(
            &predicate.predicates,
            predicate.predicate.as_deref(),
            &relationship_bindings,
            &local_nodes,
            &local_aliases,
        )?);
        let from_clause = Self::render_precomputed_relationship_from_clause(
            predicate,
            &relationship_bindings,
            &local_nodes,
            &local_aliases,
        )?;
        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conditions.join(" AND "))
        };
        let value_expression = match &precomputed.candidate {
            ScalarSubqueryCandidate::Exists(_) => "COUNT(*) > 0".to_string(),
            ScalarSubqueryCandidate::Count {
                distinct_target: None,
                ..
            } => "COUNT(*)".to_string(),
            ScalarSubqueryCandidate::Count {
                distinct_target: Some(target),
                ..
            } => {
                let Some(target_sql) = self.render_precomputed_relationship_distinct_count_target(
                    target,
                    &relationship_bindings,
                    &local_nodes,
                    &local_aliases,
                )?
                else {
                    return Ok(None);
                };
                return self
                    .render_precomputed_relationship_distinct_count_join(
                        precomputed,
                        &outer_variable,
                        &outer_key_ref,
                        &from_clause,
                        &where_clause,
                        &target_sql,
                    )
                    .map(Some);
            }
            ScalarSubqueryCandidate::Collect { distinct, .. } => {
                let target_sql = collect_target_sql.as_deref().ok_or_else(|| {
                    CoreError::internal("precomputed collect target SQL was not rendered")
                })?;
                Self::render_collect_target_select_expression(target_sql, *distinct)
            }
        };
        let subquery = format!(
            "SELECT {outer_key_ref} AS {}, {value_expression} AS {} FROM {from_clause}{where_clause} GROUP BY {outer_key_ref}",
            quote_ident(&precomputed.outer_key_alias),
            quote_ident(&precomputed.value_alias)
        );
        Ok(Some(format!(
            "LEFT JOIN ({subquery}) AS {} ON {}.{} = {}",
            quote_ident(&precomputed.table_alias),
            quote_ident(&precomputed.table_alias),
            quote_ident(&precomputed.outer_key_alias),
            self.render_binding_key_ref(&outer_variable)?
        )))
    }

    fn render_precomputed_relationship_distinct_count_target<'b>(
        &self,
        target: &ScalarExpression,
        relationship_bindings: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<Option<String>, CoreError> {
        if !Self::scoped_scalar_expression_is_inner(target, relationship_bindings, local_nodes) {
            return Ok(None);
        }
        self.render_scoped_scalar_expression(
            target,
            relationship_bindings,
            local_nodes,
            local_aliases,
        )
        .map(Some)
    }

    fn render_precomputed_relationship_distinct_count_join(
        &self,
        precomputed: &PrecomputedScalarSubquery,
        outer_variable: &str,
        outer_key_ref: &str,
        from_clause: &str,
        where_clause: &str,
        target_sql: &str,
    ) -> Result<String, CoreError> {
        let outer_key_alias = quote_ident(&precomputed.outer_key_alias);
        let value_alias = quote_ident(&precomputed.value_alias);
        let distinct_alias = quote_ident("__coral_count_distinct");
        let distinct_value_alias = quote_ident("__coral_count_value");
        let distinct_rows = format!(
            "SELECT DISTINCT {outer_key_ref} AS {outer_key_alias}, {target_sql} AS {distinct_value_alias} FROM {from_clause}{where_clause}"
        );
        let subquery = format!(
            "SELECT {outer_key_alias}, COUNT(*) AS {value_alias} FROM ({distinct_rows}) AS {distinct_alias} GROUP BY {outer_key_alias}"
        );
        Ok(format!(
            "LEFT JOIN ({subquery}) AS {} ON {}.{} = {}",
            quote_ident(&precomputed.table_alias),
            quote_ident(&precomputed.table_alias),
            outer_key_alias,
            self.render_binding_key_ref(outer_variable)?
        ))
    }

    fn precomputed_outer_anchor<'b>(
        &self,
        predicate: &'b ExistsPatternPredicate,
        local_nodes: &BTreeMap<&'b str, &'a Node>,
    ) -> Result<Option<String>, CoreError> {
        let mut outer_counts = BTreeMap::<&str, usize>::new();
        for relationship in &predicate.relationships {
            for variable in [relationship.left.as_str(), relationship.right.as_str()] {
                if !local_nodes.contains_key(variable) {
                    *outer_counts.entry(variable).or_default() += 1;
                }
            }
        }
        let mut outer_counts = outer_counts.iter();
        let Some((&outer_variable, &occurrence_count)) = outer_counts.next() else {
            return Ok(None);
        };
        if outer_counts.next().is_some() || occurrence_count != 1 {
            return Ok(None);
        }
        let binding = self.validated.binding(outer_variable)?;
        if !matches!(binding.kind(), ValidatedBindingKind::Node(_)) {
            return Ok(None);
        }
        Ok(Some(outer_variable.to_string()))
    }

    fn render_precomputed_relationship_from_clause<'b>(
        predicate: &'b ExistsPatternPredicate,
        relationship_bindings: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
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
                CoreError::internal("validated precomputed scalar local node mapping was missing")
            })?;
            let alias = local_aliases.get(node.variable.as_str()).ok_or_else(|| {
                CoreError::internal("validated precomputed scalar local node alias was missing")
            })?;
            write!(
                from_clause,
                " JOIN {} AS {} ON TRUE",
                render_table_ref(&node_mapping.table),
                quote_ident(alias)
            )
            .map_err(|_| CoreError::internal("failed to render precomputed scalar SQL"))?;
        }
        Ok(from_clause)
    }

    fn render_precomputed_relationship_conditions<'b>(
        &self,
        relationship_bindings: &[ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
        outer_variable: &str,
    ) -> Result<Option<(String, Vec<String>)>, CoreError> {
        let mut outer_key_ref = None;
        let mut conditions = Vec::with_capacity(relationship_bindings.len());
        for binding in relationship_bindings {
            let left_is_outer = binding.pattern.left == outer_variable;
            let right_is_outer = binding.pattern.right == outer_variable;
            if left_is_outer && right_is_outer {
                return Ok(None);
            }
            if !left_is_outer && !right_is_outer {
                conditions.push(self.exists_relationship_condition(
                    binding.pattern,
                    binding.relationship,
                    &binding.alias,
                    local_nodes,
                    local_aliases,
                )?);
                continue;
            }

            let left_node = self.exists_node_mapping(local_nodes, &binding.pattern.left)?;
            let right_node = self.exists_node_mapping(local_nodes, &binding.pattern.right)?;
            let orientations = Self::relationship_orientations_for_labels(
                binding.relationship,
                binding.pattern.direction,
                &left_node.label,
                &right_node.label,
            )?;
            let inner_variable = if left_is_outer {
                binding.pattern.right.as_str()
            } else {
                binding.pattern.left.as_str()
            };
            if !local_nodes.contains_key(inner_variable) {
                return Ok(None);
            }
            let inner_node = local_nodes.get(inner_variable).ok_or_else(|| {
                CoreError::internal("validated precomputed scalar local node mapping was missing")
            })?;
            let inner_alias = local_aliases.get(inner_variable).ok_or_else(|| {
                CoreError::internal("validated precomputed scalar local node alias was missing")
            })?;
            let Some((current_outer_key_ref, condition)) =
                Self::precomputed_outer_key_and_inner_condition(
                    &binding.alias,
                    &orientations,
                    left_is_outer,
                    inner_alias,
                    &inner_node.key,
                )?
            else {
                return Ok(None);
            };
            outer_key_ref = Some(current_outer_key_ref);
            conditions.push(condition);
        }
        Ok(outer_key_ref.map(|outer_key_ref| (outer_key_ref, conditions)))
    }

    fn precomputed_outer_key_and_inner_condition(
        relationship_alias: &str,
        orientations: &[RelationshipOrientation],
        left_is_outer: bool,
        inner_alias: &str,
        inner_key: &str,
    ) -> Result<Option<(String, String)>, CoreError> {
        if orientations.is_empty() {
            return Ok(None);
        }

        let mut branches = Vec::with_capacity(orientations.len());
        let mut conditions = Vec::with_capacity(orientations.len());
        for orientation in orientations {
            let (outer_relationship_key, inner_relationship_key) = if left_is_outer {
                (
                    orientation.left_relationship_key.as_str(),
                    orientation.right_relationship_key.as_str(),
                )
            } else {
                (
                    orientation.right_relationship_key.as_str(),
                    orientation.left_relationship_key.as_str(),
                )
            };
            let outer_ref = format!(
                "{}.{}",
                quote_ident(relationship_alias),
                quote_ident(outer_relationship_key)
            );
            let inner_condition = format!(
                "{}.{} = {}.{}",
                quote_ident(relationship_alias),
                quote_ident(inner_relationship_key),
                quote_ident(inner_alias),
                quote_ident(inner_key)
            );
            branches.push((inner_condition.clone(), outer_ref));
            conditions.push(inner_condition);
        }

        let outer_key_ref = if let [(condition, outer_ref)] = branches.as_slice() {
            let _ = condition;
            outer_ref.clone()
        } else {
            let when_clauses = branches
                .iter()
                .map(|(condition, outer_ref)| format!("WHEN {condition} THEN {outer_ref}"))
                .collect::<Vec<_>>()
                .join(" ");
            format!("CASE {when_clauses} ELSE NULL END")
        };
        Ok(Some((
            outer_key_ref,
            Self::render_condition_disjunction(&conditions)?,
        )))
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
            ScalarExpression::Replace {
                expression,
                search,
                replacement,
            } => {
                Self::scoped_scalar_pair_is_inner(
                    expression,
                    search,
                    relationship_bindings,
                    local_nodes,
                ) && Self::scoped_scalar_expression_is_inner(
                    replacement,
                    relationship_bindings,
                    local_nodes,
                )
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
        let scoped_relationships = self.optional_match_scope_relationships();
        let optional_match_nodes =
            self.validated
                .plan()
                .optional_matches
                .iter()
                .flat_map(|optional_match| {
                    optional_match
                        .node_indices
                        .iter()
                        .copied()
                        .filter_map(|index| self.validated.plan().nodes.get(index))
                        .map(|node| node.variable.as_str())
                });
        let unscoped_optional_relationship_nodes = self
            .validated
            .plan()
            .optional_relationships
            .iter()
            .filter(|index| !scoped_relationships.contains(index))
            .filter_map(|index| self.validated.plan().relationships.get(*index))
            .flat_map(|relationship| [relationship.left.as_str(), relationship.right.as_str()]);
        let optional_nodes = optional_match_nodes
            .chain(unscoped_optional_relationship_nodes)
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
        let optional_nodes = self.optional_relationship_node_variables();
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

    fn optional_relationship_node_variables(&self) -> BTreeSet<&'a str> {
        self.validated
            .plan()
            .optional_relationships
            .iter()
            .filter_map(|index| self.validated.plan().relationships.get(*index))
            .flat_map(|relationship| [relationship.left.as_str(), relationship.right.as_str()])
            .collect()
    }

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
                    let anchor = self.optional_relationship_component_anchor(index)?;
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
        let scoped_relationships = self.optional_match_scope_relationships();
        self.join_optional_match_scopes()?;

        let mut remaining_relationships = self
            .validated
            .plan()
            .optional_relationships
            .iter()
            .copied()
            .filter(|index| !scoped_relationships.contains(index))
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

    fn optional_match_scope_relationships(&self) -> BTreeSet<usize> {
        self.validated
            .plan()
            .optional_matches
            .iter()
            .flat_map(|optional_match| optional_match.relationship_indices.iter().copied())
            .collect()
    }

    fn join_optional_match_scopes(&mut self) -> Result<(), CoreError> {
        let mut remaining_scopes =
            (0..self.validated.plan().optional_matches.len()).collect::<BTreeSet<_>>();
        while !remaining_scopes.is_empty() {
            let mut progressed = false;
            for index in remaining_scopes.iter().copied().collect::<Vec<_>>() {
                let optional_match = self
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
                .validated
                .plan()
                .optional_matches
                .get(index)
                .ok_or_else(|| CoreError::internal("optional match scope index missing"))?;
            let anchor = self.optional_match_scope_component_anchor(optional_match)?;
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

        let relationship = self.validated.relationship_mapping(*relationship_index)?;
        let optional_predicate = self.render_optional_match_predicate(optional_match)?;
        Self::join_relationship(
            &self.validated,
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

    fn optional_match_scope_join_anchor(
        &self,
        optional_match: &OptionalMatchScope,
    ) -> Result<Option<OptionalScopeAnchor<'a>>, CoreError> {
        let mut anchor = None;
        for relationship_index in optional_match.relationship_indices.iter().copied() {
            let pattern = self
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
        let (mut join_group, outer_condition) =
            self.render_optional_match_group_anchor(anchor, &mut inner_joined_nodes)?;
        let mut outer_conditions = vec![outer_condition];
        remaining_relationships.remove(&anchor.relationship_index);

        while !remaining_relationships.is_empty() {
            let mut progressed = false;
            for relationship_index in remaining_relationships.iter().copied().collect::<Vec<_>>() {
                let pattern = self
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

        let optional_predicate = self.render_optional_match_predicate(optional_match)?;
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
        let outer_condition =
            Self::join_condition_with_predicate(outer_condition, optional_predicate.as_deref());
        write!(
            self.from_clause,
            " LEFT JOIN ({join_group}) ON {outer_condition}"
        )
        .map_err(|_| CoreError::internal("failed to render graph SQL"))?;

        for relationship_index in optional_match.relationship_indices.iter().copied() {
            let pattern = self
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

    fn render_optional_match_group_relationship(
        &self,
        join_group: &mut String,
        inner_joined_nodes: &mut BTreeSet<&'a str>,
        relationship_index: usize,
        left_joined: bool,
        right_joined: bool,
    ) -> Result<Option<String>, CoreError> {
        let pattern = self
            .validated
            .plan()
            .relationships
            .get(relationship_index)
            .ok_or_else(|| CoreError::internal("validated relationship index was out of bounds"))?;
        let relationship = self.validated.relationship_mapping(relationship_index)?;
        let relationship_alias = self
            .validated
            .relationship_alias(relationship_index, pattern);
        if left_joined && right_joined {
            let condition = Self::relationship_pair_condition(
                &self.validated,
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
        let relationship_join = Self::relationship_known_node_condition(
            &self.validated,
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
            let outer_join = Self::relationship_known_node_condition(
                &self.validated,
                relationship,
                pattern,
                &relationship_alias,
                unknown_variable,
                !known_is_left,
            )?;
            return Self::relationship_outer_condition_for_known_node(
                &self.validated,
                relationship,
                pattern,
                &relationship_alias,
                outer_join,
            )
            .map(Some);
        }

        let unknown_join = Self::relationship_inner_unknown_condition_for_known_node(
            &self.validated,
            relationship,
            pattern,
            &relationship_alias,
            unknown_variable,
            !known_is_left,
        )?;
        let unknown_node = self.validated.node_binding(unknown_variable)?;
        let unknown_alias = self.validated.binding(unknown_variable)?.alias();
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

    fn node_position(&self, variable: &str) -> Result<usize, CoreError> {
        self.validated
            .plan()
            .nodes
            .iter()
            .position(|node| node.variable == variable)
            .ok_or_else(|| CoreError::internal("validated node variable was missing"))
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

    fn relationship_matches_labels(
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

    fn relationship_orientations_for_labels(
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

    fn next_scalar_subquery_alias(&self, prefix: &str) -> String {
        let index = self.next_scalar_subquery_alias.get();
        self.next_scalar_subquery_alias.set(index + 1);
        quote_ident(&format!("{prefix}_{index}"))
    }

    fn render_precomputed_count_subquery_ref(
        &self,
        pattern: &CountSubqueryPattern,
        distinct_target: Option<&ScalarExpression>,
    ) -> Option<String> {
        self.precomputed_scalar_subqueries
            .iter()
            .find(|precomputed| {
                precomputed.candidate
                    == ScalarSubqueryCandidate::Count {
                        pattern: pattern.clone(),
                        distinct_target: distinct_target.cloned(),
                    }
            })
            .map(Self::render_precomputed_count_ref)
    }

    fn render_precomputed_exists_pattern_ref(
        &self,
        predicate: &ExistsPatternPredicate,
    ) -> Option<String> {
        self.precomputed_scalar_subqueries
            .iter()
            .find(|precomputed| {
                precomputed.candidate == ScalarSubqueryCandidate::Exists(predicate.clone())
            })
            .map(Self::render_precomputed_exists_ref)
    }

    fn render_precomputed_collect_subquery_ref(
        &self,
        pattern: &ExistsPatternPredicate,
        target: &ScalarExpression,
        distinct: bool,
    ) -> Option<String> {
        self.precomputed_scalar_subqueries
            .iter()
            .find(|precomputed| {
                precomputed.candidate
                    == ScalarSubqueryCandidate::Collect {
                        pattern: pattern.clone(),
                        target: target.clone(),
                        distinct,
                    }
            })
            .map(Self::render_precomputed_collect_ref)
    }

    fn render_precomputed_count_ref(precomputed: &PrecomputedScalarSubquery) -> String {
        format!(
            "COALESCE({}.{}, 0)",
            quote_ident(&precomputed.table_alias),
            quote_ident(&precomputed.value_alias)
        )
    }

    fn render_precomputed_exists_ref(precomputed: &PrecomputedScalarSubquery) -> String {
        format!(
            "COALESCE({}.{}, FALSE)",
            quote_ident(&precomputed.table_alias),
            quote_ident(&precomputed.value_alias)
        )
    }

    fn render_precomputed_collect_ref(precomputed: &PrecomputedScalarSubquery) -> String {
        format!(
            "COALESCE({}.{}, make_array())",
            quote_ident(&precomputed.table_alias),
            quote_ident(&precomputed.value_alias)
        )
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
        match expression {
            ScalarExpression::PresenceGated { expression, .. } => {
                self.reject_unprecomputed_projection_scalar_subqueries(expression)?;
            }
            ScalarExpression::Coalesce { expressions } => {
                for expression in expressions {
                    self.reject_unprecomputed_projection_scalar_subqueries(expression)?;
                }
            }
            ScalarExpression::NullIf { expression, value } => {
                self.reject_unprecomputed_projection_scalar_subqueries(expression)?;
                self.reject_unprecomputed_projection_scalar_subqueries(value)?;
            }
            ScalarExpression::Round { expression, places } => {
                self.reject_unprecomputed_projection_scalar_subqueries(expression)?;
                if let Some(places) = places {
                    self.reject_unprecomputed_projection_scalar_subqueries(places)?;
                }
            }
            ScalarExpression::Left { expression, count }
            | ScalarExpression::Right { expression, count } => {
                self.reject_unprecomputed_projection_scalar_subqueries(expression)?;
                self.reject_unprecomputed_projection_scalar_subqueries(count)?;
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
                self.reject_unprecomputed_projection_scalar_subqueries(expression)?;
                self.reject_unprecomputed_projection_scalar_subqueries(operand)?;
            }
            ScalarExpression::Replace {
                expression,
                search,
                replacement,
            } => {
                self.reject_unprecomputed_projection_scalar_subqueries(expression)?;
                self.reject_unprecomputed_projection_scalar_subqueries(search)?;
                self.reject_unprecomputed_projection_scalar_subqueries(replacement)?;
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
            ScalarExpression::Arithmetic { left, right, .. } => {
                self.reject_unprecomputed_projection_scalar_subqueries(left)?;
                self.reject_unprecomputed_projection_scalar_subqueries(right)?;
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
            ScalarExpression::Atan2 { y, x } => {
                self.reject_unprecomputed_projection_scalar_subqueries(y)?;
                self.reject_unprecomputed_projection_scalar_subqueries(x)?;
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

    fn exists_local_node_map<'b>(
        &self,
        predicate: &'b ExistsPatternPredicate,
    ) -> Result<BTreeMap<&'b str, &'a Node>, CoreError> {
        self.scoped_local_node_map(&predicate.nodes)
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

    fn exists_local_node_aliases(predicate: &ExistsPatternPredicate) -> BTreeMap<&str, String> {
        predicate
            .nodes
            .iter()
            .enumerate()
            .map(|(index, node)| (node.variable.as_str(), format!("__coral_exists_n{index}")))
            .collect()
    }

    fn count_local_node_aliases(nodes: &[NodePattern]) -> BTreeMap<&str, String> {
        nodes
            .iter()
            .enumerate()
            .map(|(index, node)| (node.variable.as_str(), format!("__coral_count_n{index}")))
            .collect()
    }

    fn exists_relationship_bindings<'b>(
        &self,
        predicate: &'b ExistsPatternPredicate,
        local_nodes: &BTreeMap<&'b str, &'a Node>,
    ) -> Result<Vec<ExistsRelationshipSqlBinding<'a, 'b>>, CoreError> {
        predicate
            .relationships
            .iter()
            .enumerate()
            .map(|(index, pattern)| {
                self.exists_relationship_mapping(pattern, local_nodes)
                    .map(|relationship| ExistsRelationshipSqlBinding {
                        pattern,
                        relationship,
                        alias: Self::exists_relationship_alias(index),
                    })
            })
            .collect()
    }

    fn exists_relationship_alias(index: usize) -> String {
        format!("__coral_exists_r{index}")
    }

    fn exists_relationship_mapping<'b>(
        &self,
        pattern: &'b RelationshipPattern,
        local_nodes: &BTreeMap<&'b str, &'a Node>,
    ) -> Result<&'a Relationship, CoreError> {
        let left_node = self.exists_node_mapping(local_nodes, &pattern.left)?;
        let right_node = self.exists_node_mapping(local_nodes, &pattern.right)?;
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
                "validated EXISTS relationship mapping was not resolvable",
            )),
            _ => Err(CoreError::internal(
                "validated EXISTS relationship mapping was ambiguous",
            )),
        }
    }

    fn exists_node_mapping<'b>(
        &self,
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        variable: &str,
    ) -> Result<&'a Node, CoreError> {
        if let Some(node) = local_nodes.get(variable).copied() {
            return Ok(node);
        }
        let binding = self.validated.binding(variable)?;
        let ValidatedBindingKind::Node(node) = binding.kind() else {
            return Err(CoreError::internal(
                "validated EXISTS endpoint was not a node binding",
            ));
        };
        Ok(*node)
    }

    fn exists_relationship_condition<'b>(
        &self,
        pattern: &'b RelationshipPattern,
        relationship: &Relationship,
        relationship_alias: &str,
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        let left_node = self.exists_node_mapping(local_nodes, &pattern.left)?;
        let right_node = self.exists_node_mapping(local_nodes, &pattern.right)?;
        let orientations = Self::relationship_orientations_for_labels(
            relationship,
            pattern.direction,
            &left_node.label,
            &right_node.label,
        )?;
        let has_multiple_orientations = orientations.len() > 1;
        let conditions = orientations
            .iter()
            .map(|orientation| {
                let left_ref =
                    self.exists_node_key_ref(&pattern.left, left_node, local_nodes, local_aliases)?;
                let right_ref = self.exists_node_key_ref(
                    &pattern.right,
                    right_node,
                    local_nodes,
                    local_aliases,
                )?;
                let condition = format!(
                    "{}.{} = {} AND {}.{} = {}",
                    quote_ident(relationship_alias),
                    quote_ident(&orientation.left_relationship_key),
                    left_ref,
                    quote_ident(relationship_alias),
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

    fn exists_node_key_ref<'b>(
        &self,
        variable: &str,
        node: &Node,
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        if local_nodes.contains_key(variable) {
            let alias = local_aliases
                .get(variable)
                .ok_or_else(|| CoreError::internal("validated EXISTS node alias was missing"))?;
            return Ok(format!("{}.{}", quote_ident(alias), quote_ident(&node.key)));
        }
        self.render_binding_key_ref(variable)
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
            } => self.render_aggregate_invocation(*function, target, *distinct),
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

    fn render_graph_key_list_ref(&self, variables: &[String]) -> Result<String, CoreError> {
        let values = variables
            .iter()
            .map(|variable| self.render_binding_key_ref(variable))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(render_sql_array(&values))
    }

    fn render_binding_element_id_ref(&self, variable: &str) -> Result<String, CoreError> {
        Ok(format!(
            "CAST({} AS VARCHAR)",
            self.render_binding_key_ref(variable)?
        ))
    }

    fn render_binding_graph_identity_ref(&self, variable: &str) -> Result<String, CoreError> {
        let binding = self.validated.binding(variable)?;
        let prefix = match binding.kind() {
            ValidatedBindingKind::Node(node) => format!("node:{}:", node.label),
            ValidatedBindingKind::Relationship(relationship) => {
                format!("relationship:{}:", relationship.relationship_type)
            }
        };
        let key = self.render_binding_key_ref(variable)?;
        Ok(format!(
            "CASE WHEN {key} IS NULL THEN NULL ELSE concat({}, CAST({key} AS VARCHAR)) END",
            render_literal(&Literal::String(prefix))
        ))
    }

    fn render_binding_graph_presence_ref(&self, variable: &str) -> Result<String, CoreError> {
        Ok(format!(
            "CAST({} AS VARCHAR)",
            self.render_binding_presence_ref(variable)?
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

    fn render_undirected_endpoint_property_ref(
        &self,
        relationship_variable: &str,
        endpoint: UndirectedRelationshipEndpoint,
        property: &str,
    ) -> Result<String, CoreError> {
        let selection =
            self.render_undirected_endpoint_selection(relationship_variable, endpoint)?;
        let left_property = self.render_property_ref(&PropertyRef {
            variable: selection.left_variable,
            property: property.to_string(),
        })?;
        let right_property = self.render_property_ref(&PropertyRef {
            variable: selection.right_variable,
            property: property.to_string(),
        })?;
        let presence = selection.presence;
        let left_matches_endpoint = selection.left_matches_endpoint;
        Ok(format!(
            "CASE WHEN {presence} IS NULL THEN NULL ELSE CASE WHEN {left_matches_endpoint} THEN {left_property} ELSE {right_property} END END"
        ))
    }

    fn render_undirected_endpoint_key_ref(
        &self,
        relationship_variable: &str,
        endpoint: UndirectedRelationshipEndpoint,
    ) -> Result<String, CoreError> {
        let selection =
            self.render_undirected_endpoint_selection(relationship_variable, endpoint)?;
        let presence = selection.presence;
        let left_matches_endpoint = selection.left_matches_endpoint;
        let left_key = selection.left_key;
        let right_key = selection.right_key;
        Ok(format!(
            "CASE WHEN {presence} IS NULL THEN NULL ELSE CASE WHEN {left_matches_endpoint} THEN {left_key} ELSE {right_key} END END"
        ))
    }

    fn render_undirected_endpoint_element_id_ref(
        &self,
        relationship_variable: &str,
        endpoint: UndirectedRelationshipEndpoint,
    ) -> Result<String, CoreError> {
        Ok(format!(
            "CAST({} AS VARCHAR)",
            self.render_undirected_endpoint_key_ref(relationship_variable, endpoint)?
        ))
    }

    fn render_undirected_endpoint_labels_ref(
        &self,
        relationship_variable: &str,
        label: &str,
    ) -> Result<String, CoreError> {
        let presence = self.render_relationship_presence_ref(relationship_variable)?;
        Ok(format!(
            "CASE WHEN {presence} IS NULL THEN NULL ELSE make_array({}) END",
            quote_string_literal(label)
        ))
    }

    fn render_undirected_endpoint_property_keys_ref(
        &self,
        relationship_variable: &str,
    ) -> Result<String, CoreError> {
        let (_, relationship_pattern) =
            self.relationship_pattern_for_variable(relationship_variable)?;
        let binding = self.validated.binding(&relationship_pattern.left)?;
        let ValidatedBindingKind::Node(node) = binding.kind() else {
            return Err(CoreError::internal(
                "validated undirected endpoint keys did not reference a node",
            ));
        };
        let property_names = node
            .properties
            .keys()
            .map(|property| quote_string_literal(property))
            .collect::<Vec<_>>()
            .join(", ");
        let presence = self.render_relationship_presence_ref(relationship_variable)?;
        Ok(format!(
            "CASE WHEN {presence} IS NULL THEN NULL ELSE make_array({property_names}) END"
        ))
    }

    fn relationship_pattern_for_variable(
        &self,
        relationship_variable: &str,
    ) -> Result<(usize, &RelationshipPattern), CoreError> {
        self.validated
            .plan()
            .relationships
            .iter()
            .enumerate()
            .find(|(_, relationship)| {
                relationship.variable.as_deref() == Some(relationship_variable)
            })
            .ok_or_else(|| {
                CoreError::internal("validated undirected endpoint referenced unknown relationship")
            })
    }

    fn render_undirected_endpoint_selection(
        &self,
        relationship_variable: &str,
        endpoint: UndirectedRelationshipEndpoint,
    ) -> Result<UndirectedEndpointSelection, CoreError> {
        let (relationship_index, relationship_pattern) =
            self.relationship_pattern_for_variable(relationship_variable)?;
        let relationship = self.validated.relationship_mapping(relationship_index)?;
        let relationship_alias = self
            .validated
            .relationship_alias(relationship_index, relationship_pattern);
        let endpoint_column = match endpoint {
            UndirectedRelationshipEndpoint::Start => &relationship.from.key,
            UndirectedRelationshipEndpoint::End => &relationship.to.key,
        };
        let selector = format!(
            "{}.{}",
            quote_ident(&relationship_alias),
            quote_ident(endpoint_column)
        );
        let presence = self.render_relationship_presence_ref(relationship_variable)?;
        let left_key = self.render_binding_key_ref(&relationship_pattern.left)?;
        let right_key = self.render_binding_key_ref(&relationship_pattern.right)?;
        Ok(UndirectedEndpointSelection {
            presence,
            left_matches_endpoint: format!("{left_key} = {selector}"),
            left_key,
            right_key,
            left_variable: relationship_pattern.left.clone(),
            right_variable: relationship_pattern.right.clone(),
        })
    }

    fn render_scalar_expression(&self, expression: &ScalarExpression) -> Result<String, CoreError> {
        if let Some(rendered) = self.render_simple_scalar_expression(expression)? {
            return Ok(rendered);
        }
        if let Some(rendered) = self.render_graph_metadata_scalar_expression(expression)? {
            return Ok(rendered);
        }

        self.render_structural_scalar_expression(expression)
    }

    fn render_structural_scalar_expression(
        &self,
        expression: &ScalarExpression,
    ) -> Result<String, CoreError> {
        match expression {
            ScalarExpression::Property(property) => self.render_property_ref(property),
            ScalarExpression::Literal(literal) => Ok(render_literal(literal)),
            ScalarExpression::LiteralList { literals } => Ok(render_literal_list(literals)),
            ScalarExpression::TypedLiteralList {
                literals,
                element_type,
            } => Ok(render_typed_literal_list(literals, *element_type)),
            ScalarExpression::Predicate(predicate) => {
                self.render_scalar_predicate_expression(predicate)
            }
            ScalarExpression::CountSubquery {
                pattern,
                distinct_target,
            } => self.render_count_subquery_expression(pattern, distinct_target.as_deref()),
            ScalarExpression::CollectSubquery {
                pattern,
                target,
                distinct,
            } => self.render_collect_subquery_expression(pattern, target, *distinct),
            ScalarExpression::PresenceGated {
                presence_variable,
                expression,
            } => self.render_presence_gated_scalar_expression(presence_variable, expression),
            ScalarExpression::Coalesce { expressions } => {
                self.render_coalesce_expression(expressions)
            }
            ScalarExpression::NullIf { expression, value } => Ok(format!(
                "NULLIF({}, {})",
                self.render_scalar_expression(expression)?,
                self.render_scalar_expression(value)?
            )),
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
            _ => unreachable!("scalar expression handled above"),
        }
    }

    fn render_coalesce_expression(
        &self,
        expressions: &[ScalarExpression],
    ) -> Result<String, CoreError> {
        let rendered = expressions
            .iter()
            .map(|expression| self.render_scalar_expression(expression))
            .collect::<Result<Vec<_>, _>>()?
            .join(", ");
        Ok(format!("COALESCE({rendered})"))
    }

    fn render_graph_metadata_scalar_expression(
        &self,
        expression: &ScalarExpression,
    ) -> Result<Option<String>, CoreError> {
        match expression {
            ScalarExpression::Key { variable } => self.render_binding_key_ref(variable).map(Some),
            ScalarExpression::GraphKeyList { variables } => {
                self.render_graph_key_list_ref(variables).map(Some)
            }
            ScalarExpression::ElementId { variable } => {
                self.render_binding_element_id_ref(variable).map(Some)
            }
            ScalarExpression::GraphIdentity { variable } => {
                self.render_binding_graph_identity_ref(variable).map(Some)
            }
            ScalarExpression::GraphPresence { variable } => {
                self.render_binding_graph_presence_ref(variable).map(Some)
            }
            ScalarExpression::NodeLabels { variable, label } => {
                self.render_node_labels_ref(variable, label).map(Some)
            }
            ScalarExpression::PropertyKeys { variable } => {
                self.render_property_keys_ref(variable).map(Some)
            }
            ScalarExpression::UndirectedEndpointProperty {
                relationship,
                endpoint,
                property,
            } => self
                .render_undirected_endpoint_property_ref(relationship, *endpoint, property)
                .map(Some),
            ScalarExpression::UndirectedEndpointKey {
                relationship,
                endpoint,
            } => self
                .render_undirected_endpoint_key_ref(relationship, *endpoint)
                .map(Some),
            ScalarExpression::UndirectedEndpointElementId {
                relationship,
                endpoint,
            } => self
                .render_undirected_endpoint_element_id_ref(relationship, *endpoint)
                .map(Some),
            ScalarExpression::UndirectedEndpointLabels {
                relationship,
                label,
                ..
            } => self
                .render_undirected_endpoint_labels_ref(relationship, label)
                .map(Some),
            ScalarExpression::UndirectedEndpointPropertyKeys { relationship, .. } => self
                .render_undirected_endpoint_property_keys_ref(relationship)
                .map(Some),
            ScalarExpression::RelationshipType {
                variable,
                relationship_type,
            } => self
                .render_relationship_type_ref(variable, relationship_type)
                .map(Some),
            _ => Ok(None),
        }
    }

    fn render_simple_scalar_expression(
        &self,
        expression: &ScalarExpression,
    ) -> Result<Option<String>, CoreError> {
        if let Some(rendered) = self.render_scalar_cast_expression(expression)? {
            return Ok(Some(rendered));
        }
        if let Some((function_name, expression, pattern)) =
            Self::string_predicate_function_expression(expression)
        {
            return self
                .render_binary_function_expression(function_name, expression, pattern)
                .map(Some);
        }
        if let Some((function_name, expression)) = Self::unary_sql_function_expression(expression) {
            return self
                .render_unary_function_expression(function_name, expression)
                .map(Some);
        }

        match expression {
            ScalarExpression::Left { expression, count } => self
                .render_binary_function_expression("left", expression, count)
                .map(Some),
            ScalarExpression::Right { expression, count } => self
                .render_binary_function_expression("right", expression, count)
                .map(Some),
            ScalarExpression::Atan2 { y, x } => self
                .render_binary_function_expression("atan2", y, x)
                .map(Some),
            ScalarExpression::Negate { expression } => Ok(Some(format!(
                "-({})",
                self.render_scalar_expression(expression)?
            ))),
            _ => Ok(None),
        }
    }

    fn unary_sql_function_expression(
        expression: &ScalarExpression,
    ) -> Option<(&'static str, &ScalarExpression)> {
        match expression {
            ScalarExpression::ToLower { expression } => Some(("LOWER", expression)),
            ScalarExpression::ToUpper { expression } => Some(("UPPER", expression)),
            ScalarExpression::Trim { expression } => Some(("TRIM", expression)),
            ScalarExpression::LTrim { expression } => Some(("LTRIM", expression)),
            ScalarExpression::RTrim { expression } => Some(("RTRIM", expression)),
            ScalarExpression::CharacterLength { expression } => {
                Some(("character_length", expression))
            }
            ScalarExpression::Reverse { expression } => Some(("reverse", expression)),
            ScalarExpression::Abs { expression } => Some(("abs", expression)),
            ScalarExpression::Ceil { expression } => Some(("ceil", expression)),
            ScalarExpression::Floor { expression } => Some(("floor", expression)),
            ScalarExpression::Sqrt { expression } => Some(("sqrt", expression)),
            ScalarExpression::Sign { expression } => Some(("signum", expression)),
            ScalarExpression::Exp { expression } => Some(("exp", expression)),
            ScalarExpression::Log { expression } => Some(("ln", expression)),
            ScalarExpression::Log10 { expression } => Some(("log10", expression)),
            ScalarExpression::Sin { expression } => Some(("sin", expression)),
            ScalarExpression::Cos { expression } => Some(("cos", expression)),
            ScalarExpression::Tan { expression } => Some(("tan", expression)),
            ScalarExpression::Cot { expression } => Some(("cot", expression)),
            ScalarExpression::Asin { expression } => Some(("asin", expression)),
            ScalarExpression::Acos { expression } => Some(("acos", expression)),
            ScalarExpression::Atan { expression } => Some(("atan", expression)),
            ScalarExpression::Degrees { expression } => Some(("degrees", expression)),
            ScalarExpression::Radians { expression } => Some(("radians", expression)),
            ScalarExpression::IsNaN { expression } => Some(("isnan", expression)),
            _ => None,
        }
    }

    fn string_predicate_function_expression(
        expression: &ScalarExpression,
    ) -> Option<(&'static str, &ScalarExpression, &ScalarExpression)> {
        match expression {
            ScalarExpression::StringContains {
                expression,
                pattern,
            } => Some(("contains", expression, pattern)),
            ScalarExpression::StringStartsWith {
                expression,
                pattern,
            } => Some(("starts_with", expression, pattern)),
            ScalarExpression::StringEndsWith {
                expression,
                pattern,
            } => Some(("ends_with", expression, pattern)),
            _ => None,
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
                self.render_scalar_predicate_expression(&alternative.when)?,
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

    fn render_presence_gated_scalar_expression(
        &self,
        presence_variable: &str,
        expression: &ScalarExpression,
    ) -> Result<String, CoreError> {
        let presence = self.render_binding_presence_ref(presence_variable)?;
        let expression = self.render_scalar_expression(expression)?;
        Ok(format!(
            "CASE WHEN {presence} IS NULL THEN NULL ELSE {expression} END"
        ))
    }
}

fn scalar_expression_unary_operand(expression: &ScalarExpression) -> Option<&ScalarExpression> {
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
        | ScalarExpression::IsNaN { expression }
        | ScalarExpression::Negate { expression } => Some(expression),
        _ => None,
    }
}

fn render_table_ref(table: &TableRef) -> String {
    format!(
        "{}.{}",
        quote_ident(&table.schema),
        quote_ident(&table.name)
    )
}

fn render_union_branch_sql(sql: &str, index: usize) -> String {
    format!(
        "SELECT * FROM ({sql}) AS {}",
        quote_ident(&format!("__coral_union_b{index}"))
    )
}

fn render_union_outer_sql(sql: String, union: &GraphUnion) -> Result<String, CoreError> {
    if union.outer_projection.is_none()
        && !union.distinct
        && union.order_by.is_empty()
        && union.skip.is_none()
        && union.limit.is_none()
    {
        return Ok(sql);
    }

    let distinct = if union.distinct { "DISTINCT " } else { "" };
    let projection = render_union_outer_projection(union);
    let mut outer_sql = format!(
        "SELECT {distinct}{projection} FROM ({sql}) AS {}",
        quote_ident("__coral_union_outer")
    );
    if let Some(outer_projection) = &union.outer_projection
        && !outer_projection.group_by.is_empty()
    {
        let groups = outer_projection
            .group_by
            .iter()
            .map(|column| quote_ident(column))
            .collect::<Vec<_>>()
            .join(", ");
        write!(outer_sql, " GROUP BY {groups}")
            .map_err(|_| CoreError::internal("failed to render graph union GROUP BY"))?;
    }
    if !union.order_by.is_empty() {
        let mut keys = Vec::with_capacity(union.order_by.len());
        for (index, key) in union.order_by.iter().enumerate() {
            let nulls = render_null_order(key.nulls);
            keys.push(format!(
                "{} {}{}",
                render_union_outer_order_expression(&key.expression, index)?,
                match key.direction {
                    OrderDirection::Ascending => "ASC",
                    OrderDirection::Descending => "DESC",
                },
                nulls,
            ));
        }
        write!(outer_sql, " ORDER BY {}", keys.join(", "))
            .map_err(|_| CoreError::internal("failed to render graph union SQL"))?;
    }
    if let Some(limit) = union.limit {
        write!(outer_sql, " LIMIT {limit}")
            .map_err(|_| CoreError::internal("failed to render graph union SQL"))?;
    }
    if let Some(skip) = union.skip {
        write!(outer_sql, " OFFSET {skip}")
            .map_err(|_| CoreError::internal("failed to render graph union SQL"))?;
    }
    Ok(outer_sql)
}

fn render_null_order(nulls: Option<NullOrder>) -> &'static str {
    match nulls {
        Some(NullOrder::First) => " NULLS FIRST",
        Some(NullOrder::Last) => " NULLS LAST",
        None => "",
    }
}

fn render_union_outer_projection(union: &GraphUnion) -> String {
    let Some(outer_projection) = &union.outer_projection else {
        return "*".to_string();
    };
    outer_projection
        .items
        .iter()
        .map(render_union_outer_projection_item)
        .collect::<Vec<_>>()
        .join(", ")
}

fn render_union_outer_projection_item(item: &GraphUnionOuterProjectionItem) -> String {
    match item {
        GraphUnionOuterProjectionItem::Column { name } => quote_ident(name),
        GraphUnionOuterProjectionItem::CountAll { alias } => {
            format!("COUNT(*) AS {}", quote_ident(alias))
        }
        GraphUnionOuterProjectionItem::Aggregate {
            function,
            source,
            distinct,
            alias,
        } => {
            let source = quote_ident(source);
            format!(
                "{} AS {}",
                render_aggregate_invocation_sql(*function, &source, *distinct),
                quote_ident(alias)
            )
        }
    }
}

fn render_union_outer_order_expression(
    expression: &OrderExpression,
    index: usize,
) -> Result<String, CoreError> {
    match expression {
        OrderExpression::ProjectionAlias(alias) => Ok(quote_ident(alias)),
        _ => Err(Diagnostic::new(
            "UNSUPPORTED_GRAPH_QUERY",
            format!("union.order_by[{index}].expression"),
            "graph union outer ORDER BY only supports projection aliases",
        )
        .into_core_error()),
    }
}

fn validate_union_branch_output_names(
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
        AggregateFunction::PercentileCont { .. } => "PERCENTILE_CONT",
        AggregateFunction::StdDev => "STDDEV_SAMP",
        AggregateFunction::StdDevP => "STDDEV_POP",
        AggregateFunction::Min => "MIN",
        AggregateFunction::Max => "MAX",
    }
}

fn render_aggregate_invocation_sql(
    function: AggregateFunction,
    target: &str,
    distinct: bool,
) -> String {
    let distinct_sql = if distinct { "DISTINCT " } else { "" };
    if let AggregateFunction::PercentileCont { percentile } = function {
        return format!(
            "PERCENTILE_CONT({distinct_sql}{target}, {})",
            render_float_literal(percentile.into_inner())
        );
    }
    if function == AggregateFunction::Collect {
        return format!(
            "COALESCE(ARRAY_AGG({distinct_sql}{target}) FILTER (WHERE ({target}) IS NOT NULL), make_array())"
        );
    }
    if distinct {
        match function {
            AggregateFunction::StdDev => {
                return format!("SQRT(VAR_SAMP(DISTINCT {target}))");
            }
            AggregateFunction::StdDevP => {
                return format!("SQRT(VAR_POP(DISTINCT {target}))");
            }
            _ => {}
        }
    }
    format!(
        "{}({distinct_sql}{target})",
        render_aggregate_function(function)
    )
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
        Literal::Float(value) => render_float_literal((*value).into_inner()),
        Literal::Boolean(value) => value.to_string(),
        Literal::Null => "NULL".to_string(),
    }
}

fn render_float_literal(value: f64) -> String {
    let rendered = value.to_string();
    if rendered.contains('.') || rendered.contains('e') || rendered.contains('E') {
        rendered
    } else {
        format!("{rendered}.0")
    }
}

fn render_order_literal(literal: &Literal) -> String {
    match literal {
        Literal::Integer(_) => format!("CAST({} AS BIGINT)", render_literal(literal)),
        _ => render_literal(literal),
    }
}

fn render_literal_list(literals: &[Literal]) -> String {
    let values = literals.iter().map(render_literal).collect::<Vec<_>>();
    render_sql_array(&values)
}

fn render_sql_array(values: &[String]) -> String {
    let values = values.join(", ");
    format!("make_array({values})")
}

fn render_typed_literal_list(literals: &[Literal], element_type: LiteralListElementType) -> String {
    if !literals.is_empty() {
        return render_literal_list(literals);
    }
    format!(
        "array_resize(make_array(CAST(NULL AS {})), 0)",
        render_literal_list_element_type(element_type)
    )
}

fn render_literal_list_element_type(element_type: LiteralListElementType) -> &'static str {
    match element_type {
        LiteralListElementType::String => "VARCHAR",
        LiteralListElementType::Integer => "BIGINT",
        LiteralListElementType::Float => "DOUBLE",
        LiteralListElementType::Boolean => "BOOLEAN",
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
        AggregateFunction, AggregateTarget, ComparisonOperator, CountSubqueryPattern, Direction,
        ExistsPatternPredicate, GraphPlan, KeyPredicate, Literal, NodePattern, OptionalMatchScope,
        OrderDirection, OrderExpression, OrderKey, PredicateExpression, PredicateRhs, Projection,
        ProjectionPredicate, ProjectionPredicateExpression, ProjectionPredicateRhs,
        PropertyPredicate, PropertyRef, RelationshipPattern, ScalarExpression, ScalarPredicate,
        ScalarPredicateRhs,
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
    fn lower_graph_plan_renders_optional_relationship_from_disconnected_component() {
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
                variable: None,
                relationship_type: "OWNS".to_string(),
                left: "person".to_string(),
                direction: Direction::Outgoing,
                right: "owned".to_string(),
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
                    alias: Some("person".to_string()),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "owned".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("owned".to_string()),
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
            .expect("optional relationship from disconnected component should lower");

        assert_eq!(
            translation.sql(),
            "SELECT \"n0\".\"service_name\" AS \"service\", \"n1\".\"full_name\" AS \"person\", \"n2\".\"service_name\" AS \"owned\" \
             FROM \"ops\".\"services\" AS \"n0\" \
             CROSS JOIN \"ops\".\"people\" AS \"n1\" \
             LEFT JOIN \"ops\".\"ownerships\" AS \"r0\" ON \"r0\".\"person_id\" = \"n1\".\"id\" \
             LEFT JOIN \"ops\".\"services\" AS \"n2\" ON \"r0\".\"service_id\" = \"n2\".\"id\""
        );
    }

    #[test]
    fn lower_graph_plan_renders_multihop_optional_scope_as_grouped_left_join() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let plan = GraphPlan {
            nodes: vec![
                NodePattern {
                    variable: "service".to_string(),
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
                    left: "service".to_string(),
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
                node_indices: vec![1, 2],
                relationship_indices: vec![0, 1],
                predicate: None,
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
            .expect("multi-hop optional scope should lower");

        assert_eq!(
            translation.sql(),
            "SELECT \"n0\".\"service_name\" AS \"service\", \"n1\".\"service_name\" AS \"middle\", \"n2\".\"service_name\" AS \"target\" \
             FROM \"ops\".\"services\" AS \"n0\" \
             LEFT JOIN (\"ops\".\"service_dependencies\" AS \"r0\" \
             JOIN \"ops\".\"services\" AS \"n1\" ON \"r0\".\"to_service_id\" = \"n1\".\"id\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r1\" ON \"r1\".\"from_service_id\" = \"n1\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n2\" ON \"r1\".\"to_service_id\" = \"n2\".\"id\") \
             ON \"r0\".\"from_service_id\" = \"n0\".\"id\""
        );
    }

    #[test]
    fn lower_graph_plan_renders_multihop_optional_scope_between_bound_endpoints_as_grouped_left_join()
     {
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
                    variable: "middle".to_string(),
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
                node_indices: vec![2],
                relationship_indices: vec![0, 1],
                predicate: None,
            }],
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
                        variable: "middle".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("middle".to_string()),
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
            .expect("bound-endpoint multi-hop optional scope should lower");

        assert_eq!(
            translation.sql(),
            "SELECT \"n0\".\"service_name\" AS \"source\", \"n1\".\"service_name\" AS \"target\", \"n2\".\"service_name\" AS \"middle\" \
             FROM \"ops\".\"services\" AS \"n0\" \
             CROSS JOIN \"ops\".\"services\" AS \"n1\" \
             LEFT JOIN (\"ops\".\"service_dependencies\" AS \"r0\" \
             JOIN \"ops\".\"services\" AS \"n2\" ON \"r0\".\"to_service_id\" = \"n2\".\"id\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r1\" ON \"r1\".\"from_service_id\" = \"n2\".\"id\") \
             ON (\"r0\".\"from_service_id\" = \"n0\".\"id\") AND (\"r1\".\"to_service_id\" = \"n1\".\"id\")"
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
                node_indices: vec![1],
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
                node_indices: vec![1],
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
            nulls: None,
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
            nulls: None,
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
            nulls: None,
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
            nulls: None,
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
    fn lower_graph_plan_renders_identity_scalar_expressions() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.relationships
            .first_mut()
            .expect("ownership plan should have a relationship")
            .variable = Some("owns".to_string());
        plan.predicates.clear();
        plan.projections = vec![
            Projection::Expression {
                expression: ScalarExpression::Arithmetic {
                    operator: ArithmeticOperator::Add,
                    left: Box::new(ScalarExpression::Key {
                        variable: "service".to_string(),
                    }),
                    right: Box::new(ScalarExpression::Literal(Literal::Integer(1))),
                },
                alias: "next_service_id".to_string(),
            },
            Projection::Expression {
                expression: ScalarExpression::Coalesce {
                    expressions: vec![
                        ScalarExpression::ElementId {
                            variable: "owns".to_string(),
                        },
                        ScalarExpression::Literal(Literal::String("missing".to_string())),
                    ],
                },
                alias: "ownership_element_id".to_string(),
            },
        ];
        plan.predicate = Some(PredicateExpression::ScalarComparison(ScalarPredicate {
            lhs: ScalarExpression::ElementId {
                variable: "owns".to_string(),
            },
            operator: ComparisonOperator::StartsWith,
            rhs: ScalarPredicateRhs::Expression(ScalarExpression::Literal(Literal::String(
                "1".to_string(),
            ))),
        }));
        plan.order_by = vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::ToString {
                expression: Box::new(ScalarExpression::Key {
                    variable: "service".to_string(),
                }),
            }),
            direction: OrderDirection::Ascending,
            nulls: None,
        }];

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("identity scalar expressions should lower");

        assert!(
            translation.sql().contains(
                "SELECT (\"n1\".\"id\" + 1) AS \"next_service_id\", COALESCE(CAST(\"r0\".\"ownership_id\" AS VARCHAR), 'missing') AS \"ownership_element_id\""
            ),
            "{}",
            translation.sql()
        );
        assert!(
            translation
                .sql()
                .contains("WHERE CAST(\"r0\".\"ownership_id\" AS VARCHAR) LIKE '1%' ESCAPE '\\'"),
            "{}",
            translation.sql()
        );
        assert!(
            translation
                .sql()
                .contains("ORDER BY CAST(\"n1\".\"id\" AS VARCHAR) ASC"),
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
            nulls: None,
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
            nulls: None,
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
            nulls: None,
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
    fn lower_graph_plan_renders_string_predicate_function_expressions() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.predicates.clear();
        plan.projections = vec![
            expression_projection(
                "has_api",
                ScalarExpression::StringContains {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    })),
                    pattern: Box::new(ScalarExpression::Literal(Literal::String(
                        "api".to_string(),
                    ))),
                },
            ),
            expression_projection(
                "starts_bill",
                ScalarExpression::StringStartsWith {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    })),
                    pattern: Box::new(ScalarExpression::Literal(Literal::String(
                        "bill".to_string(),
                    ))),
                },
            ),
            expression_projection(
                "ends_api",
                ScalarExpression::StringEndsWith {
                    expression: Box::new(ScalarExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    })),
                    pattern: Box::new(ScalarExpression::Literal(Literal::String(
                        "api".to_string(),
                    ))),
                },
            ),
        ];
        plan.order_by = vec![OrderKey {
            expression: OrderExpression::Scalar(ScalarExpression::StringContains {
                expression: Box::new(ScalarExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                })),
                pattern: Box::new(ScalarExpression::Literal(Literal::String(
                    "api".to_string(),
                ))),
            }),
            direction: OrderDirection::Descending,
            nulls: None,
        }];

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("string predicate function expressions should lower");

        assert!(
            translation.sql().contains(
                "SELECT contains(\"n1\".\"service_name\", 'api') AS \"has_api\", starts_with(\"n1\".\"service_name\", 'bill') AS \"starts_bill\", ends_with(\"n1\".\"service_name\", 'api') AS \"ends_api\""
            ),
            "{}",
            translation.sql()
        );
        assert!(
            translation
                .sql()
                .contains("ORDER BY contains(\"n1\".\"service_name\", 'api') DESC"),
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
            nulls: None,
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
            nulls: None,
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
    fn render_literal_preserves_whole_float_type() {
        assert_eq!(
            render_literal(&Literal::Float(ordered_float::OrderedFloat(3.0))),
            "3.0"
        );
        assert_eq!(
            render_literal(&Literal::Float(ordered_float::OrderedFloat(0.5))),
            "0.5"
        );
    }

    #[test]
    fn lower_graph_plan_preserves_whole_float_literals_in_numeric_expressions() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.predicates.clear();
        plan.projections = vec![expression_projection(
            "risk_thirds",
            ScalarExpression::Round {
                expression: Box::new(ScalarExpression::Arithmetic {
                    operator: ArithmeticOperator::Divide,
                    left: Box::new(service_risk_expression()),
                    right: Box::new(float_literal(3.0)),
                }),
                places: None,
            },
        )];

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("whole float literals should lower as floats");

        assert!(
            translation
                .sql()
                .contains("round((\"n1\".\"risk_score\" / 3.0)) AS \"risk_thirds\""),
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
            nulls: None,
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
    fn lower_graph_plan_renders_is_nan_scalar_expressions() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.predicates.clear();
        plan.projections = vec![expression_projection(
            "risk_is_nan",
            ScalarExpression::IsNaN {
                expression: Box::new(service_risk_expression()),
            },
        )];

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("isNaN scalar expression should lower");

        assert!(
            translation
                .sql()
                .contains("isnan(\"n1\".\"risk_score\") AS \"risk_is_nan\""),
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
            "acos(1.0) AS \"one_acos\"",
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
            nulls: None,
        }];

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("angle conversion scalar expressions should lower");

        for expected in [
            "atan2(\"n1\".\"risk_score\", 1) AS \"risk_atan2\"",
            "degrees(\"n1\".\"risk_score\") AS \"risk_degrees\"",
            "radians(180.0) AS \"pi_radians\"",
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
            nulls: None,
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
            nulls: None,
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
    #[expect(
        clippy::too_many_lines,
        reason = "The test builds a complete graph plan fixture inline for SQL assertion readability"
    )]
    fn lower_graph_plan_renders_exists_pattern_predicates_as_correlated_subqueries() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = GraphPlan {
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
            predicates: Vec::new(),
            predicate: Some(PredicateExpression::ExistsPattern(ExistsPatternPredicate {
                nodes: vec![NodePattern {
                    variable: "dependency".to_string(),
                    label: "Service".to_string(),
                }],
                relationships: vec![RelationshipPattern {
                    variable: Some("dependency_edge".to_string()),
                    relationship_type: "DEPENDS_ON".to_string(),
                    left: "service".to_string(),
                    direction: Direction::Outgoing,
                    right: "dependency".to_string(),
                }],
                predicates: vec![
                    PropertyPredicate {
                        property: PropertyRef {
                            variable: "dependency".to_string(),
                            property: "tier".to_string(),
                        },
                        operator: ComparisonOperator::Equal,
                        rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
                    },
                    PropertyPredicate {
                        property: PropertyRef {
                            variable: "dependency_edge".to_string(),
                            property: "criticality".to_string(),
                        },
                        operator: ComparisonOperator::Equal,
                        rhs: PredicateRhs::Literal(Literal::String("runtime".to_string())),
                    },
                ],
                predicate: None,
            })),
            post_projection_predicate: None,
            order_by: Vec::new(),
            skip: None,
            limit: None,
        };

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("EXISTS predicate expression should lower");

        assert!(
            translation.sql().contains(
                "EXISTS (SELECT 1 FROM \"ops\".\"service_dependencies\" AS \"__coral_exists_r0\" \
                 JOIN \"ops\".\"services\" AS \"__coral_exists_n0\" ON TRUE WHERE"
            ),
            "{}",
            translation.sql()
        );
        assert!(
            translation
                .sql()
                .contains("\"__coral_exists_r0\".\"from_service_id\" = \"n0\".\"id\""),
            "{}",
            translation.sql()
        );
        assert!(
            translation
                .sql()
                .contains("\"__coral_exists_r0\".\"to_service_id\" = \"__coral_exists_n0\".\"id\""),
            "{}",
            translation.sql()
        );
        assert!(
            translation
                .sql()
                .contains("\"__coral_exists_n0\".\"tier\" = 'prod'"),
            "{}",
            translation.sql()
        );
        assert!(
            translation
                .sql()
                .contains("\"__coral_exists_r0\".\"criticality\" = 'runtime'"),
            "{}",
            translation.sql()
        );

        plan.predicate = Some(PredicateExpression::Not {
            expression: Box::new(plan.predicate.take().expect("predicate")),
        });
        let negated = graph
            .lower_graph_plan(&plan)
            .expect("negated EXISTS predicate expression should lower");
        assert!(negated.sql().contains("WHERE NOT (EXISTS (SELECT 1"));
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
            nulls: None,
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
            nulls: None,
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
    #[expect(
        clippy::too_many_lines,
        reason = "The test keeps the correlated node-count plan inline so the SQL shape under test is explicit"
    )]
    fn lower_graph_plan_precomputes_hidden_correlated_node_count_ordering() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = GraphPlan {
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
            predicates: Vec::new(),
            predicate: None,
            post_projection_predicate: None,
            order_by: vec![OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::CountSubquery {
                    pattern: Box::new(CountSubqueryPattern::Nodes {
                        nodes: vec![NodePattern {
                            variable: "other".to_string(),
                            label: "Service".to_string(),
                        }],
                        predicates: vec![
                            PropertyPredicate {
                                property: PropertyRef {
                                    variable: "other".to_string(),
                                    property: "tier".to_string(),
                                },
                                operator: ComparisonOperator::Equal,
                                rhs: PredicateRhs::Property(PropertyRef {
                                    variable: "service".to_string(),
                                    property: "tier".to_string(),
                                }),
                            },
                            PropertyPredicate {
                                property: PropertyRef {
                                    variable: "other".to_string(),
                                    property: "name".to_string(),
                                },
                                operator: ComparisonOperator::NotEqual,
                                rhs: PredicateRhs::Literal(Literal::String("legacy".to_string())),
                            },
                        ],
                        predicate: None,
                    }),
                    distinct_target: None,
                }),
                direction: OrderDirection::Descending,
                nulls: None,
            }],
            skip: None,
            limit: None,
        };

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("correlated node count ordering should lower");

        assert!(
            translation.sql().contains(
                "LEFT JOIN (SELECT \"__coral_count_n0\".\"tier\" AS \"__coral_outer_key\", \
                 COUNT(*) AS \"__coral_value\" FROM \"ops\".\"services\" AS \"__coral_count_n0\" \
                 WHERE \"__coral_count_n0\".\"service_name\" <> 'legacy' \
                 GROUP BY \"__coral_count_n0\".\"tier\") AS \"__coral_scalar_subquery_0\" \
                 ON \"__coral_scalar_subquery_0\".\"__coral_outer_key\" = \"n0\".\"tier\""
            ),
            "{}",
            translation.sql()
        );
        assert!(
            translation.sql().contains(
                "ORDER BY COALESCE(\"__coral_scalar_subquery_0\".\"__coral_value\", 0) DESC"
            ),
            "{}",
            translation.sql()
        );

        let order_expression = &mut plan.order_by.first_mut().expect("order key").expression;
        let CountSubqueryPattern::Nodes { predicates, .. } = (match order_expression {
            OrderExpression::Scalar(ScalarExpression::CountSubquery { pattern, .. }) => {
                pattern.as_mut()
            }
            _ => panic!("expected count subquery order expression"),
        }) else {
            panic!("expected node count subquery");
        };
        predicates.push(PropertyPredicate {
            property: PropertyRef {
                variable: "other".to_string(),
                property: "name".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Property(PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            }),
        });
        let error = graph
            .lower_graph_plan(&plan)
            .expect_err("multiple correlated node-count keys should remain rejected");
        assert!(
            error
                .to_string()
                .contains("requires a precomputable single-anchor relationship or node pattern"),
            "{error}"
        );
    }

    #[test]
    fn lower_graph_plan_precomputes_hidden_correlated_node_exists_ordering() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = GraphPlan {
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
            predicates: Vec::new(),
            predicate: None,
            post_projection_predicate: None,
            order_by: vec![OrderKey {
                expression: OrderExpression::Scalar(ScalarExpression::Predicate(Box::new(
                    PredicateExpression::ExistsPattern(ExistsPatternPredicate {
                        nodes: vec![NodePattern {
                            variable: "other".to_string(),
                            label: "Service".to_string(),
                        }],
                        relationships: Vec::new(),
                        predicates: vec![PropertyPredicate {
                            property: PropertyRef {
                                variable: "other".to_string(),
                                property: "tier".to_string(),
                            },
                            operator: ComparisonOperator::Equal,
                            rhs: PredicateRhs::Property(PropertyRef {
                                variable: "service".to_string(),
                                property: "tier".to_string(),
                            }),
                        }],
                        predicate: None,
                    }),
                ))),
                direction: OrderDirection::Descending,
                nulls: None,
            }],
            skip: None,
            limit: None,
        };

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("correlated node exists ordering should lower");

        assert!(
            translation.sql().contains(
                "LEFT JOIN (SELECT \"__coral_exists_n0\".\"tier\" AS \"__coral_outer_key\", \
                 COUNT(*) > 0 AS \"__coral_value\" FROM \"ops\".\"services\" AS \"__coral_exists_n0\" \
                 GROUP BY \"__coral_exists_n0\".\"tier\") AS \"__coral_scalar_subquery_0\" \
                 ON \"__coral_scalar_subquery_0\".\"__coral_outer_key\" = \"n0\".\"tier\""
            ),
            "{}",
            translation.sql()
        );
        assert!(
            translation.sql().contains(
                "ORDER BY COALESCE(\"__coral_scalar_subquery_0\".\"__coral_value\", FALSE) DESC"
            ),
            "{}",
            translation.sql()
        );

        let order_expression = &mut plan.order_by.first_mut().expect("order key").expression;
        let exists_predicate = match order_expression {
            OrderExpression::Scalar(ScalarExpression::Predicate(predicate)) => {
                match predicate.as_mut() {
                    PredicateExpression::ExistsPattern(predicate) => predicate,
                    _ => panic!("expected exists predicate order expression"),
                }
            }
            _ => panic!("expected exists predicate order expression"),
        };
        exists_predicate.predicates.push(PropertyPredicate {
            property: PropertyRef {
                variable: "other".to_string(),
                property: "name".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Property(PropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            }),
        });
        let error = graph
            .lower_graph_plan(&plan)
            .expect_err("multiple correlated node-exists keys should remain rejected");
        assert!(
            error
                .to_string()
                .contains("requires a precomputable single-anchor relationship or node pattern"),
            "{error}"
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
                .contains("COALESCE(ARRAY_AGG(DISTINCT \"n1\".\"service_name\") FILTER (WHERE (\"n1\".\"service_name\") IS NOT NULL), make_array()) AS \"services\""),
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
            function: AggregateFunction::PercentileCont {
                percentile: ordered_float::OrderedFloat(0.75),
            },
            target: AggregateTarget::Property(PropertyRef {
                variable: "service".to_string(),
                property: "risk".to_string(),
            }),
            distinct: false,
            alias: "p75_risk".to_string(),
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
        plan.projections.push(Projection::Aggregate {
            function: AggregateFunction::StdDev,
            target: AggregateTarget::Property(PropertyRef {
                variable: "service".to_string(),
                property: "risk".to_string(),
            }),
            distinct: true,
            alias: "distinct_sample_risk".to_string(),
        });
        plan.projections.push(Projection::Aggregate {
            function: AggregateFunction::StdDevP,
            target: AggregateTarget::Property(PropertyRef {
                variable: "service".to_string(),
                property: "risk".to_string(),
            }),
            distinct: true,
            alias: "distinct_population_risk".to_string(),
        });

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("statistical aggregate projections should lower");

        assert!(
            translation.sql().contains(
                "MEDIAN(\"n1\".\"risk_score\") AS \"median_risk\", \
                 PERCENTILE_CONT(\"n1\".\"risk_score\", 0.75) AS \"p75_risk\", \
                 STDDEV_SAMP(\"n1\".\"risk_score\") AS \"sample_risk\", \
                 STDDEV_POP(\"n1\".\"risk_score\") AS \"population_risk\", \
                 MEDIAN(DISTINCT \"n1\".\"risk_score\") AS \"distinct_median_risk\", \
                 SQRT(VAR_SAMP(DISTINCT \"n1\".\"risk_score\")) AS \"distinct_sample_risk\", \
                 SQRT(VAR_POP(DISTINCT \"n1\".\"risk_score\")) AS \"distinct_population_risk\""
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
            nulls: None,
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
                nulls: None,
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
