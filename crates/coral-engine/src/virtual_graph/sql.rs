//! SQL Lowerer hub: lowers a validated graph plan into `DataFusion` SQL. Owns
//! the `SqlTranslation` result, the `Declaration::lower_graph_plan` /
//! `lower_graph_query` / union entry points, and the stateful `Lowerer` that
//! assembles the SELECT/FROM/WHERE/GROUP BY/HAVING/ORDER BY/LIMIT/OFFSET clause
//! text. Drives the `sql/*` rendering submodules (`joins`, `metadata`,
//! `predicates`, `projection`, `render`, `scalar`, `scoped`, `subqueries`) and
//! consumes the `ValidatedGraphPlan` produced by the `GraphPlanValidator`.

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
mod predicates;
mod projection;
mod render;
mod scalar;
mod scoped;
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
}

#[path = "sql_tests.rs"]
#[cfg(test)]
mod tests;
