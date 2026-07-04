//! SQL `SqlRenderer` hub: lowers a validated graph plan into `DataFusion` SQL. Owns
//! the `SqlTranslation` result, the `Declaration::lower_graph_plan` /
//! `lower_graph_query` / union entry points, and the stateful `SqlRenderer` that
//! assembles the SELECT/FROM/WHERE/GROUP BY/HAVING/ORDER BY/LIMIT/OFFSET clause
//! text. Drives the `sql/*` rendering submodules (`joins`, `metadata`,
//! `predicates`, `projection`, `render`, `scalar`, `scoped`, `subqueries`) and
//! consumes the `ValidatedGraphPlan` produced by the `GraphPlanValidator`.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::fmt::Write as _;

use super::declaration::{Declaration, Node, Relationship, TableRef};
use super::diagnostic::Diagnostic;
use super::diagnostic_codes;
use super::ir::{
    AggregateFunction, AggregateTarget, ArithmeticOperator, ComparisonOperator,
    CountSubqueryPattern, Direction, ElementIdPredicate, ExistsPatternPredicate, GraphPlan,
    GraphQuery, GraphStagedQuery, GraphUnion, GraphUnionOuterProjectionItem, GraphUnwind,
    GraphUnwindPipeline, KeyPredicate, Literal, LiteralListElementType, NodePattern, NullOrder,
    OptionalMatchScope, OrderDirection, OrderExpression, PredicateExpression, PredicateRhs,
    PresencePredicate, Projection, ProjectionPredicate, ProjectionPredicateExpression,
    ProjectionPredicateRhs, PropertyKeyMembershipPredicate, PropertyPredicate, PropertyRef,
    RelationshipPattern, ScalarCaseAlternative, ScalarExpression, ScalarPredicate,
    ScalarPredicateRhs, TemporalComponentUnit, TemporalExpr, TemporalKind,
    UndirectedRelationshipEndpoint,
};
use super::validation::{
    ValidatedBindingKind, ValidatedGraphPlan, stage_column_bindings, unwind_stage_column_bindings,
};
use crate::{CatalogInfo, CoreError};

mod joins;
mod metadata;
mod predicates;
mod projection;
mod render;
mod scalar;
mod scoped;
mod subqueries;

use self::joins::FromClauseBuilder;

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
        SqlRenderer::new(validated).lower()
    }

    pub(crate) fn lower_graph_plan_against_catalog(
        &self,
        plan: &GraphPlan,
        catalog: &CatalogInfo,
    ) -> Result<SqlTranslation, CoreError> {
        let validated = self.validated_graph_plan_against_catalog(plan, catalog)?;
        SqlRenderer::new(validated).lower()
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
            GraphQuery::Unwind(unwind) => Self::lower_graph_unwind(unwind),
            GraphQuery::UnwindPipeline(pipeline) => self.lower_graph_unwind_pipeline(pipeline),
            GraphQuery::Staged(staged) => self.lower_graph_staged_query(staged),
            GraphQuery::Union(union) => self.lower_graph_union(union),
        }
    }

    fn lower_graph_unwind(unwind: &GraphUnwind) -> Result<SqlTranslation, CoreError> {
        Self::validate_graph_unwind(unwind)?;
        let [projection] = unwind.projections.as_slice() else {
            return Err(CoreError::internal(
                "UNWIND row source requires exactly one projection",
            ));
        };
        let super::ir::GraphUnwindProjection::Variable { alias } = projection;
        let input_alias = unwind.input.as_ref().map(|_| "__coral_unwind_input");
        let list_sql = render_unwind_list_expression(&unwind.list, input_alias)?;
        let input = unwind
            .input
            .as_ref()
            .map(|input| render_unwind_input(input, input_alias.unwrap_or("__coral_unwind_input")))
            .transpose()?
            .unwrap_or_default();
        Ok(SqlTranslation::new(
            format!("SELECT UNNEST({list_sql}) AS {}{input}", quote_ident(alias)),
            Vec::new(),
        ))
    }

    fn lower_graph_unwind_pipeline(
        &self,
        pipeline: &GraphUnwindPipeline,
    ) -> Result<SqlTranslation, CoreError> {
        Self::validate_graph_unwind(&pipeline.unwind)?;
        let unwind = Self::lower_graph_unwind(&pipeline.unwind)?;
        let stage_columns = unwind_stage_column_bindings(&pipeline.unwind, "stage0")?;
        let final_validated =
            self.validate_graph_plan_with_stage_columns(&pipeline.final_plan, stage_columns)?;
        let final_translation = SqlRenderer::new(final_validated).lower()?;
        let mut diagnostics = unwind.diagnostics().to_vec();
        diagnostics.extend(final_translation.diagnostics().iter().cloned());
        Ok(SqlTranslation::new(
            format!(
                "WITH {} AS ({}) {}",
                quote_ident("stage0"),
                unwind.sql(),
                final_translation.sql()
            ),
            diagnostics,
        ))
    }

    pub(crate) fn lower_graph_query_against_catalog(
        &self,
        query: &GraphQuery,
        catalog: &CatalogInfo,
    ) -> Result<SqlTranslation, CoreError> {
        self.validate_graph_query_against_catalog(query, catalog)?;
        match query {
            GraphQuery::Plan(plan) => self.lower_graph_plan_against_catalog(plan, catalog),
            GraphQuery::Staged(staged) => {
                self.lower_graph_staged_query_against_catalog(staged, catalog)
            }
            GraphQuery::Union(union) => self.lower_graph_union_against_catalog(union, catalog),
            GraphQuery::Unwind(unwind) => Self::lower_graph_unwind(unwind),
            GraphQuery::UnwindPipeline(pipeline) => {
                self.lower_graph_unwind_pipeline_against_catalog(pipeline, catalog)
            }
        }
    }

    fn lower_graph_unwind_pipeline_against_catalog(
        &self,
        pipeline: &GraphUnwindPipeline,
        catalog: &CatalogInfo,
    ) -> Result<SqlTranslation, CoreError> {
        Self::validate_graph_unwind(&pipeline.unwind)?;
        let unwind = Self::lower_graph_unwind(&pipeline.unwind)?;
        let stage_columns = unwind_stage_column_bindings(&pipeline.unwind, "stage0")?;
        let final_validated = self.validate_graph_plan_with_stage_columns_against_catalog(
            &pipeline.final_plan,
            stage_columns,
            catalog,
        )?;
        let final_translation = SqlRenderer::new(final_validated).lower()?;
        let mut diagnostics = unwind.diagnostics().to_vec();
        diagnostics.extend(final_translation.diagnostics().iter().cloned());
        Ok(SqlTranslation::new(
            format!(
                "WITH {} AS ({}) {}",
                quote_ident("stage0"),
                unwind.sql(),
                final_translation.sql()
            ),
            diagnostics,
        ))
    }

    fn lower_graph_staged_query(
        &self,
        staged: &GraphStagedQuery,
    ) -> Result<SqlTranslation, CoreError> {
        if staged.stages.is_empty() {
            return Err(CoreError::internal("staged graph query had no stages"));
        }

        let mut ctes = Vec::with_capacity(staged.stages.len());
        let mut diagnostics = Vec::new();
        for (index, stage) in staged.stages.iter().enumerate() {
            let translation = self.lower_graph_plan(&stage.plan)?;
            diagnostics.extend(translation.diagnostics().iter().cloned());
            ctes.push(format!(
                "{} AS ({})",
                quote_ident(&format!("stage{index}")),
                translation.sql()
            ));
        }

        let stage_columns = stage_column_bindings(self, staged)?;
        let final_validated =
            self.validate_graph_plan_with_stage_columns(&staged.final_plan, stage_columns)?;
        let final_translation = SqlRenderer::new(final_validated).lower()?;
        diagnostics.extend(final_translation.diagnostics().iter().cloned());
        Ok(SqlTranslation::new(
            format!("WITH {} {}", ctes.join(", "), final_translation.sql()),
            diagnostics,
        ))
    }

    fn lower_graph_staged_query_against_catalog(
        &self,
        staged: &GraphStagedQuery,
        catalog: &CatalogInfo,
    ) -> Result<SqlTranslation, CoreError> {
        if staged.stages.is_empty() {
            return Err(CoreError::internal("staged graph query had no stages"));
        }

        let mut ctes = Vec::with_capacity(staged.stages.len());
        let mut diagnostics = Vec::new();
        for (index, stage) in staged.stages.iter().enumerate() {
            let translation = self.lower_graph_plan_against_catalog(&stage.plan, catalog)?;
            diagnostics.extend(translation.diagnostics().iter().cloned());
            ctes.push(format!(
                "{} AS ({})",
                quote_ident(&format!("stage{index}")),
                translation.sql()
            ));
        }

        let stage_columns = self.stage_column_bindings_against_catalog(staged, catalog)?;
        let final_validated = self.validate_graph_plan_with_stage_columns_against_catalog(
            &staged.final_plan,
            stage_columns,
            catalog,
        )?;
        let final_translation = SqlRenderer::new(final_validated).lower()?;
        diagnostics.extend(final_translation.diagnostics().iter().cloned());
        Ok(SqlTranslation::new(
            format!("WITH {} {}", ctes.join(", "), final_translation.sql()),
            diagnostics,
        ))
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

    fn lower_graph_union_against_catalog(
        &self,
        union: &GraphUnion,
        catalog: &CatalogInfo,
    ) -> Result<SqlTranslation, CoreError> {
        if union.branches.is_empty() {
            return Err(CoreError::internal("graph union had no union branches"));
        }

        let expected_names = union.first.projection_output_names();
        let mut diagnostics = Vec::new();
        let first = self.lower_graph_plan_against_catalog(&union.first, catalog)?;
        diagnostics.extend(first.diagnostics().iter().cloned());
        let mut sql = render_union_branch_sql(first.sql(), 0);

        for (index, branch) in union.branches.iter().enumerate() {
            validate_union_branch_output_names(
                &expected_names,
                &branch.plan.projection_output_names(),
                index,
            )?;
            let translation = self.lower_graph_plan_against_catalog(&branch.plan, catalog)?;
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

fn render_unwind_input(
    input: &super::ir::GraphUnwindInput,
    input_alias: &str,
) -> Result<String, CoreError> {
    if input.projections.is_empty() {
        return Err(CoreError::internal("UNWIND input stage had no projections"));
    }
    let projections = input
        .projections
        .iter()
        .map(|projection| {
            let expression = render_unwind_list_expression(&projection.expression, None)?;
            Ok(format!(
                "{expression} AS {}",
                quote_ident(&projection.alias)
            ))
        })
        .collect::<Result<Vec<_>, CoreError>>()?;
    Ok(format!(
        " FROM (SELECT {}) AS {}",
        projections.join(", "),
        quote_ident(input_alias)
    ))
}

fn render_unwind_list_expression(
    expression: &ScalarExpression,
    input_alias: Option<&str>,
) -> Result<String, CoreError> {
    match expression {
        ScalarExpression::TypedLiteralList {
            literals,
            element_type,
        } => Ok(render_typed_literal_list(literals, *element_type)),
        ScalarExpression::StageValue { alias } => {
            let input_alias = input_alias
                .ok_or_else(|| CoreError::internal("UNWIND stage value requires an input alias"))?;
            Ok(format!(
                "{}.{}",
                quote_ident(input_alias),
                quote_ident(alias)
            ))
        }
        ScalarExpression::ListConcat { left, right } => Ok(format!(
            "array_concat({}, {})",
            render_unwind_list_expression(left, input_alias)?,
            render_unwind_list_expression(right, input_alias)?
        )),
        _ => Err(CoreError::internal(
            "UNWIND row source requires a list expression",
        )),
    }
}

struct SqlRenderer<'a> {
    validated: ValidatedGraphPlan<'a>,
    subquery_plan: ScalarSubqueryPlan,
    percentile_disc_plan: PercentileDiscAggregatePlan,
    /// Uniqueness counter for inline exists-count aliases (see `render_scalar_predicate_expression`,
    /// predicates.rs). Separate from `subquery_plan`: the alias it mints is local to a generated
    /// COUNT(*) subquery SELECT and is never referenced by the outer query. Kept on the
    /// `SqlRenderer`
    /// (keep-and-share) rather than folded into `ScalarSubqueryPlan`.
    next_scalar_subquery_alias: Cell<usize>,
}

#[derive(Debug, Clone)]
struct ExistsRelationshipSqlBinding<'a, 'b> {
    pattern: &'b RelationshipPattern,
    relationship: &'a Relationship,
    alias: String,
}

#[derive(Clone, Copy)]
enum ScalarScope<'a, 'b, 'c> {
    TopLevel,
    Scoped {
        relationships: &'c [ExistsRelationshipSqlBinding<'a, 'b>],
        local_nodes: &'c BTreeMap<&'b str, &'a Node>,
        local_aliases: &'c BTreeMap<&'b str, String>,
    },
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

#[derive(Debug, Default)]
struct ScalarSubqueryPlan {
    subqueries: Vec<PrecomputedScalarSubquery>,
    from_joins: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PercentileDiscAggregate {
    function: AggregateFunction,
    target: AggregateTarget,
    distinct: bool,
}

#[derive(Debug, Clone)]
struct PrecomputedPercentileDiscAggregate {
    aggregate: PercentileDiscAggregate,
    table_alias: String,
    value_alias: String,
    group_aliases: Vec<String>,
}

#[derive(Debug, Default)]
struct PercentileDiscAggregatePlan {
    aggregates: Vec<PrecomputedPercentileDiscAggregate>,
    from_joins: String,
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

impl<'a> SqlRenderer<'a> {
    fn new(validated: ValidatedGraphPlan<'a>) -> Self {
        Self {
            validated,
            subquery_plan: ScalarSubqueryPlan::default(),
            percentile_disc_plan: PercentileDiscAggregatePlan::default(),
            next_scalar_subquery_alias: Cell::new(0),
        }
    }

    fn lower(mut self) -> Result<SqlTranslation, CoreError> {
        // The borrowed SqlRenderer carries an EMPTY ScalarSubqueryPlan during FROM construction, so
        // any subquery reached while rendering optional-match predicates renders inline, exactly
        // as today.
        let mut from_clause = FromClauseBuilder::new(&self).build()?;
        let plan = self.build_scalar_subquery_plan()?;
        from_clause.push_str(&plan.from_joins);
        self.subquery_plan = plan;
        let percentile_disc_plan = self.build_percentile_disc_aggregate_plan()?;
        from_clause.push_str(&percentile_disc_plan.from_joins);
        self.percentile_disc_plan = percentile_disc_plan;

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
                "{select} {from_clause}{where_clause}{group_by}{having}{order_by}{limit}{offset}"
            ),
            Vec::new(),
        ))
    }
}

#[path = "sql_tests.rs"]
#[cfg(test)]
mod tests;
