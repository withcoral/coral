use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::DataType;
use coral_spec::WireType;
use coral_spec::backends::http::HttpTableSpec;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{Column, DFSchemaRef, ExprSchema, Result, TableReference};
use datafusion::datasource::source_as_provider;
use datafusion::logical_expr::{Expr, Extension, Join, JoinType, LogicalPlan};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};

use crate::backends::http::HttpSourceTableProvider;
use crate::backends::shared::filter_expr::literal_to_string;
use crate::runtime::dependent_join::logical::{BindingKey, DependentJoinNode};

const DEFAULT_MAX_BINDINGS: usize = 500;
const DEFAULT_MAX_RESOLVER_ROWS: usize = 10_000;
const DEFAULT_MAX_ROWS_PER_BINDING: usize = 50_000;
const DEFAULT_BINDING_CONCURRENCY: usize = 8;

/// Optimizer rule for dependent predicate pushdown.
#[derive(Default)]
pub(crate) struct DependentJoinOptimizerRule;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[expect(
    dead_code,
    reason = "fallback taxonomy mirrors the RFC; later optimizer slices construct the remaining reasons"
)]
pub(crate) enum DependentJoinFallbackReason {
    NonInner,
    NonEqui,
    NonHttpProvider,
    NonPeelableWrapper,
    MixedBindable,
    MissingRequired,
    OverConstrained,
    NonCoercible,
    CostUnfavourable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JoinSide {
    Left,
    Right,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DependentJoinCandidate {
    pub(crate) dependent_side: JoinSide,
    pub(crate) source_name: String,
    pub(crate) table_name: String,
    pub(crate) binding_filters: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DependentJoinAnalysis {
    Candidate(DependentJoinCandidate),
    Fallback(DependentJoinFallbackReason),
}

struct PeeledDependentScan {
    source_name: String,
    table_ref: TableReference,
    table_schema: DFSchemaRef,
    table: Arc<HttpTableSpec>,
    literal_filters: BTreeMap<String, String>,
    dependent_projection: Vec<usize>,
    max_concurrency: Option<usize>,
}

enum PeelOutcome {
    Match(PeeledDependentScan),
    NotHttp,
    NonPeelableWrapper,
}

impl fmt::Debug for DependentJoinOptimizerRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DependentJoinOptimizerRule").finish()
    }
}

impl OptimizerRule for DependentJoinOptimizerRule {
    fn name(&self) -> &'static str {
        "dependent_join_pushdown"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if let LogicalPlan::Join(join) = &plan
            && let Some(rewritten) = rewrite_join(join)
        {
            return Ok(Transformed::yes(rewritten));
        }

        Ok(Transformed::no(plan))
    }
}

pub(crate) fn rule() -> DependentJoinOptimizerRule {
    DependentJoinOptimizerRule
}

fn analyze_join(join: &Join) -> DependentJoinAnalysis {
    if join.join_type != JoinType::Inner {
        return DependentJoinAnalysis::Fallback(DependentJoinFallbackReason::NonInner);
    }

    if join.on.is_empty() || join.filter.is_some() {
        return DependentJoinAnalysis::Fallback(DependentJoinFallbackReason::NonEqui);
    }

    let left_dependent = peel_dependent_side(join.left.as_ref());
    let right_dependent = peel_dependent_side(join.right.as_ref());

    match (left_dependent, right_dependent) {
        (PeelOutcome::NonPeelableWrapper, _) | (_, PeelOutcome::NonPeelableWrapper) => {
            DependentJoinAnalysis::Fallback(DependentJoinFallbackReason::NonPeelableWrapper)
        }
        (PeelOutcome::Match(dependent), PeelOutcome::NotHttp) => {
            analyze_dependent_bindings(JoinSide::Left, &dependent, join.right.schema(), &join.on)
        }
        (PeelOutcome::NotHttp, PeelOutcome::Match(dependent)) => {
            analyze_dependent_bindings(JoinSide::Right, &dependent, join.left.schema(), &join.on)
        }
        (PeelOutcome::NotHttp, PeelOutcome::NotHttp) => {
            DependentJoinAnalysis::Fallback(DependentJoinFallbackReason::NonHttpProvider)
        }
        (PeelOutcome::Match(_), PeelOutcome::Match(_)) => {
            DependentJoinAnalysis::Fallback(DependentJoinFallbackReason::MixedBindable)
        }
    }
}

fn analyze_dependent_bindings(
    dependent_side: JoinSide,
    dependent: &PeeledDependentScan,
    resolver_schema: &DFSchemaRef,
    join_on: &[(Expr, Expr)],
) -> DependentJoinAnalysis {
    let mut binding_filters = Vec::with_capacity(join_on.len());

    for (left_expr, right_expr) in join_on {
        let Some((dependent_column, resolver_column)) =
            split_dependent_resolver_columns(dependent, resolver_schema, left_expr, right_expr)
        else {
            return DependentJoinAnalysis::Fallback(DependentJoinFallbackReason::NonEqui);
        };

        if !dependent_has_column(dependent, dependent_column) {
            return DependentJoinAnalysis::Fallback(DependentJoinFallbackReason::NonEqui);
        }

        let Some(filter) = dependent
            .table
            .filters()
            .iter()
            .find(|filter| filter.name == dependent_column.name)
        else {
            return DependentJoinAnalysis::Fallback(DependentJoinFallbackReason::MixedBindable);
        };

        if !filter.bindable {
            return DependentJoinAnalysis::Fallback(DependentJoinFallbackReason::MixedBindable);
        }

        let Ok(field) = resolver_schema.field_from_column(resolver_column) else {
            return DependentJoinAnalysis::Fallback(DependentJoinFallbackReason::NonCoercible);
        };

        if !matches!(
            field.data_type(),
            DataType::Utf8 | DataType::Int64 | DataType::Boolean
        ) {
            return DependentJoinAnalysis::Fallback(DependentJoinFallbackReason::NonCoercible);
        }

        binding_filters.push(filter.name.clone());
    }

    let missing_required = dependent
        .table
        .filters()
        .iter()
        .filter(|filter| filter.required)
        .any(|filter| {
            !binding_filters.contains(&filter.name)
                && !dependent.literal_filters.contains_key(&filter.name)
        });

    if missing_required {
        return DependentJoinAnalysis::Fallback(DependentJoinFallbackReason::MissingRequired);
    }

    if binding_filters
        .iter()
        .any(|filter| dependent.literal_filters.contains_key(filter))
    {
        return DependentJoinAnalysis::Fallback(DependentJoinFallbackReason::OverConstrained);
    }

    DependentJoinAnalysis::Candidate(DependentJoinCandidate {
        dependent_side,
        source_name: dependent.source_name.clone(),
        table_name: dependent.table.name().to_string(),
        binding_filters,
    })
}

fn rewrite_join(join: &Join) -> Option<LogicalPlan> {
    let DependentJoinAnalysis::Candidate(candidate) = analyze_join(join) else {
        return None;
    };

    let (dependent_plan, resolver_plan, resolver_schema, dependent_first) =
        match candidate.dependent_side {
            JoinSide::Left => (
                join.left.as_ref(),
                join.right.as_ref(),
                join.right.schema(),
                true,
            ),
            JoinSide::Right => (
                join.right.as_ref(),
                join.left.as_ref(),
                join.left.schema(),
                false,
            ),
        };

    let PeelOutcome::Match(dependent) = peel_dependent_side(dependent_plan) else {
        return None;
    };

    let binding_keys = binding_keys_for_join(&dependent, resolver_schema, &join.on)?;

    let max_bindings = resolve_max_bindings(&dependent, &binding_keys);
    let node = DependentJoinNode {
        resolver: resolver_plan.clone(),
        dependent_table: dependent.table_ref,
        binding_keys,
        literal_filters: dependent.literal_filters,
        dependent_projection: dependent.dependent_projection,
        dependent_first,
        schema: join.schema.clone(),
        max_bindings,
        max_resolver_rows: dependent
            .table
            .dependent_join
            .max_resolver_rows
            .unwrap_or(DEFAULT_MAX_RESOLVER_ROWS),
        max_rows_per_binding: dependent
            .table
            .dependent_join
            .max_rows_per_binding
            .unwrap_or(DEFAULT_MAX_ROWS_PER_BINDING),
        max_concurrency: dependent
            .max_concurrency
            .unwrap_or(DEFAULT_BINDING_CONCURRENCY),
        page_hint: None,
    };

    Some(LogicalPlan::Extension(Extension {
        node: Arc::new(node),
    }))
}

fn binding_keys_for_join(
    dependent: &PeeledDependentScan,
    resolver_schema: &DFSchemaRef,
    join_on: &[(Expr, Expr)],
) -> Option<Vec<BindingKey>> {
    let mut binding_keys = Vec::with_capacity(join_on.len());

    for (left_expr, right_expr) in join_on {
        let (dependent_column, resolver_column) =
            split_dependent_resolver_columns(dependent, resolver_schema, left_expr, right_expr)?;

        if !matches!(
            resolver_schema
                .field_from_column(resolver_column)
                .ok()?
                .data_type(),
            DataType::Utf8 | DataType::Int64 | DataType::Boolean
        ) {
            return None;
        }

        let filter = dependent
            .table
            .filters()
            .iter()
            .find(|filter| filter.name == dependent_column.name)?;

        if !filter.bindable || filter.wire_type != WireType::String {
            return None;
        }

        binding_keys.push(BindingKey {
            resolver_column: Column::new(resolver_column.relation.clone(), &resolver_column.name),
            dependent_filter: filter.name.clone(),
            wire_type: filter.wire_type,
        });
    }

    Some(binding_keys)
}

fn split_dependent_resolver_columns<'a>(
    dependent: &PeeledDependentScan,
    resolver_schema: &DFSchemaRef,
    left_expr: &'a Expr,
    right_expr: &'a Expr,
) -> Option<(&'a Column, &'a Column)> {
    let (Expr::Column(left_column), Expr::Column(right_column)) = (left_expr, right_expr) else {
        return None;
    };

    let left_is_dependent = dependent_has_column(dependent, left_column);
    let right_is_dependent = dependent_has_column(dependent, right_column);
    let left_is_resolver = resolver_schema.field_from_column(left_column).is_ok();
    let right_is_resolver = resolver_schema.field_from_column(right_column).is_ok();

    match (
        left_is_dependent,
        right_is_dependent,
        left_is_resolver,
        right_is_resolver,
    ) {
        (true, false, false, true) => Some((left_column, right_column)),
        (false, true, true, false) => Some((right_column, left_column)),
        _ => None,
    }
}

fn dependent_has_column(dependent: &PeeledDependentScan, column: &Column) -> bool {
    dependent.table_schema.field_from_column(column).is_ok()
}

fn resolve_max_bindings(dependent: &PeeledDependentScan, binding_keys: &[BindingKey]) -> usize {
    if let [binding_key] = binding_keys
        && let Some(filter_cap) = dependent
            .table
            .filters()
            .iter()
            .find(|filter| filter.name == binding_key.dependent_filter)
            .and_then(|filter| filter.max_bindings)
    {
        return filter_cap;
    }

    dependent
        .table
        .dependent_join
        .max_bindings
        .unwrap_or(DEFAULT_MAX_BINDINGS)
}

fn peel_dependent_side(plan: &LogicalPlan) -> PeelOutcome {
    match plan {
        LogicalPlan::TableScan(scan) => {
            let Ok(provider) = source_as_provider(&scan.source) else {
                return PeelOutcome::NotHttp;
            };
            let Some(provider) = provider.as_any().downcast_ref::<HttpSourceTableProvider>() else {
                return PeelOutcome::NotHttp;
            };
            let Some(literal_filters) =
                extract_dependent_literal_filters(&scan.filters, provider.table_spec())
            else {
                return PeelOutcome::NonPeelableWrapper;
            };

            PeelOutcome::Match(PeeledDependentScan {
                source_name: provider.source_schema().to_string(),
                table_ref: scan.table_name.clone(),
                table_schema: scan.projected_schema.clone(),
                table: Arc::clone(provider.table_spec()),
                literal_filters,
                dependent_projection: scan
                    .projection
                    .clone()
                    .unwrap_or_else(|| (0..provider.table_spec().columns().len()).collect()),
                max_concurrency: provider.client().max_concurrency(),
            })
        }
        LogicalPlan::Filter(filter) => match peel_dependent_side(filter.input.as_ref()) {
            PeelOutcome::Match(mut dependent) => {
                let Some(literals) = extract_dependent_literal_filters(
                    std::slice::from_ref(&filter.predicate),
                    &dependent.table,
                ) else {
                    return PeelOutcome::NonPeelableWrapper;
                };
                if merge_literal_filters(&mut dependent.literal_filters, literals).is_none() {
                    return PeelOutcome::NonPeelableWrapper;
                }
                PeelOutcome::Match(dependent)
            }
            PeelOutcome::NonPeelableWrapper => PeelOutcome::NonPeelableWrapper,
            PeelOutcome::NotHttp => PeelOutcome::NotHttp,
        },
        LogicalPlan::Projection(projection) => match peel_dependent_side(projection.input.as_ref())
        {
            PeelOutcome::Match(dependent) => peel_dependent_projection(dependent, projection),
            PeelOutcome::NonPeelableWrapper => PeelOutcome::NonPeelableWrapper,
            PeelOutcome::NotHttp => PeelOutcome::NotHttp,
        },
        LogicalPlan::SubqueryAlias(alias) => match peel_dependent_side(alias.input.as_ref()) {
            PeelOutcome::Match(mut dependent) => {
                dependent.table_schema = alias.schema.clone();
                PeelOutcome::Match(dependent)
            }
            PeelOutcome::NonPeelableWrapper => PeelOutcome::NonPeelableWrapper,
            PeelOutcome::NotHttp => PeelOutcome::NotHttp,
        },
        _ => PeelOutcome::NotHttp,
    }
}

fn peel_dependent_projection(
    mut dependent: PeeledDependentScan,
    projection: &datafusion::logical_expr::Projection,
) -> PeelOutcome {
    let mut projected_indices = Vec::with_capacity(projection.expr.len());

    for expr in &projection.expr {
        let Expr::Column(column) = expr else {
            return PeelOutcome::NonPeelableWrapper;
        };
        let Ok(input_idx) = dependent.table_schema.index_of_column(column) else {
            return PeelOutcome::NonPeelableWrapper;
        };
        let Some(table_idx) = dependent.dependent_projection.get(input_idx).copied() else {
            return PeelOutcome::NonPeelableWrapper;
        };
        projected_indices.push(table_idx);
    }

    dependent.table_schema = projection.schema.clone();
    dependent.dependent_projection = projected_indices;
    PeelOutcome::Match(dependent)
}

fn extract_dependent_literal_filters(
    predicates: &[Expr],
    table: &HttpTableSpec,
) -> Option<BTreeMap<String, String>> {
    let allowed = table
        .filters()
        .iter()
        .map(|filter| filter.name.as_str())
        .collect::<Vec<_>>();
    let mut literals = BTreeMap::new();

    for predicate in predicates {
        let extracted = extract_dependent_literal_filter(predicate, &allowed)?;
        merge_literal_filters(&mut literals, extracted)?;
    }

    Some(literals)
}

fn extract_dependent_literal_filter(
    predicate: &Expr,
    allowed_filters: &[&str],
) -> Option<BTreeMap<String, String>> {
    match predicate {
        Expr::BinaryExpr(binary) if binary.op == datafusion::logical_expr::Operator::And => {
            let mut left = extract_dependent_literal_filter(binary.left.as_ref(), allowed_filters)?;
            let right = extract_dependent_literal_filter(binary.right.as_ref(), allowed_filters)?;
            merge_literal_filters(&mut left, right)?;
            Some(left)
        }
        Expr::BinaryExpr(binary) if binary.op == datafusion::logical_expr::Operator::Eq => {
            literal_equality_filter(binary.left.as_ref(), binary.right.as_ref(), allowed_filters)
                .or_else(|| {
                    literal_equality_filter(
                        binary.right.as_ref(),
                        binary.left.as_ref(),
                        allowed_filters,
                    )
                })
                .map(|(filter, value)| BTreeMap::from([(filter, value)]))
        }
        _ => None,
    }
}

fn literal_equality_filter(
    column_expr: &Expr,
    literal_expr: &Expr,
    allowed_filters: &[&str],
) -> Option<(String, String)> {
    let Expr::Column(column) = column_expr else {
        return None;
    };

    if !allowed_filters.contains(&column.name.as_str()) {
        return None;
    }

    Some((column.name.clone(), literal_to_string(literal_expr)?))
}

fn merge_literal_filters(
    target: &mut BTreeMap<String, String>,
    incoming: BTreeMap<String, String>,
) -> Option<()> {
    for (filter, value) in incoming {
        if let Some(existing) = target.get(&filter)
            && existing != &value
        {
            return None;
        }

        target.insert(filter, value);
    }

    Some(())
}
