use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::DataType;
use coral_spec::WireType;
use coral_spec::backends::http::HttpTableSpec;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{Column, DFSchemaRef, ExprSchema, NullEquality, Result, TableReference};
use datafusion::datasource::source_as_provider;
use datafusion::logical_expr::{
    Expr, Extension, FetchType, Join, JoinType, Limit, LogicalPlan, Projection, SkipType,
};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};

use crate::backends::http::HttpSourceTableProvider;
use crate::backends::http::filter_usage::request_filter_names;
use crate::backends::shared::filter_expr::literal_to_string;
use crate::runtime::dependent_join::logical::{BindingKey, DependentJoinNode};

const DEFAULT_MAX_BINDINGS: usize = 500;
const DEFAULT_MAX_RESOLVER_ROWS: usize = 10_000;
const DEFAULT_MAX_ROWS_PER_BINDING: usize = 1_000;
const DEFAULT_MAX_RESOLVER_ROWS_PER_BINDING: usize = 1_000;
const DEFAULT_BINDING_CONCURRENCY: usize = 8;
const BINDING_COLUMN_PREFIX: &str = "__coral_dj_bind_";

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
    UnconsumedFilter,
}

impl DependentJoinFallbackReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::NonInner => "not_inner_join",
            Self::NonEqui => "not_inner_equi_join",
            Self::NonHttpProvider => "not_http_provider",
            Self::NonPeelableWrapper => "non_peelable_wrapper",
            Self::MixedBindable => "mixed_or_missing_bindable_filter",
            Self::MissingRequired => "missing_required_filter",
            Self::OverConstrained => "over_constrained_filter",
            Self::NonCoercible => "non_coercible_binding_type",
            Self::CostUnfavourable => "cost_unfavourable",
            Self::UnconsumedFilter => "unconsumed_filter",
        }
    }
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
        if let LogicalPlan::Limit(limit) = &plan
            && let Some(rewritten) = rewrite_limit_page_hint(limit)?
        {
            return Ok(Transformed::yes(rewritten));
        }

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

    if join.null_equality == NullEquality::NullEqualsNull {
        return DependentJoinAnalysis::Fallback(DependentJoinFallbackReason::NonEqui);
    }

    if join.on.is_empty() || join.filter.is_some() {
        return DependentJoinAnalysis::Fallback(DependentJoinFallbackReason::NonEqui);
    }

    let left = analyze_side_as_dependent(
        JoinSide::Left,
        join.left.as_ref(),
        join.right.schema(),
        &join.on,
    );
    let right = analyze_side_as_dependent(
        JoinSide::Right,
        join.right.as_ref(),
        join.left.schema(),
        &join.on,
    );

    match (&left, &right) {
        (DependentJoinAnalysis::Candidate(_), DependentJoinAnalysis::Fallback(_)) => left,
        (DependentJoinAnalysis::Fallback(_), DependentJoinAnalysis::Candidate(_)) => right,
        (DependentJoinAnalysis::Candidate(_), DependentJoinAnalysis::Candidate(_)) => {
            DependentJoinAnalysis::Fallback(DependentJoinFallbackReason::MixedBindable)
        }
        (
            DependentJoinAnalysis::Fallback(DependentJoinFallbackReason::NonHttpProvider),
            DependentJoinAnalysis::Fallback(DependentJoinFallbackReason::NonHttpProvider),
        ) => DependentJoinAnalysis::Fallback(DependentJoinFallbackReason::NonHttpProvider),
        (DependentJoinAnalysis::Fallback(reason), _) => DependentJoinAnalysis::Fallback(*reason),
    }
}

fn analyze_side_as_dependent(
    dependent_side: JoinSide,
    dependent_plan: &LogicalPlan,
    resolver_schema: &DFSchemaRef,
    join_on: &[(Expr, Expr)],
) -> DependentJoinAnalysis {
    let peeled = peel_dependent_side(dependent_plan);
    match &peeled {
        PeelOutcome::Match(dependent) => {
            analyze_dependent_bindings(dependent_side, dependent, resolver_schema, join_on)
        }
        PeelOutcome::NotHttp => {
            DependentJoinAnalysis::Fallback(DependentJoinFallbackReason::NonHttpProvider)
        }
        PeelOutcome::NonPeelableWrapper => {
            DependentJoinAnalysis::Fallback(DependentJoinFallbackReason::NonPeelableWrapper)
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

        if binding_filters.contains(&filter.name) {
            return DependentJoinAnalysis::Fallback(DependentJoinFallbackReason::OverConstrained);
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

    let provided_filters = dependent
        .literal_filters
        .keys()
        .chain(binding_filters.iter())
        .cloned()
        .collect::<std::collections::HashSet<_>>();
    let active_request = dependent.table.resolve_request(&provided_filters);
    let consumed_filters = request_filter_names(active_request);
    if provided_filters
        .iter()
        .any(|filter| !consumed_filters.contains(filter))
    {
        return DependentJoinAnalysis::Fallback(DependentJoinFallbackReason::UnconsumedFilter);
    }

    DependentJoinAnalysis::Candidate(DependentJoinCandidate {
        dependent_side,
        source_name: dependent.source_name.clone(),
        table_name: dependent.table.name().to_string(),
        binding_filters,
    })
}

fn rewrite_join(join: &Join) -> Option<LogicalPlan> {
    let candidate = match analyze_join(join) {
        DependentJoinAnalysis::Candidate(candidate) => candidate,
        DependentJoinAnalysis::Fallback(reason) => {
            tracing::debug!(
                target = "coral_engine::dependent_join",
                reason = reason.as_str(),
                join_kind = ?join.join_type,
                null_equality = ?join.null_equality,
                join_predicates = join.on.len(),
                has_join_filter = join.filter.is_some(),
                "skipping dependent join rewrite candidate",
            );
            return None;
        }
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

    let (resolver, binding_keys, resolver_projection_len) =
        resolver_with_binding_columns(resolver_plan, &dependent, resolver_schema, &join.on)?;

    let node = DependentJoinNode {
        resolver,
        dependent_table: dependent.table_ref,
        binding_keys,
        literal_filters: dependent.literal_filters,
        dependent_projection: dependent.dependent_projection,
        resolver_projection_len,
        dependent_first,
        schema: join.schema.clone(),
        max_bindings: DEFAULT_MAX_BINDINGS,
        max_resolver_rows: DEFAULT_MAX_RESOLVER_ROWS,
        max_rows_per_binding: DEFAULT_MAX_ROWS_PER_BINDING,
        max_resolver_rows_per_binding: DEFAULT_MAX_RESOLVER_ROWS_PER_BINDING,
        max_concurrency: dependent
            .max_concurrency
            .unwrap_or(DEFAULT_BINDING_CONCURRENCY),
        page_hint: None,
    };

    Some(LogicalPlan::Extension(Extension {
        node: Arc::new(node),
    }))
}

fn resolver_with_binding_columns(
    resolver_plan: &LogicalPlan,
    dependent: &PeeledDependentScan,
    resolver_schema: &DFSchemaRef,
    join_on: &[(Expr, Expr)],
) -> Option<(LogicalPlan, Vec<BindingKey>, usize)> {
    let resolver_columns = resolver_schema.columns();
    let resolver_projection_len = resolver_columns.len();
    let mut used_names = resolver_columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<BTreeSet<_>>();
    let mut expr = resolver_columns
        .into_iter()
        .map(Expr::Column)
        .collect::<Vec<_>>();
    let mut binding_keys = Vec::with_capacity(join_on.len());
    let mut binding_filter_names = BTreeSet::new();

    for (binding_index, (left_expr, right_expr)) in join_on.iter().enumerate() {
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
        if !binding_filter_names.insert(filter.name.as_str()) {
            return None;
        }

        let resolver_binding_name = unique_binding_column_name(&mut used_names, binding_index);
        expr.push(
            Expr::Column(Column::new(
                resolver_column.relation.clone(),
                &resolver_column.name,
            ))
            .alias(&resolver_binding_name),
        );
        binding_keys.push(BindingKey {
            resolver_column: Column::new(resolver_column.relation.clone(), &resolver_column.name),
            resolver_binding_name,
            dependent_filter: filter.name.clone(),
            wire_type: filter.wire_type,
        });
    }

    let projection = Projection::try_new(expr, Arc::new(resolver_plan.clone())).ok()?;
    Some((
        LogicalPlan::Projection(projection),
        binding_keys,
        resolver_projection_len,
    ))
}

fn unique_binding_column_name(used_names: &mut BTreeSet<String>, binding_index: usize) -> String {
    let mut candidate = format!("{BINDING_COLUMN_PREFIX}{binding_index}");
    let mut suffix = 0usize;
    while !used_names.insert(candidate.clone()) {
        suffix += 1;
        candidate = format!("{BINDING_COLUMN_PREFIX}{binding_index}_{suffix}");
    }
    candidate
}

fn rewrite_limit_page_hint(limit: &Limit) -> Result<Option<LogicalPlan>> {
    let FetchType::Literal(Some(fetch)) = limit.get_fetch_type()? else {
        return Ok(None);
    };
    let SkipType::Literal(skip) = limit.get_skip_type()? else {
        return Ok(None);
    };
    let page_hint = fetch.saturating_add(skip);
    if page_hint == 0 {
        return Ok(None);
    }

    let LogicalPlan::Extension(extension) = limit.input.as_ref() else {
        return Ok(None);
    };
    let Some(node) = extension.node.as_any().downcast_ref::<DependentJoinNode>() else {
        return Ok(None);
    };
    if node.page_hint == Some(page_hint) {
        return Ok(None);
    }

    let mut hinted = node.clone();
    hinted.page_hint = Some(page_hint);

    Ok(Some(LogicalPlan::Limit(Limit {
        skip: limit.skip.clone(),
        fetch: limit.fetch.clone(),
        input: Arc::new(LogicalPlan::Extension(Extension {
            node: Arc::new(hinted),
        })),
    })))
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
