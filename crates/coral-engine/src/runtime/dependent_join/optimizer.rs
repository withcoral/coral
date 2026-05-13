use std::fmt;
use std::sync::Arc;

use arrow::datatypes::DataType;
use coral_spec::backends::http::HttpTableSpec;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{DFSchemaRef, ExprSchema, Result};
use datafusion::datasource::source_as_provider;
use datafusion::logical_expr::{Expr, Join, JoinType, LogicalPlan};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};

use crate::backends::http::HttpSourceTableProvider;

/// Optimizer rule shell for dependent predicate pushdown.
///
/// This rule is intentionally inert until the physical dependent join executor
/// exists. Rewriting supported joins before execution is available would turn
/// otherwise-valid fallback plans into runtime failures.
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
    table_schema: DFSchemaRef,
    table: Arc<HttpTableSpec>,
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
        if let LogicalPlan::Join(join) = &plan {
            let _analysis = analyze_join(join);
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
        let (dependent_expr, resolver_expr) = match dependent_side {
            JoinSide::Left => (left_expr, right_expr),
            JoinSide::Right => (right_expr, left_expr),
        };

        let (Expr::Column(dependent_column), Expr::Column(resolver_column)) =
            (dependent_expr, resolver_expr)
        else {
            return DependentJoinAnalysis::Fallback(DependentJoinFallbackReason::NonEqui);
        };

        if dependent
            .table_schema
            .field_from_column(dependent_column)
            .is_err()
        {
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
        .any(|filter| !binding_filters.contains(&filter.name));

    if missing_required {
        return DependentJoinAnalysis::Fallback(DependentJoinFallbackReason::MissingRequired);
    }

    DependentJoinAnalysis::Candidate(DependentJoinCandidate {
        dependent_side,
        source_name: dependent.source_name.clone(),
        table_name: dependent.table.name().to_string(),
        binding_filters,
    })
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

            PeelOutcome::Match(PeeledDependentScan {
                source_name: provider.source_schema().to_string(),
                table_schema: scan.projected_schema.clone(),
                table: Arc::clone(provider.table_spec()),
            })
        }
        LogicalPlan::Filter(filter) => match peel_dependent_side(filter.input.as_ref()) {
            PeelOutcome::Match(_) | PeelOutcome::NonPeelableWrapper => {
                PeelOutcome::NonPeelableWrapper
            }
            PeelOutcome::NotHttp => PeelOutcome::NotHttp,
        },
        LogicalPlan::Projection(projection) => match peel_dependent_side(projection.input.as_ref())
        {
            PeelOutcome::Match(_) | PeelOutcome::NonPeelableWrapper => {
                PeelOutcome::NonPeelableWrapper
            }
            PeelOutcome::NotHttp => PeelOutcome::NotHttp,
        },
        _ => PeelOutcome::NotHttp,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use coral_spec::parse_source_manifest_value;
    use datafusion::common::{Column, TableReference};
    use datafusion::datasource::provider_as_source;
    use datafusion::logical_expr::logical_plan::builder::LogicalTableSource;
    use datafusion::logical_expr::{JoinType, LogicalPlan, LogicalPlanBuilder};
    use datafusion::optimizer::OptimizerRule;
    use serde_json::json;

    use super::{
        DependentJoinAnalysis, DependentJoinFallbackReason, DependentJoinOptimizerRule, JoinSide,
        analyze_join,
    };
    use crate::backends::http::{HttpSourceClient, HttpSourceTableProvider};

    #[test]
    fn rule_is_registered_under_stable_name() {
        let rule = DependentJoinOptimizerRule;
        assert_eq!(rule.name(), "dependent_join_pushdown");
    }

    #[test]
    fn inner_equi_single_binding_is_candidate() {
        let plan = resolver_plan()
            .join(
                dependent_http_plan(bindable_pr_table()),
                JoinType::Inner,
                (
                    vec![column("i", "github_owner")],
                    vec![column("github.pull_requests", "owner")],
                ),
                None,
            )
            .expect("join should build")
            .build()
            .expect("plan should build");

        let analysis = analyze_join(join(&plan));

        assert_eq!(
            analysis,
            DependentJoinAnalysis::Candidate(super::DependentJoinCandidate {
                dependent_side: JoinSide::Right,
                source_name: "github".to_string(),
                table_name: "pull_requests".to_string(),
                binding_filters: vec!["owner".to_string()],
            })
        );
    }

    #[test]
    fn non_inner_join_falls_back() {
        let plan = resolver_plan()
            .join(
                dependent_http_plan(bindable_pr_table()),
                JoinType::Left,
                (
                    vec![column("i", "github_owner")],
                    vec![column("github.pull_requests", "owner")],
                ),
                None,
            )
            .expect("join should build")
            .build()
            .expect("plan should build");

        assert_eq!(
            analyze_join(join(&plan)),
            DependentJoinAnalysis::Fallback(DependentJoinFallbackReason::NonInner)
        );
    }

    #[test]
    fn non_http_provider_falls_back() {
        let plan = resolver_plan()
            .join(
                file_like_plan("right", &[("owner", DataType::Utf8)]),
                JoinType::Inner,
                (
                    vec![column("i", "github_owner")],
                    vec![column("right", "owner")],
                ),
                None,
            )
            .expect("join should build")
            .build()
            .expect("plan should build");

        assert_eq!(
            analyze_join(join(&plan)),
            DependentJoinAnalysis::Fallback(DependentJoinFallbackReason::NonHttpProvider)
        );
    }

    #[test]
    fn non_bindable_dependent_filter_falls_back() {
        let plan = resolver_plan()
            .join(
                dependent_http_plan(non_bindable_pr_table()),
                JoinType::Inner,
                (
                    vec![column("i", "github_owner")],
                    vec![column("github.pull_requests", "owner")],
                ),
                None,
            )
            .expect("join should build")
            .build()
            .expect("plan should build");

        assert_eq!(
            analyze_join(join(&plan)),
            DependentJoinAnalysis::Fallback(DependentJoinFallbackReason::MixedBindable)
        );
    }

    fn join(plan: &LogicalPlan) -> &datafusion::logical_expr::Join {
        let LogicalPlan::Join(join) = plan else {
            panic!("expected join plan");
        };
        join
    }

    fn resolver_plan() -> LogicalPlanBuilder {
        LogicalPlanBuilder::new(file_like_plan(
            "i",
            &[
                ("github_owner", DataType::Utf8),
                ("github_repo", DataType::Utf8),
                ("github_pr_number", DataType::Int64),
                ("status", DataType::Utf8),
            ],
        ))
    }

    fn file_like_plan(table: &str, fields: &[(&str, DataType)]) -> LogicalPlan {
        let schema = Arc::new(Schema::new(
            fields
                .iter()
                .map(|(name, data_type)| Field::new(*name, data_type.clone(), true))
                .collect::<Vec<_>>(),
        ));
        let source = Arc::new(LogicalTableSource::new(schema));
        LogicalPlanBuilder::scan(TableReference::bare(table), source, None)
            .expect("scan should build")
            .build()
            .expect("plan should build")
    }

    fn dependent_http_plan(bindable_owner: bool) -> LogicalPlan {
        let manifest = parse_source_manifest_value(json!({
            "name": "github",
            "version": "0.1.0",
            "dsl_version": 3,
            "backend": "http",
            "base_url": "https://example.invalid",
            "tables": [{
                "name": "pull_requests",
                "description": "Pull requests",
                "filters": [
                    { "name": "owner", "bindable": bindable_owner },
                    { "name": "repo" },
                    { "name": "number" }
                ],
                "request": { "path": "/repos/{{filter.owner}}/{{filter.repo}}/pulls/{{filter.number}}" },
                "columns": [
                    { "name": "owner", "type": "Utf8" },
                    { "name": "repo", "type": "Utf8" },
                    { "name": "number", "type": "Int64" },
                    { "name": "state", "type": "Utf8" }
                ]
            }]
        }))
        .expect("manifest should parse");
        let manifest = manifest.as_http().expect("http manifest").clone();
        let client = HttpSourceClient::from_manifest(
            &manifest,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &HashMap::new(),
        )
        .expect("client should build");
        let table = manifest
            .tables
            .first()
            .expect("manifest has one table")
            .clone();
        let provider = Arc::new(
            HttpSourceTableProvider::new(client, "github".to_string(), table)
                .expect("provider should build"),
        );
        LogicalPlanBuilder::scan(
            TableReference::partial("github", "pull_requests"),
            provider_as_source(provider),
            None,
        )
        .expect("scan should build")
        .build()
        .expect("plan should build")
    }

    fn bindable_pr_table() -> bool {
        true
    }

    fn non_bindable_pr_table() -> bool {
        false
    }

    fn column(relation: &str, name: &str) -> Column {
        Column::new(Some(TableReference::parse_str(relation)), name)
    }
}
