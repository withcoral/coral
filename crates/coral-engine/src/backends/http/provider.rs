//! `DataFusion` table provider for manifest-driven HTTP-backed tables.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::project_schema;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use serde_json::Value;

use crate::backends::http::HttpSourceClient;
use crate::backends::http::ProviderQueryError;
use crate::backends::http::filter_usage::request_filter_names;
use crate::backends::http::target::HttpFetchTarget;
use crate::backends::schema_from_columns;
use crate::backends::shared::filter_expr::{
    FilterExtraction, extract_exact_filter_values_checked, extract_filter_values,
    extract_filter_values_checked, literal_to_string,
};
use crate::backends::shared::json_exec::{JsonExec, RowFetcher};
use crate::backends::shared::mapping::{convert_items, filter_items_by_column_values};
use coral_spec::FilterMode;
use coral_spec::backends::http::HttpTableSpec;

/// Table provider that exposes one manifest-defined HTTP table to `DataFusion`.
pub(crate) struct HttpSourceTableProvider {
    backend: HttpSourceClient,
    source_schema: String,
    table: Arc<HttpTableSpec>,
    target: HttpFetchTarget,
    schema: SchemaRef,
}

impl std::fmt::Debug for HttpSourceTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpSourceTableProvider")
            .field("source_schema", &self.source_schema)
            .field("table", &self.table.name())
            .finish_non_exhaustive()
    }
}

impl HttpSourceTableProvider {
    /// Build a table provider for an `HTTP`-backed source table.
    ///
    /// # Errors
    ///
    /// Returns a `DataFusionError` if the table schema declared in the manifest
    /// is invalid.
    pub(crate) fn new(
        backend: HttpSourceClient,
        source_schema: String,
        table: HttpTableSpec,
    ) -> Result<Self> {
        let schema = schema_from_columns(table.columns(), &source_schema, table.name())?;
        let target = HttpFetchTarget::from_resolved_table_request(&table, table.request.clone());
        Ok(Self {
            backend,
            source_schema,
            table: Arc::new(table),
            target,
            schema,
        })
    }

    pub(crate) fn source_schema(&self) -> &str {
        &self.source_schema
    }

    pub(crate) fn client(&self) -> &HttpSourceClient {
        &self.backend
    }

    pub(crate) fn table_spec(&self) -> &Arc<HttpTableSpec> {
        &self.table
    }
}

#[derive(Debug)]
struct HttpFetchPlan {
    backend: HttpSourceClient,
    target: Arc<HttpFetchTarget>,
    request_filter_values: Arc<HashMap<String, String>>,
    arg_values: Arc<HashMap<String, String>>,
    limit: Option<usize>,
    has_residual_filters: bool,
}

pub(crate) struct HttpJsonExecRequest<'a> {
    pub(crate) backend: HttpSourceClient,
    pub(crate) source_schema: &'a str,
    pub(crate) target: HttpFetchTarget,
    pub(crate) schema: SchemaRef,
    pub(crate) request_filter_values: HashMap<String, String>,
    pub(crate) local_filter_values: HashMap<String, String>,
    pub(crate) has_residual_filters: bool,
    pub(crate) arg_values: HashMap<String, String>,
    pub(crate) projection: Option<&'a Vec<usize>>,
    pub(crate) limit: Option<usize>,
}

#[async_trait]
impl RowFetcher for HttpFetchPlan {
    async fn fetch(&self) -> Result<Vec<Value>> {
        if self.has_residual_filters {
            return self
                .backend
                .fetch_complete(
                    self.target.as_ref(),
                    &self.request_filter_values,
                    &self.arg_values,
                    None,
                    None,
                )
                .await;
        }

        self.backend
            .fetch(
                self.target.as_ref(),
                &self.request_filter_values,
                &self.arg_values,
                self.limit,
            )
            .await
    }
}

pub(crate) fn http_json_exec(request: HttpJsonExecRequest<'_>) -> Result<Arc<dyn ExecutionPlan>> {
    let HttpJsonExecRequest {
        backend,
        source_schema,
        target,
        schema,
        request_filter_values,
        local_filter_values,
        has_residual_filters,
        arg_values,
        projection,
        limit,
    } = request;
    let target = Arc::new(target);
    let request_filter_values = Arc::new(request_filter_values);
    let local_filter_values = Arc::new(local_filter_values);
    let arg_values = Arc::new(arg_values);
    let post_filter_limit = if local_filter_values.is_empty() {
        None
    } else {
        limit.or(target.fetch_limit_default())
    };
    let fetcher = Arc::new(HttpFetchPlan {
        backend,
        target: target.clone(),
        request_filter_values: request_filter_values.clone(),
        arg_values,
        limit,
        has_residual_filters,
    });

    let converter = {
        let target = target.clone();
        let schema = schema.clone();
        let request_filter_values = request_filter_values.clone();
        let local_filter_values = local_filter_values.clone();
        Arc::new(move |items: &[Value]| {
            let mut filtered_items;
            let items = if local_filter_values.is_empty() {
                items
            } else {
                filtered_items =
                    filter_items_by_column_values(target.columns(), &local_filter_values, items);
                if let Some(limit) = post_filter_limit {
                    filtered_items.truncate(limit);
                }
                &filtered_items
            };
            convert_items(
                target.columns(),
                schema.clone(),
                &request_filter_values,
                items,
            )
        })
    };

    let exec = JsonExec::new(
        source_schema,
        target.name(),
        schema,
        fetcher,
        converter,
        projection.cloned(),
    )?;

    Ok(Arc::new(exec))
}

#[async_trait]
impl TableProvider for HttpSourceTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let allowed: HashSet<&str> = self
            .table
            .filters()
            .iter()
            .map(|f| f.name.as_str())
            .collect();
        let filter_modes: HashMap<&str, FilterMode> = self
            .table
            .filters()
            .iter()
            .map(|f| (f.name.as_str(), f.mode))
            .collect();
        let filter_exprs = filters
            .iter()
            .map(|expr| (*expr).clone())
            .collect::<Vec<_>>();
        let filter_values = extract_filter_values(&filter_exprs, self.table.filters());
        let filter_value_keys: HashSet<String> = filter_values.keys().cloned().collect();
        let active_request = self.table.resolve_request(&filter_value_keys);
        let consumed_filters = request_filter_names(active_request);

        Ok(filters
            .iter()
            .map(|expr| classify_filter(expr, &allowed, &filter_modes, &consumed_filters))
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let filter_values = match extract_filter_values_checked(filters, self.table.filters()) {
            FilterExtraction::Values(values) => values,
            FilterExtraction::Contradiction => {
                let projected_schema = project_schema(&self.schema, projection)?;
                return Ok(Arc::new(EmptyExec::new(projected_schema)));
            }
        };

        for required in self.table.filters().iter().filter(|f| f.required) {
            if !filter_values.contains_key(&required.name) {
                return Err(DataFusionError::External(Box::new(
                    ProviderQueryError::MissingRequiredFilter {
                        schema: self.source_schema.clone(),
                        table: self.table.name().to_string(),
                        column: required.name.clone(),
                    },
                )));
            }
        }

        let filter_value_keys: HashSet<String> = filter_values.keys().cloned().collect();
        let active_request = self.table.resolve_request(&filter_value_keys).clone();
        let consumed_filters = request_filter_names(&active_request);
        let request_filter_values = filter_values
            .iter()
            .filter(|(filter, _)| consumed_filters.contains(*filter))
            .map(|(filter, value)| (filter.clone(), value.clone()))
            .collect();
        let has_residual_filters = filter_values
            .keys()
            .any(|filter| !consumed_filters.contains(filter));
        let local_filter_values =
            match extract_exact_filter_values_checked(filters, self.table.filters()) {
                FilterExtraction::Values(values) => values
                    .into_iter()
                    .filter(|(filter, _)| !consumed_filters.contains(filter))
                    .collect(),
                FilterExtraction::Contradiction => HashMap::new(),
            };
        let target = self.target.with_resolved_request(active_request);

        http_json_exec(HttpJsonExecRequest {
            backend: self.backend.clone(),
            source_schema: &self.source_schema,
            target,
            schema: self.schema.clone(),
            request_filter_values,
            local_filter_values,
            has_residual_filters,
            arg_values: HashMap::new(),
            projection,
            limit,
        })
    }
}

fn classify_filter(
    expr: &Expr,
    allowed: &HashSet<&str>,
    filter_modes: &HashMap<&str, FilterMode>,
    consumed_filters: &HashSet<String>,
) -> TableProviderFilterPushDown {
    if let Expr::Column(col) = expr
        && allowed.contains(col.name())
    {
        return exact_if_consumed(col.name(), consumed_filters);
    }
    if let Expr::Not(inner) = expr
        && let Expr::Column(col) = inner.as_ref()
        && allowed.contains(col.name())
    {
        return exact_if_consumed(col.name(), consumed_filters);
    }
    if let Expr::IsTrue(inner) | Expr::IsFalse(inner) = expr
        && let Expr::Column(col) = inner.as_ref()
        && allowed.contains(col.name())
    {
        return exact_if_consumed(col.name(), consumed_filters);
    }
    if let Expr::BinaryExpr(binary) = expr
        && binary.op == Operator::Eq
        && let Expr::Column(col) = binary.left.as_ref()
        && allowed.contains(col.name())
        && literal_to_string(binary.right.as_ref()).is_some()
    {
        return exact_if_consumed(col.name(), consumed_filters);
    }
    if let Expr::Like(like) = expr
        && !like.negated
        && let Expr::Column(col) = like.expr.as_ref()
        && allowed.contains(col.name())
        && literal_to_string(like.pattern.as_ref()).is_some()
    {
        let mode = filter_modes.get(col.name()).copied().unwrap_or_default();
        if matches!(mode, FilterMode::Search | FilterMode::Contains)
            && consumed_filters.contains(col.name())
        {
            // Inexact: the API receives the stripped search term (performance
            // win) but DataFusion keeps a residual filter to enforce exact
            // LIKE/ILIKE semantics client-side (correctness win).
            return TableProviderFilterPushDown::Inexact;
        }
    }
    TableProviderFilterPushDown::Unsupported
}

fn exact_if_consumed(
    col_name: &str,
    consumed_filters: &HashSet<String>,
) -> TableProviderFilterPushDown {
    if consumed_filters.contains(col_name) {
        TableProviderFilterPushDown::Exact
    } else {
        TableProviderFilterPushDown::Unsupported
    }
}

#[cfg(test)]
mod tests {
    use super::classify_filter;
    use coral_spec::FilterMode;
    use datafusion::common::Column;
    use datafusion::logical_expr::{
        Expr, Operator, TableProviderFilterPushDown, binary_expr, expr::Like, lit,
    };
    use std::collections::{HashMap, HashSet};
    use std::ops::Not;

    fn allowed<'a>(names: &'a [&'a str]) -> HashSet<&'a str> {
        names.iter().copied().collect()
    }

    fn modes<'a>(entries: &'a [(&'a str, FilterMode)]) -> HashMap<&'a str, FilterMode> {
        entries.iter().copied().collect()
    }

    fn consumed(names: &[&str]) -> HashSet<String> {
        names.iter().map(|name| (*name).to_string()).collect()
    }

    fn like_expr(col_name: &str, pattern: &str) -> Expr {
        Expr::Like(Like::new(
            false,
            Box::new(col(col_name)),
            Box::new(lit(pattern)),
            None,
            false,
        ))
    }

    fn col(name: &str) -> Expr {
        Expr::Column(Column::from_name(name))
    }

    #[test]
    fn like_ignored_for_equality_mode_filter() {
        let pushdown = classify_filter(
            &like_expr("status", "%open%"),
            &allowed(&["status"]),
            &modes(&[("status", FilterMode::Equality)]),
            &consumed(&["status"]),
        );
        assert_eq!(pushdown, TableProviderFilterPushDown::Unsupported);
    }

    #[test]
    fn strips_wildcards_from_like_pattern() {
        let pushdown = classify_filter(
            &like_expr("q", "%deploy runbook%"),
            &allowed(&["q"]),
            &modes(&[("q", FilterMode::Search)]),
            &consumed(&["q"]),
        );
        assert_eq!(pushdown, TableProviderFilterPushDown::Inexact);
    }

    #[test]
    fn search_filter_also_accepts_equality() {
        let pushdown = classify_filter(
            &binary_expr(col("query"), Operator::Eq, lit("deploy")),
            &allowed(&["query"]),
            &modes(&[("query", FilterMode::Search)]),
            &consumed(&["query"]),
        );
        assert_eq!(pushdown, TableProviderFilterPushDown::Exact);
    }

    #[test]
    fn equality_filter_is_unsupported_when_active_request_does_not_consume_it() {
        let pushdown = classify_filter(
            &binary_expr(col("state"), Operator::Eq, lit("open")),
            &allowed(&["state"]),
            &modes(&[]),
            &consumed(&[]),
        );
        assert_eq!(pushdown, TableProviderFilterPushDown::Unsupported);
    }

    #[test]
    fn extracts_like_value_for_search_mode_filter() {
        let pushdown = classify_filter(
            &like_expr("query", "%deploy%"),
            &allowed(&["query"]),
            &modes(&[("query", FilterMode::Search)]),
            &consumed(&["query"]),
        );
        assert_eq!(pushdown, TableProviderFilterPushDown::Inexact);
    }

    #[test]
    fn search_like_filter_is_unsupported_when_active_request_does_not_consume_it() {
        let pushdown = classify_filter(
            &like_expr("query", "%deploy%"),
            &allowed(&["query"]),
            &modes(&[("query", FilterMode::Search)]),
            &consumed(&[]),
        );
        assert_eq!(pushdown, TableProviderFilterPushDown::Unsupported);
    }

    #[test]
    fn boolean_column_filter_pushes_down_exactly() {
        let pushdown = classify_filter(
            &col("descending"),
            &allowed(&["descending"]),
            &modes(&[]),
            &consumed(&["descending"]),
        );
        assert_eq!(pushdown, TableProviderFilterPushDown::Exact);
    }

    #[test]
    fn negated_boolean_column_filter_pushes_down_exactly() {
        let pushdown = classify_filter(
            &col("descending").not(),
            &allowed(&["descending"]),
            &modes(&[]),
            &consumed(&["descending"]),
        );
        assert_eq!(pushdown, TableProviderFilterPushDown::Exact);
    }

    #[test]
    fn boolean_is_true_and_is_false_push_down_exactly() {
        for expr in [
            Expr::IsTrue(Box::new(col("descending"))),
            Expr::IsFalse(Box::new(col("descending"))),
        ] {
            let pushdown = classify_filter(
                &expr,
                &allowed(&["descending"]),
                &modes(&[]),
                &consumed(&["descending"]),
            );
            assert_eq!(pushdown, TableProviderFilterPushDown::Exact);
        }
    }

    #[test]
    fn null_inclusive_boolean_is_predicates_are_not_pushed_down() {
        for expr in [
            Expr::IsNotTrue(Box::new(col("descending"))),
            Expr::IsNotFalse(Box::new(col("descending"))),
        ] {
            let pushdown = classify_filter(
                &expr,
                &allowed(&["descending"]),
                &modes(&[]),
                &consumed(&["descending"]),
            );
            assert_eq!(pushdown, TableProviderFilterPushDown::Unsupported);
        }
    }
}
