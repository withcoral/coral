//! `DataFusion` table provider for manifest-driven HTTP-backed tables.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use serde_json::Value;

use crate::backends::http::HttpSourceClient;
use crate::backends::http::ProviderQueryError;
use crate::backends::schema_from_columns;
use crate::backends::shared::filter_expr::{extract_filter_values, literal_to_string};
use crate::backends::shared::json_exec::{JsonExec, RowFetcher};
use crate::backends::shared::mapping::convert_items;
use coral_spec::FilterMode;
use coral_spec::backends::http::HttpTableSpec;

/// Table provider that exposes one manifest-defined HTTP table to `DataFusion`.
pub(crate) struct HttpSourceTableProvider {
    backend: HttpSourceClient,
    source_schema: String,
    table: HttpTableSpec,
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
        Ok(Self {
            backend,
            source_schema,
            table,
            schema,
        })
    }
}

#[derive(Debug)]
struct HttpFetchPlan {
    backend: HttpSourceClient,
    table: Arc<HttpTableSpec>,
    filters: HashMap<String, String>,
    limit: Option<usize>,
}

#[async_trait]
impl RowFetcher for HttpFetchPlan {
    async fn fetch(&self) -> Result<Vec<Value>> {
        self.backend
            .fetch(self.table.as_ref(), &self.filters, self.limit)
            .await
    }
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

        Ok(filters
            .iter()
            .map(|expr| classify_filter(expr, &allowed, &filter_modes))
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let filter_values = extract_filter_values(filters, self.table.filters());

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

        let fetcher = Arc::new(HttpFetchPlan {
            backend: self.backend.clone(),
            table: Arc::new(self.table.clone()),
            filters: filter_values.clone(),
            limit,
        });

        let schema = self.schema.clone();
        let table = self.table.clone();
        let filters_for_convert = filter_values;
        let converter = Arc::new(move |items: &[Value]| {
            convert_items(table.columns(), schema.clone(), &filters_for_convert, items)
        });

        let exec = JsonExec::new(
            &self.source_schema,
            self.table.name(),
            self.schema.clone(),
            fetcher,
            converter,
            projection.cloned(),
        )?;

        Ok(Arc::new(exec))
    }
}

fn classify_filter(
    expr: &Expr,
    allowed: &HashSet<&str>,
    filter_modes: &HashMap<&str, FilterMode>,
) -> TableProviderFilterPushDown {
    if let Expr::Column(col) = expr
        && allowed.contains(col.name())
    {
        return TableProviderFilterPushDown::Exact;
    }
    if let Expr::Not(inner) = expr
        && let Expr::Column(col) = inner.as_ref()
        && allowed.contains(col.name())
    {
        return TableProviderFilterPushDown::Exact;
    }
    if let Expr::IsTrue(inner) | Expr::IsFalse(inner) = expr
        && let Expr::Column(col) = inner.as_ref()
        && allowed.contains(col.name())
    {
        return TableProviderFilterPushDown::Exact;
    }
    if let Expr::BinaryExpr(binary) = expr
        && binary.op == Operator::Eq
        && let Expr::Column(col) = binary.left.as_ref()
        && allowed.contains(col.name())
        && literal_to_string(binary.right.as_ref()).is_some()
    {
        return TableProviderFilterPushDown::Exact;
    }
    if let Expr::Like(like) = expr
        && !like.negated
        && let Expr::Column(col) = like.expr.as_ref()
        && allowed.contains(col.name())
        && literal_to_string(like.pattern.as_ref()).is_some()
    {
        let mode = filter_modes.get(col.name()).copied().unwrap_or_default();
        if matches!(mode, FilterMode::Search | FilterMode::Contains) {
            // Inexact: the API receives the stripped search term (performance
            // win) but DataFusion keeps a residual filter to enforce exact
            // LIKE/ILIKE semantics client-side (correctness win).
            return TableProviderFilterPushDown::Inexact;
        }
    }
    TableProviderFilterPushDown::Unsupported
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
        );
        assert_eq!(pushdown, TableProviderFilterPushDown::Unsupported);
    }

    #[test]
    fn strips_wildcards_from_like_pattern() {
        let pushdown = classify_filter(
            &like_expr("q", "%deploy runbook%"),
            &allowed(&["q"]),
            &modes(&[("q", FilterMode::Search)]),
        );
        assert_eq!(pushdown, TableProviderFilterPushDown::Inexact);
    }

    #[test]
    fn search_filter_also_accepts_equality() {
        let pushdown = classify_filter(
            &binary_expr(col("query"), Operator::Eq, lit("deploy")),
            &allowed(&["query"]),
            &modes(&[("query", FilterMode::Search)]),
        );
        assert_eq!(pushdown, TableProviderFilterPushDown::Exact);
    }

    #[test]
    fn extracts_like_value_for_search_mode_filter() {
        let pushdown = classify_filter(
            &like_expr("query", "%deploy%"),
            &allowed(&["query"]),
            &modes(&[("query", FilterMode::Search)]),
        );
        assert_eq!(pushdown, TableProviderFilterPushDown::Inexact);
    }

    #[test]
    fn boolean_column_filter_pushes_down_exactly() {
        let pushdown = classify_filter(&col("descending"), &allowed(&["descending"]), &modes(&[]));
        assert_eq!(pushdown, TableProviderFilterPushDown::Exact);
    }

    #[test]
    fn negated_boolean_column_filter_pushes_down_exactly() {
        let pushdown = classify_filter(
            &col("descending").not(),
            &allowed(&["descending"]),
            &modes(&[]),
        );
        assert_eq!(pushdown, TableProviderFilterPushDown::Exact);
    }

    #[test]
    fn boolean_is_true_and_is_false_push_down_exactly() {
        for expr in [
            Expr::IsTrue(Box::new(col("descending"))),
            Expr::IsFalse(Box::new(col("descending"))),
        ] {
            let pushdown = classify_filter(&expr, &allowed(&["descending"]), &modes(&[]));
            assert_eq!(pushdown, TableProviderFilterPushDown::Exact);
        }
    }

    #[test]
    fn null_inclusive_boolean_is_predicates_are_not_pushed_down() {
        for expr in [
            Expr::IsNotTrue(Box::new(col("descending"))),
            Expr::IsNotFalse(Box::new(col("descending"))),
        ] {
            let pushdown = classify_filter(&expr, &allowed(&["descending"]), &modes(&[]));
            assert_eq!(pushdown, TableProviderFilterPushDown::Unsupported);
        }
    }
}
