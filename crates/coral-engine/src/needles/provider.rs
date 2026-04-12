//! Wraps a `TableProvider` to union benchmark needle rows with live provider data.
//!
//! Needle batches are built from the YAML needles file at registration time and
//! passed to [`NeedleTableProvider`] as pre-built Arrow record batches.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::datasource::{MemTable, TableProvider, TableType};
use datafusion::error::Result;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::union::UnionExec;

use super::error::NeedleError;

/// Converts grouped JSON row values into Arrow record batches against a target
/// schema.
///
/// Each JSON value is re-serialized as a single NDJSON line and decoded through
/// `arrow::json::ReaderBuilder` with the target schema. This round-trip
/// leverages Arrow's built-in JSON-to-Arrow type coercion rather than requiring
/// manual array construction. Columns are then reconciled (cast, null-fill,
/// drop extras) for safety.
pub(crate) fn build_needle_batches(
    rows: &[serde_json::Value],
    target_schema: &SchemaRef,
) -> std::result::Result<Vec<RecordBatch>, NeedleError> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut ndjson = Vec::new();
    for row in rows {
        serde_json::to_writer(&mut ndjson, row)
            .map_err(|e| NeedleError::JsonConversion(e.to_string()))?;
        ndjson.push(b'\n');
    }

    let reader = datafusion::arrow::json::ReaderBuilder::new(target_schema.clone())
        .build(std::io::Cursor::new(ndjson))
        .map_err(|e| NeedleError::JsonConversion(e.to_string()))?;

    let mut batches = Vec::new();
    for batch_result in reader {
        let batch = batch_result.map_err(|e| NeedleError::JsonConversion(e.to_string()))?;
        let columns = reconcile_columns(&batch, target_schema)?;
        batches.push(RecordBatch::try_new(target_schema.clone(), columns)?);
    }

    Ok(batches)
}

/// A `TableProvider` that unions real provider data with benchmark needle rows.
///
/// `supports_filters_pushdown` delegates to the inner provider but caps
/// `Exact` at `Inexact`. This ensures `DataFusion` always inserts a
/// `FilterExec` above the union, because the needle `MemTable` does not push
/// filters down on its own. Without this, needle rows that do not match the
/// `WHERE` clause could leak into query results.
pub(crate) struct NeedleTableProvider {
    inner: Arc<dyn TableProvider>,
    needle_batches: Vec<RecordBatch>,
}

impl std::fmt::Debug for NeedleTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NeedleTableProvider")
            .finish_non_exhaustive()
    }
}

impl NeedleTableProvider {
    pub(crate) fn new(inner: Arc<dyn TableProvider>, needle_batches: Vec<RecordBatch>) -> Self {
        Self {
            inner,
            needle_batches,
        }
    }
}

#[async_trait]
impl TableProvider for NeedleTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    /// Caps `Exact` pushdown to `Inexact` so `DataFusion` inserts a `FilterExec`
    /// above the union, preventing needle rows from bypassing `WHERE` clauses.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let inner = self.inner.supports_filters_pushdown(filters)?;
        Ok(inner
            .into_iter()
            .map(|pd| match pd {
                TableProviderFilterPushDown::Exact => TableProviderFilterPushDown::Inexact,
                other => other,
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let live = self.inner.scan(state, projection, filters, limit).await?;

        if self.needle_batches.is_empty() {
            return Ok(live);
        }

        let needle_table = MemTable::try_new(self.schema(), vec![self.needle_batches.clone()])?;

        // Forward projection and filters but NOT limit to the needle side.
        // The limit applies to the union output; applying it to needles alone
        // could drop valid needle rows before the union merges them.
        let needle = needle_table.scan(state, projection, filters, None).await?;

        // Both sides share the same schema by construction (guaranteed by
        // build_needle_batches + reconcile_columns), so try_new's compatibility
        // check would be redundant.
        // TODO: migrate to UnionExec::try_new when upgrading DataFusion.
        #[allow(
            deprecated,
            reason = "Both sides share the same schema by construction; \
                      UnionExec::try_new adds a redundant compatibility check."
        )]
        Ok(Arc::new(UnionExec::new(vec![live, needle])))
    }
}

/// Reorder and cast columns from a source batch to match the target schema.
///
/// Columns present in the target but missing from the source are filled with
/// nulls. Extra source columns are dropped because the needle rows must match
/// the live provider's schema exactly; the target schema is the source of truth.
fn reconcile_columns(
    batch: &RecordBatch,
    target_schema: &SchemaRef,
) -> std::result::Result<Vec<Arc<dyn datafusion::arrow::array::Array>>, NeedleError> {
    use datafusion::arrow::compute::cast;

    let mut columns = Vec::with_capacity(target_schema.fields().len());
    for field in target_schema.fields() {
        if let Ok(col_idx) = batch.schema().index_of(field.name()) {
            let col = batch.column(col_idx);
            if col.data_type() == field.data_type() {
                columns.push(col.clone());
            } else {
                let casted =
                    cast(col, field.data_type()).map_err(|source| NeedleError::CastFailed {
                        column: field.name().clone(),
                        from: col.data_type().clone(),
                        to: field.data_type().clone(),
                        source,
                    })?;
                columns.push(casted);
            }
        } else {
            columns.push(datafusion::arrow::array::new_null_array(
                field.data_type(),
                batch.num_rows(),
            ));
        }
    }
    Ok(columns)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::{MemTable, TableProvider};
    use datafusion::logical_expr::TableProviderFilterPushDown;
    use datafusion::prelude::SessionContext;

    use super::{NeedleTableProvider, build_needle_batches};

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, true),
        ]))
    }

    fn make_provider(
        live_ids: &[&str],
        live_values: &[i32],
        needle_ids: &[&str],
        needle_values: &[i32],
    ) -> NeedleTableProvider {
        let schema = test_schema();
        let live = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(live_ids.to_vec())),
                Arc::new(Int32Array::from(live_values.to_vec())),
            ],
        )
        .unwrap();

        let needle = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(needle_ids.to_vec())),
                Arc::new(Int32Array::from(needle_values.to_vec())),
            ],
        )
        .unwrap();

        NeedleTableProvider::new(
            Arc::new(MemTable::try_new(schema, vec![vec![live]]).unwrap()),
            vec![needle],
        )
    }

    async fn query_ids(ctx: &SessionContext, sql: &str) -> Vec<String> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let mut ids = Vec::new();
        for batch in &batches {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..col.len() {
                ids.push(col.value(i).to_string());
            }
        }
        ids
    }

    #[tokio::test]
    async fn union_returns_both_live_and_needle_rows() {
        let provider = make_provider(&["real-1"], &[100], &["needle-1"], &[999]);
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(provider)).unwrap();

        let ids = query_ids(&ctx, "SELECT id FROM t ORDER BY id").await;
        assert_eq!(ids, vec!["needle-1", "real-1"]);
    }

    #[tokio::test]
    async fn where_clause_excludes_non_matching_needle_rows() {
        let provider = make_provider(
            &["real-1", "real-2"],
            &[100, 200],
            &["needle-1", "needle-2"],
            &[999, 1],
        );
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(provider)).unwrap();

        let ids = query_ids(&ctx, "SELECT id FROM t WHERE value > 50 ORDER BY id").await;
        assert_eq!(ids, vec!["needle-1", "real-1", "real-2"]);
    }

    #[tokio::test]
    async fn empty_needle_batches_returns_live_only() {
        let schema = test_schema();
        let live = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["real-1"])),
                Arc::new(Int32Array::from(vec![100])),
            ],
        )
        .unwrap();
        let provider = NeedleTableProvider::new(
            Arc::new(MemTable::try_new(schema, vec![vec![live]]).unwrap()),
            vec![],
        );
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(provider)).unwrap();

        let ids = query_ids(&ctx, "SELECT id FROM t ORDER BY id").await;
        assert_eq!(ids, vec!["real-1"]);
    }

    #[test]
    fn build_needle_batches_from_json_values() {
        let schema = test_schema();
        let rows = vec![
            serde_json::json!({"id": "needle-1", "value": 42}),
            serde_json::json!({"id": "needle-2", "value": 99}),
        ];
        let batches = build_needle_batches(&rows, &schema).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn build_needle_batches_null_fills_missing_columns() {
        let schema = test_schema();
        // Only provide "id", omit "value"
        let rows = vec![serde_json::json!({"id": "needle-1"})];
        let batches = build_needle_batches(&rows, &schema).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        let value_col = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(value_col.is_null(0));
    }

    #[test]
    fn build_needle_batches_empty_rows_returns_empty() {
        let schema = test_schema();
        let batches = build_needle_batches(&[], &schema).unwrap();
        assert!(batches.is_empty());
    }

    #[tokio::test]
    async fn supports_filters_pushdown_caps_exact_to_inexact() {
        let provider = make_provider(&["real-1"], &[100], &["needle-1"], &[999]);
        let expr = datafusion::logical_expr::col("id").eq(datafusion::logical_expr::lit("x"));
        let result = provider.supports_filters_pushdown(&[&expr]).unwrap();
        assert!(
            result
                .iter()
                .all(|pd| *pd != TableProviderFilterPushDown::Exact),
            "Exact should be capped to Inexact; got: {result:?}"
        );
    }
}
