//! Table-provider wrapper for live-eval row injection.

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
            .map_err(|error| NeedleError::JsonConversion(error.to_string()))?;
        ndjson.push(b'\n');
    }

    let reader = datafusion::arrow::json::ReaderBuilder::new(target_schema.clone())
        .build(std::io::Cursor::new(ndjson))
        .map_err(|error| NeedleError::JsonConversion(error.to_string()))?;

    let mut batches = Vec::new();
    for batch_result in reader {
        let batch = batch_result.map_err(|error| NeedleError::JsonConversion(error.to_string()))?;
        let columns = reconcile_columns(&batch, target_schema)?;
        batches.push(RecordBatch::try_new(target_schema.clone(), columns)?);
    }

    Ok(batches)
}

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

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let inner = self.inner.supports_filters_pushdown(filters)?;
        Ok(inner
            .into_iter()
            .map(|pushdown| match pushdown {
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
        let needle = needle_table.scan(state, projection, filters, None).await?;

        #[allow(
            deprecated,
            reason = "Both sides share the same schema by construction; \
                      UnionExec::try_new adds a redundant compatibility check."
        )]
        Ok(Arc::new(UnionExec::new(vec![live, needle])))
    }
}

fn reconcile_columns(
    batch: &RecordBatch,
    target_schema: &SchemaRef,
) -> std::result::Result<Vec<Arc<dyn datafusion::arrow::array::Array>>, NeedleError> {
    use datafusion::arrow::compute::cast;

    let mut columns = Vec::with_capacity(target_schema.fields().len());
    for field in target_schema.fields() {
        if let Ok(col_idx) = batch.schema().index_of(field.name()) {
            let column = batch.column(col_idx);
            if column.data_type() == field.data_type() {
                columns.push(column.clone());
            } else {
                columns.push(cast(column, field.data_type()).map_err(|source| {
                    NeedleError::CastFailed {
                        column: field.name().clone(),
                        from: column.data_type().clone(),
                        to: field.data_type().clone(),
                        source,
                    }
                })?);
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
    use datafusion::arrow::array::{Array, Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    use super::*;

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
        .expect("build live batch");
        let needle = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(needle_ids.to_vec())),
                Arc::new(Int32Array::from(needle_values.to_vec())),
            ],
        )
        .expect("build needle batch");

        NeedleTableProvider::new(
            Arc::new(MemTable::try_new(schema, vec![vec![live]]).expect("mem table")),
            vec![needle],
        )
    }

    async fn query_ids(ctx: &SessionContext, sql: &str) -> Vec<String> {
        let batches = ctx
            .sql(sql)
            .await
            .expect("plan query")
            .collect()
            .await
            .expect("execute query");
        let mut ids = Vec::new();
        for batch in &batches {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("string ids");
            for index in 0..column.len() {
                ids.push(column.value(index).to_string());
            }
        }
        ids
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
        ctx.register_table("t", Arc::new(provider))
            .expect("register table");

        let ids = query_ids(&ctx, "SELECT id FROM t WHERE value > 50 ORDER BY id").await;
        assert_eq!(ids, vec!["needle-1", "real-1", "real-2"]);
    }

    #[tokio::test]
    async fn supports_filters_pushdown_caps_exact_to_inexact() {
        let provider = make_provider(&["real-1"], &[100], &["needle-1"], &[999]);
        let expr = datafusion::logical_expr::col("id").eq(datafusion::logical_expr::lit("x"));
        let result = provider
            .supports_filters_pushdown(&[&expr])
            .expect("pushdown support");
        assert!(
            result
                .iter()
                .all(|pushdown| *pushdown != TableProviderFilterPushDown::Exact)
        );
    }
}
