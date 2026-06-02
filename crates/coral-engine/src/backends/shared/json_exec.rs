//! Shared execution-plan adapter for backends that materialize rows as JSON values.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures::{TryStreamExt, stream};
use serde_json::Value;

/// Fetches raw JSON rows for one logical table scan.
#[async_trait]
pub(crate) trait RowFetcher: fmt::Debug + Send + Sync {
    /// Materializes the JSON values that should be converted into one or more
    /// `RecordBatch` values.
    async fn fetch(&self) -> Result<Vec<Value>>;
}

/// Shared trait-object wrapper for a [`RowFetcher`] implementation.
pub(crate) type Fetcher = Arc<dyn RowFetcher>;
/// Converts fetched JSON rows into a projected `RecordBatch`.
pub(crate) type Converter = Arc<dyn Fn(&[Value]) -> Result<RecordBatch> + Send + Sync>;

/// Execution-plan node for backends that fetch JSON rows and convert them into
/// `Arrow` record batches.
pub(crate) struct JsonExec {
    source_name: String,
    table_name: String,
    projected_schema: SchemaRef,
    props: Arc<PlanProperties>,
    fetcher: Fetcher,
    converter: Converter,
    projection: Option<Vec<usize>>,
}

impl fmt::Debug for JsonExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsonExec")
            .field("source", &self.source_name)
            .field("table", &self.table_name)
            .finish_non_exhaustive()
    }
}

impl JsonExec {
    /// Build a `JsonExec` plan node for an `HTTP`/`JSON`-backed table scan.
    ///
    /// # Errors
    ///
    /// Returns a `DataFusionError` if the requested projection does not match
    /// the supplied schema.
    pub(crate) fn new(
        source_name: &str,
        table_name: &str,
        schema: SchemaRef,
        fetcher: Fetcher,
        converter: Converter,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let projected_schema = match &projection {
            Some(indices) => Arc::new(schema.project(indices).map_err(|error| {
                datafusion::error::DataFusionError::ArrowError(Box::new(error), None)
            })?),
            None => schema,
        };
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        Ok(Self {
            source_name: source_name.to_string(),
            table_name: table_name.to_string(),
            projected_schema,
            props,
            fetcher,
            converter,
            projection,
        })
    }
}

impl DisplayAs for JsonExec {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}Exec: table={}", self.source_name, self.table_name)
    }
}

impl ExecutionPlan for JsonExec {
    fn name(&self) -> &'static str {
        "JsonExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn partition_statistics(
        &self,
        _partition: Option<usize>,
    ) -> Result<datafusion::common::Statistics> {
        Ok(datafusion::common::Statistics::new_unknown(&self.schema()))
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let fetcher = self.fetcher.clone();
        let converter = self.converter.clone();
        let projected_schema = self.projected_schema.clone();
        let projection = self.projection.clone();
        // Emit the fetched rows in `batch_size`-row chunks rather than a single
        // batch spanning the whole result set. Each chunk's `Vec<Value>` is
        // dropped as soon as it is converted, so the heavy serde_json
        // representation is released incrementally instead of being held
        // alongside one large `RecordBatch`.
        let batch_size = context.session_config().batch_size().max(1);

        let stream = stream::once(async move {
            let items = fetcher.fetch().await?;
            let state = ChunkState {
                rows: items.into_iter(),
                converter,
                projection,
                batch_size,
                emitted: false,
            };
            Ok::<_, DataFusionError>(stream::try_unfold(state, next_projected_batch))
        })
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            projected_schema,
            stream,
        )))
    }
}

/// Streaming state for [`JsonExec`]: the remaining fetched rows plus the
/// conversion/projection context needed to emit each bounded batch.
struct ChunkState {
    rows: std::vec::IntoIter<Value>,
    converter: Converter,
    projection: Option<Vec<usize>>,
    batch_size: usize,
    emitted: bool,
}

/// Pulls up to `batch_size` rows, converts them into one projected
/// `RecordBatch`, and returns the advanced state. An empty result still yields
/// a single empty batch so the schema is carried downstream, matching the
/// previous single-batch behavior.
async fn next_projected_batch(mut state: ChunkState) -> Result<Option<(RecordBatch, ChunkState)>> {
    let chunk: Vec<Value> = state.rows.by_ref().take(state.batch_size).collect();
    if chunk.is_empty() && state.emitted {
        return Ok(None);
    }
    state.emitted = true;

    let batch = (state.converter)(&chunk)?;
    let batch = match &state.projection {
        Some(indices) => batch
            .project(indices)
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?,
        None => batch,
    };
    // `chunk` is dropped here, releasing this slice of `serde_json::Value`
    // rows before the next batch is produced.
    Ok(Some((batch, state)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use datafusion::arrow::array::Array;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::ExecutionPlan;
    use serde_json::Value;

    use super::{ChunkState, Converter, Fetcher, JsonExec, RowFetcher, next_projected_batch};

    #[derive(Debug)]
    struct NoopFetcher;

    fn noop_fetcher() -> Fetcher {
        Arc::new(NoopFetcher)
    }

    #[async_trait]
    impl RowFetcher for NoopFetcher {
        async fn fetch(&self) -> datafusion::error::Result<Vec<Value>> {
            Ok(Vec::new())
        }
    }

    fn converter_with_schema(schema: Arc<Schema>) -> Converter {
        Arc::new(move |_| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(vec![1])) as Arc<dyn Array>],
            )
            .map_err(|error| datafusion::error::DataFusionError::ArrowError(Box::new(error), None))
        })
    }

    fn int_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]))
    }

    // Builds one Int32 column "n" with one row per item, sized to the chunk so
    // per-batch row counts and ordering are observable.
    fn int_converter(schema: Arc<Schema>) -> Converter {
        Arc::new(move |items: &[Value]| {
            let values: Int32Array = items
                .iter()
                .map(|value| value.as_i64().and_then(|n| i32::try_from(n).ok()))
                .collect();
            RecordBatch::try_new(schema.clone(), vec![Arc::new(values) as Arc<dyn Array>]).map_err(
                |error| datafusion::error::DataFusionError::ArrowError(Box::new(error), None),
            )
        })
    }

    #[test]
    fn streams_rows_in_bounded_batches_preserving_order() {
        let rows: Vec<Value> = (0..5).map(Value::from).collect();
        let mut state = ChunkState {
            rows: rows.into_iter(),
            converter: int_converter(int_schema()),
            projection: None,
            batch_size: 2,
            emitted: false,
        };

        let mut batches = Vec::new();
        while let Some((batch, next)) =
            futures::executor::block_on(next_projected_batch(state)).expect("batch")
        {
            batches.push(batch);
            state = next;
        }

        // 5 rows at batch_size 2 -> [2, 2, 1], not one batch of 5.
        assert_eq!(
            batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            vec![2, 2, 1]
        );
        let observed: Vec<i32> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("int column")
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(observed, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn empty_result_yields_single_empty_batch() {
        let state = ChunkState {
            rows: Vec::new().into_iter(),
            converter: int_converter(int_schema()),
            projection: None,
            batch_size: 8,
            emitted: false,
        };

        let (batch, state) = futures::executor::block_on(next_projected_batch(state))
            .expect("batch")
            .expect("empty results still emit one schema-carrying batch");
        assert_eq!(batch.num_rows(), 0);
        assert!(
            futures::executor::block_on(next_projected_batch(state))
                .expect("done")
                .is_none()
        );
    }

    #[test]
    fn new_applies_projection_to_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));

        let exec = JsonExec::new(
            "provider",
            "table",
            schema.clone(),
            noop_fetcher(),
            converter_with_schema(schema),
            Some(vec![1]),
        )
        .expect("projection should succeed");

        assert_eq!(exec.schema().fields().len(), 1);
        assert_eq!(exec.schema().field(0).name(), "b");
    }

    #[test]
    fn new_rejects_out_of_bounds_projection() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));

        let err = JsonExec::new(
            "provider",
            "table",
            schema.clone(),
            noop_fetcher(),
            converter_with_schema(schema),
            Some(vec![1]),
        )
        .expect_err("invalid projection should return an error");

        assert!(matches!(
            err,
            datafusion::error::DataFusionError::ArrowError(_, _)
        ));
    }
}
