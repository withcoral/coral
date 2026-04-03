//! Shared execution-plan adapter for backends that materialize rows as JSON values.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures::stream;
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
    props: PlanProperties,
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
        let props = PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

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

    fn properties(&self) -> &PlanProperties {
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
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let fetcher = self.fetcher.clone();
        let converter = self.converter.clone();
        let projected_schema = self.projected_schema.clone();
        let projection = self.projection.clone();

        let stream = stream::once(async move {
            let items = fetcher.fetch().await?;
            let batch = converter(&items)?;

            match &projection {
                Some(indices) => batch.project(indices).map_err(|error| {
                    datafusion::error::DataFusionError::ArrowError(Box::new(error), None)
                }),
                None => Ok(batch),
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            projected_schema,
            stream,
        )))
    }
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

    use super::{Converter, Fetcher, JsonExec, RowFetcher};

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
