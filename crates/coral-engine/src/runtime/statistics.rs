//! Runtime helpers for scan-level column statistics observations.

use std::any::Any;
use std::collections::HashSet;
use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use chrono::{SecondsFormat, Utc};
use datafusion::arrow::array::{
    Array, BooleanArray, DictionaryArray, Int64Array, LargeStringArray, RecordBatch, StringArray,
    StringViewArray, TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{
    ArrowDictionaryKeyType, DataType, Int8Type, Int16Type, Int32Type, Int64Type, SchemaRef,
    TimeUnit, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::Stream;
use opentelemetry::KeyValue;
use opentelemetry::trace::TraceContextExt as _;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::contracts::{
    ColumnSchemaSignature, ColumnStatisticsObservation, StatisticPrecision, StatisticValue,
    StatisticsObservation, StatisticsObservationScope, TableSchemaSignature,
};

const DISTINCT_COUNT_MAX_VALUES: usize = 4096;
pub(crate) const STATISTICS_OBSERVATION_EVENT_NAME: &str = "coral.statistics.observation";
pub(crate) const STATISTICS_OBSERVATION_EVENT_ATTRIBUTE: &str = "coral.statistics.observation";

/// Immutable scan metadata needed to turn Arrow batches into one observation.
#[derive(Debug, Clone)]
pub(crate) struct BatchStatisticsPlan {
    pub(crate) schema_name: String,
    pub(crate) table_name: String,
    pub(crate) source_version: Option<String>,
    pub(crate) schema_signature: TableSchemaSignature,
    pub(crate) scope: StatisticsObservationScope,
    pub(crate) precision: StatisticPrecision,
}

impl BatchStatisticsPlan {
    pub(crate) fn table_global(
        schema_name: impl Into<String>,
        table_name: impl Into<String>,
        source_version: Option<String>,
        schema_signature: TableSchemaSignature,
    ) -> Self {
        Self {
            schema_name: schema_name.into(),
            table_name: table_name.into(),
            source_version,
            schema_signature,
            scope: StatisticsObservationScope::TableGlobal,
            precision: StatisticPrecision::ObservedSample,
        }
    }

    pub(crate) fn with_scope(mut self, scope: StatisticsObservationScope) -> Self {
        self.scope = scope;
        self
    }
}

#[cfg(test)]
pub(crate) fn collect_batch_statistics(
    plan: &BatchStatisticsPlan,
    batches: &[RecordBatch],
) -> Option<StatisticsObservation> {
    if batches.is_empty() {
        return None;
    }

    let schema = batches.first()?.schema();
    let mut collector = BatchStatisticsCollector::new(plan.clone(), &schema);
    for batch in batches {
        collector.record_batch(batch);
    }
    collector.finish()
}

pub(crate) fn report_statistics_observation(observation: &StatisticsObservation) {
    let payload = match serde_json::to_string(observation) {
        Ok(payload) => payload,
        Err(error) => {
            tracing::warn!(
                detail = %error,
                "discarding statistics observation because it could not be serialized"
            );
            return;
        }
    };

    let context = tracing::Span::current().context();
    context.span().add_event(
        STATISTICS_OBSERVATION_EVENT_NAME,
        vec![KeyValue::new(
            STATISTICS_OBSERVATION_EVENT_ATTRIBUTE,
            payload,
        )],
    );
}

/// Wraps an execution plan and emits one statistics observation after all
/// output partitions finish successfully, or a conservative limited observation
/// when a parent operator stops consuming after observing only part of the scan.
pub(crate) fn observe_execution_plan(
    input: Arc<dyn ExecutionPlan>,
    plan: BatchStatisticsPlan,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(ObservingExec::new(input, plan))
}

#[derive(Debug)]
struct ObservingExec {
    input: Arc<dyn ExecutionPlan>,
    plan: BatchStatisticsPlan,
    accumulator: Arc<Mutex<PartitionedStatisticsAccumulator>>,
}

impl ObservingExec {
    fn new(input: Arc<dyn ExecutionPlan>, plan: BatchStatisticsPlan) -> Self {
        let partition_count = input.output_partitioning().partition_count().max(1);
        Self {
            input,
            plan,
            accumulator: Arc::new(Mutex::new(PartitionedStatisticsAccumulator::new(
                partition_count,
            ))),
        }
    }
}

impl DisplayAs for ObservingExec {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ObservingExec: table={}.{}",
            self.plan.schema_name, self.plan.table_name
        )
    }
}

impl ExecutionPlan for ObservingExec {
    fn name(&self) -> &'static str {
        "ObservingExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> {
        self.input.properties()
    }

    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> Result<datafusion::common::Statistics> {
        self.input.partition_statistics(partition)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "ObservingExec expected one child, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(Self::new(children.remove(0), self.plan.clone())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(ObservingStream::new(
            input,
            self.plan.clone(),
            self.accumulator.clone(),
        )))
    }
}

struct ObservingStream {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    plan: BatchStatisticsPlan,
    accumulator: Arc<Mutex<PartitionedStatisticsAccumulator>>,
    partition_finished: bool,
}

impl ObservingStream {
    fn new(
        input: SendableRecordBatchStream,
        plan: BatchStatisticsPlan,
        accumulator: Arc<Mutex<PartitionedStatisticsAccumulator>>,
    ) -> Self {
        let schema = input.schema();
        Self {
            input,
            schema,
            plan,
            accumulator,
            partition_finished: false,
        }
    }
}

impl RecordBatchStream for ObservingStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for ObservingStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                record_observed_batch(&self.accumulator, &self.plan, &batch);
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(error))) => {
                self.partition_finished = true;
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                self.partition_finished = true;
                finish_observed_partition(&self.accumulator);
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for ObservingStream {
    fn drop(&mut self) {
        if !self.partition_finished {
            finish_partially_observed_scan(&self.accumulator);
        }
    }
}

fn record_observed_batch(
    accumulator: &Mutex<PartitionedStatisticsAccumulator>,
    plan: &BatchStatisticsPlan,
    batch: &RecordBatch,
) {
    match accumulator.lock() {
        Ok(mut accumulator) => accumulator.record_batch(plan, batch),
        Err(error) => tracing::warn!(
            detail = %error,
            "discarding statistics batch because accumulator lock is poisoned"
        ),
    }
}

fn finish_observed_partition(accumulator: &Mutex<PartitionedStatisticsAccumulator>) {
    match accumulator.lock() {
        Ok(mut accumulator) => {
            if let Some(observation) = accumulator.finish_partition() {
                report_statistics_observation(&observation);
            }
        }
        Err(error) => tracing::warn!(
            detail = %error,
            "discarding statistics observation because accumulator lock is poisoned"
        ),
    }
}

fn finish_partially_observed_scan(accumulator: &Mutex<PartitionedStatisticsAccumulator>) {
    match accumulator.lock() {
        Ok(mut accumulator) => {
            if let Some(observation) = accumulator.finish_partial_scan() {
                report_statistics_observation(&observation);
            }
        }
        Err(error) => tracing::warn!(
            detail = %error,
            "discarding partial statistics observation because accumulator lock is poisoned"
        ),
    }
}

#[derive(Debug)]
struct PartitionedStatisticsAccumulator {
    remaining_partitions: usize,
    collector: Option<BatchStatisticsCollector>,
    emitted: bool,
}

impl PartitionedStatisticsAccumulator {
    fn new(partition_count: usize) -> Self {
        Self {
            remaining_partitions: partition_count,
            collector: None,
            emitted: false,
        }
    }

    fn record_batch(&mut self, plan: &BatchStatisticsPlan, batch: &RecordBatch) {
        let collector = self.collector.get_or_insert_with(|| {
            let schema = batch.schema();
            BatchStatisticsCollector::new(plan.clone(), &schema)
        });
        collector.record_batch(batch);
    }

    fn finish_partition(&mut self) -> Option<StatisticsObservation> {
        self.remaining_partitions = self.remaining_partitions.saturating_sub(1);
        if self.remaining_partitions == 0 && !self.emitted {
            self.emitted = true;
            return self
                .collector
                .take()
                .and_then(BatchStatisticsCollector::finish);
        }
        None
    }

    fn finish_partial_scan(&mut self) -> Option<StatisticsObservation> {
        if self.emitted {
            return None;
        }
        if !self
            .collector
            .as_ref()
            .is_some_and(BatchStatisticsCollector::has_rows)
        {
            return None;
        }

        self.emitted = true;
        let mut collector = self.collector.take()?;
        collector.plan.scope = StatisticsObservationScope::Limited;
        collector.finish()
    }
}

#[derive(Debug)]
pub(crate) struct BatchStatisticsCollector {
    plan: BatchStatisticsPlan,
    sample_count: u64,
    columns: Vec<ColumnStatisticsCollector>,
}

impl BatchStatisticsCollector {
    pub(crate) fn new(plan: BatchStatisticsPlan, schema: &SchemaRef) -> Self {
        let columns = schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(index, field)| {
                let signature = column_signature(&plan, field.name())?;
                if signature.is_virtual || signature.is_required_filter {
                    return None;
                }
                Some(ColumnStatisticsCollector {
                    index,
                    column_name: field.name().clone(),
                    null_count: 0,
                    distinct: DistinctCollector::for_signature(signature, field.data_type()),
                })
            })
            .collect();

        Self {
            plan,
            sample_count: 0,
            columns,
        }
    }

    pub(crate) fn record_batch(&mut self, batch: &RecordBatch) {
        self.sample_count = self
            .sample_count
            .saturating_add(u64::try_from(batch.num_rows()).unwrap_or(u64::MAX));
        for column in &mut self.columns {
            column.record_batch(batch);
        }
    }

    pub(crate) fn finish(self) -> Option<StatisticsObservation> {
        if self.columns.is_empty() {
            return None;
        }

        let columns = self
            .columns
            .into_iter()
            .map(|column| column.finish(self.sample_count, self.plan.precision))
            .collect();

        Some(StatisticsObservation {
            schema_name: self.plan.schema_name,
            table_name: self.plan.table_name,
            source_version: self.plan.source_version,
            schema_signature: self.plan.schema_signature,
            scope: self.plan.scope,
            observed_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
            columns,
        })
    }

    fn has_rows(&self) -> bool {
        self.sample_count > 0
    }
}

#[derive(Debug)]
struct ColumnStatisticsCollector {
    index: usize,
    column_name: String,
    null_count: u64,
    distinct: DistinctCollector,
}

impl ColumnStatisticsCollector {
    fn record_batch(&mut self, batch: &RecordBatch) {
        let Some(array) = batch.columns().get(self.index) else {
            return;
        };
        self.null_count = self
            .null_count
            .saturating_add(u64::try_from(array.null_count()).unwrap_or(u64::MAX));
        self.distinct.record_array(array.as_ref());
    }

    fn finish(
        self,
        sample_count: u64,
        precision: StatisticPrecision,
    ) -> ColumnStatisticsObservation {
        ColumnStatisticsObservation {
            column_name: self.column_name,
            sample_count,
            null_count: Some(StatisticValue {
                value: self.null_count,
                precision,
            }),
            approx_distinct_count: self
                .distinct
                .finish()
                .map(|value| StatisticValue { value, precision }),
        }
    }
}

fn column_signature<'a>(
    plan: &'a BatchStatisticsPlan,
    column_name: &str,
) -> Option<&'a ColumnSchemaSignature> {
    plan.schema_signature
        .columns
        .iter()
        .find(|column| column.name == column_name)
}

#[derive(Debug)]
enum DistinctCollector {
    Utf8(HashSet<String>),
    Int64(HashSet<i64>),
    TimestampMicros(HashSet<i64>),
    Bool(HashSet<bool>),
    Unsupported,
    Capped,
}

impl DistinctCollector {
    fn for_signature(signature: &ColumnSchemaSignature, arrow_data_type: &DataType) -> Self {
        if signature.data_type == "Json" {
            return Self::Unsupported;
        }

        match arrow_data_type {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Self::Utf8(HashSet::new()),
            DataType::Dictionary(_, value_type) if is_string_like(value_type.as_ref()) => {
                Self::Utf8(HashSet::new())
            }
            DataType::Int64 => Self::Int64(HashSet::new()),
            DataType::Boolean => Self::Bool(HashSet::new()),
            DataType::Timestamp(TimeUnit::Microsecond, _) => Self::TimestampMicros(HashSet::new()),
            _ => Self::Unsupported,
        }
    }

    fn record_array(&mut self, array: &dyn Array) {
        let result = match self {
            Self::Utf8(values) => record_utf8_values(values, array),
            Self::Int64(values) => record_int64_values::<Int64Array>(values, array),
            Self::TimestampMicros(values) => {
                record_int64_values::<TimestampMicrosecondArray>(values, array)
            }
            Self::Bool(values) => record_bool_values(values, array),
            Self::Unsupported | Self::Capped => DistinctRecordResult::Ok,
        };

        match result {
            DistinctRecordResult::Ok => {}
            DistinctRecordResult::Unsupported => *self = Self::Unsupported,
            DistinctRecordResult::Capped => *self = Self::Capped,
        }
    }

    fn finish(self) -> Option<u64> {
        match self {
            Self::Utf8(values) => Some(u64::try_from(values.len()).unwrap_or(u64::MAX)),
            Self::Int64(values) | Self::TimestampMicros(values) => {
                Some(u64::try_from(values.len()).unwrap_or(u64::MAX))
            }
            Self::Bool(values) => Some(u64::try_from(values.len()).unwrap_or(u64::MAX)),
            Self::Unsupported | Self::Capped => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DistinctRecordResult {
    Ok,
    Unsupported,
    Capped,
}

fn record_utf8_values(values: &mut HashSet<String>, array: &dyn Array) -> DistinctRecordResult {
    match array.data_type() {
        DataType::Utf8 => {
            let Some(array) = array.as_any().downcast_ref::<StringArray>() else {
                return DistinctRecordResult::Unsupported;
            };
            record_string_array_values(values, array)
        }
        DataType::LargeUtf8 => {
            let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() else {
                return DistinctRecordResult::Unsupported;
            };
            record_large_string_array_values(values, array)
        }
        DataType::Utf8View => {
            let Some(array) = array.as_any().downcast_ref::<StringViewArray>() else {
                return DistinctRecordResult::Unsupported;
            };
            record_string_view_array_values(values, array)
        }
        DataType::Dictionary(key_type, value_type) if is_string_like(value_type.as_ref()) => {
            match key_type.as_ref() {
                DataType::Int8 => record_dictionary_utf8_values::<Int8Type>(values, array),
                DataType::Int16 => record_dictionary_utf8_values::<Int16Type>(values, array),
                DataType::Int32 => record_dictionary_utf8_values::<Int32Type>(values, array),
                DataType::Int64 => record_dictionary_utf8_values::<Int64Type>(values, array),
                DataType::UInt8 => record_dictionary_utf8_values::<UInt8Type>(values, array),
                DataType::UInt16 => record_dictionary_utf8_values::<UInt16Type>(values, array),
                DataType::UInt32 => record_dictionary_utf8_values::<UInt32Type>(values, array),
                DataType::UInt64 => record_dictionary_utf8_values::<UInt64Type>(values, array),
                _ => DistinctRecordResult::Unsupported,
            }
        }
        _ => DistinctRecordResult::Unsupported,
    }
}

fn is_string_like(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn record_string_array_values(
    values: &mut HashSet<String>,
    array: &StringArray,
) -> DistinctRecordResult {
    record_string_values(values, array.len(), |row| {
        array.is_valid(row).then(|| array.value(row))
    })
}

fn record_large_string_array_values(
    values: &mut HashSet<String>,
    array: &LargeStringArray,
) -> DistinctRecordResult {
    record_string_values(values, array.len(), |row| {
        array.is_valid(row).then(|| array.value(row))
    })
}

fn record_string_view_array_values(
    values: &mut HashSet<String>,
    array: &StringViewArray,
) -> DistinctRecordResult {
    record_string_values(values, array.len(), |row| {
        array.is_valid(row).then(|| array.value(row))
    })
}

fn record_string_values<'a>(
    values: &mut HashSet<String>,
    len: usize,
    mut value_at: impl FnMut(usize) -> Option<&'a str>,
) -> DistinctRecordResult {
    for row in 0..len {
        if let Some(value) = value_at(row) {
            values.insert(value.to_string());
            if values.len() > DISTINCT_COUNT_MAX_VALUES {
                return DistinctRecordResult::Capped;
            }
        }
    }
    DistinctRecordResult::Ok
}

fn record_dictionary_utf8_values<K>(
    values: &mut HashSet<String>,
    array: &dyn Array,
) -> DistinctRecordResult
where
    K: ArrowDictionaryKeyType,
{
    let Some(array) = array.as_any().downcast_ref::<DictionaryArray<K>>() else {
        return DistinctRecordResult::Unsupported;
    };
    let dictionary_values = array.values();

    for row in 0..array.len() {
        let Some(key) = array.key(row) else {
            continue;
        };
        if let Some(value) = string_value_at(dictionary_values.as_ref(), key) {
            values.insert(value.to_string());
            if values.len() > DISTINCT_COUNT_MAX_VALUES {
                return DistinctRecordResult::Capped;
            }
        }
    }
    DistinctRecordResult::Ok
}

fn string_value_at(array: &dyn Array, index: usize) -> Option<&str> {
    match array.data_type() {
        DataType::Utf8 => {
            let array = array.as_any().downcast_ref::<StringArray>()?;
            (index < array.len() && array.is_valid(index)).then(|| array.value(index))
        }
        DataType::LargeUtf8 => {
            let array = array.as_any().downcast_ref::<LargeStringArray>()?;
            (index < array.len() && array.is_valid(index)).then(|| array.value(index))
        }
        DataType::Utf8View => {
            let array = array.as_any().downcast_ref::<StringViewArray>()?;
            (index < array.len() && array.is_valid(index)).then(|| array.value(index))
        }
        _ => None,
    }
}

fn record_int64_values<T>(values: &mut HashSet<i64>, array: &dyn Array) -> DistinctRecordResult
where
    T: Array + 'static,
    for<'a> &'a T: Int64Values,
{
    let Some(array) = array.as_any().downcast_ref::<T>() else {
        return DistinctRecordResult::Unsupported;
    };
    for row in 0..array.len() {
        if array.is_valid(row) {
            values.insert(array.int64_value(row));
            if values.len() > DISTINCT_COUNT_MAX_VALUES {
                return DistinctRecordResult::Capped;
            }
        }
    }
    DistinctRecordResult::Ok
}

trait Int64Values {
    fn int64_value(self, index: usize) -> i64;
}

impl Int64Values for &Int64Array {
    fn int64_value(self, index: usize) -> i64 {
        self.value(index)
    }
}

impl Int64Values for &TimestampMicrosecondArray {
    fn int64_value(self, index: usize) -> i64 {
        self.value(index)
    }
}

fn record_bool_values(values: &mut HashSet<bool>, array: &dyn Array) -> DistinctRecordResult {
    let Some(array) = array.as_any().downcast_ref::<BooleanArray>() else {
        return DistinctRecordResult::Unsupported;
    };
    for row in 0..array.len() {
        if array.is_valid(row) {
            values.insert(array.value(row));
            if values.len() > DISTINCT_COUNT_MAX_VALUES {
                return DistinctRecordResult::Capped;
            }
        }
    }
    DistinctRecordResult::Ok
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        DictionaryArray, Float64Array, Int64Array, StringArray, StringViewArray, UInt16Array,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema, UInt16Type};

    use super::{BatchStatisticsPlan, DISTINCT_COUNT_MAX_VALUES, collect_batch_statistics};
    use crate::contracts::{
        ColumnSchemaSignature, StatisticsObservationScope, TableSchemaSignature,
    };

    fn signature() -> TableSchemaSignature {
        TableSchemaSignature {
            columns: vec![
                ColumnSchemaSignature {
                    name: "id".to_string(),
                    data_type: "Int64".to_string(),
                    nullable: false,
                    is_virtual: false,
                    is_required_filter: false,
                },
                ColumnSchemaSignature {
                    name: "name".to_string(),
                    data_type: "Utf8".to_string(),
                    nullable: true,
                    is_virtual: false,
                    is_required_filter: false,
                },
                ColumnSchemaSignature {
                    name: "score".to_string(),
                    data_type: "Float64".to_string(),
                    nullable: true,
                    is_virtual: false,
                    is_required_filter: false,
                },
                ColumnSchemaSignature {
                    name: "payload".to_string(),
                    data_type: "Json".to_string(),
                    nullable: true,
                    is_virtual: false,
                    is_required_filter: false,
                },
                ColumnSchemaSignature {
                    name: "filter".to_string(),
                    data_type: "Utf8".to_string(),
                    nullable: true,
                    is_virtual: true,
                    is_required_filter: true,
                },
            ],
            required_filters: vec!["filter".to_string()],
        }
    }

    #[test]
    fn collects_null_and_distinct_counts_for_supported_types() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
            Field::new("payload", DataType::Utf8, true),
            Field::new("filter", DataType::Utf8, true),
        ]));
        let batch = datafusion::arrow::array::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 3])),
                Arc::new(StringArray::from(vec![
                    Some("alpha"),
                    Some("beta"),
                    None,
                    Some("alpha"),
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(1.0),
                    Some(2.0),
                    None,
                    Some(2.0),
                ])),
                Arc::new(StringArray::from(vec![
                    Some(r#"{"kind":"alpha"}"#),
                    Some(r#"{"kind":"beta"}"#),
                    None,
                    Some(r#"{"kind":"alpha"}"#),
                ])),
                Arc::new(StringArray::from(vec![
                    Some("x"),
                    Some("x"),
                    Some("x"),
                    Some("x"),
                ])),
            ],
        )
        .expect("batch");
        let plan =
            BatchStatisticsPlan::table_global("local", "events", Some("0.1.0".into()), signature());

        let observation = collect_batch_statistics(&plan, &[batch]).expect("observation");

        assert_eq!(observation.scope, StatisticsObservationScope::TableGlobal);
        assert_eq!(observation.columns.len(), 4);
        let by_name = observation
            .columns
            .iter()
            .map(|column| (column.column_name.as_str(), column))
            .collect::<std::collections::HashMap<_, _>>();
        let id = by_name.get("id").expect("id stats");
        let name = by_name.get("name").expect("name stats");
        let score = by_name.get("score").expect("score stats");
        let payload = by_name.get("payload").expect("payload stats");
        assert_eq!(id.approx_distinct_count.as_ref().unwrap().value, 3);
        assert_eq!(name.null_count.as_ref().unwrap().value, 1);
        assert_eq!(name.approx_distinct_count.as_ref().unwrap().value, 2);
        assert!(score.approx_distinct_count.is_none());
        assert_eq!(payload.null_count.as_ref().unwrap().value, 1);
        assert!(payload.approx_distinct_count.is_none());
        assert!(!by_name.contains_key("filter"));
    }

    #[test]
    fn high_cardinality_distinct_counts_are_capped() {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
        let values = (0..=DISTINCT_COUNT_MAX_VALUES)
            .map(|index| Some(format!("value-{index}")))
            .collect::<Vec<_>>();
        let batch = datafusion::arrow::array::RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(values))],
        )
        .expect("batch");
        let signature = TableSchemaSignature {
            columns: vec![ColumnSchemaSignature {
                name: "name".to_string(),
                data_type: "Utf8".to_string(),
                nullable: true,
                is_virtual: false,
                is_required_filter: false,
            }],
            required_filters: Vec::new(),
        };
        let plan = BatchStatisticsPlan::table_global("local", "events", None, signature);

        let observation = collect_batch_statistics(&plan, &[batch]).expect("observation");

        let name = observation
            .columns
            .iter()
            .find(|column| column.column_name == "name")
            .expect("name stats");
        assert_eq!(name.null_count.as_ref().expect("null count").value, 0);
        assert!(name.approx_distinct_count.is_none());
    }

    #[test]
    fn utf8_view_distinct_counts_are_collected() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "name",
            DataType::Utf8View,
            true,
        )]));
        let batch = datafusion::arrow::array::RecordBatch::try_new(
            schema,
            vec![Arc::new(StringViewArray::from(vec![
                Some("alpha"),
                Some("beta"),
                None,
                Some("alpha"),
            ]))],
        )
        .expect("batch");
        let signature = TableSchemaSignature {
            columns: vec![ColumnSchemaSignature {
                name: "name".to_string(),
                data_type: "Utf8".to_string(),
                nullable: true,
                is_virtual: false,
                is_required_filter: false,
            }],
            required_filters: Vec::new(),
        };
        let plan = BatchStatisticsPlan::table_global("local", "events", None, signature);

        let observation = collect_batch_statistics(&plan, &[batch]).expect("observation");

        let name = observation
            .columns
            .iter()
            .find(|column| column.column_name == "name")
            .expect("name stats");
        assert_eq!(name.null_count.as_ref().expect("null count").value, 1);
        assert_eq!(
            name.approx_distinct_count
                .as_ref()
                .expect("distinct count")
                .value,
            2
        );
    }

    #[test]
    fn dictionary_utf8_distinct_counts_are_capped() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "name",
            DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
            true,
        )]));
        let keys = UInt16Array::from(
            (0..=DISTINCT_COUNT_MAX_VALUES)
                .map(|index| u16::try_from(index).expect("test value fits in u16"))
                .collect::<Vec<_>>(),
        );
        let values = Arc::new(StringArray::from(
            (0..=DISTINCT_COUNT_MAX_VALUES)
                .map(|index| format!("value-{index}"))
                .collect::<Vec<_>>(),
        ));
        let names = Arc::new(DictionaryArray::<UInt16Type>::try_new(keys, values).expect("dict"));
        let batch =
            datafusion::arrow::array::RecordBatch::try_new(schema, vec![names]).expect("batch");
        let signature = TableSchemaSignature {
            columns: vec![ColumnSchemaSignature {
                name: "name".to_string(),
                data_type: "Utf8".to_string(),
                nullable: true,
                is_virtual: false,
                is_required_filter: false,
            }],
            required_filters: Vec::new(),
        };
        let plan = BatchStatisticsPlan::table_global("local", "events", None, signature);

        let observation = collect_batch_statistics(&plan, &[batch]).expect("observation");

        let name = observation
            .columns
            .iter()
            .find(|column| column.column_name == "name")
            .expect("name stats");
        assert_eq!(name.null_count.as_ref().expect("null count").value, 0);
        assert!(name.approx_distinct_count.is_none());
    }
}
