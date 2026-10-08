//! Helpers for accounting Coral-owned retained memory in `DataFusion` pools.

use arrow::array::RecordBatch;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_plan::RecordBatchStream;
use futures::Stream;
use serde_json::Value;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Memory reservation wrapper for one execution that retains Coral-owned data.
#[derive(Debug)]
pub(crate) struct RetainedMemory {
    reservation: MemoryReservation,
}

impl RetainedMemory {
    /// Registers a retained-memory consumer for one operator execution.
    pub(crate) fn for_operator(context: &TaskContext, consumer_name: String) -> Self {
        Self::new(MemoryConsumer::new(consumer_name).register(context.memory_pool()))
    }

    /// Builds a retained-memory helper from an existing `DataFusion` reservation.
    pub(crate) fn new(reservation: MemoryReservation) -> Self {
        Self { reservation }
    }

    /// Reserves a raw byte count before retaining memory.
    pub(crate) fn try_reserve_bytes(&self, bytes: usize) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        self.reservation.try_grow(bytes)
    }

    /// Reserves the Arrow array memory retained by a record batch.
    pub(crate) fn try_reserve_record_batch(&self, batch: &RecordBatch) -> Result<()> {
        self.try_reserve_bytes(batch.get_array_memory_size())
    }

    /// Reserves the deterministic retained-memory estimate for JSON rows.
    pub(crate) fn try_reserve_json_rows(&self, rows: &[Value]) -> Result<()> {
        self.try_reserve_bytes(json_rows_retained_size(rows))
    }

    /// Releases bytes that are no longer retained.
    pub(crate) fn try_shrink_bytes(&self, bytes: usize) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        self.reservation.try_shrink(bytes).map(|_| ())
    }

    /// Adjusts a prior reservation to the actual retained byte count.
    pub(crate) fn reconcile_reserved_bytes(&self, reserved: usize, actual: usize) -> Result<()> {
        match actual.cmp(&reserved) {
            std::cmp::Ordering::Greater => self.try_reserve_bytes(actual - reserved),
            std::cmp::Ordering::Less => self.try_shrink_bytes(reserved - actual),
            std::cmp::Ordering::Equal => Ok(()),
        }
    }

    /// Creates a separate empty reservation under the same `DataFusion` consumer.
    pub(crate) fn new_empty(&self) -> Self {
        Self::new(self.reservation.new_empty())
    }

    /// Returns currently reserved bytes for tests and diagnostics.
    #[cfg(test)]
    pub(crate) fn reserved(&self) -> usize {
        self.reservation.size()
    }
}

pub(crate) fn json_rows_retained_size(rows: &[Value]) -> usize {
    std::mem::size_of::<Vec<Value>>()
        .saturating_add(rows.len().saturating_mul(std::mem::size_of::<Value>()))
        .saturating_add(rows.iter().map(json_value_heap_size).sum())
}

/// Retained Arrow batches paired with the reservation that accounts for them.
#[derive(Debug)]
pub(crate) struct RetainedRecordBatches {
    batches: Vec<RecordBatch>,
    memory: RetainedMemory,
}

impl RetainedRecordBatches {
    /// Builds a retained Arrow batch collection.
    pub(crate) fn new(memory: RetainedMemory) -> Self {
        Self {
            batches: Vec::new(),
            memory,
        }
    }

    /// Retains one Arrow batch whose memory has already been reserved.
    pub(crate) fn push_reserved(&mut self, batch: RecordBatch) {
        self.batches.push(batch);
    }

    /// Returns the reservation backing this retained collection.
    pub(crate) fn memory(&self) -> &RetainedMemory {
        &self.memory
    }

    /// Converts retained batches into a stream that owns the reservation.
    pub(crate) fn into_stream(
        self,
        schema: arrow::datatypes::SchemaRef,
    ) -> RetainedRecordBatchStream {
        RetainedRecordBatchStream {
            schema,
            batches: self.batches.into_iter(),
            _memory: self.memory,
        }
    }
}

/// Stream that keeps output memory reserved until batches are emitted or dropped.
pub(crate) struct RetainedRecordBatchStream {
    schema: arrow::datatypes::SchemaRef,
    batches: std::vec::IntoIter<RecordBatch>,
    _memory: RetainedMemory,
}

impl Stream for RetainedRecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.batches.next().map(Ok))
    }
}

impl RecordBatchStream for RetainedRecordBatchStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        std::sync::Arc::clone(&self.schema)
    }
}

fn json_value_heap_size(value: &Value) -> usize {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) => 0,
        Value::String(value) => value.capacity(),
        Value::Array(values) => values
            .capacity()
            .saturating_mul(std::mem::size_of::<Value>())
            .saturating_add(values.iter().map(json_value_heap_size).sum()),
        Value::Object(values) => std::mem::size_of::<serde_json::Map<String, Value>>()
            .saturating_add(
                values
                    .len()
                    .saturating_mul(std::mem::size_of::<(String, Value)>()),
            )
            .saturating_add(
                values
                    .iter()
                    .map(|(key, value)| key.capacity().saturating_add(json_value_heap_size(value)))
                    .sum::<usize>(),
            ),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
    use serde_json::json;

    use super::{RetainedMemory, json_rows_retained_size};

    #[test]
    fn json_retained_size_accounts_for_rows_and_containers() {
        let rows = vec![
            json!({
                "id": "abc",
                "active": true,
                "nested": [null, 12, "xy"]
            }),
            json!({}),
            json!([]),
            json!(null),
        ];

        let minimum = std::mem::size_of::<Vec<serde_json::Value>>()
            + rows.len() * std::mem::size_of::<serde_json::Value>();

        assert!(json_rows_retained_size(&rows) >= minimum);
        assert!(json_rows_retained_size(&[json!({})]) > 0);
        assert!(json_rows_retained_size(&[json!([])]) > 0);
        assert!(json_rows_retained_size(&[json!(null)]) > 0);
    }

    #[test]
    fn retained_memory_reserves_against_pool() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(8));
        let reservation = MemoryConsumer::new("test").register(&pool);
        let memory = RetainedMemory::new(reservation);

        memory
            .try_reserve_bytes(4)
            .expect("reservation below limit should succeed");
        assert_eq!(memory.reserved(), 4);

        let error = memory
            .try_reserve_json_rows(&[json!("01234567")])
            .expect_err("reservation above limit should fail");

        assert!(
            error.to_string().contains("Resources exhausted"),
            "unexpected error: {error}"
        );
        assert_eq!(memory.reserved(), 4);
    }
}
