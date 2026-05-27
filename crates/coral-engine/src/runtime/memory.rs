//! Helpers for accounting Coral-owned retained memory in `DataFusion` pools.

use arrow::array::RecordBatch;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::RecordBatchStream;
use futures::Stream;
use serde_json::Value;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Coral-owned physical operators must declare whether they retain query data.
pub(crate) trait CoralExecutionPlan: ExecutionPlan {
    /// Describes how this operator interacts with query memory accounting.
    fn memory_behavior(&self) -> CoralMemoryBehavior;
}

/// Memory behavior declaration for Coral-owned physical operators.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CoralMemoryBehavior {
    /// Operator does not retain significant query data across poll boundaries.
    #[expect(dead_code, reason = "used by future streaming Coral execution plans")]
    Streaming,
    /// Operator retains query data and must reserve against `DataFusion`'s pool.
    RetainsMemory { consumer_name: String },
}

impl CoralMemoryBehavior {
    /// Builds retained-memory accounting for operators that retain query data.
    pub(crate) fn retained_memory(&self, context: &TaskContext) -> Option<RetainedMemory> {
        match self {
            Self::Streaming => None,
            Self::RetainsMemory { consumer_name } => {
                Some(RetainedMemory::for_operator(context, consumer_name.clone()))
            }
        }
    }
}

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

    /// Reserves the deterministic JSON payload estimate retained by rows.
    pub(crate) fn try_reserve_json_rows(&self, rows: &[Value]) -> Result<()> {
        self.try_reserve_bytes(json_rows_payload_size(rows))
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

pub(crate) fn json_rows_payload_size(rows: &[Value]) -> usize {
    rows.iter().map(json_value_payload_size).sum()
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

    /// Reserves and retains one Arrow batch.
    pub(crate) fn push(&mut self, batch: RecordBatch) -> Result<()> {
        self.memory.try_reserve_record_batch(&batch)?;
        self.batches.push(batch);
        Ok(())
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

fn json_value_payload_size(value: &Value) -> usize {
    match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Number(_) => std::mem::size_of::<serde_json::Number>(),
        Value::String(value) => value.len(),
        Value::Array(values) => values.iter().map(json_value_payload_size).sum(),
        Value::Object(values) => values
            .iter()
            .map(|(key, value)| key.len() + json_value_payload_size(value))
            .sum(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
    use serde_json::json;

    use super::{RetainedMemory, json_rows_payload_size};

    #[test]
    fn json_payload_size_is_deterministic() {
        let rows = vec![json!({
            "id": "abc",
            "active": true,
            "nested": [null, 12, "xy"]
        })];

        let expected = "id".len()
            + "abc".len()
            + "active".len()
            + 1
            + "nested".len()
            + std::mem::size_of::<serde_json::Number>()
            + "xy".len();

        assert_eq!(json_rows_payload_size(&rows), expected);
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
