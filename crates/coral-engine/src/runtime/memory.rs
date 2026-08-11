//! Helpers for accounting Coral-owned retained memory in `DataFusion` pools.

use arrow::array::RecordBatch;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::RecordBatchStream;
use futures::Stream;
use serde_json::Value;
use std::fmt::{Debug, Display, Formatter};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};

use crate::{QueryMemoryObservation, QueryMemoryObserver, QueryMemoryOutcome};

/// Tracks `DataFusion` reservations for one top-level query execution.
pub(crate) struct QueryMemoryExecution {
    tracker: Arc<QueryMemoryTracker>,
}

impl QueryMemoryExecution {
    pub(crate) fn begin(observer: QueryMemoryObserver) -> Self {
        Self {
            tracker: Arc::new(QueryMemoryTracker {
                observer,
                state: Mutex::new(QueryMemoryState::default()),
            }),
        }
    }

    /// Replaces only the task's pool, retaining all other runtime services.
    pub(crate) fn task_context(&self, context: TaskContext) -> Result<TaskContext> {
        let runtime = context.runtime_env();
        let pool = Arc::new(QueryMemoryPool {
            inner: Arc::clone(&runtime.memory_pool),
            tracker: Arc::clone(&self.tracker),
        });
        let runtime = RuntimeEnvBuilder::from_runtime_env(runtime.as_ref())
            .with_memory_pool(pool)
            .build_arc()?;
        Ok(context.with_runtime(runtime))
    }

    pub(crate) fn finish(self, outcome: QueryMemoryOutcome) {
        self.tracker.set_outcome(outcome);
    }
}

impl Drop for QueryMemoryExecution {
    fn drop(&mut self) {
        self.tracker.set_outcome(QueryMemoryOutcome::Cancelled);
    }
}

#[derive(Default)]
struct QueryMemoryState {
    reserved_bytes: usize,
    peak_bytes: usize,
    outcome: Option<QueryMemoryOutcome>,
    finalized: bool,
}

struct QueryMemoryTracker {
    observer: QueryMemoryObserver,
    state: Mutex<QueryMemoryState>,
}

impl QueryMemoryTracker {
    fn lock(&self) -> MutexGuard<'_, QueryMemoryState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn set_outcome(&self, outcome: QueryMemoryOutcome) {
        let observation = {
            let mut state = self.lock();
            state.outcome.get_or_insert(outcome);
            Self::finalize(&mut state)
        };
        self.observe(observation);
    }

    fn finalize(state: &mut QueryMemoryState) -> Option<QueryMemoryObservation> {
        if state.finalized || state.reserved_bytes != 0 {
            return None;
        }
        let outcome = state.outcome?;
        state.finalized = true;
        Some(QueryMemoryObservation {
            datafusion_reserved_peak_bytes: state.peak_bytes,
            outcome,
        })
    }

    fn observe(&self, observation: Option<QueryMemoryObservation>) {
        if let Some(observation) = observation {
            drop(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                || {
                    (self.observer)(observation);
                },
            )));
        }
    }
}

struct QueryMemoryPool {
    inner: Arc<dyn MemoryPool>,
    tracker: Arc<QueryMemoryTracker>,
}

impl Debug for QueryMemoryPool {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QueryMemoryPool")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl Display for QueryMemoryPool {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self.inner.as_ref(), formatter)
    }
}

impl MemoryPool for QueryMemoryPool {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn register(&self, consumer: &MemoryConsumer) {
        let _state = self.tracker.lock();
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        let _state = self.tracker.lock();
        self.inner.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        let mut state = self.tracker.lock();
        self.inner.grow(reservation, additional);
        state.reserved_bytes += additional;
        state.peak_bytes = state.peak_bytes.max(state.reserved_bytes);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        let observation = {
            let mut state = self.tracker.lock();
            self.inner.shrink(reservation, shrink);
            state.reserved_bytes -= shrink;
            QueryMemoryTracker::finalize(&mut state)
        };
        self.tracker.observe(observation);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        let mut state = self.tracker.lock();
        self.inner.try_grow(reservation, additional)?;
        state.reserved_bytes += additional;
        state.peak_bytes = state.peak_bytes.max(state.reserved_bytes);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
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
    use std::sync::{Arc, Mutex, mpsc};
    use std::thread;

    use datafusion::execution::TaskContext;
    use datafusion::execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryLimit, MemoryPool, UnboundedMemoryPool,
    };
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use serde_json::json;

    use crate::{QueryMemoryObservation, QueryMemoryObserver, QueryMemoryOutcome};

    use super::{QueryMemoryExecution, RetainedMemory, json_rows_retained_size};

    fn query_memory_observations() -> (QueryMemoryObserver, Arc<Mutex<Vec<QueryMemoryObservation>>>)
    {
        let observations = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&observations);
        (
            Arc::new(move |observation| captured.lock().unwrap().push(observation)),
            observations,
        )
    }

    fn query_memory_context(pool: Arc<dyn MemoryPool>) -> TaskContext {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(pool)
            .build_arc()
            .unwrap();
        TaskContext::default().with_runtime(runtime)
    }

    #[test]
    fn query_memory_reports_each_terminal_outcome_once_and_contains_panics() {
        for outcome in [QueryMemoryOutcome::Success, QueryMemoryOutcome::Error] {
            let (observer, observations) = query_memory_observations();
            QueryMemoryExecution::begin(observer).finish(outcome);
            assert_eq!(
                observations.lock().unwrap().as_slice(),
                &[QueryMemoryObservation {
                    datafusion_reserved_peak_bytes: 0,
                    outcome,
                }]
            );
        }

        let (observer, observations) = query_memory_observations();
        drop(QueryMemoryExecution::begin(observer));
        assert_eq!(
            observations.lock().unwrap().as_slice(),
            &[QueryMemoryObservation {
                datafusion_reserved_peak_bytes: 0,
                outcome: QueryMemoryOutcome::Cancelled,
            }]
        );

        let observer: QueryMemoryObserver = Arc::new(|_| panic!("observer panic"));
        QueryMemoryExecution::begin(observer).finish(QueryMemoryOutcome::Success);
    }

    #[test]
    fn query_memory_aggregates_wrappers_and_waits_for_final_release() {
        let inner: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64));
        let runtime = query_memory_context(Arc::clone(&inner)).runtime_env();
        let (released_tx, released_rx) = mpsc::channel();
        let observations = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&observations);
        let observer = Arc::new(move |observation| {
            captured.lock().unwrap().push(observation);
            released_tx.send(thread::current().id()).unwrap();
        });
        let execution = QueryMemoryExecution::begin(observer);
        let tracker = Arc::clone(&execution.tracker);
        let first = execution
            .task_context(TaskContext::default().with_runtime(Arc::clone(&runtime)))
            .unwrap();
        let second = execution
            .task_context(TaskContext::default().with_runtime(Arc::clone(&runtime)))
            .unwrap();
        assert!(!runtime.memory_pool.is::<super::QueryMemoryPool>());
        assert_eq!(first.memory_pool().name(), "greedy");
        assert!(matches!(
            first.memory_pool().memory_limit(),
            MemoryLimit::Finite(64)
        ));

        let first = MemoryConsumer::new("first").register(first.memory_pool());
        let second = MemoryConsumer::new("second").register(second.memory_pool());
        first.grow(4);
        second.try_grow(7).unwrap();
        first.shrink(3);
        first.grow(2);
        assert_eq!(inner.reserved(), 10);
        assert_eq!(tracker.lock().peak_bytes, 11);

        execution.finish(QueryMemoryOutcome::Success);
        drop(first);
        assert!(observations.lock().unwrap().is_empty());
        let release_thread = thread::spawn(move || {
            drop(second);
            thread::current().id()
        });
        let callback_thread = released_rx.recv().unwrap();
        assert_eq!(callback_thread, release_thread.join().unwrap());
        assert_eq!(tracker.lock().reserved_bytes, 0);
        assert_eq!(inner.reserved(), 0);
        assert_eq!(
            observations.lock().unwrap().as_slice(),
            &[QueryMemoryObservation {
                datafusion_reserved_peak_bytes: 11,
                outcome: QueryMemoryOutcome::Success,
            }]
        );
    }

    #[test]
    fn query_memory_excludes_failed_growth_and_omits_leaks() {
        let inner: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(8));
        let (observer, observations) = query_memory_observations();
        let execution = QueryMemoryExecution::begin(observer);
        let context = execution.task_context(query_memory_context(inner)).unwrap();
        let reservation = MemoryConsumer::new("limited").register(context.memory_pool());
        reservation.try_grow(4).unwrap();
        assert!(reservation.try_grow(5).is_err());
        execution.finish(QueryMemoryOutcome::Error);
        drop(reservation);
        assert_eq!(
            observations.lock().unwrap().as_slice(),
            &[QueryMemoryObservation {
                datafusion_reserved_peak_bytes: 4,
                outcome: QueryMemoryOutcome::Error,
            }]
        );

        let (observer, observations) = query_memory_observations();
        let execution = QueryMemoryExecution::begin(observer);
        let context = execution
            .task_context(query_memory_context(Arc::new(
                UnboundedMemoryPool::default(),
            )))
            .unwrap();
        let leaked = MemoryConsumer::new("leaked").register(context.memory_pool());
        leaked.grow(1);
        let _leaked_reservation = Box::leak(Box::new(leaked));
        drop(execution);
        assert!(observations.lock().unwrap().is_empty());
    }

    #[test]
    fn query_memory_keeps_overlapping_executions_independent() {
        let inner: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let (left_observer, left_observations) = query_memory_observations();
        let (right_observer, right_observations) = query_memory_observations();
        let left = QueryMemoryExecution::begin(left_observer);
        let right = QueryMemoryExecution::begin(right_observer);
        let left_context = left
            .task_context(query_memory_context(Arc::clone(&inner)))
            .unwrap();
        let right_context = right
            .task_context(query_memory_context(Arc::clone(&inner)))
            .unwrap();
        let left_reservation = MemoryConsumer::new("left").register(left_context.memory_pool());
        let right_reservation = MemoryConsumer::new("right").register(right_context.memory_pool());
        left_reservation.grow(3);
        right_reservation.grow(5);
        left.finish(QueryMemoryOutcome::Success);
        right.finish(QueryMemoryOutcome::Error);
        drop(left_reservation);
        assert_eq!(
            left_observations.lock().unwrap().as_slice(),
            &[QueryMemoryObservation {
                datafusion_reserved_peak_bytes: 3,
                outcome: QueryMemoryOutcome::Success,
            }]
        );
        assert!(right_observations.lock().unwrap().is_empty());
        assert_eq!(inner.reserved(), 5);
        drop(right_reservation);
        assert_eq!(
            right_observations.lock().unwrap().as_slice(),
            &[QueryMemoryObservation {
                datafusion_reserved_peak_bytes: 5,
                outcome: QueryMemoryOutcome::Error,
            }]
        );
        assert_eq!(inner.reserved(), 0);
    }

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
