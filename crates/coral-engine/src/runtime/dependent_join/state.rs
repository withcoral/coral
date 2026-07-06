use std::collections::{HashMap, HashSet};

use arrow::array::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use serde_json::Value;

use crate::runtime::dependent_join::bindings::{BindingProjector, Tuple};
use crate::runtime::dependent_join::error::DependentJoinError;
use crate::runtime::memory::RetainedMemory;

#[derive(Debug, Clone)]
pub(crate) struct ResolverCaps {
    pub(crate) source_schema: String,
    pub(crate) table: String,
    pub(crate) max_bindings: usize,
    pub(crate) max_resolver_rows: usize,
    pub(crate) max_resolver_rows_per_binding: usize,
    pub(crate) binding_filters: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResolverRowId {
    pub(crate) batch_idx: usize,
    pub(crate) row_idx: usize,
}

#[derive(Debug)]
pub(crate) struct DependentJoinRuntimeState {
    memory: RetainedMemory,
    resolver_batches: Vec<RecordBatch>,
    bindings_by_tuple: HashMap<Tuple, Vec<ResolverRowId>>,
    seen_tuples: HashSet<Tuple>,
    buffered_results: HashMap<Tuple, Vec<Value>>,
    resolver_rows: usize,
    resolver_null_binding_rows: usize,
    distinct_tuples: usize,
}

impl DependentJoinRuntimeState {
    pub(crate) fn new(memory: RetainedMemory) -> Self {
        Self {
            memory,
            resolver_batches: Vec::new(),
            bindings_by_tuple: HashMap::new(),
            seen_tuples: HashSet::new(),
            buffered_results: HashMap::new(),
            resolver_rows: 0,
            resolver_null_binding_rows: 0,
            distinct_tuples: 0,
        }
    }

    pub(crate) fn ingest_resolver_batch(
        &mut self,
        batch: &RecordBatch,
        projector: &BindingProjector,
        caps: &ResolverCaps,
    ) -> Result<Vec<Tuple>> {
        let projected_tuples = self.project_batch_tuples(batch, projector, caps)?;
        let observed = self.resolver_rows.saturating_add(batch.num_rows());
        if observed > caps.max_resolver_rows {
            return Err(resolver_rows_exceeded(caps, observed));
        }

        let batch_idx = self.resolver_batches.len();
        self.memory.try_reserve_record_batch(batch)?;
        self.resolver_batches.push(batch.clone());
        self.resolver_rows = observed;
        self.resolver_null_binding_rows = self
            .resolver_null_binding_rows
            .saturating_add(projected_tuples.iter().filter(|t| t.is_none()).count());

        let mut new_tuples = Vec::new();

        for (row_idx, tuple) in projected_tuples.into_iter().enumerate() {
            let Some(tuple) = tuple else {
                continue;
            };

            let is_new_binding_tuple = !self.bindings_by_tuple.contains_key(&tuple);
            let is_new_fetch_tuple = !self.seen_tuples.contains(&tuple);
            self.reserve_tuple_copies(
                &tuple,
                usize::from(is_new_binding_tuple)
                    + usize::from(is_new_fetch_tuple)
                    + usize::from(is_new_fetch_tuple),
            )?;

            if let Some(resolver_rows) = self.bindings_by_tuple.get_mut(&tuple) {
                resolver_rows.push(ResolverRowId { batch_idx, row_idx });
            } else {
                self.bindings_by_tuple
                    .insert(tuple.clone(), vec![ResolverRowId { batch_idx, row_idx }]);
            }

            if is_new_fetch_tuple {
                self.seen_tuples.insert(tuple.clone());
                self.distinct_tuples += 1;
                new_tuples.push(tuple);
            }
        }

        Ok(new_tuples)
    }

    fn reserve_tuple_copies(&self, tuple: &Tuple, copies: usize) -> Result<()> {
        let bytes = tuple.retained_size().checked_mul(copies).ok_or_else(|| {
            DataFusionError::ResourcesExhausted(
                "dependent join binding tuple memory estimate overflow".into(),
            )
        })?;
        self.memory.try_reserve_bytes(bytes)
    }

    fn project_batch_tuples(
        &self,
        batch: &RecordBatch,
        projector: &BindingProjector,
        caps: &ResolverCaps,
    ) -> Result<Vec<Option<Tuple>>> {
        let mut projected_tuples = Vec::with_capacity(batch.num_rows());
        let mut batch_counts_by_tuple: HashMap<Tuple, usize> = HashMap::new();
        let mut new_seen_tuples: HashSet<Tuple> = HashSet::new();
        let mut distinct_tuples = self.distinct_tuples;

        for row_idx in 0..batch.num_rows() {
            let Some(tuple) = projector.project(batch, row_idx)? else {
                projected_tuples.push(None);
                continue;
            };

            let batch_count = batch_counts_by_tuple.entry(tuple.clone()).or_default();
            *batch_count = batch_count.saturating_add(1);
            let existing_count = self.bindings_by_tuple.get(&tuple).map_or(0, Vec::len);
            let rows_for_tuple = existing_count.saturating_add(*batch_count);
            if rows_for_tuple > caps.max_resolver_rows_per_binding {
                return Err(resolver_rows_per_binding_exceeded(caps, rows_for_tuple));
            }

            if !self.seen_tuples.contains(&tuple) && new_seen_tuples.insert(tuple.clone()) {
                distinct_tuples = distinct_tuples.saturating_add(1);
                if distinct_tuples > caps.max_bindings {
                    return Err(cardinality_exceeded(caps, distinct_tuples));
                }
            }

            projected_tuples.push(Some(tuple));
        }

        Ok(projected_tuples)
    }

    pub(crate) fn buffer_fetch_result(&mut self, tuple: Tuple, rows: Vec<Value>) -> Result<()> {
        self.memory.try_reserve_json_rows(&rows)?;
        self.buffered_results.insert(tuple, rows);
        Ok(())
    }

    pub(crate) fn resolver_rows(&self) -> usize {
        self.resolver_rows
    }

    pub(crate) fn resolver_null_binding_rows(&self) -> usize {
        self.resolver_null_binding_rows
    }

    pub(crate) fn distinct_tuples(&self) -> usize {
        self.distinct_tuples
    }

    pub(crate) fn fetch_count(&self) -> usize {
        self.buffered_results.len()
    }

    pub(crate) fn dependent_rows_returned(&self) -> usize {
        self.buffered_results.values().map(Vec::len).sum()
    }

    pub(crate) fn resolver_rows_for_tuple(&self, tuple: &Tuple) -> &[ResolverRowId] {
        self.bindings_by_tuple.get(tuple).map_or(&[], Vec::as_slice)
    }

    pub(crate) fn binding_tuples(&self) -> impl Iterator<Item = &Tuple> {
        self.bindings_by_tuple.keys()
    }

    pub(crate) fn resolver_batch(&self, batch_idx: usize) -> Option<&RecordBatch> {
        self.resolver_batches.get(batch_idx)
    }

    pub(crate) fn buffered_rows_for_tuple(&self, tuple: &Tuple) -> Option<&[Value]> {
        self.buffered_results.get(tuple).map(Vec::as_slice)
    }

    pub(crate) fn memory(&self) -> &RetainedMemory {
        &self.memory
    }
}

fn cardinality_exceeded(caps: &ResolverCaps, observed: usize) -> DataFusionError {
    DependentJoinError::Cardinality {
        source_schema: caps.source_schema.clone(),
        table: caps.table.clone(),
        observed,
        cap: caps.max_bindings,
        binding_filters: caps.binding_filters.clone(),
    }
    .into_datafusion()
}

fn resolver_rows_exceeded(caps: &ResolverCaps, observed: usize) -> DataFusionError {
    DependentJoinError::ResolverRows {
        source_schema: caps.source_schema.clone(),
        table: caps.table.clone(),
        observed,
        cap: caps.max_resolver_rows,
    }
    .into_datafusion()
}

fn resolver_rows_per_binding_exceeded(caps: &ResolverCaps, observed: usize) -> DataFusionError {
    DependentJoinError::ResolverRowsPerBinding {
        source_schema: caps.source_schema.clone(),
        table: caps.table.clone(),
        observed,
        cap: caps.max_resolver_rows_per_binding,
    }
    .into_datafusion()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Column;
    use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, UnboundedMemoryPool};

    use crate::runtime::dependent_join::bindings::BindingValue;
    use crate::runtime::dependent_join::logical::BindingKey;
    use crate::runtime::memory::RetainedMemory;

    use super::*;

    fn caps(
        max_bindings: usize,
        max_resolver_rows: usize,
        max_resolver_rows_per_binding: usize,
    ) -> ResolverCaps {
        ResolverCaps {
            source_schema: "github".to_string(),
            table: "pull_requests".to_string(),
            max_bindings,
            max_resolver_rows,
            max_resolver_rows_per_binding,
            binding_filters: vec!["id".to_string()],
        }
    }

    fn id_projector() -> BindingProjector {
        BindingProjector::new(Arc::from([BindingKey {
            resolver_column: Column::from_name("id"),
            resolver_binding_name: "id".to_string(),
            dependent_filter: "id".to_string(),
        }]))
    }

    fn id_batch(values: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values))])
            .expect("record batch")
    }

    fn nullable_id_batch(values: Vec<Option<&str>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values))])
            .expect("record batch")
    }

    fn test_memory() -> RetainedMemory {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let reservation = MemoryConsumer::new("test").register(&pool);
        RetainedMemory::new(reservation)
    }

    fn test_state() -> DependentJoinRuntimeState {
        DependentJoinRuntimeState::new(test_memory())
    }

    #[test]
    fn resolver_row_cap_rejects_batch_before_buffering_it() {
        let batch = id_batch(vec!["one", "two"]);
        let caps = caps(10, 1, 10);
        let projector = BindingProjector::new(Arc::from(Vec::<BindingKey>::new()));
        let mut state = test_state();

        let error = state
            .ingest_resolver_batch(&batch, &projector, &caps)
            .expect_err("batch exceeding resolver row cap should fail");

        assert!(
            error
                .to_string()
                .contains("produced 2 rows, but Coral is configured to inspect at most 1 rows"),
            "{error}"
        );
        assert_eq!(state.resolver_rows(), 0);
        assert_eq!(state.resolver_null_binding_rows(), 0);
        assert!(state.resolver_batch(0).is_none());
    }

    #[test]
    fn null_binding_rows_count_toward_resolver_row_cap() {
        let batch = nullable_id_batch(vec![None, None]);
        let caps = caps(10, 1, 10);
        let projector = id_projector();
        let mut state = test_state();

        let error = state
            .ingest_resolver_batch(&batch, &projector, &caps)
            .expect_err("null binding rows should count toward resolver row cap");

        assert!(
            error
                .to_string()
                .contains("produced 2 rows, but Coral is configured to inspect at most 1 rows"),
            "{error}"
        );
        assert_eq!(state.resolver_rows(), 0);
        assert_eq!(state.resolver_null_binding_rows(), 0);
        assert!(state.resolver_batch(0).is_none());
    }

    #[test]
    fn binding_cardinality_cap_takes_precedence_over_resolver_row_cap() {
        let batch = id_batch(vec!["one", "two"]);
        let caps = caps(1, 1, 10);
        let projector = id_projector();
        let mut state = test_state();

        let error = state
            .ingest_resolver_batch(&batch, &projector, &caps)
            .expect_err("binding cap should fail before resolver row fallback cap");

        assert!(
            error
                .to_string()
                .contains("produced 2 distinct combinations of join-key values"),
            "{error}"
        );
        assert_eq!(state.resolver_rows(), 0);
        assert_eq!(state.resolver_null_binding_rows(), 0);
        assert_eq!(state.distinct_tuples(), 0);
        assert!(state.resolver_batch(0).is_none());
    }

    #[test]
    fn retained_binding_tuples_are_accounted() {
        let batch = id_batch(vec!["one"]);
        let caps = caps(10, 10, 10);
        let projector = id_projector();
        let mut state = test_state();
        let tuple = Tuple::new(vec![BindingValue::String("one".to_string())]);
        let expected_tuple_bytes = tuple.retained_size() * 3;

        state
            .ingest_resolver_batch(&batch, &projector, &caps)
            .expect("batch should ingest");

        assert!(
            state.memory().reserved() >= batch.get_array_memory_size() + expected_tuple_bytes,
            "retained bytes should include resolver batch and binding tuple copies"
        );
    }

    #[test]
    fn per_binding_resolver_row_cap_takes_precedence_over_total_resolver_row_cap() {
        let batch = id_batch(vec!["one", "one"]);
        let caps = caps(10, 1, 1);
        let projector = id_projector();
        let mut state = test_state();

        let error = state
            .ingest_resolver_batch(&batch, &projector, &caps)
            .expect_err("per-binding cap should fail before resolver row fallback cap");

        assert!(
            error
                .to_string()
                .contains("One join-key combination for github.pull_requests matched 2 rows"),
            "{error}"
        );
        assert_eq!(state.resolver_rows(), 0);
        assert_eq!(state.resolver_null_binding_rows(), 0);
        assert_eq!(state.distinct_tuples(), 0);
        assert!(state.resolver_batch(0).is_none());
    }
}
