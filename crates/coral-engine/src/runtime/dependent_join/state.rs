use std::collections::{HashMap, HashSet};

use arrow::array::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use serde_json::Value;

use crate::runtime::dependent_join::bindings::{BindingProjector, Tuple};
use crate::runtime::dependent_join::error::DependentJoinError;

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

#[derive(Debug, Default)]
pub(crate) struct DependentJoinRuntimeState {
    resolver_batches: Vec<RecordBatch>,
    bindings_by_tuple: HashMap<Tuple, Vec<ResolverRowId>>,
    seen_tuples: HashSet<Tuple>,
    buffered_results: HashMap<Tuple, Vec<Value>>,
    resolver_rows: usize,
    distinct_tuples: usize,
}

impl DependentJoinRuntimeState {
    pub(crate) fn ingest_resolver_batch(
        &mut self,
        batch: &RecordBatch,
        projector: &BindingProjector,
        caps: &ResolverCaps,
    ) -> Result<Vec<Tuple>> {
        let observed = self.resolver_rows.saturating_add(batch.num_rows());
        if observed > caps.max_resolver_rows {
            return Err(resolver_rows_exceeded(caps, observed));
        }

        let batch_idx = self.resolver_batches.len();
        let row_count = batch.num_rows();
        self.resolver_batches.push(batch.clone());
        self.resolver_rows = observed;

        let mut new_tuples = Vec::new();

        for row_idx in 0..row_count {
            let Some(tuple) = projector.project(batch, row_idx)? else {
                continue;
            };

            let rows_for_tuple = self
                .bindings_by_tuple
                .entry(tuple.clone())
                .or_default()
                .len()
                .saturating_add(1);
            if rows_for_tuple > caps.max_resolver_rows_per_binding {
                return Err(resolver_rows_per_binding_exceeded(caps, rows_for_tuple));
            }

            self.bindings_by_tuple
                .entry(tuple.clone())
                .or_default()
                .push(ResolverRowId { batch_idx, row_idx });

            if self.seen_tuples.insert(tuple.clone()) {
                self.distinct_tuples += 1;
                if self.distinct_tuples > caps.max_bindings {
                    return Err(cardinality_exceeded(caps, self.distinct_tuples));
                }

                new_tuples.push(tuple);
            }
        }

        Ok(new_tuples)
    }

    pub(crate) fn buffer_fetch_result(&mut self, tuple: Tuple, rows: Vec<Value>) {
        self.buffered_results.insert(tuple, rows);
    }

    pub(crate) fn resolver_rows(&self) -> usize {
        self.resolver_rows
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

    use crate::runtime::dependent_join::logical::BindingKey;

    use super::*;

    #[test]
    fn resolver_row_cap_rejects_batch_before_buffering_it() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["one", "two"]))],
        )
        .expect("record batch");
        let caps = ResolverCaps {
            source_schema: "github".to_string(),
            table: "pull_requests".to_string(),
            max_bindings: 10,
            max_resolver_rows: 1,
            max_resolver_rows_per_binding: 10,
            binding_filters: vec!["id".to_string()],
        };
        let projector = BindingProjector::new(Arc::from(Vec::<BindingKey>::new()));
        let mut state = DependentJoinRuntimeState::default();

        let error = state
            .ingest_resolver_batch(&batch, &projector, &caps)
            .expect_err("batch exceeding resolver row cap should fail");

        assert!(
            error
                .to_string()
                .contains("produced 2 rows, which exceeds max_resolver_rows=1"),
            "{error}"
        );
        assert_eq!(state.resolver_rows(), 0);
        assert!(state.resolver_batch(0).is_none());
    }
}
