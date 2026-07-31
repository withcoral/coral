use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::array::{Array, BooleanArray, RecordBatch, UInt32Array};
use arrow::compute::{filter_record_batch, take};
use arrow::datatypes::{Schema, SchemaRef};
use coral_spec::backends::http::HttpTableSpec;
use datafusion::common::{DataFusionError, Result};

use crate::QueryExecutionControls;
use crate::backends::schema_from_columns;
use crate::backends::shared::mapping::convert_items;
use crate::backends::shared::source_observation::{
    SourceObservationConfig, publish_source_scan_batch,
};
use crate::runtime::dependent_join::bindings::{
    Tuple, extract_binding_value, filter_values_for_tuple,
};
use crate::runtime::dependent_join::state::DependentJoinRuntimeState;
use crate::runtime::memory::{RetainedMemory, RetainedRecordBatches};

#[derive(Clone, Copy)]
pub(crate) struct BuildJoinedBatchesConfig<'a> {
    pub(crate) state: &'a DependentJoinRuntimeState,
    pub(crate) dependent_source_schema: &'a str,
    pub(crate) dependent_table: &'a HttpTableSpec,
    pub(crate) binding_filters: &'a [String],
    pub(crate) literal_filters: &'a BTreeMap<String, String>,
    pub(crate) dependent_projection: &'a [usize],
    pub(crate) resolver_projection_len: usize,
    pub(crate) dependent_first: bool,
    pub(crate) output_schema: &'a SchemaRef,
    pub(crate) source_observation: Option<&'a SourceObservationConfig>,
    pub(crate) controls: &'a QueryExecutionControls,
}

pub(crate) fn build_joined_batches(
    config: &BuildJoinedBatchesConfig<'_>,
    output_memory: RetainedMemory,
) -> Result<RetainedRecordBatches> {
    check_execution_controls(config.controls)?;
    let dependent_schema = schema_from_columns(
        config.dependent_table.columns(),
        config.dependent_source_schema,
        config.dependent_table.name(),
    )?;
    let mut output_batches = RetainedRecordBatches::new(output_memory);

    for tuple in config.state.binding_tuples() {
        check_execution_controls(config.controls)?;
        let Some(rows) = config.state.buffered_rows_for_tuple(tuple) else {
            continue;
        };

        if rows.is_empty() {
            continue;
        }

        let filter_values =
            filter_values_for_tuple(config.literal_filters, config.binding_filters, tuple)?;
        let dependent_batch = convert_items(
            config.dependent_table.columns(),
            Arc::clone(&dependent_schema),
            &filter_values,
            &HashMap::new(),
            rows,
        )?;
        check_execution_controls(config.controls)?;
        if let Some(source_observation) = config.source_observation {
            publish_source_scan_batch(
                config.dependent_source_schema,
                config.dependent_table.name(),
                source_observation,
                &dependent_batch,
            );
        }
        // The rewrite replaced the Join node, so nothing downstream re-applies
        // the ON condition. APIs can resolve keyed lookups loosely (rename
        // redirects, case-insensitive identifiers), so enforce the join
        // equality here: keep only rows whose key columns match the binding,
        // exactly as the unrewritten hash join would.
        let dependent_batch = filter_rows_matching_binding(
            &dependent_batch,
            config.binding_filters,
            tuple,
            config.controls,
        )?;
        if dependent_batch.num_rows() == 0 {
            continue;
        }
        let dependent_batch =
            project_dependent_batch(&dependent_batch, config.dependent_projection)?;

        let mut resolver_rows_by_batch = BTreeMap::<usize, Vec<usize>>::new();
        for resolver_row in config.state.resolver_rows_for_tuple(tuple) {
            resolver_rows_by_batch
                .entry(resolver_row.batch_idx)
                .or_default()
                .push(resolver_row.row_idx);
        }

        for (resolver_batch_idx, resolver_row_indices) in resolver_rows_by_batch {
            check_execution_controls(config.controls)?;
            // Arrow `take` allocates the fanout arrays before a RecordBatch
            // exists, so reserve an estimate first and resize to the actual
            // retained batch memory after construction.
            let reserved = reserve_memory_for_join_batch(
                config.state,
                resolver_batch_idx,
                &resolver_row_indices,
                &dependent_batch,
                config.resolver_projection_len,
                output_batches.memory(),
            )?;
            let batch = match build_fanout_join_batch(
                config.state,
                resolver_batch_idx,
                &resolver_row_indices,
                &dependent_batch,
                config.resolver_projection_len,
                config.dependent_first,
                Arc::clone(config.output_schema),
                config.controls,
            ) {
                Ok(batch) => batch,
                Err(error) => {
                    release_reserved_output_memory(output_batches.memory(), reserved);
                    return Err(error);
                }
            };
            let actual = batch.get_array_memory_size();
            if let Err(error) = output_batches
                .memory()
                .reconcile_reserved_bytes(reserved, actual)
            {
                release_reserved_output_memory(output_batches.memory(), reserved);
                return Err(error);
            }
            output_batches.push_reserved(batch);
        }
    }

    Ok(output_batches)
}

fn reserve_memory_for_join_batch(
    state: &DependentJoinRuntimeState,
    resolver_batch_idx: usize,
    resolver_row_indices: &[usize],
    dependent_batch: &RecordBatch,
    resolver_projection_len: usize,
    output_memory: &RetainedMemory,
) -> Result<usize> {
    let resolver_batch = state
        .resolver_batch(resolver_batch_idx)
        .ok_or_else(|| DataFusionError::Internal("dependent join resolver batch missing".into()))?;
    let output_rows = dependent_batch
        .num_rows()
        .checked_mul(resolver_row_indices.len())
        .ok_or_else(|| {
            DataFusionError::Execution("dependent join output row count overflow".into())
        })?;
    let bytes = estimate_output_memory(
        resolver_batch,
        resolver_projection_len,
        dependent_batch,
        output_rows,
    )?;
    reserve_output_memory(output_memory, bytes)?;
    Ok(bytes)
}

fn estimate_output_memory(
    resolver_batch: &RecordBatch,
    resolver_projection_len: usize,
    dependent_batch: &RecordBatch,
    output_rows: usize,
) -> Result<usize> {
    let index_bytes = output_rows
        .checked_mul(std::mem::size_of::<u32>())
        .and_then(|bytes| bytes.checked_mul(2))
        .ok_or_else(|| {
            DataFusionError::Execution("dependent join output memory estimate overflow".into())
        })?;
    let resolver_bytes = resolver_batch
        .columns()
        .iter()
        .take(resolver_projection_len)
        .try_fold(index_bytes, |bytes, array| {
            add_taken_array_memory_estimate(bytes, array.as_ref(), output_rows)
        })?;
    dependent_batch
        .columns()
        .iter()
        .try_fold(resolver_bytes, |bytes, array| {
            add_taken_array_memory_estimate(bytes, array.as_ref(), output_rows)
        })
}

fn reserve_output_memory(output_memory: &RetainedMemory, bytes: usize) -> Result<()> {
    output_memory.try_reserve_bytes(bytes)
}

fn release_reserved_output_memory(output_memory: &RetainedMemory, bytes: usize) {
    match output_memory.try_shrink_bytes(bytes) {
        Ok(()) | Err(_) => {}
    }
}

fn add_taken_array_memory_estimate(
    bytes: usize,
    array: &dyn Array,
    output_rows: usize,
) -> Result<usize> {
    let estimated = estimate_taken_array_memory(array, output_rows)?;
    bytes.checked_add(estimated).ok_or_else(|| {
        DataFusionError::Execution("dependent join output memory estimate overflow".into())
    })
}

fn estimate_taken_array_memory(array: &dyn Array, output_rows: usize) -> Result<usize> {
    if output_rows == 0 || array.is_empty() {
        return Ok(0);
    }
    let bytes_per_row = array.get_array_memory_size().div_ceil(array.len());
    bytes_per_row.checked_mul(output_rows).ok_or_else(|| {
        DataFusionError::Execution("dependent join output memory estimate overflow".into())
    })
}

#[expect(
    clippy::too_many_arguments,
    reason = "The join batch builder receives the already-separated resolver, dependent, schema, memory, and execution-control inputs needed for one bounded allocation."
)]
fn build_fanout_join_batch(
    state: &DependentJoinRuntimeState,
    resolver_batch_idx: usize,
    resolver_row_indices: &[usize],
    dependent_batch: &RecordBatch,
    resolver_projection_len: usize,
    dependent_first: bool,
    output_schema: SchemaRef,
    controls: &QueryExecutionControls,
) -> Result<RecordBatch> {
    check_execution_controls(controls)?;
    let resolver_batch = state
        .resolver_batch(resolver_batch_idx)
        .ok_or_else(|| DataFusionError::Internal("dependent join resolver batch missing".into()))?;
    let dependent_rows = dependent_batch.num_rows();
    let output_rows = dependent_rows
        .checked_mul(resolver_row_indices.len())
        .ok_or_else(|| {
            DataFusionError::Execution("dependent join output row count overflow".into())
        })?;
    let dependent_rows = u32::try_from(dependent_rows).map_err(|error| {
        DataFusionError::Execution(format!(
            "dependent join dependent row count cannot fit Arrow take index: {error}"
        ))
    })?;
    let mut resolver_indices = Vec::with_capacity(output_rows);
    let mut dependent_indices = Vec::with_capacity(output_rows);

    for row_idx in resolver_row_indices {
        check_execution_controls(controls)?;
        let row_idx = u32::try_from(*row_idx).map_err(|error| {
            DataFusionError::Execution(format!(
                "dependent join resolver row index cannot fit Arrow take index: {error}"
            ))
        })?;
        resolver_indices.extend(std::iter::repeat_n(row_idx, dependent_rows as usize));
        dependent_indices.extend(0..dependent_rows);
    }

    let resolver_indices = UInt32Array::from(resolver_indices);
    let dependent_indices = UInt32Array::from(dependent_indices);
    let mut resolver_arrays = Vec::with_capacity(resolver_projection_len);
    for array in resolver_batch
        .columns()
        .iter()
        .take(resolver_projection_len)
    {
        check_execution_controls(controls)?;
        resolver_arrays.push(take(array.as_ref(), &resolver_indices, None).map_err(arrow_error)?);
    }
    let mut dependent_arrays = Vec::with_capacity(dependent_batch.num_columns());
    for array in dependent_batch.columns() {
        check_execution_controls(controls)?;
        dependent_arrays.push(take(array.as_ref(), &dependent_indices, None).map_err(arrow_error)?);
    }
    let mut arrays = Vec::with_capacity(resolver_arrays.len() + dependent_batch.num_columns());

    if dependent_first {
        arrays.extend(dependent_arrays);
        arrays.extend(resolver_arrays);
    } else {
        arrays.extend(resolver_arrays);
        arrays.extend(dependent_arrays);
    }

    RecordBatch::try_new(output_schema, arrays).map_err(|error| {
        DataFusionError::ArrowError(
            Box::new(error),
            Some("building dependent join output".into()),
        )
    })
}

/// Keeps only fetched rows whose join-key columns equal the binding tuple's
/// values. `from_filter` echo columns are stamped with the binding value
/// during conversion and pass trivially; path-backed columns carry real
/// response data and can diverge when the API resolves a lookup loosely. A
/// NULL key never matches, mirroring SQL join semantics.
fn filter_rows_matching_binding(
    batch: &RecordBatch,
    binding_filters: &[String],
    tuple: &Tuple,
    controls: &QueryExecutionControls,
) -> Result<RecordBatch> {
    check_execution_controls(controls)?;
    let schema = batch.schema();
    let mut key_columns = Vec::with_capacity(binding_filters.len());
    for (filter, expected) in binding_filters.iter().zip(tuple.values()) {
        let index = schema.index_of(filter).map_err(|error| {
            DataFusionError::Internal(format!(
                "dependent join key column '{filter}' missing from dependent schema: {error}"
            ))
        })?;
        key_columns.push((batch.column(index), expected));
    }

    let mut mask = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        check_execution_controls(controls)?;
        let mut matches = true;
        for (array, expected) in &key_columns {
            if array.is_null(row) {
                matches = false;
                break;
            }
            let actual = extract_binding_value(array.as_ref(), row)?;
            if !expected.join_matches(&actual) {
                matches = false;
                break;
            }
        }
        mask.push(matches);
    }

    filter_record_batch(batch, &BooleanArray::from(mask)).map_err(arrow_error)
}

fn check_execution_controls(controls: &QueryExecutionControls) -> Result<()> {
    controls
        .check_active()
        .map_err(|kind| DataFusionError::External(Box::new(kind)))
}

fn project_dependent_batch(batch: &RecordBatch, projection: &[usize]) -> Result<RecordBatch> {
    let schema = batch.schema();
    let mut fields = Vec::with_capacity(projection.len());
    let mut arrays = Vec::with_capacity(projection.len());

    for index in projection {
        let field = schema.fields().get(*index).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "dependent join projection index {index} is out of bounds for dependent schema"
            ))
        })?;
        let array = batch.columns().get(*index).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "dependent join projection index {index} is out of bounds for dependent batch"
            ))
        })?;

        fields.push(Arc::clone(field));
        arrays.push(Arc::clone(array));
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|error| {
        DataFusionError::ArrowError(
            Box::new(error),
            Some("projecting dependent join output".into()),
        )
    })
}

fn arrow_error(error: arrow::error::ArrowError) -> DataFusionError {
    DataFusionError::ArrowError(
        Box::new(error),
        Some("building dependent join output".into()),
    )
}
