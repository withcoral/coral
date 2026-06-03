use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::array::{RecordBatch, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::{Schema, SchemaRef};
use coral_spec::backends::http::HttpTableSpec;
use datafusion::common::{DataFusionError, Result};

use crate::backends::schema_from_columns;
use crate::backends::shared::mapping::convert_items;
use crate::runtime::dependent_join::bindings::filter_values_for_tuple;
use crate::runtime::dependent_join::state::DependentJoinRuntimeState;

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
}

pub(crate) fn build_joined_batches(
    config: BuildJoinedBatchesConfig<'_>,
) -> Result<Vec<RecordBatch>> {
    let BuildJoinedBatchesConfig {
        state,
        dependent_source_schema,
        dependent_table,
        binding_filters,
        literal_filters,
        dependent_projection,
        resolver_projection_len,
        dependent_first,
        output_schema,
    } = config;
    let dependent_schema = schema_from_columns(
        dependent_table.columns(),
        dependent_source_schema,
        dependent_table.name(),
    )?;
    let mut batches = Vec::new();

    for tuple in state.binding_tuples() {
        let Some(rows) = state.buffered_rows_for_tuple(tuple) else {
            continue;
        };

        if rows.is_empty() {
            continue;
        }

        let filter_values = filter_values_for_tuple(literal_filters, binding_filters, tuple)?;
        let dependent_batch = convert_items(
            dependent_table.columns(),
            Arc::clone(&dependent_schema),
            &filter_values,
            &HashMap::new(),
            rows,
        )?;
        let dependent_batch = project_dependent_batch(&dependent_batch, dependent_projection)?;

        let mut resolver_rows_by_batch = BTreeMap::<usize, Vec<usize>>::new();
        for resolver_row in state.resolver_rows_for_tuple(tuple) {
            resolver_rows_by_batch
                .entry(resolver_row.batch_idx)
                .or_default()
                .push(resolver_row.row_idx);
        }

        for (resolver_batch_idx, resolver_row_indices) in resolver_rows_by_batch {
            batches.push(join_for_resolver_rows(
                state,
                resolver_batch_idx,
                &resolver_row_indices,
                &dependent_batch,
                resolver_projection_len,
                dependent_first,
                Arc::clone(output_schema),
            )?);
        }
    }

    Ok(batches)
}

fn join_for_resolver_rows(
    state: &DependentJoinRuntimeState,
    resolver_batch_idx: usize,
    resolver_row_indices: &[usize],
    dependent_batch: &RecordBatch,
    resolver_projection_len: usize,
    dependent_first: bool,
    output_schema: SchemaRef,
) -> Result<RecordBatch> {
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
    let resolver_arrays = resolver_batch
        .columns()
        .iter()
        .take(resolver_projection_len)
        .map(|array| take(array.as_ref(), &resolver_indices, None).map_err(arrow_error))
        .collect::<Result<Vec<_>>>()?;
    let dependent_arrays = dependent_batch
        .columns()
        .iter()
        .map(|array| take(array.as_ref(), &dependent_indices, None).map_err(arrow_error))
        .collect::<Result<Vec<_>>>()?;
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
