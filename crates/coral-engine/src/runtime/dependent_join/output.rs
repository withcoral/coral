use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{RecordBatch, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::{Schema, SchemaRef};
use coral_spec::backends::http::HttpTableSpec;
use datafusion::common::{DataFusionError, Result};

use crate::backends::schema_from_columns;
use crate::backends::shared::mapping::convert_items;
use crate::runtime::dependent_join::bindings::Tuple;
use crate::runtime::dependent_join::state::{DependentJoinRuntimeState, ResolverRowId};

pub(crate) fn build_joined_batches(
    state: &DependentJoinRuntimeState,
    dependent_source_schema: &str,
    dependent_table: &HttpTableSpec,
    binding_filters: &[String],
    dependent_projection: &[usize],
    dependent_first: bool,
    output_schema: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
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

        let filter_values = filter_values_for_tuple(binding_filters, tuple)?;
        let dependent_batch = convert_items(
            dependent_table.columns(),
            Arc::clone(&dependent_schema),
            &filter_values,
            rows,
        )?;
        let dependent_batch = project_dependent_batch(&dependent_batch, dependent_projection)?;

        for resolver_row in state.resolver_rows_for_tuple(tuple) {
            batches.push(join_for_resolver_row(
                state,
                *resolver_row,
                &dependent_batch,
                dependent_first,
                Arc::clone(output_schema),
            )?);
        }
    }

    Ok(batches)
}

fn join_for_resolver_row(
    state: &DependentJoinRuntimeState,
    resolver_row: ResolverRowId,
    dependent_batch: &RecordBatch,
    dependent_first: bool,
    output_schema: SchemaRef,
) -> Result<RecordBatch> {
    let resolver_batch = state
        .resolver_batch(resolver_row.batch_idx)
        .ok_or_else(|| DataFusionError::Internal("dependent join resolver batch missing".into()))?;
    let dependent_rows = dependent_batch.num_rows();
    let row_idx = u32::try_from(resolver_row.row_idx).map_err(|error| {
        DataFusionError::Execution(format!(
            "dependent join resolver row index cannot fit Arrow take index: {error}"
        ))
    })?;
    let indices = UInt32Array::from(vec![row_idx; dependent_rows]);
    let resolver_arrays = resolver_batch
        .columns()
        .iter()
        .map(|array| take(array.as_ref(), &indices, None).map_err(arrow_error))
        .collect::<Result<Vec<_>>>()?;
    let mut arrays = Vec::with_capacity(resolver_arrays.len() + dependent_batch.num_columns());

    if dependent_first {
        arrays.extend(dependent_batch.columns().iter().cloned());
        arrays.extend(resolver_arrays);
    } else {
        arrays.extend(resolver_arrays);
        arrays.extend(dependent_batch.columns().iter().cloned());
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

fn filter_values_for_tuple(
    binding_filters: &[String],
    tuple: &Tuple,
) -> Result<HashMap<String, String>> {
    if binding_filters.len() != tuple.values().len() {
        return Err(DataFusionError::Internal(format!(
            "dependent join binding arity mismatch: {} filters for {} values",
            binding_filters.len(),
            tuple.values().len()
        )));
    }

    Ok(binding_filters
        .iter()
        .zip(tuple.values())
        .map(|(filter, value)| (filter.clone(), value.to_wire_string()))
        .collect())
}

fn arrow_error(error: arrow::error::ArrowError) -> DataFusionError {
    DataFusionError::ArrowError(
        Box::new(error),
        Some("building dependent join output".into()),
    )
}
