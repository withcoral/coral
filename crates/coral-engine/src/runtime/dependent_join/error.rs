use datafusion::common::DataFusionError;
use thiserror::Error;

use crate::CoreError;

#[derive(Debug, Clone, Copy)]
pub(crate) struct ResolverRowsExceeded<'a> {
    pub(crate) source_schema: &'a str,
    pub(crate) table: &'a str,
    pub(crate) observed: usize,
    pub(crate) cap: usize,
}

#[derive(Debug, Error)]
pub(crate) enum DependentJoinError {
    #[error(
        "dependent join into '{source_schema}.{table}' produced {observed} binding tuples, which exceeds cap {cap}. Narrow the resolver query or raise the dependent join binding cap. binding_filters=[{}]",
        binding_filters.join(", ")
    )]
    Cardinality {
        source_schema: String,
        table: String,
        observed: usize,
        cap: usize,
        binding_filters: Vec<String>,
    },

    #[error(
        "dependent join resolver for '{source_schema}.{table}' produced {observed} rows, which exceeds max_resolver_rows={cap}"
    )]
    ResolverRows {
        source_schema: String,
        table: String,
        observed: usize,
        cap: usize,
    },

    #[error(
        "dependent join fetch for '{source_schema}.{table}' returned {observed} rows for one binding, which exceeds max_rows_per_binding={cap}"
    )]
    RowsPerBinding {
        source_schema: String,
        table: String,
        observed: usize,
        cap: usize,
    },

    #[error(
        "dependent join resolver for '{source_schema}.{table}' produced {observed} rows for one binding, which exceeds max_resolver_rows_per_binding={cap}"
    )]
    ResolverRowsPerBinding {
        source_schema: String,
        table: String,
        observed: usize,
        cap: usize,
    },
}

impl DependentJoinError {
    pub(crate) fn into_datafusion(self) -> DataFusionError {
        DataFusionError::External(Box::new(self))
    }

    pub(crate) fn to_core_error(&self) -> CoreError {
        CoreError::FailedPrecondition(self.to_string())
    }
}

pub(crate) fn resolver_rows_exceeded(error: &DataFusionError) -> Option<ResolverRowsExceeded<'_>> {
    let DataFusionError::External(inner) = error.find_root() else {
        return None;
    };
    let error = inner.downcast_ref::<DependentJoinError>()?;
    match error {
        DependentJoinError::ResolverRows {
            source_schema,
            table,
            observed,
            cap,
        } => Some(ResolverRowsExceeded {
            source_schema,
            table,
            observed: *observed,
            cap: *cap,
        }),
        DependentJoinError::Cardinality { .. }
        | DependentJoinError::RowsPerBinding { .. }
        | DependentJoinError::ResolverRowsPerBinding { .. } => None,
    }
}
