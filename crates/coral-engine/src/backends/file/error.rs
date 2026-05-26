//! File-backend setup errors.

use datafusion::error::DataFusionError;

#[derive(Debug, thiserror::Error)]
pub(super) enum FileBackendError {
    #[error("{source_schema}.{table} has invalid source.location '{location}': {detail}")]
    InvalidSourceLocation {
        source_schema: String,
        table: String,
        location: String,
        detail: String,
    },

    #[error("file source '{source_schema}' is missing an S3 bucket in '{location}'")]
    MissingS3Bucket {
        source_schema: String,
        location: String,
    },

    #[error(
        "file source '{source_schema}' must declare source.object_store with type=s3 for '{location}'"
    )]
    MissingS3ObjectStore {
        source_schema: String,
        location: String,
    },

    #[error("file source '{source_schema}' uses unsupported scheme '{scheme}'")]
    UnsupportedScheme {
        source_schema: String,
        scheme: String,
    },
}

impl FileBackendError {
    pub(super) fn plan(self) -> DataFusionError {
        DataFusionError::Plan(self.to_string())
    }
}
