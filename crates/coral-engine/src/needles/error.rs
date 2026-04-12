//! Error types for benchmark needle operations.

use std::path::PathBuf;

/// Errors that can occur when loading or applying benchmark needle data.
#[derive(Debug, thiserror::Error)]
pub(crate) enum NeedleError {
    #[error("{}: {source}", path.display())]
    Io {
        path: PathBuf,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("failed to parse needles YAML: {0}")]
    Yaml(String),

    #[error("failed to cast needle column '{column}' from {from:?} to {to:?}: {source}")]
    CastFailed {
        column: String,
        from: datafusion::arrow::datatypes::DataType,
        to: datafusion::arrow::datatypes::DataType,
        source: datafusion::arrow::error::ArrowError,
    },

    #[error("failed to convert needle data to Arrow: {0}")]
    JsonConversion(String),

    #[error("failed to build needle RecordBatch: {0}")]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error("needles file contains entries for unregistered tables: {tables}")]
    UnusedEntries { tables: String },

    #[error(
        "source '{schema}' failed to register while needles target table(s) {tables}: {detail}"
    )]
    SourceRegistrationFailed {
        schema: String,
        tables: String,
        detail: String,
    },
}

impl NeedleError {
    pub(crate) fn io(
        path: &std::path::Path,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Io {
            path: path.to_path_buf(),
            source: Box::new(source),
        }
    }
}

impl From<NeedleError> for datafusion::error::DataFusionError {
    fn from(err: NeedleError) -> Self {
        match err {
            error @ (NeedleError::Io { .. } | NeedleError::SourceRegistrationFailed { .. }) => {
                datafusion::error::DataFusionError::External(Box::new(error))
            }
            other => datafusion::error::DataFusionError::Plan(other.to_string()),
        }
    }
}
