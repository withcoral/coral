//! Error types for live-eval row injection.

use std::path::PathBuf;

use coral_engine::SourceDecoratorError;

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

    pub(crate) fn into_source_decorator(self) -> SourceDecoratorError {
        match self {
            error @ Self::Io { .. } => SourceDecoratorError::failed_precondition(error.to_string()),
            other => SourceDecoratorError::invalid_input(other.to_string()),
        }
    }
}
