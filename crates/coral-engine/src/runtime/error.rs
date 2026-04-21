//! Shared runtime-error normalization for source compilation and registration.

use datafusion::error::DataFusionError;

use crate::backends::http::ProviderQueryError;
use crate::{CoreError, SourceDecoratorError};

pub(crate) fn datafusion_to_core(error: &DataFusionError) -> CoreError {
    // Unwrap Context/Shared/Diagnostic wrappers so wrapped schema errors
    // get classified by their root variant instead of all landing in the
    // `Internal` bucket. Without `find_root()`, `SELECT bogus FROM wide`
    // surfaces as `CoreError::Internal` because DataFusion wraps the
    // SchemaError in `Context`/`Execution`, hiding the structured variant
    // from the match arms below.
    match error.find_root() {
        DataFusionError::SQL(detail, _) => CoreError::InvalidInput(detail.to_string()),
        DataFusionError::Plan(detail) => CoreError::InvalidInput(detail.clone()),
        DataFusionError::SchemaError(schema_error, _) => {
            CoreError::InvalidInput(schema_error.to_string())
        }
        DataFusionError::NotImplemented(detail) => CoreError::Unimplemented(detail.clone()),
        DataFusionError::External(inner) => {
            if let Some(provider_error) = inner.downcast_ref::<ProviderQueryError>() {
                return provider_error_to_core(provider_error);
            }
            if let Some(source_decorator_error) = inner.downcast_ref::<SourceDecoratorError>() {
                return source_decorator_error_to_core(source_decorator_error);
            }
            CoreError::internal(inner.to_string())
        }
        DataFusionError::ObjectStore(err) => CoreError::Unavailable(err.to_string()),
        DataFusionError::ResourcesExhausted(detail) => CoreError::Unavailable(detail.clone()),
        other => CoreError::internal(other.to_string()),
    }
}

pub(crate) fn source_decorator_error_to_core(error: &SourceDecoratorError) -> CoreError {
    match error {
        SourceDecoratorError::InvalidInput(detail) => CoreError::InvalidInput(detail.clone()),
        SourceDecoratorError::FailedPrecondition(detail) => {
            CoreError::FailedPrecondition(detail.clone())
        }
    }
}

fn provider_error_to_core(error: &ProviderQueryError) -> CoreError {
    CoreError::QueryFailure(Box::new(error.to_structured()))
}
