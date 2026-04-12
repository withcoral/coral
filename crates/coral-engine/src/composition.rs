//! Advanced composition seams for source registration.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::datasource::TableProvider;

/// One source's table providers keyed by manifest table name.
pub type SourceTables = HashMap<String, Arc<dyn TableProvider>>;

/// Neutral error type for source-decoration failures.
#[derive(Debug, thiserror::Error)]
pub enum SourceDecoratorError {
    /// The decorator was configured with invalid input.
    #[error("{0}")]
    InvalidInput(String),
    /// The decorator could not proceed because a precondition was unmet.
    #[error("{0}")]
    FailedPrecondition(String),
}

impl SourceDecoratorError {
    #[must_use]
    /// Builds an invalid-input error.
    pub fn invalid_input(detail: impl Into<String>) -> Self {
        Self::InvalidInput(detail.into())
    }

    #[must_use]
    /// Builds a failed-precondition error.
    pub fn failed_precondition(detail: impl Into<String>) -> Self {
        Self::FailedPrecondition(detail.into())
    }
}

/// Registration-time hook for wrapping or replacing a source's table providers.
///
/// Decorators operate only on successfully registered source tables. They do
/// not participate in source registration failure policy.
pub trait SourceDecorator: Send + Sync {
    /// Stable decorator name used in diagnostics.
    fn name(&self) -> &'static str;

    /// Performs one-time setup before any sources are registered.
    ///
    /// # Errors
    ///
    /// Returns [`SourceDecoratorError`] if the decorator cannot initialize.
    fn prepare(&mut self) -> Result<(), SourceDecoratorError> {
        Ok(())
    }

    /// Decorates the registered tables for one source before catalog insertion.
    ///
    /// # Errors
    ///
    /// Returns [`SourceDecoratorError`] if the tables cannot be decorated.
    fn decorate_source(
        &mut self,
        schema_name: &str,
        tables: SourceTables,
    ) -> Result<SourceTables, SourceDecoratorError>;

    /// Performs final validation after all source registration attempts finish.
    ///
    /// # Errors
    ///
    /// Returns [`SourceDecoratorError`] if final invariants are not satisfied.
    fn finish(&mut self) -> Result<(), SourceDecoratorError> {
        Ok(())
    }
}
