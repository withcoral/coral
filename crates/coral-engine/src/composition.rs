//! Advanced composition seams for source registration.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::datasource::TableProvider;

use crate::{CoreError, contracts::QuerySource};

/// One source's table providers keyed by manifest table name.
pub type SourceTables = HashMap<String, Arc<dyn TableProvider>>;

/// Neutral bundle of optional engine extensions for one runtime build.
#[derive(Default)]
pub struct EngineExtensions {
    /// Registration-time table decorators for the selected source set.
    pub source_decorators: Vec<Box<dyn SourceDecorator>>,
}

/// Neutral policy decision for one source registration failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceFailurePolicy {
    /// The failure does not require aborting the runtime build.
    Ignore,
    /// The original source failure should abort the runtime build.
    Abort,
}

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
/// Decorators can wrap successfully registered source tables and may also
/// observe selected-source failures to decide whether runtime construction
/// should abort.
pub trait SourceDecorator: Send + Sync {
    /// Stable decorator name used in diagnostics.
    fn name(&self) -> &'static str;

    /// Performs one-time setup before any sources are registered.
    ///
    /// # Errors
    ///
    /// Returns [`SourceDecoratorError`] if the decorator cannot initialize.
    fn prepare(&mut self, _selected_sources: &[QuerySource]) -> Result<(), SourceDecoratorError> {
        Ok(())
    }

    /// Decorates the registered tables for one source before catalog insertion.
    ///
    /// # Errors
    ///
    /// Returns [`SourceDecoratorError`] if the tables cannot be decorated.
    fn decorate_source(
        &mut self,
        source: &QuerySource,
        tables: SourceTables,
    ) -> Result<SourceTables, SourceDecoratorError>;

    /// Reports a selected source that failed during registration.
    ///
    /// Returning [`SourceFailurePolicy::Abort`] causes the original source
    /// registration error to abort runtime construction.
    ///
    /// # Errors
    ///
    /// Returns [`SourceDecoratorError`] if the decorator cannot process the
    /// failure event.
    fn source_failed(
        &mut self,
        _source: &QuerySource,
        _error: &CoreError,
    ) -> Result<SourceFailurePolicy, SourceDecoratorError> {
        Ok(SourceFailurePolicy::Ignore)
    }

    /// Performs final validation after all source registration attempts finish.
    ///
    /// # Errors
    ///
    /// Returns [`SourceDecoratorError`] if final invariants are not satisfied.
    fn finish(&mut self) -> Result<(), SourceDecoratorError> {
        Ok(())
    }
}
