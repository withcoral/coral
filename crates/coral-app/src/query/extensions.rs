//! App-owned selection of optional engine extensions for query runtime builds.

use coral_engine::{EngineExtensions, QuerySource};

/// App-layer provider that selects engine extensions for one runtime build.
pub trait EngineExtensionsProvider: Send + Sync {
    /// Returns the extensions to install for a runtime built from exactly
    /// `selected_sources`.
    ///
    /// Returned extensions may act on only a subset of those sources, but they
    /// must be valid for the full selected-source set of this runtime build.
    fn extensions_for(&self, selected_sources: &[QuerySource]) -> EngineExtensions;
}

/// Default OSS provider that installs no engine extensions.
#[derive(Debug, Default)]
pub struct NoopEngineExtensionsProvider;

impl EngineExtensionsProvider for NoopEngineExtensionsProvider {
    fn extensions_for(&self, _selected_sources: &[QuerySource]) -> EngineExtensions {
        EngineExtensions::default()
    }
}
