//! App-owned selection of optional engine extensions for query runtime builds.

use coral_engine::{EngineExtensions, QuerySource};

/// App-layer provider that selects engine extensions for one runtime build.
pub trait EngineExtensionsProvider: Send + Sync {
    /// Returns the engine extensions that apply to the selected sources.
    fn extensions_for(&self, sources: &[QuerySource]) -> EngineExtensions;
}

/// Default OSS provider that installs no engine extensions.
#[derive(Debug, Default)]
pub struct NoopEngineExtensionsProvider;

impl EngineExtensionsProvider for NoopEngineExtensionsProvider {
    fn extensions_for(&self, _sources: &[QuerySource]) -> EngineExtensions {
        EngineExtensions::default()
    }
}
