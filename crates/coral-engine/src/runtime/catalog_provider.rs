//! Static catalog provider used for source-as-catalog registration.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::error::Result;

/// Immutable catalog provider backed by a fixed set of schemas.
#[derive(Debug)]
pub(crate) struct StaticCatalogProvider {
    schemas: HashMap<String, Arc<dyn SchemaProvider>>,
}

impl StaticCatalogProvider {
    #[must_use]
    /// Builds a catalog provider from the supplied schema map.
    pub(crate) fn new(schemas: HashMap<String, Arc<dyn SchemaProvider>>) -> Self {
        Self { schemas }
    }
}

#[async_trait]
impl CatalogProvider for StaticCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let mut names = self.schemas.keys().cloned().collect::<Vec<_>>();
        names.sort();
        names
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).cloned()
    }

    fn register_schema(
        &self,
        _name: &str,
        _schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        Err(datafusion::error::DataFusionError::Execution(
            "static catalog provider does not support register_schema".to_string(),
        ))
    }

    fn deregister_schema(
        &self,
        _name: &str,
        _cascade: bool,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        Err(datafusion::error::DataFusionError::Execution(
            "static catalog provider does not support deregister_schema".to_string(),
        ))
    }
}
