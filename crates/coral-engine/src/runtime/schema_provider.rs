//! Static schema provider used for the source metadata schema.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;

/// Immutable schema provider backed by a fixed set of in-memory tables.
#[derive(Debug)]
pub(crate) struct StaticSchemaProvider {
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
}

impl StaticSchemaProvider {
    #[must_use]
    /// Builds a schema provider from the supplied table map.
    pub(crate) fn new(tables: HashMap<String, Arc<dyn TableProvider>>) -> Self {
        Self {
            tables: RwLock::new(tables),
        }
    }
}

#[async_trait]
impl SchemaProvider for StaticSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let tables = self.tables.read().expect("tables lock poisoned");
        let mut names: Vec<String> = tables.keys().cloned().collect();
        names.sort();
        names
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let tables = self.tables.read().expect("tables lock poisoned");
        Ok(tables.get(name).cloned())
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        let mut tables = self.tables.write().expect("tables lock poisoned");
        Ok(tables.insert(name, table))
    }

    fn table_exist(&self, name: &str) -> bool {
        let tables = self.tables.read().expect("tables lock poisoned");
        tables.contains_key(name)
    }
}
