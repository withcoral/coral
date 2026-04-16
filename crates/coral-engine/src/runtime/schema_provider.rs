//! Static schema provider used for the source metadata schema.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;

/// Immutable schema provider backed by a fixed set of in-memory tables.
#[derive(Debug)]
pub(crate) struct StaticSchemaProvider {
    tables: HashMap<String, Arc<dyn TableProvider>>,
}

impl StaticSchemaProvider {
    #[must_use]
    /// Builds a schema provider from the supplied table map.
    pub(crate) fn new(tables: HashMap<String, Arc<dyn TableProvider>>) -> Self {
        Self { tables }
    }
}

#[async_trait]
impl SchemaProvider for StaticSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.tables.keys().cloned().collect();
        names.sort();
        names
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        Ok(self.tables.get(name).cloned())
    }

    fn register_table(
        &self,
        _name: String,
        _table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        Err(datafusion::error::DataFusionError::Execution(
            "static schema provider does not support register_table".to_string(),
        ))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::catalog::SchemaProvider;
    use datafusion::datasource::MemTable;

    use super::StaticSchemaProvider;

    #[test]
    fn rejects_runtime_table_registration() {
        let provider = StaticSchemaProvider::new(HashMap::default());
        let table = Arc::new(
            MemTable::try_new(arrow::datatypes::Schema::empty().into(), vec![vec![]])
                .expect("mem table"),
        );

        let error = provider
            .register_table("demo".to_string(), table)
            .expect_err("static schema provider should reject mutation");

        assert!(
            error
                .to_string()
                .contains("does not support register_table")
        );
    }
}
