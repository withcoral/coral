//! Workspace-scoped database connection registry.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use datafusion::error::Result as DataFusionResult;
use datafusion_table_providers::sql::db_connection_pool::mysqlpool::MySQLConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;

/// Reusable remote database connection pools for one workspace.
///
/// Coral's application layer creates one registry per workspace and passes it
/// into every query runtime built for that workspace.
pub struct DatabasePoolRegistry {
    pools: Mutex<HashMap<String, DatabasePool>>,
}

impl DatabasePoolRegistry {
    /// Creates an empty workspace database-pool map.
    #[must_use]
    pub fn new() -> Self {
        Self {
            pools: Mutex::new(HashMap::new()),
        }
    }

    pub(super) async fn get_or_create(
        &self,
        catalog_name: &str,
        create: impl Future<Output = DataFusionResult<DatabasePool>>,
    ) -> DataFusionResult<DatabasePool> {
        if let Some(pool) = self
            .pools
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(catalog_name)
            .cloned()
        {
            return Ok(pool);
        }

        let pool = create.await?;
        let mut pools = self
            .pools
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(pools
            .entry(catalog_name.to_string())
            .or_insert(pool)
            .clone())
    }

    /// Removes one catalog's pool without disturbing other workspace sources.
    pub fn remove_catalog(&self, catalog_name: &str) {
        self.pools
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(catalog_name);
    }
}

impl Default for DatabasePoolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub(super) enum DatabasePool {
    Postgres(Arc<PostgresConnectionPool>),
    MySql(Arc<MySQLConnectionPool>),
    #[cfg(test)]
    Test,
}

#[cfg(test)]
mod tests {
    use super::{DatabasePool, DatabasePoolRegistry};

    #[test]
    fn removing_one_catalog_pool_preserves_other_catalogs() {
        let registry = DatabasePoolRegistry::new();
        {
            let mut pools = registry
                .pools
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            pools.insert("orders".to_string(), DatabasePool::Test);
            pools.insert("inventory".to_string(), DatabasePool::Test);
        }

        registry.remove_catalog("orders");

        let pools = registry
            .pools
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(!pools.contains_key("orders"));
        assert!(pools.contains_key("inventory"));
    }
}
