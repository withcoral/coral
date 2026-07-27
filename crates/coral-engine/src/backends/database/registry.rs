//! Workspace-scoped database connection registry.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use datafusion::error::Result as DataFusionResult;
use datafusion_table_providers::sql::db_connection_pool::mysqlpool::MySQLConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use sha2::{Digest as _, Sha256};

/// Reusable remote database connection pools for one workspace.
///
/// Coral's application layer creates one registry per workspace and passes it
/// into every query runtime built for that workspace.
pub struct DatabasePoolRegistry {
    pools: Mutex<HashMap<PoolId, DatabasePool>>,
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
        id: PoolId,
        create: impl Future<Output = DataFusionResult<DatabasePool>>,
    ) -> DataFusionResult<DatabasePool> {
        {
            let mut pools = self
                .pools
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            pools.retain(|existing, _pool| {
                existing.catalog_name != id.catalog_name || existing == &id
            });
            if let Some(pool) = pools.get(&id).cloned() {
                return Ok(pool);
            }
        }

        let pool = create.await?;
        let mut pools = self
            .pools
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(pools.entry(id).or_insert(pool).clone())
    }

    /// Removes one catalog's pool without disturbing other workspace sources.
    pub fn remove_catalog(&self, catalog_name: &str) {
        self.pools
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|id, _pool| id.catalog_name != catalog_name);
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

/// Identity of one resolved connection configuration for a SQL catalog.
///
/// Catalog names survive source replacement. Including the connection
/// fingerprint prevents an in-flight build using an old source definition from
/// publishing a pool that a replacement source could subsequently reuse.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct PoolId {
    catalog_name: String,
    connection_fingerprint: [u8; 32],
}

impl PoolId {
    pub(super) fn new(
        catalog_name: &str,
        provider: &str,
        params: &HashMap<String, String>,
    ) -> Self {
        let mut sorted = params.iter().collect::<Vec<_>>();
        sorted.sort();
        let mut hasher = Sha256::new();
        hash_pool_id_component(&mut hasher, provider);
        for (key, value) in sorted {
            hash_pool_id_component(&mut hasher, key);
            hash_pool_id_component(&mut hasher, value);
        }
        Self {
            catalog_name: catalog_name.to_string(),
            connection_fingerprint: hasher.finalize().into(),
        }
    }
}

fn hash_pool_id_component(hasher: &mut Sha256, value: &str) {
    hasher.update((value.len() as u64).to_le_bytes());
    hasher.update(value.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::{DatabasePool, DatabasePoolRegistry, PoolId};

    #[test]
    fn removing_one_catalog_pool_preserves_other_catalogs() {
        let registry = DatabasePoolRegistry::new();
        {
            let mut pools = registry
                .pools
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            pools.insert(
                PoolId {
                    catalog_name: "orders".to_string(),
                    connection_fingerprint: [1; 32],
                },
                DatabasePool::Test,
            );
            pools.insert(
                PoolId {
                    catalog_name: "inventory".to_string(),
                    connection_fingerprint: [2; 32],
                },
                DatabasePool::Test,
            );
        }

        registry.remove_catalog("orders");

        let pools = registry
            .pools
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(!pools.keys().any(|id| id.catalog_name == "orders"));
        assert!(pools.keys().any(|id| id.catalog_name == "inventory"));
    }
}
