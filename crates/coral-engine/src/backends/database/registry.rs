//! Workspace-scoped database connection registry.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::error::Result as DataFusionResult;
use datafusion_table_providers::sql::db_connection_pool::mysqlpool::MySQLConnectionPool;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use tokio::sync::{Mutex as AsyncMutex, OwnedRwLockReadGuard, RwLock};

/// Reusable remote database connection pools for one workspace.
///
/// Coral's application layer creates one registry per workspace and passes it
/// into every query runtime built for that workspace.
pub struct DatabasePoolRegistry {
    catalogs: Mutex<HashMap<String, Arc<CatalogPoolState>>>,
}

impl DatabasePoolRegistry {
    /// Creates an empty workspace database-pool map.
    #[must_use]
    pub fn new() -> Self {
        Self {
            catalogs: Mutex::new(HashMap::new()),
        }
    }

    fn catalog_state(&self, catalog_name: &str) -> Arc<CatalogPoolState> {
        Arc::clone(
            self.catalogs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry(catalog_name.to_string())
                .or_insert_with(|| Arc::new(CatalogPoolState::new())),
        )
    }

    pub(super) async fn begin_registration(
        &self,
        catalog_name: &str,
    ) -> DatabaseRegistrationAttempt {
        let state = self.catalog_state(catalog_name);
        let registration = Arc::clone(&state.registrations).read_owned().await;
        let successful_registrations = state.successful_registrations.load(Ordering::Acquire);
        DatabaseRegistrationAttempt {
            state,
            registration,
            successful_registrations,
        }
    }

    /// Removes one catalog's pool without disturbing other workspace sources.
    pub fn remove_catalog(&self, catalog_name: &str) {
        if let Some(state) = self
            .catalogs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(catalog_name)
        {
            state
                .pool
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
        }
    }
}

impl Default for DatabasePoolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

struct CatalogPoolState {
    pool: Mutex<Option<DatabasePool>>,
    initialization: AsyncMutex<()>,
    registrations: Arc<RwLock<()>>,
    successful_registrations: AtomicU64,
}

impl CatalogPoolState {
    fn new() -> Self {
        Self {
            pool: Mutex::new(None),
            initialization: AsyncMutex::new(()),
            registrations: Arc::new(RwLock::new(())),
            successful_registrations: AtomicU64::new(0),
        }
    }

    async fn get_or_create(
        &self,
        create: impl Future<Output = DataFusionResult<DatabasePool>>,
    ) -> DataFusionResult<DatabasePool> {
        if let Some(pool) = self
            .pool
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
        {
            return Ok(pool);
        }

        let _initialization = self.initialization.lock().await;
        if let Some(pool) = self
            .pool
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
        {
            return Ok(pool);
        }

        let pool = create.await?;
        self.pool
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replace(pool.clone());
        Ok(pool)
    }
}

pub(super) struct DatabaseRegistrationAttempt {
    state: Arc<CatalogPoolState>,
    registration: OwnedRwLockReadGuard<()>,
    successful_registrations: u64,
}

impl DatabaseRegistrationAttempt {
    pub(super) async fn get_or_create(
        &self,
        create: impl Future<Output = DataFusionResult<DatabasePool>>,
    ) -> DataFusionResult<DatabasePool> {
        self.state.get_or_create(create).await
    }

    pub(super) fn succeeded(self) {
        self.state
            .successful_registrations
            .fetch_add(1, Ordering::Release);
    }

    pub(super) async fn evict_after_timeout(self) {
        let Self {
            state,
            registration,
            successful_registrations,
        } = self;
        drop(registration);

        let _exclusive_registration = Arc::clone(&state.registrations).write_owned().await;
        if state.successful_registrations.load(Ordering::Acquire) == successful_registrations {
            state
                .pool
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
        }
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
    use std::future::poll_fn;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Poll;

    use datafusion::error::Result as DataFusionResult;
    use futures::poll;

    use super::{DatabasePool, DatabasePoolRegistry};

    #[tokio::test]
    async fn concurrent_get_or_create_starts_only_one_constructor() {
        let registry = DatabasePoolRegistry::new();
        let creations = Arc::new(AtomicUsize::new(0));
        let create = || {
            let creations = Arc::clone(&creations);
            poll_fn(move |_context| {
                creations.fetch_add(1, Ordering::SeqCst);
                Poll::<DataFusionResult<DatabasePool>>::Pending
            })
        };

        let first_registration = registry.begin_registration("orders").await;
        let mut first = std::pin::pin!(first_registration.get_or_create(create()));
        assert!(poll!(&mut first).is_pending());

        let second_registration = registry.begin_registration("orders").await;
        let mut second = std::pin::pin!(second_registration.get_or_create(create()));
        assert!(poll!(&mut second).is_pending());

        assert_eq!(creations.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn different_catalogs_initialize_concurrently() {
        let registry = DatabasePoolRegistry::new();
        let creations = Arc::new(AtomicUsize::new(0));
        let create = || {
            let creations = Arc::clone(&creations);
            poll_fn(move |_context| {
                creations.fetch_add(1, Ordering::SeqCst);
                Poll::<DataFusionResult<DatabasePool>>::Pending
            })
        };

        let orders_registration = registry.begin_registration("orders").await;
        let mut orders = std::pin::pin!(orders_registration.get_or_create(create()));
        assert!(poll!(&mut orders).is_pending());

        let inventory_registration = registry.begin_registration("inventory").await;
        let mut inventory = std::pin::pin!(inventory_registration.get_or_create(create()));
        assert!(poll!(&mut inventory).is_pending());

        assert_eq!(creations.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn canceled_initializer_allows_a_waiter_to_retry() {
        let registry = DatabasePoolRegistry::new();
        let creations = Arc::new(AtomicUsize::new(0));
        let first_registration = registry.begin_registration("orders").await;
        let first_creations = Arc::clone(&creations);
        let mut first = Box::pin(first_registration.get_or_create(poll_fn(move |_context| {
            first_creations.fetch_add(1, Ordering::SeqCst);
            Poll::<DataFusionResult<DatabasePool>>::Pending
        })));
        assert!(poll!(&mut first).is_pending());
        drop(first);

        let second_registration = registry.begin_registration("orders").await;
        let second_creations = Arc::clone(&creations);
        second_registration
            .get_or_create(async move {
                second_creations.fetch_add(1, Ordering::SeqCst);
                Ok(DatabasePool::Test)
            })
            .await
            .expect("waiter should become the initializer");

        assert_eq!(creations.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn timeout_without_a_successful_peer_evicts_the_pool() {
        let registry = DatabasePoolRegistry::new();
        let creations = Arc::new(AtomicUsize::new(0));

        let first_registration = registry.begin_registration("orders").await;
        let first_creations = Arc::clone(&creations);
        first_registration
            .get_or_create(async move {
                first_creations.fetch_add(1, Ordering::SeqCst);
                Ok(DatabasePool::Test)
            })
            .await
            .expect("first pool");
        first_registration.evict_after_timeout().await;

        let retry_registration = registry.begin_registration("orders").await;
        let retry_creations = Arc::clone(&creations);
        retry_registration
            .get_or_create(async move {
                retry_creations.fetch_add(1, Ordering::SeqCst);
                Ok(DatabasePool::Test)
            })
            .await
            .expect("fresh retry pool");

        assert_eq!(creations.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn removing_one_catalog_pool_preserves_other_catalogs() {
        let registry = DatabasePoolRegistry::new();
        let orders_registration = registry.begin_registration("orders").await;
        orders_registration
            .get_or_create(async { Ok(DatabasePool::Test) })
            .await
            .expect("orders pool");
        orders_registration.succeeded();
        let inventory_registration = registry.begin_registration("inventory").await;
        inventory_registration
            .get_or_create(async { Ok(DatabasePool::Test) })
            .await
            .expect("inventory pool");
        inventory_registration.succeeded();

        registry.remove_catalog("orders");

        let catalogs = registry
            .catalogs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(!catalogs.contains_key("orders"));
        assert!(
            catalogs
                .get("inventory")
                .expect("inventory state")
                .pool
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_some()
        );
    }
}
