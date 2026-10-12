use sqlx::migrate::Migrator;

use crate::backend::DatabaseBackend;
use crate::{Database, Error};

impl Database {
    /// Validates and applies one domain-owned migration plan.
    ///
    /// Applied versions and checksums are validated on every call. Migration
    /// SQL must name every object directly; callers must not use
    /// `IF NOT EXISTS` to conceal unexpected schema state.
    ///
    /// # Errors
    ///
    /// Returns an error for changed or missing applied migrations, migration
    /// SQL failure, or a backend connection failure.
    pub async fn migrate(&self, migrator: &Migrator) -> Result<(), Error> {
        match &self.backend {
            DatabaseBackend::Sqlite { writes, .. } => migrator.run(writes).await?,
            DatabaseBackend::Postgres(pool) => migrator.run(pool).await?,
        }
        Ok(())
    }
}
