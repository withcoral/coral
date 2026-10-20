//! Universal Search's database features at boot.
//!
//! The shared migration chain creates Universal Search's tables and the
//! text-search objects each backend needs: generated vectors, trigram indexes
//! and statistics tables on Postgres, an FTS5 table and its triggers on
//! `SQLite`. Those objects need database features that a migration cannot
//! create or explain. [`prepare_storage`] checks them before the chain runs.

mod repository;
#[cfg(test)]
mod tests;

use coral_db::{Database, DatabaseSystem};

/// A failure to prepare Universal Search's storage at boot.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// The database lacks a feature the search tables need.
    #[error("{database} does not provide required search feature: {feature} ({cause}); {remedy}")]
    MissingFeature {
        /// The backend and its version.
        database: String,
        /// The missing feature.
        feature: &'static str,
        /// The backend's answer to the attempt to use or create the feature.
        cause: String,
        /// What the operator can do about it.
        remedy: &'static str,
    },
    /// Database access failed.
    #[error(transparent)]
    Database(#[from] coral_db::Error),
}

/// Checks the database features Universal Search needs, and creates
/// `pg_trgm` when it is missing and this role may.
///
/// Process bootstrap calls this once, right before the shared migration
/// chain, whose search steps use these features.
///
/// # Errors
///
/// Returns [`StorageError::MissingFeature`] naming the feature when `pg_trgm`,
/// FTS5 or its trigram tokenizer is missing.
pub async fn prepare_storage(database: &Database) -> Result<(), StorageError> {
    match database.system() {
        DatabaseSystem::Postgres => check_postgres_features(probe_postgres(database).await?),
        DatabaseSystem::Sqlite => check_sqlite_features(probe_sqlite(database).await?),
    }
}

/// Whether the database provides one feature, and if not, why.
enum FeatureProbe {
    Present,
    Missing { cause: String },
}

/// What the Postgres probe found out about `pg_trgm`.
struct PostgresFeatures {
    server_version: String,
    pg_trgm: FeatureProbe,
}

/// Reads whether `pg_trgm` exists, and creates it when it does not and this
/// role may.
///
/// A role without `CREATE` on the database, or a second process creating the
/// extension at the same time, fails to create it. A second read then
/// decides; the failure only explains an extension that is still missing.
async fn probe_postgres(database: &Database) -> Result<PostgresFeatures, StorageError> {
    let mut transaction = database.begin_read_snapshot().await?;
    let server_version = repository::postgres_server_version(&mut transaction).await?;
    let installed = repository::pg_trgm_installed(&mut transaction).await?;
    transaction.rollback().await?;
    if installed {
        return Ok(PostgresFeatures {
            server_version,
            pg_trgm: FeatureProbe::Present,
        });
    }

    let mut transaction = database.begin().await?;
    let pg_trgm = match repository::create_pg_trgm(&mut transaction).await {
        Ok(()) => {
            transaction.commit().await?;
            FeatureProbe::Present
        }
        Err(error) => {
            transaction.rollback().await?;
            let mut transaction = database.begin_read_snapshot().await?;
            let installed = repository::pg_trgm_installed(&mut transaction).await?;
            transaction.rollback().await?;
            if installed {
                FeatureProbe::Present
            } else {
                FeatureProbe::Missing {
                    cause: error.to_string(),
                }
            }
        }
    };
    Ok(PostgresFeatures {
        server_version,
        pg_trgm,
    })
}

fn check_postgres_features(probe: PostgresFeatures) -> Result<(), StorageError> {
    match probe.pg_trgm {
        FeatureProbe::Present => Ok(()),
        FeatureProbe::Missing { cause } => Err(StorageError::MissingFeature {
            database: format!("Postgres {}", probe.server_version),
            feature: "pg_trgm extension",
            cause,
            remedy: "install pg_trgm in a schema on Coral's search path, or let Coral's database role create it",
        }),
    }
}

/// What the `SQLite` probes found out about FTS5 and its trigram tokenizer.
struct SqliteFeatures {
    version: String,
    fts5: FeatureProbe,
    trigram_tokenizer: FeatureProbe,
}

/// Probes FTS5 and its trigram tokenizer on temporary virtual tables, then
/// rolls them back.
async fn probe_sqlite(database: &Database) -> Result<SqliteFeatures, StorageError> {
    let mut transaction = database.begin().await?;
    let version = repository::sqlite_version(&mut transaction).await?;
    let fts5 = repository::probe_fts5(&mut transaction).await?;
    let trigram_tokenizer = repository::probe_trigram_tokenizer(&mut transaction).await?;
    transaction.rollback().await?;
    Ok(SqliteFeatures {
        version,
        fts5,
        trigram_tokenizer,
    })
}

fn check_sqlite_features(probe: SqliteFeatures) -> Result<(), StorageError> {
    let (feature, cause) = match (probe.fts5, probe.trigram_tokenizer) {
        (FeatureProbe::Present, FeatureProbe::Present) => return Ok(()),
        (FeatureProbe::Missing { cause }, _) => ("FTS5", cause),
        (FeatureProbe::Present, FeatureProbe::Missing { cause }) => {
            ("FTS5 trigram tokenizer", cause)
        }
    };
    Err(StorageError::MissingFeature {
        database: format!("SQLite {}", probe.version),
        feature,
        cause,
        remedy: "use a Coral build whose SQLite includes FTS5 with the trigram tokenizer",
    })
}
