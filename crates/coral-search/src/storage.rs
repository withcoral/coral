//! Universal Search's database features and vector table at boot.
//!
//! The shared migration chain creates Universal Search's tables and the
//! text-search objects each backend needs: generated vectors, trigram indexes
//! and statistics tables on Postgres, an FTS5 table and its triggers on
//! `SQLite`. Those objects need database features that a migration cannot
//! create or explain. [`prepare_storage`] checks them before the chain runs,
//! and pgvector too while the vector side is on. [`prepare_vector_table`]
//! keeps the table of Search Entity vectors on Postgres after the chain runs.

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
/// `pg_trgm`, and `vector` while the vector side is on, when one is missing
/// and this role may.
///
/// Process bootstrap calls this once, right before the shared migration
/// chain, whose search steps use these features. On `SQLite` the vector side
/// changes nothing yet.
///
/// # Errors
///
/// Returns [`StorageError::MissingFeature`] naming the feature when `pg_trgm`,
/// `vector` while the vector side is on, FTS5 or its trigram tokenizer is
/// missing.
pub async fn prepare_storage(
    database: &Database,
    vectors: VectorStorage,
) -> Result<(), StorageError> {
    match database.system() {
        DatabaseSystem::Postgres => {
            check_postgres_features(probe_postgres(database, vectors).await?)
        }
        DatabaseSystem::Sqlite => check_sqlite_features(probe_sqlite(database).await?),
    }
}

/// Whether the database provides one feature, and if not, why.
#[derive(Debug)]
enum FeatureProbe {
    Present,
    Missing { cause: String },
}

/// A Postgres extension that Universal Search needs.
#[derive(Debug, Clone, Copy)]
enum Extension {
    /// Trigram matching, for the text search steps of the migration chain.
    PgTrgm,
    /// pgvector, for the vector table. The table and its query name
    /// `halfvec` without a schema, so the probe asks for that type on the
    /// search path, not only for the extension in the database.
    Vector,
}

/// What the Postgres probe found out about the extensions.
struct PostgresFeatures {
    server_version: String,
    pg_trgm: FeatureProbe,
    /// `None` when the vector side is off and nothing probed it.
    vector: Option<FeatureProbe>,
}

async fn probe_postgres(
    database: &Database,
    vectors: VectorStorage,
) -> Result<PostgresFeatures, StorageError> {
    let mut transaction = database.begin_read_snapshot().await?;
    let server_version = repository::postgres_server_version(&mut transaction).await?;
    transaction.rollback().await?;
    let pg_trgm = probe_extension(database, Extension::PgTrgm).await?;
    let vector = match vectors {
        VectorStorage::Off => None,
        VectorStorage::On { .. } => Some(probe_extension(database, Extension::Vector).await?),
    };
    Ok(PostgresFeatures {
        server_version,
        pg_trgm,
        vector,
    })
}

/// Reads whether `extension` is usable, and creates it when it is not and
/// this role may.
///
/// A role that may not create it, an extension in a schema off the search
/// path, or a second process creating it at the same time fails the create.
/// A second read then decides; the failure only explains an extension that
/// is still not usable.
async fn probe_extension(
    database: &Database,
    extension: Extension,
) -> Result<FeatureProbe, StorageError> {
    let mut transaction = database.begin_read_snapshot().await?;
    let usable = repository::extension_usable(&mut transaction, extension).await?;
    transaction.rollback().await?;
    if usable {
        return Ok(FeatureProbe::Present);
    }

    let mut transaction = database.begin().await?;
    match repository::create_extension(&mut transaction, extension).await {
        Ok(()) => {
            transaction.commit().await?;
            Ok(FeatureProbe::Present)
        }
        Err(error) => {
            transaction.rollback().await?;
            let mut transaction = database.begin_read_snapshot().await?;
            let usable = repository::extension_usable(&mut transaction, extension).await?;
            transaction.rollback().await?;
            Ok(if usable {
                FeatureProbe::Present
            } else {
                FeatureProbe::Missing {
                    cause: error.to_string(),
                }
            })
        }
    }
}

fn check_postgres_features(probe: PostgresFeatures) -> Result<(), StorageError> {
    let (feature, cause, remedy) = match (probe.pg_trgm, probe.vector) {
        (FeatureProbe::Missing { cause }, _) => (
            "pg_trgm extension",
            cause,
            "install pg_trgm in a schema on Coral's search path, or let Coral's database role create it",
        ),
        (FeatureProbe::Present, Some(FeatureProbe::Missing { cause })) => (
            "vector extension",
            cause,
            "install pgvector 0.7 or later in a schema on Coral's search path, let Coral's \
             database role create it, or set `[search] semantic_search_enabled = false`",
        ),
        (FeatureProbe::Present, None | Some(FeatureProbe::Present)) => return Ok(()),
    };
    Err(StorageError::MissingFeature {
        database: format!("Postgres {}", probe.server_version),
        feature,
        cause,
        remedy,
    })
}

/// Universal Search's vector side at boot, as `[search]
/// semantic_search_enabled` and the model manifest decide it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorStorage {
    /// The setting is off: boot checks no vector feature and creates no
    /// vector table.
    Off,
    /// Each Search Entity can keep one vector of this many dimensions.
    On {
        /// The manifest's vector length.
        dimension: usize,
    },
}

/// Creates the vector table on Postgres, or drops and creates it again when
/// this binary would create it with another statement, such as one for
/// another dimension. A table this binary would create is kept with its
/// vectors.
///
/// The table lives outside the migration chain: a migration that names a
/// vector type fails on a Postgres without pgvector, where Coral with the
/// setting off must start. Its comment records the statement that created
/// it. Process bootstrap calls this once, right after the chain: a table in
/// a schema that has no state marker yet reads as pre-v1 state at the next
/// boot.
///
/// # Errors
///
/// Returns [`StorageError::Database`] when the table cannot be read, dropped
/// or created.
pub async fn prepare_vector_table(
    database: &Database,
    vectors: VectorStorage,
) -> Result<(), StorageError> {
    let VectorStorage::On { dimension } = vectors else {
        return Ok(());
    };
    if database.system() != DatabaseSystem::Postgres {
        return Ok(());
    }
    let statement = repository::vector_table_statement(dimension);
    let mut transaction = database.begin().await?;
    // Read committed: the read below sees what a boot that held the lock
    // first committed.
    repository::lock_vector_table(&mut transaction).await?;
    match repository::vector_table_created_by(&mut transaction).await? {
        Some(Some(existing)) if existing == statement => {
            transaction.rollback().await?;
            return Ok(());
        }
        Some(_) => repository::drop_vector_table(&mut transaction).await?,
        None => {}
    }
    repository::create_vector_table(&mut transaction, &statement).await?;
    transaction.commit().await?;
    Ok(())
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
