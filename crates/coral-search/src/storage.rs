//! Universal Search's database features and vector table at boot.
//!
//! The shared migration chain creates Universal Search's tables and the
//! text-search objects each backend needs: generated `tsvector` columns, trigram indexes
//! and statistics tables on Postgres, an FTS5 table and its triggers on
//! `SQLite`. Those objects need database features that a migration cannot
//! ensure or explain. [`prepare_storage`] checks them before the chain runs,
//! and pgvector or sqlite-vec too while the vector side is on.
//! [`prepare_vector_table`] keeps the table of Search Entity embeddings after
//! the chain runs: a pgvector table on Postgres; on `SQLite`, a key table
//! beside a sqlite-vec `vec0` table of the vectors.

mod repository;
#[cfg(test)]
mod tests;

use coral_db::{Database, DatabaseSystem};

use repository::VectorTable;
pub(crate) use repository::vector_literal;

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
    /// The vector table has no `embedding` column, or one that is not a
    /// `halfvec` of a fixed dimension. Boot keeps the table as it is.
    #[error(
        "search vector table search_entity_embeddings needs an embedding column of type \
         halfvec({expected_dimension}), found {}; drop the table and the next boot creates it again",
        .column_type.as_deref().unwrap_or("no such column")
    )]
    UnexpectedVectorTable {
        /// The `embedding` column's type as Postgres names it, or `None`
        /// when the table has no such column.
        column_type: Option<String>,
        /// The manifest's vector length.
        expected_dimension: usize,
    },
    /// On `SQLite`, one of the two vector tables alone, or a sqlite-vec
    /// table that Coral did not create. Boot keeps them as they are.
    #[error(
        "search vector tables search_entity_embeddings and search_entity_embedding_vectors \
         are not the ones Coral creates for vectors of {expected_dimension} dimensions; \
         drop both and the next boot creates them again"
    )]
    UnexpectedSqliteVectorTables {
        /// The manifest's vector length.
        expected_dimension: usize,
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
/// chain, whose search steps use these features. On `SQLite`, while the
/// vector side is on, it checks that sqlite-vec is loaded.
///
/// # Errors
///
/// Returns [`StorageError::MissingFeature`] naming the feature when `pg_trgm`,
/// FTS5 or its trigram tokenizer is missing, or, while the vector side is
/// on, `vector` or sqlite-vec.
pub async fn prepare_storage(
    database: &Database,
    vector_storage: VectorStorage,
) -> Result<(), StorageError> {
    match database.system() {
        DatabaseSystem::Postgres => {
            check_postgres_features(probe_postgres(database, vector_storage).await?)
        }
        DatabaseSystem::Sqlite => {
            check_sqlite_features(probe_sqlite(database, vector_storage).await?)
        }
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

impl Extension {
    const fn feature(self) -> &'static str {
        match self {
            Self::PgTrgm => "pg_trgm extension",
            Self::Vector => "vector extension",
        }
    }

    /// Why an extension this role just created is still not usable.
    const fn unusable_once_created(self) -> &'static str {
        match self {
            Self::PgTrgm => "the created extension is not in the database",
            Self::Vector => "the created extension has no `halfvec` type",
        }
    }

    const fn remedy(self) -> &'static str {
        match self {
            Self::PgTrgm => {
                "install pg_trgm in a schema on Coral's search path, or let Coral's database role create it"
            }
            Self::Vector => {
                "install pgvector 0.7 or later in a schema on Coral's search path, let Coral's \
                 database role create it, or set `[search] semantic_search_enabled = false`"
            }
        }
    }
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
    vector_storage: VectorStorage,
) -> Result<PostgresFeatures, StorageError> {
    let mut transaction = database.begin_read_snapshot().await?;
    let server_version = repository::postgres_server_version(&mut transaction).await?;
    transaction.rollback().await?;
    let pg_trgm = probe_extension(database, Extension::PgTrgm).await?;
    let vector = match vector_storage {
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
/// is still not usable. A create that succeeds is read again before it
/// commits.
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
        // pgvector before 0.7 creates `vector` without `halfvec`. The
        // creation is rolled back, so a refused boot changes nothing.
        Ok(()) if !repository::extension_usable(&mut transaction, extension).await? => {
            transaction.rollback().await?;
            Ok(FeatureProbe::Missing {
                cause: extension.unusable_once_created().to_string(),
            })
        }
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
    let (extension, cause) = match (probe.pg_trgm, probe.vector) {
        (FeatureProbe::Missing { cause }, _) => (Extension::PgTrgm, cause),
        (FeatureProbe::Present, Some(FeatureProbe::Missing { cause })) => {
            (Extension::Vector, cause)
        }
        (FeatureProbe::Present, None | Some(FeatureProbe::Present)) => return Ok(()),
    };
    Err(StorageError::MissingFeature {
        database: format!("Postgres {}", probe.server_version),
        feature: extension.feature(),
        cause,
        remedy: extension.remedy(),
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

/// Creates the vector table, or drops and creates it again when the
/// manifest's dimension changes. A table of the manifest's dimension is
/// kept with its vectors. On `SQLite` the vector table is two: the key table
/// and the sqlite-vec table, created and dropped together.
///
/// The table lives outside the migration chain: a migration that names a
/// vector type fails on a Postgres without pgvector, where Coral with the
/// setting off must start. Process bootstrap calls this once, right after
/// the chain: a table in a schema that has no state marker yet reads as
/// pre-v1 state at the next boot.
///
/// # Errors
///
/// Returns [`StorageError::UnexpectedVectorTable`] and keeps the table when
/// its `embedding` column is missing or is not a `halfvec` of a fixed
/// dimension, [`StorageError::UnexpectedSqliteVectorTables`] and keeps them
/// when one `SQLite` table is alone or the sqlite-vec table is not Coral's,
/// and
/// [`StorageError::Database`] when the table cannot be read, dropped or
/// created.
pub async fn prepare_vector_table(
    database: &Database,
    vector_storage: VectorStorage,
) -> Result<(), StorageError> {
    let VectorStorage::On { dimension } = vector_storage else {
        return Ok(());
    };
    let mut transaction = database.begin().await?;
    // Read committed: the read below sees what a boot that held the lock
    // first committed.
    repository::lock_vector_table(&mut transaction).await?;
    match repository::vector_table(&mut transaction, dimension).await? {
        VectorTable::SameDimension => {
            transaction.rollback().await?;
            return Ok(());
        }
        VectorTable::OtherDimension => {
            tracing::info!(
                "dropping the search vector table of another dimension; every Search Entity embeds again"
            );
            repository::drop_vector_table(&mut transaction).await?;
        }
        VectorTable::UnexpectedEmbedding { column_type } => {
            transaction.rollback().await?;
            return Err(StorageError::UnexpectedVectorTable {
                column_type,
                expected_dimension: dimension,
            });
        }
        VectorTable::UnexpectedSqliteTables => {
            transaction.rollback().await?;
            return Err(StorageError::UnexpectedSqliteVectorTables {
                expected_dimension: dimension,
            });
        }
        VectorTable::Absent => {}
    }
    repository::create_vector_table(&mut transaction, dimension).await?;
    transaction.commit().await?;
    Ok(())
}

/// What the `SQLite` probes found out about FTS5, its trigram tokenizer,
/// and sqlite-vec.
struct SqliteFeatures {
    version: String,
    fts5: FeatureProbe,
    trigram_tokenizer: FeatureProbe,
    /// `None` when the vector side is off and nothing probed it.
    vec0: Option<FeatureProbe>,
}

/// Probes FTS5, its trigram tokenizer, and sqlite-vec while the vector side
/// is on, on temporary virtual tables, then rolls them back.
async fn probe_sqlite(
    database: &Database,
    vector_storage: VectorStorage,
) -> Result<SqliteFeatures, StorageError> {
    let mut transaction = database.begin().await?;
    let version = repository::sqlite_version(&mut transaction).await?;
    let fts5 = repository::probe_fts5(&mut transaction).await?;
    let trigram_tokenizer = repository::probe_trigram_tokenizer(&mut transaction).await?;
    let vec0 = match vector_storage {
        VectorStorage::Off => None,
        VectorStorage::On { .. } => Some(repository::probe_vec0(&mut transaction).await?),
    };
    transaction.rollback().await?;
    Ok(SqliteFeatures {
        version,
        fts5,
        trigram_tokenizer,
        vec0,
    })
}

fn check_sqlite_features(probe: SqliteFeatures) -> Result<(), StorageError> {
    const TEXT_REMEDY: &str =
        "use a Coral build whose SQLite includes FTS5 with the trigram tokenizer";
    let (feature, cause, remedy) = match (probe.fts5, probe.trigram_tokenizer, probe.vec0) {
        (FeatureProbe::Missing { cause }, _, _) => ("FTS5", cause, TEXT_REMEDY),
        (FeatureProbe::Present, FeatureProbe::Missing { cause }, _) => {
            ("FTS5 trigram tokenizer", cause, TEXT_REMEDY)
        }
        (FeatureProbe::Present, FeatureProbe::Present, Some(FeatureProbe::Missing { cause })) => (
            "sqlite-vec",
            cause,
            "use a Coral build whose SQLite includes sqlite-vec, or set \
             `[search] semantic_search_enabled = false`",
        ),
        (FeatureProbe::Present, FeatureProbe::Present, None | Some(FeatureProbe::Present)) => {
            return Ok(());
        }
    };
    Err(StorageError::MissingFeature {
        database: format!("SQLite {}", probe.version),
        feature,
        cause,
        remedy,
    })
}
