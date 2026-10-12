//! Universal Search's own storage at boot.
//!
//! The shared migration chain creates the portable search tables. Each backend
//! then needs text-search objects of its own: generated vectors, trigram
//! indexes and statistics tables on Postgres, an FTS5 table and its triggers on
//! `SQLite`. They live in one search-owned stream per backend with its own
//! ledger, `search_migrations`. [`prepare_storage`] checks the database features
//! the stream needs, then applies every step the ledger lacks, one transaction
//! per step together with its ledger row.

mod repository;
#[cfg(test)]
mod tests;

use coral_db::{Database, DatabaseSystem};

use repository::Stream;

/// A failure to prepare Universal Search's storage at boot.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// The database lacks a feature the search stream needs.
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
    /// The ledger records a stream version this binary does not know.
    #[error(
        "the {stream} search schema is at version {recorded}, but this Coral supports up to version {supported}; start a Coral release that supports it or use a matching database"
    )]
    UnsupportedStreamVersion {
        /// The backend stream.
        stream: &'static str,
        /// The newest version the ledger records.
        recorded: i64,
        /// The newest version this binary applies.
        supported: i64,
    },
    /// A stream step failed; nothing of it was kept or recorded.
    #[error(
        "the {stream} search schema step {version} failed and was rolled back: {source}; correct the cause and start Coral again to run the step again"
    )]
    StreamStep {
        /// The backend stream.
        stream: &'static str,
        /// The step's version.
        version: i64,
        /// The backend's error.
        #[source]
        source: coral_db::Error,
    },
    /// The system clock cannot stamp the ledger.
    #[error("system clock is before 1970 or past the durable nanosecond range")]
    Clock,
    /// Database access failed.
    #[error(transparent)]
    Database(#[from] coral_db::Error),
}

/// Checks the database features Universal Search needs, then brings the
/// backend's search stream up to this binary's version.
///
/// Process bootstrap calls this once, right after the shared migration chain,
/// before any Module serves. Each missing step runs once, in its own
/// transaction together with its ledger row: under a transaction-scoped
/// advisory lock on Postgres, in an immediate transaction on `SQLite`.
///
/// # Errors
///
/// Returns [`StorageError::MissingFeature`] naming the feature when `pg_trgm`,
/// FTS5 or its trigram tokenizer is missing,
/// [`StorageError::UnsupportedStreamVersion`] when the ledger is newer than
/// this binary, and [`StorageError::StreamStep`] when a step fails, including
/// on an object that already exists.
pub async fn prepare_storage(database: &Database) -> Result<(), StorageError> {
    match database.system() {
        DatabaseSystem::Postgres => check_postgres_features(probe_postgres(database).await?)?,
        DatabaseSystem::Sqlite => check_sqlite_features(probe_sqlite(database).await?)?,
    }
    apply_stream(database, repository::stream(database.system())).await
}

async fn apply_stream(database: &Database, stream: &Stream) -> Result<(), StorageError> {
    let supported = stream.supported_version();
    for step in stream.steps {
        let mut transaction = database.begin().await?;
        repository::lock_stream(&mut transaction).await?;
        let recorded = repository::recorded_version(&mut transaction, stream.name).await?;
        if recorded > supported {
            return Err(StorageError::UnsupportedStreamVersion {
                stream: stream.name,
                recorded,
                supported,
            });
        }
        if recorded >= step.version {
            transaction.rollback().await?;
            continue;
        }
        repository::apply_step(&mut transaction, step.script)
            .await
            .map_err(|source| StorageError::StreamStep {
                stream: stream.name,
                version: step.version,
                source,
            })?;
        repository::record_step(
            &mut transaction,
            stream.name,
            step.version,
            crate::unix_now_nanos().ok_or(StorageError::Clock)?,
        )
        .await?;
        transaction.commit().await?;
    }
    Ok(())
}

/// Applies the backend's stream up to `version` only, as a binary whose
/// newest step is `version` does at boot.
#[cfg(test)]
pub(crate) async fn prepare_storage_through(
    database: &Database,
    version: i64,
) -> Result<(), StorageError> {
    let stream = repository::stream(database.system());
    let count = stream
        .steps
        .iter()
        .take_while(|step| step.version <= version)
        .count();
    let steps = stream.steps.get(..count).unwrap_or_default();
    apply_stream(
        database,
        &Stream {
            name: stream.name,
            steps,
        },
    )
    .await
}

/// Records one stream version past this binary's newest step.
#[cfg(any(test, feature = "test-fixtures"))]
pub(crate) async fn record_unsupported_version(database: &Database) -> Result<(), StorageError> {
    let stream = repository::stream(database.system());
    let mut transaction = database.begin().await?;
    repository::record_step(
        &mut transaction,
        stream.name,
        stream.supported_version() + 1,
        0,
    )
    .await?;
    transaction.commit().await?;
    Ok(())
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
