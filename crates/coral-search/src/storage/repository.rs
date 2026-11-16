//! The boot statements: the feature probes and the vector table's, and how
//! a vector is written for pgvector.
//!
//! Every function joins the caller's transaction; the storage module owns
//! when each transaction begins, commits, or rolls back.

use std::fmt::Write as _;

use coral_db::{DatabaseSystem, Error, Transaction};
use sea_query::{Expr, ExprTrait as _, Iden, Query};

use super::{Extension, FeatureProbe};

/// `SQLite`'s primary result code for a generic error, which it reports for
/// an unknown module or tokenizer.
const SQLITE_ERROR: &str = "1";

/// Postgres's catalog of installed extensions.
#[derive(Iden)]
#[iden = "pg_catalog"]
enum PgCatalog {
    Table,
    PgExtension,
    Extname,
}

pub(super) async fn postgres_server_version(
    transaction: &mut Transaction<'_>,
) -> Result<String, Error> {
    let (version,): (String,) = transaction
        .fetch_optional(
            Query::select()
                .expr(Expr::cust("current_setting('server_version')"))
                .to_owned(),
        )
        .await?
        .ok_or(sqlx::Error::RowNotFound)?;
    Ok(version)
}

/// Whether `extension` is there: `pg_trgm` when the database has it, in any
/// schema; `vector` when its `halfvec` type is on the search path.
pub(super) async fn extension_usable(
    transaction: &mut Transaction<'_>,
    extension: Extension,
) -> Result<bool, Error> {
    let statement = match extension {
        Extension::PgTrgm => Query::select()
            .expr(Expr::val(1_i32))
            .from((PgCatalog::Table, PgCatalog::PgExtension))
            .and_where(Expr::col(PgCatalog::Extname).eq("pg_trgm"))
            .to_owned(),
        Extension::Vector => Query::select()
            .expr(Expr::val(1_i32))
            .and_where(Expr::cust("to_regtype('halfvec') IS NOT NULL"))
            .to_owned(),
    };
    let row: Option<(i32,)> = transaction.fetch_optional(statement).await?;
    Ok(row.is_some())
}

/// Creates `extension` in the first schema on the search path.
pub(super) async fn create_extension(
    transaction: &mut Transaction<'_>,
    extension: Extension,
) -> Result<(), Error> {
    transaction
        .execute_script(match extension {
            Extension::PgTrgm => "CREATE EXTENSION pg_trgm",
            Extension::Vector => "CREATE EXTENSION vector",
        })
        .await
}

/// What the first schema on the search path holds under the vector table's
/// name.
pub(super) enum VectorTable {
    Absent,
    /// A table whose vectors have the dimension asked about.
    SameDimension,
    /// A table whose vectors have another dimension.
    OtherDimension,
    /// On Postgres, a table with no `embedding` column, or one that is not
    /// a `halfvec` of a fixed dimension.
    UnexpectedEmbedding {
        /// The column's type as Postgres names it, or `None` when the table
        /// has no such column.
        column_type: Option<String>,
    },
    /// On `SQLite`, one of the two tables alone, or a sqlite-vec table that
    /// Coral did not create.
    UnexpectedSqliteTables,
}

/// The `SQLite` table that keys each vector: a Search Entity's key, the
/// rowid of its vector in the `vec0` table, and what the vector was made
/// from. The vector query's eligibility subquery joins it to
/// `search_entities` and gives `vec0` the rowids. `vector_rowid` is an
/// `INTEGER PRIMARY KEY`, so `VACUUM` keeps it; `search_entities` has none.
/// No foreign key: the reconcile pass removes orphans, as on Postgres.
const SQLITE_KEY_TABLE: &str = "CREATE TABLE search_entity_embeddings (\
    vector_rowid INTEGER PRIMARY KEY, \
    workspace_id TEXT NOT NULL, \
    doc_id TEXT NOT NULL, \
    embedder_revision TEXT NOT NULL, \
    source_indexed_at_unix_nanos INTEGER NOT NULL, \
    CONSTRAINT search_entity_embeddings_key UNIQUE (workspace_id, doc_id)\
)";

/// The statement that creates the sqlite-vec table of vectors, up to their
/// dimension.
const SQLITE_VECTOR_TABLE_BEFORE_DIMENSION: &str =
    "CREATE VIRTUAL TABLE search_entity_embedding_vectors USING vec0(embedding float[";

/// The rest of that statement, after the dimension.
const SQLITE_VECTOR_TABLE_AFTER_DIMENSION: &str = "] distance_metric=cosine)";

fn sqlite_vector_table_statement(dimension: usize) -> String {
    format!(
        "{SQLITE_VECTOR_TABLE_BEFORE_DIMENSION}{dimension}{SQLITE_VECTOR_TABLE_AFTER_DIMENSION}"
    )
}

/// The dimension of a sqlite-vec table that Coral created with `statement`,
/// or `None` for any other statement.
fn sqlite_vector_dimension(statement: &str) -> Option<usize> {
    let dimension = statement
        .strip_prefix(SQLITE_VECTOR_TABLE_BEFORE_DIMENSION)?
        .strip_suffix(SQLITE_VECTOR_TABLE_AFTER_DIMENSION)?
        .parse::<usize>()
        .ok()?;
    (sqlite_vector_table_statement(dimension) == statement).then_some(dimension)
}

/// The statement that creates the vector table for vectors of `dimension`.
///
/// The table is keyed like `search_entities` and carries the columns the
/// vector query filters on, so that query reads this table alone. It has no
/// foreign key, not even to `workspace_records`: the Embedder's reconcile
/// pass (#3055) will remove vectors whose Search Entity or Workspace is gone.
/// The ADR "Universal Search adds a vector list from a bundled CPU model"
/// amends ADR 0017's rule that each search table keeps its Workspace key.
fn vector_table_statement(dimension: usize) -> String {
    format!(
        "CREATE TABLE search_entity_embeddings (\
            workspace_id TEXT NOT NULL, \
            doc_id TEXT NOT NULL, \
            kind TEXT NOT NULL, \
            provider_id TEXT, \
            provider_generation_id TEXT, \
            tool_key TEXT, \
            embedder_revision TEXT NOT NULL, \
            source_indexed_at_unix_nanos BIGINT NOT NULL, \
            embedding halfvec({dimension}) NOT NULL, \
            CONSTRAINT search_entity_embeddings_pk PRIMARY KEY (workspace_id, doc_id)\
        )"
    )
}

/// Holds, until the transaction ends, the lock that makes concurrent boots
/// prepare the vector table one at a time. On `SQLite` the transaction
/// already holds the one writer.
pub(super) async fn lock_vector_table(transaction: &mut Transaction<'_>) -> Result<(), Error> {
    match transaction.system() {
        DatabaseSystem::Postgres => {
            transaction
                .execute_script(
                    "SELECT pg_advisory_xact_lock(\
                     hashtextextended('coral.search_entity_embeddings', 0))",
                )
                .await
        }
        DatabaseSystem::Sqlite => Ok(()),
    }
}

/// Reads what the database holds under the vector tables' names, measured
/// against vectors of `dimension`.
pub(super) async fn vector_table(
    transaction: &mut Transaction<'_>,
    dimension: usize,
) -> Result<VectorTable, Error> {
    match transaction.system() {
        DatabaseSystem::Postgres => postgres_vector_table(transaction, dimension).await,
        DatabaseSystem::Sqlite => sqlite_vector_tables(transaction, dimension).await,
    }
}

/// Reads the dimension of the vector table in the first schema on the
/// search path. pgvector keeps a `halfvec` column's dimension as its type
/// modifier.
async fn postgres_vector_table(
    transaction: &mut Transaction<'_>,
    dimension: usize,
) -> Result<VectorTable, Error> {
    let rows: Vec<(Option<String>, Option<i32>, Option<String>)> = transaction
        .fetch_all_sql(
            "SELECT t.typname::text, a.atttypmod, format_type(a.atttypid, a.atttypmod) \
             FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             LEFT JOIN pg_catalog.pg_attribute a \
                 ON a.attrelid = c.oid AND a.attname = 'embedding' AND NOT a.attisdropped \
             LEFT JOIN pg_catalog.pg_type t ON t.oid = a.atttypid \
             WHERE n.nspname = current_schema() \
             AND c.relname = 'search_entity_embeddings' \
             AND c.relkind = 'r'",
            sea_query::Values(Vec::new()),
        )
        .await?;
    Ok(match rows.into_iter().next() {
        None => VectorTable::Absent,
        // A `halfvec` declared without a dimension has the type modifier -1.
        Some((Some(type_name), Some(type_modifier), column_type)) if type_name == "halfvec" => {
            match usize::try_from(type_modifier) {
                Ok(stored) if stored == dimension => VectorTable::SameDimension,
                Ok(_) => VectorTable::OtherDimension,
                Err(_) => VectorTable::UnexpectedEmbedding { column_type },
            }
        }
        Some((_, _, column_type)) => VectorTable::UnexpectedEmbedding { column_type },
    })
}

/// Reads the two `SQLite` vector tables. The sqlite-vec table decides, as
/// the `embedding` column does on Postgres: one that Coral created has the
/// same or another dimension, and any other is unexpected. A key table
/// beside it keeps any other change to its shape. One of the two alone is
/// unexpected.
async fn sqlite_vector_tables(
    transaction: &mut Transaction<'_>,
    dimension: usize,
) -> Result<VectorTable, Error> {
    let tables: Vec<(String, String)> = transaction
        .fetch_all_sql(
            "SELECT name, sql FROM sqlite_schema WHERE type = 'table' \
             AND name IN ('search_entity_embeddings', 'search_entity_embedding_vectors')",
            sea_query::Values(Vec::new()),
        )
        .await?;
    let statement = |table: &str| {
        tables
            .iter()
            .find(|(name, _statement)| name == table)
            .map(|(_name, statement)| statement.as_str())
    };
    Ok(
        match (
            statement("search_entity_embeddings"),
            statement("search_entity_embedding_vectors"),
        ) {
            (None, None) => VectorTable::Absent,
            (Some(_keys), Some(vectors)) => match sqlite_vector_dimension(vectors) {
                Some(stored) if stored == dimension => VectorTable::SameDimension,
                Some(_) => VectorTable::OtherDimension,
                None => VectorTable::UnexpectedSqliteTables,
            },
            (Some(_), None) | (None, Some(_)) => VectorTable::UnexpectedSqliteTables,
        },
    )
}

pub(super) async fn drop_vector_table(transaction: &mut Transaction<'_>) -> Result<(), Error> {
    let statement = match transaction.system() {
        DatabaseSystem::Postgres => "DROP TABLE search_entity_embeddings",
        DatabaseSystem::Sqlite => {
            "DROP TABLE search_entity_embedding_vectors; DROP TABLE search_entity_embeddings"
        }
    };
    transaction.execute_script(statement).await
}

pub(super) async fn create_vector_table(
    transaction: &mut Transaction<'_>,
    dimension: usize,
) -> Result<(), Error> {
    match transaction.system() {
        DatabaseSystem::Postgres => {
            transaction
                .execute_script(&vector_table_statement(dimension))
                .await
        }
        DatabaseSystem::Sqlite => {
            transaction.execute_script(SQLITE_KEY_TABLE).await?;
            transaction
                .execute_script(&sqlite_vector_table_statement(dimension))
                .await
        }
    }
}

pub(super) async fn sqlite_version(transaction: &mut Transaction<'_>) -> Result<String, Error> {
    let (version,): (String,) = transaction
        .fetch_optional(
            Query::select()
                .expr(Expr::cust("sqlite_version()"))
                .to_owned(),
        )
        .await?
        .ok_or(sqlx::Error::RowNotFound)?;
    Ok(version)
}

pub(super) async fn probe_fts5(transaction: &mut Transaction<'_>) -> Result<FeatureProbe, Error> {
    probe_sqlite(
        transaction,
        "CREATE VIRTUAL TABLE temp.coral_search_fts5_probe USING fts5(value)",
    )
    .await
}

pub(super) async fn probe_trigram_tokenizer(
    transaction: &mut Transaction<'_>,
) -> Result<FeatureProbe, Error> {
    probe_sqlite(
        transaction,
        "CREATE VIRTUAL TABLE temp.coral_search_trigram_probe \
         USING fts5(value, tokenize = 'trigram')",
    )
    .await
}

/// Probes sqlite-vec, which Coral's build compiles in and registers for
/// every connection.
pub(super) async fn probe_vec0(transaction: &mut Transaction<'_>) -> Result<FeatureProbe, Error> {
    probe_sqlite(
        transaction,
        "CREATE VIRTUAL TABLE temp.coral_search_vec0_probe USING vec0(embedding float[1])",
    )
    .await
}

async fn probe_sqlite(
    transaction: &mut Transaction<'_>,
    statement: &str,
) -> Result<FeatureProbe, Error> {
    match transaction.execute_script(statement).await {
        Ok(()) => Ok(FeatureProbe::Present),
        Err(Error::Sqlx(sqlx::Error::Database(error)))
            if error.code().as_deref() == Some(SQLITE_ERROR) =>
        {
            Ok(FeatureProbe::Missing {
                cause: error.message().to_string(),
            })
        }
        Err(error) => Err(error),
    }
}

/// A vector as pgvector reads it: `[0.5,0.25,1]`.
pub(crate) fn vector_literal(vector: &[f32]) -> String {
    let mut literal = String::from("[");
    for (position, value) in vector.iter().enumerate() {
        if position > 0 {
            literal.push(',');
        }
        write!(literal, "{value}").expect("writing to a String does not fail");
    }
    literal.push(']');
    literal
}
