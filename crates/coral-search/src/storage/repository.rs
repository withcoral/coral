//! The boot statements: the feature probes and the vector table's.
//!
//! Every function joins the caller's transaction; the storage module owns
//! when each transaction begins, commits, or rolls back.

use coral_db::{Error, Transaction};
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

/// Whether this role can use `extension` now: `pg_trgm` when the database
/// has it, `vector` when its `halfvec` type is on the search path.
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
    /// A table whose vectors have another dimension, or no `halfvec`
    /// column.
    OtherDimension,
}

/// The statement that creates the vector table for vectors of `dimension`.
///
/// The table is keyed like `search_entities` and carries the columns the
/// vector query filters on, so that query reads this table alone. It has no
/// foreign key, not even to `workspace_records`: the reconcile pass removes
/// vectors whose Search Entity or Workspace is gone. The ADR "Universal
/// Search adds a vector list from a bundled CPU model" amends ADR 0017's
/// rule that each search table keeps its Workspace key.
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
/// prepare the vector table one at a time.
pub(super) async fn lock_vector_table(transaction: &mut Transaction<'_>) -> Result<(), Error> {
    transaction
        .execute_script(
            "SELECT pg_advisory_xact_lock(hashtextextended('coral.search_entity_embeddings', 0))",
        )
        .await
}

/// Reads the dimension of the vector table in the first schema on the
/// search path. pgvector keeps a `halfvec` column's dimension as its type
/// modifier.
pub(super) async fn vector_table(
    transaction: &mut Transaction<'_>,
    dimension: usize,
) -> Result<VectorTable, Error> {
    let rows: Vec<(Option<String>, Option<i32>)> = transaction
        .fetch_all_sql(
            "SELECT t.typname::text, a.atttypmod \
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
        Some((Some(type_name), Some(type_modifier)))
            if type_name == "halfvec"
                && usize::try_from(type_modifier).is_ok_and(|stored| stored == dimension) =>
        {
            VectorTable::SameDimension
        }
        Some(_) => VectorTable::OtherDimension,
    })
}

pub(super) async fn drop_vector_table(transaction: &mut Transaction<'_>) -> Result<(), Error> {
    transaction
        .execute_script("DROP TABLE search_entity_embeddings")
        .await
}

pub(super) async fn create_vector_table(
    transaction: &mut Transaction<'_>,
    dimension: usize,
) -> Result<(), Error> {
    transaction
        .execute_script(&vector_table_statement(dimension))
        .await
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
