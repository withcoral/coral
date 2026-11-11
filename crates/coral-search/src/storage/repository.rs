//! The feature probes' statements.
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

/// The statement that creates the vector table for vectors of `dimension`.
///
/// The table is keyed like `search_entities` and carries the columns the
/// vector query filters on, so that query reads this table alone. It has no
/// foreign key: the reconcile pass removes vectors whose Search Entity or
/// Workspace is gone.
pub(super) fn vector_table_statement(dimension: usize) -> String {
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

/// The statement that created the vector table in the first schema on the
/// search path, as its comment records it: `None` when that schema has no
/// vector table, `Some(None)` when the table has no comment.
pub(super) async fn vector_table_created_by(
    transaction: &mut Transaction<'_>,
) -> Result<Option<Option<String>>, Error> {
    let rows: Vec<(Option<String>,)> = transaction
        .fetch_all_sql(
            "SELECT obj_description(c.oid, 'pg_class') \
             FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = current_schema() \
             AND c.relname = 'search_entity_embeddings' \
             AND c.relkind = 'r'",
            sea_query::Values(Vec::new()),
        )
        .await?;
    Ok(rows.into_iter().next().map(|(comment,)| comment))
}

pub(super) async fn drop_vector_table(transaction: &mut Transaction<'_>) -> Result<(), Error> {
    transaction
        .execute_script("DROP TABLE search_entity_embeddings")
        .await
}

/// Runs `statement` and records it as the table's comment.
pub(super) async fn create_vector_table(
    transaction: &mut Transaction<'_>,
    statement: &str,
) -> Result<(), Error> {
    transaction.execute_script(statement).await?;
    transaction
        .execute_script(&format!(
            "COMMENT ON TABLE search_entity_embeddings IS '{}'",
            statement.replace('\'', "''")
        ))
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
