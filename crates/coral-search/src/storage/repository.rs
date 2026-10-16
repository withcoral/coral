//! The search streams, their ledger, and the feature probes' statements.
//!
//! Every function joins the caller's transaction; the storage module owns
//! when each transaction begins, commits, or rolls back.

use coral_db::{DatabaseSystem, Error, Transaction};
use sea_query::{Expr, ExprTrait as _, Func, Iden, Query};

use super::FeatureProbe;

/// One backend's search-owned DDL, in version order, under its ledger name.
pub(super) struct Stream {
    pub(super) name: &'static str,
    pub(super) steps: &'static [StreamStep],
}

pub(super) struct StreamStep {
    pub(super) version: i64,
    pub(super) script: &'static str,
}

impl Stream {
    pub(super) fn supported_version(&self) -> i64 {
        self.steps.last().map_or(0, |step| step.version)
    }
}

const POSTGRES_STREAM: Stream = Stream {
    name: "postgres",
    steps: &[
        StreamStep {
            version: 1,
            script: include_str!("../../streams/postgres/0001_text_search.sql"),
        },
        StreamStep {
            version: 2,
            script: include_str!("../../streams/postgres/0002_split_camel_case_names.sql"),
        },
        StreamStep {
            version: 3,
            script: include_str!("../../streams/postgres/0003_statistics_watermark.sql"),
        },
    ],
};

const SQLITE_STREAM: Stream = Stream {
    name: "sqlite",
    steps: &[StreamStep {
        version: 1,
        script: include_str!("../../streams/sqlite/0001_text_search.sql"),
    }],
};

/// Transaction-scoped advisory lock key for the Postgres stream, ASCII `coral_sm`.
const POSTGRES_STREAM_LOCK_KEY: i64 = 0x636f_7261_6c5f_736d;

/// `SQLite`'s primary result code for a generic error, which it reports for
/// an unknown module or tokenizer.
const SQLITE_ERROR: &str = "1";

/// The ledger of the backend streams: one row per applied step.
#[derive(Iden)]
enum SearchMigrations {
    Table,
    Stream,
    Version,
    AppliedAtUnixNanos,
}

/// Postgres's catalog of installed extensions.
#[derive(Iden)]
#[iden = "pg_catalog"]
enum PgCatalog {
    Table,
    PgExtension,
    Extname,
}

pub(super) const fn stream(system: DatabaseSystem) -> &'static Stream {
    match system {
        DatabaseSystem::Postgres => &POSTGRES_STREAM,
        DatabaseSystem::Sqlite => &SQLITE_STREAM,
    }
}

/// Serializes stream steps across Postgres workers until the transaction ends.
/// An immediate `SQLite` transaction already holds the write lock.
pub(super) async fn lock_stream(transaction: &mut Transaction<'_>) -> Result<(), Error> {
    if transaction.system() == DatabaseSystem::Postgres {
        transaction
            .execute_sql(
                "SELECT pg_advisory_xact_lock($1)",
                sea_query::Values(vec![POSTGRES_STREAM_LOCK_KEY.into()]),
            )
            .await?;
    }
    Ok(())
}

/// Returns the newest recorded version of `stream`, or zero.
pub(super) async fn recorded_version(
    transaction: &mut Transaction<'_>,
    stream: &str,
) -> Result<i64, Error> {
    let row: Option<(Option<i64>,)> = transaction
        .fetch_optional(
            Query::select()
                .expr(Func::max(Expr::col(SearchMigrations::Version)))
                .from(SearchMigrations::Table)
                .and_where(Expr::col(SearchMigrations::Stream).eq(stream))
                .to_owned(),
        )
        .await?;
    Ok(row.and_then(|(version,)| version).unwrap_or(0))
}

pub(super) async fn apply_step(
    transaction: &mut Transaction<'_>,
    script: &str,
) -> Result<(), Error> {
    transaction.execute_script(script).await
}

pub(super) async fn record_step(
    transaction: &mut Transaction<'_>,
    stream: &str,
    version: i64,
    applied_at_unix_nanos: i64,
) -> Result<(), Error> {
    transaction
        .execute(
            Query::insert()
                .into_table(SearchMigrations::Table)
                .columns([
                    SearchMigrations::Stream,
                    SearchMigrations::Version,
                    SearchMigrations::AppliedAtUnixNanos,
                ])
                .values_panic([stream.into(), version.into(), applied_at_unix_nanos.into()])
                .to_owned(),
        )
        .await?;
    Ok(())
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

pub(super) async fn pg_trgm_installed(transaction: &mut Transaction<'_>) -> Result<bool, Error> {
    let row: Option<(i32,)> = transaction
        .fetch_optional(
            Query::select()
                .expr(Expr::val(1_i32))
                .from((PgCatalog::Table, PgCatalog::PgExtension))
                .and_where(Expr::col(PgCatalog::Extname).eq("pg_trgm"))
                .to_owned(),
        )
        .await?;
    Ok(row.is_some())
}

/// Creates `pg_trgm` in the first schema on the search path.
pub(super) async fn create_pg_trgm(transaction: &mut Transaction<'_>) -> Result<(), Error> {
    transaction.execute_script("CREATE EXTENSION pg_trgm").await
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
