use std::future::Future;

use sea_query::SelectStatement;
use sqlx::FromRow;
use sqlx::postgres::PgRow;
use sqlx::sqlite::SqliteRow;

use crate::{Database, Error, Transaction};

mod private {
    pub trait Sealed {}
}

/// A borrowed execution context for domain-owned read statements.
///
/// Domain repositories use this interface when the same read is valid either
/// directly against a [`Database`] or inside a caller-owned [`Transaction`].
/// The supplied context still makes the transaction choice explicit. This
/// interface is sealed because database execution is not an Adapter seam for
/// domain fakes.
pub trait QueryExecutor: private::Sealed + Sized {
    /// Fetches the first matching domain row, if any.
    ///
    /// This does not verify that the statement matches at most one row.
    fn fetch_optional<T>(
        self,
        statement: SelectStatement,
    ) -> impl Future<Output = Result<Option<T>, Error>> + Send
    where
        T: Send + Unpin,
        for<'row> T: FromRow<'row, SqliteRow>,
        for<'row> T: FromRow<'row, PgRow>;

    /// Fetches and fully materializes all matching domain rows.
    fn fetch_all<T>(
        self,
        statement: SelectStatement,
    ) -> impl Future<Output = Result<Vec<T>, Error>> + Send
    where
        T: Send + Unpin,
        for<'row> T: FromRow<'row, SqliteRow>,
        for<'row> T: FromRow<'row, PgRow>;
}

impl<Access> private::Sealed for &Database<Access> where Access: Sync {}

impl<Access> QueryExecutor for &Database<Access>
where
    Access: Sync,
{
    fn fetch_optional<T>(
        self,
        statement: SelectStatement,
    ) -> impl Future<Output = Result<Option<T>, Error>> + Send
    where
        T: Send + Unpin,
        for<'row> T: FromRow<'row, SqliteRow>,
        for<'row> T: FromRow<'row, PgRow>,
    {
        Database::fetch_optional(self, statement)
    }

    fn fetch_all<T>(
        self,
        statement: SelectStatement,
    ) -> impl Future<Output = Result<Vec<T>, Error>> + Send
    where
        T: Send + Unpin,
        for<'row> T: FromRow<'row, SqliteRow>,
        for<'row> T: FromRow<'row, PgRow>,
    {
        Database::fetch_all(self, statement)
    }
}

impl private::Sealed for &mut Transaction<'_> {}

impl QueryExecutor for &mut Transaction<'_> {
    fn fetch_optional<T>(
        self,
        statement: SelectStatement,
    ) -> impl Future<Output = Result<Option<T>, Error>> + Send
    where
        T: Send + Unpin,
        for<'row> T: FromRow<'row, SqliteRow>,
        for<'row> T: FromRow<'row, PgRow>,
    {
        Transaction::fetch_optional(self, statement)
    }

    fn fetch_all<T>(
        self,
        statement: SelectStatement,
    ) -> impl Future<Output = Result<Vec<T>, Error>> + Send
    where
        T: Send + Unpin,
        for<'row> T: FromRow<'row, SqliteRow>,
        for<'row> T: FromRow<'row, PgRow>,
    {
        Transaction::fetch_all(self, statement)
    }
}
