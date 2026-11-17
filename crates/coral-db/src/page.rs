//! One forward page of a list that its owner reads by key.
//!
//! A list orders its rows by a key that is unique within the list's scope.
//! The repository reads `WHERE scope AND key > after ORDER BY key LIMIT size + 1`
//! and [`Page::from_lookahead`] turns those rows into the page. The key of
//! the last item is the page's cursor: [`PageKey`] gives its text form on the
//! wire. The cursor binds nothing, so the scope predicate stays in the SQL
//! and a key from another scope only positions the read (ADR 0037).

use std::num::NonZeroU32;

use sea_query::{Expr, IntoColumnRef};

use crate::DatabaseSystem;

/// How many items a page holds at most: one or more. The owner of a list
/// decides the largest size it serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageSize(NonZeroU32);

impl PageSize {
    /// A page of `size` items, or `None` for zero.
    #[must_use]
    pub const fn new(size: u32) -> Option<Self> {
        match NonZeroU32::new(size) {
            Some(size) => Some(Self(size)),
            None => None,
        }
    }

    /// Returns the size.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }

    /// The `LIMIT` of the page's query: one row more than the page holds,
    /// which shows whether another page follows.
    #[must_use]
    pub fn lookahead_limit(self) -> u64 {
        u64::from(self.get()) + 1
    }
}

/// Which page of a list to read: the one after the key `after`, or the first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageRequest<K> {
    /// The key the page follows. It need not name a row.
    pub after: Option<K>,
    /// How many items the page holds at most.
    pub size: PageSize,
}

/// One page of a list in key order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page<T, K> {
    /// The items on this page.
    pub items: Vec<T>,
    /// The key the next page follows, or `None` on the last page.
    pub next_after: Option<K>,
}

impl<T, K> Page<T, K> {
    /// Cuts the rows of a page's query to the page. `rows` holds at most
    /// `size + 1` rows in key order: a row beyond `size` shows that another
    /// page follows, and that page follows the key of the last item kept.
    #[must_use]
    pub fn from_lookahead(mut rows: Vec<T>, size: PageSize, key: impl Fn(&T) -> K) -> Self {
        let size = usize::try_from(size.get()).unwrap_or(usize::MAX);
        let more = rows.len() > size;
        rows.truncate(size);
        let next_after = rows.last().filter(|_| more).map(key);
        Self {
            items: rows,
            next_after,
        }
    }
}

/// A list's key, and its text form on the wire.
pub trait PageKey: Sized {
    /// The key as the cursor a page returns.
    fn to_cursor(&self) -> String;

    /// The key a cursor spells, or `None` for text that is no key of this
    /// list.
    fn from_cursor(text: &str) -> Option<Self>;
}

/// `column` as a page's query orders and compares a text key: in byte order
/// on both databases, which is Rust's string order. `SQLite` compares `TEXT`
/// that way by default; Postgres compares in the database's locale unless
/// the expression names the `C` collation.
///
/// Use the one expression in the `ORDER BY` and in the `key > after`
/// predicate, so the two agree.
#[must_use]
pub fn text_key_order(system: DatabaseSystem, column: impl IntoColumnRef) -> Expr {
    let column = Expr::col(column);
    match system {
        DatabaseSystem::Sqlite => column,
        DatabaseSystem::Postgres => Expr::cust_with_expr("$1 COLLATE \"C\"", column),
    }
}

#[cfg(test)]
mod tests {
    use sea_query::{
        Alias, ExprTrait as _, Order, PostgresQueryBuilder, Query, SqliteQueryBuilder,
    };

    use super::*;

    #[test]
    fn a_page_size_is_one_or_more() {
        for (size, expected) in [(0, None), (1, Some(1)), (u32::MAX, Some(u32::MAX))] {
            assert_eq!(PageSize::new(size).map(PageSize::get), expected, "{size}");
        }
        let largest = PageSize::new(u32::MAX).expect("size");
        assert_eq!(largest.lookahead_limit(), u64::from(u32::MAX) + 1);
    }

    // Catches: a page that keeps the lookahead row, names a next page when
    // none follows, or continues from a row it did not return.
    #[test]
    fn a_lookahead_row_names_the_next_page_and_is_not_on_this_one() {
        for (case, rows, size, items, next_after) in [
            ("no rows", vec![], 2, vec![], None),
            ("fewer rows than the size", vec![1], 2, vec![1], None),
            ("exactly the size", vec![1, 2], 2, vec![1, 2], None),
            ("one row more", vec![1, 2, 3], 2, vec![1, 2], Some(20)),
            ("a page of one", vec![7, 8], 1, vec![7], Some(70)),
        ] {
            let size = PageSize::new(size).expect("size");
            let page = Page::from_lookahead(rows, size, |row| row * 10);
            assert_eq!(page, Page { items, next_after }, "{case}");
        }
    }

    // Catches: a text key that Postgres orders in the database's locale, or
    // an order and a predicate that spell the key differently.
    #[test]
    fn a_text_key_is_compared_in_byte_order_on_both_databases() {
        let statement = |system| {
            let key = text_key_order(system, (Alias::new("tools"), Alias::new("path")));
            Query::select()
                .column(Alias::new("path"))
                .from(Alias::new("tools"))
                .and_where(key.clone().gt("a"))
                .order_by_expr(key, Order::Asc)
                .to_owned()
        };
        assert_eq!(
            statement(DatabaseSystem::Postgres).to_string(PostgresQueryBuilder),
            r#"SELECT "path" FROM "tools" WHERE ("tools"."path" COLLATE "C") > 'a' ORDER BY "tools"."path" COLLATE "C" ASC"#
        );
        assert_eq!(
            statement(DatabaseSystem::Sqlite).to_string(SqliteQueryBuilder),
            r#"SELECT "path" FROM "tools" WHERE "tools"."path" > 'a' ORDER BY "tools"."path" ASC"#
        );
    }
}
