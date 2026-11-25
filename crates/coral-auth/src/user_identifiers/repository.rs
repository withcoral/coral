//! User Identifier persistence. The rows are for the whole deployment, as
//! `users` is: they are not Workspace data.
//!
//! Nothing observes these rows, so the write uses `execute`, not
//! `execute_change`: a link must not wake activity observers or Run waits.

use coral_db::{QueryExecutor, Transaction, UnixNanos};
use sea_query::{Expr, ExprTrait as _, Iden, OnConflict, Query};

use super::{UserIdentifier, UserIdentifierKind};
use crate::{Error, UserId};

#[derive(Iden)]
enum UserIdentifiers {
    Table,
    Kind,
    Value,
    UserId,
    LinkedAtUnixNanos,
}

#[derive(sqlx::FromRow)]
struct HolderRow {
    user_id: String,
}

/// The name of a kind as its row stores it.
const fn kind_name(kind: UserIdentifierKind) -> &'static str {
    match kind {
        UserIdentifierKind::Subject => "subject",
        UserIdentifierKind::NameId => "name_id",
    }
}

/// The User that holds `identifier`. The match is on the exact value.
pub(super) async fn find(
    executor: impl QueryExecutor,
    identifier: UserIdentifier<'_>,
) -> Result<Option<UserId>, Error> {
    let row: Option<HolderRow> = executor
        .fetch_optional(
            Query::select()
                .column(UserIdentifiers::UserId)
                .from(UserIdentifiers::Table)
                .and_where(Expr::col(UserIdentifiers::Kind).eq(kind_name(identifier.kind())))
                .and_where(Expr::col(UserIdentifiers::Value).eq(identifier.value()))
                .to_owned(),
        )
        .await?;
    row.map(|row| {
        UserId::parse(&row.user_id).map_err(|_error| {
            Error::CorruptData("User Identifier row has invalid Coral User identity".to_string())
        })
    })
    .transpose()
}

/// Stores `identifier` for `user_id` unless a row of either uniqueness rule
/// exists, and reports whether it did. An insert that meets a row that another
/// transaction has not committed yet waits for that transaction.
pub(super) async fn insert(
    transaction: &mut Transaction<'_>,
    identifier: UserIdentifier<'_>,
    user_id: &UserId,
    linked_at: UnixNanos,
) -> Result<bool, Error> {
    let rows = transaction
        .execute(
            Query::insert()
                .into_table(UserIdentifiers::Table)
                .columns([
                    UserIdentifiers::Kind,
                    UserIdentifiers::Value,
                    UserIdentifiers::UserId,
                    UserIdentifiers::LinkedAtUnixNanos,
                ])
                .values_panic([
                    kind_name(identifier.kind()).into(),
                    identifier.value().into(),
                    user_id.as_str().into(),
                    linked_at.into(),
                ])
                .on_conflict(OnConflict::new().do_nothing().to_owned())
                .to_owned(),
        )
        .await?;
    Ok(rows == 1)
}
