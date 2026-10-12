//! Authentication directory persistence, joining the provisioning transaction.

use coral_db::Transaction;
use sea_query::{Alias, Expr, ExprTrait as _, Iden, OnConflict, Query};

use crate::{Error, UserId, VerifiedOidcLogin};

pub(super) struct DirectoryIdentity {
    pub(super) user_id: UserId,
    pub(super) issuer: String,
}

#[derive(sqlx::FromRow)]
struct DirectoryIdentityRow {
    user_id: String,
    issuer: String,
}

#[derive(Iden)]
enum Users {
    Table,
    UserId,
    Issuer,
    Subject,
    DisplayName,
    CreatedAtUnixNanos,
    LastLoginAtUnixNanos,
}

/// Concurrent first logins converge; an older completion cannot replace newer
/// metadata, and a different issuer cannot change the existing subject mapping.
pub(super) async fn upsert_login(
    transaction: &mut Transaction<'_>,
    candidate_user_id: &UserId,
    login: &VerifiedOidcLogin<'_>,
) -> Result<DirectoryIdentity, Error> {
    let insert = Query::insert()
        .into_table(Users::Table)
        .columns([
            Users::UserId,
            Users::Issuer,
            Users::Subject,
            Users::DisplayName,
            Users::CreatedAtUnixNanos,
            Users::LastLoginAtUnixNanos,
        ])
        .values_panic([
            Expr::val(candidate_user_id.as_str()),
            Expr::val(login.issuer.to_string()),
            Expr::val(login.subject.to_string()),
            Expr::val(login.display_name.map(ToString::to_string)),
            Expr::val(login.occurred_at_unix_nanos),
            Expr::val(login.occurred_at_unix_nanos),
        ])
        .on_conflict(
            OnConflict::column(Users::Subject)
                .update_columns([Users::DisplayName, Users::LastLoginAtUnixNanos])
                .action_and_where(
                    Expr::col((Users::Table, Users::Issuer))
                        .eq(login.issuer)
                        .and(
                            Expr::col((Alias::new("excluded"), Users::LastLoginAtUnixNanos))
                                .gt(Expr::col((Users::Table, Users::LastLoginAtUnixNanos))),
                        ),
                )
                .to_owned(),
        )
        .to_owned();
    transaction.execute(insert).await?;
    let row: DirectoryIdentityRow = transaction
        .fetch_optional(
            Query::select()
                .columns([Users::UserId, Users::Issuer])
                .from(Users::Table)
                .and_where(Expr::col(Users::Subject).eq(login.subject))
                .to_owned(),
        )
        .await?
        .ok_or_else(|| {
            Error::CorruptData(
                "provisioned login is missing from the authentication directory".to_string(),
            )
        })?;
    Ok(DirectoryIdentity {
        user_id: UserId::parse(&row.user_id).map_err(|_error| {
            Error::CorruptData(
                "authentication directory row has invalid Coral User identity".to_string(),
            )
        })?,
        issuer: row.issuer,
    })
}
