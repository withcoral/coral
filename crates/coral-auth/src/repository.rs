//! Authentication directory persistence, joining the provisioning transaction.
//!
//! `users` keeps the issuer and the Subject of an OIDC User, because the
//! release before this one finds a User there. The store for User Identifiers
//! finds every User in this release.

use coral_db::Transaction;
use sea_query::{Expr, ExprTrait as _, Iden, Query};

use crate::user_identifiers::repository::UserIdentifiers;
use crate::{Error, UserId, VerifiedLogin};

#[derive(sqlx::FromRow)]
struct UserIdRow {
    user_id: String,
}

#[derive(sqlx::FromRow)]
struct IssuerRow {
    issuer: Option<String>,
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

/// The User whose `users.subject` is `subject` and who holds no User
/// Identifier in the store.
pub(super) async fn find_unlinked_subject(
    transaction: &mut Transaction<'_>,
    subject: &str,
) -> Result<Option<UserId>, Error> {
    let row: Option<UserIdRow> = transaction
        .fetch_optional(
            Query::select()
                .column(Users::UserId)
                .from(Users::Table)
                .and_where(Expr::col(Users::Subject).eq(subject))
                .and_where(
                    Expr::exists(
                        Query::select()
                            .expr(Expr::val(1))
                            .from(UserIdentifiers::Table)
                            .and_where(
                                Expr::col((UserIdentifiers::Table, UserIdentifiers::UserId))
                                    .equals((Users::Table, Users::UserId)),
                            )
                            .to_owned(),
                    )
                    .not(),
                )
                .to_owned(),
        )
        .await?;
    row.map(|row| {
        UserId::parse(&row.user_id).map_err(|_error| {
            Error::CorruptData(
                "authentication directory row has invalid Coral User identity".into(),
            )
        })
    })
    .transpose()
}

/// Adds the `users` row of a new User. An OIDC login writes its issuer and
/// Subject; another login leaves both empty.
pub(super) async fn insert_user(
    transaction: &mut Transaction<'_>,
    user_id: &UserId,
    login: &VerifiedLogin<'_>,
) -> Result<(), Error> {
    let subject = login.oidc_issuer.map(|_issuer| login.identifier.value());
    transaction
        .execute(
            Query::insert()
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
                    Expr::val(user_id.as_str()),
                    Expr::val(login.oidc_issuer.map(ToString::to_string)),
                    Expr::val(subject.map(ToString::to_string)),
                    Expr::val(login.display_name.map(ToString::to_string)),
                    Expr::val(login.occurred_at),
                    Expr::val(login.occurred_at),
                ])
                .to_owned(),
        )
        .await?;
    Ok(())
}

/// The issuer that the User's row keeps, or `None` when it keeps none.
pub(super) async fn issuer(
    transaction: &mut Transaction<'_>,
    user_id: &UserId,
) -> Result<Option<String>, Error> {
    let row: IssuerRow = transaction
        .fetch_optional(
            Query::select()
                .column(Users::Issuer)
                .from(Users::Table)
                .and_where(Expr::col(Users::UserId).eq(user_id.as_str()))
                .to_owned(),
        )
        .await?
        .ok_or_else(|| {
            Error::CorruptData(
                "a User Identifier names a User that the authentication directory lacks"
                    .to_string(),
            )
        })?;
    Ok(row.issuer)
}

/// Records a later login of a User. An older completion cannot replace newer
/// metadata.
pub(super) async fn record_login(
    transaction: &mut Transaction<'_>,
    user_id: &UserId,
    login: &VerifiedLogin<'_>,
) -> Result<(), Error> {
    transaction
        .execute(
            Query::update()
                .table(Users::Table)
                .values([
                    (
                        Users::DisplayName,
                        Expr::val(login.display_name.map(ToString::to_string)),
                    ),
                    (Users::LastLoginAtUnixNanos, Expr::val(login.occurred_at)),
                ])
                .and_where(Expr::col(Users::UserId).eq(user_id.as_str()))
                .and_where(Expr::col(Users::LastLoginAtUnixNanos).lt(login.occurred_at))
                .to_owned(),
        )
        .await?;
    Ok(())
}
