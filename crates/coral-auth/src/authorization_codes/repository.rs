//! Deployment-level authorization code persistence. Codes are not Workspace data.

use coral_db::Transaction;
use sea_query::{Expr, ExprTrait as _, Iden, OnConflict, Query};

use super::{AuthorizationCodeError, AuthorizationGrant, CodeRedemption};

#[derive(Iden)]
enum Codes {
    #[iden = "oauth_authorization_codes"]
    Table,
    CodeDigest,
    SignInDigest,
    UserId,
    ClientId,
    RedirectUri,
    CodeChallenge,
    Resource,
    ExpiresAtUnixNanos,
    RedeemedAtUnixNanos,
}

/// Removes every code that can no longer be redeemed, spent or not.
pub(super) async fn purge_expired(
    tx: &mut Transaction<'_>,
    now_unix_nanos: i64,
) -> Result<(), AuthorizationCodeError> {
    tx.execute(
        Query::delete()
            .from_table(Codes::Table)
            .and_where(Expr::col(Codes::ExpiresAtUnixNanos).lte(now_unix_nanos))
            .to_owned(),
    )
    .await
    .map_err(|_error| AuthorizationCodeError::Unavailable)?;
    Ok(())
}

/// Stores a code unless its sign-in already stored one; reports whether it did.
pub(super) async fn insert(
    tx: &mut Transaction<'_>,
    code_digest: &str,
    sign_in_digest: &str,
    grant: &AuthorizationGrant,
    expires_at_unix_nanos: i64,
) -> Result<bool, AuthorizationCodeError> {
    let rows = tx
        .execute(
            Query::insert()
                .into_table(Codes::Table)
                .columns([
                    Codes::CodeDigest,
                    Codes::SignInDigest,
                    Codes::UserId,
                    Codes::ClientId,
                    Codes::RedirectUri,
                    Codes::CodeChallenge,
                    Codes::Resource,
                    Codes::ExpiresAtUnixNanos,
                ])
                .values_panic([
                    code_digest.into(),
                    sign_in_digest.into(),
                    grant.user_id.clone().into(),
                    grant.client_id.clone().into(),
                    grant.redirect_uri.clone().into(),
                    grant.code_challenge.clone().into(),
                    grant.resource.clone().into(),
                    expires_at_unix_nanos.into(),
                ])
                .on_conflict(
                    OnConflict::column(Codes::SignInDigest)
                        .do_nothing()
                        .to_owned(),
                )
                .to_owned(),
        )
        .await
        .map_err(|_error| AuthorizationCodeError::Unavailable)?;
    Ok(rows == 1)
}

#[derive(sqlx::FromRow)]
struct GrantRow {
    user_id: String,
    client_id: String,
    redirect_uri: String,
    code_challenge: String,
    resource: String,
}

/// Marks an unspent, unexpired code spent when every binding matches.
///
/// The bindings sit in the update's predicate, so a mismatched redemption
/// changes nothing and the code stays redeemable.
pub(super) async fn spend(
    tx: &mut Transaction<'_>,
    code_digest: &str,
    redemption: CodeRedemption<'_>,
    now_unix_nanos: i64,
) -> Result<Option<AuthorizationGrant>, AuthorizationCodeError> {
    let row: Option<GrantRow> = tx
        .fetch_optional_returning(
            Query::update()
                .table(Codes::Table)
                .value(Codes::RedeemedAtUnixNanos, now_unix_nanos)
                .and_where(Expr::col(Codes::CodeDigest).eq(code_digest))
                .and_where(Expr::col(Codes::RedeemedAtUnixNanos).is_null())
                .and_where(Expr::col(Codes::ExpiresAtUnixNanos).gt(now_unix_nanos))
                .and_where(Expr::col(Codes::ClientId).eq(redemption.client_id))
                .and_where(Expr::col(Codes::RedirectUri).eq(redemption.redirect_uri))
                .and_where(Expr::col(Codes::CodeChallenge).eq(redemption.code_challenge))
                .and_where(Expr::col(Codes::Resource).eq(redemption.resource))
                .returning(Query::returning().columns([
                    Codes::UserId,
                    Codes::ClientId,
                    Codes::RedirectUri,
                    Codes::CodeChallenge,
                    Codes::Resource,
                ]))
                .to_owned(),
        )
        .await
        .map_err(|_error| AuthorizationCodeError::Unavailable)?;
    Ok(row.map(|row| AuthorizationGrant {
        user_id: row.user_id,
        client_id: row.client_id,
        redirect_uri: row.redirect_uri,
        code_challenge: row.code_challenge,
        resource: row.resource,
    }))
}
