//! Profile persistence mechanics; callers own authorization, ordering, and commit.
use super::profile::VerifiedUserProfile;
use crate::{Error, UserId};
use coral_db::Transaction;
use sea_query::{Alias, Expr, ExprTrait as _, Iden, OnConflict, Query};

#[derive(Iden)]
enum UserProfiles {
    Table,
    UserId,
    Name,
    Email,
    EmailKey,
    EmailVerified,
    HostedDomain,
    ObservedAt,
}

#[derive(sqlx::FromRow)]
pub(super) struct ProfileRow {
    pub name: Option<String>,
    pub email: Option<String>,
}

pub(super) async fn upsert(
    tx: &mut Transaction<'_>,
    user: &UserId,
    profile: &VerifiedUserProfile<'_>,
    email_key: Option<String>,
    observed_at: i64,
) -> Result<(), Error> {
    tx.execute(
        Query::insert()
            .into_table(UserProfiles::Table)
            .columns([
                UserProfiles::UserId,
                UserProfiles::Name,
                UserProfiles::Email,
                UserProfiles::EmailKey,
                UserProfiles::EmailVerified,
                UserProfiles::HostedDomain,
                UserProfiles::ObservedAt,
            ])
            .values_panic([
                user.as_str().into(),
                profile.name.into(),
                profile.email.into(),
                email_key.into(),
                profile.email_verified.map(i64::from).into(),
                profile.hosted_domain.into(),
                observed_at.into(),
            ])
            .on_conflict(
                OnConflict::column(UserProfiles::UserId)
                    .update_columns([
                        UserProfiles::Name,
                        UserProfiles::Email,
                        UserProfiles::EmailKey,
                        UserProfiles::EmailVerified,
                        UserProfiles::HostedDomain,
                        UserProfiles::ObservedAt,
                    ])
                    .action_and_where(
                        Expr::col((Alias::new("excluded"), UserProfiles::ObservedAt))
                            .gt(Expr::col((UserProfiles::Table, UserProfiles::ObservedAt))),
                    )
                    .to_owned(),
            )
            .to_owned(),
    )
    .await?;
    Ok(())
}
pub(super) async fn find(
    tx: &mut Transaction<'_>,
    user: &UserId,
) -> Result<Option<ProfileRow>, Error> {
    Ok(tx
        .fetch_optional(
            Query::select()
                .columns([UserProfiles::Name, UserProfiles::Email])
                .from(UserProfiles::Table)
                .and_where(Expr::col(UserProfiles::UserId).eq(user.as_str()))
                .to_owned(),
        )
        .await?)
}
pub(super) async fn verified_candidates(
    tx: &mut Transaction<'_>,
    email: &str,
) -> Result<Vec<UserId>, Error> {
    let rows: Vec<(String,)> = tx
        .fetch_all(
            Query::select()
                .column(UserProfiles::UserId)
                .from(UserProfiles::Table)
                .and_where(Expr::col(UserProfiles::EmailKey).eq(email))
                .and_where(Expr::col(UserProfiles::EmailVerified).eq(1_i64))
                .to_owned(),
        )
        .await?;
    rows.into_iter().map(|(id,)| UserId::parse(&id)).collect()
}
