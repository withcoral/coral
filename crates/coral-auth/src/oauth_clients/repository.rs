//! Deployment-level OAuth client persistence. Registrations are not Workspace data.

use coral_db::{Database, Transaction};
use sea_query::{Expr, ExprTrait as _, Iden, Query};

use super::{ClientMetadataError, OAuthClientRegistration, RegisteredClient, RegistrationError};

#[derive(Iden)]
enum OAuthClients {
    #[iden = "oauth_clients"]
    Table,
    ClientId,
    ClientName,
    RedirectUris,
    IssuedAt,
}

pub(super) async fn insert(
    tx: &mut Transaction<'_>,
    client: &RegisteredClient,
) -> Result<(), RegistrationError> {
    let redirects = serde_json::to_string(&client.metadata.redirect_uris)
        .map_err(|_error| RegistrationError::Unavailable)?;
    tx.execute(
        Query::insert()
            .into_table(OAuthClients::Table)
            .columns([
                OAuthClients::ClientId,
                OAuthClients::ClientName,
                OAuthClients::RedirectUris,
                OAuthClients::IssuedAt,
            ])
            .values_panic([
                client.client_id.clone().into(),
                client.metadata.client_name.clone().into(),
                redirects.into(),
                client.client_id_issued_at.into(),
            ])
            .to_owned(),
    )
    .await
    .map_err(|_error| RegistrationError::Unavailable)?;
    Ok(())
}

#[derive(Iden)]
enum Admission {
    #[iden = "oauth_registration_admission"]
    Table,
    Singleton,
    WindowStartedAt,
    Accepted,
}

pub(super) struct AdmissionEvidence {
    pub total: i64,
    pub window_started_at: i64,
    pub accepted: i64,
}

pub(super) async fn lock_admission(
    tx: &mut Transaction<'_>,
) -> Result<AdmissionEvidence, RegistrationError> {
    // First statement is a write, so admission serializes across pools and
    // processes.
    let rows = tx
        .execute(
            Query::update()
                .table(Admission::Table)
                .value(Admission::Accepted, Expr::col(Admission::Accepted))
                .and_where(Expr::col(Admission::Singleton).eq(1))
                .to_owned(),
        )
        .await
        .map_err(|_error| RegistrationError::Unavailable)?;
    if rows != 1 {
        return Err(RegistrationError::Unavailable);
    }
    let (window_started_at, accepted): (i64, i64) = tx
        .fetch_optional(
            Query::select()
                .columns([Admission::WindowStartedAt, Admission::Accepted])
                .from(Admission::Table)
                .and_where(Expr::col(Admission::Singleton).eq(1))
                .to_owned(),
        )
        .await
        .map_err(|_error| RegistrationError::Unavailable)?
        .ok_or(RegistrationError::Unavailable)?;
    let (total,): (i64,) = tx
        .fetch_optional(
            Query::select()
                .expr(Expr::col(OAuthClients::ClientId).count())
                .from(OAuthClients::Table)
                .to_owned(),
        )
        .await
        .map_err(|_error| RegistrationError::Unavailable)?
        .ok_or(RegistrationError::Unavailable)?;
    Ok(AdmissionEvidence {
        total,
        window_started_at,
        accepted,
    })
}

pub(super) async fn record_admission(
    tx: &mut Transaction<'_>,
    started: i64,
    accepted: i64,
) -> Result<(), RegistrationError> {
    tx.execute(
        Query::update()
            .table(Admission::Table)
            .values([
                (Admission::WindowStartedAt, started.into()),
                (Admission::Accepted, accepted.into()),
            ])
            .and_where(Expr::col(Admission::Singleton).eq(1))
            .to_owned(),
    )
    .await
    .map_err(|_error| RegistrationError::Unavailable)?;
    Ok(())
}

#[derive(sqlx::FromRow)]
struct ClientRow {
    client_name: String,
    redirect_uris: String,
}

pub(super) async fn resolve(
    database: &Database,
    id: &str,
) -> Result<OAuthClientRegistration, ClientMetadataError> {
    let row: ClientRow = database
        .fetch_optional(
            Query::select()
                .columns([OAuthClients::ClientName, OAuthClients::RedirectUris])
                .from(OAuthClients::Table)
                .and_where(Expr::col(OAuthClients::ClientId).eq(id))
                .to_owned(),
        )
        .await
        .map_err(|_error| ClientMetadataError::Fetch)?
        .ok_or(ClientMetadataError::InvalidClientId)?;
    Ok(OAuthClientRegistration {
        client_name: row.client_name,
        redirect_uris: serde_json::from_str(&row.redirect_uris)
            .map_err(|_error| ClientMetadataError::InvalidMetadata)?,
    })
}
