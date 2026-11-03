//! Deployment-level OAuth client persistence. Registrations are not Workspace data.

use coral_db::{Database, Transaction};
use sea_query::{Expr, ExprTrait as _, Iden, Query};

use super::{
    ClientAuthenticationError, ClientMetadataError, OAuthClientRegistration, RegisteredClient,
    RegistrationError, StoredAuthentication,
};

#[derive(Iden)]
enum OAuthClients {
    #[iden = "oauth_clients"]
    Table,
    ClientId,
    ClientName,
    RedirectUris,
    IssuedAt,
    RefreshEligible,
    TokenEndpointAuthMethod,
    ClientSecretVerifier,
}

const NONE: &str = "none";
const CLIENT_SECRET_POST: &str = "client_secret_post";

pub(super) async fn insert(
    tx: &mut Transaction<'_>,
    client: &RegisteredClient,
    authentication: &StoredAuthentication,
) -> Result<(), RegistrationError> {
    let redirects = serde_json::to_string(&client.metadata.redirect_uris)
        .map_err(|_error| RegistrationError::Unavailable)?;
    let (method, verifier) = match authentication {
        StoredAuthentication::None => (NONE, None),
        StoredAuthentication::ClientSecretPost { verifier } => {
            (CLIENT_SECRET_POST, Some(verifier.clone()))
        }
    };
    tx.execute(
        Query::insert()
            .into_table(OAuthClients::Table)
            .columns([
                OAuthClients::ClientId,
                OAuthClients::ClientName,
                OAuthClients::RedirectUris,
                OAuthClients::IssuedAt,
                OAuthClients::RefreshEligible,
                OAuthClients::TokenEndpointAuthMethod,
                OAuthClients::ClientSecretVerifier,
            ])
            .values_panic([
                client.client_id.clone().into(),
                client.metadata.client_name.clone().into(),
                redirects.into(),
                client.client_id_issued_at.into(),
                client.metadata.refresh_eligible.into(),
                method.into(),
                verifier.into(),
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
    refresh_eligible: bool,
}

pub(super) async fn resolve(
    database: &Database,
    id: &str,
) -> Result<OAuthClientRegistration, ClientMetadataError> {
    let row: ClientRow = database
        .fetch_optional(
            Query::select()
                .columns([
                    OAuthClients::ClientName,
                    OAuthClients::RedirectUris,
                    OAuthClients::RefreshEligible,
                ])
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
        refresh_eligible: row.refresh_eligible,
    })
}

#[derive(sqlx::FromRow)]
struct AuthenticationRow {
    token_endpoint_auth_method: String,
    client_secret_verifier: Option<String>,
}

/// How the registered client `id` authenticates, or `None` when no such
/// client is registered.
pub(super) async fn authentication(
    database: &Database,
    id: &str,
) -> Result<Option<StoredAuthentication>, ClientAuthenticationError> {
    let row: Option<AuthenticationRow> = database
        .fetch_optional(
            Query::select()
                .columns([
                    OAuthClients::TokenEndpointAuthMethod,
                    OAuthClients::ClientSecretVerifier,
                ])
                .from(OAuthClients::Table)
                .and_where(Expr::col(OAuthClients::ClientId).eq(id))
                .to_owned(),
        )
        .await
        .map_err(|_error| ClientAuthenticationError::Unavailable)?;
    row.map(|row| {
        match (
            row.token_endpoint_auth_method.as_str(),
            row.client_secret_verifier,
        ) {
            (NONE, None) => Ok(StoredAuthentication::None),
            (CLIENT_SECRET_POST, Some(verifier)) => {
                Ok(StoredAuthentication::ClientSecretPost { verifier })
            }
            // The schema forbids any other pair; a row that holds one is corrupt.
            _ => Err(ClientAuthenticationError::Unavailable),
        }
    })
    .transpose()
}
