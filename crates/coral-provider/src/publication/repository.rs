use coral_workspace::WorkspaceId;
use sea_query::{Expr, ExprTrait as _, Iden, Query};
use sqlx::FromRow;

use coral_db::Transaction;
use coral_workspace::UserId;

use crate::access::{CredentialRoute, ProviderAccess, ProviderNamespace};
use crate::{
    Error, ManagementRequestId, Provider, ProviderDisplayName, ProviderId, ProviderPublication,
    ProviderPublicationId, ProviderPublicationOutcome, ProviderRegenerationPublication,
    ProviderRegenerationRequest, ProviderSetupId, ProviderState, PublicationDiagnostic,
    WorkspaceProviderPublicationRequest,
};

use super::{Command, Terminal};

impl ProviderPublicationOutcome {
    pub(crate) const fn as_storage_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }

    pub(crate) fn from_storage_str(value: &str) -> Result<Self, Error> {
        match value {
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            _ => Err(Error::CorruptData(format!(
                "Provider Publication has unrecognized outcome '{value}'"
            ))),
        }
    }
}
use crate::repository::decode_owner;

pub(super) async fn terminal_for_command<R>(
    transaction: &mut Transaction<'_>,
    command: &Command<R>,
) -> Result<Option<Terminal>, Error> {
    match command {
        Command::Workspace(request) => publication_by_request_in_transaction(
            transaction,
            request.workspace_id(),
            request.request_id(),
        )
        .await
        .map(|result| result.map(Terminal::Workspace)),

        Command::Regenerate { request, .. } => regeneration_publication_by_request_in_transaction(
            transaction,
            request.owner(),
            request.request_id(),
        )
        .await
        .map(|result| result.map(Terminal::Regeneration)),
    }
}

pub(super) async fn request_key_exists(
    transaction: &mut Transaction<'_>,
    command: &Command,
) -> Result<bool, Error> {
    match command {
        Command::Workspace(request) => {
            publication_request_key_exists_in_transaction(
                transaction,
                request.workspace_id(),
                request.request_id(),
            )
            .await
        }

        Command::Regenerate { request, .. } => {
            publication_request_key_exists_in_transaction(
                transaction,
                request.owner(),
                request.request_id(),
            )
            .await
        }
    }
}

pub(super) async fn insert_terminal(
    transaction: &mut Transaction<'_>,
    actor: &UserId,
    terminal: &Terminal,
) -> Result<(), Error> {
    match terminal {
        Terminal::Workspace(result) => {
            insert_publication_request_key(
                transaction,
                result.request().workspace_id(),
                result.request_id(),
                actor,
            )
            .await?;
            insert_publication(transaction, result).await
        }

        Terminal::Regeneration(result) => {
            insert_publication_request_key(
                transaction,
                result.request().owner(),
                result.request().request_id(),
                actor,
            )
            .await?;
            insert_regeneration_publication(transaction, result).await
        }
    }
}

#[derive(Iden)]
enum ProviderPublicationRequestKeys {
    Table,
    RequestId,
    WorkspaceId,
    ActorUserId,
}

#[derive(Iden)]
enum ProviderPublications {
    Table,
    Id,
    RequestId,
    WorkspaceId,
    SetupId,
    RequestedNamespace,
    RequestedRoute,
    RequestedAccountGrant,
    Outcome,
    DiagnosticCode,
    DiagnosticMessage,
    ProviderId,
    ProviderDisplayName,
    ProviderAccountSettingVersion,
    CompletedAtUnixNanos,
}
#[derive(Iden)]
enum ProviderRegenerationPublications {
    Table,
    AccountGrant,
    AccountSetting,
    AllowIncompatibleConfiguration,
    ConfigurationRevision,
    Id,
    RequestId,
    WorkspaceId,
    ProviderId,
    SetupId,
    Outcome,
    DiagnosticCode,
    DiagnosticMessage,
    CompletedAtUnixNanos,
}

#[derive(Debug, FromRow)]
struct PublicationRequestRow {
    request_id: String,
    workspace_id: String,
    setup_id: String,
    requested_namespace: String,
    requested_route: String,
    requested_account_grant: Option<String>,
}

impl PublicationRequestRow {
    fn decode(self) -> Result<WorkspaceProviderPublicationRequest, Error> {
        let request_id = ManagementRequestId::parse(&self.request_id).map_err(|_error| {
            Error::CorruptData("Provider Publication has invalid request identity".to_string())
        })?;
        let workspace_id = WorkspaceId::parse(&self.workspace_id).map_err(|_error| {
            Error::CorruptData("Provider Publication has invalid Workspace identity".to_string())
        })?;
        let setup_id = ProviderSetupId::parse(&self.setup_id).map_err(|_error| {
            Error::CorruptData("Provider Publication has invalid Setup identity".to_string())
        })?;
        let namespace = ProviderNamespace::parse(&self.requested_namespace).map_err(|_error| {
            Error::CorruptData("Provider Publication has invalid namespace".to_string())
        })?;
        let route = CredentialRoute::from_storage_str(&self.requested_route)?;
        let request = WorkspaceProviderPublicationRequest::new(
            request_id,
            workspace_id,
            setup_id,
            namespace,
            route,
        );
        Ok(match self.requested_account_grant {
            Some(grant) => {
                request.with_account_grant(crate::account::ProviderAccountId::parse(&grant)?)
            }
            None => request,
        })
    }
}

#[derive(Debug, FromRow)]
struct PublicationRow {
    id: String,
    request_id: String,
    workspace_id: String,
    setup_id: String,
    requested_namespace: String,
    requested_route: String,
    requested_account_grant: Option<String>,
    outcome: String,
    diagnostic_code: Option<String>,
    diagnostic_message: Option<String>,
    provider_id: Option<String>,
    provider_display_name: Option<String>,
    provider_account_setting_version: Option<i64>,
    completed_at_unix_nanos: i64,
}

impl PublicationRow {
    fn decode(self) -> Result<ProviderPublication, Error> {
        let id = ProviderPublicationId::parse(&self.id).map_err(|_error| {
            Error::CorruptData("Provider Publication has invalid identity".to_string())
        })?;
        let request = PublicationRequestRow {
            request_id: self.request_id,
            workspace_id: self.workspace_id,
            setup_id: self.setup_id,
            requested_namespace: self.requested_namespace,
            requested_route: self.requested_route,
            requested_account_grant: self.requested_account_grant,
        }
        .decode()?;
        let outcome = ProviderPublicationOutcome::from_storage_str(&self.outcome)?;
        let diagnostic = match (self.diagnostic_code, self.diagnostic_message) {
            (Some(code), Some(message)) => {
                Some(PublicationDiagnostic::from_storage(code, message)?)
            }
            (None, None) => None,
            _ => {
                return Err(Error::CorruptData(
                    "Provider Publication has partial diagnostic evidence".to_string(),
                ));
            }
        };
        let provider_id = self
            .provider_id
            .map(|value| ProviderId::parse(&value))
            .transpose()
            .map_err(|_error| {
                Error::CorruptData("Provider Publication has invalid Provider identity".to_string())
            })?;
        let published_provider = match (
            provider_id,
            self.provider_display_name,
            self.provider_account_setting_version,
        ) {
            (Some(provider_id), Some(display_name), Some(version))
                if outcome == ProviderPublicationOutcome::Succeeded
                    && diagnostic.is_none()
                    && !display_name.is_empty()
                    && version > 0 =>
            {
                let display_name = ProviderDisplayName::parse(&display_name).map_err(|_error| {
                    Error::CorruptData(
                        "Provider Publication has invalid display-name snapshot".to_string(),
                    )
                })?;
                let access = ProviderAccess::new(
                    provider_id,
                    request.workspace_id().clone(),
                    request.call_root(),
                    request.credential_route(),
                    version,
                );
                Some(Provider::new(access, display_name, ProviderState::Active))
            }
            (None, None, None)
                if outcome == ProviderPublicationOutcome::Failed && diagnostic.is_some() =>
            {
                None
            }
            _ => {
                return Err(Error::CorruptData(
                    "Provider Publication has inconsistent terminal snapshot".to_string(),
                ));
            }
        };
        Ok(ProviderPublication::new(
            id,
            request,
            outcome,
            diagnostic,
            published_provider,
            self.completed_at_unix_nanos,
        ))
    }
}
#[derive(Debug, FromRow)]
struct RegenerationPublicationRow {
    account_setting: Option<String>,
    account_grant: Option<String>,
    configuration_revision: i64,
    allow_incompatible_configuration: bool,
    id: String,
    request_id: String,
    workspace_id: Option<String>,
    provider_id: String,
    setup_id: String,
    outcome: String,
    diagnostic_code: Option<String>,
    diagnostic_message: Option<String>,
    completed_at_unix_nanos: i64,
}

impl RegenerationPublicationRow {
    fn decode(self) -> Result<ProviderRegenerationPublication, Error> {
        let id = ProviderPublicationId::parse(&self.id).map_err(|_error| {
            Error::CorruptData("Provider regeneration has invalid identity".to_string())
        })?;
        let request_id = ManagementRequestId::parse(&self.request_id).map_err(|_error| {
            Error::CorruptData("Provider regeneration has invalid request identity".to_string())
        })?;
        let owner = decode_owner(self.workspace_id.as_deref(), "Provider regeneration")?;
        let provider_id = ProviderId::parse(&self.provider_id).map_err(|_error| {
            Error::CorruptData("Provider regeneration has invalid Provider identity".to_string())
        })?;
        let setup_id = ProviderSetupId::parse(&self.setup_id).map_err(|_error| {
            Error::CorruptData("Provider regeneration has invalid Setup identity".to_string())
        })?;
        let request = ProviderRegenerationRequest::new(
            request_id,
            owner,
            provider_id,
            setup_id,
            self.configuration_revision,
        );
        let request = match self.account_setting {
            Some(route) => request.with_account_setting(CredentialRoute::from_storage_str(&route)?),
            None => request,
        };
        let request = match self.account_grant {
            Some(grant) => {
                request.with_account_grant(crate::account::ProviderAccountId::parse(&grant)?)
            }
            None => request,
        };
        let request = if self.allow_incompatible_configuration {
            request.with_incompatible_configuration_confirmation()
        } else {
            request
        };
        let outcome = ProviderPublicationOutcome::from_storage_str(&self.outcome)?;
        let diagnostic = match (self.diagnostic_code, self.diagnostic_message, outcome) {
            (None, None, ProviderPublicationOutcome::Succeeded) => None,
            (Some(code), Some(message), ProviderPublicationOutcome::Failed) => {
                Some(PublicationDiagnostic::from_storage(code, message)?)
            }
            _ => {
                return Err(Error::CorruptData(
                    "Provider regeneration has inconsistent terminal evidence".to_string(),
                ));
            }
        };
        Ok(ProviderRegenerationPublication::new(
            id,
            request,
            outcome,
            diagnostic,
            self.completed_at_unix_nanos,
        ))
    }
}

pub(super) async fn publication_by_request_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    request_id: &ManagementRequestId,
) -> Result<Option<ProviderPublication>, Error> {
    let row: Option<PublicationRow> = transaction
        .fetch_optional(publication_statement(workspace_id, request_id))
        .await?;
    row.map(PublicationRow::decode).transpose()
}

fn publication_statement(
    workspace_id: &WorkspaceId,
    request_id: &ManagementRequestId,
) -> sea_query::SelectStatement {
    Query::select()
        .columns([
            ProviderPublications::Id,
            ProviderPublications::RequestId,
            ProviderPublications::WorkspaceId,
            ProviderPublications::SetupId,
            ProviderPublications::RequestedNamespace,
            ProviderPublications::RequestedRoute,
            ProviderPublications::RequestedAccountGrant,
            ProviderPublications::Outcome,
            ProviderPublications::DiagnosticCode,
            ProviderPublications::DiagnosticMessage,
            ProviderPublications::ProviderId,
            ProviderPublications::ProviderDisplayName,
            ProviderPublications::ProviderAccountSettingVersion,
            ProviderPublications::CompletedAtUnixNanos,
        ])
        .from(ProviderPublications::Table)
        .and_where(Expr::col(ProviderPublications::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(ProviderPublications::RequestId).eq(request_id.to_string()))
        .to_owned()
}

pub(super) async fn regeneration_publication_by_request_in_transaction(
    transaction: &mut Transaction<'_>,
    owner: &WorkspaceId,
    request_id: &ManagementRequestId,
) -> Result<Option<ProviderRegenerationPublication>, Error> {
    let row: Option<RegenerationPublicationRow> = transaction
        .fetch_optional(
            Query::select()
                .columns([
                    ProviderRegenerationPublications::Id,
                    ProviderRegenerationPublications::RequestId,
                    ProviderRegenerationPublications::WorkspaceId,
                    ProviderRegenerationPublications::ProviderId,
                    ProviderRegenerationPublications::AllowIncompatibleConfiguration,
                    ProviderRegenerationPublications::ConfigurationRevision,
                    ProviderRegenerationPublications::SetupId,
                    ProviderRegenerationPublications::Outcome,
                    ProviderRegenerationPublications::DiagnosticCode,
                    ProviderRegenerationPublications::DiagnosticMessage,
                    ProviderRegenerationPublications::CompletedAtUnixNanos,
                    ProviderRegenerationPublications::AccountGrant,
                    ProviderRegenerationPublications::AccountSetting,
                ])
                .from(ProviderRegenerationPublications::Table)
                .and_where({
                    let workspace_id = owner;
                    Expr::col(ProviderRegenerationPublications::WorkspaceId)
                        .eq(workspace_id.to_string())
                })
                .and_where(
                    Expr::col(ProviderRegenerationPublications::RequestId)
                        .eq(request_id.to_string()),
                )
                .to_owned(),
        )
        .await?;
    row.map(RegenerationPublicationRow::decode).transpose()
}

pub(super) async fn insert_regeneration_publication(
    transaction: &mut Transaction<'_>,
    publication: &ProviderRegenerationPublication,
) -> Result<(), Error> {
    let request = publication.request();
    transaction
        .execute(
            Query::insert()
                .into_table(ProviderRegenerationPublications::Table)
                .columns([
                    ProviderRegenerationPublications::Id,
                    ProviderRegenerationPublications::RequestId,
                    ProviderRegenerationPublications::WorkspaceId,
                    ProviderRegenerationPublications::ProviderId,
                    ProviderRegenerationPublications::AllowIncompatibleConfiguration,
                    ProviderRegenerationPublications::ConfigurationRevision,
                    ProviderRegenerationPublications::SetupId,
                    ProviderRegenerationPublications::Outcome,
                    ProviderRegenerationPublications::DiagnosticCode,
                    ProviderRegenerationPublications::DiagnosticMessage,
                    ProviderRegenerationPublications::CompletedAtUnixNanos,
                    ProviderRegenerationPublications::AccountGrant,
                    ProviderRegenerationPublications::AccountSetting,
                ])
                .values_panic([
                    Expr::val(publication.id().to_string()),
                    Expr::val(request.request_id().to_string()),
                    Expr::val(Some(ToString::to_string(request.owner()))),
                    Expr::val(request.provider_id().to_string()),
                    Expr::val(request.allows_incompatible_configuration()),
                    Expr::val(request.configuration_revision()),
                    Expr::val(request.setup_id().to_string()),
                    Expr::val(publication.outcome().as_storage_str()),
                    Expr::val(
                        publication
                            .diagnostic()
                            .map(|diagnostic| diagnostic.code().to_string()),
                    ),
                    Expr::val(
                        publication
                            .diagnostic()
                            .map(|diagnostic| diagnostic.message().to_string()),
                    ),
                    Expr::val(publication.completed_at_unix_nanos()),
                    Expr::val(request.account_grant().map(|id| id.to_string())),
                    Expr::val(
                        request
                            .account_setting()
                            .map(CredentialRoute::as_storage_str),
                    ),
                ])
                .to_owned(),
        )
        .await?;
    Ok(())
}

pub(super) async fn publication_request_key_exists_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    request_id: &ManagementRequestId,
) -> Result<bool, Error> {
    Ok(
        publication_request_actor_in_transaction(transaction, workspace_id, request_id)
            .await?
            .is_some(),
    )
}

pub(super) async fn publication_request_actor_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    request_id: &ManagementRequestId,
) -> Result<Option<UserId>, Error> {
    let row: Option<(String,)> = transaction
        .fetch_optional(
            Query::select()
                .column(ProviderPublicationRequestKeys::ActorUserId)
                .from(ProviderPublicationRequestKeys::Table)
                .and_where(
                    Expr::col(ProviderPublicationRequestKeys::WorkspaceId)
                        .eq(workspace_id.to_string()),
                )
                .and_where(
                    Expr::col(ProviderPublicationRequestKeys::RequestId).eq(request_id.to_string()),
                )
                .to_owned(),
        )
        .await?;
    row.map(|(actor,)| {
        UserId::parse(&actor).map_err(|_error| {
            Error::CorruptData("Publication has an invalid initiating actor".into())
        })
    })
    .transpose()
}

pub(super) async fn insert_publication_request_key(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    request_id: &ManagementRequestId,
    actor: &UserId,
) -> Result<(), Error> {
    let statement = Query::insert()
        .into_table(ProviderPublicationRequestKeys::Table)
        .columns([
            ProviderPublicationRequestKeys::RequestId,
            ProviderPublicationRequestKeys::WorkspaceId,
            ProviderPublicationRequestKeys::ActorUserId,
        ])
        .values_panic([
            Expr::val(request_id.to_string()),
            Expr::val(workspace_id.to_string()),
            Expr::val(actor.to_string()),
        ])
        .to_owned();
    transaction.execute(statement).await?;
    Ok(())
}

pub(super) async fn insert_publication(
    transaction: &mut Transaction<'_>,
    publication: &ProviderPublication,
) -> Result<(), Error> {
    let published = publication.published_provider();
    let statement = Query::insert()
        .into_table(ProviderPublications::Table)
        .columns([
            ProviderPublications::Id,
            ProviderPublications::RequestId,
            ProviderPublications::WorkspaceId,
            ProviderPublications::SetupId,
            ProviderPublications::RequestedNamespace,
            ProviderPublications::RequestedRoute,
            ProviderPublications::RequestedAccountGrant,
            ProviderPublications::Outcome,
            ProviderPublications::DiagnosticCode,
            ProviderPublications::DiagnosticMessage,
            ProviderPublications::ProviderId,
            ProviderPublications::ProviderDisplayName,
            ProviderPublications::ProviderAccountSettingVersion,
            ProviderPublications::CompletedAtUnixNanos,
        ])
        .values_panic([
            Expr::val(publication.id().to_string()),
            Expr::val(publication.request_id().to_string()),
            Expr::val(publication.request().workspace_id().to_string()),
            Expr::val(publication.request().setup_id().to_string()),
            Expr::val(publication.request().namespace().as_str()),
            Expr::val(publication.request().credential_route().as_storage_str()),
            Expr::val(
                publication
                    .request()
                    .account_grant()
                    .map(|grant| grant.to_string()),
            ),
            Expr::val(publication.outcome().as_storage_str()),
            Expr::val(
                publication
                    .diagnostic()
                    .map(|diagnostic| diagnostic.code().to_string()),
            ),
            Expr::val(
                publication
                    .diagnostic()
                    .map(|diagnostic| diagnostic.message().to_string()),
            ),
            Expr::val(published.map(|resources| resources.id().to_string())),
            Expr::val(published.map(|resources| resources.display_name().to_string())),
            Expr::val(published.map(super::super::types::Provider::account_setting_version)),
            Expr::val(publication.completed_at_unix_nanos()),
        ])
        .to_owned();
    transaction.execute(statement).await?;
    Ok(())
}
