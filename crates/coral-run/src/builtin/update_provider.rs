//! Performs `coral.workspace.updateProvider`, and settles one whose worker was
//! lost after the Policy Enforcement Point.
//!
//! The call takes the path of Reef's update page: an Update Setup of the
//! saved source or a new one, the sent fields confirmed over the configuration
//! saved when the call runs. The result is Reef's update page for the Setup,
//! where a person reviews and publishes the change. A call that changes only
//! the display name takes the path of Reef's settings page instead: a rename
//! that keeps the Provider's Generation.

use super::setup_tool::{self, setup_error};
use super::{BuiltinHost, provider_facts};
use crate::lifecycle::RunRecord;
use crate::{CoralFailure, Error, InvocationId, InvocationResultKind, Rejection};
use coral_catalog::{SourceChange, UpdateProviderArguments, UpdateProviderResult};
use coral_db::Transaction;
use coral_provider::access::{CredentialRoute, ProviderNamespace};
use coral_provider::{
    ProgramSetup, ProgramSetupPurpose, ProgramSetupRequest, Provider, ProviderDisplayName,
    ProviderId, ProviderSetupInputKind, ProviderSetupSource, ProviderWithConfiguration,
};
use coral_workspace::ChangeContext;

/// `updateProvider`'s arguments in Coral's domain types.
#[derive(Clone)]
pub(crate) struct UpdateProvider {
    namespace: ProviderNamespace,
    arguments: UpdateProviderArguments,
}

impl UpdateProvider {
    /// The call for arguments the contract accepted.
    pub(super) fn new(arguments: UpdateProviderArguments) -> Result<Self, Rejection> {
        // The contract checks the namespace with the same rule, so this does
        // not fail.
        let namespace = ProviderNamespace::parse(&arguments.namespace)
            .map_err(|_error| Rejection::from(CoralFailure::InvalidInput))?;
        Ok(Self {
            namespace,
            arguments,
        })
    }

    /// Renames the Provider when the call changes only its display name.
    /// Otherwise prepares and configures an Update Setup as the invoking User.
    /// The result is the Reef page where a person reviews and publishes it. The
    /// call never waits for that person.
    pub(super) async fn perform(&self, host: &BuiltinHost<'_>) -> Result<serde_json::Value, Error> {
        // Only a User reaches updateProvider; an Agent changes no configuration.
        let Some(context) = host.context else {
            return Err(Error::InvocationRejected(
                CoralFailure::AuthorityLost.into(),
            ));
        };
        let saved = host
            .providers
            .inspect_provider_configuration(context.actor(), host.workspace_id, &self.namespace)
            .await
            .map_err(update_error)?;
        if let Some(name) = self.arguments.rename() {
            return rename(host, context, &saved.provider, name).await;
        }
        let request = ProgramSetupRequest {
            source: self.source(&saved)?,
            purpose: ProgramSetupPurpose::Update {
                provider: saved.provider.id().clone(),
                route: self.arguments.credential_route.map(CredentialRoute::from),
                patch: self.arguments.choices.clone(),
            },
        };
        let prepared = setup_tool::prepare(host, context, request, update_error).await?;
        Ok(self.update_page(host, &saved.provider, &prepared))
    }

    /// Where the Update Setup reads the Provider from. A refresh reads the
    /// source URL saved when the call runs. An MCP server is discovered with
    /// the saved non-secret headers and the sent changes to them.
    fn source(&self, saved: &ProviderWithConfiguration) -> Result<ProviderSetupSource, Error> {
        let request_headers = || {
            self.arguments
                .choices
                .over(saved.configuration())
                .request_headers
        };
        Ok(match &self.arguments.source {
            SourceChange::Keep => ProviderSetupSource::Current,
            SourceChange::Refresh => match (saved.source.kind, &saved.source.url) {
                (_, None) => {
                    return Err(Error::InvocationRejected(Rejection::invalid_input(
                        "refresh",
                        "the Provider was read from an uploaded document, which Coral cannot \
                         read again; send `source` with the new document instead",
                    )));
                }
                (ProviderSetupInputKind::OpenApi, Some(url)) => {
                    ProviderSetupSource::OpenApiUrl(url.clone())
                }
                (ProviderSetupInputKind::Mcp, Some(url)) => ProviderSetupSource::McpUrl {
                    url: url.clone(),
                    request_headers: request_headers(),
                },
            },
            SourceChange::Replace(source) => {
                setup_tool::setup_source(source.clone(), request_headers())
            }
        })
    }

    /// Reef's update page for the Setup, on the task Reef starts for the same
    /// change from the Provider's settings: Check for updates or Replace source
    /// for a new read, Change account setting for a route alone, and Edit
    /// configuration otherwise. As Reef does after a read or a route change
    /// that breaks access, it opens on the configuration, so the person sees
    /// the saved values the change keeps. It names the Provider's etag, so a
    /// change made since fails there, and the route the Program asked for. It
    /// never confirms an incompatible configuration: the person does.
    fn update_page(
        &self,
        host: &BuiltinHost<'_>,
        provider: &Provider,
        prepared: &ProgramSetup,
    ) -> serde_json::Value {
        let arguments = &self.arguments;
        let (task, configure) = match arguments.source {
            SourceChange::Refresh | SourceChange::Replace(_) => {
                ("refresh", prepared.interrupts_access)
            }
            SourceChange::Keep if arguments.choices.is_empty() => {
                ("accounts", prepared.interrupts_access)
            }
            // Edit configuration opens on the configuration already.
            SourceChange::Keep => ("configuration", false),
        };
        // A Setup id and an etag need no URL encoding.
        let mut query = format!(
            "setup={}&task={task}&etag={}",
            prepared.setup,
            provider.etag()
        );
        if configure {
            query.push_str("&configure=1");
        }
        // Reef names each route as a Program does.
        if let Some(route) = arguments.credential_route {
            query.push_str("&account=");
            query.push_str(route.name());
        }
        UpdateProviderResult::NeedsPerson {
            url: host.human_interface.page_url(&format!(
                "/workspaces/{}/providers/{}/update?{query}",
                host.workspace_id,
                provider.id()
            )),
            namespace: self.namespace.as_str().to_owned(),
        }
        .into_tool_result()
    }
}

/// Renames `provider` through the workflow Reef renames with, under the etag
/// it had when the call began, and returns what the Program needs to call it.
async fn rename(
    host: &BuiltinHost<'_>,
    context: &ChangeContext,
    provider: &Provider,
    name: &str,
) -> Result<serde_json::Value, Error> {
    // An update Setup trims the name it confirms, so a rename does too.
    let display_name = ProviderDisplayName::parse(name.trim()).map_err(|error| match error {
        coral_provider::Error::InvalidInput(reason) => {
            Error::InvocationRejected(Rejection::invalid_input("displayName", &reason))
        }
        other => other.into(),
    })?;
    let Some(renamer) = host.renamer else {
        return Err(Error::InvocationRejected(
            CoralFailure::ToolUnavailable.into(),
        ));
    };
    // A Run that ended could no longer return the Provider: rename nothing.
    if !host.run.still_current().await? {
        return Err(Error::ProgramHostTerminated);
    }
    let context = context.clone().with_etag(&provider.etag())?;
    let renamed = renamer
        .rename_provider(&context, host.workspace_id, provider.id(), display_name)
        .await
        .map_err(|error| match error {
            // An Owner changed the Provider since the call began, as a
            // regeneration reports it.
            coral_provider::Error::EtagMismatch => {
                Error::InvocationRejected(CoralFailure::ProviderPublicationFailed.into())
            }
            other => update_error(other),
        })?;
    renamed_result(host, renamed.id()).await
}

/// What the Program needs to call `provider`, which Coral renamed, read in one
/// snapshot, so it need not guess whether its next call needs a person. A
/// Provider deleted since is a failure the Program can catch, as when the call
/// began.
async fn renamed_result(
    host: &BuiltinHost<'_>,
    provider: &ProviderId,
) -> Result<serde_json::Value, Error> {
    let mut snapshot = host.database.begin_read_snapshot().await?;
    let published = coral_provider::published_provider_in_transaction(
        &mut snapshot,
        host.workspace_id,
        provider,
        host.principal,
    )
    .await
    .map_err(update_error)?;
    let rules =
        coral_policy::exact_tool_rules_in_transaction(&mut snapshot, host.workspace_id).await?;
    snapshot.rollback().await?;
    Ok(UpdateProviderResult::Published {
        namespace: published.provider.call_root().name().to_owned(),
        facts: provider_facts(&published, &rules),
    }
    .into_tool_result())
}

/// Settles an `updateProvider` whose worker was lost after the Policy
/// Enforcement Point. A rename leaves no record of the call that made it, so a
/// lost rename is indeterminate. Any other call publishes nothing, so the Run
/// ended first; its Setup expires at its deadline.
pub(super) async fn recover(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    invocation: &InvocationId,
) -> Result<(InvocationResultKind, Option<String>), Error> {
    let arguments = crate::invocation_repository::arguments(
        transaction,
        &run.workspace_id,
        &run.id,
        invocation,
    )
    .await?;
    // The contract accepted these arguments when the Invocation was admitted.
    if UpdateProviderArguments::parse(&arguments).is_ok_and(|call| call.rename().is_some()) {
        return Ok((InvocationResultKind::Indeterminate, None));
    }
    Ok((InvocationResultKind::RunTerminated, None))
}

/// A Provider that no longer holds the namespace is a failure the Program can
/// catch, as for every other Setup failure.
fn update_error(error: coral_provider::Error) -> Error {
    match error {
        coral_provider::Error::ProviderNotFound => {
            Error::InvocationRejected(CoralFailure::ProviderNotFound.into())
        }
        other => setup_error(other),
    }
}
