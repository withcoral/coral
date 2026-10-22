//! Performs `coral.workspace.updateProvider`, and settles one whose worker was
//! lost after the Dispatch Fence.
//!
//! The call takes the path of Reef's update page: an Update Setup of the
//! saved source or a new one, the sent fields confirmed over the configuration
//! saved when the call runs, then a regeneration Publication. Coral publishes
//! the change only when nobody needs to act; otherwise the result is Reef's
//! update page for the Setup, where the Owner confirms what Coral cannot.

use async_trait::async_trait;
use coral_catalog::{
    ProviderChanges, SavedProvider, SourceChange, UpdateProviderArguments, UpdateProviderResult,
};
use coral_db::Transaction;
use coral_provider::access::{CredentialRoute, ProviderNamespace};
use coral_provider::{
    Provider, ProviderPublicationOutcome, ProviderRegenerationPublication,
    ProviderRegenerationRequest, ProviderSetup, ProviderSetupId, ProviderSetupInputKind,
    ProviderSetupSource, ProviderWithConfiguration, PublishedProvider, WorkspaceProviderPublisher,
};
use coral_workspace::WorkspaceId;

use super::get_provider::source_output;
use super::setup_operation::{
    self, Publisher, SetupOperation, configuration_rejection, recover_published, request_id,
    setup_error,
};
use super::{BuiltinHost, provider_facts};
use crate::lifecycle::RunRecord;
use crate::{CoralFailure, Error, InvocationId, InvocationResultKind, Rejection};

/// `updateProvider`'s arguments in Coral's domain types.
#[derive(Clone)]
pub(crate) struct UpdateProvider {
    namespace: ProviderNamespace,
    arguments: UpdateProviderArguments,
}

/// One `updateProvider` call, aimed at the Provider saved when it began.
struct TargetedUpdate<'a> {
    call: &'a UpdateProvider,
    /// The Provider as saved when the call began. Its etag fences a change an
    /// Owner makes since.
    provider: Provider,
    source: ProviderSetupSource,
}

/// What configuring an Update Setup leaves for the rest of the call.
struct ConfiguredUpdate {
    /// The confirmed revision, which the Publication names.
    revision: i64,
    /// The confirmed configuration breaks access to the Provider.
    interrupts_access: bool,
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

    /// Prepares and configures an Update Setup as the invoking User. Coral
    /// publishes it when nobody needs to act; otherwise the result is the Reef
    /// page where a person finishes it. The call never waits for that person.
    pub(super) async fn perform(&self, host: &BuiltinHost<'_>) -> Result<serde_json::Value, Error> {
        let saved = host
            .providers
            .inspect_provider_configuration(
                host.context.actor(),
                host.workspace_id,
                &self.namespace,
            )
            .await
            .map_err(update_error)?;
        let source = self.source(&saved)?;
        let update = TargetedUpdate {
            call: self,
            provider: saved.provider,
            source,
        };
        setup_operation::perform(&update, host).await
    }

    /// Where the Update Setup reads the Provider from. A refresh reads the
    /// source URL saved when the call runs. An MCP server is discovered with
    /// the sent non-secret headers, or else the saved ones.
    fn source(&self, saved: &ProviderWithConfiguration) -> Result<ProviderSetupSource, Error> {
        let request_headers = || {
            self.arguments
                .choices
                .request_headers
                .clone()
                .unwrap_or_else(|| saved.request_headers.clone())
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
                setup_operation::setup_source(source.clone(), request_headers())
            }
        })
    }
}

#[async_trait]
impl SetupOperation for TargetedUpdate<'_> {
    type Configured = ConfiguredUpdate;
    type Publication = ProviderRegenerationPublication;

    async fn prepare(&self, host: &BuiltinHost<'_>) -> Result<ProviderSetup, Error> {
        host.providers
            .prepare_setup(
                host.context,
                host.workspace_id,
                self.source.clone(),
                Some(self.provider.id()),
            )
            .await
            .map_err(update_error)
    }

    /// Confirms the sent fields over the configuration the Update Setup
    /// saved, and decides who publishes it.
    async fn configure(
        &self,
        host: &BuiltinHost<'_>,
        setup: &ProviderSetup,
    ) -> Result<(Publisher, ConfiguredUpdate), Error> {
        let review = host
            .providers
            .review_setup_configuration(host.context.actor(), host.workspace_id, setup.id())
            .await
            .map_err(update_error)?;
        // An Update Setup starts from the Provider's saved configuration.
        let Some(saved) = review.confirmed.clone() else {
            return Err(coral_provider::Error::CorruptData(
                "an Update Setup has no saved configuration".into(),
            )
            .into());
        };
        let discovered = match setup.input_kind() {
            ProviderSetupInputKind::Mcp => review.document.map(|document| document.suggested),
            ProviderSetupInputKind::OpenApi => None,
        };
        let confirmed = host
            .providers
            .confirm_setup_configuration(
                host.context,
                host.workspace_id,
                setup.id(),
                review.revision,
                self.call
                    .arguments
                    .configuration(saved, discovered.as_ref()),
            )
            .await
            .map_err(update_error)?;
        if let Some(rejection) = configuration_rejection(&confirmed) {
            return Err(Error::InvocationRejected(rejection));
        }
        let publishability = host
            .providers
            .update_publishability(
                host.context.actor(),
                host.workspace_id,
                setup.id(),
                self.call
                    .arguments
                    .credential_route
                    .map(CredentialRoute::from),
            )
            .await
            .map_err(update_error)?;
        let publisher = setup_operation::publisher(
            host,
            setup.id(),
            publishability,
            confirmed.confirmed.as_ref(),
        )
        .await?;
        Ok((
            publisher,
            ConfiguredUpdate {
                revision: confirmed.revision,
                interrupts_access: confirmed.interrupts_access == Some(true),
            },
        ))
    }

    /// Reef's update page for the Setup, on the task Reef starts for the same
    /// change from the Provider's settings: Check for updates or Replace source
    /// for a new read, Change account setting for a route alone, and Edit
    /// configuration otherwise. As Reef does after a read or a route change
    /// that breaks access, it opens on the configuration, so the person sees
    /// the saved values the change keeps. It names the Provider's etag, so a
    /// change made since fails there, and the route the Program asked for. It
    /// never confirms an incompatible configuration: the person does.
    fn needs_sign_in(
        &self,
        host: &BuiltinHost<'_>,
        setup: &ProviderSetupId,
        configured: ConfiguredUpdate,
    ) -> serde_json::Value {
        let arguments = &self.call.arguments;
        let (task, configure) = match arguments.source {
            SourceChange::Refresh | SourceChange::Replace(_) => {
                ("refresh", configured.interrupts_access)
            }
            SourceChange::Keep if arguments.choices.is_empty() => {
                ("accounts", configured.interrupts_access)
            }
            // Edit configuration opens on the configuration already.
            SourceChange::Keep => ("configuration", false),
        };
        // A Setup id and an etag need no URL encoding.
        let mut query = format!("setup={setup}&task={task}&etag={}", self.provider.etag());
        if configure {
            query.push_str("&configure=1");
        }
        // Reef names each route as a Program does.
        if let Some(route) = arguments.credential_route {
            query.push_str("&account=");
            query.push_str(route.name());
        }
        UpdateProviderResult::NeedsSignIn {
            url: host.human_interface.page_url(&format!(
                "/workspaces/{}/providers/{}/update?{query}",
                host.workspace_id,
                self.provider.id()
            )),
            namespace: self.call.namespace.as_str().to_owned(),
        }
        .into_operation_result()
    }

    async fn publish(
        &self,
        host: &BuiltinHost<'_>,
        publisher: &dyn WorkspaceProviderPublisher,
        setup: &ProviderSetupId,
        configured: ConfiguredUpdate,
    ) -> Result<ProviderRegenerationPublication, coral_provider::Error> {
        let context = host.context.clone().with_etag(&self.provider.etag())?;
        publisher
            .regenerate_workspace_provider(
                &context,
                ProviderRegenerationRequest::new(
                    host.request_id.clone(),
                    host.workspace_id.clone(),
                    self.provider.id().clone(),
                    setup.clone(),
                    configured.revision,
                ),
            )
            .await
    }

    fn publication_error(&self, error: coral_provider::Error) -> Error {
        match error {
            coral_provider::Error::ProviderNotFound => update_error(error),
            // The Provider changed since Coral decided nobody needs to act.
            coral_provider::Error::IncompatibleConfigurationConfirmationRequired
            | coral_provider::Error::AccountRequired => {
                Error::InvocationRejected(CoralFailure::ProviderPublicationFailed.into())
            }
            other => setup_operation::publication_error(other),
        }
    }

    async fn published(
        &self,
        host: &BuiltinHost<'_>,
        publication: ProviderRegenerationPublication,
    ) -> Result<serde_json::Value, Error> {
        if publication.outcome() != ProviderPublicationOutcome::Succeeded {
            return Err(Error::InvocationRejected(
                CoralFailure::ProviderPublicationFailed.into(),
            ));
        }
        let details = host
            .providers
            .published_provider(host.workspace_id, &self.provider, host.principal)
            .await
            .map_err(update_error)?;
        Ok(published_result(&details))
    }
}

/// Settles an `updateProvider` whose worker was lost after the dispatch
/// fence, from the regeneration Publication Coral attempted under the
/// Invocation's identity. A published change becomes the Operation Result;
/// otherwise the Run ended first. A Setup that was never published expires at
/// its deadline.
pub(super) async fn recover(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    invocation: &InvocationId,
) -> Result<(InvocationResultKind, Option<String>), Error> {
    let publication = coral_provider::regeneration_for_request_in_transaction(
        transaction,
        &run.workspace_id,
        &request_id(invocation)?,
    )
    .await?;
    let provider = publication
        .as_ref()
        .filter(|publication| publication.outcome() == ProviderPublicationOutcome::Succeeded)
        .map(|publication| publication.request().provider_id());
    recover_published(transaction, run, provider, published_result).await
}

/// What an `updateProvider` call with `arguments` changes in the Provider as
/// saved now, in the caller's snapshot, for the Owner who reviews the call.
/// Arguments this version cannot read, as another Coral version could have
/// stored them, show no changes.
pub(super) async fn changes(
    transaction: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    arguments: &serde_json::Value,
) -> Result<Option<ProviderChanges>, Error> {
    let Ok(arguments) = UpdateProviderArguments::parse(arguments) else {
        return Ok(None);
    };
    let Ok(namespace) = ProviderNamespace::parse(&arguments.namespace) else {
        return Ok(None);
    };
    let saved =
        coral_provider::provider_configuration_in_transaction(transaction, workspace, &namespace)
            .await?
            .map(saved_provider)
            .transpose()?;
    Ok(Some(arguments.changes(saved)))
}

/// A saved Provider, as the review of an update reads it.
fn saved_provider(saved: ProviderWithConfiguration) -> Result<SavedProvider, Error> {
    Ok(SavedProvider {
        configuration: saved.configuration(),
        credential_route: saved.provider.access().credential_route().into(),
        source: source_output(saved.source)?,
    })
}

/// The Operation Result for a change Coral published. It says what Coral
/// resolved, so the Program need not guess whether its next call needs a person.
fn published_result(published: &PublishedProvider) -> serde_json::Value {
    UpdateProviderResult::Published {
        namespace: published.provider.call_root().name().to_owned(),
        facts: provider_facts(published),
    }
    .into_operation_result()
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
