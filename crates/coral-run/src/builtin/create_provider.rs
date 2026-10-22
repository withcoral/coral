//! Performs `coral.workspace.createProvider`, and settles one whose worker was
//! lost after the Dispatch Fence.

use coral_catalog::{
    ConfigurationChoices, CreateProviderArguments, CreateProviderResult, ProviderConfiguration,
    ProviderSourceArgument,
};
use coral_db::Transaction;
use coral_provider::access::{CredentialRoute, ProviderNamespace};
use coral_provider::account::AccountModule;
use coral_provider::{
    ManagementRequestId, Provider, ProviderSetupId, ProviderSetupSource, PublishabilityRejection,
    SetupPublishability, SetupRequest, WorkspaceProviderPublicationRequest,
};
use coral_workspace::{ChangeContext, Principal, WorkspaceId};

use super::{BuiltinHost, authority_error, provider_facts, setup_url};
use crate::error::InputProblem;
use crate::lifecycle::RunRecord;
use crate::{CoralFailure, Error, InvocationId, InvocationResultKind, Rejection};

/// `createProvider`'s arguments in Coral's domain types.
#[derive(Clone)]
pub(crate) struct CreateProvider {
    namespace: ProviderNamespace,
    source: ProviderSetupSource,
    route: CredentialRoute,
    choices: ConfigurationChoices,
}

/// Who publishes a configured Setup.
enum Publisher {
    /// Nobody needs to sign in, so Coral publishes it at once.
    Coral,
    /// A person signs in or enters a credential, and Reef publishes it.
    Person,
}

impl CreateProvider {
    /// The call for arguments the contract accepted. An MCP server is
    /// discovered with the same non-secret headers it is configured with.
    pub(super) fn new(arguments: CreateProviderArguments) -> Result<Self, Rejection> {
        // The contract checks the namespace with the same rule, so this does
        // not fail.
        let namespace = ProviderNamespace::parse(&arguments.namespace)
            .map_err(|_error| Rejection::from(CoralFailure::InvalidInput))?;
        let source = match arguments.source {
            ProviderSourceArgument::OpenApiDocument(document) => {
                ProviderSetupSource::OpenApiDocument(document)
            }
            ProviderSourceArgument::OpenApiUrl(url) => ProviderSetupSource::OpenApiUrl(url),
            ProviderSourceArgument::McpUrl(url) => ProviderSetupSource::McpUrl {
                url,
                request_headers: arguments
                    .choices
                    .request_headers
                    .clone()
                    .unwrap_or_default(),
            },
        };
        Ok(Self {
            namespace,
            source,
            route: arguments.credential_route.into(),
            choices: arguments.choices,
        })
    }

    /// Prepares and configures a Setup as the invoking User. Coral publishes it
    /// when nobody needs to sign in; otherwise the result is the Reef page where
    /// a person finishes it. The call never waits for that person.
    pub(super) async fn perform(&self, host: &BuiltinHost<'_>) -> Result<serde_json::Value, Error> {
        // Only a User reaches createProvider; an Agent changes no configuration.
        let Some(context) = host.context else {
            return Err(Error::InvocationRejected(
                CoralFailure::AuthorityLost.into(),
            ));
        };
        // A taken namespace would fail the Publication; fail before any fetch or Setup.
        if host
            .providers
            .namespace_in_use(context.actor(), host.workspace_id, &self.namespace)
            .await
            .map_err(setup_error)?
        {
            return Err(Error::InvocationRejected(
                CoralFailure::ProviderNamespaceConflict.into(),
            ));
        }
        // Only the fetch waits on the network, so only it stops with the Run.
        // Once a Setup exists, the call runs to an end that publishes it,
        // links a person to it, or ends it.
        let setup = tokio::select! {
            biased;
            () = host.run.stopped() => return Err(Error::ProgramHostTerminated),
            setup = host.providers.prepare_requested_setup(
                context,
                host.workspace_id,
                self.source.clone(),
                SetupRequest {
                    namespace: self.namespace.clone(),
                    route: self.route,
                },
            ) => setup.map_err(setup_error)?,
        };
        #[cfg(test)]
        host.run.setup_prepared();
        let publisher = match self.configure(host, context, setup.id()).await {
            Ok(publisher) => publisher,
            Err(error) => {
                // Nobody will finish a Setup whose configuration failed.
                end_setup(host, setup.id()).await;
                return Err(error);
            }
        };
        match publisher {
            Publisher::Person => {
                // A Run that ended could no longer hand anyone the link.
                require_current_run(host, setup.id()).await?;
                Ok(self.needs_sign_in(host, setup.id()))
            }
            Publisher::Coral => self.publish(host, context, setup.id()).await,
        }
    }

    /// Confirms the Program's configuration over the source's suggestions and
    /// decides who publishes the Setup. The route must suit the source.
    async fn configure(
        &self,
        host: &BuiltinHost<'_>,
        context: &ChangeContext,
        setup: &ProviderSetupId,
    ) -> Result<Publisher, Error> {
        let invalid = |field: &str, reason: &str| {
            Error::InvocationRejected(Rejection::invalid_input(field, reason))
        };
        let review = host
            .providers
            .review_setup_configuration(context.actor(), host.workspace_id, setup)
            .await
            .map_err(setup_error)?;
        let Some(document) = review.document else {
            return Err(Error::InvocationRejected(
                configuration_rejection(&review).unwrap_or_else(|| {
                    Rejection::invalid_input(
                        "source",
                        "Coral could not read a configuration from the source",
                    )
                }),
            ));
        };
        let suggested = document.suggested;
        let confirmed = host
            .providers
            .confirm_setup_configuration(
                context,
                host.workspace_id,
                setup,
                review.revision,
                self.choices.over(suggested),
            )
            .await
            .map_err(setup_error)?;
        if let Some(rejection) = configuration_rejection(&confirmed) {
            return Err(Error::InvocationRejected(rejection));
        }
        match host
            .providers
            .setup_publishability(context.actor(), host.workspace_id, setup, self.route)
            .await
            .map_err(setup_error)?
        {
            SetupPublishability::Coral => Ok(Publisher::Coral),
            // The person's sign-in could not start without an OAuth application.
            SetupPublishability::Person
                if !has_oauth_application(host, context, setup, confirmed.confirmed.as_ref())
                    .await? =>
            {
                Err(invalid(
                    "authentication.client",
                    "Coral cannot register an OAuth application with this service; register \
                     one with the service and give its `client_id`",
                ))
            }
            SetupPublishability::Person => Ok(Publisher::Person),
            SetupPublishability::Rejected(PublishabilityRejection::NotConfigured) => Err(invalid(
                "source",
                "Coral could not tell whether the source needs a credential",
            )),
            // The route must suit the source, as Reef offers it; no route falls back to another.
            SetupPublishability::Rejected(PublishabilityRejection::SourceNeedsCredential) => {
                Err(invalid(
                    "credentialRoute",
                    "the source needs a credential; use `fixed` or `caller`",
                ))
            }
            SetupPublishability::Rejected(PublishabilityRejection::SourceNeedsNoCredential) => {
                Err(invalid(
                    "credentialRoute",
                    "the source declares no credential; use `none`, or give `authentication` \
                     to say how a person signs in",
                ))
            }
        }
    }

    /// The Reef page where a person finishes the Setup.
    fn needs_sign_in(&self, host: &BuiltinHost<'_>, setup: &ProviderSetupId) -> serde_json::Value {
        CreateProviderResult::NeedsSignIn {
            url: setup_url(host, setup),
            namespace: self.namespace.as_str().to_owned(),
        }
        .into_operation_result()
    }

    /// Publishes the Setup as the invoking User, through the workflow Reef
    /// publishes with, and returns what the Program needs to call it.
    async fn publish(
        &self,
        host: &BuiltinHost<'_>,
        context: &ChangeContext,
        setup: &ProviderSetupId,
    ) -> Result<serde_json::Value, Error> {
        let Some(publisher) = host.publisher else {
            end_setup(host, setup).await;
            return Err(Error::InvocationRejected(
                CoralFailure::OperationUnavailable.into(),
            ));
        };
        // A Run that ended could no longer return the Provider: publish nothing.
        // The Run can still end between this check and the Publication; the
        // Provider then exists and search finds it, though the call ended.
        require_current_run(host, setup).await?;
        let publication = match publisher
            .publish_workspace_provider(
                context,
                WorkspaceProviderPublicationRequest::new(
                    host.request_id.clone(),
                    host.workspace_id.clone(),
                    setup.clone(),
                    self.namespace.clone(),
                    self.route,
                ),
            )
            .await
        {
            Ok(publication) => publication,
            Err(error) => {
                // The Publication left the Setup reusable, but nobody will reuse it.
                end_setup(host, setup).await;
                return Err(publication_error(error));
            }
        };
        let Some(provider) = publication.published_provider() else {
            return Err(Error::InvocationRejected(
                if publication.failed_on_namespace_conflict() {
                    CoralFailure::ProviderNamespaceConflict
                } else {
                    CoralFailure::ProviderPublicationFailed
                }
                .into(),
            ));
        };
        let mut snapshot = host.database.begin_read_snapshot().await?;
        let result = published_result_in_transaction(
            &mut snapshot,
            host.workspace_id,
            provider,
            host.principal,
        )
        .await?;
        snapshot.rollback().await?;
        Ok(result)
    }
}

/// Settles a `createProvider` whose worker was lost after the dispatch fence,
/// from the Publication Coral attempted under the Invocation's identity. A
/// published Provider becomes the Operation Result; otherwise the Run ended
/// first. A Setup that was never published expires at its deadline.
pub(super) async fn recover(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    invocation: &InvocationId,
) -> Result<(InvocationResultKind, Option<String>), Error> {
    let request = ManagementRequestId::parse(&invocation.to_string())?;
    let published = match coral_provider::publication_for_request_in_transaction(
        transaction,
        &run.workspace_id,
        &request,
    )
    .await?
    {
        Some(publication) => publication.published_provider().cloned(),
        None => None,
    };
    let Some(provider) = published else {
        return Ok((InvocationResultKind::RunTerminated, None));
    };
    let result = match published_result_in_transaction(
        transaction,
        &run.workspace_id,
        &provider,
        &run.principal,
    )
    .await
    {
        Ok(result) => result,
        // An Owner deleted the Provider since; no result can describe it now.
        Err(Error::Provider(coral_provider::Error::ProviderNotFound)) => {
            return Ok((InvocationResultKind::RunTerminated, None));
        }
        Err(error) => return Err(error),
    };
    let result = serde_json::to_string(&result).map_err(Error::ProgramValueJson)?;
    Ok((InvocationResultKind::UpstreamOutcome, Some(result)))
}

/// Fails unless the Run still accepts this call's result. A Run that ended,
/// or whose state cannot be read, ends the Setup this call prepared.
async fn require_current_run(host: &BuiltinHost<'_>, setup: &ProviderSetupId) -> Result<(), Error> {
    let current = host.run.still_current().await;
    if matches!(current, Ok(true)) {
        return Ok(());
    }
    end_setup(host, setup).await;
    current.and(Err(Error::ProgramHostTerminated))
}

/// Whether a person's sign-in to `configuration` has an OAuth application, as
/// Reef offers it. Without accounts, Coral sets none up.
async fn has_oauth_application(
    host: &BuiltinHost<'_>,
    context: &ChangeContext,
    setup: &ProviderSetupId,
    configuration: Option<&ProviderConfiguration>,
) -> Result<bool, Error> {
    let authentication = configuration.and_then(|values| values.authentication.as_ref());
    match host.accounts {
        Some(accounts) => accounts
            .setup_oauth_application_available(
                context.actor(),
                host.workspace_id,
                setup,
                authentication,
            )
            .await
            .map_err(setup_error),
        None => Ok(AccountModule::names_oauth_application(authentication)),
    }
}

/// Ends, as Coral, a Setup that this call prepared but nobody will finish.
/// Best effort: the Setup still expires at its deadline.
async fn end_setup(host: &BuiltinHost<'_>, setup: &ProviderSetupId) {
    let Ok(now) = host.run.now() else {
        return;
    };
    let _ended = host
        .providers
        .abandon_setup(host.workspace_id, setup, now)
        .await;
}

/// The Operation Result for `provider`, which Coral published, read in the
/// caller's transaction. It says what Coral resolved, so the Program need not
/// guess whether its next call needs a person.
async fn published_result_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    provider: &Provider,
    principal: &Principal,
) -> Result<serde_json::Value, Error> {
    let published = coral_provider::published_provider_in_transaction(
        transaction,
        workspace_id,
        provider,
        principal,
    )
    .await?;
    let rules =
        coral_policy::exact_operation_rules_in_transaction(transaction, workspace_id).await?;
    Ok(CreateProviderResult::Published {
        namespace: published.provider.call_root().name().to_owned(),
        facts: provider_facts(&published, &rules),
    }
    .into_operation_result())
}

/// A Publication rejected because its Setup ended or changed, or its request
/// is stale, is a failure the Program can catch. Persistence and corrupt
/// evidence still stop the Run for recovery.
fn publication_error(error: coral_provider::Error) -> Error {
    match error {
        coral_provider::Error::ProviderSetupNotFound
        | coral_provider::Error::ProviderSetupNotReady
        | coral_provider::Error::ProviderSetupConsumed
        | coral_provider::Error::EtagMismatch
        | coral_provider::Error::RequestIdConflict
        | coral_provider::Error::ProviderChangedDuringPublication => {
            Error::InvocationRejected(CoralFailure::ProviderPublicationFailed.into())
        }
        other => setup_error(other),
    }
}

/// Maps a Setup preparation failure to the Program-safe vocabulary. Only
/// input problems are the Program's to catch; anything else is a host defect.
fn setup_error(error: coral_provider::Error) -> Error {
    let source =
        |reason: &str| Error::InvocationRejected(Rejection::invalid_input("source", reason));
    match error {
        coral_provider::Error::OutboundDestinationRejected => {
            Error::InvocationRejected(CoralFailure::OutboundDestinationRejected.into())
        }
        // Coral got no usable response from the source: it did not answer, or
        // answered with an error status. The arguments may be right, so this
        // is not the Program's to correct.
        coral_provider::Error::InputUnavailable => {
            Error::InvocationRejected(CoralFailure::UpstreamIncomplete.into())
        }
        // A source Coral fetched but cannot use is the Program's to correct.
        // Each reason is Coral's own: a fetched body or error never reaches it.
        coral_provider::Error::InvalidInput(_)
        | coral_provider::Error::Catalog(coral_catalog::Error::InvalidInput(_)) => {
            source("Coral cannot read an OpenAPI document or an MCP server from the source")
        }
        coral_provider::Error::Catalog(coral_catalog::Error::OpenApiDocumentTooLarge) => {
            source("the OpenAPI document exceeds Coral's size limit")
        }
        coral_provider::Error::AuthorizationDiscovery(code) => source(&format!(
            "Coral could not discover how to sign in to the MCP server ({code})"
        )),
        coral_provider::Error::AccountInput => Error::InvocationRejected(Rejection::invalid_input(
            "authentication",
            "Coral cannot use this authentication method for the source",
        )),
        coral_provider::Error::ProviderCallRootConflict => {
            Error::InvocationRejected(CoralFailure::ProviderNamespaceConflict.into())
        }
        other => authority_error(other),
    }
}

/// Every problem Coral found when it read or confirmed the configuration, as
/// the arguments to correct. Coral writes these field errors and diagnostic
/// codes without any submitted value.
fn configuration_rejection(
    confirmed: &coral_provider::SetupConfigurationReview,
) -> Option<Rejection> {
    let problems: Vec<_> = confirmed
        .errors
        .iter()
        .map(|error| {
            InputProblem::new(
                CreateProviderArguments::argument_for(&error.field),
                error.message.clone(),
            )
        })
        .chain(confirmed.diagnostic.iter().map(|diagnostic| {
            InputProblem::new(
                "source",
                format!("Coral cannot build a Provider from the source ({diagnostic})"),
            )
        }))
        .collect();
    (!problems.is_empty()).then(|| Rejection::invalid_inputs(problems))
}
