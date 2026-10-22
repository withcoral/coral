//! The flow each Built-in Operation that prepares a Provider Setup for a
//! Program follows: `createProvider` for a new Provider, `updateProvider` for
//! a new Generation of one. Coral prepares and configures the Setup as the
//! invoking User, then publishes it when nobody needs to act, or returns the
//! Reef page where a person finishes it. The call never waits for that
//! person, and Coral ends a Setup it prepared that nobody will finish.

use async_trait::async_trait;
use std::collections::BTreeMap;

use coral_catalog::{ConfigurationChoices, ProviderConfiguration, ProviderSourceArgument};
use coral_db::Transaction;
use coral_provider::account::AccountModule;
use coral_provider::{
    ManagementRequestId, ProviderId, ProviderSetup, ProviderSetupId, ProviderSetupSource,
    PublishabilityRejection, PublishedProvider, SetupPublishability, WorkspaceProviderPublisher,
};

use super::{BuiltinHost, authority_error};
use crate::error::InputProblem;
use crate::lifecycle::RunRecord;
use crate::{CoralFailure, Error, InvocationId, InvocationResultKind, Rejection};

/// Who publishes a configured Setup.
pub(super) enum Publisher {
    /// Nobody needs to act, so Coral publishes it at once.
    Coral,
    /// A person signs in, enters a credential or confirms the change, and
    /// Reef publishes it.
    Person,
}

/// One Built-in Operation that prepares a Provider Setup. Each step is the
/// Operation's own; [`perform`] runs them in order.
#[async_trait]
pub(super) trait SetupOperation: Sync {
    /// What configuring the Setup leaves for its Publication, or for the
    /// page where a person finishes it.
    type Configured: Send;
    /// The terminal Publication Coral attempts.
    type Publication: Send;

    /// Prepares the Setup. Only this step waits on the network, so only it
    /// stops with the Run.
    async fn prepare(&self, host: &BuiltinHost<'_>) -> Result<ProviderSetup, Error>;

    /// Confirms the Program's configuration and decides who publishes the Setup.
    async fn configure(
        &self,
        host: &BuiltinHost<'_>,
        setup: &ProviderSetup,
    ) -> Result<(Publisher, Self::Configured), Error>;

    /// The Reef page where a person finishes the Setup.
    fn needs_sign_in(
        &self,
        host: &BuiltinHost<'_>,
        setup: &ProviderSetupId,
        configured: Self::Configured,
    ) -> serde_json::Value;

    /// Attempts the Publication through the workflow Reef publishes with.
    async fn publish(
        &self,
        host: &BuiltinHost<'_>,
        publisher: &dyn WorkspaceProviderPublisher,
        setup: &ProviderSetupId,
        configured: Self::Configured,
    ) -> Result<Self::Publication, coral_provider::Error>;

    /// The failure a Program catches for a Publication that published
    /// nothing and left the Setup reusable.
    fn publication_error(&self, error: coral_provider::Error) -> Error {
        publication_error(error)
    }

    /// The Operation Result for an attempted Publication, or the failure it
    /// ended in.
    async fn published(
        &self,
        host: &BuiltinHost<'_>,
        publication: Self::Publication,
    ) -> Result<serde_json::Value, Error>;
}

/// Prepares and configures `operation`'s Setup, then publishes it or returns
/// the Reef page where a person finishes it.
pub(super) async fn perform(
    operation: &impl SetupOperation,
    host: &BuiltinHost<'_>,
) -> Result<serde_json::Value, Error> {
    // Once a Setup exists, the call runs to an end that publishes it, links a
    // person to it, or ends it.
    let setup = tokio::select! {
        biased;
        () = host.run.stopped() => return Err(Error::ProgramHostTerminated),
        setup = operation.prepare(host) => setup?,
    };
    #[cfg(test)]
    host.run.setup_prepared();
    let (publisher, configured) = match operation.configure(host, &setup).await {
        Ok(configured) => configured,
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
            Ok(operation.needs_sign_in(host, setup.id(), configured))
        }
        Publisher::Coral => publish(operation, host, setup.id(), configured).await,
    }
}

/// Publishes the Setup as the invoking User and returns what the Program
/// needs to call the Provider.
async fn publish<O: SetupOperation>(
    operation: &O,
    host: &BuiltinHost<'_>,
    setup: &ProviderSetupId,
    configured: O::Configured,
) -> Result<serde_json::Value, Error> {
    let Some(publisher) = host.publisher else {
        end_setup(host, setup).await;
        return Err(Error::InvocationRejected(
            CoralFailure::OperationUnavailable.into(),
        ));
    };
    // A Run that ended could no longer return the Provider: publish nothing.
    // The Run can still end between this check and the Publication; the
    // Provider then changes and search finds it, though the call ended.
    require_current_run(host, setup).await?;
    match operation.publish(host, publisher, setup, configured).await {
        Ok(publication) => operation.published(host, publication).await,
        Err(error) => {
            // The Publication left the Setup reusable, but nobody will reuse it.
            end_setup(host, setup).await;
            Err(operation.publication_error(error))
        }
    }
}

/// Who publishes a Setup that `publishability` describes. A person's sign-in
/// could not start without an OAuth application, as Reef offers it, so a
/// Setup only a person can publish needs one.
pub(super) async fn publisher(
    host: &BuiltinHost<'_>,
    setup: &ProviderSetupId,
    publishability: SetupPublishability,
    configuration: Option<&ProviderConfiguration>,
) -> Result<Publisher, Error> {
    let invalid = |field: &str, reason: &str| {
        Error::InvocationRejected(Rejection::invalid_input(field, reason))
    };
    match publishability {
        SetupPublishability::Coral => Ok(Publisher::Coral),
        SetupPublishability::Person
            if !has_oauth_application(host, setup, configuration).await? =>
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

/// Where a Setup reads the Provider from, for a source the Program sent. An
/// MCP server is discovered with `request_headers`, the non-secret headers it
/// is configured with.
pub(super) fn setup_source(
    source: ProviderSourceArgument,
    request_headers: BTreeMap<String, String>,
) -> ProviderSetupSource {
    match source {
        ProviderSourceArgument::OpenApiDocument(document) => {
            ProviderSetupSource::OpenApiDocument(document)
        }
        ProviderSourceArgument::OpenApiUrl(url) => ProviderSetupSource::OpenApiUrl(url),
        ProviderSourceArgument::McpUrl(url) => ProviderSetupSource::McpUrl {
            url,
            request_headers,
        },
    }
}

/// The management request a Built-in Invocation's Publication carries: its
/// own identity, which makes a repeated Publication an exact replay.
pub(super) fn request_id(invocation: &InvocationId) -> Result<ManagementRequestId, Error> {
    Ok(ManagementRequestId::parse(&invocation.to_string())?)
}

/// Settles a call whose worker was lost after the dispatch fence, from the
/// Provider its Publication published: `result` describes it, as the call
/// would have. Without one, the Run ended first.
pub(super) async fn recover_published(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    provider: Option<&ProviderId>,
    result: fn(&PublishedProvider) -> serde_json::Value,
) -> Result<(InvocationResultKind, Option<String>), Error> {
    let Some(provider) = provider else {
        return Ok((InvocationResultKind::RunTerminated, None));
    };
    let published = match coral_provider::published_provider_in_transaction(
        transaction,
        &run.workspace_id,
        provider,
        &run.principal,
    )
    .await
    {
        Ok(published) => published,
        // An Owner deleted the Provider since; no result can describe it now.
        Err(coral_provider::Error::ProviderNotFound) => {
            return Ok((InvocationResultKind::RunTerminated, None));
        }
        Err(error) => return Err(error.into()),
    };
    let result = serde_json::to_string(&result(&published)).map_err(Error::ProgramValueJson)?;
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
    setup: &ProviderSetupId,
    configuration: Option<&ProviderConfiguration>,
) -> Result<bool, Error> {
    let authentication = configuration.and_then(|values| values.authentication.as_ref());
    match host.accounts {
        Some(accounts) => accounts
            .setup_oauth_application_available(
                host.context.actor(),
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

/// A Publication rejected because its Setup ended or changed, or its request
/// is stale, is a failure the Program can catch. Persistence and corrupt
/// evidence still stop the Run for recovery.
pub(super) fn publication_error(error: coral_provider::Error) -> Error {
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
pub(super) fn setup_error(error: coral_provider::Error) -> Error {
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
pub(super) fn configuration_rejection(
    confirmed: &coral_provider::SetupConfigurationReview,
) -> Option<Rejection> {
    let problems: Vec<_> = confirmed
        .errors
        .iter()
        .map(|error| {
            InputProblem::new(
                ConfigurationChoices::argument_for(&error.field),
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
