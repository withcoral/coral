//! The flow each Built-in Tool that prepares a Provider Setup for a
//! Program follows: `addProvider` for a new Provider, `updateProvider` for
//! a new Generation of one. Coral prepares and configures the Setup as the
//! invoking User, then returns the Reef page where a person previews and
//! publishes it. The call never waits for that person, and Coral ends a Setup
//! it prepared that nobody will finish.

use async_trait::async_trait;
use std::collections::BTreeMap;

use coral_catalog::{ConfigurationChoices, ProviderConfiguration, ProviderSourceArgument};
use coral_provider::account::AccountModule;
use coral_provider::{
    ProviderSetup, ProviderSetupId, ProviderSetupSource, PublishabilityRejection,
    SetupPublishability,
};
use coral_workspace::ChangeContext;

use super::{BuiltinHost, authority_error};
use crate::error::InputProblem;
use crate::{CoralFailure, Error, Rejection};

/// One Built-in Tool that prepares a Provider Setup. Each step is the
/// Tool's own; [`perform`] runs them in order.
#[async_trait]
pub(super) trait SetupTool: Sync {
    /// What configuring the Setup leaves for the page where a person
    /// finishes it.
    type Configured: Send;

    /// Prepares the Setup as the User `context` names. Only this step waits
    /// on the network, so only it stops with the Run.
    async fn prepare(
        &self,
        host: &BuiltinHost<'_>,
        context: &ChangeContext,
    ) -> Result<ProviderSetup, Error>;

    /// Confirms the Program's configuration, or fails with what to correct.
    async fn configure(
        &self,
        host: &BuiltinHost<'_>,
        context: &ChangeContext,
        setup: &ProviderSetup,
    ) -> Result<Self::Configured, Error>;

    /// The Reef page where a person finishes the Setup.
    fn needs_person(
        &self,
        host: &BuiltinHost<'_>,
        setup: &ProviderSetupId,
        configured: Self::Configured,
    ) -> serde_json::Value;
}

/// Prepares and configures `tool`'s Setup as the User `context` names, then
/// returns the Reef page where a person finishes it.
pub(super) async fn perform(
    tool: &impl SetupTool,
    host: &BuiltinHost<'_>,
    context: &ChangeContext,
) -> Result<serde_json::Value, Error> {
    // Once a Setup exists, the call runs to an end that links a person to it
    // or ends it.
    let setup = tokio::select! {
        biased;
        () = host.run.stopped() => return Err(Error::ProgramHostTerminated),
        setup = tool.prepare(host, context) => setup?,
    };
    #[cfg(test)]
    host.run.setup_prepared();
    let configured = match tool.configure(host, context, &setup).await {
        Ok(configured) => configured,
        Err(error) => {
            // Nobody will finish a Setup whose configuration failed.
            end_setup(host, setup.id()).await;
            return Err(error);
        }
    };
    // A Run that ended could no longer hand anyone the link.
    require_current_run(host, setup.id()).await?;
    Ok(tool.needs_person(host, setup.id(), configured))
}

/// Fails unless a person can finish a Setup that `publishability` describes.
/// A person's sign-in could not start without an OAuth application, as Reef
/// offers it, so the Setup needs one.
pub(super) async fn require_finishable(
    host: &BuiltinHost<'_>,
    context: &ChangeContext,
    setup: &ProviderSetupId,
    publishability: SetupPublishability,
    configuration: Option<&ProviderConfiguration>,
) -> Result<(), Error> {
    let invalid = |field: &str, reason: &str| {
        Error::InvocationRejected(Rejection::invalid_input(field, reason))
    };
    match publishability {
        SetupPublishability::Coral | SetupPublishability::Person => {
            if has_oauth_application(host, context, setup, configuration).await? {
                Ok(())
            } else {
                Err(invalid(
                    "authentication.client",
                    "Coral cannot register an OAuth application with this service; register \
                     one with the service and give its `client_id`",
                ))
            }
        }
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
