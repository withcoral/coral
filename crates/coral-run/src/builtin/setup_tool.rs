//! What each Built-in Tool that prepares a Provider Setup for a Program
//! shares: `addProvider` for a new Provider, `updateProvider` for a new
//! Generation of one. coral-provider prepares and configures the Setup as the
//! invoking User, and the Tool returns the Reef page where a person previews
//! and publishes it. The call never waits for that person, and Coral ends a
//! Setup it prepared that nobody will finish.

use std::collections::BTreeMap;

use coral_catalog::{ConfigurationChoices, ProviderSourceArgument};
use coral_provider::{
    ProgramSetup, ProgramSetupError, ProgramSetupRequest, ProviderSetupId, ProviderSetupSource,
};
use coral_workspace::ChangeContext;

use super::{BuiltinHost, authority_error};
use crate::error::InputProblem;
use crate::{CoralFailure, Error, Rejection};

/// Prepares the Setup `request` asks for as the User `context` names, for a
/// person to finish. `provider_error` maps what is not the call's own failure.
pub(super) async fn prepare(
    host: &BuiltinHost<'_>,
    context: &ChangeContext,
    request: ProgramSetupRequest,
    provider_error: fn(coral_provider::Error) -> Error,
) -> Result<ProgramSetup, Error> {
    let prepared = host
        .providers
        .prepare_program_setup(
            context,
            host.workspace_id,
            host.accounts,
            request,
            host.run.stopped(),
        )
        .await
        .map_err(|error| program_setup_error(error, provider_error))?;
    #[cfg(test)]
    host.run.setup_prepared();
    // A Run that ended could no longer hand anyone the link.
    require_current_run(host, &prepared.setup).await?;
    Ok(prepared)
}

/// The failure a Program catches for a Setup Coral did not prepare.
fn program_setup_error(
    error: ProgramSetupError,
    provider_error: fn(coral_provider::Error) -> Error,
) -> Error {
    let invalid = |field: &str, reason: &str| {
        Error::InvocationRejected(Rejection::invalid_input(field, reason))
    };
    match error {
        ProgramSetupError::Stopped => Error::ProgramHostTerminated,
        ProgramSetupError::NamespaceInUse => {
            Error::InvocationRejected(CoralFailure::ProviderNamespaceConflict.into())
        }
        ProgramSetupError::Configuration { errors, diagnostic } => {
            Error::InvocationRejected(configuration_rejection(&errors, diagnostic.as_deref()))
        }
        ProgramSetupError::OAuthApplicationMissing => invalid(
            "authentication.client",
            "Coral cannot register an OAuth application with this service; register \
             one with the service and give its `client_id`",
        ),
        // The route must suit the source, as Reef offers it; no route falls back to another.
        ProgramSetupError::SourceNeedsCredential => invalid(
            "credentialRoute",
            "the source needs a credential; use `fixed` or `caller`",
        ),
        ProgramSetupError::SourceNeedsNoCredential => invalid(
            "credentialRoute",
            "the source declares no credential; use `none`, or give `authentication` \
             to say how a person signs in",
        ),
        ProgramSetupError::Provider(error) => provider_error(error),
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
fn configuration_rejection(
    errors: &[coral_catalog::ConfigurationFieldError],
    diagnostic: Option<&str>,
) -> Rejection {
    let problems: Vec<_> = errors
        .iter()
        .map(|error| {
            InputProblem::new(
                ConfigurationChoices::argument_for(&error.field),
                error.message.clone(),
            )
        })
        .chain(diagnostic.iter().map(|diagnostic| {
            InputProblem::new(
                "source",
                format!("Coral cannot build a Provider from the source ({diagnostic})"),
            )
        }))
        .collect();
    if problems.is_empty() {
        Rejection::invalid_input(
            "source",
            "Coral could not read a configuration from the source",
        )
    } else {
        Rejection::invalid_inputs(problems)
    }
}
