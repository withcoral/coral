//! Performs `coral.workspace.addProvider`.

use coral_catalog::{AddProviderArguments, AddProviderResult, ConfigurationChoices};
use coral_provider::access::{CredentialRoute, ProviderNamespace};
use coral_provider::{ProgramSetupPurpose, ProgramSetupRequest, ProviderSetupSource};

use super::setup_tool::{self, setup_error};
use super::{BuiltinHost, setup_url};
use crate::{CoralFailure, Error, Rejection};

/// `addProvider`'s arguments in Coral's domain types.
#[derive(Clone)]
pub(crate) struct AddProvider {
    namespace: ProviderNamespace,
    source: ProviderSetupSource,
    route: CredentialRoute,
    choices: ConfigurationChoices,
}

impl AddProvider {
    /// The call for arguments the contract accepted. An MCP server is
    /// discovered with the same non-secret headers it is configured with.
    pub(super) fn new(arguments: AddProviderArguments) -> Result<Self, Rejection> {
        // The contract checks the namespace with the same rule, so this does
        // not fail.
        let namespace = ProviderNamespace::parse(&arguments.namespace)
            .map_err(|_error| Rejection::from(CoralFailure::InvalidInput))?;
        let request_headers = arguments
            .choices
            .request_headers
            .clone()
            .unwrap_or_default();
        let source = setup_tool::setup_source(arguments.source, request_headers);
        Ok(Self {
            namespace,
            source,
            route: arguments.credential_route.into(),
            choices: arguments.choices,
        })
    }

    /// Prepares and configures a Setup as the invoking User. The result is the
    /// Reef page where a person previews and publishes it. The call never
    /// waits for that person.
    pub(super) async fn perform(&self, host: &BuiltinHost<'_>) -> Result<serde_json::Value, Error> {
        // Only a User reaches addProvider; an Agent changes no configuration.
        let Some(context) = host.context else {
            return Err(Error::InvocationRejected(
                CoralFailure::AuthorityLost.into(),
            ));
        };
        let request = ProgramSetupRequest {
            source: self.source.clone(),
            purpose: ProgramSetupPurpose::Add {
                namespace: self.namespace.clone(),
                route: self.route,
                choices: self.choices.clone(),
            },
        };
        let prepared = setup_tool::prepare(host, context, request, setup_error).await?;
        Ok(AddProviderResult::NeedsPerson {
            url: setup_url(host, &prepared.setup),
            namespace: self.namespace.as_str().to_owned(),
        }
        .into_tool_result())
    }
}
