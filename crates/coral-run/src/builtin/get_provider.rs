//! Performs `coral.workspace.getProvider`.

use coral_catalog::{
    ConfigurationOutput, GetProviderArguments, GetProviderResult, ProviderSetupInputKind,
    ProviderSourceOutput,
};
use coral_provider::ProviderWithConfiguration;
use coral_provider::access::ProviderNamespace;

use super::{BuiltinHost, authority_error};
use crate::{CoralFailure, Error, Rejection};

/// `getProvider`'s arguments in Coral's domain types.
#[derive(Clone)]
pub(crate) struct GetProvider {
    namespace: ProviderNamespace,
}

impl GetProvider {
    /// The call for arguments the contract accepted.
    pub(super) fn new(arguments: &GetProviderArguments) -> Result<Self, Rejection> {
        // The contract checks the namespace with the same rule, so this does
        // not fail.
        let namespace = ProviderNamespace::parse(arguments.namespace())
            .map_err(|_error| Rejection::from(CoralFailure::InvalidInput))?;
        Ok(Self { namespace })
    }

    /// Reads the Provider Configuration as the invoking User.
    pub(super) async fn perform(&self, host: &BuiltinHost<'_>) -> Result<serde_json::Value, Error> {
        // Only an Owner reaches getProvider; an Agent reads no configuration.
        let Some(context) = host.context else {
            return Err(Error::InvocationRejected(
                CoralFailure::AuthorityLost.into(),
            ));
        };
        let inspected = host
            .providers
            .inspect_provider_configuration(context.actor(), host.workspace_id, &self.namespace)
            .await
            .map_err(|error| match error {
                coral_provider::Error::ProviderNotFound => {
                    Error::InvocationRejected(CoralFailure::ProviderNotFound.into())
                }
                other => authority_error(other),
            })?;
        Ok(self.result(inspected)?.into_tool_result())
    }

    fn result(&self, inspected: ProviderWithConfiguration) -> Result<GetProviderResult, Error> {
        let source = match (inspected.source.kind, inspected.source.url) {
            (ProviderSetupInputKind::OpenApi, None) => ProviderSourceOutput::OpenapiDocument,
            (ProviderSetupInputKind::OpenApi, Some(url)) => {
                ProviderSourceOutput::OpenapiUrl { url }
            }
            (ProviderSetupInputKind::Mcp, Some(url)) => ProviderSourceOutput::McpUrl { url },
            (ProviderSetupInputKind::Mcp, None) => {
                return Err(coral_provider::Error::CorruptData(
                    "an MCP Provider has no source URL".into(),
                )
                .into());
            }
        };
        let provider = inspected.provider;
        Ok(GetProviderResult {
            namespace: self.namespace.as_str().to_owned(),
            state: provider.state().into(),
            source,
            credential_route: provider.access().credential_route().into(),
            configuration: ConfigurationOutput::new(
                provider.display_name().to_owned(),
                inspected.servers,
                inspected.authentication,
                inspected.request_headers,
                inspected.credential_fields,
            ),
        })
    }
}
