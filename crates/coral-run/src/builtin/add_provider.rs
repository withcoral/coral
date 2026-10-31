//! Performs `coral.workspace.addProvider`.

use async_trait::async_trait;
use coral_catalog::{AddProviderArguments, AddProviderResult, ConfigurationChoices};
use coral_provider::access::{CredentialRoute, ProviderNamespace};
use coral_provider::{ProviderSetup, ProviderSetupId, ProviderSetupSource, SetupRequest};
use coral_workspace::ChangeContext;

use super::setup_tool::{self, SetupTool, configuration_rejection, setup_error};
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
        // A taken namespace would fail the person's Publication; fail before
        // any fetch or Setup.
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
        setup_tool::perform(self, host, context).await
    }
}

#[async_trait]
impl SetupTool for AddProvider {
    type Configured = ();

    async fn prepare(
        &self,
        host: &BuiltinHost<'_>,
        context: &ChangeContext,
    ) -> Result<ProviderSetup, Error> {
        host.providers
            .prepare_requested_setup(
                context,
                host.workspace_id,
                self.source.clone(),
                SetupRequest {
                    namespace: self.namespace.clone(),
                    route: self.route,
                },
            )
            .await
            .map_err(setup_error)
    }

    /// Confirms the Program's configuration over the source's suggestions. The
    /// route must suit the source.
    async fn configure(
        &self,
        host: &BuiltinHost<'_>,
        context: &ChangeContext,
        setup: &ProviderSetup,
    ) -> Result<(), Error> {
        let review = host
            .providers
            .review_setup_configuration(context.actor(), host.workspace_id, setup.id())
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
                setup.id(),
                review.revision,
                self.choices.over(suggested),
            )
            .await
            .map_err(setup_error)?;
        if let Some(rejection) = configuration_rejection(&confirmed) {
            return Err(Error::InvocationRejected(rejection));
        }
        let publishability = host
            .providers
            .setup_publishability(context.actor(), host.workspace_id, setup.id(), self.route)
            .await
            .map_err(setup_error)?;
        setup_tool::require_finishable(
            host,
            context,
            setup.id(),
            publishability,
            confirmed.confirmed.as_ref(),
        )
        .await
    }

    fn needs_person(
        &self,
        host: &BuiltinHost<'_>,
        setup: &ProviderSetupId,
        (): (),
    ) -> serde_json::Value {
        AddProviderResult::NeedsPerson {
            url: setup_url(host, setup),
            namespace: self.namespace.as_str().to_owned(),
        }
        .into_tool_result()
    }
}
