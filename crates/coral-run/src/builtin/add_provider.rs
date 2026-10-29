//! Performs `coral.workspace.addProvider`, and settles one whose worker was
//! lost after the Policy Enforcement Point.

use async_trait::async_trait;
use coral_catalog::{AddProviderArguments, AddProviderResult, ConfigurationChoices};
use coral_db::Transaction;
use coral_policy::ExactToolRules;
use coral_provider::access::{CredentialRoute, ProviderNamespace};
use coral_provider::{
    Provider, ProviderPublication, ProviderSetup, ProviderSetupId, ProviderSetupSource,
    PublishedProvider, SetupRequest, WorkspaceProviderPublicationRequest,
    WorkspaceProviderPublisher,
};
use coral_workspace::ChangeContext;

use super::setup_tool::{
    self, Publisher, SetupTool, configuration_rejection, recover_published, request_id, setup_error,
};
use super::{BuiltinHost, provider_facts, setup_url};
use crate::lifecycle::RunRecord;
use crate::{CoralFailure, Error, InvocationId, InvocationResultKind, Rejection};

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

    /// Prepares and configures a Setup as the invoking User. Coral publishes it
    /// when nobody needs to sign in; otherwise the result is the Reef page where
    /// a person finishes it. The call never waits for that person.
    pub(super) async fn perform(&self, host: &BuiltinHost<'_>) -> Result<serde_json::Value, Error> {
        // Only a User reaches addProvider; an Agent changes no configuration.
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
        setup_tool::perform(self, host, context).await
    }
}

#[async_trait]
impl SetupTool for AddProvider {
    type Configured = ();
    type Publication = ProviderPublication;

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

    /// Confirms the Program's configuration over the source's suggestions and
    /// decides who publishes the Setup. The route must suit the source.
    async fn configure(
        &self,
        host: &BuiltinHost<'_>,
        context: &ChangeContext,
        setup: &ProviderSetup,
    ) -> Result<(Publisher, ()), Error> {
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
        let publisher = setup_tool::publisher(
            host,
            context,
            setup.id(),
            publishability,
            confirmed.confirmed.as_ref(),
        )
        .await?;
        Ok((publisher, ()))
    }

    fn needs_sign_in(
        &self,
        host: &BuiltinHost<'_>,
        setup: &ProviderSetupId,
        (): (),
    ) -> serde_json::Value {
        AddProviderResult::NeedsSignIn {
            url: setup_url(host, setup),
            namespace: self.namespace.as_str().to_owned(),
        }
        .into_tool_result()
    }

    async fn publish(
        &self,
        host: &BuiltinHost<'_>,
        context: &ChangeContext,
        publisher: &dyn WorkspaceProviderPublisher,
        setup: &ProviderSetupId,
        (): (),
    ) -> Result<ProviderPublication, coral_provider::Error> {
        publisher
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
    }

    async fn published(
        &self,
        host: &BuiltinHost<'_>,
        publication: ProviderPublication,
    ) -> Result<serde_json::Value, Error> {
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
        setup_tool::published_result(host, provider.id(), published_result).await
    }
}

/// Settles an `addProvider` whose worker was lost after the Policy Enforcement
/// Point, from the Publication Coral attempted under the Invocation's identity.
/// A published Provider becomes the Tool Result; otherwise the Run ended first.
/// A Setup that was never published expires at its deadline.
pub(super) async fn recover(
    transaction: &mut Transaction<'_>,
    run: &RunRecord,
    invocation: &InvocationId,
) -> Result<(InvocationResultKind, Option<String>), Error> {
    let publication = coral_provider::publication_for_request_in_transaction(
        transaction,
        &run.workspace_id,
        &request_id(invocation)?,
    )
    .await?;
    let provider = publication
        .as_ref()
        .and_then(ProviderPublication::published_provider)
        .map(Provider::id);
    recover_published(transaction, run, provider, published_result).await
}

/// The Tool Result for a Provider Coral published.
fn published_result(published: &PublishedProvider, rules: &ExactToolRules) -> serde_json::Value {
    AddProviderResult::Published {
        namespace: published.provider.call_root().name().to_owned(),
        facts: provider_facts(published, rules),
    }
    .into_tool_result()
}
