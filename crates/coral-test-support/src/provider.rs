//! Shared fixture choreography through the same Setup interface Reef calls.

/// Unique request key for domain test fixtures.
#[must_use]
pub fn fixture_request_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Adds local fixture helpers for the supplied Provider module path.
/// Fixture authors explicitly choose anonymous authentication when their document
/// has no supported default. Production never makes that decision for an owner.
/// The path is `crate` in Provider unit tests and `coral_provider` in consumers.
#[macro_export]
macro_rules! provider_setup_fixtures {
    ($domain:ident) => {
        mod configured_provider_fixtures {
            use coral_catalog::{
                OpenApiDocument, ProviderConfiguration, ProviderCredentialContract,
            };
            use coral_workspace::{ChangeContext, WorkspaceId};
            use $domain as providers;

            pub(super) trait ConfiguredProviderFixtures {
                async fn prepare_update_fixture(
                    &self,
                    context: &ChangeContext,
                    workspace: &WorkspaceId,
                    provider: &providers::ProviderId,
                    document: OpenApiDocument,
                ) -> Result<providers::ProviderSetup, providers::Error>;
                async fn change_account_setting_fixture(
                    &self,
                    context: &ChangeContext,
                    workspace: &WorkspaceId,
                    provider: &providers::ProviderId,
                    route: providers::CredentialRoute,
                ) -> Result<providers::ProviderAccess, providers::Error>;
                async fn publish_shared_fixture(
                    &self,
                    context: &ChangeContext,
                    workspace: &WorkspaceId,
                    document: OpenApiDocument,
                    namespace: &str,
                    accounts: &providers::account::AccountModule,
                    values: $crate::FixtureCredentialValues,
                ) -> Result<providers::Provider, providers::Error>;
                async fn prepare_mcp_fixture(
                    &self,
                    context: &ChangeContext,
                    workspace: &WorkspaceId,
                    snapshot: coral_catalog::McpDiscoverySnapshot,
                ) -> Result<providers::ProviderSetup, providers::Error>;
                async fn prepare_openapi_fixture(
                    &self,
                    context: &ChangeContext,
                    workspace: &WorkspaceId,
                    document: OpenApiDocument,
                ) -> Result<providers::ProviderSetup, providers::Error>;
                async fn publish_openapi_fixture(
                    &self,
                    context: &ChangeContext,
                    workspace: &WorkspaceId,
                    document: OpenApiDocument,
                    namespace: &str,
                    route: providers::CredentialRoute,
                ) -> Result<providers::Provider, providers::Error>;
                /// Attempts the Publication of a configured Setup, as the Owner does in Reef.
                async fn publish_setup_fixture(
                    &self,
                    context: &ChangeContext,
                    workspace: &WorkspaceId,
                    setup: &providers::ProviderSetupId,
                    namespace: &str,
                    route: providers::CredentialRoute,
                ) -> Result<providers::ProviderPublication, providers::Error>;
            }
            impl ConfiguredProviderFixtures for providers::ProviderModule {
                async fn prepare_update_fixture(
                    &self,
                    context: &ChangeContext,
                    workspace: &WorkspaceId,
                    provider: &providers::ProviderId,
                    document: OpenApiDocument,
                ) -> Result<providers::ProviderSetup, providers::Error> {
                    let values = document
                        .configuration_review()
                        .expect("fixture configuration")
                        .suggested;
                    let setup = self
                        .prepare_setup(
                            context,
                            workspace,
                            providers::ProviderSetupSource::OpenApiDocument(document),
                            Some(provider),
                        )
                        .await?;
                    let review = self
                        .review_setup_configuration(context.actor(), workspace, setup.id())
                        .await?;
                    self.confirm_setup_configuration(
                        context,
                        workspace,
                        setup.id(),
                        review.revision,
                        values,
                    )
                    .await?;
                    self.get_setup(context.actor(), workspace, setup.id()).await
                }
                async fn change_account_setting_fixture(
                    &self,
                    context: &ChangeContext,
                    workspace: &WorkspaceId,
                    provider: &providers::ProviderId,
                    route: providers::CredentialRoute,
                ) -> Result<providers::ProviderAccess, providers::Error> {
                    let setup = self
                        .prepare_setup(
                            context,
                            workspace,
                            providers::ProviderSetupSource::Current,
                            Some(provider),
                        )
                        .await?;
                    let review = self
                        .review_setup_configuration(context.actor(), workspace, setup.id())
                        .await?;
                    let review = self
                        .confirm_setup_configuration(
                            context,
                            workspace,
                            setup.id(),
                            review.revision,
                            review.confirmed.expect("current configuration"),
                        )
                        .await?;
                    let publication =
                        self
                            .regenerate_provider(
                                context,
                                providers::ProviderRegenerationRequest::new(
                                    providers::ManagementRequestId::parse(
                                        &$crate::fixture_request_id(),
                                    )
                                    .expect("request"),
                                    workspace.clone(),
                                    provider.clone(),
                                    setup.id().clone(),
                                    review.revision,
                                )
                                .with_account_setting(route)
                                .with_incompatible_configuration_confirmation(),
                            )
                            .await?;
                    if publication.outcome() == providers::ProviderPublicationOutcome::Failed {
                        return Err(providers::Error::ProviderChangedDuringPublication);
                    }
                    Ok(self
                        .inspect_provider(context.actor(), workspace, provider)
                        .await?
                        .access()
                        .clone())
                }
                async fn publish_shared_fixture(
                    &self,
                    context: &ChangeContext,
                    workspace: &WorkspaceId,
                    document: OpenApiDocument,
                    namespace: &str,
                    accounts: &providers::account::AccountModule,
                    values: $crate::FixtureCredentialValues,
                ) -> Result<providers::Provider, providers::Error> {
                    let setup = self
                        .prepare_openapi_fixture(context, workspace, document)
                        .await?;
                    let review = self
                        .review_setup_configuration(context.actor(), workspace, setup.id())
                        .await?;
                    let grant = accounts
                        .authorize_setup(context, workspace, setup.id(), review.revision, values)
                        .await?;
                    let publication =
                        self
                            .publish_workspace_provider(
                                context,
                                providers::WorkspaceProviderPublicationRequest::new(
                                    providers::ManagementRequestId::parse(
                                        &$crate::fixture_request_id(),
                                    )
                                    .expect("request"),
                                    workspace.clone(),
                                    setup.id().clone(),
                                    providers::ProviderNamespace::parse(namespace)
                                        .expect("namespace"),
                                    providers::CredentialRoute::Fixed,
                                )
                                .with_account_grant(
                                    grant.provisional_grant_id.expect("provisional grant"),
                                ),
                            )
                            .await?;
                    Ok(publication
                        .published_provider()
                        .expect("published shared Provider")
                        .clone())
                }
                async fn prepare_mcp_fixture(
                    &self,
                    context: &ChangeContext,
                    workspace: &WorkspaceId,
                    snapshot: coral_catalog::McpDiscoverySnapshot,
                ) -> Result<providers::ProviderSetup, providers::Error> {
                    let setup = self.create_mcp_setup(context, workspace, snapshot).await?;
                    let review = self
                        .review_setup_configuration(context.actor(), setup.owner(), setup.id())
                        .await?;
                    let values = review.document.map(|review| review.suggested).unwrap_or(
                        ProviderConfiguration {
                            request_headers: std::collections::BTreeMap::new(),
                            authentication: Some(coral_catalog::AuthenticationConfiguration::None),
                            credential_fields: Default::default(),
                            servers: Default::default(),
                            display_name: "MCP fixture".into(),
                        },
                    );
                    let confirmed = self
                        .confirm_setup_configuration(
                            context,
                            setup.owner(),
                            setup.id(),
                            review.revision,
                            values,
                        )
                        .await?;
                    assert!(
                        confirmed.errors.is_empty(),
                        "fixture configuration: {:?}",
                        confirmed.errors
                    );
                    self.get_setup(context.actor(), workspace, setup.id()).await
                }
                async fn prepare_openapi_fixture(
                    &self,
                    context: &ChangeContext,
                    workspace: &WorkspaceId,
                    document: OpenApiDocument,
                ) -> Result<providers::ProviderSetup, providers::Error> {
                    let configuration = fixture_configuration(&document);
                    let setup = self
                        .create_openapi_setup(context, workspace, document)
                        .await?;
                    let confirmed = self
                        .confirm_setup_configuration(
                            context,
                            setup.owner(),
                            setup.id(),
                            0,
                            configuration,
                        )
                        .await?;
                    assert!(
                        confirmed.errors.is_empty(),
                        "fixture configuration: {:?}",
                        confirmed.errors
                    );
                    self.get_setup(context.actor(), workspace, setup.id()).await
                }
                async fn publish_openapi_fixture(
                    &self,
                    context: &ChangeContext,
                    workspace: &WorkspaceId,
                    document: OpenApiDocument,
                    namespace: &str,
                    route: providers::CredentialRoute,
                ) -> Result<providers::Provider, providers::Error> {
                    let setup = self
                        .prepare_openapi_fixture(context, workspace, document)
                        .await?;
                    let publication = self
                        .publish_setup_fixture(context, workspace, setup.id(), namespace, route)
                        .await?;
                    Ok(publication
                        .published_provider()
                        .unwrap_or_else(|| {
                            panic!(
                                "fixture publication failed: {:?} {:?}",
                                publication.outcome(),
                                publication.diagnostic()
                            )
                        })
                        .clone())
                }
                async fn publish_setup_fixture(
                    &self,
                    context: &ChangeContext,
                    workspace: &WorkspaceId,
                    setup: &providers::ProviderSetupId,
                    namespace: &str,
                    route: providers::CredentialRoute,
                ) -> Result<providers::ProviderPublication, providers::Error> {
                    self.publish_workspace_provider(
                        context,
                        providers::WorkspaceProviderPublicationRequest::new(
                            providers::ManagementRequestId::parse(&$crate::fixture_request_id())
                                .expect("request"),
                            workspace.clone(),
                            setup.clone(),
                            providers::ProviderNamespace::parse(namespace).expect("namespace"),
                            route,
                        ),
                    )
                    .await
                }
            }
            fn fixture_configuration(document: &OpenApiDocument) -> ProviderConfiguration {
                let mut configuration = document
                    .configuration_review()
                    .map(|review| review.suggested)
                    .unwrap_or(ProviderConfiguration {
                        request_headers: std::collections::BTreeMap::new(),
                        authentication: None,
                        credential_fields: Default::default(),
                        servers: Default::default(),
                        display_name: "Fixture".into(),
                    });
                configuration
                    .authentication
                    .get_or_insert(coral_catalog::AuthenticationConfiguration::None);
                configuration
            }
        }
        use self::configured_provider_fixtures::ConfiguredProviderFixtures as _;
    };
}
