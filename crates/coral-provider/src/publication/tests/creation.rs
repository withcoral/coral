//! Discovery grant retention through Workspace publication interrupted around its commit.
use super::*;
use coral_outbound::DestinationPolicy;

use crate::Error;

fn retention_request(
    setup: &crate::ProviderSetup,
    route: CredentialRoute,
) -> WorkspaceProviderPublicationRequest {
    WorkspaceProviderPublicationRequest::new(
        ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
        setup.owner().clone(),
        setup.id().clone(),
        crate::ProviderNamespace::parse("retention").unwrap(),
        route,
    )
}

async fn terminal_event_count(
    providers: &ProviderModule,
    actor: &AuthenticatedUser,
    workspace: &WorkspaceId,
) -> usize {
    providers
        .list_lifecycle_events(actor, workspace)
        .await
        .unwrap()
        .iter()
        .filter(|event| event.action() == LifecycleAction::PublicationCompleted)
        .count()
}

fn with_route(
    request: &WorkspaceProviderPublicationRequest,
    route: CredentialRoute,
) -> WorkspaceProviderPublicationRequest {
    WorkspaceProviderPublicationRequest::new(
        request.request_id().clone(),
        request.workspace_id().clone(),
        request.setup_id().clone(),
        request.namespace().clone(),
        route,
    )
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one publication journey proves explicit shared/self promotion and caller discard across response loss"
)]
async fn workspace_account_promotion_is_atomic_and_replays_after_restart() {
    use coral_credentials::Secret;

    for retained_route in [
        None,
        Some(CredentialRoute::Fixed),
        Some(CredentialRoute::Caller),
    ] {
        let retain = retained_route.is_some();
        for before_commit in [false, true] {
            let (fixture, database, workspaces) = test_foundation().await;
            let actor = organization_user(
                fixture.database(),
                "user:retention-replay",
                OrganizationRole::Owner,
            )
            .await;
            let context = change_context(actor.clone(), 20, '1');
            let owner = workspaces
                .create(&context, WorkspaceDisplayName::parse("Retention").unwrap())
                .await
                .unwrap()
                .id()
                .clone();
            let hold = crate::tests::HoldForever::at(if before_commit {
                PublicationPoint::TerminalCommit
            } else {
                PublicationPoint::Committed
            });
            let providers = ProviderModule::with_test_dependencies(
                database.clone(),
                workspaces.clone(),
                Arc::new(CatalogMaterializer(
                    DestinationPolicy::default()
                        .with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
                )),
                hold.clone(),
            );
            let directory = tempfile::tempdir().unwrap();
            let accounts = crate::tests::accounts_for(&providers, directory.path());
            let setup = providers
                .prepare_openapi_fixture(
                    &context,
                    &owner,
                    OpenApiDocument::parse(CREDENTIAL_FREE_OPENAPI).unwrap(),
                )
                .await
                .unwrap();
            let review = providers
                .review_setup_configuration(&actor, &owner, setup.id())
                .await
                .unwrap();
            let mut values = review.confirmed.unwrap();
            values.authentication = Some(AuthenticationConfiguration::Bearer);
            let confirmed = providers
                .confirm_setup_configuration(&context, &owner, setup.id(), review.revision, values)
                .await
                .unwrap();
            assert!(confirmed.errors.is_empty());
            let authorized = accounts
                .authorize_setup(
                    &context,
                    &owner,
                    setup.id(),
                    confirmed.revision,
                    coral_credentials::CredentialValues::token(
                        Secret::new("discovery-retention-canary".into()).unwrap(),
                    ),
                )
                .await
                .unwrap();
            let provisional_grant_id = authorized.provisional_grant_id.unwrap();
            let original = retention_request(&setup, CredentialRoute::Caller);
            let request = retained_route.map_or_else(
                || original.clone(),
                |route| with_route(&original, route).with_account_grant(provisional_grant_id),
            );
            // The request stops before or after its terminal commit.
            let interrupted = tokio::spawn({
                let (providers, context, request) =
                    (providers.clone(), context.clone(), request.clone());
                async move {
                    providers
                        .publish_workspace_provider(&context, request)
                        .await
                }
            });
            hold.held().await;
            interrupted.abort();
            assert!(interrupted.await.unwrap_err().is_cancelled());
            assert_eq!(
                providers
                    .setup_authentication(&actor, &owner, setup.id(), 21)
                    .await
                    .unwrap()
                    .authorized,
                before_commit,
                "the grant is scrubbed only when publication commits",
            );
            let committed = providers.inspect_providers(&actor, &owner).await.unwrap();
            assert_eq!(committed.len(), usize::from(!before_commit));
            if let Some(provider) = committed.first() {
                let account = accounts
                    .get_account(
                        &context,
                        &owner,
                        provider.id(),
                        &if retained_route == Some(CredentialRoute::Caller) {
                            crate::account::AccountScope::User
                        } else {
                            crate::account::AccountScope::Shared
                        },
                    )
                    .await
                    .unwrap()
                    .account;
                assert_eq!(
                    account.map(|account| account.id),
                    retain.then_some(provisional_grant_id)
                );
            }
            assert_eq!(
                terminal_event_count(&providers, &actor, &owner).await,
                usize::from(!before_commit)
            );

            if before_commit {
                assert!(
                    providers
                        .inspect_providers(&actor, &owner)
                        .await
                        .unwrap()
                        .is_empty()
                );
                assert!(
                    accounts
                        .list_user_lifecycle_events(&context, &owner)
                        .await
                        .unwrap()
                        .is_empty()
                );
            }
            let restarted = ProviderModule::with_test_dependencies(
                database,
                workspaces,
                Arc::new(CatalogMaterializer(
                    DestinationPolicy::default()
                        .with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
                )),
                Arc::new(NoopPublicationObserver),
            );
            let publication = restarted
                .publish_workspace_provider(&context, request.clone())
                .await
                .unwrap();
            assert_eq!(publication.outcome(), ProviderPublicationOutcome::Succeeded);
            assert_eq!(publication.request(), &request);
            assert_eq!(
                restarted
                    .get_publication(&actor, &owner, request.request_id())
                    .await
                    .unwrap(),
                publication,
            );
            assert_eq!(
                restarted
                    .publish_workspace_provider(&context, request)
                    .await
                    .unwrap(),
                publication,
            );
            let changed = if retain {
                original
            } else {
                with_route(&original, CredentialRoute::Fixed)
                    .with_account_grant(provisional_grant_id)
            };
            assert!(matches!(
                restarted
                    .publish_workspace_provider(&context, changed)
                    .await,
                Err(Error::RequestIdConflict)
            ));
            let different_provisional_grant_id =
                crate::account::ProviderAccountId::parse("f0000000-0000-4000-8000-000000000099")
                    .unwrap();
            assert!(matches!(
                restarted
                    .publish_workspace_provider(
                        &context,
                        with_route(publication.request(), CredentialRoute::Fixed)
                            .with_account_grant(different_provisional_grant_id),
                    )
                    .await,
                Err(Error::RequestIdConflict)
            ));
            let accounts = crate::tests::accounts_for(&restarted, directory.path());
            assert!(
                !restarted
                    .setup_authentication(&actor, &owner, setup.id(), 22)
                    .await
                    .unwrap()
                    .authorized
            );
            let provider = publication.published_provider().unwrap();
            let shared = accounts
                .get_account(
                    &context,
                    &owner,
                    provider.id(),
                    &crate::account::AccountScope::Shared,
                )
                .await
                .unwrap()
                .account;
            let caller = accounts
                .get_account(
                    &context,
                    &owner,
                    provider.id(),
                    &crate::account::AccountScope::User,
                )
                .await
                .unwrap()
                .account;
            assert_eq!(
                caller.as_ref().map(|account| account.id),
                (retained_route == Some(CredentialRoute::Caller)).then_some(provisional_grant_id),
                "Only explicit self retention connects the creator's Account"
            );
            assert_eq!(
                shared.as_ref().map(|account| account.id),
                (retained_route == Some(CredentialRoute::Fixed)).then_some(provisional_grant_id)
            );
            if let Some(account) = shared.or(caller) {
                assert_eq!(account.state, crate::account::AccountState::Ready);
                assert_eq!(account.connected_at_unix_nanos, Some(20));
                assert_eq!(
                    account.configured_by_user_id.as_ref(),
                    Some(actor.user_id())
                );
            }
            let events = restarted
                .list_lifecycle_events(&actor, &owner)
                .await
                .unwrap();
            assert_eq!(
                events
                    .iter()
                    .filter(|event| event.action() == LifecycleAction::AccountConnected)
                    .count(),
                usize::from(retained_route == Some(CredentialRoute::Fixed))
            );
            let private_events = accounts
                .list_user_lifecycle_events(&context, &owner)
                .await
                .unwrap();
            assert_eq!(
                private_events
                    .iter()
                    .filter(|event| event.action() == LifecycleAction::AccountConnected)
                    .count(),
                usize::from(retained_route == Some(CredentialRoute::Caller))
            );
            assert!(!format!("{publication:?}{events:?}").contains("discovery-retention-canary"));
            assert_eq!(terminal_event_count(&restarted, &actor, &owner).await, 1);
        }
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one controlled publication race verifies rollback, evidence, and successful retry"
)]
async fn creation_configuration_change_during_materialization_leaves_setup_reusable() {
    let (fixture, database, workspaces) = test_foundation().await;
    let actor = organization_user(
        fixture.database(),
        "user:creation-config-race",
        OrganizationRole::Member,
    )
    .await;
    let context = change_context(actor.clone(), 10, 'a');
    let workspace = workspaces
        .create(
            &context,
            WorkspaceDisplayName::parse("Configuration race").unwrap(),
        )
        .await
        .unwrap();
    let materializer = TestMaterializer::gated();
    let providers = ProviderModule::with_test_dependencies(
        database,
        workspaces,
        materializer.clone(),
        Arc::new(NoopPublicationObserver),
    );
    let setup = providers
        .prepare_openapi_fixture(
            &context,
            workspace.id(),
            OpenApiDocument::parse(CREDENTIAL_FREE_OPENAPI).unwrap(),
        )
        .await
        .unwrap();
    let request = retention_request(&setup, CredentialRoute::None);
    let publishing = {
        let providers = providers.clone();
        let context = context.clone();
        let request = request.clone();
        tokio::spawn(async move {
            providers
                .publish_workspace_provider(&context, request)
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(5), materializer.held())
        .await
        .unwrap();
    let review = providers
        .review_setup_configuration(&actor, workspace.id(), setup.id())
        .await
        .unwrap();
    let mut configuration = review.confirmed.unwrap();
    configuration.servers = BTreeMap::from([(
        "#/servers".into(),
        coral_catalog::ServerSelection::Custom {
            url: "https://weather.example/changed".into(),
        },
    )]);
    let change = tokio::time::timeout(
        Duration::from_secs(5),
        providers.confirm_setup_configuration(
            &context,
            workspace.id(),
            setup.id(),
            review.revision,
            configuration,
        ),
    )
    .await;
    materializer.release();
    assert!(
        change
            .expect("configuration edit holds no materialization lock")
            .unwrap()
            .errors
            .is_empty()
    );
    assert!(matches!(
        publishing.await.unwrap(),
        Err(Error::EtagMismatch)
    ));
    assert!(matches!(
        providers
            .get_publication(&actor, workspace.id(), request.request_id())
            .await,
        Err(Error::ProviderPublicationNotFound)
    ));
    assert_eq!(
        providers
            .get_setup(&actor, workspace.id(), setup.id())
            .await
            .unwrap()
            .state(),
        ProviderSetupState::Ready
    );
    assert!(
        providers
            .inspect_providers(&actor, workspace.id())
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        terminal_event_count(&providers, &actor, workspace.id()).await,
        0
    );
    assert_eq!(
        providers
            .publish_workspace_provider(&context, request)
            .await
            .unwrap()
            .outcome(),
        ProviderPublicationOutcome::Succeeded
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one controlled publication race verifies rollback, evidence, and successful retry"
)]
async fn creation_rechecks_owner_demotion_after_materialization() {
    let (fixture, database, workspaces) = test_foundation().await;
    let actor = organization_user(
        fixture.database(),
        "user:creation-demoted",
        OrganizationRole::Owner,
    )
    .await;
    let administrator = organization_user(
        fixture.database(),
        "user:creation-admin",
        OrganizationRole::Owner,
    )
    .await;
    let context = change_context(actor.clone(), 10, 'a');
    let workspace = workspaces
        .create(
            &change_context(administrator.clone(), 9, 'b'),
            WorkspaceDisplayName::parse("Demotion race").unwrap(),
        )
        .await
        .unwrap();
    // Organization Owner authority gives the initiator nothing here: they
    // publish as a designated Owner and lose that role mid-publication.
    crate::tests::join_as_owner(&workspaces, &actor, workspace.id()).await;
    let materializer = TestMaterializer::gated();
    let providers = ProviderModule::with_test_dependencies(
        database.clone(),
        workspaces.clone(),
        materializer.clone(),
        Arc::new(NoopPublicationObserver),
    );
    let setup = providers
        .prepare_openapi_fixture(
            &context,
            workspace.id(),
            OpenApiDocument::parse(CREDENTIAL_FREE_OPENAPI).unwrap(),
        )
        .await
        .unwrap();
    let request = retention_request(&setup, CredentialRoute::None);
    let publishing = {
        let providers = providers.clone();
        let context = context.clone();
        let request = request.clone();
        tokio::spawn(async move {
            providers
                .publish_workspace_provider(&context, request)
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(5), materializer.held())
        .await
        .unwrap();
    let admin_context = change_context(administrator.clone(), 11, 'b');
    let demotion = tokio::time::timeout(
        Duration::from_secs(5),
        workspaces.set_membership(
            &admin_context,
            workspace.id(),
            &actor,
            coral_workspace::WorkspaceRole::Member,
        ),
    )
    .await;
    materializer.release();
    demotion
        .expect("demotion holds no materialization lock")
        .unwrap();
    assert!(matches!(
        publishing.await.unwrap(),
        Err(Error::Workspace(coral_workspace::Error::PermissionDenied(
            _
        )))
    ));
    assert!(matches!(
        providers
            .get_publication(&administrator, workspace.id(), request.request_id())
            .await,
        Err(Error::ProviderPublicationNotFound)
    ));
    assert_eq!(
        crate::tests::stored_setup(&providers.database, workspace.id(), setup.id())
            .await
            .setup
            .state(),
        ProviderSetupState::Ready
    );
    assert!(
        providers
            .inspect_providers(&administrator, workspace.id())
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        terminal_event_count(&providers, &administrator, workspace.id()).await,
        0
    );
    workspaces
        .set_membership(
            &admin_context,
            workspace.id(),
            &actor,
            coral_workspace::WorkspaceRole::Owner,
        )
        .await
        .unwrap();
    assert_eq!(
        providers
            .publish_workspace_provider(&context, request)
            .await
            .unwrap()
            .outcome(),
        ProviderPublicationOutcome::Succeeded
    );
}
