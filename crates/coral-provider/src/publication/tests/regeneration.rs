//! Final revalidation is observable through publication and current Provider reads.
use super::*;
use crate::Error;
use coral_catalog::ServerSelection;
use coral_workspace::{WorkspaceAction, WorkspaceRole, WorkspaceState};

enum InterveningChange {
    ArchiveWorkspace,
    DemoteOwner,
    EditConfiguration,
}

#[tokio::test]
async fn regeneration_rechecks_workspace_lifecycle_after_materialization() {
    rejected_regeneration(InterveningChange::ArchiveWorkspace).await;
}

#[tokio::test]
async fn regeneration_rechecks_owner_authority_after_materialization() {
    rejected_regeneration(InterveningChange::DemoteOwner).await;
}

#[tokio::test]
async fn regeneration_rechecks_setup_configuration_after_materialization() {
    rejected_regeneration(InterveningChange::EditConfiguration).await;
}

#[expect(
    clippy::too_many_lines,
    reason = "the race and its public before/after evidence form one acceptance journey"
)]
async fn rejected_regeneration(change: InterveningChange) {
    let (_database_fixture, database, workspaces) = test_foundation().await;
    let default_providers = ProviderModule::with_running_catalog(
        DestinationPolicy::default(),
        database.clone(),
        workspaces.clone(),
    );
    let fixture = WorkspaceRegenerationFixture::new(
        &workspaces,
        &default_providers,
        "user:regeneration-final-validation",
        "01000000-0000-4000-8000-000000000021",
    )
    .await;
    let owner = fixture.workspace_id.clone();
    let context = change_context(fixture.owner.clone(), 40, '1');
    // A second designated Owner can inspect retained state after the publisher
    // loses authority.
    let inspector = organization_user(
        database.clone(),
        "user:regeneration-final-validation-inspector",
        OrganizationRole::Member,
    )
    .await;
    workspaces
        .set_membership(
            &context,
            &fixture.workspace_id,
            &inspector,
            WorkspaceRole::Owner,
        )
        .await
        .unwrap();
    let inspection_context = change_context(inspector.clone(), 45, '3');
    let setup = fixture
        .replacement_setup(&default_providers, UPDATED_CREDENTIAL_FREE_OPENAPI, 40, '1')
        .await;
    let request = ProviderRegenerationRequest::new(
        ManagementRequestId::parse("01000000-0000-4000-8000-000000000022").unwrap(),
        owner.clone(),
        fixture.provider_id.clone(),
        setup.id().clone(),
        1,
    );
    let original_provider = default_providers
        .inspect_provider(&fixture.owner, &owner, &fixture.provider_id)
        .await
        .unwrap();
    let tool_key = ToolKey::parse("getForecast").unwrap();
    let original_tool = default_providers
        .inspect_tool(
            &fixture.owner,
            &fixture.workspace_id,
            &fixture.provider_id,
            &tool_key,
        )
        .await
        .unwrap();
    let materializer = TestMaterializer::gated();
    let providers = ProviderModule::with_test_dependencies(
        database,
        workspaces.clone(),
        materializer.clone(),
        Arc::new(NoopPublicationObserver),
    );
    let publishing = spawn_regeneration(
        providers.clone(),
        fixture.owner.clone(),
        request.clone(),
        50,
        '2',
    );
    tokio::time::timeout(Duration::from_secs(5), materializer.held())
        .await
        .unwrap();
    let intervening = tokio::time::timeout(Duration::from_secs(5), async {
        match change {
            InterveningChange::ArchiveWorkspace => {
                workspaces
                    .archive(&context, &fixture.workspace_id)
                    .await
                    .unwrap();
            }
            InterveningChange::DemoteOwner => {
                workspaces
                    .set_membership(
                        &inspection_context,
                        &fixture.workspace_id,
                        &fixture.owner,
                        WorkspaceRole::Member,
                    )
                    .await
                    .unwrap();
                let evidence = workspaces
                    .check(
                        &fixture.owner,
                        &fixture.workspace_id,
                        WorkspaceAction::Inspect,
                    )
                    .await
                    .unwrap();
                assert_eq!(evidence.workspace().state(), WorkspaceState::Active);
                assert_eq!(evidence.effective_role(), WorkspaceRole::Member);
            }
            InterveningChange::EditConfiguration => {
                let mut configuration = providers
                    .review_setup_configuration(&fixture.owner, &owner, setup.id())
                    .await
                    .unwrap()
                    .confirmed
                    .unwrap();
                configuration.servers.insert(
                    "#/servers".into(),
                    ServerSelection::Custom {
                        url: "https://weather.example/changed".into(),
                    },
                );
                assert!(
                    providers
                        .confirm_setup_configuration(&context, &owner, setup.id(), 1, configuration)
                        .await
                        .unwrap()
                        .errors
                        .is_empty()
                );
            }
        }
    })
    .await;
    materializer.release();
    intervening.expect("final-state edits do not wait for a materialization transaction");
    let error = tokio::time::timeout(Duration::from_secs(5), publishing)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    match change {
        InterveningChange::ArchiveWorkspace => assert!(matches!(
            error,
            Error::Workspace(coral_workspace::Error::WorkspaceArchived)
        )),
        InterveningChange::DemoteOwner => assert!(matches!(
            error,
            Error::Workspace(coral_workspace::Error::PermissionDenied(_))
        )),
        InterveningChange::EditConfiguration => assert!(matches!(error, Error::EtagMismatch)),
    }
    assert!(matches!(
        providers
            .get_regeneration_publication(&inspector, &owner, request.request_id())
            .await,
        Err(Error::ProviderPublicationNotFound)
    ));
    assert_eq!(
        crate::tests::stored_setup(&providers.database, &fixture.workspace_id, setup.id())
            .await
            .setup
            .state(),
        ProviderSetupState::Ready
    );
    assert_eq!(
        providers
            .inspect_provider(&inspector, &owner, &fixture.provider_id)
            .await
            .unwrap(),
        original_provider
    );
    assert_eq!(
        providers
            .inspect_tool(
                &inspector,
                &fixture.workspace_id,
                &fixture.provider_id,
                &tool_key
            )
            .await
            .unwrap(),
        original_tool
    );
    assert_eq!(
        providers
            .inspect_providers(&inspector, &fixture.workspace_id)
            .await
            .unwrap()
            .iter()
            .map(crate::Provider::access)
            .cloned()
            .collect::<Vec<_>>(),
        vec![fixture.provider_access.clone()]
    );
    assert_eq!(
        providers
            .list_lifecycle_events(&inspector, &fixture.workspace_id)
            .await
            .unwrap()
            .iter()
            .filter(|event| event.action() == LifecycleAction::PublicationCompleted)
            .count(),
        1
    );

    let revision = match change {
        InterveningChange::ArchiveWorkspace => {
            workspaces
                .restore(&context, &fixture.workspace_id)
                .await
                .unwrap();
            1
        }
        InterveningChange::DemoteOwner => {
            workspaces
                .set_membership(
                    &inspection_context,
                    &fixture.workspace_id,
                    &fixture.owner,
                    WorkspaceRole::Owner,
                )
                .await
                .unwrap();
            1
        }
        InterveningChange::EditConfiguration => 2,
    };
    let retried = providers
        .regenerate_provider(
            &context,
            ProviderRegenerationRequest::new(
                request.request_id().clone(),
                owner,
                fixture.provider_id,
                setup.id().clone(),
                revision,
            ),
        )
        .await
        .unwrap();
    assert_eq!(retried.outcome(), ProviderPublicationOutcome::Succeeded);
    assert_eq!(
        providers
            .get_setup(&fixture.owner, &fixture.workspace_id, setup.id())
            .await
            .unwrap()
            .state(),
        ProviderSetupState::Consumed(retried.id().clone())
    );
}

#[tokio::test]
async fn competing_regeneration_requests_consume_one_setup_once() {
    let (_database_fixture, database, workspaces) = test_foundation().await;
    let default_providers = ProviderModule::with_running_catalog(
        DestinationPolicy::default(),
        database.clone(),
        workspaces.clone(),
    );
    let fixture = WorkspaceRegenerationFixture::new(
        &workspaces,
        &default_providers,
        "user:regeneration-consumption",
        "01000000-0000-4000-8000-000000000023",
    )
    .await;
    let setup = fixture
        .replacement_setup(&default_providers, UPDATED_CREDENTIAL_FREE_OPENAPI, 40, '3')
        .await;
    let owner = fixture.workspace_id.clone();
    let first_request = ProviderRegenerationRequest::new(
        ManagementRequestId::parse("01000000-0000-4000-8000-000000000024").unwrap(),
        owner.clone(),
        fixture.provider_id.clone(),
        setup.id().clone(),
        1,
    );
    let second_request = ProviderRegenerationRequest::new(
        ManagementRequestId::parse("01000000-0000-4000-8000-000000000025").unwrap(),
        owner.clone(),
        fixture.provider_id.clone(),
        setup.id().clone(),
        1,
    );
    let barrier = Arc::new(Barrier::new(3));
    let providers = ProviderModule::with_test_dependencies(
        database,
        workspaces,
        TestMaterializer::meeting_at(barrier.clone()),
        Arc::new(NoopPublicationObserver),
    );
    let first = spawn_regeneration(
        providers.clone(),
        fixture.owner.clone(),
        first_request.clone(),
        50,
        '4',
    );
    let second = spawn_regeneration(
        providers.clone(),
        fixture.owner.clone(),
        second_request.clone(),
        60,
        '5',
    );
    let (first, second) = tokio::time::timeout(Duration::from_secs(10), async {
        barrier.wait().await;
        (first.await.unwrap(), second.await.unwrap())
    })
    .await
    .unwrap();
    let (winner, losing_request) = match (first, second) {
        (Ok(winner), Err(Error::ProviderSetupConsumed)) => (winner, second_request),
        (Err(Error::ProviderSetupConsumed), Ok(winner)) => (winner, first_request),
        other => panic!("expected one terminal winner: {other:?}"),
    };
    assert_eq!(winner.outcome(), ProviderPublicationOutcome::Succeeded);
    assert_eq!(
        providers
            .get_setup(&fixture.owner, &fixture.workspace_id, setup.id())
            .await
            .unwrap()
            .state(),
        ProviderSetupState::Consumed(winner.id().clone())
    );
    assert!(matches!(
        providers
            .get_regeneration_publication(&fixture.owner, &owner, losing_request.request_id())
            .await,
        Err(Error::ProviderPublicationNotFound)
    ));
    assert_eq!(
        providers
            .list_lifecycle_events(&fixture.owner, &fixture.workspace_id)
            .await
            .unwrap()
            .iter()
            .filter(|event| event.action() == LifecycleAction::PublicationCompleted)
            .count(),
        2
    );
}
