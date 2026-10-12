use super::*;

#[tokio::test]
async fn creation_and_update_share_the_workspace_publication_request_namespace() {
    let (_database, workspaces, providers) = test_modules().await;
    let creation_id = "71000000-0000-4000-8000-000000000001";
    let fixture = WorkspaceRegenerationFixture::new(
        &workspaces,
        &providers,
        "user:publication-request-scope",
        creation_id,
    )
    .await;
    let setup = fixture
        .replacement_setup(&providers, UPDATED_CREDENTIAL_FREE_OPENAPI, 40, '4')
        .await;
    assert!(matches!(
        providers
            .regenerate_provider(
                &change_context(fixture.owner.clone(), 50, '5'),
                fixture.request(setup.id(), creation_id),
            )
            .await,
        Err(crate::Error::RequestIdConflict)
    ));
    assert_eq!(
        providers
            .get_setup(&fixture.owner, &fixture.workspace_id, setup.id())
            .await
            .expect("creation request conflict leaves update Setup reusable")
            .state(),
        ProviderSetupState::Ready
    );

    let update_id = "71000000-0000-4000-8000-000000000002";
    providers
        .regenerate_provider(
            &change_context(fixture.owner.clone(), 60, '6'),
            fixture.request(setup.id(), update_id),
        )
        .await
        .expect("publish update with its own request identity");
    let creation_setup = fixture
        .replacement_setup(&providers, CREDENTIAL_FREE_OPENAPI, 70, '7')
        .await;
    assert!(matches!(
        providers
            .publish_workspace_provider(
                &change_context(fixture.owner.clone(), 80, '8'),
                publication_request(
                    &fixture.workspace_id,
                    creation_setup.id(),
                    "another_weather",
                    CredentialRoute::None,
                    update_id
                ),
            )
            .await,
        Err(crate::Error::RequestIdConflict)
    ));
    assert_eq!(
        providers
            .get_setup(&fixture.owner, &fixture.workspace_id, creation_setup.id())
            .await
            .expect("update request conflict leaves creation Setup reusable")
            .state(),
        ProviderSetupState::Ready
    );
}
