use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn offboarding_and_initial_publication_never_leave_a_departed_users_account() {
    for removal in [Removal::Workspace, Removal::Organization] {
        for shared in [false, true] {
            for removal_first in [false, true] {
                Box::pin(create_and_remove(removal, shared, removal_first)).await;
            }
        }
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "initial publication in both commit orders verifies provisional promotion, exact replay, retained shared authority and User credential retirement"
)]
async fn create_and_remove(removal: Removal, shared: bool, removal_first: bool) {
    let mut f = AccountFixture::new().await;
    let context = change(&f.member);
    let providers = ProviderModule::with_running_catalog(
        coral_outbound::DestinationPolicy::default()
            .with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
        f.db.database(),
        f.workspaces.clone(),
    );
    let setup = providers.prepare_openapi_fixture(&context, f.workspace.id(), OpenApiDocument::parse(r#"{"openapi":"3.0.3","info":{"title":"New","version":"1"},"servers":[{"url":"https://example.com"}],"security":[{"token":[]}],"components":{"securitySchemes":{"token":{"type":"http","scheme":"bearer"}}},"paths":{"/value":{"get":{"operationId":"value","responses":{"200":{"description":"OK"}}}}}}"#).unwrap()).await.unwrap();
    let review = providers
        .review_setup_configuration(&f.member, f.workspace.id(), setup.id())
        .await
        .unwrap();
    let grant = f
        .accounts
        .authorize_setup(
            &context,
            f.workspace.id(),
            setup.id(),
            review.revision,
            coral_credentials::CredentialValues::token(
                Secret::new("new-account-secret".into()).unwrap(),
            ),
        )
        .await
        .unwrap()
        .provisional_grant_id
        .unwrap();
    let request = WorkspaceProviderPublicationRequest::new(
        request(),
        f.workspace.id().clone(),
        setup.id().clone(),
        ProviderNamespace::parse("newProvider").unwrap(),
        if shared {
            CredentialRoute::Fixed
        } else {
            CredentialRoute::Caller
        },
    )
    .with_account_grant(grant);
    let gate = Arc::new(RemovalGate::default());
    if removal_first {
        f.lifecycle.pause_after_prepare = Some(gate.clone());
    }
    let f = Arc::new(f);
    let result = if removal_first {
        let removing = {
            let f = f.clone();
            tokio::spawn(async move {
                remove(&f, removal).await;
            })
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), gate.prepared.notified())
            .await
            .unwrap();
        let (result, ()) = tokio::join!(biased;
            providers.publish_workspace_provider(&context, request.clone()),
            async { gate.proceed.notify_one(); },
        );
        removing.await.unwrap();
        result.unwrap_err();
        None
    } else {
        let result = providers
            .publish_workspace_provider(&context, request.clone())
            .await
            .unwrap();
        assert_eq!(result.outcome(), ProviderPublicationOutcome::Succeeded);
        remove(&f, removal).await;
        Some(result)
    };
    rejoin(&f, removal).await;
    let before = providers
        .list_lifecycle_events(&f.owner, f.workspace.id())
        .await
        .unwrap();
    assert!(
        !providers
            .setup_authentication(&f.member, f.workspace.id(), setup.id(), 100)
            .await
            .unwrap()
            .authorized
    );
    if let Some(result) = result {
        let published = result.published_provider().unwrap();
        let scope = if shared {
            AccountScope::Shared
        } else {
            AccountScope::User
        };
        let account = f
            .accounts
            .get_account(&context, f.workspace.id(), published.id(), &scope)
            .await
            .unwrap()
            .account;
        if shared {
            assert_eq!(account.unwrap().id, grant);
        } else {
            assert!(account.is_none());
        }
        assert_eq!(
            providers
                .publish_workspace_provider(&context, request)
                .await
                .unwrap(),
            result
        );
    } else {
        providers
            .publish_workspace_provider(&context, request)
            .await
            .unwrap_err();
    }
    assert_eq!(
        providers
            .inspect_providers(&f.member, f.workspace.id())
            .await
            .unwrap()
            .len(),
        if removal_first { 1 } else { 2 }
    );
    assert_eq!(
        providers
            .list_lifecycle_events(&f.owner, f.workspace.id())
            .await
            .unwrap(),
        before
    );
}
