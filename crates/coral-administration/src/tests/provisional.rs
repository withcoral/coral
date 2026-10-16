use super::*;
use coral_provider::access::{CredentialRoute, ProviderNamespace};
use coral_provider::account::AccountModule;
use coral_provider::{
    ManagementRequestId, OpenApiDocument, ProviderModule, ProviderSetupState,
    WorkspaceProviderPublicationRequest,
};

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "rollback preserves a usable provisional grant; successful offboarding makes that same grant unpublishable"
)]
async fn provisional_credentials_roll_back_with_authority_and_cannot_be_promoted_after_removal() {
    for remove in [false, true] {
        let fixture = fixture().await;
        let database = fixture.database();
        let workspaces = WorkspaceModule::new(database.clone());
        let owner = provision_user(database.clone(), "user:owner", OrganizationRole::Owner).await;
        let member =
            provision_user(database.clone(), "user:member", OrganizationRole::Member).await;
        let workspace = workspaces
            .create(
                &change(&member),
                WorkspaceDisplayName::parse("Provisional cleanup").unwrap(),
            )
            .await
            .unwrap();
        join_as_owner(&workspaces, &owner, workspace.id()).await;
        let providers = ProviderModule::with_running_catalog(
            coral_outbound::DestinationPolicy::default()
                .with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
            database.clone(),
            workspaces.clone(),
        );
        let key = tempfile::tempdir().unwrap();
        let accounts = AccountModule::new(
            providers.clone(),
            coral_credentials::CredentialVault::open(&key.path().join("key")).unwrap(),
        );
        let setup = providers
            .prepare_openapi_fixture(
                &change(&member),
                workspace.id(),
                OpenApiDocument::parse(BEARER_OPENAPI).unwrap(),
            )
            .await
            .unwrap();
        let review = providers
            .review_setup_configuration(&member, workspace.id(), setup.id())
            .await
            .unwrap();
        let authentication = accounts
            .authorize_setup(
                &change(&member),
                workspace.id(),
                setup.id(),
                review.revision,
                coral_credentials::CredentialValues::token(
                    coral_credentials::Secret::new("provisional-secret".into()).unwrap(),
                ),
            )
            .await
            .unwrap();
        assert!(authentication.authorized);
        let grant = authentication.provisional_grant_id.unwrap();
        let before = providers
            .list_lifecycle_events(&owner, workspace.id())
            .await
            .unwrap();
        let lifecycle = AccessLifecycleModule::new(database.clone(), workspaces.clone());
        for context in failure_contexts(
            &owner,
            &[EventFault::SetupCanceled, EventFault::MembershipRemoved],
        ) {
            lifecycle
                .remove_workspace_member(&context, workspace.id(), member.user_id())
                .await
                .unwrap_err();
            assert_eq!(
                providers
                    .setup_authentication(&member, workspace.id(), setup.id(), 100)
                    .await
                    .unwrap(),
                authentication
            );
            assert_eq!(
                providers
                    .list_lifecycle_events(&owner, workspace.id())
                    .await
                    .unwrap(),
                before
            );
            assert_eq!(
                providers
                    .get_setup(&member, workspace.id(), setup.id())
                    .await
                    .unwrap()
                    .state(),
                ProviderSetupState::Ready
            );
        }
        if remove {
            lifecycle
                .remove_workspace_member(&change(&owner), workspace.id(), member.user_id())
                .await
                .unwrap();
            workspaces
                .set_membership(
                    &change(&owner),
                    workspace.id(),
                    &member,
                    coral_workspace::WorkspaceRole::Owner,
                )
                .await
                .unwrap();
            let retired = providers
                .setup_authentication(&member, workspace.id(), setup.id(), 100)
                .await
                .unwrap();
            assert!(!retired.authorized);
            // Safe attribution can remain; the grant must no longer be usable.
            assert_eq!(retired.provisional_grant_id, Some(grant));
            assert_eq!(
                providers
                    .get_setup(&member, workspace.id(), setup.id())
                    .await
                    .unwrap()
                    .state(),
                ProviderSetupState::Canceled
            );
        }
        let published = providers
            .publish_workspace_provider(
                &change(&member),
                WorkspaceProviderPublicationRequest::new(
                    ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
                    workspace.id().clone(),
                    setup.id().clone(),
                    ProviderNamespace::parse("private").unwrap(),
                    CredentialRoute::Caller,
                )
                .with_account_grant(grant),
            )
            .await;
        if remove {
            published.unwrap_err();
            assert!(
                providers
                    .inspect_providers(&owner, workspace.id())
                    .await
                    .unwrap()
                    .is_empty()
            );
        } else {
            let publication = published.unwrap();
            let provider = publication.published_provider().unwrap().access().id();
            let evidence = accounts::credential_evidence(&database, workspace.id(), &member).await;
            assert_eq!(
                accounts
                    .credentials(workspace.id(), provider, &evidence, 100)
                    .await
                    .unwrap()
                    .get(coral_credentials::CredentialValues::TOKEN)
                    .unwrap()
                    .expose(),
                "provisional-secret"
            );
        }
    }
}
