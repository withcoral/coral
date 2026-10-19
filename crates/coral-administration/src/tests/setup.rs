use super::{change, failure_contexts, fixture, repository::EventFault};
use crate::AccessLifecycleModule;
use coral_credentials::{CredentialVault, Secret};
use coral_provider::account::{AccountModule, SetupOAuthClient};
use coral_provider::{
    OAuthClientAuthentication, OAuthClientConfiguration, OpenApiDocument, ProviderModule,
    ProviderSetupState,
};
use coral_test_support::provision_user;
use coral_workspace::{OrganizationRole, WorkspaceDisplayName, WorkspaceModule, WorkspaceRole};

use super::configured_provider_fixtures::ConfiguredProviderFixtures as _;

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "the public Setup and offboarding journey includes rollback, rejoin, and stale configuration rejection"
)]
async fn removal_scrubs_configuration_only_setup_secrets_and_prevents_reuse_after_rejoin() {
    let db = fixture().await;
    let database = db.database();
    let workspaces = WorkspaceModule::new(database.clone());
    let owner = provision_user(
        database.clone(),
        "user:setup-admin",
        OrganizationRole::Owner,
    )
    .await;
    let member = provision_user(
        database.clone(),
        "user:setup-member",
        OrganizationRole::Member,
    )
    .await;
    let workspace = workspaces
        .create(
            &change(&owner),
            WorkspaceDisplayName::parse("Setup cleanup").unwrap(),
        )
        .await
        .unwrap();
    workspaces
        .set_membership(
            &change(&owner),
            workspace.id(),
            &member,
            WorkspaceRole::Owner,
        )
        .await
        .unwrap();
    let providers = ProviderModule::with_running_catalog(
        coral_outbound::DestinationPolicy::default()
            .with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
        database.clone(),
        workspaces.clone(),
    );
    let dir = tempfile::tempdir().unwrap();
    let accounts = AccountModule::new(
        providers.clone(),
        CredentialVault::open(&dir.path().join("key")).unwrap(),
    );
    let scope = workspace.id().clone();
    let document = OpenApiDocument::parse(r#"{
        "openapi":"3.1.0", "info":{"title":"Configuration only", "version":"1"},
        "servers":[{"url":"https://example.com"}], "security":[{"account":["read"]}],
        "components":{"securitySchemes":{"account":{"type":"oauth2","flows":{
            "authorizationCode":{"authorizationUrl":"https://example.com/authorize",
                "tokenUrl":"https://example.com/token", "scopes":{"read":"Read"}}
        }}}},
        "paths":{"/data":{"get":{"operationId":"getData","responses":{"200":{"description":"Data"}}}}}
    }"#).unwrap();
    let setup = providers
        .prepare_openapi_fixture(&change(&member), workspace.id(), document)
        .await
        .unwrap();
    let review = providers
        .review_setup_configuration(&member, &scope, setup.id())
        .await
        .unwrap();
    let review = accounts
        .confirm_setup_configuration(
            &change(&member),
            &scope,
            setup.id(),
            review.revision,
            review.confirmed.unwrap(),
            Some(
                SetupOAuthClient::application(
                    "https://reef.example/callback".into(),
                    OAuthClientConfiguration::new(
                        "manual-client".into(),
                        OAuthClientAuthentication::ClientSecretPost {
                            secret: Some(Secret::new("configuration-only-secret".into()).unwrap()),
                        },
                    ),
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
    assert!(review.errors.is_empty());
    assert!(
        providers
            .setup_authentication(&member, &scope, setup.id(), 100)
            .await
            .unwrap()
            .client_secret_configured
    );

    // No account authorization has started; cleanup must enumerate the Setup,
    // not depend on a row containing provisional account credentials.
    let lifecycle = AccessLifecycleModule::new(database, workspaces.clone());
    let faults = failure_contexts(
        &owner,
        &[EventFault::SetupCanceled, EventFault::MembershipRemoved],
    );
    for context in faults {
        assert!(
            lifecycle
                .remove_workspace_member(&context, workspace.id(), member.user_id())
                .await
                .is_err()
        );
        assert!(
            providers
                .setup_authentication(&member, &scope, setup.id(), 100)
                .await
                .unwrap()
                .client_secret_configured
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
    lifecycle
        .remove_workspace_member(&change(&owner), workspace.id(), member.user_id())
        .await
        .unwrap();
    workspaces
        .set_membership(
            &change(&owner),
            workspace.id(),
            &member,
            WorkspaceRole::Owner,
        )
        .await
        .unwrap();
    assert_eq!(
        providers
            .get_setup(&member, workspace.id(), setup.id())
            .await
            .unwrap()
            .state(),
        ProviderSetupState::Canceled
    );
    assert!(
        !providers
            .setup_authentication(&member, &scope, setup.id(), 100)
            .await
            .unwrap()
            .client_secret_configured
    );
    assert!(matches!(
        providers
            .confirm_setup_configuration(
                &change(&member),
                &scope,
                setup.id(),
                review.revision,
                review.confirmed.unwrap()
            )
            .await,
        Err(coral_provider::Error::ProviderSetupNotApplicable)
    ));
}
