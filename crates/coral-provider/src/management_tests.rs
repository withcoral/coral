//! Provider management inspection and etag preconditions at the Module seam.
use super::tests::{change_context, organization_user, test_modules};
use crate::access::{CredentialRoute, ProviderNamespace};
use crate::{
    Error, ManagementRequestId, ProviderPublicationOutcome, ProviderRegenerationRequest,
    ProviderSetupState, ProviderState, WorkspaceProviderPublicationRequest,
};
use coral_workspace::{ChangeContext, OrganizationRole, WorkspaceDisplayName};

fn document(title: &str) -> crate::OpenApiDocument {
    crate::OpenApiDocument::parse(&format!(
        r#"{{"openapi":"3.1.0","info":{{"title":"{title}","version":"1"}},
"servers":[{{"url":"https://{title}.example.test"}}],
"paths":{{"/data":{{"get":{{"operationId":"getData","responses":{{"200":{{"description":"Data"}}}}}}}}}}}}"#
    ))
    .unwrap()
}

fn with_etag(context: ChangeContext, etag: &str) -> ChangeContext {
    context.with_etag(etag).unwrap()
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one journey proves every etag-guarded Provider lifecycle and credential route change"
)]
async fn stale_etag_preconditions_abort_lifecycle_and_configuration_changes() {
    let (db, workspaces, providers) = test_modules().await;
    let owner = organization_user(db.database(), "user:etags", OrganizationRole::Owner).await;
    let workspace = workspaces
        .create(
            &change_context(owner.clone(), 1, '1'),
            WorkspaceDisplayName::parse("Etags").unwrap(),
        )
        .await
        .unwrap();
    let setup = providers
        .prepare_openapi_fixture(
            &change_context(owner.clone(), 2, '2'),
            workspace.id(),
            document("etag"),
        )
        .await
        .unwrap();
    let provider = providers
        .publish_workspace_provider(
            &change_context(owner.clone(), 3, '3'),
            WorkspaceProviderPublicationRequest::new(
                ManagementRequestId::parse("c0000000-0000-4000-8000-000000000001").unwrap(),
                workspace.id().clone(),
                setup.id().clone(),
                ProviderNamespace::parse("etag").unwrap(),
                CredentialRoute::None,
            ),
        )
        .await
        .unwrap()
        .published_provider()
        .unwrap()
        .clone();
    let stale = provider.etag();
    let scope = workspace.id().clone();
    providers
        .rename_provider(
            &with_etag(change_context(owner.clone(), 5, '5'), &stale),
            &scope,
            provider.id(),
            crate::ProviderDisplayName::parse("Renamed").unwrap(),
        )
        .await
        .unwrap();
    let current = providers
        .inspect_providers(&owner, &scope)
        .await
        .unwrap()
        .into_iter()
        .find(|inspection| inspection.id() == provider.id())
        .unwrap();
    assert_ne!(current.etag(), stale);
    assert!(matches!(
        providers
            .disable_provider(
                &with_etag(change_context(owner.clone(), 6, '6'), &stale),
                &scope,
                provider.id()
            )
            .await,
        Err(Error::EtagMismatch)
    ));
    assert_eq!(current.state(), ProviderState::Active);
    let revoked = providers
        .disable_provider(
            &with_etag(change_context(owner.clone(), 7, '7'), &current.etag()),
            &scope,
            provider.id(),
        )
        .await
        .unwrap();
    assert_eq!(revoked.state(), ProviderState::Disabled);
    assert_ne!(revoked.etag(), current.etag());
    assert!(matches!(
        providers
            .restore_provider(
                &with_etag(change_context(owner.clone(), 8, '8'), &current.etag()),
                &scope,
                provider.id()
            )
            .await,
        Err(Error::EtagMismatch)
    ));
    let restored = providers
        .restore_provider(
            &with_etag(change_context(owner.clone(), 9, '9'), &revoked.etag()),
            &scope,
            provider.id(),
        )
        .await
        .unwrap();
    assert_eq!(restored.state(), ProviderState::Active);

    // Route changes use the Provider resource etag and advance account-setting evidence.
    let provider_access = restored.access();
    let stale_provider_etag = restored.etag();
    assert!(matches!(
        providers
            .change_account_setting_fixture(
                &with_etag(
                    change_context(owner.clone(), 10, 'a'),
                    &uuid::Uuid::new_v4().to_string()
                ),
                workspace.id(),
                provider_access.id(),
                CredentialRoute::Caller,
            )
            .await,
        Err(Error::EtagMismatch)
    ));
    let rerouted = providers
        .change_account_setting_fixture(
            &with_etag(change_context(owner.clone(), 11, 'b'), &stale_provider_etag),
            workspace.id(),
            provider_access.id(),
            CredentialRoute::Caller,
        )
        .await
        .unwrap();
    let rerouted_provider = providers
        .inspect_provider(&owner, &scope, provider.id())
        .await
        .unwrap();
    assert_ne!(rerouted_provider.etag(), stale_provider_etag);
    assert_eq!(rerouted_provider.access(), &rerouted);
    assert!(matches!(
        providers
            .delete_provider(
                &with_etag(change_context(owner.clone(), 12, 'c'), &stale_provider_etag),
                workspace.id(),
                provider_access.id(),
            )
            .await,
        Err(Error::EtagMismatch)
    ));
    // Regeneration checks the Provider etag before materializing and leaves the
    // Setup reusable on mismatch; success advances the etag.
    let replacement = providers
        .prepare_openapi_fixture(
            &change_context(owner.clone(), 14, 'e'),
            workspace.id(),
            document("etag-two"),
        )
        .await
        .unwrap();
    let request = ProviderRegenerationRequest::new(
        ManagementRequestId::parse("c0000000-0000-4000-8000-000000000002").unwrap(),
        scope.clone(),
        provider.id().clone(),
        replacement.id().clone(),
        1,
    );
    assert!(matches!(
        providers
            .regenerate_provider(
                &with_etag(change_context(owner.clone(), 15, 'f'), &stale),
                request.clone()
            )
            .await,
        Err(Error::EtagMismatch)
    ));
    assert_eq!(
        providers
            .get_setup(&owner, &scope, replacement.id())
            .await
            .unwrap()
            .state(),
        ProviderSetupState::Ready
    );
    let regenerated = providers
        .regenerate_provider(
            &with_etag(
                change_context(owner.clone(), 16, '1'),
                &rerouted_provider.etag(),
            ),
            request,
        )
        .await
        .unwrap();
    assert_eq!(regenerated.outcome(), ProviderPublicationOutcome::Succeeded);
    let after = providers
        .inspect_providers(&owner, &scope)
        .await
        .unwrap()
        .into_iter()
        .find(|inspection| inspection.id() == provider.id())
        .unwrap();
    assert_ne!(after.etag(), restored.etag());
    assert_eq!(after.display_name(), "etag-two");
    // Deletion checks the etag too, and a context without one still deletes.
    assert!(matches!(
        providers
            .delete_provider(
                &with_etag(change_context(owner.clone(), 17, '2'), &restored.etag()),
                &scope,
                provider.id()
            )
            .await,
        Err(Error::EtagMismatch)
    ));
    providers
        .delete_provider(
            &change_context(owner.clone(), 18, '3'),
            &scope,
            provider.id(),
        )
        .await
        .unwrap();
}

coral_test_support::provider_setup_fixtures!(crate);

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one visibility journey checks scope, membership, lifecycle, and secret-free catalog inspection"
)]
async fn provider_and_catalog_inspection_require_current_workspace_access() {
    use coral_workspace::WorkspaceRole;
    let (db, workspaces, providers) = test_modules().await;
    let owner = organization_user(
        db.database(),
        "user:inspection-owner",
        OrganizationRole::Member,
    )
    .await;
    let member = organization_user(
        db.database(),
        "user:inspection-member",
        OrganizationRole::Member,
    )
    .await;
    let stranger = organization_user(
        db.database(),
        "user:inspection-stranger",
        OrganizationRole::Member,
    )
    .await;
    let administrator = organization_user(
        db.database(),
        "user:inspection-administrator",
        OrganizationRole::Owner,
    )
    .await;
    let context = change_context(owner.clone(), 10, 'a');
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Inspection").unwrap())
        .await
        .unwrap();
    let other = workspaces
        .create(&context, WorkspaceDisplayName::parse("Other").unwrap())
        .await
        .unwrap();
    workspaces
        .set_membership(&context, workspace.id(), &member, WorkspaceRole::Member)
        .await
        .unwrap();
    let provider = crate::tests::publish_provider(
        &providers,
        &context,
        workspace.id(),
        crate::tests::CREDENTIAL_FREE_OPENAPI,
        "weather",
        CredentialRoute::None,
    )
    .await;
    for actor in [&owner, &member] {
        assert_eq!(
            providers
                .inspect_providers(actor, workspace.id())
                .await
                .unwrap(),
            std::slice::from_ref(&provider)
        );
        assert_eq!(
            providers
                .inspect_provider(actor, workspace.id(), provider.id())
                .await
                .unwrap(),
            provider
        );
        assert_eq!(
            providers
                .inspect_catalog_page(actor, workspace.id(), provider.id(), None, 10)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            crate::tests::published_providers(
                &providers,
                &coral_workspace::AuthenticatedPrincipal::user(actor.clone()),
                workspace.id(),
            )
            .await
            .unwrap()
            .into_iter()
            .map(|published| published.provider)
            .collect::<Vec<_>>(),
            std::slice::from_ref(&provider)
        );
    }
    // An Organization Owner without a Membership is as much a stranger as any non-Member.
    for actor in [&stranger, &administrator] {
        assert!(matches!(
            providers.inspect_providers(actor, workspace.id()).await,
            Err(Error::Workspace(coral_workspace::Error::WorkspaceNotFound))
        ));
        assert!(matches!(
            providers
                .inspect_provider(actor, workspace.id(), provider.id())
                .await,
            Err(Error::Workspace(coral_workspace::Error::WorkspaceNotFound))
        ));
        assert!(matches!(
            providers
                .inspect_catalog_page(actor, workspace.id(), provider.id(), None, 10)
                .await,
            Err(Error::Workspace(coral_workspace::Error::WorkspaceNotFound))
        ));
        assert!(matches!(
            crate::tests::published_providers(
                &providers,
                &coral_workspace::AuthenticatedPrincipal::user(actor.clone()),
                workspace.id(),
            )
            .await,
            Err(Error::Workspace(coral_workspace::Error::WorkspaceNotFound))
        ));
    }
    assert!(matches!(
        providers.inspect_providers(&member, other.id()).await,
        Err(Error::Workspace(_))
    ));
    assert!(matches!(
        providers
            .inspect_provider(&owner, other.id(), provider.id())
            .await,
        Err(Error::ProviderNotFound)
    ));
    assert!(matches!(
        providers
            .inspect_catalog_page(&owner, other.id(), provider.id(), None, 10)
            .await,
        Err(Error::ProviderNotFound)
    ));
    let disabled = providers
        .disable_provider(&context, workspace.id(), provider.id())
        .await
        .unwrap();
    workspaces.archive(&context, workspace.id()).await.unwrap();
    assert_eq!(
        providers
            .inspect_provider(&member, workspace.id(), provider.id())
            .await
            .unwrap(),
        disabled
    );
    assert_eq!(
        providers
            .inspect_catalog_page(&member, workspace.id(), provider.id(), None, 10)
            .await
            .unwrap()
            .len(),
        1
    );
    coral_workspace::OrganizationModule::new(db.database())
        .set_enabled(
            &change_context(administrator, 11, 'b'),
            member.user_id(),
            1,
            false,
        )
        .await
        .unwrap();
    assert!(matches!(
        providers
            .inspect_provider(&member, workspace.id(), provider.id())
            .await,
        Err(Error::Workspace(_))
    ));
    assert!(matches!(
        providers
            .inspect_catalog_page(&member, workspace.id(), provider.id(), None, 10)
            .await,
        Err(Error::Workspace(_))
    ));
}

// Catches: a read that shows an older name than the Provider's, that misses a
// disabled Provider, that crosses Workspaces, that reaches the Built-in
// Provider by a namespace, or that shows a member what only an Owner
// configures.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one read and the table of every lookup that finds nothing"
)]
async fn an_owner_reads_a_providers_configuration_by_namespace() {
    use coral_workspace::WorkspaceRole;
    let (db, workspaces, providers) = test_modules().await;
    let owner =
        organization_user(db.database(), "user:saved-owner", OrganizationRole::Member).await;
    let member =
        organization_user(db.database(), "user:saved-member", OrganizationRole::Member).await;
    let context = change_context(owner.clone(), 10, 'a');
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Saved").unwrap())
        .await
        .unwrap();
    let database = db.database();
    let mut transaction = database.begin().await.unwrap();
    providers
        .install_builtin_in_transaction(&mut transaction, &context, workspace.id())
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let other = workspaces
        .create(&context, WorkspaceDisplayName::parse("Other").unwrap())
        .await
        .unwrap();
    workspaces
        .set_membership(&context, workspace.id(), &member, WorkspaceRole::Member)
        .await
        .unwrap();
    let published = crate::tests::publish_provider(
        &providers,
        &context,
        workspace.id(),
        crate::tests::CREDENTIAL_FREE_OPENAPI,
        "weather",
        CredentialRoute::None,
    )
    .await;
    // A rename publishes no Generation.
    let renamed = providers
        .rename_provider(
            &with_etag(change_context(owner.clone(), 11, 'b'), &published.etag()),
            workspace.id(),
            published.id(),
            crate::ProviderDisplayName::parse("Forecasts").unwrap(),
        )
        .await
        .unwrap();
    let disabled = providers
        .disable_provider(
            &with_etag(change_context(owner.clone(), 12, 'c'), &renamed.etag()),
            workspace.id(),
            published.id(),
        )
        .await
        .unwrap();
    let weather = ProviderNamespace::parse("weather").unwrap();

    let inspected = providers
        .inspect_provider_configuration(&owner, workspace.id(), &weather)
        .await
        .unwrap();
    assert_eq!(inspected.provider, disabled);
    assert_eq!(inspected.provider.display_name(), "Forecasts");
    assert_eq!(
        inspected.source,
        crate::ProviderSource {
            url: None,
            kind: coral_catalog::ProviderSetupInputKind::OpenApi,
        }
    );
    assert_eq!(
        inspected.servers,
        std::collections::BTreeMap::from([(
            "#/servers".to_owned(),
            coral_catalog::ServerSelection::Declared {
                url: "https://weather.example".to_owned(),
                variables: std::collections::BTreeMap::new(),
            },
        )])
    );
    assert_eq!(
        inspected.authentication,
        coral_catalog::AuthenticationConfiguration::None
    );
    assert!(inspected.request_headers.is_empty());
    assert!(inspected.credential_fields.is_empty());

    let not_found: fn(&Error) -> bool = |error| matches!(error, Error::ProviderNotFound);
    let not_an_owner: fn(&Error) -> bool = |error| {
        matches!(
            error,
            Error::Workspace(coral_workspace::Error::PermissionDenied(_))
        )
    };
    let forecasts = ProviderNamespace::parse("forecasts").unwrap();
    // The Built-in Provider's Operations are under coral.workspace.
    let builtin = ProviderNamespace::parse("workspace").unwrap();
    for (actor, workspace, namespace, expected, denial) in [
        (
            &owner,
            workspace.id(),
            &forecasts,
            not_found,
            "no Provider holds the namespace",
        ),
        (
            &owner,
            workspace.id(),
            &builtin,
            not_found,
            "the Built-in Provider",
        ),
        (&owner, other.id(), &weather, not_found, "another Workspace"),
        (&member, workspace.id(), &weather, not_an_owner, "a member"),
    ] {
        let error = providers
            .inspect_provider_configuration(actor, workspace, namespace)
            .await
            .expect_err(denial);
        assert!(expected(&error), "{denial}: {error:?}");
    }
}
