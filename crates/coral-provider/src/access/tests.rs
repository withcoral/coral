//! Provider authority and scoped credential routes through the domain interface.
use super::*;
use crate::LifecycleAction;
use crate::account_tests::{CREDENTIAL_REQUIRED_OPENAPI, authorization_request};
use crate::tests::{
    CREDENTIAL_FREE_OPENAPI, change_context, organization_user, publish_provider, test_modules,
};
use coral_credentials::Secret;
use coral_workspace::{OrganizationRole, WorkspaceDisplayName, WorkspaceRole};

#[tokio::test]
async fn exact_inspection_uses_workspace_scope_and_requires_a_membership() {
    let (db, workspaces, providers) = test_modules().await;
    let owner =
        organization_user(db.database(), "user:exact-owner", OrganizationRole::Member).await;
    let administrator =
        organization_user(db.database(), "user:administrator", OrganizationRole::Owner).await;
    let context = change_context(owner.clone(), 10, 'a');
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Exact").unwrap())
        .await
        .unwrap();
    let other = workspaces
        .create(&context, WorkspaceDisplayName::parse("Other").unwrap())
        .await
        .unwrap();
    let provider = publish_provider(
        &providers,
        &context,
        workspace.id(),
        CREDENTIAL_FREE_OPENAPI,
        "exact",
        CredentialRoute::None,
    )
    .await;
    // An Organization Owner without a Membership gets the non-Member denial.
    assert!(matches!(
        providers
            .inspect_provider(&administrator, workspace.id(), provider.id())
            .await,
        Err(Error::Workspace(coral_workspace::Error::WorkspaceNotFound))
    ));
    assert!(matches!(
        providers
            .delete_provider(
                &change_context(administrator.clone(), 11, 'b'),
                workspace.id(),
                provider.id(),
            )
            .await,
        Err(Error::Workspace(coral_workspace::Error::WorkspaceNotFound))
    ));
    assert_eq!(
        providers
            .inspect_provider(&owner, workspace.id(), provider.id())
            .await
            .unwrap(),
        provider
    );
    assert!(matches!(
        providers
            .inspect_provider(&owner, other.id(), provider.id())
            .await,
        Err(Error::ProviderNotFound)
    ));
    // Assigned as a designated Owner, they act like any other Owner.
    crate::tests::join_as_owner(&workspaces, &administrator, workspace.id()).await;
    providers
        .delete_provider(
            &change_context(administrator.clone(), 11, 'b'),
            workspace.id(),
            provider.id(),
        )
        .await
        .unwrap();
    assert!(matches!(
        providers
            .inspect_provider(&administrator, workspace.id(), provider.id())
            .await,
        Err(Error::ProviderNotFound)
    ));
}

#[tokio::test]
async fn route_etag_race_has_one_winner_and_no_op_preserves_version_and_evidence() {
    let (db, workspaces, providers) = test_modules().await;
    let owner = organization_user(db.database(), "user:route-race", OrganizationRole::Owner).await;
    let context = change_context(owner.clone(), 10, 'a');
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Route").unwrap())
        .await
        .unwrap();
    let provider = publish_provider(
        &providers,
        &context,
        workspace.id(),
        CREDENTIAL_FREE_OPENAPI,
        "race",
        CredentialRoute::Caller,
    )
    .await;
    let stale = context.clone().with_etag(&provider.etag()).unwrap();
    let (left, right) = tokio::join!(
        providers.change_account_setting_fixture(
            &stale,
            workspace.id(),
            provider.id(),
            CredentialRoute::None
        ),
        providers.change_account_setting_fixture(
            &stale,
            workspace.id(),
            provider.id(),
            CredentialRoute::None
        ),
    );
    let updated = match (left, right) {
        (Ok(updated), Err(Error::EtagMismatch | Error::ProviderChangedDuringPublication))
        | (Err(Error::EtagMismatch | Error::ProviderChangedDuringPublication), Ok(updated)) => {
            updated
        }
        other => panic!("exactly one route change must commit: {other:?}"),
    };
    assert_eq!(updated.version(), 2);
    assert_eq!(updated.namespace(), provider.namespace());
    let events = providers
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.action() == LifecycleAction::ProviderCredentialRouteChanged)
            .count(),
        1
    );
    let current = providers
        .inspect_provider(&owner, workspace.id(), provider.id())
        .await
        .unwrap();
    let current_context = context.with_etag(&current.etag()).unwrap();
    assert_eq!(
        providers
            .change_account_setting_fixture(
                &current_context,
                workspace.id(),
                provider.id(),
                CredentialRoute::None
            )
            .await
            .unwrap(),
        updated
    );
    assert_eq!(
        providers
            .list_lifecycle_events(&owner, workspace.id())
            .await
            .unwrap()
            .iter()
            .filter(|event| event.action() == LifecycleAction::ProviderCredentialRouteChanged)
            .count(),
        1
    );
    assert!(matches!(
        providers
            .delete_provider(&stale, workspace.id(), provider.id())
            .await,
        Err(Error::EtagMismatch)
    ));
    assert_eq!(
        providers
            .inspect_provider(&owner, workspace.id(), provider.id())
            .await
            .unwrap(),
        current
    );
}

#[test]
fn provider_namespace_rejects_thenable_segment() {
    assert!(matches!(
        ProviderNamespace::parse("then"),
        Err(Error::InvalidInput(_))
    ));
}

#[tokio::test]
async fn workspace_member_cannot_change_provider_configuration() {
    let (db, workspaces, providers) = test_modules().await;
    let owner = organization_user(
        db.database(),
        "user:configuration-owner",
        OrganizationRole::Member,
    )
    .await;
    let member = organization_user(
        db.database(),
        "user:configuration-member",
        OrganizationRole::Member,
    )
    .await;
    let context = change_context(owner, 10, 'a');
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Authority").unwrap())
        .await
        .unwrap();
    workspaces
        .set_membership(&context, workspace.id(), &member, WorkspaceRole::Member)
        .await
        .unwrap();
    let provider = publish_provider(
        &providers,
        &context,
        workspace.id(),
        CREDENTIAL_FREE_OPENAPI,
        "weather",
        CredentialRoute::None,
    )
    .await;
    let before = providers
        .list_lifecycle_events(context.actor(), workspace.id())
        .await
        .unwrap();
    let unauthorized = change_context(member, 11, 'b');
    assert!(matches!(
        providers
            .change_account_setting_fixture(
                &unauthorized,
                workspace.id(),
                provider.id(),
                CredentialRoute::Caller
            )
            .await,
        Err(Error::Workspace(coral_workspace::Error::PermissionDenied(
            _
        )))
    ));
    assert!(matches!(
        providers
            .delete_provider(&unauthorized, workspace.id(), provider.id())
            .await,
        Err(Error::Workspace(coral_workspace::Error::PermissionDenied(
            _
        )))
    ));
    assert_eq!(
        providers
            .list_lifecycle_events(context.actor(), workspace.id())
            .await
            .unwrap(),
        before
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one scoped credential journey checks eligible owners and every rejected target"
)]
async fn fixed_routes_require_exact_provider_and_eligible_administrative_owner() {
    let (db, workspaces, providers) = test_modules().await;
    let owner =
        organization_user(db.database(), "user:fixed-owner", OrganizationRole::Member).await;
    let second =
        organization_user(db.database(), "user:fixed-second", OrganizationRole::Owner).await;
    let context = change_context(owner, 10, 'a');
    let admin = change_context(second.clone(), 10, 'b');
    let left = workspaces
        .create(&context, WorkspaceDisplayName::parse("Left").unwrap())
        .await
        .unwrap();
    let right = workspaces
        .create(&context, WorkspaceDisplayName::parse("Right").unwrap())
        .await
        .unwrap();
    for workspace in [&left, &right] {
        crate::tests::join_as_owner(&workspaces, &second, workspace.id()).await;
    }
    let directory = tempfile::tempdir().unwrap();
    let accounts = crate::tests::accounts_for(&providers, directory.path());
    let provider = providers
        .publish_shared_fixture(
            &admin,
            left.id(),
            crate::OpenApiDocument::parse(CREDENTIAL_REQUIRED_OPENAPI).unwrap(),
            "fixed",
            &accounts,
            coral_credentials::CredentialValues::token(Secret::new("fixed-secret".into()).unwrap()),
        )
        .await
        .unwrap();
    let other = publish_provider(
        &providers,
        &admin,
        left.id(),
        CREDENTIAL_REQUIRED_OPENAPI,
        "other",
        CredentialRoute::Caller,
    )
    .await;
    let foreign = publish_provider(
        &providers,
        &admin,
        right.id(),
        CREDENTIAL_REQUIRED_OPENAPI,
        "fixed",
        CredentialRoute::Caller,
    )
    .await;
    let account_id = accounts
        .get_account(
            &admin,
            left.id(),
            provider.id(),
            &crate::account::AccountScope::Shared,
        )
        .await
        .unwrap()
        .account
        .unwrap()
        .id;
    for (workspace, target) in [(&left, &other), (&right, &foreign)] {
        assert!(matches!(
            providers
                .change_account_setting_fixture(
                    &context,
                    workspace.id(),
                    target.id(),
                    CredentialRoute::Fixed
                )
                .await,
            Err(Error::AccountRequired)
        ));
    }
    assert!(
        !providers
            .list_lifecycle_events(context.actor(), right.id())
            .await
            .unwrap()
            .iter()
            .any(|event| event.action() == LifecycleAction::ProviderCredentialRouteChanged)
    );
    assert!(matches!(
        accounts
            .start_reconnect(
                &admin,
                right.id(),
                provider.id(),
                account_id,
                authorization_request(),
                None
            )
            .await,
        Err(Error::AccountNotFound)
    ));
    assert!(matches!(
        providers
            .change_account_setting_fixture(
                &context,
                left.id(),
                provider.id(),
                CredentialRoute::None
            )
            .await,
        Err(Error::AccountRequired)
    ));
    for route in [CredentialRoute::None, CredentialRoute::Fixed] {
        let setup = providers
            .prepare_openapi_fixture(
                &context,
                left.id(),
                crate::OpenApiDocument::parse(CREDENTIAL_REQUIRED_OPENAPI).unwrap(),
            )
            .await
            .unwrap();
        let publication = providers
            .publish_workspace_provider(
                &context,
                crate::WorkspaceProviderPublicationRequest::new(
                    authorization_request(),
                    left.id().clone(),
                    setup.id().clone(),
                    ProviderNamespace::parse("invalid").unwrap(),
                    route,
                ),
            )
            .await
            .unwrap();
        assert_eq!(
            publication.outcome(),
            crate::ProviderPublicationOutcome::Failed
        );
        assert_eq!(publication.diagnostic().unwrap().code(), "account_required");
    }
}

coral_test_support::provider_setup_fixtures!(crate);

/// Each Provider's account use for an Agent and its blockers, by namespace.
type AgentView = std::collections::BTreeMap<
    String,
    (
        (&'static str, Option<crate::account::ProviderAccountId>),
        Vec<ProviderBlocker>,
    ),
>;

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one inspection compares every account setting before and after the Agent connects"
)]
async fn agent_inspection_shows_each_settings_account_for_that_agent_only() {
    use coral_workspace::{AgentDisplayName, WorkspaceAction};
    let h = crate::tests::Harness::new().await;
    let owner = organization_user(
        h.database.clone(),
        "user:agent-inspection",
        OrganizationRole::Owner,
    )
    .await;
    let context = change_context(owner.clone(), 10, 'a');
    let workspace = h
        .workspaces
        .create(&context, WorkspaceDisplayName::parse("Agents").unwrap())
        .await
        .unwrap();
    let other_workspace = h
        .workspaces
        .create(&context, WorkspaceDisplayName::parse("Other").unwrap())
        .await
        .unwrap();
    let create_agent = async |workspace: &crate::WorkspaceId, name: &str| {
        h.workspaces
            .create_agent(
                &context,
                workspace,
                AgentDisplayName::parse(name).unwrap(),
                None,
            )
            .await
            .unwrap()
            .agent
    };
    let agent = create_agent(workspace.id(), "Worker").await;
    let foreign_agent = create_agent(other_workspace.id(), "Elsewhere").await;
    publish_provider(
        &h.providers,
        &context,
        workspace.id(),
        CREDENTIAL_FREE_OPENAPI,
        "open",
        CredentialRoute::None,
    )
    .await;
    let shared = h
        .providers
        .publish_shared_fixture(
            &context,
            workspace.id(),
            crate::OpenApiDocument::parse(CREDENTIAL_REQUIRED_OPENAPI).unwrap(),
            "shared",
            &h.accounts,
            coral_credentials::CredentialValues::token(
                Secret::new("shared-inspection-canary".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    let shared_account = h
        .accounts
        .get_account(
            &context,
            workspace.id(),
            shared.id(),
            &crate::account::AccountScope::Shared,
        )
        .await
        .unwrap()
        .account
        .unwrap()
        .id;
    let separate = publish_provider(
        &h.providers,
        &context,
        workspace.id(),
        CREDENTIAL_REQUIRED_OPENAPI,
        "separate",
        CredentialRoute::Caller,
    )
    .await;
    let connect = async |started: crate::account::Authorization| {
        h.accounts
            .submit_secret(
                &context,
                workspace.id(),
                started.id,
                coral_credentials::CredentialValues::token(
                    Secret::new("inspection-canary".into()).unwrap(),
                ),
            )
            .await
            .unwrap()
            .account_id
            .unwrap()
    };
    // The inspecting Owner's own Account never stands in for the Agent's.
    connect(
        h.accounts
            .start_user_authorization(
                &context,
                workspace.id(),
                separate.id(),
                authorization_request(),
                None,
            )
            .await
            .unwrap(),
    )
    .await;

    let inspect = async |actor: &coral_workspace::AuthenticatedUser,
                         agent: &coral_workspace::Agent|
           -> Result<AgentView, Error> {
        let mut tx = h.database.begin().await.unwrap();
        let evidence = h
            .workspaces
            .check_in_transaction(&mut tx, actor, workspace.id(), WorkspaceAction::Inspect)
            .await
            .unwrap();
        let inspected = inspect_agent_providers_in_transaction(&mut tx, &evidence, agent).await;
        tx.rollback().await.unwrap();
        Ok(inspected?
            .into_iter()
            .map(|inspection| {
                let account = match inspection.account {
                    AgentAccountUse::None => ("none", None),
                    AgentAccountUse::Shared(view) => {
                        ("shared", view.account.map(|account| account.id))
                    }
                    AgentAccountUse::Separate(view) => {
                        ("separate", view.account.map(|account| account.id))
                    }
                };
                (
                    inspection
                        .readiness
                        .provider
                        .namespace()
                        .as_str()
                        .to_owned(),
                    (account, inspection.readiness.blockers),
                )
            })
            .collect())
    };
    let mut expected = AgentView::from([
        ("open".to_owned(), (("none", None), vec![])),
        (
            "separate".to_owned(),
            (
                ("separate", None),
                vec![ProviderBlocker::CallerAccountRequired],
            ),
        ),
        (
            "shared".to_owned(),
            (("shared", Some(shared_account)), vec![]),
        ),
    ]);
    assert_eq!(inspect(&owner, &agent).await.unwrap(), expected);

    let agent_account = connect(
        h.accounts
            .start_agent_authorization(
                &context,
                workspace.id(),
                agent.principal().id(),
                separate.id(),
                authorization_request(),
                None,
            )
            .await
            .unwrap(),
    )
    .await;
    expected.insert(
        "separate".to_owned(),
        (("separate", Some(agent_account)), vec![]),
    );
    assert_eq!(inspect(&owner, &agent).await.unwrap(), expected);

    assert!(
        matches!(
            inspect(&owner, &foreign_agent).await,
            Err(Error::Workspace(coral_workspace::Error::AgentNotFound))
        ),
        "an Agent from another Workspace is not found"
    );
    let member = organization_user(
        h.database.clone(),
        "user:agent-inspection-member",
        OrganizationRole::Member,
    )
    .await;
    h.workspaces
        .set_membership(&context, workspace.id(), &member, WorkspaceRole::Member)
        .await
        .unwrap();
    assert!(
        matches!(
            inspect(&member, &agent).await,
            Err(Error::Workspace(coral_workspace::Error::PermissionDenied(
                _
            )))
        ),
        "only an Owner inspects Agent Providers"
    );
}
