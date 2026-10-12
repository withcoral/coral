use super::tests::{change_context, organization_user, test_modules};

pub(super) fn authorization_request() -> crate::ManagementRequestId {
    crate::ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string())
        .expect("management request id")
}

pub(super) use crate::tests::publish_provider;

use crate::{
    OpenApiDocument,
    access::CredentialRoute,
    account::{AccountModule, AuthorizationState},
};
use coral_credentials::Secret;

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one deletion journey sets up every kind of credential state the cascade removes"
)]
async fn workspace_provider_deletion_removes_its_accounts_and_cancels_open_authorizations() {
    use crate::{
        LifecycleAction, ManagementRequestId, WorkspaceProviderPublicationRequest,
        access::ProviderNamespace,
    };
    let (db, workspaces, providers) = test_modules().await;
    let owner =
        organization_user(db.database(), "user:cascade-owner", OrganizationRole::Owner).await;
    let member = organization_user(
        db.database(),
        "user:cascade-member",
        OrganizationRole::Member,
    )
    .await;
    let admin = change_context(owner.clone(), 10, 'a');
    let context = change_context(member.clone(), 10, 'b');
    let workspace = workspaces
        .create(&admin, WorkspaceDisplayName::parse("Cascade").unwrap())
        .await
        .unwrap();
    workspaces
        .set_membership(
            &admin,
            workspace.id(),
            &member,
            coral_workspace::WorkspaceRole::Member,
        )
        .await
        .unwrap();
    let setup = providers
        .prepare_openapi_fixture(
            &admin,
            workspace.id(),
            OpenApiDocument::parse(CREDENTIAL_REQUIRED_OPENAPI).unwrap(),
        )
        .await
        .unwrap();
    let published = providers
        .publish_workspace_provider(
            &admin,
            WorkspaceProviderPublicationRequest::new(
                ManagementRequestId::parse("73000000-0000-4000-8000-000000000001").unwrap(),
                workspace.id().clone(),
                setup.id().clone(),
                ProviderNamespace::parse("cascade").unwrap(),
                CredentialRoute::Caller,
            ),
        )
        .await
        .unwrap()
        .published_provider()
        .unwrap()
        .clone();
    let provider = published.clone();
    let provider_access = published.access().clone();
    let scope = workspace.id().clone();
    let dir = tempfile::tempdir().unwrap();
    let accounts = crate::tests::accounts_for(&providers, dir.path());
    // A member's own scoped Account.
    let user_authorization = accounts
        .start_user_authorization(
            &context,
            workspace.id(),
            provider_access.id(),
            authorization_request(),
            None,
        )
        .await
        .unwrap();
    let user_account_id = accounts
        .submit_secret(
            &context,
            &scope,
            user_authorization.id,
            coral_credentials::CredentialValues::token(
                Secret::new("member-secret-canary".into()).unwrap(),
            ),
        )
        .await
        .unwrap()
        .account_id
        .unwrap();
    assert_eq!(
        accounts
            .get_account(
                &context,
                workspace.id(),
                provider_access.id(),
                &crate::account::AccountScope::User
            )
            .await
            .map(|view| view.account.map(|account| account.id))
            .unwrap(),
        Some(user_account_id)
    );
    // The owner's private Account is distinct from the member's Account.
    let owner_authorization = accounts
        .start_user_authorization(&admin, &scope, provider.id(), authorization_request(), None)
        .await
        .unwrap();
    let owner_account_id = accounts
        .submit_secret(
            &admin,
            &scope,
            owner_authorization.id,
            coral_credentials::CredentialValues::token(
                Secret::new("shared-secret-canary".into()).unwrap(),
            ),
        )
        .await
        .unwrap()
        .account_id
        .unwrap();
    // An authorization still waiting for its secret.
    let open = accounts
        .start_reconnect(
            &admin,
            &scope,
            provider.id(),
            owner_account_id,
            authorization_request(),
            None,
        )
        .await
        .unwrap();
    providers
        .delete_provider(&admin, &scope, provider.id())
        .await
        .unwrap();
    for (actor, account) in [(&context, user_account_id), (&admin, owner_account_id)] {
        assert!(matches!(
            accounts
                .start_reconnect(
                    actor,
                    &scope,
                    provider.id(),
                    account,
                    authorization_request(),
                    None
                )
                .await,
            Err(crate::Error::AccountNotFound)
        ));
    }
    assert_eq!(
        accounts
            .get_authorization(&admin, &scope, open.id)
            .await
            .unwrap()
            .state,
        AuthorizationState::Canceled
    );
    assert_eq!(
        providers
            .inspect_providers(&owner, workspace.id())
            .await
            .unwrap(),
        []
    );
    // A late secret replays the canceled outcome and creates nothing.
    let late = accounts
        .submit_secret(
            &admin,
            &scope,
            open.id,
            coral_credentials::CredentialValues::token(Secret::new("too-late".into()).unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(late.state, AuthorizationState::Canceled);
    assert_eq!(late.account_id, None);
    // Events of one transaction share a timestamp, so counts are asserted, not order.
    let events = providers
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .unwrap();
    let count = |action: LifecycleAction| {
        events
            .iter()
            .filter(|event| event.action() == action)
            .count()
    };
    assert_eq!(count(LifecycleAction::ProviderDeleted), 1);
    assert!(!format!("{events:?}").contains("secret-canary"));
    assert_eq!(count(LifecycleAction::AccountDeleted), 0);
    for (reader, own_account, other_account) in [
        (&context, user_account_id, owner_account_id),
        (&admin, owner_account_id, user_account_id),
    ] {
        let history = accounts
            .list_user_lifecycle_events(reader, workspace.id())
            .await
            .unwrap();
        let deleted = history
            .iter()
            .filter(|event| event.action() == LifecycleAction::AccountDeleted)
            .collect::<Vec<_>>();
        assert_eq!(deleted.len(), 1);
        assert_eq!(
            deleted.first().unwrap().target_id(),
            own_account.to_string()
        );
        assert!(!format!("{history:?}").contains(&other_account.to_string()));
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "scoped replay, conflicts, and lifecycle evidence form one authorization journey"
)]
async fn authorization_request_collision_preserves_workspace_scoping_and_exact_replay() {
    let (db, workspaces, providers) = test_modules().await;
    let owner = organization_user(
        db.database(),
        "user:request-collision",
        OrganizationRole::Owner,
    )
    .await;
    let context = change_context(owner.clone(), 10, 'a');
    let left = workspaces
        .create(&context, WorkspaceDisplayName::parse("Left").expect("name"))
        .await
        .expect("Workspace");
    let right = workspaces
        .create(
            &context,
            WorkspaceDisplayName::parse("Right").expect("name"),
        )
        .await
        .expect("Workspace");
    let left_scope = left.id().clone();
    let right_scope = right.id().clone();
    let provider = publish_provider(
        &providers,
        &context,
        left.id(),
        CREDENTIAL_REQUIRED_OPENAPI,
        "private",
        CredentialRoute::Caller,
    )
    .await;
    let right_provider = publish_provider(
        &providers,
        &context,
        right.id(),
        CREDENTIAL_REQUIRED_OPENAPI,
        "private",
        CredentialRoute::Caller,
    )
    .await;
    let directory = tempfile::tempdir().expect("filesystem credentials");
    let accounts = crate::tests::accounts_for(&providers, directory.path());
    let request = authorization_request();
    let (first, replay) = tokio::join!(
        accounts.start_user_authorization(
            &context,
            &left_scope,
            provider.id(),
            request.clone(),
            None
        ),
        accounts.start_user_authorization(
            &context,
            &left_scope,
            provider.id(),
            request.clone(),
            None
        ),
    );
    let first = first.expect("authorization");
    assert_eq!(replay.expect("concurrent exact replay"), first);
    let other = organization_user(
        db.database(),
        "user:other-request-actor",
        OrganizationRole::Owner,
    )
    .await;
    crate::tests::join_as_owner(&workspaces, &other, left.id()).await;
    let other_actor = change_context(other, 11, 'b');
    assert!(matches!(
        accounts
            .start_user_authorization(
                &other_actor,
                &left_scope,
                provider.id(),
                request.clone(),
                None
            )
            .await,
        Err(crate::Error::RequestIdConflict)
    ));
    assert!(matches!(
        accounts
            .get_authorization(&other_actor, &left_scope, first.id)
            .await,
        Err(crate::Error::AccountNotFound)
    ));
    assert!(matches!(
        accounts
            .start_secret(&context, &left_scope, provider.id(), request.clone())
            .await,
        Err(crate::Error::RequestIdConflict)
    ));
    let other_provider = publish_provider(
        &providers,
        &context,
        left.id(),
        CREDENTIAL_REQUIRED_OPENAPI,
        "different",
        CredentialRoute::Caller,
    )
    .await;
    assert!(matches!(
        accounts
            .start_user_authorization(
                &context,
                &left_scope,
                other_provider.id(),
                request.clone(),
                None
            )
            .await,
        Err(crate::Error::RequestIdConflict)
    ));
    let left_events = providers
        .list_lifecycle_events(&owner, left.id())
        .await
        .expect("events");
    let right_events = providers
        .list_lifecycle_events(&owner, right.id())
        .await
        .expect("events");
    let independent = accounts
        .start_user_authorization(
            &context,
            &right_scope,
            right_provider.id(),
            request.clone(),
            None,
        )
        .await
        .expect("the same request ID is independent in another Workspace");
    assert_ne!(independent.id, first.id);
    assert_eq!(
        accounts
            .start_user_authorization(
                &context,
                &right_scope,
                right_provider.id(),
                request.clone(),
                None
            )
            .await
            .expect("exact replay in the second Workspace"),
        independent
    );
    assert!(matches!(
        accounts
            .get_authorization(&context, &right_scope, first.id)
            .await,
        Err(crate::Error::AccountNotFound)
    ));
    assert_eq!(
        accounts
            .start_user_authorization(&context, &left_scope, provider.id(), request, None)
            .await
            .expect("original replay survives collision"),
        first
    );
    assert_eq!(
        providers
            .list_lifecycle_events(&owner, left.id())
            .await
            .expect("events"),
        left_events
    );
    assert_eq!(
        providers
            .list_lifecycle_events(&owner, right.id())
            .await
            .expect("events")
            .len(),
        right_events.len()
    );
    let separate = accounts
        .start_user_authorization(
            &context,
            &right_scope,
            right_provider.id(),
            authorization_request(),
            None,
        )
        .await
        .expect("independent request");
    assert_ne!(separate.id, first.id);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one User authorization journey covers atomic connection, concealment, and safe lifecycle evidence"
)]
async fn user_authorization_connects_only_its_user_and_never_satisfies_a_shared_route() {
    let (db, workspaces, providers) = test_modules().await;
    let owner =
        organization_user(db.database(), "user:caller-owner", OrganizationRole::Owner).await;
    let user = organization_user(
        db.database(),
        "user:caller-member",
        OrganizationRole::Member,
    )
    .await;
    let context = change_context(user.clone(), 10, 'a');
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Caller").unwrap())
        .await
        .unwrap();
    crate::tests::join_as_owner(&workspaces, &owner, workspace.id()).await;
    let admin = change_context(owner.clone(), 10, 'b');
    workspaces
        .set_membership(
            &admin,
            workspace.id(),
            &user,
            coral_workspace::WorkspaceRole::Member,
        )
        .await
        .unwrap();
    let provider = publish_provider(
        &providers,
        &admin,
        workspace.id(),
        CREDENTIAL_REQUIRED_OPENAPI,
        "caller",
        CredentialRoute::Caller,
    )
    .await;
    let provider_access = provider.access();
    let dir = tempfile::tempdir().unwrap();
    let accounts = crate::tests::accounts_for(&providers, dir.path());
    let authorization = accounts
        .start_user_authorization(
            &context,
            workspace.id(),
            provider_access.id(),
            authorization_request(),
            None,
        )
        .await
        .unwrap();
    let scope = workspace.id().clone();
    assert!(matches!(
        accounts
            .submit_secret(
                &admin,
                &scope,
                authorization.id,
                coral_credentials::CredentialValues::token(
                    Secret::new("not-the-user".into()).unwrap()
                )
            )
            .await,
        Err(crate::Error::AccountNotFound)
    ));
    let completed = accounts
        .submit_secret(
            &context,
            &scope,
            authorization.id,
            coral_credentials::CredentialValues::token(
                Secret::new("user-secret-canary".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    let account = completed.account_id.unwrap();
    assert_eq!(
        accounts
            .get_account(
                &context,
                workspace.id(),
                provider_access.id(),
                &crate::account::AccountScope::User
            )
            .await
            .map(|view| view.account.map(|account| account.id))
            .unwrap(),
        Some(account)
    );
    assert_eq!(
        accounts
            .get_account(
                &admin,
                workspace.id(),
                provider_access.id(),
                &crate::account::AccountScope::User
            )
            .await
            .map(|view| view.account.map(|account| account.id))
            .unwrap(),
        None
    );
    assert!(
        accounts
            .start_user_authorization(
                &context,
                workspace.id(),
                provider_access.id(),
                authorization_request(),
                None
            )
            .await
            .is_err(),
        "initial connection cannot implicitly replace a connected Account"
    );
    assert_eq!(
        accounts
            .get_account(
                &context,
                workspace.id(),
                provider_access.id(),
                &crate::account::AccountScope::User
            )
            .await
            .map(|view| view.account.map(|account| account.id))
            .unwrap(),
        Some(account),
        "Rejected implicit replacement must not change the scoped Account"
    );
    let other_workspace = workspaces
        .create(
            &context,
            WorkspaceDisplayName::parse("Another caller Workspace").unwrap(),
        )
        .await
        .unwrap();
    crate::tests::join_as_owner(&workspaces, &owner, other_workspace.id()).await;
    let other_provider_access = publish_provider(
        &providers,
        &admin,
        other_workspace.id(),
        CREDENTIAL_REQUIRED_OPENAPI,
        "another",
        CredentialRoute::Caller,
    )
    .await
    .access()
    .clone();
    assert_eq!(
        accounts
            .get_account(
                &context,
                other_workspace.id(),
                other_provider_access.id(),
                &crate::account::AccountScope::User
            )
            .await
            .map(|view| view.account.map(|account| account.id))
            .unwrap(),
        None
    );
    assert!(matches!(
        accounts
            .start_reconnect(
                &context,
                other_workspace.id(),
                other_provider_access.id(),
                account,
                authorization_request(),
                None
            )
            .await,
        Err(crate::Error::AccountNotFound)
    ));
    assert_eq!(
        accounts
            .get_account(
                &context,
                other_workspace.id(),
                other_provider_access.id(),
                &crate::account::AccountScope::User
            )
            .await
            .map(|view| view.account.map(|account| account.id))
            .unwrap(),
        None
    );
    assert!(matches!(
        accounts
            .get_authorization(&context, &other_workspace.id().clone(), authorization.id)
            .await,
        Err(crate::Error::AccountNotFound)
    ));
    assert!(matches!(
        accounts
            .start_reconnect(
                &admin,
                workspace.id(),
                provider_access.id(),
                account,
                authorization_request(),
                None
            )
            .await,
        Err(crate::Error::AccountNotFound)
    ));
    assert!(matches!(
        providers
            .change_account_setting_fixture(
                &admin,
                workspace.id(),
                provider_access.id(),
                CredentialRoute::Fixed
            )
            .await,
        Err(crate::Error::AccountRequired)
    ));
    assert!(matches!(
        accounts
            .start_reconnect(
                &admin,
                &scope,
                provider.id(),
                account,
                authorization_request(),
                None
            )
            .await,
        Err(crate::Error::AccountNotFound)
    ));
    let reconnect = accounts
        .start_reconnect(
            &context,
            &scope,
            provider.id(),
            account,
            authorization_request(),
            None,
        )
        .await
        .unwrap();
    let competing = accounts
        .start_reconnect(
            &context,
            &scope,
            provider.id(),
            account,
            authorization_request(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        accounts
            .submit_secret(
                &context,
                &scope,
                reconnect.id,
                coral_credentials::CredentialValues::token(
                    Secret::new("user-replacement-canary".into()).unwrap()
                )
            )
            .await
            .unwrap()
            .account_id,
        Some(account)
    );
    assert_eq!(
        accounts
            .submit_secret(
                &context,
                &scope,
                competing.id,
                coral_credentials::CredentialValues::token(
                    Secret::new("losing-replacement".into()).unwrap()
                )
            )
            .await
            .unwrap()
            .state,
        AuthorizationState::Failed
    );
    for (workspace, provider_access, expected) in [
        (workspace.id(), provider_access.id(), Some(account)),
        (other_workspace.id(), other_provider_access.id(), None),
    ] {
        assert_eq!(
            accounts
                .get_account(
                    &context,
                    workspace,
                    provider_access,
                    &crate::account::AccountScope::User
                )
                .await
                .map(|view| view.account.map(|account| account.id))
                .unwrap(),
            expected
        );
    }
    let events = accounts
        .list_user_lifecycle_events(&context, workspace.id())
        .await
        .unwrap();
    assert!(
        events
            .iter()
            .any(|event| event.action() == crate::LifecycleAction::AccountReconnected)
    );
    assert!(!format!("{events:?}{completed:?}").contains("user-secret-canary"));
    assert!(
        !providers
            .list_lifecycle_events(&owner, workspace.id())
            .await
            .unwrap()
            .iter()
            .any(|event| event.target_id() == account.to_string())
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one Agent provisioning journey checks ownership, exact Account scope, replay, and lifecycle attribution"
)]
async fn owner_provisions_an_agent_not_the_authorizing_human() {
    use coral_workspace::AgentDisplayName;
    let (db, workspaces, providers) = test_modules().await;
    let human = organization_user(
        db.database(),
        "user:agent-provisioner",
        OrganizationRole::Member,
    )
    .await;
    let context = change_context(human.clone(), 10, 'a');
    let workspace = workspaces
        .create(&context, WorkspaceDisplayName::parse("Agents").unwrap())
        .await
        .unwrap();
    let owner =
        organization_user(db.database(), "user:second-owner", OrganizationRole::Owner).await;
    crate::tests::join_as_owner(&workspaces, &owner, workspace.id()).await;
    let admin = change_context(owner.clone(), 11, 'b');
    let agent = workspaces
        .create_agent(
            &context,
            workspace.id(),
            AgentDisplayName::parse("Worker").unwrap(),
            None,
        )
        .await
        .unwrap();
    let other = workspaces
        .create_agent(
            &context,
            workspace.id(),
            AgentDisplayName::parse("Other").unwrap(),
            None,
        )
        .await
        .unwrap();
    let agent_id = agent.agent.principal().id();
    let provider = publish_provider(
        &providers,
        &admin,
        workspace.id(),
        CREDENTIAL_REQUIRED_OPENAPI,
        "caller",
        CredentialRoute::Caller,
    )
    .await;
    let provider_access = provider.access();
    let dir = tempfile::tempdir().unwrap();
    let accounts = crate::tests::accounts_for(&providers, dir.path());
    let request = authorization_request();
    let authorization = accounts
        .start_agent_authorization(
            &context,
            workspace.id(),
            agent_id,
            provider_access.id(),
            request.clone(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        accounts
            .start_agent_authorization(
                &context,
                workspace.id(),
                agent_id,
                provider_access.id(),
                request.clone(),
                None
            )
            .await
            .unwrap(),
        authorization
    );
    assert!(matches!(
        accounts
            .start_agent_authorization(
                &context,
                workspace.id(),
                other.agent.principal().id(),
                provider_access.id(),
                request,
                None
            )
            .await,
        Err(crate::Error::RequestIdConflict)
    ));
    let scope = workspace.id().clone();
    let competing = accounts
        .start_agent_authorization(
            &context,
            workspace.id(),
            agent_id,
            provider_access.id(),
            authorization_request(),
            None,
        )
        .await
        .unwrap();
    workspaces
        .set_membership(
            &admin,
            workspace.id(),
            &human,
            coral_workspace::WorkspaceRole::Member,
        )
        .await
        .unwrap();
    accounts
        .submit_secret(
            &context,
            &scope,
            authorization.id,
            coral_credentials::CredentialValues::token(Secret::new("unauthorized".into()).unwrap()),
        )
        .await
        .unwrap_err();
    accounts
        .get_authorization(&context, &scope, authorization.id)
        .await
        .unwrap_err();
    assert!(matches!(
        accounts
            .submit_secret(
                &admin,
                &scope,
                authorization.id,
                coral_credentials::CredentialValues::token(
                    Secret::new("wrong-actor".into()).unwrap()
                )
            )
            .await,
        Err(crate::Error::AccountNotFound)
    ));
    workspaces
        .set_membership(
            &admin,
            workspace.id(),
            &human,
            coral_workspace::WorkspaceRole::Owner,
        )
        .await
        .unwrap();
    let completed = accounts
        .submit_secret(
            &context,
            &scope,
            authorization.id,
            coral_credentials::CredentialValues::token(
                Secret::new("agent-secret-canary".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        accounts
            .submit_secret(
                &context,
                &scope,
                competing.id,
                coral_credentials::CredentialValues::token(
                    Secret::new("competing-first-account".into()).unwrap()
                )
            )
            .await
            .unwrap()
            .state,
        AuthorizationState::Failed
    );
    assert_eq!(
        accounts
            .get_authorization(&context, &scope, competing.id)
            .await
            .unwrap()
            .account_id,
        None
    );
    let account = completed.account_id.unwrap();
    // Any current Owner may replace the Agent's authorization; the
    // human who originally supplied the credentials gains no private ownership.
    let reconnect = accounts
        .start_reconnect(
            &admin,
            &scope,
            provider.id(),
            account,
            authorization_request(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        accounts
            .submit_secret(
                &admin,
                &scope,
                reconnect.id,
                coral_credentials::CredentialValues::token(
                    Secret::new("agent-replacement-canary".into()).unwrap()
                )
            )
            .await
            .unwrap()
            .account_id,
        Some(account)
    );
    assert_eq!(
        accounts
            .get_account(
                &admin,
                workspace.id(),
                provider_access.id(),
                &crate::account::AccountScope::Agent(agent_id.clone())
            )
            .await
            .map(|view| view.account.map(|account| account.id))
            .unwrap(),
        Some(account)
    );
    assert_eq!(
        accounts
            .get_account(
                &context,
                workspace.id(),
                provider_access.id(),
                &crate::account::AccountScope::User
            )
            .await
            .map(|view| view.account.map(|account| account.id))
            .unwrap(),
        None
    );
    let agent_account = accounts
        .get_account(
            &admin,
            workspace.id(),
            provider_access.id(),
            &crate::account::AccountScope::Agent(agent_id.clone()),
        )
        .await
        .unwrap()
        .account
        .unwrap();
    assert_eq!(
        agent_account.configured_by_user_id.as_ref(),
        Some(admin.actor().user_id())
    );
    assert!(
        accounts
            .get_account(
                &admin,
                workspace.id(),
                provider_access.id(),
                &crate::account::AccountScope::Agent(other.agent.principal().id().clone())
            )
            .await
            .unwrap()
            .account
            .is_none(),
        "another Agent never inherits these credentials"
    );
    assert!(matches!(
        providers
            .change_account_setting_fixture(
                &context,
                workspace.id(),
                provider_access.id(),
                CredentialRoute::Fixed
            )
            .await,
        Err(crate::Error::AccountRequired)
    ));
    let elsewhere = workspaces
        .create(&admin, WorkspaceDisplayName::parse("Elsewhere").unwrap())
        .await
        .unwrap();
    let foreign = publish_provider(
        &providers,
        &admin,
        elsewhere.id(),
        CREDENTIAL_REQUIRED_OPENAPI,
        "caller",
        CredentialRoute::Caller,
    )
    .await
    .access()
    .clone();
    assert!(matches!(
        accounts
            .start_agent_authorization(
                &admin,
                elsewhere.id(),
                agent_id,
                foreign.id(),
                authorization_request(),
                None
            )
            .await,
        Err(crate::Error::Workspace(
            coral_workspace::Error::AgentNotFound
        ))
    ));
    assert!(matches!(
        accounts
            .get_account(
                &admin,
                elsewhere.id(),
                provider_access.id(),
                &crate::account::AccountScope::Agent(agent_id.clone())
            )
            .await
            .map(|view| view.account.map(|account| account.id)),
        Err(crate::Error::Workspace(
            coral_workspace::Error::AgentNotFound
        ))
    ));
    assert!(matches!(
        accounts
            .start_reconnect(
                &admin,
                elsewhere.id(),
                foreign.id(),
                account,
                authorization_request(),
                None
            )
            .await,
        Err(crate::Error::AccountNotFound)
    ));
    assert!(matches!(
        accounts
            .start_reconnect(
                &admin,
                workspace.id(),
                foreign.id(),
                account,
                authorization_request(),
                None
            )
            .await,
        Err(crate::Error::AccountNotFound)
    ));
    let other_provider = publish_provider(
        &providers,
        &admin,
        workspace.id(),
        CREDENTIAL_REQUIRED_OPENAPI,
        "other_provider",
        CredentialRoute::Caller,
    )
    .await;
    let other_provider_access = other_provider.access();
    assert!(matches!(
        accounts
            .start_reconnect(
                &admin,
                workspace.id(),
                other_provider_access.id(),
                account,
                authorization_request(),
                None
            )
            .await,
        Err(crate::Error::AccountNotFound)
    ));
    let user_authorization = accounts
        .start_user_authorization(
            &context,
            workspace.id(),
            provider_access.id(),
            authorization_request(),
            None,
        )
        .await
        .unwrap();
    let user_account_id = accounts
        .submit_secret(
            &context,
            &scope,
            user_authorization.id,
            coral_credentials::CredentialValues::token(
                Secret::new("human-credential".into()).unwrap(),
            ),
        )
        .await
        .unwrap()
        .account_id
        .unwrap();
    assert!(matches!(
        accounts
            .start_reconnect(
                &admin,
                workspace.id(),
                provider_access.id(),
                user_account_id,
                authorization_request(),
                None
            )
            .await,
        Err(crate::Error::AccountNotFound)
    ));
    let another = accounts
        .start_reconnect(
            &admin,
            workspace.id(),
            provider_access.id(),
            account,
            authorization_request(),
            None,
        )
        .await
        .unwrap();
    let another_account_id = accounts
        .submit_secret(
            &admin,
            &scope,
            another.id,
            coral_credentials::CredentialValues::token(
                Secret::new("another-agent-account".into()).unwrap(),
            ),
        )
        .await
        .unwrap()
        .account_id
        .unwrap();
    assert_eq!(
        accounts
            .get_account(
                &admin,
                workspace.id(),
                provider_access.id(),
                &crate::account::AccountScope::Agent(agent_id.clone())
            )
            .await
            .map(|view| view.account.map(|account| account.id))
            .unwrap(),
        Some(account)
    );
    assert_eq!(
        another_account_id, account,
        "explicit replacement preserves the stable Account"
    );
    assert_eq!(
        accounts
            .get_account(
                &admin,
                workspace.id(),
                provider_access.id(),
                &crate::account::AccountScope::Agent(agent_id.clone())
            )
            .await
            .map(|view| view.account.map(|account| account.id))
            .unwrap(),
        Some(another_account_id)
    );
    // Losing the provisioning human's Owner role does not transfer or disable the Agent's Account.
    workspaces
        .set_membership(
            &admin,
            workspace.id(),
            &human,
            coral_workspace::WorkspaceRole::Member,
        )
        .await
        .unwrap();
    accounts
        .start_agent_authorization(
            &context,
            workspace.id(),
            agent_id,
            provider_access.id(),
            authorization_request(),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(
        accounts
            .get_account(
                &admin,
                workspace.id(),
                provider_access.id(),
                &crate::account::AccountScope::Agent(agent_id.clone())
            )
            .await
            .unwrap()
            .account
            .unwrap()
            .id,
        account
    );
    let other_authorization = accounts
        .start_agent_authorization(
            &admin,
            workspace.id(),
            other.agent.principal().id(),
            provider_access.id(),
            authorization_request(),
            None,
        )
        .await
        .unwrap();
    accounts
        .submit_secret(
            &admin,
            &scope,
            other_authorization.id,
            coral_credentials::CredentialValues::token(
                Secret::new("other-agent-secret".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    let events = providers
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .unwrap();
    for principal in [agent_id, other.agent.principal().id()] {
        let account = accounts
            .get_account(
                &admin,
                workspace.id(),
                provider_access.id(),
                &crate::account::AccountScope::Agent(principal.clone()),
            )
            .await
            .unwrap()
            .account
            .unwrap();
        assert!(
            events.iter().any(
                |event| event.action() == crate::LifecycleAction::AccountConnected
                    && event.target_id() == account.id.to_string()
            ),
            "each exact Agent Account has connection evidence"
        );
    }
    let authorized = events
        .iter()
        .find(|event| event.target_id() == account.to_string())
        .unwrap();
    assert_eq!(
        authorized.action(),
        crate::LifecycleAction::AccountConnected
    );
    assert_eq!(
        authorized.actor(),
        &crate::LifecycleActor::User {
            user_id: human.user_id().clone(),
            authority_source: coral_workspace::AuthoritySource::WorkspaceMembership
        }
    );
    assert!(
        events
            .iter()
            .any(|event| event.action() == crate::LifecycleAction::AccountReconnected)
    );
    assert!(!format!("{events:?}{completed:?}").contains("agent-secret-canary"));
}

pub(super) const CREDENTIAL_REQUIRED_OPENAPI: &str = r"openapi: 3.1.0
info:
  title: Private weather
  version: '1'
servers:
  - url: https://weather.example
security:
  - bearer: []
components:
  securitySchemes:
    bearer:
      type: http
      scheme: bearer
paths:
  /forecast:
    get:
      operationId: getPrivateForecast
      responses:
        '200':
          description: Forecast
";
use coral_workspace::{OrganizationRole, WorkspaceDisplayName};

#[tokio::test]
async fn oauth_callback_is_pinned_one_use_and_survives_request_loss() {
    oauth_callback_journey(false).await;
}

#[tokio::test]
async fn owner_oauth_connects_only_the_exact_agent_account() {
    oauth_callback_journey(true).await;
}

#[expect(
    clippy::too_many_lines,
    reason = "one real OAuth journey proves callback validation, disconnection survival, replay, and safe evidence"
)]
async fn oauth_callback_journey(provision_agent: bool) {
    use crate::account::OAuthClient;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{body_string_contains, method, path},
    };
    let server = MockServer::start().await;
    let (db, workspaces, providers) = test_modules().await;
    let owner = organization_user(db.database(), "user:oauth-owner", OrganizationRole::Owner).await;
    let context = change_context(owner.clone(), 10, 'a');
    let workspace = workspaces
        .create(
            &context,
            WorkspaceDisplayName::parse("OAuth fixed").expect("name"),
        )
        .await
        .expect("Workspace");
    let scope = workspace.id().clone();
    let document = serde_json::json!({
        "openapi":"3.1.0", "info":{"title":"OAuth","version":"1"},
        "servers":[{"url":server.uri()}], "security":[{"oauth":["read"]}],
        "components":{"securitySchemes":{"oauth":{"type":"oauth2","flows":{"authorizationCode":{
            "authorizationUrl":format!("{}/authorize",server.uri()), "tokenUrl":format!("{}/token",server.uri()), "scopes":{"read":"Read"}
        }}}}},
        "paths":{"/data":{"get":{"operationId":"getData","responses":{"200":{"description":"Data"}}}}}
    });
    let provider = publish_provider(
        &providers,
        &context,
        workspace.id(),
        &document.to_string(),
        "agent_oauth",
        CredentialRoute::Caller,
    )
    .await;
    let directory = tempfile::tempdir().expect("filesystem storage");
    let gate = std::sync::Arc::new(crate::account::OAuthExchangeGate::default());
    let accounts =
        crate::tests::accounts_for(&providers, directory.path()).with_exchange_gate(gate.clone());
    let agent_target = if provision_agent {
        let agent = workspaces
            .create_agent(
                &context,
                workspace.id(),
                coral_workspace::AgentDisplayName::parse("OAuth Agent").unwrap(),
                None,
            )
            .await
            .unwrap();
        let provider_access = provider.access();
        Some((agent.agent, provider_access))
    } else {
        None
    };
    let pending = if let Some((agent, provider_access)) = &agent_target {
        accounts
            .start_agent_authorization(
                &context,
                workspace.id(),
                agent.principal().id(),
                provider_access.id(),
                authorization_request(),
                Some(
                    OAuthClient::new(
                        "registered-public-client".into(),
                        "https://reef.example/callback".into(),
                    )
                    .unwrap(),
                ),
            )
            .await
            .unwrap()
    } else {
        accounts
            .start_user_authorization(
                &context,
                &scope,
                provider.id(),
                authorization_request(),
                Some(
                    OAuthClient::new(
                        "registered-public-client".into(),
                        "https://reef.example/callback".into(),
                    )
                    .expect("client"),
                ),
            )
            .await
            .expect("start OAuth")
    };
    let navigation =
        url::Url::parse(pending.authorization_url.as_deref().expect("navigation")).expect("URL");
    let params = navigation
        .query_pairs()
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(
        params
            .get("code_challenge_method")
            .map(std::borrow::Cow::as_ref),
        Some("S256")
    );
    assert_eq!(
        params.get("scope").map(std::borrow::Cow::as_ref),
        Some("read")
    );
    let state = params.get("state").expect("state").to_string();
    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("state", &state)
        .append_pair("code", "code-canary")
        .finish();
    assert!(matches!(
        accounts
            .complete_callback(&context, "state=account.forged&code=code-canary")
            .await,
        Err(crate::Error::AccountInput)
    ));
    assert_invalid_callbacks(&accounts, &context, &state).await;
    // Each token is within the individual credential limit, but the complete
    // document exceeds it while remaining within the OAuth response bound.
    let token_response = serde_json::json!({
        "access_token": format!("oauth-token-canary{}", "a".repeat(8 * 1024)),
        "token_type": "Bearer",
        "refresh_token": format!("refresh-canary{}", "r".repeat(9 * 1024)),
        "scope": "read extra",
    });
    assert!((16 * 1024 + 1..=64 * 1024).contains(&token_response.to_string().len()));
    Mock::given(method("POST"))
        .and(path("/token"))
        .and(body_string_contains("grant_type=authorization_code"))
        .and(body_string_contains("client_id=registered-public-client"))
        .and(body_string_contains("code_verifier="))
        .respond_with(ResponseTemplate::new(200).set_body_json(token_response))
        .expect(1)
        .mount(&server)
        .await;
    let module = accounts.clone();
    let callback_context = context.clone();
    let callback_query = query.clone();
    let callback = tokio::spawn(async move {
        module
            .complete_callback(&callback_context, &callback_query)
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), gate.entered.notified())
        .await
        .expect("exchange accepted");
    assert_eq!(
        accounts
            .get_authorization(&context, &scope, pending.id)
            .await
            .expect("progress")
            .state,
        AuthorizationState::Exchanging
    );
    callback.abort();
    gate.release.notify_one();
    let completed = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let progress = accounts
                .get_authorization(&context, &scope, pending.id)
                .await
                .expect("progress");
            if !matches!(
                progress.state,
                AuthorizationState::Pending | AuthorizationState::Exchanging
            ) {
                break progress;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("disconnection did not cancel authorization");
    assert_eq!(completed.state, AuthorizationState::Succeeded);
    let replay = accounts
        .complete_callback(&context, &query)
        .await
        .expect("terminal replay");
    assert_eq!(
        replay,
        crate::account::CallbackCompletion::Account {
            owner: scope.clone(),
            authorization_id: pending.id,
        }
    );
    if let Some((agent, provider_access)) = &agent_target {
        assert_eq!(
            accounts
                .get_account(
                    &context,
                    workspace.id(),
                    provider_access.id(),
                    &crate::account::AccountScope::Agent(agent.principal().id().clone())
                )
                .await
                .map(|view| view.account.map(|account| account.id))
                .unwrap(),
            completed.account_id
        );
        assert_eq!(
            accounts
                .get_account(
                    &context,
                    workspace.id(),
                    provider_access.id(),
                    &crate::account::AccountScope::User
                )
                .await
                .map(|view| view.account.map(|account| account.id))
                .unwrap(),
            None
        );
    }
    let events = providers
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .expect("events");
    let safe = format!("{events:?}{completed:?}");
    for secret in ["oauth-token-canary", "refresh-canary", "code-canary"] {
        assert!(!safe.contains(secret));
    }
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one durable secret-entry journey covers restart, replay, cancellation, expiry, and safe evidence"
)]
async fn shared_authorization_survives_restart_and_preserves_account_without_exposing_secrets() {
    let (db, workspaces, providers) = test_modules().await;
    let owner = organization_user(db.database(), "user:fixed-owner", OrganizationRole::Owner).await;
    let context = change_context(owner.clone(), 10, 'a');
    let workspace = workspaces
        .create(
            &context,
            WorkspaceDisplayName::parse("Fixed credentials").expect("name"),
        )
        .await
        .expect("Workspace");
    let scope = workspace.id().clone();
    let directory = tempfile::tempdir().expect("filesystem credential storage");
    let accounts = crate::tests::accounts_for(&providers, directory.path());
    let provider = providers
        .publish_shared_fixture(
            &context,
            &scope,
            OpenApiDocument::parse(CREDENTIAL_REQUIRED_OPENAPI).unwrap(),
            "private",
            &accounts,
            coral_credentials::CredentialValues::token(Secret::new("setup-canary".into()).unwrap()),
        )
        .await
        .unwrap();
    let original = accounts
        .get_account(
            &context,
            &scope,
            provider.id(),
            &crate::account::AccountScope::Shared,
        )
        .await
        .unwrap()
        .account
        .unwrap();
    accounts
        .disconnect(&context, &scope, provider.id(), original.id)
        .await
        .unwrap();
    let request = authorization_request();
    let authorization = accounts
        .start_secret(&context, &scope, provider.id(), request.clone())
        .await
        .expect("start");
    assert_eq!(authorization.state, AuthorizationState::Pending);
    drop(accounts);
    let accounts = crate::tests::accounts_for(&providers, directory.path());
    assert_eq!(
        accounts
            .get_authorization(&context, &scope, authorization.id)
            .await
            .expect("durable authorization"),
        authorization
    );
    assert_eq!(
        accounts
            .start_secret(&context, &scope, provider.id(), request.clone())
            .await
            .expect("recover lost start response"),
        authorization
    );
    let other = workspaces
        .create(
            &context,
            WorkspaceDisplayName::parse("Other fixed scope").unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        accounts
            .start_secret(&context, other.id(), provider.id(), request)
            .await,
        Err(crate::Error::ProviderNotFound)
    ));
    let completed = accounts
        .submit_secret(
            &context,
            &scope,
            authorization.id,
            coral_credentials::CredentialValues::token(
                Secret::new("canary-fixed-credential".into()).expect("secret"),
            ),
        )
        .await
        .expect("authorize");
    let account_id = completed.account_id.expect("Account");
    let provider_access = providers
        .change_account_setting_fixture(
            &context,
            workspace.id(),
            provider.id(),
            CredentialRoute::Fixed,
        )
        .await
        .unwrap();
    assert_eq!(provider_access.credential_route(), CredentialRoute::Fixed);
    let account = accounts
        .get_account(
            &context,
            &scope,
            provider.id(),
            &crate::account::AccountScope::Shared,
        )
        .await
        .unwrap()
        .account
        .unwrap();
    assert_eq!(account.id, original.id);
    assert_eq!(account.management_scope, scope);
    let request = authorization_request();
    let reconnect = accounts
        .start_reconnect(
            &context,
            &scope,
            provider.id(),
            account_id,
            request.clone(),
            None,
        )
        .await
        .expect("explicit reconnect");
    assert_eq!(
        accounts
            .start_reconnect(&context, &scope, provider.id(), account_id, request, None)
            .await
            .unwrap(),
        reconnect
    );
    let replaced = accounts
        .submit_secret(
            &context,
            &scope,
            reconnect.id,
            coral_credentials::CredentialValues::token(
                Secret::new("replacement-canary".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(replaced.account_id, Some(account_id));
    assert_eq!(
        accounts
            .get_account(
                &context,
                &scope,
                provider.id(),
                &crate::account::AccountScope::Shared
            )
            .await
            .unwrap()
            .account
            .unwrap(),
        account,
        "reconnect preserves the Account identifier and administrative ownership"
    );
    let provider_inspections = providers
        .inspect_providers(&owner, workspace.id())
        .await
        .unwrap();
    let current = provider_inspections
        .iter()
        .find(|item| item.id() == provider_access.id())
        .unwrap();
    assert_eq!(current.credential_route(), CredentialRoute::Fixed);
    assert_eq!(current.account_setting_version(), provider_access.version());
    assert!(matches!(
        providers
            .change_account_setting_fixture(
                &context,
                workspace.id(),
                provider_access.id(),
                CredentialRoute::None
            )
            .await,
        Err(crate::Error::AccountRequired)
    ));
    let replay = accounts
        .submit_secret(
            &context,
            &scope,
            authorization.id,
            coral_credentials::CredentialValues::token(
                Secret::new("not-a-replacement".into()).expect("secret"),
            ),
        )
        .await
        .expect("exact terminal result");
    assert_eq!(replay, completed);
    let pending = accounts
        .start_reconnect(
            &context,
            &scope,
            provider.id(),
            account_id,
            authorization_request(),
            None,
        )
        .await
        .expect("second authorization");
    assert_eq!(
        accounts
            .cancel(&context, &scope, pending.id)
            .await
            .expect("cancel")
            .state,
        AuthorizationState::Canceled
    );
    assert_eq!(
        accounts
            .submit_secret(
                &context,
                &scope,
                pending.id,
                coral_credentials::CredentialValues::token(
                    Secret::new("unused".into()).expect("secret")
                )
            )
            .await
            .expect("canceled terminal result")
            .account_id,
        None
    );
    let pending = accounts
        .start_reconnect(
            &context,
            &scope,
            provider.id(),
            account_id,
            authorization_request(),
            None,
        )
        .await
        .expect("expiring authorization");
    let late = change_context(owner.clone(), pending.expires_at_unix_nanos, 'b');
    assert_eq!(
        accounts
            .get_authorization(&late, &scope, pending.id)
            .await
            .expect("expire")
            .state,
        AuthorizationState::Expired
    );
    assert!(matches!(
        accounts
            .start_secret(
                &change_context(
                    organization_user(db.database(), "user:unprivileged", OrganizationRole::Member)
                        .await,
                    10,
                    'a'
                ),
                &scope,
                provider.id(),
                authorization_request()
            )
            .await,
        Err(crate::Error::Workspace(_))
    ));
    let events = providers
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .expect("safe events");
    assert!(!format!("{completed:?}{account:?}{events:?}").contains("canary-fixed-credential"));
    assert!(
        events
            .iter()
            .any(|event| event.action() == crate::LifecycleAction::AccountConnected)
    );
}

coral_test_support::provider_setup_fixtures!(crate);

async fn assert_invalid_callbacks(
    accounts: &AccountModule,
    context: &coral_workspace::ChangeContext,
    state: &str,
) {
    let valid = format!("state={state}&code=code-canary");
    for query in [
        String::new(),
        format!("state={state}"),
        "code=code-canary".into(),
        "state=&code=code-canary".into(),
        format!("state={state}&code="),
        format!("{valid}&state={state}"),
        format!("{valid}&%63ode=duplicate"),
        format!("{valid}&error=access_denied"),
        format!("{valid}&error="),
        format!("state={}&code=code-canary", "x".repeat(1025)),
        format!("state={state}&code={}", "x".repeat(4097)),
        format!("{valid}&extra={}", "x".repeat(16 * 1024)),
    ] {
        assert!(matches!(
            accounts.complete_callback(context, &query).await,
            Err(crate::Error::AccountInput)
        ));
    }
}
