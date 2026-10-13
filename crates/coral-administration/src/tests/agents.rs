//! Agent deletion: who may delete, and what it keeps.
use super::accounts::prepare_accounts;
use super::*;
use coral_provider::account::{AccountScope, AuthorizationState};

async fn registered_agent(
    workspaces: &WorkspaceModule,
    owner: &AuthenticatedUser,
    workspace: &WorkspaceId,
) -> AuthenticatedPrincipal {
    let issued = workspaces
        .create_agent(
            &change(owner),
            workspace,
            coral_workspace::AgentDisplayName::parse("Worker").unwrap(),
            None,
        )
        .await
        .unwrap();
    workspaces
        .authenticate_agent(workspace, issued.credential.expose(), 100)
        .await
        .unwrap()
}

/// Deleting an Agent removes its Account, but its completed authorization keeps
/// the Account it produced, and a sibling Agent's Account is untouched.
#[tokio::test]
async fn deleting_an_agent_keeps_authorization_history_and_sibling_accounts() {
    let db = fixture().await;
    let database = db.database();
    let workspaces = WorkspaceModule::new(database.clone());
    let owner = provision_user(database.clone(), "user:owner", OrganizationRole::Owner).await;
    let workspace = workspaces
        .create(
            &change(&owner),
            WorkspaceDisplayName::parse("Agents").unwrap(),
        )
        .await
        .unwrap();
    let scope = workspace.id().clone();
    let (provider, accounts, _key) =
        prepare_accounts(database.clone(), workspaces.clone(), &owner, &scope).await;
    let mut connected = Vec::new();
    for secret in ["deleted-credential-canary", "sibling-credential-canary"] {
        let agent = registered_agent(&workspaces, &owner, &scope).await;
        let started = accounts
            .start_agent_authorization(
                &change(&owner),
                &scope,
                agent.principal().id(),
                &provider,
                coral_provider::ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string())
                    .unwrap(),
                None,
            )
            .await
            .unwrap();
        let completed = accounts
            .submit_secret(
                &change(&owner),
                &scope,
                started.id,
                coral_credentials::CredentialValues::token(
                    coral_credentials::Secret::new(secret.into()).unwrap(),
                ),
            )
            .await
            .unwrap();
        assert_eq!(completed.state, AuthorizationState::Succeeded);
        connected.push((agent, completed));
    }
    let [(agent, completed), (sibling, sibling_completed)] = connected.as_slice() else {
        unreachable!()
    };
    let lifecycle = AccessLifecycleModule::new(database.clone(), workspaces.clone());
    lifecycle
        .delete_agent(&change(&owner), &scope, agent.principal().id())
        .await
        .unwrap();

    assert_eq!(
        accounts
            .get_authorization(&change(&owner), &scope, completed.id)
            .await
            .unwrap()
            .account_id,
        completed.account_id,
        "authorization history keeps the Account it produced"
    );
    assert_eq!(
        accounts
            .get_account(
                &change(&owner),
                &scope,
                &provider,
                &AccountScope::Agent(sibling.principal().id().clone())
            )
            .await
            .unwrap()
            .account
            .map(|account| account.id),
        sibling_completed.account_id,
        "a sibling Agent keeps its Account"
    );
    let events = coral_provider::ProviderModule::with_running_catalog(
        coral_outbound::DestinationPolicy::default(),
        database,
        workspaces,
    )
    .list_lifecycle_events(&owner, &scope)
    .await
    .unwrap();
    assert!(
        events
            .iter()
            .any(|event| event.action() == coral_provider::LifecycleAction::AccountDeleted)
    );
    assert!(!format!("{events:?}").contains("credential-canary"));
}

/// Only a current Owner of the Agent's own active Workspace deletes it, and
/// deletion leaves every other Agent in place.
#[tokio::test]
async fn agent_deletion_requires_current_owner_and_exact_workspace_without_affecting_other_agents()
{
    let db = fixture().await;
    let database = db.database();
    let workspaces = WorkspaceModule::new(database.clone());
    let left_owner = provision_user(database.clone(), "user:left", OrganizationRole::Member).await;
    let right_owner =
        provision_user(database.clone(), "user:right", OrganizationRole::Member).await;
    let administrator = provision_user(
        database.clone(),
        "user:administrator",
        OrganizationRole::Owner,
    )
    .await;
    let mut scopes = Vec::new();
    for (owner, name) in [(&left_owner, "Left"), (&right_owner, "Right")] {
        let workspace = workspaces
            .create(&change(owner), WorkspaceDisplayName::parse(name).unwrap())
            .await
            .unwrap();
        join_as_owner(&workspaces, &administrator, workspace.id()).await;
        scopes.push(workspace.id().clone());
    }
    let [left, right] = scopes.as_slice() else {
        unreachable!()
    };
    let agent = registered_agent(&workspaces, &left_owner, left).await;
    let sibling = registered_agent(&workspaces, &left_owner, left).await;
    let other = registered_agent(&workspaces, &right_owner, right).await;
    let lifecycle = AccessLifecycleModule::new(database, workspaces.clone());
    let context = change(&administrator);
    let before = (
        workspaces.list_agents(&administrator, left).await.unwrap(),
        workspaces.list_agents(&administrator, right).await.unwrap(),
    );

    let wrong_workspace = lifecycle
        .delete_agent(&context, right, agent.principal().id())
        .await;
    assert!(
        matches!(
            wrong_workspace,
            Err(Error::Workspace(coral_workspace::Error::AgentNotFound))
        ),
        "{wrong_workspace:?}"
    );
    let other_owner = lifecycle
        .delete_agent(&change(&right_owner), left, agent.principal().id())
        .await;
    assert!(
        matches!(
            other_owner,
            Err(Error::Workspace(coral_workspace::Error::WorkspaceNotFound))
        ),
        "{other_owner:?}"
    );
    workspaces.archive(&context, left).await.unwrap();
    let archived = lifecycle
        .delete_agent(&context, left, agent.principal().id())
        .await;
    assert!(
        matches!(
            archived,
            Err(Error::Workspace(coral_workspace::Error::WorkspaceArchived))
        ),
        "{archived:?}"
    );
    workspaces.restore(&context, left).await.unwrap();
    assert_eq!(
        (
            workspaces.list_agents(&administrator, left).await.unwrap(),
            workspaces.list_agents(&administrator, right).await.unwrap(),
        ),
        before,
        "denied deletions change nothing"
    );

    lifecycle
        .delete_agent(&context, left, agent.principal().id())
        .await
        .unwrap();
    let current = workspaces.list_agents(&administrator, left).await.unwrap();
    assert_eq!(
        current
            .iter()
            .map(coral_workspace::Agent::principal)
            .collect::<Vec<_>>(),
        [sibling.principal()]
    );
    assert_eq!(
        workspaces.list_agents(&administrator, right).await.unwrap(),
        before.1
    );
    workspaces
        .get_agent_registration(&administrator, right, other.principal().id())
        .await
        .unwrap();
}
