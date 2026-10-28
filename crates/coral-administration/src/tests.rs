mod accounts;
mod agents;
mod builtin_catalog;
mod owner_transfer;
mod providers;
mod provisional;
mod repository;
mod search_eval;
mod setup;
mod workspaces;

use super::*;
use coral_functions::{
    SavedFunctionChangeContext, SavedFunctionDescription, SavedFunctionModule, SavedFunctionPath,
    SavedFunctionSave, SavedFunctionSource,
};
use coral_test_support::provision_user;
use coral_workspace::{
    AuthenticatedPrincipal, AuthenticatedUser, OrganizationRole, TraceId, WorkspaceDisplayName,
};
use repository::{EventFault, fixture};

/// One change context per injected lifecycle-event failure. Each fails the owning
/// domain's real insert, so the whole removal must roll back on both databases.
fn failure_contexts(actor: &AuthenticatedUser, events: &[EventFault]) -> Vec<ChangeContext> {
    events.iter().map(|event| event.context(actor)).collect()
}

/// Exercise the same removal through the standalone wrapper and caller-owned facet.
#[derive(Clone, Copy)]
enum RemovalAttempt {
    CallerRollback,
    CallerFailure(EventFault),
    StandaloneFailure(EventFault),
}

impl RemovalAttempt {
    fn cases(events: &[EventFault]) -> Vec<Self> {
        std::iter::once(Self::CallerRollback)
            .chain(
                events.iter().flat_map(|event| {
                    [Self::StandaloneFailure(*event), Self::CallerFailure(*event)]
                }),
            )
            .collect()
    }

    async fn assert_rolled_back(
        self,
        lifecycle: &AccessLifecycleModule,
        database: &Database,
        actor: &AuthenticatedUser,
        target: AccessRemovalTarget<'_>,
    ) {
        let fault = match self {
            Self::CallerRollback => None,
            Self::CallerFailure(event) | Self::StandaloneFailure(event) => Some(event),
        };
        let context = fault.map_or_else(|| change(actor), |event| event.context(actor));
        let result = if matches!(self, Self::StandaloneFailure(_)) {
            match target {
                AccessRemovalTarget::WorkspaceMember(workspace, user) => {
                    lifecycle
                        .remove_workspace_member(&context, workspace, user)
                        .await
                }
                AccessRemovalTarget::OrganizationMember {
                    user,
                    revision,
                    transfer_to,
                } => {
                    lifecycle
                        .remove_organization_member(&context, user, revision, transfer_to)
                        .await
                }
                AccessRemovalTarget::Agent(workspace, agent) => {
                    lifecycle.delete_agent(&context, workspace, agent).await
                }
            }
        } else {
            let mut tx = database.begin().await.unwrap();
            let result = lifecycle
                .apply_in_transaction(&mut tx, &context, target)
                .await;
            tx.rollback().await.unwrap();
            result
        };
        if let Some(fault) = fault {
            fault.assert_error(&result.unwrap_err());
        } else {
            result.unwrap();
        }
    }
}

fn change(user: &AuthenticatedUser) -> ChangeContext {
    coral_test_support::change(user.clone(), 100, '1')
}

/// Gives an Organization Owner a designated Owner Membership through the
/// governance surface. Without one they have no authority inside the Workspace.
async fn join_as_owner(
    workspaces: &WorkspaceModule,
    user: &AuthenticatedUser,
    workspace: &WorkspaceId,
) {
    workspaces
        .assign_owner(&change(user), workspace, user.user_id())
        .await
        .unwrap();
}

/// One bearer-token Tool, so every published Provider needs an Account.
const BEARER_OPENAPI: &str = r#"{"openapi":"3.0.3","info":{"title":"Private","version":"1"},"servers":[{"url":"https://example.com"}],"security":[{"token":[]}],"components":{"securitySchemes":{"token":{"type":"http","scheme":"bearer"}}},"paths":{"/value":{"get":{"operationId":"value","responses":{"200":{"description":"OK"}}}}}}"#;
/// An Organization Owner keeps nothing when their Workspace Membership ends, and
/// the sole designated Owner cannot leave until another Owner exists.
#[tokio::test]
async fn organization_owner_leaving_loses_current_functions_once_another_owner_exists() {
    let db = fixture().await;
    let database = db.database();
    let workspaces = WorkspaceModule::new(database.clone());
    let owner = provision_user(database.clone(), "user:owner", OrganizationRole::Member).await;
    let workspace = workspaces
        .create(
            &change(&owner),
            WorkspaceDisplayName::parse("Workspace").unwrap(),
        )
        .await
        .unwrap();
    let functions = SavedFunctionModule::new(
        database.clone(),
        workspaces.clone(),
        coral_functions::IsolateLimits::default(),
    )
    .unwrap();
    let path = SavedFunctionPath::parse("answer").unwrap();
    let administrator = provision_user(
        database.clone(),
        "user:administrator",
        OrganizationRole::Owner,
    )
    .await;
    coral_workspace::OrganizationModule::new(database.clone())
        .set_role(
            &change(&administrator),
            owner.user_id(),
            1,
            OrganizationRole::Owner,
        )
        .await
        .unwrap();
    let owner =
        AuthenticatedUser::organization_member(owner.user_id().clone(), OrganizationRole::Owner);
    let author = AuthenticatedPrincipal::user(owner.clone());
    save_answer(&functions, &author, workspace.id()).await;
    let state = async || {
        (
            workspaces
                .list_memberships(&owner, workspace.id())
                .await
                .unwrap(),
            workspaces
                .list_lifecycle_events(&owner, workspace.id())
                .await
                .unwrap(),
            functions.get(&author, workspace.id(), &path).await.unwrap(),
            functions
                .list_lifecycle_events(&author, workspace.id())
                .await
                .unwrap(),
        )
    };
    let before = state().await;
    let lifecycle = AccessLifecycleModule::new(database, workspaces.clone());

    let denied = lifecycle
        .remove_workspace_member(&change(&owner), workspace.id(), owner.user_id())
        .await;
    assert!(
        matches!(
            denied,
            Err(Error::Workspace(coral_workspace::Error::LastWorkspaceOwner))
        ),
        "{denied:?}"
    );
    assert_eq!(state().await, before, "a denial changes nothing");

    join_as_owner(&workspaces, &administrator, workspace.id()).await;
    lifecycle
        .remove_workspace_member(&change(&owner), workspace.id(), owner.user_id())
        .await
        .unwrap();
    // Organization Owner authority gives no way back into the Workspace.
    assert!(matches!(
        workspaces.list_memberships(&owner, workspace.id()).await,
        Err(coral_workspace::Error::WorkspaceNotFound)
    ));
    assert!(
        workspaces
            .list_memberships(&administrator, workspace.id())
            .await
            .unwrap()
            .iter()
            .all(|membership| membership.user_id() != owner.user_id())
    );
    // A new Membership cannot restore the deleted definition.
    join_as_owner(&workspaces, &owner, workspace.id()).await;
    assert!(matches!(
        functions.get(&author, workspace.id(), &path).await,
        Err(coral_functions::Error::SavedFunctionNotFound)
    ));
}

coral_test_support::provider_setup_fixtures!(coral_provider);

/// A Workspace Member with a connected personal Account, a pending reconnect, and one
/// current Saved Function, in a Workspace an Organization Owner created. The Member's
/// Organization role must not change what removal cleans up.
struct MemberRemoval {
    db: coral_test_support::TestDatabase,
    workspaces: WorkspaceModule,
    lifecycle: AccessLifecycleModule,
    functions: SavedFunctionModule,
    owner: AuthenticatedUser,
    member: AuthenticatedUser,
    author: AuthenticatedPrincipal,
    workspace: coral_workspace::Workspace,
    connected: accounts::ConnectedScope,
}

impl MemberRemoval {
    async fn new() -> Self {
        Self::with_member_role(OrganizationRole::Member).await
    }

    async fn with_member_role(role: OrganizationRole) -> Self {
        let db = fixture().await;
        let database = db.database();
        let workspaces = WorkspaceModule::new(database.clone());
        let owner = provision_user(database.clone(), "user:owner", OrganizationRole::Owner).await;
        let member = provision_user(database.clone(), "user:member", role).await;
        let workspace = workspaces
            .create(
                &change(&owner),
                WorkspaceDisplayName::parse("Workspace").unwrap(),
            )
            .await
            .unwrap();
        workspaces
            .set_membership(
                &change(&owner),
                workspace.id(),
                &member,
                coral_workspace::WorkspaceRole::Member,
            )
            .await
            .unwrap();
        let connected = accounts::ConnectedScope::new(
            database.clone(),
            workspaces.clone(),
            &owner,
            &member,
            workspace.id(),
        )
        .await;
        let functions = SavedFunctionModule::new(
            database.clone(),
            workspaces.clone(),
            coral_functions::IsolateLimits::default(),
        )
        .unwrap();
        let author = AuthenticatedPrincipal::user(member.clone());
        save_answer(&functions, &author, workspace.id()).await;
        Self {
            db,
            lifecycle: AccessLifecycleModule::new(database, workspaces.clone()),
            workspaces,
            functions,
            owner,
            member,
            author,
            workspace,
            connected,
        }
    }

    async fn remove(&self) {
        self.lifecycle
            .remove_workspace_member(
                &change(&self.owner),
                self.workspace.id(),
                self.member.user_id(),
            )
            .await
            .unwrap();
    }

    async fn rejoin(&self) {
        self.workspaces
            .set_membership(
                &change(&self.owner),
                self.workspace.id(),
                &self.member,
                coral_workspace::WorkspaceRole::Member,
            )
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn failed_member_removal_changes_no_credentials_functions_or_events() {
    let f = MemberRemoval::new().await;
    let path = SavedFunctionPath::parse("answer").unwrap();
    let saved = f
        .functions
        .get(&f.author, f.workspace.id(), &path)
        .await
        .unwrap();
    let function_events = f
        .functions
        .list_lifecycle_events(&f.author, f.workspace.id())
        .await
        .unwrap();
    let before = f
        .workspaces
        .list_lifecycle_events(&f.owner, f.workspace.id())
        .await
        .unwrap();
    let memberships = f
        .workspaces
        .list_memberships(&f.owner, f.workspace.id())
        .await
        .unwrap();
    let attempts = RemovalAttempt::cases(&[
        EventFault::AuthorizationCompleted,
        EventFault::AccountDeleted,
        EventFault::FunctionDeleted,
        EventFault::MembershipRemoved,
    ]);
    for attempt in attempts {
        attempt
            .assert_rolled_back(
                &f.lifecycle,
                &f.db.database(),
                &f.owner,
                AccessRemovalTarget::WorkspaceMember(f.workspace.id(), f.member.user_id()),
            )
            .await;
        assert_eq!(
            f.workspaces
                .list_memberships(&f.owner, f.workspace.id())
                .await
                .unwrap(),
            memberships
        );
        f.connected.assert_unchanged(&f.member).await;
        assert_eq!(
            f.functions
                .get(&f.author, f.workspace.id(), &path)
                .await
                .unwrap(),
            saved
        );
        assert_eq!(
            f.functions
                .list_lifecycle_events(&f.author, f.workspace.id())
                .await
                .unwrap(),
            function_events
        );
        assert_eq!(
            f.workspaces
                .list_lifecycle_events(&f.owner, f.workspace.id())
                .await
                .unwrap(),
            before
        );
    }
}

/// Removal always ends access there: an Organization Owner keeps no more of
/// their private work than an ordinary Member does.
#[tokio::test]
async fn member_removal_retires_credentials_pending_connections_and_functions() {
    for role in [OrganizationRole::Member, OrganizationRole::Owner] {
        member_removal_retires_current_work(role).await;
    }
}

async fn member_removal_retires_current_work(role: OrganizationRole) {
    use coral_provider::account::AuthorizationState;
    let f = MemberRemoval::with_member_role(role).await;
    f.remove().await;
    f.rejoin().await;
    f.connected.assert_retired(&f.member).await;
    assert!(matches!(
        f.functions
            .get(
                &f.author,
                f.workspace.id(),
                &SavedFunctionPath::parse("answer").unwrap()
            )
            .await,
        Err(coral_functions::Error::SavedFunctionNotFound)
    ));
    let deletions = f
        .functions
        .list_lifecycle_events(&f.author, f.workspace.id())
        .await
        .unwrap();
    assert!(deletions.iter().any(|event| event.action()
        == coral_functions::LifecycleAction::Deleted
        && event.actor().id().as_str() == f.owner.user_id().as_str()));
    assert!(
        f.functions
            .list_lifecycle_events(
                &AuthenticatedPrincipal::user(f.owner.clone()),
                f.workspace.id()
            )
            .await
            .unwrap()
            .is_empty(),
        "the remover gains no private deletion evidence"
    );
    // A secret that arrives after removal cannot revive the canceled connection.
    assert_eq!(
        f.connected
            .accounts
            .submit_secret(
                &change(&f.member),
                f.workspace.id(),
                f.connected.pending,
                coral_credentials::CredentialValues::token(
                    coral_credentials::Secret::new("late-secret".into()).unwrap()
                )
            )
            .await
            .unwrap()
            .state,
        AuthorizationState::Canceled
    );
}

#[tokio::test]
async fn rejoined_member_connects_a_fresh_account_and_cannot_reconnect_the_old_one() {
    use coral_provider::account::AccountScope;
    let f = MemberRemoval::new().await;
    let accounts = &f.connected.accounts;
    f.remove().await;
    f.rejoin().await;
    assert!(matches!(
        accounts
            .start_reconnect(
                &change(&f.member),
                f.workspace.id(),
                &f.connected.provider,
                f.connected.account,
                accounts::request(),
                None
            )
            .await,
        Err(coral_provider::Error::AccountNotFound)
    ));
    let rejoined = accounts
        .start_user_authorization(
            &change(&f.member),
            f.workspace.id(),
            &f.connected.provider,
            accounts::request(),
            None,
        )
        .await
        .unwrap();
    let fresh = accounts
        .get_account(
            &change(&f.member),
            f.workspace.id(),
            &f.connected.provider,
            &AccountScope::User,
        )
        .await
        .unwrap()
        .account
        .unwrap();
    assert_ne!(fresh.id, f.connected.account);
    assert_eq!(fresh.connected_at_unix_nanos, None);
    accounts
        .cancel(&change(&f.member), f.workspace.id(), rejoined.id)
        .await
        .unwrap();
}

#[tokio::test]
async fn member_removal_keeps_the_agents_they_provisioned() {
    use coral_provider::account::AccountScope;
    // Provisioning attribution is not Account ownership. Offboarding this
    // human must leave the Workspace-owned Agent and its credentials intact.
    let f = MemberRemoval::new().await;
    let accounts = &f.connected.accounts;
    f.workspaces
        .set_membership(
            &change(&f.owner),
            f.workspace.id(),
            &f.member,
            coral_workspace::WorkspaceRole::Owner,
        )
        .await
        .unwrap();
    let issued = f
        .workspaces
        .create_agent(
            &change(&f.member),
            f.workspace.id(),
            coral_workspace::AgentDisplayName::parse("Provisioned worker").unwrap(),
            None,
        )
        .await
        .unwrap();
    let agent_scope = AccountScope::Agent(issued.agent.principal().id().clone());
    let authorization = accounts
        .start_agent_authorization(
            &change(&f.member),
            f.workspace.id(),
            issued.agent.principal().id(),
            &f.connected.provider,
            accounts::request(),
            None,
        )
        .await
        .unwrap();
    accounts
        .submit_secret(
            &change(&f.member),
            f.workspace.id(),
            authorization.id,
            coral_credentials::CredentialValues::token(
                coral_credentials::Secret::new("agent-keeps-its-own-secret".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    let agent_account = || async {
        accounts
            .get_account(
                &change(&f.owner),
                f.workspace.id(),
                &f.connected.provider,
                &agent_scope,
            )
            .await
            .unwrap()
            .account
            .unwrap()
    };
    let provisioned = agent_account().await;
    assert_eq!(
        provisioned.configured_by_user_id.as_ref(),
        Some(f.member.user_id())
    );
    f.remove().await;
    assert_eq!(agent_account().await, provisioned);
    let agent = f
        .workspaces
        .authenticate_agent(f.workspace.id(), issued.credential.expose(), 100)
        .await
        .unwrap();
    f.workspaces
        .check_run_admission(&agent, f.workspace.id(), 100)
        .await
        .unwrap();
}

async fn save_answer(
    functions: &SavedFunctionModule,
    actor: &AuthenticatedPrincipal,
    workspace: &WorkspaceId,
) {
    functions
        .save(
            &SavedFunctionChangeContext::new(
                actor.clone(),
                100,
                TraceId::parse("11111111111111111111111111111111").unwrap(),
            ),
            workspace,
            SavedFunctionSave::new(
                SavedFunctionPath::parse("answer").unwrap(),
                SavedFunctionDescription::parse("Answer").unwrap(),
                SavedFunctionSource::parse("export default () => 42;").unwrap(),
            ),
        )
        .await
        .unwrap();
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one offboarding journey compares all domain state across rollback and rejoining"
)]
async fn organization_removal_cleans_all_workspaces_atomically_and_protects_last_owner() {
    let db = fixture().await;
    let database = db.database();
    let workspaces = WorkspaceModule::new(database.clone());
    let organization = coral_workspace::OrganizationModule::new(database.clone());
    let owner = provision_user(database.clone(), "user:owner", OrganizationRole::Owner).await;
    let member = provision_user(database.clone(), "user:member", OrganizationRole::Member).await;
    let author = AuthenticatedPrincipal::user(member.clone());
    let functions = SavedFunctionModule::new(
        database.clone(),
        workspaces.clone(),
        coral_functions::IsolateLimits::default(),
    )
    .unwrap();
    let mut scopes = Vec::new();
    let mut account_scopes = Vec::new();
    for name in ["First", "Second"] {
        let workspace = workspaces
            .create(&change(&member), WorkspaceDisplayName::parse(name).unwrap())
            .await
            .unwrap();
        // A second designated Owner: removal needs no transfer target.
        join_as_owner(&workspaces, &owner, workspace.id()).await;
        save_answer(&functions, &author, workspace.id()).await;
        account_scopes.push(
            accounts::ConnectedScope::new(
                database.clone(),
                workspaces.clone(),
                &owner,
                &member,
                workspace.id(),
            )
            .await,
        );
        scopes.push(workspace);
    }
    let before = organization
        .list_lifecycle_events(&owner)
        .await
        .unwrap()
        .len();
    let lifecycle = AccessLifecycleModule::new(database.clone(), workspaces.clone());
    assert!(matches!(
        lifecycle
            .remove_organization_member(&change(&owner), owner.user_id(), 1, None)
            .await,
        Err(Error::Workspace(coral_workspace::Error::PermissionDenied(
            _
        )))
    ));
    assert!(matches!(
        lifecycle
            .remove_organization_member(&change(&owner), member.user_id(), 2, None)
            .await,
        Err(Error::Workspace(
            coral_workspace::Error::OrganizationAuthorityConflict
        ))
    ));
    let attempts = RemovalAttempt::cases(&[
        EventFault::FunctionDeleted,
        EventFault::MembershipRemoved,
        EventFault::OrganizationMembershipRemoved,
    ]);
    let membership_before = organization
        .resolve_user_authority(member.user_id())
        .await
        .unwrap();
    let mut workspace_before = Vec::new();
    for workspace in &scopes {
        workspace_before.push((
            workspaces
                .list_memberships(&owner, workspace.id())
                .await
                .unwrap(),
            workspaces
                .list_lifecycle_events(&owner, workspace.id())
                .await
                .unwrap(),
            functions
                .get(
                    &author,
                    workspace.id(),
                    &SavedFunctionPath::parse("answer").unwrap(),
                )
                .await
                .unwrap(),
            functions
                .list_lifecycle_events(&author, workspace.id())
                .await
                .unwrap(),
        ));
    }
    for attempt in attempts {
        attempt
            .assert_rolled_back(
                &lifecycle,
                &database,
                &owner,
                AccessRemovalTarget::OrganizationMember {
                    user: member.user_id(),
                    revision: 1,
                    transfer_to: None,
                },
            )
            .await;
        for account in &account_scopes {
            account.assert_unchanged(&member).await;
        }
        assert_eq!(
            organization
                .resolve_user_authority(member.user_id())
                .await
                .unwrap(),
            membership_before
        );
        for (workspace, before) in scopes.iter().zip(&workspace_before) {
            assert_eq!(
                workspaces
                    .list_memberships(&owner, workspace.id())
                    .await
                    .unwrap(),
                before.0
            );
            assert_eq!(
                workspaces
                    .list_lifecycle_events(&owner, workspace.id())
                    .await
                    .unwrap(),
                before.1
            );
            assert_eq!(
                functions
                    .get(
                        &author,
                        workspace.id(),
                        &SavedFunctionPath::parse("answer").unwrap()
                    )
                    .await
                    .unwrap(),
                before.2
            );
            assert_eq!(
                functions
                    .list_lifecycle_events(&author, workspace.id())
                    .await
                    .unwrap(),
                before.3
            );
            assert!(
                functions
                    .list_lifecycle_events(
                        &AuthenticatedPrincipal::user(owner.clone()),
                        workspace.id()
                    )
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
        assert_eq!(
            organization
                .list_lifecycle_events(&owner)
                .await
                .unwrap()
                .len(),
            before
        );
    }
    let fault = ChangeContext::new(
        owner.clone(),
        200,
        TraceId::parse("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee").unwrap(),
    );
    assert!(matches!(
        lifecycle
            .remove_organization_member(&fault, member.user_id(), 1, None)
            .await,
        Err(Error::Functions(_))
    ));
    for account in &account_scopes {
        account.assert_unchanged(&member).await;
    }
    organization
        .resolve_user_authority(member.user_id())
        .await
        .unwrap();
    for workspace in &scopes {
        functions
            .get(
                &author,
                workspace.id(),
                &SavedFunctionPath::parse("answer").unwrap(),
            )
            .await
            .unwrap();
        assert!(
            functions
                .list_lifecycle_events(&AuthenticatedPrincipal::user(owner.clone()), workspace.id())
                .await
                .unwrap()
                .is_empty()
        );
    }
    lifecycle
        .remove_organization_member(&change(&owner), member.user_id(), 1, None)
        .await
        .unwrap();
    organization
        .resolve_user_authority(member.user_id())
        .await
        .unwrap_err();
    // A fresh eligible login and grant cannot restore the previous definitions.
    let returned = provision_user(database, "user:member", OrganizationRole::Member).await;
    for (workspace, before) in scopes.iter().zip(&workspace_before) {
        // Only the departed Membership is gone, and nobody gained one.
        assert_eq!(
            workspaces
                .list_memberships(&owner, workspace.id())
                .await
                .unwrap(),
            before
                .0
                .iter()
                .filter(|membership| membership.user_id() != member.user_id())
                .cloned()
                .collect::<Vec<_>>()
        );
        workspaces
            .set_membership(
                &change(&owner),
                workspace.id(),
                &returned,
                coral_workspace::WorkspaceRole::Member,
            )
            .await
            .unwrap();
        assert!(matches!(
            functions
                .get(
                    &AuthenticatedPrincipal::user(returned.clone()),
                    workspace.id(),
                    &SavedFunctionPath::parse("answer").unwrap()
                )
                .await,
            Err(coral_functions::Error::SavedFunctionNotFound)
        ));
        assert_eq!(
            functions
                .list_lifecycle_events(
                    &AuthenticatedPrincipal::user(returned.clone()),
                    workspace.id()
                )
                .await
                .unwrap()
                .len(),
            2
        );
    }
    for account in &account_scopes {
        account.assert_retired(&returned).await;
    }
    assert!(
        organization
            .list_lifecycle_events(&owner)
            .await
            .unwrap()
            .iter()
            .any(|event| event.action == coral_workspace::OrganizationLifecycleAction::Removed)
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one offboarding fixture proves credentials, authorizations, functions, authority and events roll back together"
)]
async fn agent_removal_rolls_back_registration_functions_and_events_together() {
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
    let issued = workspaces
        .create_agent(
            &change(&owner),
            workspace.id(),
            coral_workspace::AgentDisplayName::parse("Worker").unwrap(),
            None,
        )
        .await
        .unwrap();
    let agent = workspaces
        .authenticate_agent(workspace.id(), issued.credential.expose(), 100)
        .await
        .unwrap();
    let providers = coral_provider::ProviderModule::with_running_catalog(
        coral_outbound::DestinationPolicy::default()
            .with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
        database.clone(),
        workspaces.clone(),
    );
    let published = providers
        .publish_openapi_fixture(
            &change(&owner),
            workspace.id(),
            coral_provider::OpenApiDocument::parse(BEARER_OPENAPI).unwrap(),
            "agentAccount",
            coral_provider::access::CredentialRoute::Caller,
        )
        .await
        .unwrap();
    let request =
        || coral_provider::ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap();
    let provider_id = published.access().id();
    let scope = workspace.id().clone();
    let dir = tempfile::tempdir().unwrap();
    let accounts = coral_provider::account::AccountModule::new(
        providers.clone(),
        coral_credentials::CredentialVault::open(&dir.path().join("key")).unwrap(),
    );
    let initial = accounts
        .start_agent_authorization(
            &change(&owner),
            workspace.id(),
            agent.principal().id(),
            provider_id,
            request(),
            None,
        )
        .await
        .unwrap();
    let pending = accounts
        .start_agent_authorization(
            &change(&owner),
            workspace.id(),
            agent.principal().id(),
            provider_id,
            request(),
            None,
        )
        .await
        .unwrap();
    let connected = accounts
        .submit_secret(
            &change(&owner),
            &scope,
            initial.id,
            coral_credentials::CredentialValues::token(
                coral_credentials::Secret::new("agent-offboarding-secret".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    let reconnect = accounts
        .start_reconnect(
            &change(&owner),
            &scope,
            published.id(),
            connected.account_id.unwrap(),
            request(),
            None,
        )
        .await
        .unwrap();
    // Agent cleanup must atomically remove its current credentials and pending flows.
    let provider_events = providers
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .unwrap();
    let functions = SavedFunctionModule::new(
        database.clone(),
        workspaces.clone(),
        coral_functions::IsolateLimits::default(),
    )
    .unwrap();
    save_answer(&functions, &agent, workspace.id()).await;
    let before = workspaces
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .unwrap();
    let registration = workspaces
        .get_agent_registration(&owner, workspace.id(), agent.principal().id())
        .await
        .unwrap();
    let path = SavedFunctionPath::parse("answer").unwrap();
    let saved = functions.get(&agent, workspace.id(), &path).await.unwrap();
    let inspector = AuthenticatedPrincipal::user(owner.clone());
    let function_events = functions
        .list_lifecycle_events(&inspector, workspace.id())
        .await
        .unwrap();
    let lifecycle = AccessLifecycleModule::new(database.clone(), workspaces.clone());
    let attempts = RemovalAttempt::cases(&[
        EventFault::AuthorizationCompleted,
        EventFault::AccountDeleted,
        EventFault::FunctionDeleted,
        EventFault::AgentDeleted,
    ]);
    for attempt in attempts {
        attempt
            .assert_rolled_back(
                &lifecycle,
                &database,
                &owner,
                AccessRemovalTarget::Agent(workspace.id(), agent.principal().id()),
            )
            .await;
        assert_eq!(
            workspaces
                .get_agent_registration(&owner, workspace.id(), agent.principal().id())
                .await
                .unwrap(),
            registration
        );
        workspaces
            .authenticate_agent(workspace.id(), issued.credential.expose(), 100)
            .await
            .unwrap();
        assert_eq!(
            accounts
                .get_account(
                    &change(&owner),
                    workspace.id(),
                    provider_id,
                    &coral_provider::account::AccountScope::Agent(agent.principal().id().clone())
                )
                .await
                .map(|view| view.account.map(|account| account.id))
                .unwrap(),
            connected.account_id
        );
        for authorization in [pending.id, reconnect.id] {
            assert_eq!(
                accounts
                    .get_authorization(&change(&owner), &scope, authorization)
                    .await
                    .unwrap()
                    .state,
                coral_provider::account::AuthorizationState::Pending
            );
        }
        assert_eq!(
            providers
                .list_lifecycle_events(&owner, workspace.id())
                .await
                .unwrap(),
            provider_events
        );
        assert_eq!(
            functions.get(&agent, workspace.id(), &path).await.unwrap(),
            saved
        );
        assert_eq!(
            functions
                .list_lifecycle_events(&inspector, workspace.id())
                .await
                .unwrap(),
            function_events
        );
        assert_eq!(
            workspaces
                .list_lifecycle_events(&owner, workspace.id())
                .await
                .unwrap(),
            before
        );
    }
    lifecycle
        .delete_agent(&change(&owner), workspace.id(), agent.principal().id())
        .await
        .unwrap();
    workspaces
        .authenticate_agent(workspace.id(), issued.credential.expose(), 100)
        .await
        .unwrap_err();
    for authorization in [pending.id, reconnect.id] {
        assert_eq!(
            accounts
                .get_authorization(&change(&owner), &scope, authorization)
                .await
                .unwrap()
                .state,
            coral_provider::account::AuthorizationState::Canceled
        );
        assert_eq!(
            accounts
                .submit_secret(
                    &change(&owner),
                    &scope,
                    authorization,
                    coral_credentials::CredentialValues::token(
                        coral_credentials::Secret::new("late-agent-secret".into()).unwrap()
                    )
                )
                .await
                .unwrap()
                .state,
            coral_provider::account::AuthorizationState::Canceled
        );
    }
    assert!(
        workspaces
            .list_agents(&owner, workspace.id())
            .await
            .unwrap()
            .is_empty()
    );
    let events = functions
        .list_lifecycle_events(&AuthenticatedPrincipal::user(owner), workspace.id())
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(
        events.first().unwrap().action(),
        coral_functions::LifecycleAction::Deleted
    );
}

#[tokio::test]
async fn prepared_removal_cannot_be_reattributed_or_reused_after_rollback() {
    const FOREIGN: &str = "access removal belongs to another transaction or change context";
    let db = fixture().await;
    let database = db.database();
    let workspaces = WorkspaceModule::new(database.clone());
    let owner = provision_user(database.clone(), "user:owner", OrganizationRole::Owner).await;
    let member = provision_user(database.clone(), "user:member", OrganizationRole::Member).await;
    let workspace = workspaces
        .create(
            &change(&member),
            WorkspaceDisplayName::parse("Workspace").unwrap(),
        )
        .await
        .unwrap();
    join_as_owner(&workspaces, &owner, workspace.id()).await;
    let memberships = workspaces
        .list_memberships(&owner, workspace.id())
        .await
        .unwrap();
    let context = change(&owner);
    let mut tx = database.begin().await.unwrap();
    let prepared = workspaces
        .prepare_access_removal_in_transaction(
            &mut tx,
            &context,
            AccessRemovalTarget::WorkspaceMember(workspace.id(), member.user_id()),
        )
        .await
        .unwrap();
    assert!(matches!(
        coral_functions::delete_current_for_access_removal_in_transaction(
            &mut tx,
            &change(&member),
            &prepared
        )
        .await,
        Err(coral_functions::Error::Workspace(
            coral_workspace::Error::PermissionDenied(FOREIGN)
        ))
    ));
    tx.rollback().await.unwrap();
    let mut other = database.begin().await.unwrap();
    assert!(matches!(
        coral_functions::delete_current_for_access_removal_in_transaction(
            &mut other, &context, &prepared
        )
        .await,
        Err(coral_functions::Error::Workspace(
            coral_workspace::Error::PermissionDenied(FOREIGN)
        ))
    ));
    assert!(matches!(
        coral_provider::account::retire_for_access_removal_in_transaction(
            &mut other, &context, &prepared
        )
        .await,
        Err(coral_provider::Error::Workspace(
            coral_workspace::Error::PermissionDenied(FOREIGN)
        ))
    ));
    assert!(matches!(
        workspaces
            .apply_access_removal_in_transaction(&mut other, &context, prepared)
            .await,
        Err(coral_workspace::Error::PermissionDenied(FOREIGN))
    ));
    other.rollback().await.unwrap();
    assert_eq!(
        workspaces
            .list_memberships(&owner, workspace.id())
            .await
            .unwrap(),
        memberships
    );
}

#[tokio::test]
async fn unauthorized_removal_conceals_whether_the_target_user_exists() {
    let db = fixture().await;
    let database = db.database();
    let workspaces = WorkspaceModule::new(database.clone());
    let owner = provision_user(database.clone(), "user:owner", OrganizationRole::Owner).await;
    let member = provision_user(database.clone(), "user:member", OrganizationRole::Member).await;
    let workspace = workspaces
        .create(
            &change(&owner),
            WorkspaceDisplayName::parse("Private").unwrap(),
        )
        .await
        .unwrap();
    let lifecycle = AccessLifecycleModule::new(database, workspaces);
    let absent = UserId::parse("user:absent").unwrap();
    let known = lifecycle
        .remove_workspace_member(&change(&member), workspace.id(), owner.user_id())
        .await
        .unwrap_err();
    let unknown = lifecycle
        .remove_workspace_member(&change(&member), workspace.id(), &absent)
        .await
        .unwrap_err();
    assert_eq!(known.to_string(), unknown.to_string());
    let known = lifecycle
        .remove_organization_member(&change(&member), owner.user_id(), 1, None)
        .await
        .unwrap_err();
    let unknown = lifecycle
        .remove_organization_member(&change(&member), &absent, 1, None)
        .await
        .unwrap_err();
    assert_eq!(known.to_string(), unknown.to_string());
}
