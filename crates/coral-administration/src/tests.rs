mod accounts;
mod agents;
mod builtin_catalog;
mod owner_transfer;
mod providers;
mod provisional;
mod repository;
mod search_eval;
mod setup;
mod shared_tools;
mod tool_removal;
mod workspaces;

use super::*;
use coral_catalog::AuthoredToolKind;
use coral_test_support::provision_user;
use coral_tools::{
    AuthoredToolDescription, AuthoredToolSource, PrivateToolChangeContext, PrivateToolModule,
    PrivateToolPath, PrivateToolSave,
};
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

/// Creates `owner`'s Workspace as production does, with the Providers Coral
/// defines, and tells `index` of them.
async fn workspace_with_coral_defined_providers(
    database: &Arc<Database>,
    workspaces: &WorkspaceModule,
    index: &coral_search::SearchIndex,
    owner: &AuthenticatedUser,
) -> WorkspaceId {
    WorkspaceWorkflow::new(
        database.clone(),
        workspaces.clone(),
        coral_provider::ProviderModule::with_running_catalog(
            coral_outbound::DestinationPolicy::default(),
            database.clone(),
            workspaces.clone(),
        ),
        index.clone(),
    )
    .create_workspace(
        &coral_test_support::change(owner.clone(), 10, '1'),
        WorkspaceDisplayName::parse("Ops").unwrap(),
    )
    .await
    .expect("create the Workspace")
    .id()
    .clone()
}

/// How many rows of `kind` the search index holds in the Workspace. Search
/// hydrates a hit from the Tool itself, so only the row shows a change that
/// search was not told of.
async fn indexed_rows(database: &Database, workspace: &WorkspaceId, kind: &str) -> usize {
    use sea_query::{Alias, Expr, ExprTrait as _, Query};
    let rows: Vec<(String,)> = database
        .fetch_all(
            Query::select()
                .column(Alias::new("doc_id"))
                .from(Alias::new("search_entities"))
                .and_where(Expr::col(Alias::new("workspace_id")).eq(workspace.to_string()))
                .and_where(Expr::col(Alias::new("kind")).eq(kind))
                .to_owned(),
        )
        .await
        .expect("read the index rows");
    rows.len()
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
async fn organization_owner_leaving_loses_current_private_tools_once_another_owner_exists() {
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
    let private_tools = PrivateToolModule::new(
        database.clone(),
        workspaces.clone(),
        coral_tools::IsolateLimits::default(),
        coral_test_support::TEST_COMPILATION_TIMEOUT,
    )
    .unwrap();
    let path = PrivateToolPath::parse("answer").unwrap();
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
    save_answer(&private_tools, &author, workspace.id()).await;
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
            private_tools
                .get(&author, workspace.id(), &path)
                .await
                .unwrap(),
            private_tools
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
        private_tools.get(&author, workspace.id(), &path).await,
        Err(coral_tools::Error::NotFound(AuthoredToolKind::Private))
    ));
}

coral_test_support::provider_setup_fixtures!(coral_provider);

/// A Workspace Member with a connected personal Account, a pending reconnect, and one
/// current Private Tool, in a Workspace an Organization Owner created. The Member's
/// Organization role must not change what removal cleans up.
struct MemberRemoval {
    db: coral_test_support::TestDatabase,
    workspaces: WorkspaceModule,
    lifecycle: AccessLifecycleModule,
    private_tools: PrivateToolModule,
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
        let private_tools = PrivateToolModule::new(
            database.clone(),
            workspaces.clone(),
            coral_tools::IsolateLimits::default(),
            coral_test_support::TEST_COMPILATION_TIMEOUT,
        )
        .unwrap();
        let author = AuthenticatedPrincipal::user(member.clone());
        save_answer(&private_tools, &author, workspace.id()).await;
        Self {
            db,
            lifecycle: AccessLifecycleModule::new(database, workspaces.clone()),
            workspaces,
            private_tools,
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
async fn failed_member_removal_changes_no_credentials_private_tools_or_events() {
    let f = MemberRemoval::new().await;
    let path = PrivateToolPath::parse("answer").unwrap();
    let saved = f
        .private_tools
        .get(&f.author, f.workspace.id(), &path)
        .await
        .unwrap();
    let private_tool_events = f
        .private_tools
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
        EventFault::PrivateToolDeleted,
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
            f.private_tools
                .get(&f.author, f.workspace.id(), &path)
                .await
                .unwrap(),
            saved
        );
        assert_eq!(
            f.private_tools
                .list_lifecycle_events(&f.author, f.workspace.id())
                .await
                .unwrap(),
            private_tool_events
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
async fn member_removal_retires_credentials_pending_connections_and_private_tools() {
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
    // `losing_a_workspace_removes_only_that_principals_private_tools`
    // covers which Private Tools go; this checks who the deletion names.
    let deletions = f
        .private_tools
        .list_lifecycle_events(&f.author, f.workspace.id())
        .await
        .unwrap();
    assert!(deletions.iter().any(
        |event| event.action() == coral_tools::LifecycleAction::Deleted
            && event.actor().id().as_str() == f.owner.user_id().as_str()
    ));
    assert!(
        f.private_tools
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

#[derive(Debug, Clone, Copy)]
enum AccessLoss {
    /// The Owner removes the User's Workspace Membership. An Organization
    /// Owner keeps no more of their Private Tools than a Member does.
    WorkspaceMembership(OrganizationRole),
    OrganizationMembership,
    AgentDeletion,
}

/// Each way a Principal loses a Workspace removes that Principal's Private
/// Tools there, and nobody else's.
// Catches: a removal that keeps the departed Principal's Private Tools, or
// one that removes another Principal's.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table covers every way a Principal loses a Workspace"
)]
async fn losing_a_workspace_removes_only_that_principals_private_tools() {
    let path = PrivateToolPath::parse("answer").unwrap();
    for loss in [
        AccessLoss::WorkspaceMembership(OrganizationRole::Member),
        AccessLoss::WorkspaceMembership(OrganizationRole::Owner),
        AccessLoss::OrganizationMembership,
        AccessLoss::AgentDeletion,
    ] {
        let db = fixture().await;
        let database = db.database();
        let workspaces = WorkspaceModule::new(database.clone());
        let private_tools = PrivateToolModule::new(
            database.clone(),
            workspaces.clone(),
            coral_tools::IsolateLimits::default(),
            coral_test_support::TEST_COMPILATION_TIMEOUT,
        )
        .unwrap();
        let owner = provision_user(database.clone(), "user:owner", OrganizationRole::Owner).await;
        let member_role = match loss {
            AccessLoss::WorkspaceMembership(role) => role,
            AccessLoss::OrganizationMembership | AccessLoss::AgentDeletion => {
                OrganizationRole::Member
            }
        };
        let member = provision_user(database.clone(), "user:member", member_role).await;
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
        let owner_principal = AuthenticatedPrincipal::user(owner.clone());
        let member_principal = AuthenticatedPrincipal::user(member.clone());
        for author in [&owner_principal, &member_principal, &agent] {
            save_answer(&private_tools, author, workspace.id()).await;
        }
        let tool = async |author: &AuthenticatedPrincipal| {
            private_tools
                .get(author, workspace.id(), &path)
                .await
                .unwrap()
        };
        let (owner_tool, member_tool, agent_tool) = (
            tool(&owner_principal).await,
            tool(&member_principal).await,
            tool(&agent).await,
        );
        let lifecycle = AccessLifecycleModule::new(database.clone(), workspaces.clone());

        match loss {
            AccessLoss::WorkspaceMembership(_) => lifecycle
                .remove_workspace_member(&change(&owner), workspace.id(), member.user_id())
                .await
                .unwrap(),
            AccessLoss::OrganizationMembership => lifecycle
                .remove_organization_member(&change(&owner), member.user_id(), 1, None)
                .await
                .unwrap(),
            AccessLoss::AgentDeletion => lifecycle
                .delete_agent(&change(&owner), workspace.id(), agent.principal().id())
                .await
                .unwrap(),
        }

        assert_eq!(tool(&owner_principal).await, owner_tool, "{loss:?}");
        match loss {
            AccessLoss::WorkspaceMembership(_) | AccessLoss::OrganizationMembership => {
                assert_eq!(tool(&agent).await, agent_tool, "{loss:?}");
                // A fresh Membership finds nothing at the departed path.
                let returned = match loss {
                    AccessLoss::OrganizationMembership => {
                        provision_user(database.clone(), "user:member", OrganizationRole::Member)
                            .await
                    }
                    _ => member.clone(),
                };
                workspaces
                    .set_membership(
                        &change(&owner),
                        workspace.id(),
                        &returned,
                        coral_workspace::WorkspaceRole::Member,
                    )
                    .await
                    .unwrap();
                let gone = private_tools
                    .get(
                        &AuthenticatedPrincipal::user(returned),
                        workspace.id(),
                        &path,
                    )
                    .await;
                assert!(
                    matches!(
                        gone,
                        Err(coral_tools::Error::NotFound(AuthoredToolKind::Private))
                    ),
                    "{loss:?}: {gone:?}"
                );
            }
            AccessLoss::AgentDeletion => {
                assert_eq!(tool(&member_principal).await, member_tool, "{loss:?}");
                // The Owner reads the deletion of the Agent's Private Tool.
                let deleted = private_tools
                    .list_lifecycle_events(&owner_principal, workspace.id())
                    .await
                    .unwrap()
                    .into_iter()
                    .filter(|event| event.action() == coral_tools::LifecycleAction::Deleted)
                    .map(|event| event.target_id().clone())
                    .collect::<Vec<_>>();
                assert_eq!(deleted, [agent_tool.id().clone()], "{loss:?}");
            }
        }
    }
}

/// How a Shared Tool's author loses the Workspace or stops existing.
#[derive(Debug, Clone, Copy)]
enum AuthorLoss {
    WorkspaceMembership,
    OrganizationMembership,
    UserDisablement,
    AgentDeletion,
}

/// A Shared Tool is the Workspace's: its author is attribution only.
// Catches: a removal, a disablement or an Agent deletion that removes,
// changes or hides a Shared Tool the departed Principal wrote or published.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table covers every way a Shared Tool's author is lost"
)]
async fn a_shared_tool_survives_the_loss_of_its_author() {
    for loss in [
        AuthorLoss::WorkspaceMembership,
        AuthorLoss::OrganizationMembership,
        AuthorLoss::UserDisablement,
        AuthorLoss::AgentDeletion,
    ] {
        let db = fixture().await;
        let database = db.database();
        let workspaces = WorkspaceModule::new(database.clone());
        let private_tools = PrivateToolModule::new(
            database.clone(),
            workspaces.clone(),
            coral_tools::IsolateLimits::default(),
            coral_test_support::TEST_COMPILATION_TIMEOUT,
        )
        .unwrap();
        let shared_tools = coral_tools::SharedToolModule::new(
            database.clone(),
            workspaces.clone(),
            coral_tools::IsolateLimits::default(),
            coral_test_support::TEST_COMPILATION_TIMEOUT,
        )
        .unwrap();
        let owner = provision_user(database.clone(), "user:owner", OrganizationRole::Owner).await;
        let author =
            provision_user(database.clone(), "user:author", OrganizationRole::Member).await;
        let workspace = workspaces
            .create(
                &change(&owner),
                WorkspaceDisplayName::parse("Workspace").unwrap(),
            )
            .await
            .unwrap();
        // The author is a second Owner, so they publish their own tool.
        workspaces
            .set_membership(
                &change(&owner),
                workspace.id(),
                &author,
                coral_workspace::WorkspaceRole::Owner,
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
        // The publisher and the Principal whose Private Tool it copies: the
        // author's own, or the Agent's, which the Owner publishes.
        let (publisher, source_owner) = match loss {
            AuthorLoss::AgentDeletion => (&owner, agent.clone()),
            _ => (&author, AuthenticatedPrincipal::user(author.clone())),
        };
        save_answer(&private_tools, &source_owner, workspace.id()).await;
        let path = coral_tools::SharedToolPath::parse("team.answer").unwrap();
        shared_tools
            .publish(
                &change(publisher),
                workspace.id(),
                coral_tools::SharedToolPublish::new(
                    path.clone(),
                    coral_tools::SharedToolOrigin::PrivateTool {
                        owner: source_owner.principal().clone(),
                        path: PrivateToolPath::parse("answer").unwrap(),
                    },
                    AuthoredToolDescription::parse("Answer").unwrap(),
                    AuthoredToolSource::parse("export default () => 42;").unwrap(),
                    coral_catalog::ToolAccessState::Enabled,
                    0,
                ),
                None,
            )
            .await
            .unwrap_or_else(|error| panic!("{loss:?}: {error}"));
        let reader = AuthenticatedPrincipal::user(owner.clone());
        // The Shared Tool, its revisions and its history.
        let read = async || {
            let tool = shared_tools
                .get(&reader, workspace.id(), &path)
                .await
                .unwrap_or_else(|error| panic!("{loss:?}: {error}"));
            let revisions = shared_tools
                .list_revisions(&reader, workspace.id(), tool.id())
                .await
                .unwrap_or_else(|error| panic!("{loss:?}: {error}"));
            let events = shared_tools
                .list_lifecycle_events(&reader, workspace.id(), Some(tool.id()), None, 100)
                .await
                .unwrap_or_else(|error| panic!("{loss:?}: {error}"));
            (tool, revisions, events)
        };
        let before = read().await;
        let lifecycle = AccessLifecycleModule::new(database.clone(), workspaces.clone());

        match loss {
            AuthorLoss::WorkspaceMembership => lifecycle
                .remove_workspace_member(&change(&owner), workspace.id(), author.user_id())
                .await
                .unwrap(),
            AuthorLoss::OrganizationMembership => lifecycle
                .remove_organization_member(&change(&owner), author.user_id(), 1, None)
                .await
                .unwrap(),
            AuthorLoss::UserDisablement => {
                coral_workspace::OrganizationModule::new(database.clone())
                    .set_enabled(&change(&owner), author.user_id(), 1, false)
                    .await
                    .unwrap();
            }
            AuthorLoss::AgentDeletion => lifecycle
                .delete_agent(&change(&owner), workspace.id(), agent.principal().id())
                .await
                .unwrap(),
        }

        let after = read().await;
        assert_eq!(after, before, "{loss:?}");
        assert_eq!(
            after.0.current().published_by(),
            publisher.user_id(),
            "{loss:?}"
        );
    }
}

async fn save_answer(
    private_tools: &PrivateToolModule,
    actor: &AuthenticatedPrincipal,
    workspace: &WorkspaceId,
) {
    private_tools
        .save(
            &PrivateToolChangeContext::new(
                actor.clone(),
                100,
                TraceId::parse("11111111111111111111111111111111").unwrap(),
            ),
            workspace,
            PrivateToolSave::new(
                PrivateToolPath::parse("answer").unwrap(),
                AuthoredToolDescription::parse("Answer").unwrap(),
                AuthoredToolSource::parse("export default () => 42;").unwrap(),
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
    let private_tools = PrivateToolModule::new(
        database.clone(),
        workspaces.clone(),
        coral_tools::IsolateLimits::default(),
        coral_test_support::TEST_COMPILATION_TIMEOUT,
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
        save_answer(&private_tools, &author, workspace.id()).await;
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
        EventFault::PrivateToolDeleted,
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
            private_tools
                .get(
                    &author,
                    workspace.id(),
                    &PrivateToolPath::parse("answer").unwrap(),
                )
                .await
                .unwrap(),
            private_tools
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
                private_tools
                    .get(
                        &author,
                        workspace.id(),
                        &PrivateToolPath::parse("answer").unwrap()
                    )
                    .await
                    .unwrap(),
                before.2
            );
            assert_eq!(
                private_tools
                    .list_lifecycle_events(&author, workspace.id())
                    .await
                    .unwrap(),
                before.3
            );
            assert!(
                private_tools
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
        Err(Error::Tools(_))
    ));
    for account in &account_scopes {
        account.assert_unchanged(&member).await;
    }
    organization
        .resolve_user_authority(member.user_id())
        .await
        .unwrap();
    for workspace in &scopes {
        private_tools
            .get(
                &author,
                workspace.id(),
                &PrivateToolPath::parse("answer").unwrap(),
            )
            .await
            .unwrap();
        assert!(
            private_tools
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
            private_tools
                .get(
                    &AuthenticatedPrincipal::user(returned.clone()),
                    workspace.id(),
                    &PrivateToolPath::parse("answer").unwrap()
                )
                .await,
            Err(coral_tools::Error::NotFound(AuthoredToolKind::Private))
        ));
        assert_eq!(
            private_tools
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
async fn agent_removal_rolls_back_registration_private_tools_and_events_together() {
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
    let private_tools = PrivateToolModule::new(
        database.clone(),
        workspaces.clone(),
        coral_tools::IsolateLimits::default(),
        coral_test_support::TEST_COMPILATION_TIMEOUT,
    )
    .unwrap();
    save_answer(&private_tools, &agent, workspace.id()).await;
    let before = workspaces
        .list_lifecycle_events(&owner, workspace.id())
        .await
        .unwrap();
    let registration = workspaces
        .get_agent_registration(&owner, workspace.id(), agent.principal().id())
        .await
        .unwrap();
    let path = PrivateToolPath::parse("answer").unwrap();
    let saved = private_tools
        .get(&agent, workspace.id(), &path)
        .await
        .unwrap();
    let inspector = AuthenticatedPrincipal::user(owner.clone());
    let private_tool_events = private_tools
        .list_lifecycle_events(&inspector, workspace.id())
        .await
        .unwrap();
    let lifecycle = AccessLifecycleModule::new(database.clone(), workspaces.clone());
    let attempts = RemovalAttempt::cases(&[
        EventFault::AuthorizationCompleted,
        EventFault::AccountDeleted,
        EventFault::PrivateToolDeleted,
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
            private_tools
                .get(&agent, workspace.id(), &path)
                .await
                .unwrap(),
            saved
        );
        assert_eq!(
            private_tools
                .list_lifecycle_events(&inspector, workspace.id())
                .await
                .unwrap(),
            private_tool_events
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
        coral_tools::delete_current_for_access_removal_in_transaction(
            &mut tx,
            &change(&member),
            &prepared
        )
        .await,
        Err(coral_tools::Error::Workspace(
            coral_workspace::Error::PermissionDenied(FOREIGN)
        ))
    ));
    tx.rollback().await.unwrap();
    let mut other = database.begin().await.unwrap();
    assert!(matches!(
        coral_tools::delete_current_for_access_removal_in_transaction(
            &mut other, &context, &prepared
        )
        .await,
        Err(coral_tools::Error::Workspace(
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
