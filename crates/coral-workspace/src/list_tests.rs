use std::collections::BTreeMap;
use std::fmt::Debug;

use coral_test_support::TestDatabase;

use super::{change, organization_user, remove_access, test_module};
use crate::{
    AccessRemovalTarget, AgentDisplayName, AgentState, AuthenticatedUser, OrganizationModule,
    OrganizationRole, PrincipalId, UserId, VerifiedUserProfile, Workspace, WorkspaceDisplayName,
    WorkspaceId, WorkspaceModule, WorkspaceOwner, WorkspaceRole,
};

/// One Organization whose lists have about `rows` rows each.
struct Lists {
    fixture: TestDatabase,
    module: WorkspaceModule,
    organization: OrganizationModule,
    /// An Organization Owner with a Membership in every Workspace but one.
    owner: AuthenticatedUser,
    /// The Workspace whose Members and Agents are listed.
    workspace: Workspace,
    /// The Users with a Membership in [`Self::workspace`], ordered by User.
    members: Vec<UserId>,
    /// The Members whose Organization Membership is disabled.
    disabled: Vec<UserId>,
    /// The Agents of [`Self::workspace`] that are not deleted, ordered by id.
    agents: Vec<PrincipalId>,
    /// The Workspaces where [`Self::owner`] holds a Membership, ordered by id.
    workspaces: Vec<WorkspaceId>,
    /// Every Workspace in the Organization, ordered by id.
    every_workspace: Vec<WorkspaceId>,
}

/// Builds an Organization where `rows` Users hold a Membership in one
/// Workspace, that Workspace has `rows` Agents, and its first Owner is the
/// one designated Owner of at least `rows / 2` Workspaces. The rows differ in
/// what a list reads about them: role, profile, Organization Membership
/// state, Agent state, expiry, creator and Workspace state.
async fn lists(rows: usize) -> Lists {
    let (fixture, module) = test_module().await;
    let organization = OrganizationModule::new(module.database.clone());
    let owner = organization_user(
        module.database.clone(),
        "user:lists:00",
        OrganizationRole::Owner,
    )
    .await;
    let workspace = module
        .create(
            &change(owner.clone(), 10, '1'),
            WorkspaceDisplayName::parse("Lists").unwrap(),
        )
        .await
        .unwrap();
    let mut lists = Lists {
        fixture,
        module,
        organization,
        owner,
        workspace,
        members: Vec::new(),
        disabled: Vec::new(),
        agents: Vec::new(),
        workspaces: Vec::new(),
        every_workspace: Vec::new(),
    };
    let co_owner = lists.add_members(rows).await;
    lists.add_agents(rows, co_owner.as_ref()).await;
    lists.add_workspaces(rows, co_owner.as_ref()).await;
    // A disabled User stays in each list, as disabled.
    for user in &lists.disabled {
        lists
            .organization
            .set_enabled(&change(lists.owner.clone(), 20, 'a'), user, 1, false)
            .await
            .unwrap();
    }
    lists
}

impl Lists {
    /// Gives the Workspace `rows` Members. Returns a second designated Owner
    /// when there is one, who keeps an enabled Organization Membership.
    async fn add_members(&mut self, rows: usize) -> Option<AuthenticatedUser> {
        let database = self.module.database.clone();
        self.members.push(self.owner.user_id().clone());
        let mut co_owner = None;
        // Members join in the reverse of the order the list returns them in.
        for index in (1..rows).rev() {
            let user = organization_user(
                database.clone(),
                &format!("user:lists:{index:02}"),
                OrganizationRole::Member,
            )
            .await;
            let role = if index % 2 == 0 {
                WorkspaceRole::Owner
            } else {
                WorkspaceRole::Member
            };
            self.module
                .set_membership(
                    &change(self.owner.clone(), 11, '2'),
                    self.workspace.id(),
                    &user,
                    role,
                )
                .await
                .unwrap();
            if index % 3 != 0 {
                let mut tx = database.begin().await.unwrap();
                OrganizationModule::hold_profile_updates_in_transaction(&mut tx)
                    .await
                    .unwrap();
                OrganizationModule::refresh_profile_in_transaction(
                    &mut tx,
                    user.user_id(),
                    VerifiedUserProfile::new(
                        Some(&format!("Person {index}")),
                        Some(&format!("person{index}@example.test")),
                        Some(true),
                        None,
                    ),
                    12,
                )
                .await
                .unwrap();
                tx.commit().await.unwrap();
            }
            self.members.push(user.user_id().clone());
            match index {
                // A Member and, where the fixture has one, a designated Owner.
                1 | 4 => self.disabled.push(user.user_id().clone()),
                2 => co_owner = Some(user),
                _ => {}
            }
        }
        self.members.sort();
        self.disabled.sort();
        co_owner
    }

    /// Gives the Workspace `rows` Agents and one deleted Agent.
    async fn add_agents(&mut self, rows: usize, co_owner: Option<&AuthenticatedUser>) {
        let (module, workspace) = (&self.module, self.workspace.id());
        for index in 0..=rows {
            let creator = co_owner.filter(|_| index % 2 == 1).unwrap_or(&self.owner);
            let issued = module
                .create_agent(
                    &change(creator.clone(), 13, '3'),
                    workspace,
                    AgentDisplayName::parse(&format!("Agent {index}")).unwrap(),
                    (index % 3 == 0).then_some(1_000),
                )
                .await
                .unwrap();
            let id = issued.agent.principal().id().clone();
            let context = change(self.owner.clone(), 14, '4');
            if index == rows {
                // A deleted Agent is in no list.
                let removal = AccessRemovalTarget::Agent(workspace, &id);
                remove_access(module, &context, removal).await.unwrap();
                continue;
            }
            if index == 0 {
                module
                    .set_agent_state(&context, workspace, &id, AgentState::Disabled)
                    .await
                    .unwrap();
            }
            self.agents.push(id);
        }
        self.agents.sort();
    }

    /// Gives the first Owner a Membership in `rows + 1` Workspaces, and the
    /// Organization one more Workspace.
    async fn add_workspaces(&mut self, rows: usize, co_owner: Option<&AuthenticatedUser>) {
        let (module, owner) = (&self.module, &self.owner);
        self.workspaces.push(self.workspace.id().clone());
        for index in (1..rows).rev() {
            let other = module
                .create(
                    &change(owner.clone(), 15, '5'),
                    WorkspaceDisplayName::parse(&format!("Lists {index:02}")).unwrap(),
                )
                .await
                .unwrap();
            // The first Owner is not the only designated Owner of this one.
            if let Some(co_owner) = co_owner.filter(|_| index % 2 == 0) {
                module
                    .set_membership(
                        &change(owner.clone(), 16, '6'),
                        other.id(),
                        co_owner,
                        WorkspaceRole::Owner,
                    )
                    .await
                    .unwrap();
            }
            if index == 1 {
                module
                    .archive(&change(owner.clone(), 17, '7'), other.id())
                    .await
                    .unwrap();
            }
            self.workspaces.push(other.id().clone());
        }
        // The first Owner is a Member of one of the outsider's Workspaces and
        // holds no Membership in the other.
        let outsider = organization_user(
            module.database.clone(),
            "user:lists:outsider",
            OrganizationRole::Member,
        )
        .await;
        let mut created = Vec::new();
        for name in ["Joined", "Elsewhere"] {
            let workspace = module
                .create(
                    &change(outsider.clone(), 18, '8'),
                    WorkspaceDisplayName::parse(name).unwrap(),
                )
                .await
                .unwrap();
            created.push(workspace.id().clone());
        }
        let [joined, elsewhere] = created.try_into().unwrap();
        module
            .set_membership(
                &change(outsider, 19, '9'),
                &joined,
                owner,
                WorkspaceRole::Member,
            )
            .await
            .unwrap();
        self.workspaces.push(joined);
        self.workspaces.sort();
        self.every_workspace.clone_from(&self.workspaces);
        self.every_workspace.push(elsewhere);
        self.every_workspace.sort();
    }

    /// Runs one list and returns its rows with the statements it sent.
    async fn sent<T>(&self, list: impl Future<Output = Vec<T>>) -> (usize, u64) {
        let before = self.fixture.statements_sent();
        let rows = list.await.len();
        (rows, self.fixture.statements_sent() - before)
    }

    /// Runs every list once. Returns the rows each one returned and the
    /// statements it sent, by list.
    async fn sent_by_list(&self) -> BTreeMap<&'static str, (usize, u64)> {
        let (module, owner, workspace) = (&self.module, &self.owner, self.workspace.id());
        let mut sent = BTreeMap::new();
        sent.insert(
            "Workspace Member inspections",
            self.sent(async { module.inspect_memberships(owner, workspace).await.unwrap() })
                .await,
        );
        sent.insert(
            "Workspace Memberships",
            self.sent(async { module.list_memberships(owner, workspace).await.unwrap() })
                .await,
        );
        sent.insert(
            "Workspace Owners",
            self.sent(async { module.list_owners(owner, workspace).await.unwrap() })
                .await,
        );
        sent.insert(
            "Agent inspections",
            self.sent(async { module.inspect_agents(owner, workspace).await.unwrap() })
                .await,
        );
        sent.insert(
            "Agents",
            self.sent(async { module.list_agents(owner, workspace).await.unwrap() })
                .await,
        );
        sent.insert(
            "Workspace inspections",
            self.sent(async { module.inspect_workspaces(owner).await.unwrap().workspaces })
                .await,
        );
        sent.insert(
            "Organization Workspaces",
            self.sent(async { module.list_organization_workspaces(owner).await.unwrap() })
                .await,
        );
        sent.insert(
            "Organization Memberships",
            self.sent(async { self.organization.list_memberships(owner).await.unwrap() })
                .await,
        );
        sent.insert(
            "Workspaces with one designated Owner",
            self.sent(self.sole_owner_workspaces(owner.user_id())).await,
        );
        sent
    }

    /// The Workspaces where `user` is the only designated Owner.
    async fn sole_owner_workspaces(&self, user: &UserId) -> Vec<WorkspaceId> {
        let mut tx = self.module.database.begin_read_snapshot().await.unwrap();
        let sole = crate::repository::sole_owner_workspace_ids(&mut tx, user)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        sole
    }
}

/// The text two values share when every field of them is equal, for views
/// that carry no equality of their own.
fn shown(value: &impl Debug) -> String {
    format!("{value:#?}")
}

// Catches: a list that reads about its rows one row at a time. Each such read
// is a round trip to the database inside the list's read snapshot, so the
// list slows with every row it has.
#[tokio::test]
async fn a_list_sends_as_many_statements_for_twelve_rows_as_for_two() {
    let few = lists(2).await.sent_by_list().await;
    let many = lists(12).await.sent_by_list().await;

    assert_eq!(
        few.keys().collect::<Vec<_>>(),
        many.keys().collect::<Vec<_>>()
    );
    for ((list, (few_rows, few_sent)), (many_rows, many_sent)) in few.iter().zip(many.values()) {
        assert!(
            few_rows < many_rows,
            "{list}: the larger fixture lists more rows than {few_rows}: {many_rows}"
        );
        assert_eq!(few_sent, many_sent, "{list}");
    }
}

// Catches: a list that reads about all its rows at once and then gives a row
// what it read about another row, drops a row that has nothing stored about
// it, keeps a row that is not in the list, or returns the rows in an order
// other than the one a row-by-row read returned.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one fixture is compared with a row-by-row read of each list"
)]
async fn a_list_returns_each_row_as_its_own_inspection_does_in_the_order_of_the_list() {
    let lists = lists(12).await;
    let (module, owner, workspace) = (&lists.module, &lists.owner, lists.workspace.id());

    // Members, ordered by User.
    let mut members = Vec::new();
    for user in &lists.members {
        let (membership, metadata, enabled) = module
            .inspect_membership(owner, workspace, user)
            .await
            .unwrap();
        let profile = module
            .inspect_user_profile(owner, workspace, user)
            .await
            .unwrap();
        members.push((membership, metadata, profile, enabled));
    }
    assert_eq!(
        shown(&module.inspect_memberships(owner, workspace).await.unwrap()),
        shown(&members)
    );
    assert_eq!(
        module.list_memberships(owner, workspace).await.unwrap(),
        members
            .iter()
            .map(|(membership, ..)| membership.clone())
            .collect::<Vec<_>>()
    );
    let disabled = members
        .iter()
        .filter(|(_, _, _, enabled)| !enabled)
        .map(|(membership, ..)| membership.user_id().clone())
        .collect::<Vec<_>>();
    assert_eq!(disabled, lists.disabled);

    // Designated Owners, in the order of the Members.
    let owners = members
        .iter()
        .filter(|(membership, ..)| membership.role() == WorkspaceRole::Owner)
        .map(|(_, _, profile, enabled)| WorkspaceOwner {
            profile: profile.clone(),
            enabled: *enabled,
        })
        .collect::<Vec<_>>();
    assert!(owners.iter().any(|owner| !owner.enabled));
    assert_eq!(
        shown(&module.list_owners(owner, workspace).await.unwrap()),
        shown(&owners)
    );

    // Agents, in the order the list of Agents has.
    let listed = module.list_agents(owner, workspace).await.unwrap();
    let mut agents = Vec::new();
    for agent in &listed {
        agents.push(
            module
                .inspect_agent(owner, workspace, agent.principal().id())
                .await
                .unwrap(),
        );
    }
    assert_eq!(
        shown(&module.inspect_agents(owner, workspace).await.unwrap()),
        shown(&agents)
    );
    let mut ids = listed
        .iter()
        .map(|agent| agent.principal().id().clone())
        .collect::<Vec<_>>();
    ids.sort();
    assert_eq!(ids, lists.agents);

    // The User's Workspaces, in listing order, with the User's own profile.
    let inspected = module.inspect_workspaces(owner).await.unwrap();
    let mut workspaces = Vec::new();
    for listed in module.list(owner).await.unwrap() {
        workspaces.push(module.inspect_workspace(owner, listed.id()).await.unwrap());
        assert_eq!(
            inspected.profile,
            module
                .inspect_user_profile(owner, listed.id(), owner.user_id())
                .await
                .unwrap()
        );
    }
    assert_eq!(shown(&inspected.workspaces), shown(&workspaces));
    let mut ids = inspected
        .workspaces
        .iter()
        .map(|(evidence, _)| evidence.workspace().id().clone())
        .collect::<Vec<_>>();
    ids.sort();
    assert_eq!(ids, lists.workspaces);
    let roles = inspected
        .workspaces
        .iter()
        .map(|(evidence, _)| evidence.effective_role());
    assert!(roles.clone().any(|role| role == WorkspaceRole::Member));
    assert!(roles.clone().any(|role| role == WorkspaceRole::Owner));

    // Every Workspace of the Organization, in the stable order of Workspaces.
    let mut tx = module.database.begin_read_snapshot().await.unwrap();
    let ids = module.workspace_ids_in_transaction(&mut tx).await.unwrap();
    tx.commit().await.unwrap();
    let mut every_workspace = Vec::new();
    for id in &ids {
        every_workspace.push(
            module
                .inspect_organization_workspace(owner, id)
                .await
                .unwrap(),
        );
    }
    assert_eq!(
        shown(&module.list_organization_workspaces(owner).await.unwrap()),
        shown(&every_workspace)
    );
    let mut sorted = ids.clone();
    sorted.sort();
    assert_eq!(sorted, lists.every_workspace);

    // Organization Memberships, ordered by User. An enabled User inspects
    // their own; a disabled one cannot, and is a Member of the Workspace.
    let listed = lists.organization.list_memberships(owner).await.unwrap();
    for (membership, profile) in &listed {
        let user = membership.user_id();
        if membership.enabled() {
            let own = AuthenticatedUser::organization_member(user.clone(), membership.role());
            assert_eq!(
                (membership.clone(), profile.clone()),
                lists
                    .organization
                    .inspect_current_membership(&own)
                    .await
                    .unwrap()
            );
        } else {
            assert!(lists.disabled.contains(user), "{user}");
            assert_eq!(
                profile,
                &module
                    .inspect_user_profile(owner, workspace, user)
                    .await
                    .unwrap()
            );
        }
    }
    let users = listed
        .iter()
        .map(|(membership, _)| membership.user_id().clone())
        .collect::<Vec<_>>();
    let mut expected = lists.members.clone();
    expected.push(UserId::parse("user:lists:outsider").unwrap());
    assert_eq!(users, expected);

    // The Workspaces a User is the only designated Owner of, in the stable
    // order of Workspaces.
    for user in &lists.members {
        let sole = every_workspace
            .iter()
            .filter(|found| {
                found
                    .owners
                    .iter()
                    .map(|owner| &owner.profile.user_id)
                    .eq([user])
            })
            .map(|found| found.workspace.id().clone())
            .collect::<Vec<_>>();
        assert_eq!(lists.sole_owner_workspaces(user).await, sole, "{user}");
    }
    assert_ne!(lists.sole_owner_workspaces(owner.user_id()).await, []);
}
