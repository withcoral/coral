//! Each removal of a Private or a Shared Tool that this crate composes
//! removes the exact rules that name the Tool, in the removal's own
//! transaction, and a Private Tool's delete tells Universal Search after it
//! commits. The rule rows are coral-policy's, and the removal rules are
//! coral-tools' and coral-workspace's: this tests only what the workflows
//! compose around them.
use std::collections::{BTreeMap, BTreeSet};

use super::agents::registered_agent;
use super::repository::{EventFault, fixture};
use crate::{AccessLifecycleModule, PrivateToolWorkflow, SharedToolWorkflow};
use coral_catalog::{AuthoredToolKind, ToolAccessState, ToolKey, ToolPolicyFacts};
use coral_policy::{ToolPolicyModule, ToolPolicyTarget};
use coral_provider::ProviderId;
use coral_search::{SearchIndex, SearchQuery, SearchRequest, UniversalSearchModule};
use coral_test_support::{TEST_COMPILATION_TIMEOUT, TestDatabase, change, provision_user, trace};
use coral_tools::{
    AuthoredToolDescription, AuthoredToolSource, IsolateLimits, PrivateToolChangeContext,
    PrivateToolDeletion, PrivateToolModule, PrivateToolPath, PrivateToolSave, SharedToolModule,
    SharedToolOrigin, SharedToolPath, SharedToolPublish,
};
use coral_workspace::{
    AuthenticatedPrincipal, AuthenticatedUser, OrganizationRole, WorkspaceId, WorkspaceModule,
    WorkspaceRole,
};

const SOURCE: &str = "export default async function tide() { return 'high at noon'; }";

/// One Workspace with the Providers Coral defines, whose Owner set a rule
/// on each of their own Private Tools, of two Agents' Private Tools and of
/// two Shared Tools, and whose second Owner set one on their own Private
/// Tool. Each ruled Tool has a name: `<whose>.gone` for a Tool a test
/// removes, `<whose>.kept` for the other Tool beside it.
struct Fixture {
    db: TestDatabase,
    index: SearchIndex,
    search: UniversalSearchModule,
    private_tools: PrivateToolWorkflow,
    shared_tools: SharedToolWorkflow,
    access: AccessLifecycleModule,
    owner: AuthenticatedUser,
    /// The second Owner, whose Workspace Membership the Owner removes.
    co_owner: AuthenticatedUser,
    member: AuthenticatedUser,
    /// The Agent whose Private Tool the Owner deletes.
    worker: AuthenticatedPrincipal,
    /// The Agent that loses its Workspace access.
    leaver: AuthenticatedPrincipal,
    workspace: WorkspaceId,
    ruled: BTreeMap<&'static str, (ProviderId, ToolKey)>,
}

impl Fixture {
    async fn new() -> Self {
        let db = fixture().await;
        let database = db.database();
        let workspaces = WorkspaceModule::new(database.clone());
        let index = SearchIndex::new(database.clone(), std::time::Duration::from_mins(5));
        let owner = provision_user(database.clone(), "user:owner", OrganizationRole::Member).await;
        let workspace =
            super::workspace_with_coral_defined_providers(&database, &workspaces, &index, &owner)
                .await;
        let member = member_with_role(
            &database,
            &workspaces,
            &owner,
            &workspace,
            "member",
            WorkspaceRole::Member,
        )
        .await;
        let co_owner = member_with_role(
            &database,
            &workspaces,
            &owner,
            &workspace,
            "co-owner",
            WorkspaceRole::Owner,
        )
        .await;
        let worker = registered_agent(&workspaces, &owner, &workspace).await;
        let leaver = registered_agent(&workspaces, &owner, &workspace).await;

        let owner_principal = AuthenticatedPrincipal::user(owner.clone());
        let co_owner_principal = AuthenticatedPrincipal::user(co_owner.clone());
        let mut private_tool_keys = save_private_tools(
            &database,
            &workspaces,
            &index,
            &workspace,
            &[
                ("own.gone", &owner_principal, "tide.gone"),
                ("own.kept", &owner_principal, "tide.kept"),
                ("worker.gone", &worker, "tide.gone"),
                ("worker.kept", &worker, "tide.kept"),
                ("leaver.gone", &leaver, "tide.gone"),
                ("co_owner.gone", &co_owner_principal, "tide.gone"),
            ],
        )
        .await;
        // An Owner sets a rule on their own Private Tool, never on another
        // User's.
        let co_owner_keys = private_tool_keys.split_off(5);

        let search = UniversalSearchModule::new(database.clone(), workspaces.clone());
        let shared_tools = SharedToolWorkflow::new(
            database.clone(),
            SharedToolModule::new(
                database.clone(),
                workspaces.clone(),
                IsolateLimits::default(),
                TEST_COMPILATION_TIMEOUT,
            )
            .expect("start the Shared Tool compiler"),
            search.clone(),
            index.clone(),
        );
        let shared_tool_keys = publish_shared_tools(&shared_tools, &owner, &workspace).await;

        let ruled = set_rules(
            &database,
            &workspaces,
            &workspace,
            vec![
                (&owner, AuthoredToolKind::Private, private_tool_keys),
                (&owner, AuthoredToolKind::Shared, shared_tool_keys),
                (&co_owner, AuthoredToolKind::Private, co_owner_keys),
            ],
        )
        .await;
        index.settled().await;

        Self {
            private_tools: PrivateToolWorkflow::new(
                database.clone(),
                PrivateToolDeletion::new(workspaces.clone()),
                index.clone(),
            ),
            access: AccessLifecycleModule::new(database, workspaces),
            db,
            index,
            search,
            shared_tools,
            owner,
            co_owner,
            member,
            worker,
            leaver,
            workspace,
            ruled,
        }
    }

    /// The names of the Tools that an exact rule of the Workspace names.
    async fn ruled(&self) -> BTreeSet<&'static str> {
        let database = self.db.database();
        let mut transaction = database.begin_read_snapshot().await.unwrap();
        let rules =
            coral_policy::exact_tool_rules_in_transaction(&mut transaction, &self.workspace)
                .await
                .expect("read the Workspace's rules");
        transaction.rollback().await.unwrap();
        // A Tool with no policy facts defaults to `approval_required`, so
        // only its rule enables it.
        self.ruled
            .iter()
            .filter(|(_name, (provider, key))| {
                rules.access_state(provider, key, ToolPolicyFacts::default())
                    == ToolAccessState::Enabled
            })
            .map(|(name, _tool)| *name)
            .collect()
    }

    /// Whether a search by `reader` finds its own Private Tool `tide.gone`.
    async fn finds_private_tool(&self, reader: &AuthenticatedPrincipal) -> bool {
        self.search
            .search(
                reader,
                &self.workspace,
                &SearchRequest::query(SearchQuery::parse("tide gone").unwrap()),
            )
            .await
            .expect("search the Workspace")
            .hits()
            .iter()
            .any(|hit| hit.call_path().as_str() == "coral.private.tide.gone")
    }

    /// How many Private Tool rows the search index holds in the Workspace.
    async fn indexed_private_tools(&self) -> usize {
        super::indexed_rows(&self.db.database(), &self.workspace, "private_tool").await
    }

    /// Attempts one removal as this crate composes it.
    async fn attempt(&self, removal: Removal) -> Result<(), crate::Error> {
        match removal {
            Removal::PrivateTool { by, of, fault } => {
                let agent = match of {
                    Whose::Own => None,
                    Whose::Workers => Some(self.worker.principal().id()),
                };
                let by = match by {
                    Actor::Owner => &self.owner,
                    Actor::Member => &self.member,
                };
                let trace =
                    fault.map_or_else(|| trace('5'), |fault| fault.context(by).trace_id().clone());
                self.private_tools
                    .delete(
                        &PrivateToolChangeContext::new(
                            AuthenticatedPrincipal::user(by.clone()),
                            50,
                            trace,
                        ),
                        &self.workspace,
                        agent,
                        &PrivateToolPath::parse("tide.gone").unwrap(),
                    )
                    .await
                    .map(drop)
                    .map_err(crate::Error::Tools)
            }
            Removal::Agent(fault) => {
                let context = fault.map_or_else(
                    || change(self.owner.clone(), 50, '5'),
                    |fault| fault.context(&self.owner),
                );
                self.access
                    .delete_agent(&context, &self.workspace, self.leaver.principal().id())
                    .await
            }
            Removal::Member => {
                self.access
                    .remove_workspace_member(
                        &change(self.owner.clone(), 50, '5'),
                        &self.workspace,
                        self.co_owner.user_id(),
                    )
                    .await
            }
            Removal::SharedTool { expected_revision } => self
                .shared_tools
                .remove(
                    &change(self.owner.clone(), 50, '5'),
                    &self.workspace,
                    &SharedToolPath::parse("tide.gone").unwrap(),
                    expected_revision,
                )
                .await
                .map(drop)
                .map_err(crate::Error::Tools),
        }
    }
}

/// A new User whom `owner` gives `role` in the Workspace.
async fn member_with_role(
    database: &std::sync::Arc<coral_db::Database>,
    workspaces: &WorkspaceModule,
    owner: &AuthenticatedUser,
    workspace: &WorkspaceId,
    name: &str,
    role: WorkspaceRole,
) -> AuthenticatedUser {
    let user = provision_user(
        database.clone(),
        &format!("user:{name}"),
        OrganizationRole::Member,
    )
    .await;
    workspaces
        .set_membership(&change(owner.clone(), 11, '1'), workspace, &user, role)
        .await
        .expect("add the Workspace Member");
    user
}

/// Publishes the Shared Tools `tide.gone` and `tide.kept` from the Owner's
/// Private Tool `tide.kept`, and returns each one's Tool Key by its name.
async fn publish_shared_tools(
    shared_tools: &SharedToolWorkflow,
    owner: &AuthenticatedUser,
    workspace: &WorkspaceId,
) -> NamedTools {
    let mut keys = Vec::new();
    for (name, path) in [("shared.gone", "tide.gone"), ("shared.kept", "tide.kept")] {
        let published = Box::pin(
            shared_tools.publish_as_reviewed(
                &change(owner.clone(), 30, '3'),
                workspace,
                SharedToolPublish::review(
                    SharedToolPath::parse(path).unwrap(),
                    SharedToolOrigin::PrivateTool {
                        owner: AuthenticatedPrincipal::user(owner.clone())
                            .principal()
                            .clone(),
                        path: PrivateToolPath::parse("tide.kept").unwrap(),
                    },
                    AuthoredToolDescription::parse("Read the tide").unwrap(),
                    AuthoredToolSource::parse(SOURCE).unwrap(),
                    ToolAccessState::Enabled,
                    0,
                ),
            ),
        )
        .await
        .expect("publish the Shared Tool");
        keys.push((name, published.published().expect("a tool").id().tool_key()));
    }
    keys
}

/// Saves each named Private Tool at its path as its author, tells search of
/// it, and returns its Tool Key by its name.
async fn save_private_tools(
    database: &std::sync::Arc<coral_db::Database>,
    workspaces: &WorkspaceModule,
    index: &SearchIndex,
    workspace: &WorkspaceId,
    tools: &[(&'static str, &AuthenticatedPrincipal, &str)],
) -> NamedTools {
    let private_tools = PrivateToolModule::new(
        database.clone(),
        workspaces.clone(),
        IsolateLimits::default(),
        TEST_COMPILATION_TIMEOUT,
    )
    .expect("start the Private Tool compiler");
    let mut keys = Vec::new();
    for &(name, author, path) in tools {
        let saved = private_tools
            .save(
                &PrivateToolChangeContext::new(author.clone(), 20, trace('2')),
                workspace,
                PrivateToolSave::new(
                    PrivateToolPath::parse(path).unwrap(),
                    AuthoredToolDescription::parse("Read the tide").unwrap(),
                    AuthoredToolSource::parse(SOURCE).unwrap(),
                ),
            )
            .await
            .expect("save the Private Tool");
        // A save through coral-tools tells search nothing.
        index.sync_authored_tool(workspace.clone(), saved.id().clone());
        keys.push((name, saved.id().tool_key()));
    }
    keys
}

/// Tools by the name a test knows each by.
type NamedTools = Vec<(&'static str, ToolKey)>;

/// Sets each Owner's rule on their named Tools of a kind's Provider, and
/// returns every ruled Tool by its name. The rule enables its Tool, so the
/// Tool stays in search.
async fn set_rules(
    database: &std::sync::Arc<coral_db::Database>,
    workspaces: &WorkspaceModule,
    workspace: &WorkspaceId,
    tools: Vec<(&AuthenticatedUser, AuthoredToolKind, NamedTools)>,
) -> BTreeMap<&'static str, (ProviderId, ToolKey)> {
    let policies = ToolPolicyModule::new(database.clone(), workspaces.clone());
    let mut ruled = BTreeMap::new();
    for (owner, kind, keys) in tools {
        let mut transaction = database.begin_read_snapshot().await.unwrap();
        let provider = coral_provider::authored_tool_provider_in_transaction(
            &mut transaction,
            workspace,
            kind,
        )
        .await
        .unwrap()
        .expect("the Provider Coral defines for the kind")
        .id()
        .clone();
        transaction.rollback().await.unwrap();
        policies
            .set_access_states(
                &change(owner.clone(), 40, '4'),
                keys.iter()
                    .map(|(_name, key)| {
                        (
                            ToolPolicyTarget::new(workspace.clone(), provider.clone(), key.clone()),
                            ToolAccessState::Enabled,
                        )
                    })
                    .collect(),
            )
            .await
            .expect("set the Owner's rules");
        ruled.extend(
            keys.into_iter()
                .map(|(name, key)| (name, (provider.clone(), key))),
        );
    }
    ruled
}

/// What a refused or failed removal returns.
type Refusal = fn(&crate::Error) -> bool;

/// Who deletes a Private Tool.
#[derive(Clone, Copy)]
enum Actor {
    Owner,
    Member,
}

/// Whose Private Tool `tide.gone` a delete names.
#[derive(Clone, Copy)]
enum Whose {
    /// The caller's own.
    Own,
    /// The Agent `worker`'s.
    Workers,
}

/// One removal path of a Private or a Shared Tool.
#[derive(Clone, Copy)]
enum Removal {
    /// `DeletePrivateTool`: `by` deletes `tide.gone`, their own or the
    /// worker's. A fault fails the delete at its lifecycle Event.
    PrivateTool {
        by: Actor,
        of: Whose,
        fault: Option<EventFault>,
    },
    /// Access removal: the Owner deletes the leaver, which removes its
    /// Private Tools. A fault fails the deletion after its cleanup ran.
    Agent(Option<EventFault>),
    /// Access removal: the Owner removes the second Owner's Workspace
    /// Membership, which removes their Private Tools.
    Member,
    /// The Owner removes the Shared Tool `tide.gone`.
    SharedTool { expected_revision: u64 },
}

// Catches: a removal path that leaves its Tool's exact rule among the
// Workspace's rules, or takes another Tool's; a refused or failed removal
// whose rule removal still commits; or a removal that commits although its
// rule removal failed.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table covers each removal path, refused, failed and accepted"
)]
async fn each_removal_of_a_tool_removes_its_rule_and_a_refused_or_failed_one_keeps_it() {
    let f = Fixture::new().await;
    assert_eq!(
        f.ruled().await,
        f.ruled.keys().copied().collect(),
        "an Owner's rule names every Tool"
    );

    let refusals: [(&str, Removal, Refusal); 4] = [
        (
            "a Member's delete of an Agent's Private Tool",
            Removal::PrivateTool {
                by: Actor::Member,
                of: Whose::Workers,
                fault: None,
            },
            |error| {
                matches!(
                    error,
                    crate::Error::Tools(coral_tools::Error::NotFound(AuthoredToolKind::Private))
                )
            },
        ),
        (
            "a Private Tool delete whose Event fails",
            Removal::PrivateTool {
                by: Actor::Owner,
                of: Whose::Own,
                fault: Some(EventFault::PrivateToolDeleted),
            },
            |error| matches!(error, crate::Error::Tools(coral_tools::Error::Database(_))),
        ),
        (
            "an Agent deletion that fails after its cleanup",
            Removal::Agent(Some(EventFault::AgentDeleted)),
            |error| {
                matches!(
                    error,
                    crate::Error::Workspace(coral_workspace::Error::Database(_))
                )
            },
        ),
        (
            "a Shared Tool removal at a stale revision",
            Removal::SharedTool {
                expected_revision: 7,
            },
            |error| {
                matches!(
                    error,
                    crate::Error::Tools(coral_tools::Error::RevisionConflict {
                        kind: AuthoredToolKind::Shared,
                        current: 1,
                    })
                )
            },
        ),
    ];
    let before = f.ruled().await;
    for (label, removal, is_the_refusal) in refusals {
        let refused = f.attempt(removal).await;
        assert!(
            refused.as_ref().is_err_and(is_the_refusal),
            "{label}: {refused:?}"
        );
        assert_eq!(f.ruled().await, before, "{label}");
    }

    let removals = [
        (
            "a delete of the caller's own Private Tool",
            Removal::PrivateTool {
                by: Actor::Owner,
                of: Whose::Own,
                fault: None,
            },
            "own.gone",
        ),
        (
            "an Owner's delete of an Agent's Private Tool",
            Removal::PrivateTool {
                by: Actor::Owner,
                of: Whose::Workers,
                fault: None,
            },
            "worker.gone",
        ),
        (
            "access removal of an Agent",
            Removal::Agent(None),
            "leaver.gone",
        ),
        (
            "access removal of a Workspace Member",
            Removal::Member,
            "co_owner.gone",
        ),
        (
            "a Shared Tool removal",
            Removal::SharedTool {
                expected_revision: 1,
            },
            "shared.gone",
        ),
    ];

    // While the rule delete fails, each removal fails with it and keeps its
    // Tool: the removal below still finds the Tool it removes.
    f.db.fail_deletes("tool_access_rules", true).await.unwrap();
    for (label, removal, _gone) in removals {
        let failed = f.attempt(removal).await;
        assert!(
            matches!(
                failed,
                Err(crate::Error::Tools(coral_tools::Error::Database(_))
                    | crate::Error::Policy(coral_policy::Error::Database(_)))
            ),
            "{label}: {failed:?}"
        );
        assert_eq!(f.ruled().await, before, "{label}");
    }
    f.db.fail_deletes("tool_access_rules", false).await.unwrap();

    for (label, removal, gone) in removals {
        let mut expected = f.ruled().await;
        assert!(expected.remove(gone), "{label}: {gone} had a rule");
        f.attempt(removal).await.expect(label);
        assert_eq!(f.ruled().await, expected, "{label}");
    }
}

// Catches: a Private Tool delete that commits without telling search, so
// the Private Tool's row stays in the index until a reconcile pass.
#[tokio::test]
async fn a_private_tool_delete_tells_search_after_it_commits() {
    let f = Fixture::new().await;
    let owner = AuthenticatedPrincipal::user(f.owner.clone());
    for (label, of, reader) in [
        ("the caller's own", Whose::Own, &owner),
        ("an Agent's", Whose::Workers, &f.worker),
    ] {
        assert!(f.finds_private_tool(reader).await, "{label}");
        let indexed = f.indexed_private_tools().await;
        f.attempt(Removal::PrivateTool {
            by: Actor::Owner,
            of,
            fault: None,
        })
        .await
        .expect(label);
        f.index.settled().await;
        assert!(!f.finds_private_tool(reader).await, "{label}");
        assert_eq!(
            f.indexed_private_tools().await,
            indexed - 1,
            "{label}: the delete took the row"
        );
    }
}
