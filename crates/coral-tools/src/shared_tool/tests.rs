use std::sync::Arc;

use coral_catalog::{AuthoredToolKind, ToolAccessState};
use coral_test_support::{TestDatabase, change, provision_user};
use coral_workspace::{
    AuthenticatedPrincipal, AuthenticatedUser, OrganizationRole, WorkspaceDisplayName, WorkspaceId,
    WorkspaceModule, WorkspaceRole,
};

use super::{
    SharedToolLifecycleAction, SharedToolLifecycleEvent, SharedToolModule, SharedToolOrigin,
    SharedToolPath, SharedToolPublication, SharedToolPublish, SharedToolRevisionOrigin,
};
use crate::{
    AuthoredToolDescription, AuthoredToolPath, AuthoredToolSource, Error, LinkedToolSelection,
    PrivateToolChangeContext, PrivateToolModule, PrivateToolPath, PrivateToolSave,
    hold_linked_tools_in_transaction, select_linked_tool_in_transaction,
};

/// One Workspace with an Owner, an ordinary Member and an Agent, each of
/// which can save Private Tools.
struct SharedToolFixture {
    _fixture: TestDatabase,
    database: Arc<coral_db::Database>,
    private_tools: PrivateToolModule,
    shared_tools: SharedToolModule,
    workspace: WorkspaceId,
    owner: AuthenticatedUser,
    member: AuthenticatedUser,
    agent: AuthenticatedPrincipal,
}

impl SharedToolFixture {
    async fn new() -> Self {
        let fixture = TestDatabase::new(coral_db::migration_plan())
            .await
            .expect("create Shared Tool test database");
        let database = fixture.database();
        let workspaces = WorkspaceModule::new(database.clone());
        let private_tools = PrivateToolModule::new(
            database.clone(),
            workspaces.clone(),
            crate::IsolateLimits::default(),
            coral_test_support::TEST_COMPILATION_TIMEOUT,
        )
        .expect("start Private Tool compiler");
        let shared_tools = SharedToolModule::new(
            database.clone(),
            workspaces.clone(),
            crate::IsolateLimits::default(),
            coral_test_support::TEST_COMPILATION_TIMEOUT,
        )
        .expect("start Shared Tool compiler");
        let owner = provision_user(database.clone(), "user:owner", OrganizationRole::Member).await;
        let workspace = workspaces
            .create(
                &change(owner.clone(), 10, 'a'),
                WorkspaceDisplayName::parse("Shared Tools").expect("display name"),
            )
            .await
            .expect("create Workspace")
            .id()
            .clone();
        let member =
            provision_user(database.clone(), "user:member", OrganizationRole::Member).await;
        workspaces
            .set_membership(
                &change(owner.clone(), 11, 'a'),
                &workspace,
                &member,
                WorkspaceRole::Member,
            )
            .await
            .expect("add the Member");
        let issued = workspaces
            .create_agent(
                &change(owner.clone(), 12, 'a'),
                &workspace,
                coral_workspace::AgentDisplayName::parse("Worker").expect("Agent name"),
                None,
            )
            .await
            .expect("create Agent");
        let agent = workspaces
            .authenticate_agent(&workspace, issued.credential.expose(), 13)
            .await
            .expect("authenticate Agent");
        Self {
            _fixture: fixture,
            database,
            private_tools,
            shared_tools,
            workspace,
            owner,
            member,
            agent,
        }
    }

    /// Saves `source` as `author`'s Private Tool at `path`.
    async fn save_private(&self, author: &AuthenticatedPrincipal, path: &str, source: &str) {
        self.private_tools
            .save(
                &PrivateToolChangeContext::new(
                    author.clone(),
                    20,
                    coral_workspace::TraceId::parse(&"b".repeat(32)).expect("trace id"),
                ),
                &self.workspace,
                PrivateToolSave::new(
                    PrivateToolPath::parse(path).expect("Private Tool Path"),
                    AuthoredToolDescription::parse("A private draft").expect("description"),
                    AuthoredToolSource::parse(source).expect("source"),
                ),
            )
            .await
            .expect("save the Private Tool");
    }

    /// Publishes `source` at `coral.shared.<path>` as `actor`, from the
    /// Owner's own Private Tool `draft`.
    async fn publish(
        &self,
        actor: &AuthenticatedUser,
        path: &str,
        source: &str,
        expected_revision: u64,
    ) -> Result<SharedToolPublication, Error> {
        self.publish_at(30, actor, path, source, expected_revision)
            .await
    }

    /// Publishes as [`Self::publish`] does, at the time `at`.
    async fn publish_at(
        &self,
        at: i64,
        actor: &AuthenticatedUser,
        path: &str,
        source: &str,
        expected_revision: u64,
    ) -> Result<SharedToolPublication, Error> {
        self.shared_tools
            .publish_as_reviewed(
                &change(actor.clone(), at, 'c'),
                &self.workspace,
                publish(
                    path,
                    own_draft(&self.owner),
                    "Remind someone",
                    source,
                    expected_revision,
                ),
                None,
            )
            .await
    }

    /// Every live Shared Tool with every revision's content hash: what a
    /// refused publish must leave unchanged.
    async fn everything(&self) -> Vec<(String, u64, Vec<String>)> {
        let reader = AuthenticatedPrincipal::user(self.owner.clone());
        let page = self
            .shared_tools
            .list(&reader, &self.workspace, None, 100)
            .await
            .expect("list Shared Tools");
        let mut everything = Vec::new();
        for summary in page.summaries() {
            let revisions = self
                .shared_tools
                .list_revisions(&reader, &self.workspace, summary.id())
                .await
                .expect("list revisions");
            everything.push((
                summary.path().as_str().to_string(),
                summary.current_revision(),
                revisions
                    .iter()
                    .map(|revision| revision.content_hash().to_string())
                    .collect(),
            ));
        }
        everything
    }

    /// The first of `selected` that no longer holds at an acceptance by
    /// `principal`, if any.
    async fn first_changed(
        &self,
        principal: &AuthenticatedPrincipal,
        selected: Vec<LinkedToolSelection>,
    ) -> Option<AuthoredToolPath> {
        let mut transaction = self.database.begin().await.expect("begin");
        let changed = hold_linked_tools_in_transaction(
            &mut transaction,
            &self.workspace,
            principal.principal(),
            &selected,
        )
        .await
        .expect("hold")
        .map(|selection| selection.path().clone());
        transaction.commit().await.expect("commit");
        changed
    }

    /// What `principal` discovers: the Call Path of each Authored Tool it
    /// lists, Private Tools first, each of which hydrates, and the identity,
    /// revision and source of the exact description at `path`.
    async fn discovered(
        &self,
        principal: &AuthenticatedPrincipal,
        path: &AuthoredToolPath,
    ) -> (Vec<String>, Option<(crate::AuthoredToolId, u64, String)>) {
        let mut transaction = self.database.begin_read_snapshot().await.expect("begin");
        let mut listed = Vec::new();
        for kind in AuthoredToolKind::ALL {
            for material in crate::authored_tool_search_material_in_transaction(
                &mut transaction,
                &self.workspace,
                principal.principal(),
                kind,
            )
            .await
            .expect("list search material")
            {
                let hydrated = crate::hydrate_authored_tool_search_material_in_transaction(
                    &mut transaction,
                    &self.workspace,
                    principal.principal(),
                    &material,
                )
                .await
                .expect("hydrate")
                .expect("a listed Tool hydrates");
                assert_eq!(hydrated.path(), material.path());
                assert!(hydrated.declaration().as_str().contains("Promise<string>"));
                listed.push(material.path().call_path());
            }
        }
        let exact = crate::exact_authored_tool_description_in_transaction(
            &mut transaction,
            &self.workspace,
            principal.principal(),
            path,
        )
        .await
        .expect("describe")
        .map(|exact| {
            (
                exact.id().clone(),
                exact.revision(),
                exact.detail().source().as_str().to_owned(),
            )
        });
        transaction.rollback().await.expect("finish");
        (listed, exact)
    }

    /// What Universal Search indexes of Shared Tools: every entry, the
    /// entry of `id`, and every identity with its current revision.
    async fn indexed(
        &self,
        id: &crate::AuthoredToolId,
    ) -> (
        Vec<crate::AuthoredToolEntry>,
        Option<crate::AuthoredToolEntry>,
        Vec<(crate::AuthoredToolId, u64)>,
    ) {
        let mut transaction = self.database.begin_read_snapshot().await.expect("begin");
        let shared = |id: &crate::AuthoredToolId| id.kind() == AuthoredToolKind::Shared;
        let entries = crate::authored_tool_entries_in_transaction(&mut transaction)
            .await
            .expect("read entries")
            .into_iter()
            .filter(|entry| shared(entry.id()))
            .collect();
        let entry =
            crate::authored_tool_entry_in_transaction(&mut transaction, &self.workspace, id)
                .await
                .expect("read the entry");
        let revisions = crate::authored_tool_index_revisions_in_transaction(&mut transaction)
            .await
            .expect("read index revisions")
            .into_iter()
            .filter(|revision| shared(revision.id()))
            .map(|revision| (revision.id().clone(), revision.revision()))
            .collect();
        transaction.rollback().await.expect("finish");
        (entries, entry, revisions)
    }

    /// Every lifecycle Event of the Workspace's Shared Tools, newest first.
    async fn events(&self) -> Vec<SharedToolLifecycleEvent> {
        self.shared_tools
            .list_lifecycle_events(
                &AuthenticatedPrincipal::user(self.owner.clone()),
                &self.workspace,
                None,
                None,
                100,
            )
            .await
            .expect("list events")
            .events()
            .to_vec()
    }
}

fn shared_path(path: &str) -> SharedToolPath {
    SharedToolPath::parse(path).expect("Shared Tool Path")
}

/// The Private Tool `draft` of `user`.
fn own_draft(user: &AuthenticatedUser) -> SharedToolOrigin {
    draft_of(&AuthenticatedPrincipal::user(user.clone()))
}

fn draft_of(owner: &AuthenticatedPrincipal) -> SharedToolOrigin {
    SharedToolOrigin::PrivateTool {
        owner: owner.principal().clone(),
        path: PrivateToolPath::parse("draft").expect("Private Tool Path"),
    }
}

fn publish(
    path: &str,
    origin: SharedToolOrigin,
    description: &str,
    source: &str,
    expected_revision: u64,
) -> SharedToolPublish {
    SharedToolPublish::review(
        shared_path(path),
        origin,
        AuthoredToolDescription::parse(description).expect("description"),
        AuthoredToolSource::parse(source).expect("source"),
        ToolAccessState::Enabled,
        expected_revision,
    )
}

/// Whether an error is the refusal a case expects.
type Refusal = fn(&Error) -> bool;

const DRAFT: &str = "export default async function remind(): Promise<string> { return 'draft'; }";
const REVIEWED: &str =
    "export default async function remind(): Promise<string> { return 'reviewed'; }";

// Catches: a Member or an Agent's keeper publishing, an Owner publishing a
// Member's private work, or an Owner refused their own or an Agent's tool.
#[tokio::test]
async fn only_an_owner_publishes_and_only_from_their_own_or_an_agents_private_tool() {
    enum Publisher {
        Owner,
        Member,
    }
    enum Source {
        Owner,
        Member,
        Agent,
    }
    // (case, who publishes, whose Private Tool, the refusal if any)
    let cases: [(&str, Publisher, Source, Option<Refusal>); 4] = [
        (
            "an Owner publishes their own",
            Publisher::Owner,
            Source::Owner,
            None,
        ),
        (
            "a Member publishes their own",
            Publisher::Member,
            Source::Member,
            Some(|error| matches!(error, Error::PermissionDenied(_))),
        ),
        (
            "an Owner publishes an Agent's",
            Publisher::Owner,
            Source::Agent,
            None,
        ),
        (
            "an Owner publishes a Member's",
            Publisher::Owner,
            Source::Member,
            Some(|error| matches!(error, Error::SharedToolSourceUnreadable)),
        ),
    ];
    for (case, publisher, source, refusal) in cases {
        let fixture = SharedToolFixture::new().await;
        let source_owner = match source {
            Source::Owner => AuthenticatedPrincipal::user(fixture.owner.clone()),
            Source::Member => AuthenticatedPrincipal::user(fixture.member.clone()),
            Source::Agent => fixture.agent.clone(),
        };
        fixture.save_private(&source_owner, "draft", DRAFT).await;
        let actor = match publisher {
            Publisher::Owner => &fixture.owner,
            Publisher::Member => &fixture.member,
        };
        let before = fixture.everything().await;

        let published = fixture
            .shared_tools
            .publish_as_reviewed(
                &change(actor.clone(), 30, 'c'),
                &fixture.workspace,
                publish(
                    "mail.remind",
                    draft_of(&source_owner),
                    "Remind someone",
                    DRAFT,
                    0,
                ),
                None,
            )
            .await;

        match refusal {
            None => {
                let published = published.unwrap_or_else(|error| panic!("{case}: {error}"));
                let tool = published.published().expect("the published Shared Tool");
                assert_eq!(tool.current().revision(), 1, "{case}");
                assert_eq!(
                    tool.current().published_by(),
                    fixture.owner.user_id(),
                    "{case}"
                );
                // Every Principal in the Workspace reads it: the Member and
                // the Agent too.
                for reader in [
                    AuthenticatedPrincipal::user(fixture.member.clone()),
                    fixture.agent.clone(),
                ] {
                    let read = fixture
                        .shared_tools
                        .get(&reader, &fixture.workspace, &shared_path("mail.remind"))
                        .await
                        .unwrap_or_else(|error| panic!("{case}: {error}"));
                    assert_eq!(read.current().source().as_str(), DRAFT, "{case}");
                }
            }
            Some(refused) => {
                let error = published.expect_err(case);
                assert!(refused(&error), "{case}: {error:?}");
                assert_eq!(fixture.everything().await, before, "{case}");
            }
        }
    }
}

// Catches: a publish that stores the Private Tool's bytes at commit time
// instead of the bytes the Owner reviewed, that changes the Private Tool it
// copies, or that follows a later save of it.
#[tokio::test]
async fn a_publish_stores_exactly_the_reviewed_bytes_and_leaves_the_private_tool_as_it_was() {
    let fixture = SharedToolFixture::new().await;
    let owner = AuthenticatedPrincipal::user(fixture.owner.clone());
    fixture.save_private(&owner, "draft", DRAFT).await;
    let draft_path = PrivateToolPath::parse("draft").expect("path");
    let private_before = fixture
        .private_tools
        .get(&owner, &fixture.workspace, &draft_path)
        .await
        .expect("read the draft");
    // The reviewed bytes keep their exact spelling, trailing space included.
    let reviewed = format!("{REVIEWED}  \n// reviewed by the Owner\n");

    let published = fixture
        .shared_tools
        .publish_as_reviewed(
            &change(fixture.owner.clone(), 30, 'c'),
            &fixture.workspace,
            SharedToolPublish::review(
                shared_path("mail.remind"),
                own_draft(&fixture.owner),
                AuthoredToolDescription::parse("Remind someone, as reviewed").expect("description"),
                AuthoredToolSource::parse(&reviewed).expect("source"),
                ToolAccessState::ApprovalRequired,
                0,
            ),
            None,
        )
        .await
        .expect("publish the reviewed bytes");
    let tool = published.published().expect("the published Shared Tool");

    // A later save of the draft reaches no Shared Tool.
    fixture
        .save_private(&owner, "draft", "export default async () => 'slipped in';")
        .await;
    let stored = fixture
        .shared_tools
        .get(&owner, &fixture.workspace, &shared_path("mail.remind"))
        .await
        .expect("read the Shared Tool");
    assert_eq!(stored.current().source().as_str(), reviewed);
    assert_eq!(
        stored.current().description().as_str(),
        "Remind someone, as reviewed"
    );
    assert_eq!(
        stored.current().default_access_state(),
        ToolAccessState::ApprovalRequired
    );
    assert_eq!(
        stored.current().content_hash(),
        tool.current().content_hash()
    );
    assert_eq!(
        stored.current().origin(),
        &SharedToolRevisionOrigin::PrivateTool(private_before.id().clone())
    );
    assert_eq!(stored.created_by(), fixture.owner.user_id());

    // Publishing copied the draft: before the later save it was untouched.
    assert_eq!(private_before.source().as_str(), DRAFT);
    assert_eq!(private_before.revision(), 1);
}

// Catches: a second Owner overwriting a revision they have not seen, a
// create that replaces an existing Shared Tool, or a refused publish that
// leaves a revision behind.
#[tokio::test]
async fn a_publish_whose_expected_revision_moved_is_refused_and_changes_nothing() {
    let fixture = SharedToolFixture::new().await;
    let owner = AuthenticatedPrincipal::user(fixture.owner.clone());
    fixture.save_private(&owner, "draft", DRAFT).await;

    // (the expected revision, the conflict's current revision or the new one)
    let steps: [(u64, Result<u64, u64>); 5] = [
        (3, Err(0)),
        (0, Ok(1)),
        (0, Err(1)),
        (1, Ok(2)),
        (1, Err(2)),
    ];
    for (step, (expected, outcome)) in steps.into_iter().enumerate() {
        let before = fixture.everything().await;
        let source = format!("export default async () => {step};");
        let published = fixture
            .publish(&fixture.owner, "mail.remind", &source, expected)
            .await;
        match outcome {
            Ok(revision) => {
                let published = published.unwrap_or_else(|error| panic!("step {step}: {error}"));
                assert_eq!(
                    published
                        .published()
                        .expect("the published Shared Tool")
                        .current()
                        .revision(),
                    revision,
                    "step {step}"
                );
            }
            Err(current) => {
                assert!(
                    matches!(
                        published,
                        Err(Error::RevisionConflict { kind: AuthoredToolKind::Shared, current: found }) if found == current
                    ),
                    "step {step}: {published:?}"
                );
                assert_eq!(fixture.everything().await, before, "step {step}");
            }
        }
    }
}

// Catches: a restore that rewrites or drops an earlier revision, that does
// not become current, or that loses which revision it copied.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one journey restores a revision and refuses the two restores that copy nothing"
)]
async fn publishing_an_earlier_revision_again_restores_it_as_a_new_revision() {
    let fixture = SharedToolFixture::new().await;
    let owner = AuthenticatedPrincipal::user(fixture.owner.clone());
    fixture.save_private(&owner, "draft", DRAFT).await;
    fixture
        .publish(&fixture.owner, "mail.remind", DRAFT, 0)
        .await
        .expect("publish revision 1");
    let second = fixture
        .publish(&fixture.owner, "mail.remind", REVIEWED, 1)
        .await
        .expect("publish revision 2");
    let id = second.published().expect("the Shared Tool").id();
    let path = shared_path("mail.remind");
    let first = fixture
        .shared_tools
        .get_revision(&owner, &fixture.workspace, id, 1)
        .await
        .expect("read revision 1");
    let before = fixture
        .shared_tools
        .list_revisions(&owner, &fixture.workspace, id)
        .await
        .expect("list revisions");

    let restored = fixture
        .shared_tools
        .publish_as_reviewed(
            &change(fixture.owner.clone(), 40, 'd'),
            &fixture.workspace,
            SharedToolPublish::review(
                path.clone(),
                SharedToolOrigin::Revision(1),
                first.description().clone(),
                first.source().clone(),
                first.default_access_state(),
                2,
            ),
            None,
        )
        .await
        .expect("restore revision 1");

    let current = restored
        .published()
        .expect("the restored Shared Tool")
        .current();
    assert_eq!(current.revision(), 3);
    assert_eq!(current.source(), first.source());
    assert_eq!(current.content_hash(), first.content_hash());
    assert_eq!(current.origin(), &SharedToolRevisionOrigin::Revision(1));
    let after = fixture
        .shared_tools
        .list_revisions(&owner, &fixture.workspace, id)
        .await
        .expect("list revisions");
    assert_eq!(
        after
            .iter()
            .map(super::SharedToolRevision::revision)
            .collect::<Vec<_>>(),
        [3, 2, 1]
    );
    // Revisions 1 and 2 are exactly as they were.
    assert_eq!(after.get(1..), Some(before.as_slice()));

    // A restore carries the content of the revision it names, and no other.
    let altered = fixture
        .shared_tools
        .publish_as_reviewed(
            &change(fixture.owner.clone(), 41, 'd'),
            &fixture.workspace,
            SharedToolPublish::review(
                path.clone(),
                SharedToolOrigin::Revision(1),
                first.description().clone(),
                AuthoredToolSource::parse(REVIEWED).expect("source"),
                first.default_access_state(),
                3,
            ),
            None,
        )
        .await;
    assert!(
        matches!(altered, Err(Error::SharedToolRestoreDiffers)),
        "{altered:?}"
    );

    // A revision that never existed restores nothing.
    let missing = fixture
        .shared_tools
        .publish_as_reviewed(
            &change(fixture.owner.clone(), 41, 'd'),
            &fixture.workspace,
            SharedToolPublish::review(
                path.clone(),
                SharedToolOrigin::Revision(9),
                first.description().clone(),
                first.source().clone(),
                first.default_access_state(),
                3,
            ),
            None,
        )
        .await;
    assert!(
        matches!(missing, Err(Error::SharedToolSourceUnreadable)),
        "{missing:?}"
    );
    assert_eq!(
        fixture
            .shared_tools
            .list_revisions(&owner, &fixture.workspace, id)
            .await
            .expect("list revisions"),
        after
    );
}

// Catches: a review that writes, or one that omits what the Owner
// approves: the diff's other side, the Tools the source calls, the Shared
// Tools it calls with their revisions, and the paths it builds at runtime.
#[tokio::test]
async fn a_review_returns_what_the_owner_approves_and_writes_nothing() {
    let fixture = SharedToolFixture::new().await;
    let owner = AuthenticatedPrincipal::user(fixture.owner.clone());
    fixture.save_private(&owner, "draft", DRAFT).await;
    fixture
        .publish(&fixture.owner, "mail.send", DRAFT, 0)
        .await
        .expect("publish the Shared Tool the reviewed one calls");
    fixture
        .publish(&fixture.owner, "mail.send", REVIEWED, 1)
        .await
        .expect("publish its second revision");
    fixture
        .publish(&fixture.owner, "mail.remind", DRAFT, 0)
        .await
        .expect("publish the first revision");
    let before = fixture.everything().await;
    let reviewed = "export default async function remind(name: string): Promise<string> {\n\
        let tool = name;\n\
        await coral.providers.demo[tool]();\n\
        await coral.private.contacts.find();\n\
        await coral.providers.mailer.send({});\n\
        return coral.shared.mail.send();\n\
    }";

    let shown = fixture
        .shared_tools
        .publish(
            &change(fixture.owner.clone(), 30, 'c'),
            &fixture.workspace,
            publish(
                "mail.remind",
                own_draft(&fixture.owner),
                "Remind",
                reviewed,
                1,
            ),
            None,
        )
        .await
        .expect("review the publish");

    assert!(shown.published().is_none());
    assert_eq!(fixture.everything().await, before);
    let review = shown.review();
    let previous = review.previous().expect("the current revision");
    assert_eq!(previous.revision(), 1);
    assert_eq!(previous.source().as_str(), DRAFT);
    assert!(
        review.declaration().as_str().contains("name: string"),
        "{}",
        review.declaration().as_str()
    );
    assert_eq!(
        review.called_tools(),
        [
            "coral.private.contacts.find",
            "coral.providers.mailer.send",
            "coral.shared.mail.send",
        ]
    );
    assert_eq!(review.runtime_built_paths(), ["coral.providers.demo"]);
    assert!(
        matches!(
            review.shared_tools(),
            [dependency]
                if dependency.path().as_str() == "mail.send" && dependency.revision() == 2
        ),
        "{:?}",
        review.shared_tools()
    );

    // A review is refused as the publish would be.
    let stale = fixture
        .shared_tools
        .publish(
            &change(fixture.owner.clone(), 31, 'c'),
            &fixture.workspace,
            publish(
                "mail.remind",
                own_draft(&fixture.owner),
                "Remind",
                reviewed,
                0,
            ),
            None,
        )
        .await;
    assert!(
        matches!(
            stale,
            Err(Error::RevisionConflict {
                kind: AuthoredToolKind::Shared,
                current: 1
            })
        ),
        "{stale:?}"
    );
}

// Catches: a publish that stores a declaration its Owner never saw, because
// a Shared Tool the source calls was published again after the review; and
// a review that does not show the declaration the next publish stores.
#[tokio::test]
async fn a_publish_whose_declaration_changed_since_its_review_is_refused_and_changes_nothing() {
    // It returns what `mail.send` returns, so its declaration follows that
    // Shared Tool's.
    const CALLER: &str =
        "export default async function remind() { return coral.shared.mail.send(); }";
    const COUNTS: &str = "export default async function send(): Promise<number> { return 1; }";
    let fixture = SharedToolFixture::new().await;
    let owner = AuthenticatedPrincipal::user(fixture.owner.clone());
    fixture.save_private(&owner, "draft", DRAFT).await;
    fixture
        .publish(&fixture.owner, "mail.send", DRAFT, 0)
        .await
        .expect("publish the Shared Tool the reviewed one calls");
    let review = publish(
        "mail.remind",
        own_draft(&fixture.owner),
        "Remind",
        CALLER,
        0,
    );
    let send = async |publish: SharedToolPublish| {
        fixture
            .shared_tools
            .publish(
                &change(fixture.owner.clone(), 40, 'c'),
                &fixture.workspace,
                publish,
                None,
            )
            .await
    };
    let reviewed = send(review.clone())
        .await
        .expect("review the publish")
        .review()
        .declaration()
        .as_str()
        .to_owned();
    fixture
        .publish(&fixture.owner, "mail.send", COUNTS, 1)
        .await
        .expect("publish the called Shared Tool again, with another result");
    let before = fixture.everything().await;

    let stale = send(review.clone().approve(reviewed.clone())).await;

    assert!(
        matches!(stale, Err(Error::SharedToolDeclarationChanged)),
        "{stale:?}"
    );
    assert_eq!(fixture.everything().await, before);
    // The review run again shows the declaration that the publish stores.
    let current = send(review.clone())
        .await
        .expect("review the publish again")
        .review()
        .declaration()
        .as_str()
        .to_owned();
    assert_ne!(current, reviewed);
    let published = send(review.approve(current.clone()))
        .await
        .expect("publish what the second review showed");
    assert_eq!(
        published
            .published()
            .expect("the published Shared Tool")
            .current()
            .declaration()
            .as_str(),
        current
    );
}

/// Whether the text of a refusal says each of `phrases` exactly once.
fn says_once(error: &Error, phrases: &[&str]) -> bool {
    error.safe_rejection().is_some_and(|(_code, message)| {
        phrases
            .iter()
            .all(|phrase| message.matches(phrase).count() == 1)
    })
}

/// Whether an error refuses the source published at `mail.other`, and its
/// text names the Shared Tool and the failure once each.
fn authoring_refused(error: &Error) -> bool {
    matches!(error, Error::Admission(AuthoredToolKind::Shared, _))
        && says_once(
            error,
            &["Shared Tool", "coral.shared.mail.other", "authoring failed"],
        )
}

// Catches: a source the compiler rejects, a call that is not data, a call
// to a Shared Tool that does not exist, or a path inside another Shared
// Tool's namespace becoming a revision; and a refusal whose text repeats
// the Shared Tool or the failure.
#[tokio::test]
async fn a_publish_that_cannot_become_a_tool_is_refused_and_changes_nothing() {
    let fixture = SharedToolFixture::new().await;
    let owner = AuthenticatedPrincipal::user(fixture.owner.clone());
    fixture.save_private(&owner, "draft", DRAFT).await;
    fixture
        .publish(&fixture.owner, "mail.remind", DRAFT, 0)
        .await
        .expect("publish a Shared Tool");
    let before = fixture.everything().await;
    let cases: [(&str, &str, &str, Refusal); 6] = [
        (
            "a syntax error",
            "mail.other",
            "export default async function (",
            authoring_refused,
        ),
        (
            "no default export",
            "mail.other",
            "export const remind = 'later';",
            authoring_refused,
        ),
        (
            "a callback argument",
            "mail.other",
            "export default async function each(visit: () => void): Promise<void> { visit(); }",
            |error| {
                matches!(error, Error::NotData(AuthoredToolKind::Shared, _))
                    && says_once(error, &["Shared Tool", "coral.shared.mail.other"])
            },
        ),
        (
            "a missing Shared Tool",
            "mail.other",
            "export default async function other() { return coral.shared.mail.absent(); }",
            |error| {
                authoring_refused(error) && error.to_string().contains("coral.shared.mail.absent")
            },
        ),
        (
            "a path under a Shared Tool",
            "mail.remind.later",
            DRAFT,
            |error| matches!(error, Error::PathConflict(AuthoredToolKind::Shared)),
        ),
        ("a path over a Shared Tool", "mail", DRAFT, |error| {
            matches!(error, Error::PathConflict(AuthoredToolKind::Shared))
        }),
    ];
    for (case, path, source, refused) in cases {
        let error = fixture
            .publish(&fixture.owner, path, source, 0)
            .await
            .expect_err(case);
        assert!(refused(&error), "{case}: {error:?} says {error}");
        assert_eq!(fixture.everything().await, before, "{case}");
    }
}

// Catches: an acceptance that keeps a definition its owner replaced or a
// revision a publish replaced, a hold that names the wrong selection, or a
// path that selects a Tool of the other kind.
#[tokio::test]
async fn a_linked_selection_holds_only_while_it_is_current() {
    let fixture = SharedToolFixture::new().await;
    let owner = AuthenticatedPrincipal::user(fixture.owner.clone());
    fixture.save_private(&owner, "draft", DRAFT).await;
    fixture
        .publish(&fixture.owner, "mail.remind", DRAFT, 0)
        .await
        .expect("publish revision 1");
    let private = AuthoredToolPath::Private(PrivateToolPath::parse("draft").expect("path"));
    let shared = AuthoredToolPath::Shared(shared_path("mail.remind"));

    let mut transaction = fixture.database.begin_read_snapshot().await.expect("begin");
    let mut selected = Vec::new();
    for path in [&shared, &private] {
        selected.push(
            select_linked_tool_in_transaction(
                &mut transaction,
                &fixture.workspace,
                owner.principal(),
                path,
            )
            .await
            .expect("select")
            .expect("the current definition"),
        );
    }
    // Each root names only its own kind: no Shared Tool is at `draft`, and
    // the Owner has no Private Tool at `mail.remind`.
    for absent in [
        AuthoredToolPath::Shared(shared_path("draft")),
        AuthoredToolPath::Private(PrivateToolPath::parse("mail.remind").expect("path")),
    ] {
        assert_eq!(
            select_linked_tool_in_transaction(
                &mut transaction,
                &fixture.workspace,
                owner.principal(),
                &absent,
            )
            .await
            .expect("select"),
            None,
            "{absent:?}"
        );
    }
    transaction.commit().await.expect("commit");
    assert_eq!(
        selected
            .iter()
            .map(|selection| (
                selection.path(),
                selection.revision(),
                selection.source().as_str()
            ))
            .collect::<Vec<_>>(),
        [(&shared, 1, DRAFT), (&private, 1, DRAFT)]
    );

    assert_eq!(fixture.first_changed(&owner, selected.clone()).await, None);
    fixture
        .publish(&fixture.owner, "mail.remind", REVIEWED, 1)
        .await
        .expect("publish revision 2");
    assert_eq!(
        fixture.first_changed(&owner, selected.clone()).await,
        Some(shared.clone())
    );
    fixture.save_private(&owner, "draft", REVIEWED).await;
    // Private Tools are held first, so the replaced draft is the one named.
    assert_eq!(fixture.first_changed(&owner, selected).await, Some(private));
}

// Catches: a Run that links a removed Shared Tool, or an acceptance that
// keeps a selection of it made before its removal.
#[tokio::test]
async fn a_removed_shared_tool_is_selected_by_nothing_and_a_selection_of_it_no_longer_holds() {
    let fixture = SharedToolFixture::new().await;
    let owner = AuthenticatedPrincipal::user(fixture.owner.clone());
    fixture.save_private(&owner, "draft", DRAFT).await;
    fixture
        .publish(&fixture.owner, "mail.remind", DRAFT, 0)
        .await
        .expect("publish revision 1");
    let shared = AuthoredToolPath::Shared(shared_path("mail.remind"));
    let select = async || {
        let mut transaction = fixture.database.begin_read_snapshot().await.expect("begin");
        let selection = select_linked_tool_in_transaction(
            &mut transaction,
            &fixture.workspace,
            owner.principal(),
            &shared,
        )
        .await
        .expect("select");
        transaction.commit().await.expect("commit");
        selection
    };
    let selected = select().await.expect("revision 1");
    assert_eq!(
        fixture.first_changed(&owner, vec![selected.clone()]).await,
        None
    );

    fixture
        .shared_tools
        .remove(
            &change(fixture.owner.clone(), 50, 'e'),
            &fixture.workspace,
            &shared_path("mail.remind"),
            1,
        )
        .await
        .expect("remove the Shared Tool");

    assert_eq!(select().await, None);
    assert_eq!(
        fixture.first_changed(&owner, vec![selected]).await,
        Some(shared)
    );
}

// Catches: a publish that drops the Provider Tool typing a Private Tool
// save has, a save that leaves `coral.shared` untyped, or a save and a
// publish that emit different declarations for one source.
#[tokio::test]
async fn one_source_is_typed_alike_when_saved_and_when_published() {
    let fixture = SharedToolFixture::new().await;
    let owner = AuthenticatedPrincipal::user(fixture.owner.clone());
    fixture.save_private(&owner, "draft", DRAFT).await;
    fixture
        .publish(&fixture.owner, "mail.remind", DRAFT, 0)
        .await
        .expect("publish the Shared Tool the source calls");
    // The source passes a Shared Tool an argument it does not take, and a
    // Provider Tool a field it does not have.
    let source = "export default async function typed() {
        await coral.shared.mail.remind(1);
        return coral.providers.github.repos.get({ owner: \"a\", nope: 1 });
    }";
    let codes = |diagnostics: &[crate::AuthoredToolDiagnostic]| {
        let mut codes = diagnostics
            .iter()
            .map(crate::AuthoredToolDiagnostic::code)
            .collect::<Vec<_>>();
        codes.sort_unstable();
        codes
    };

    let prepared = fixture
        .private_tools
        .prepare_save(
            &PrivateToolChangeContext::new(
                owner.clone(),
                40,
                coral_workspace::TraceId::parse(&"d".repeat(32)).expect("trace id"),
            ),
            &fixture.workspace,
            PrivateToolSave::new(
                PrivateToolPath::parse("typed").expect("Private Tool Path"),
                AuthoredToolDescription::parse("Typed").expect("description"),
                AuthoredToolSource::parse(source).expect("source"),
            ),
            Some(&crate::tests::DeclaresReposGet),
        )
        .await
        .expect("typing never fails a save");
    let mut transaction = fixture.database.begin().await.expect("begin");
    let saved = fixture
        .private_tools
        .save_in_transaction(&mut transaction, prepared)
        .await
        .expect("save");
    transaction.commit().await.expect("commit");

    let review_of = |tools| {
        let fixture = &fixture;
        async move {
            fixture
                .shared_tools
                .publish(
                    &change(fixture.owner.clone(), 41, 'e'),
                    &fixture.workspace,
                    publish("mail.typed", own_draft(&fixture.owner), "Typed", source, 0),
                    tools,
                )
                .await
                .expect("typing never fails a publish")
        }
    };
    let typed = review_of(Some(
        &crate::tests::DeclaresReposGet as &dyn crate::ToolDeclarationReader,
    ))
    .await;
    let review = typed.review();
    assert_eq!(
        codes(saved.diagnostics()),
        [2353, 2554],
        "a save reports the misuse of both Tools: {:?}",
        saved.diagnostics()
    );
    assert_eq!(codes(review.diagnostics()), codes(saved.diagnostics()));
    assert_eq!(review.declaration(), saved.declaration());

    // Without the Owner's declarations, Provider Tools stay untyped.
    let untyped = review_of(None).await;
    assert_eq!(codes(untyped.review().diagnostics()), [2554]);
}

// Catches: a removal that deletes the Shared Tool's revisions, that leaves
// its path taken or still readable, or a replacement at the path that
// continues the removed tool's identity and revisions.
#[tokio::test]
async fn a_removal_tombstones_the_shared_tool_frees_its_path_and_keeps_its_revisions() {
    let fixture = SharedToolFixture::new().await;
    let owner = AuthenticatedPrincipal::user(fixture.owner.clone());
    fixture.save_private(&owner, "draft", DRAFT).await;
    fixture
        .publish(&fixture.owner, "mail.remind", DRAFT, 0)
        .await
        .expect("publish revision 1");
    let published = fixture
        .publish(&fixture.owner, "mail.remind", REVIEWED, 1)
        .await
        .expect("publish revision 2");
    let tool = published.published().expect("the Shared Tool");
    let path = shared_path("mail.remind");
    let kept = fixture
        .shared_tools
        .list_revisions(&owner, &fixture.workspace, tool.id())
        .await
        .expect("list revisions");

    let removed = fixture
        .shared_tools
        .remove(
            &change(fixture.owner.clone(), 50, 'e'),
            &fixture.workspace,
            &path,
            2,
        )
        .await
        .expect("remove the Shared Tool");

    assert_eq!(&removed, tool.id());
    let read = fixture
        .shared_tools
        .get(&owner, &fixture.workspace, &path)
        .await;
    assert!(
        matches!(read, Err(Error::NotFound(AuthoredToolKind::Shared))),
        "{read:?}"
    );
    assert_eq!(fixture.everything().await, []);
    // Its identity still names its revisions, exactly as they were.
    let revisions_of = async |id| {
        fixture
            .shared_tools
            .list_revisions(&owner, &fixture.workspace, id)
            .await
            .expect("list revisions")
    };
    assert_eq!(revisions_of(&removed).await, kept);
    assert_eq!(
        fixture
            .shared_tools
            .get_revision(&owner, &fixture.workspace, &removed, 1)
            .await
            .expect("read a kept revision")
            .source()
            .as_str(),
        DRAFT
    );

    // The path is free: the next publish there creates another Shared Tool.
    let replacement = fixture
        .publish(&fixture.owner, "mail.remind", REVIEWED, 0)
        .await
        .expect("publish at the freed path");
    let replacement = replacement.published().expect("the replacement");
    assert_ne!(replacement.id(), &removed);
    assert_eq!(replacement.current().revision(), 1);
    assert_eq!(revisions_of(&removed).await, kept);
    assert_eq!(
        revisions_of(replacement.id())
            .await
            .iter()
            .map(super::SharedToolRevision::revision)
            .collect::<Vec<_>>(),
        [1]
    );
}

// Catches: a Shared Tool that only its publisher discovers, a discovery read
// of a superseded revision or of a removed Shared Tool, a search entry that
// names the publisher, and a Private Tool that another Principal discovers.
#[tokio::test]
async fn every_principal_discovers_a_live_shared_tool_at_its_current_revision() {
    let fixture = SharedToolFixture::new().await;
    let owner = AuthenticatedPrincipal::user(fixture.owner.clone());
    fixture.save_private(&owner, "draft", DRAFT).await;
    fixture
        .publish(&fixture.owner, "mail.remind", DRAFT, 0)
        .await
        .expect("publish revision 1");
    let published = fixture
        .publish(&fixture.owner, "mail.remind", REVIEWED, 1)
        .await
        .expect("publish revision 2");
    let id = crate::AuthoredToolId::from(published.published().expect("the tool").id().clone());
    let path = AuthoredToolPath::parse(AuthoredToolKind::Shared, "mail.remind").expect("a path");
    let principals = [
        owner.clone(),
        AuthenticatedPrincipal::user(fixture.member.clone()),
        fixture.agent.clone(),
    ];
    for (principal, private) in principals
        .iter()
        .zip([&["coral.private.draft"][..], &[], &[]])
    {
        let (listed, exact) = fixture.discovered(principal, &path).await;
        let mut expected = private.to_vec();
        expected.push("coral.shared.mail.remind");
        assert_eq!(listed, expected, "{principal:?}");
        assert_eq!(
            exact,
            Some((id.clone(), 2, REVIEWED.to_owned())),
            "{principal:?}"
        );
    }
    let (entries, entry, revisions) = fixture.indexed(&id).await;
    let entry = entry.expect("a live Shared Tool has an entry");
    assert_eq!(entries, std::slice::from_ref(&entry));
    assert_eq!(entry.id(), &id);
    assert_eq!(entry.principal(), None, "the Workspace owns it");
    assert_eq!(entry.path(), &path);
    assert_eq!(entry.revision(), 2);
    assert_eq!(revisions, [(id.clone(), 2)]);

    fixture
        .shared_tools
        .remove(
            &change(fixture.owner.clone(), 50, 'e'),
            &fixture.workspace,
            &shared_path("mail.remind"),
            2,
        )
        .await
        .expect("remove the Shared Tool");

    for (principal, private) in principals
        .iter()
        .zip([&["coral.private.draft"][..], &[], &[]])
    {
        assert_eq!(
            fixture.discovered(principal, &path).await,
            (private.iter().map(ToString::to_string).collect(), None),
            "{principal:?}"
        );
    }
    assert_eq!(fixture.indexed(&id).await, (Vec::new(), None, Vec::new()));
}

// Catches: a Member or a stale request removing a Shared Tool, a removal
// that breaks a Shared Tool whose current revision calls the removed one,
// a refusal that leaves a tombstone behind, or a Shared Tool that stays
// unremovable after its callers moved on or were removed themselves.
#[tokio::test]
async fn a_refused_removal_names_its_reason_and_changes_nothing() {
    const CALLS_SEND: &str =
        "export default async function relay() { return coral.shared.mail.send(); }";
    let fixture = SharedToolFixture::new().await;
    let owner = AuthenticatedPrincipal::user(fixture.owner.clone());
    fixture.save_private(&owner, "draft", DRAFT).await;
    for (path, source) in [
        ("mail.send", DRAFT),
        ("mail.remind", CALLS_SEND),
        ("mail.digest", CALLS_SEND),
    ] {
        fixture
            .publish(&fixture.owner, path, source, 0)
            .await
            .unwrap_or_else(|error| panic!("publish {path}: {error}"));
    }
    let remove = async |actor: &AuthenticatedUser, path: &str, expected_revision: u64| {
        fixture
            .shared_tools
            .remove(
                &change(actor.clone(), 50, 'e'),
                &fixture.workspace,
                &shared_path(path),
                expected_revision,
            )
            .await
    };
    let before = (fixture.everything().await, fixture.events().await);
    // (case, who removes, the path, the expected revision, the refusal)
    let cases: [(&str, &AuthenticatedUser, &str, u64, Refusal); 4] = [
        (
            "a Member removes",
            &fixture.member,
            "mail.remind",
            1,
            |error| matches!(error, Error::PermissionDenied(_)),
        ),
        (
            "no Shared Tool is at the path",
            &fixture.owner,
            "mail.absent",
            1,
            |error| matches!(error, Error::NotFound(AuthoredToolKind::Shared)),
        ),
        (
            "the revision moved",
            &fixture.owner,
            "mail.remind",
            2,
            |error| {
                matches!(
                    error,
                    Error::RevisionConflict {
                        kind: AuthoredToolKind::Shared,
                        current: 1
                    }
                )
            },
        ),
        (
            "other Shared Tools call it",
            &fixture.owner,
            "mail.send",
            1,
            // Of the two that call it, the first in path order is named.
            |error| {
                matches!(error, Error::SharedToolHasDependents { dependent }
                    if dependent.as_str() == "mail.digest")
            },
        ),
    ];
    for (case, actor, path, expected_revision, refused) in cases {
        let error = remove(actor, path, expected_revision)
            .await
            .expect_err(case);
        assert!(refused(&error), "{case}: {error:?}");
        assert_eq!(
            (fixture.everything().await, fixture.events().await),
            before,
            "{case}"
        );
    }

    // A Shared Tool whose current revision no longer calls it does not hold
    // it, and neither does one that was removed.
    fixture
        .publish(&fixture.owner, "mail.digest", DRAFT, 1)
        .await
        .expect("publish a digest that calls nothing");
    let held = remove(&fixture.owner, "mail.send", 1)
        .await
        .expect_err("the reminder still calls it");
    assert!(
        matches!(&held, Error::SharedToolHasDependents { dependent }
            if dependent.as_str() == "mail.remind"),
        "{held:?}"
    );
    remove(&fixture.owner, "mail.remind", 1)
        .await
        .expect("remove the reminder");
    remove(&fixture.owner, "mail.send", 1)
        .await
        .expect("remove the Shared Tool nothing calls");
}

// Catches: an Agent or a User outside the Workspace reading history, a
// Member refused it, a history that loses a removed Shared Tool or mixes it
// with the Shared Tool that took its path, or an Event that names the wrong
// actor, revision, content or time.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one journey gives every reader the same history to read"
)]
async fn member_users_read_the_history_of_shared_tools_and_agents_cannot() {
    use SharedToolLifecycleAction::{Published, Removed};
    let fixture = SharedToolFixture::new().await;
    let owner = AuthenticatedPrincipal::user(fixture.owner.clone());
    fixture.save_private(&owner, "draft", DRAFT).await;
    // The reminder is published twice and removed, and another Shared Tool
    // then takes its path.
    let mut hashes = Vec::new();
    let mut ids = Vec::new();
    for (at, path, source, expected_revision) in [
        (30, "mail.remind", DRAFT, 0),
        (31, "mail.remind", REVIEWED, 1),
        (32, "mail.send", DRAFT, 0),
    ] {
        let published = fixture
            .publish_at(at, &fixture.owner, path, source, expected_revision)
            .await
            .unwrap_or_else(|error| panic!("publish {path}: {error}"));
        let tool = published.published().expect("the Shared Tool");
        hashes.push(tool.current().content_hash().to_string());
        ids.push(tool.id().clone());
    }
    let [first_hash, second_hash, send_hash] = hashes.try_into().expect("three publishes");
    let [reminder, _, send] = ids.try_into().expect("three publishes");
    // Another Owner removes it, so a removal names who removed, not who
    // published.
    let workspaces = WorkspaceModule::new(fixture.database.clone());
    let remover = provision_user(
        fixture.database.clone(),
        "user:co-owner",
        OrganizationRole::Member,
    )
    .await;
    workspaces
        .set_membership(
            &change(fixture.owner.clone(), 32, 'a'),
            &fixture.workspace,
            &remover,
            WorkspaceRole::Owner,
        )
        .await
        .expect("add another Owner");
    fixture
        .shared_tools
        .remove(
            &change(remover.clone(), 33, 'e'),
            &fixture.workspace,
            &shared_path("mail.remind"),
            2,
        )
        .await
        .expect("remove the reminder");
    let replacement = fixture
        .publish_at(34, &fixture.owner, "mail.remind", DRAFT, 0)
        .await
        .expect("publish at the freed path");
    let replacement = replacement.published().expect("the replacement").id();

    // (action, Shared Tool, path, revision, content hash, time), newest first.
    let whole = [
        (Published, replacement, "mail.remind", 1, &first_hash, 34),
        (Removed, &reminder, "mail.remind", 2, &second_hash, 33),
        (Published, &send, "mail.send", 1, &send_hash, 32),
        (Published, &reminder, "mail.remind", 2, &second_hash, 31),
        (Published, &reminder, "mail.remind", 1, &first_hash, 30),
    ];
    let of_the_reminder = [whole[1], whole[3], whole[4]];
    let read = |events: &[SharedToolLifecycleEvent]| {
        events
            .iter()
            .map(|event| {
                let actor = match event.action() {
                    Published => &fixture.owner,
                    Removed => &remover,
                };
                assert_eq!(event.actor(), actor.user_id());
                (
                    event.action(),
                    event.shared_tool_id().clone(),
                    event.path().as_str().to_string(),
                    event.revision(),
                    event.content_hash().to_string(),
                    event.occurred_at_unix_nanos(),
                )
            })
            .collect::<Vec<_>>()
    };
    let expected = |events: &[(
        SharedToolLifecycleAction,
        &super::SharedToolId,
        &str,
        u64,
        &String,
        i64,
    )]| {
        events
            .iter()
            .map(|(action, id, path, revision, hash, at)| {
                (
                    *action,
                    (*id).clone(),
                    (*path).to_string(),
                    *revision,
                    (*hash).clone(),
                    *at,
                )
            })
            .collect::<Vec<_>>()
    };

    let outsider = provision_user(
        fixture.database.clone(),
        "user:outsider",
        OrganizationRole::Member,
    )
    .await;
    // (reader, who it is, the refusal if any)
    let readers: [(&str, AuthenticatedPrincipal, Option<Refusal>); 4] = [
        ("the Owner", owner.clone(), None),
        (
            "a Member User",
            AuthenticatedPrincipal::user(fixture.member.clone()),
            None,
        ),
        (
            "an Agent",
            fixture.agent.clone(),
            Some(|error| matches!(error, Error::PermissionDenied(_))),
        ),
        (
            "a User outside the Workspace",
            AuthenticatedPrincipal::user(outsider),
            Some(|error| matches!(error, Error::NotFound(AuthoredToolKind::Shared))),
        ),
    ];
    for (case, reader, refusal) in readers {
        let events = async |shared_tool| {
            fixture
                .shared_tools
                .list_lifecycle_events(&reader, &fixture.workspace, shared_tool, None, 100)
                .await
        };
        let revisions = fixture
            .shared_tools
            .list_revisions(&reader, &fixture.workspace, &reminder)
            .await;
        let revision = fixture
            .shared_tools
            .get_revision(&reader, &fixture.workspace, &reminder, 1)
            .await;
        match refusal {
            None => {
                let all = events(None)
                    .await
                    .unwrap_or_else(|error| panic!("{case}: {error}"));
                assert_eq!(read(all.events()), expected(&whole), "{case}");
                assert!(all.next_after().is_none(), "{case}");
                // One Shared Tool's history is its own, removed or not.
                let one = events(Some(&reminder))
                    .await
                    .unwrap_or_else(|error| panic!("{case}: {error}"));
                assert_eq!(read(one.events()), expected(&of_the_reminder), "{case}");
                // The removed Shared Tool's revisions are still read.
                let revisions = revisions.unwrap_or_else(|error| panic!("{case}: {error}"));
                assert_eq!(
                    revisions
                        .iter()
                        .map(|revision| (revision.revision(), revision.content_hash()))
                        .collect::<Vec<_>>(),
                    [(2, second_hash.as_str()), (1, first_hash.as_str())],
                    "{case}"
                );
                assert_eq!(
                    revision
                        .unwrap_or_else(|error| panic!("{case}: {error}"))
                        .source()
                        .as_str(),
                    DRAFT,
                    "{case}"
                );
            }
            Some(refused) => {
                for error in [
                    events(None).await.expect_err(case),
                    events(Some(&reminder)).await.expect_err(case),
                    revisions.expect_err(case),
                    revision.expect_err(case),
                ] {
                    assert!(refused(&error), "{case}: {error:?}");
                }
            }
        }
    }

    // An identity that names no Shared Tool of the Workspace reads nothing.
    let unknown = super::SharedToolId::parse("00000000-0000-4000-8000-000000000000").expect("id");
    for absent in [
        fixture
            .shared_tools
            .list_revisions(&owner, &fixture.workspace, &unknown)
            .await
            .map(drop),
        fixture
            .shared_tools
            .get_revision(&owner, &fixture.workspace, &unknown, 1)
            .await
            .map(drop),
        fixture
            .shared_tools
            .get_revision(&owner, &fixture.workspace, &reminder, 3)
            .await
            .map(drop),
    ] {
        assert!(
            matches!(absent, Err(Error::NotFound(AuthoredToolKind::Shared))),
            "{absent:?}"
        );
    }
    let of_none = fixture
        .shared_tools
        .list_lifecycle_events(&owner, &fixture.workspace, Some(&unknown), None, 100)
        .await
        .expect("list the events of no Shared Tool");
    assert_eq!(of_none.events(), []);

    // Another Workspace of the same Owner reads none of it, by the Shared
    // Tool's real identity or from one of its Events.
    let elsewhere = workspaces
        .create(
            &change(fixture.owner.clone(), 40, 'a'),
            WorkspaceDisplayName::parse("Elsewhere").expect("display name"),
        )
        .await
        .expect("create another Workspace")
        .id()
        .clone();
    let an_event = fixture.events().await.swap_remove(0);
    for absent in [
        fixture
            .shared_tools
            .list_revisions(&owner, &elsewhere, &reminder)
            .await
            .map(drop),
        fixture
            .shared_tools
            .get_revision(&owner, &elsewhere, &reminder, 1)
            .await
            .map(drop),
    ] {
        assert!(
            matches!(absent, Err(Error::NotFound(AuthoredToolKind::Shared))),
            "{absent:?}"
        );
    }
    for shared_tool in [None, Some(&reminder)] {
        let page = fixture
            .shared_tools
            .list_lifecycle_events(&owner, &elsewhere, shared_tool, None, 100)
            .await
            .expect("list the other Workspace's events");
        assert_eq!(page.events(), [], "{shared_tool:?}");
    }
    let continued = fixture
        .shared_tools
        .list_lifecycle_events(&owner, &elsewhere, None, Some(an_event.id()), 100)
        .await;
    assert!(
        matches!(continued, Err(Error::InvalidInput(_))),
        "{continued:?}"
    );
}

// Catches: a page that repeats or skips an Event, among Events that occurred
// together too, or a page request that is out of bounds or continues from
// nowhere being served.
#[tokio::test]
async fn pages_of_history_follow_one_another_without_a_gap_or_a_repeat() {
    let fixture = SharedToolFixture::new().await;
    let owner = AuthenticatedPrincipal::user(fixture.owner.clone());
    fixture.save_private(&owner, "draft", DRAFT).await;
    // Three Shared Tools published at one instant, and one after them.
    for (at, path) in [(30, "a.one"), (30, "a.two"), (30, "a.three"), (31, "b.one")] {
        fixture
            .publish_at(at, &fixture.owner, path, DRAFT, 0)
            .await
            .unwrap_or_else(|error| panic!("publish {path}: {error}"));
    }
    let whole = fixture.events().await;
    assert_eq!(
        whole.first().map(|event| event.path().as_str()),
        Some("b.one"),
        "the newest Event is first"
    );
    let page = async |after: Option<&super::SharedToolLifecycleEventId>, page_size| {
        fixture
            .shared_tools
            .list_lifecycle_events(&owner, &fixture.workspace, None, after, page_size)
            .await
    };

    let mut paged = Vec::new();
    let mut after = None;
    loop {
        let one = page(after.as_ref(), 1).await.expect("list a page");
        assert!(one.events().len() <= 1);
        paged.extend(one.events().iter().cloned());
        after = one.next_after().cloned();
        if after.is_none() {
            break;
        }
    }
    assert_eq!(paged, whole);

    let nowhere = super::SharedToolLifecycleEventId::parse("00000000-0000-4000-8000-000000000000")
        .expect("id");
    for refused in [
        page(None, 0).await,
        page(None, super::SHARED_TOOL_PAGE_MAX_SIZE + 1).await,
        page(Some(&nowhere), 1).await,
    ] {
        assert!(
            matches!(refused, Err(Error::InvalidInput(_))),
            "{refused:?}"
        );
    }
}
