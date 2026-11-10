use std::sync::Arc;

use coral_catalog::{AuthoredToolKind, ToolAccessState};
use coral_test_support::{TestDatabase, change, provision_user};
use coral_workspace::{
    AuthenticatedPrincipal, AuthenticatedUser, OrganizationRole, WorkspaceDisplayName, WorkspaceId,
    WorkspaceModule, WorkspaceRole,
};

use super::{
    SharedToolModule, SharedToolOrigin, SharedToolPath, SharedToolPublication, SharedToolPublish,
    SharedToolRevisionOrigin,
};
use crate::{
    AuthoredToolDescription, AuthoredToolSource, Error, LinkedToolPath, LinkedToolSelection,
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
        self.shared_tools
            .publish(
                &change(actor.clone(), 30, 'c'),
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
                .list_revisions(&reader, &self.workspace, summary.path())
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
    SharedToolPublish::new(
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
            .publish(
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
        .publish(
            &change(fixture.owner.clone(), 30, 'c'),
            &fixture.workspace,
            SharedToolPublish::new(
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
    fixture
        .publish(&fixture.owner, "mail.remind", REVIEWED, 1)
        .await
        .expect("publish revision 2");
    let path = shared_path("mail.remind");
    let first = fixture
        .shared_tools
        .get_revision(&owner, &fixture.workspace, &path, 1)
        .await
        .expect("read revision 1");
    let before = fixture
        .shared_tools
        .list_revisions(&owner, &fixture.workspace, &path)
        .await
        .expect("list revisions");

    let restored = fixture
        .shared_tools
        .publish(
            &change(fixture.owner.clone(), 40, 'd'),
            &fixture.workspace,
            SharedToolPublish::new(
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
        .list_revisions(&owner, &fixture.workspace, &path)
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
        .publish(
            &change(fixture.owner.clone(), 41, 'd'),
            &fixture.workspace,
            SharedToolPublish::new(
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
        matches!(altered, Err(Error::InvalidInput(_))),
        "{altered:?}"
    );

    // A revision that never existed restores nothing.
    let missing = fixture
        .shared_tools
        .publish(
            &change(fixture.owner.clone(), 41, 'd'),
            &fixture.workspace,
            SharedToolPublish::new(
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
            .list_revisions(&owner, &fixture.workspace, &path)
            .await
            .expect("list revisions"),
        after
    );
}

// Catches: a validation that writes, or a review that omits what the Owner
// approves: the diff's other side, the Tools the source calls, the Shared
// Tools it calls with their revisions, and the paths it builds at runtime.
#[tokio::test]
async fn a_validate_only_publish_returns_the_review_and_writes_nothing() {
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

    let validated = fixture
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
            )
            .validate_only(),
            None,
        )
        .await
        .expect("validate the publish");

    assert!(validated.published().is_none());
    assert_eq!(fixture.everything().await, before);
    let review = validated.review();
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

    // A validation is refused as the publish would be.
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
            )
            .validate_only(),
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

// Catches: a source the compiler rejects, a call that is not data, a call
// to a Shared Tool that does not exist, or a path inside another Shared
// Tool's namespace becoming a revision.
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
    let cases: [(&str, &str, &str, Refusal); 5] = [
        (
            "a syntax error",
            "mail.other",
            "export default async function (",
            |error| matches!(error, Error::Admission(AuthoredToolKind::Shared, _)),
        ),
        (
            "a callback argument",
            "mail.other",
            "export default async function each(visit: () => void): Promise<void> { visit(); }",
            |error| matches!(error, Error::NotData(AuthoredToolKind::Shared, _)),
        ),
        (
            "a missing Shared Tool",
            "mail.other",
            "export default async function other() { return coral.shared.mail.absent(); }",
            |error| {
                matches!(error, Error::Admission(AuthoredToolKind::Shared, message)
                    if message.contains("coral.shared.mail.absent"))
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
        assert!(refused(&error), "{case}: {error:?}");
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
    let private = LinkedToolPath::Private(PrivateToolPath::parse("draft").expect("path"));
    let shared = LinkedToolPath::Shared(shared_path("mail.remind"));

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
        LinkedToolPath::Shared(shared_path("draft")),
        LinkedToolPath::Private(PrivateToolPath::parse("mail.remind").expect("path")),
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

    let changed = |fixture: &SharedToolFixture, selected: Vec<LinkedToolSelection>| {
        let database = fixture.database.clone();
        let workspace = fixture.workspace.clone();
        let principal = owner.principal().clone();
        async move {
            let mut transaction = database.begin().await.expect("begin");
            let changed = hold_linked_tools_in_transaction(
                &mut transaction,
                &workspace,
                &principal,
                &selected,
            )
            .await
            .expect("hold")
            .map(|selection| selection.path().clone());
            transaction.commit().await.expect("commit");
            changed
        }
    };
    assert_eq!(changed(&fixture, selected.clone()).await, None);
    fixture
        .publish(&fixture.owner, "mail.remind", REVIEWED, 1)
        .await
        .expect("publish revision 2");
    assert_eq!(
        changed(&fixture, selected.clone()).await,
        Some(shared.clone())
    );
    fixture.save_private(&owner, "draft", REVIEWED).await;
    // Private Tools are held first, so the replaced draft is the one named.
    assert_eq!(changed(&fixture, selected).await, Some(private));
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
                    publish("mail.typed", own_draft(&fixture.owner), "Typed", source, 0)
                        .validate_only(),
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
