//! Calls to `coral.shared.*`, over the real V8 engine: every Principal in
//! the Workspace calls a Shared Tool with its own authority, each Run links
//! and records the current revision, and a publish during admission fails
//! that admission. Publishing rules are tested at their owner, coral-tools.
use super::provider_setup::{AdminScope, final_value};
use super::*;
use coral_tools::{SharedToolModule, SharedToolOrigin, SharedToolPath, SharedToolPublish};
use serde_json::{Value, json};

/// The Shared Tool every test calls.
const REMIND: &str = "coral.shared.mail.remind";

/// A Workspace whose Owner publishes Shared Tools, with a Member and an
/// Agent who call them.
struct SharedScope {
    scope: AdminScope,
    member: AuthenticatedPrincipal,
    agent: AuthenticatedPrincipal,
    private_tools: PrivateToolModule,
    shared_tools: SharedToolModule,
}

impl SharedScope {
    async fn new(user: &str) -> Self {
        let mut scope = AdminScope::new(user).await;
        let private_tools = PrivateToolModule::new(
            scope.fixture.database.clone(),
            scope.fixture.workspaces.clone(),
            crate::IsolateLimits::default(),
            coral_test_support::TEST_COMPILATION_TIMEOUT,
        )
        .expect("the Private Tool module");
        let shared_tools = SharedToolModule::new(
            scope.fixture.database.clone(),
            scope.fixture.workspaces.clone(),
            crate::IsolateLimits::default(),
            coral_test_support::TEST_COMPILATION_TIMEOUT,
        )
        .expect("the Shared Tool module");
        let index = coral_search::SearchIndex::new(
            scope.fixture.database.clone(),
            Duration::from_secs(3_600),
        );
        scope.fixture.runs = scope
            .fixture
            .runs
            .clone()
            .with_private_tools(crate::PrivateTools::new(private_tools.clone(), index));
        let member_user = organization_user(
            scope.fixture.database.clone(),
            &format!("{user}-member"),
            OrganizationRole::Member,
        )
        .await;
        scope
            .fixture
            .workspaces
            .set_membership(
                &change(scope.owner_user(), 14, '5'),
                &scope.workspace_id,
                &member_user,
                WorkspaceRole::Member,
            )
            .await
            .expect("add the Member");
        let agent =
            registered_agent(&scope.fixture.workspaces, &scope.owner, &scope.workspace_id).await;
        Self {
            member: AuthenticatedPrincipal::user(member_user),
            agent,
            private_tools,
            shared_tools,
            scope,
        }
    }

    /// Saves `author`'s Private Tool at `path` with the module `source`.
    async fn save_private(&self, author: &AuthenticatedPrincipal, path: &str, source: &str) {
        self.private_tools
            .save(
                &PrivateToolChangeContext::new(
                    author.clone(),
                    60,
                    coral_workspace::TraceId::current_or_new(),
                ),
                &self.scope.workspace_id,
                PrivateToolSave::new(
                    PrivateToolPath::parse(path).expect("a path"),
                    PrivateToolDescription::parse("A private tool").expect("a description"),
                    PrivateToolSource::parse(source).expect("a source"),
                ),
            )
            .await
            .expect("save the Private Tool");
    }

    /// Publishes `source` at `coral.shared.mail.remind` as the Owner, from
    /// the Owner's Private Tool `draft`, expecting `expected_revision`.
    async fn publish(&self, source: &str, default: ToolAccessState, expected_revision: u64) {
        self.shared_tools
            .publish(
                &change(self.scope.owner_user(), 70, '6'),
                &self.scope.workspace_id,
                SharedToolPublish::new(
                    SharedToolPath::parse("mail.remind").expect("a path"),
                    SharedToolOrigin::PrivateTool {
                        owner: self.scope.owner.principal().clone(),
                        path: PrivateToolPath::parse("draft").expect("a path"),
                    },
                    PrivateToolDescription::parse("Remind someone").expect("a description"),
                    PrivateToolSource::parse(source).expect("a source"),
                    default,
                    expected_revision,
                ),
            )
            .await
            .expect("publish the Shared Tool");
    }

    /// Who reads the Activity of `principal`'s Runs: a User itself, or the
    /// Owner of an Agent.
    fn reader<'a>(&'a self, principal: &'a AuthenticatedPrincipal) -> &'a AuthenticatedPrincipal {
        if principal.authenticated_user().is_some() {
            principal
        } else {
            &self.scope.owner
        }
    }

    /// The Shared Tool revisions `run` recorded, as (path, revision).
    async fn linked(
        &self,
        principal: &AuthenticatedPrincipal,
        run: &crate::RunId,
    ) -> Vec<(String, u64)> {
        self.scope
            .fixture
            .runs
            .inspect_run(
                self.reader(principal),
                &self.scope.workspace_id,
                run,
                &crate::RunInspection::default(),
            )
            .await
            .expect("inspect the Run")
            .linked_shared_tools
            .into_iter()
            .map(|snapshot| (snapshot.path, snapshot.revision))
            .collect()
    }

    /// Runs `source` as `principal` and returns its Final Value with the
    /// Run.
    async fn run(&self, principal: &AuthenticatedPrincipal, source: &str) -> (crate::RunId, Value) {
        let run = self.scope.start(principal, submission(source)).await;
        let value = final_value(&self.scope.terminal(principal, &run).await);
        (run, value)
    }
}

/// A Shared Tool that calls the caller's own `coral.private.contacts.find`
/// and returns an array with a hole.
const REMIND_SOURCE: &str = "export default async function remind(name: string) {
    const to = await coral.private.contacts.find(name);
    return { to, sent: [1, undefined] };
}";

/// A Private Tool that names whose it is.
fn contacts(owner: &str) -> String {
    format!("export default async (name: string) => `{owner}:${{name}}`;")
}

// Catches: a Shared Tool that only its publisher can call, that returns a
// Tool Result instead of its own value, whose `coral.private` is the
// publisher's instead of the caller's, a Run that does not record the
// revision it linked, or a call recorded as anything but one Shared Tool
// Invocation with its body's calls under it.
#[tokio::test]
async fn every_principal_calls_a_shared_tool_with_its_own_private_tools_and_gets_the_raw_value() {
    let shared = SharedScope::new("user:shared-callers").await;
    let owner = shared.scope.owner.clone();
    let callers = [
        ("owner", owner.clone()),
        ("member", shared.member.clone()),
        ("agent", shared.agent.clone()),
    ];
    for (name, caller) in &callers {
        shared
            .save_private(caller, "contacts.find", &contacts(name))
            .await;
    }
    shared.save_private(&owner, "draft", REMIND_SOURCE).await;
    shared
        .publish(REMIND_SOURCE, ToolAccessState::Enabled, 0)
        .await;

    for (name, caller) in &callers {
        let (run, value) = shared
            .run(caller, "return await coral.shared.mail.remind('Ada');")
            .await;

        // Its own value, copied as data: no `{ data, metadata }` wrapper, and
        // the array's hole arrives as null.
        assert_eq!(
            value,
            json!({ "to": format!("{name}:Ada"), "sent": [1, null] }),
            "{name}"
        );
        assert_eq!(
            shared.linked(caller, &run).await,
            [("mail.remind".to_string(), 1)],
            "{name}"
        );
        let mut calls = Vec::new();
        for row in shared
            .scope
            .fixture
            .invocation_rows(&shared.scope.workspace_id, &run)
            .await
        {
            let summary = shared
                .scope
                .fixture
                .runs
                .inspect_invocation(shared.reader(caller), &shared.scope.workspace_id, &row.id)
                .await
                .expect("inspect the call")
                .summary;
            calls.push((
                summary.call_path,
                summary.tool_kind,
                summary.shared_tool_revision,
                summary.parent.map(|parent| {
                    (
                        parent.call_path,
                        parent.tool_kind,
                        parent.shared_tool_revision,
                    )
                }),
                // Code inside a Shared Tool is never the caller's own source.
                row.source_private_tool_id,
            ));
        }
        calls.sort();
        assert_eq!(
            calls,
            [
                (
                    "coral.private.contacts.find".to_string(),
                    crate::ToolKind::Private,
                    None,
                    Some((REMIND.to_string(), crate::ToolKind::Shared, Some(1))),
                    None,
                ),
                (
                    REMIND.to_string(),
                    crate::ToolKind::Shared,
                    Some(1),
                    None,
                    None,
                ),
            ],
            "{name}"
        );
    }
}

// Catches: a Shared Tool whose `coral.private` call reaches someone else's
// Private Tool when the caller has none, or a Program that names a Shared
// Tool the Workspace does not have being accepted.
#[tokio::test]
async fn admission_refuses_a_linked_tool_that_is_missing_and_names_it() {
    let shared = SharedScope::new("user:shared-missing").await;
    let owner = shared.scope.owner.clone();
    shared
        .save_private(&owner, "contacts.find", &contacts("owner"))
        .await;
    shared.save_private(&owner, "draft", REMIND_SOURCE).await;
    shared
        .publish(REMIND_SOURCE, ToolAccessState::Enabled, 0)
        .await;
    // (case, the Program, what the refusal names)
    let cases = [
        (
            "the caller has no such Private Tool",
            "return await coral.shared.mail.remind('Ada');",
            vec![
                "Shared Tool coral.shared.mail.remind:2:",
                "coral.private.contacts.find",
            ],
        ),
        (
            "the Workspace has no such Shared Tool",
            "return await coral.shared.mail.absent();",
            vec!["Program:1:", "coral.shared.mail.absent"],
        ),
    ];
    let before = shared
        .scope
        .fixture
        .runs
        .count_runs()
        .await
        .expect("count Runs");
    for (case, source, named) in cases {
        let refused = shared
            .scope
            .fixture
            .runs
            .start(
                &shared.member,
                &shared.scope.workspace_id,
                submission(source),
            )
            .await;
        let Err(crate::Error::ProgramAdmission(message)) = refused else {
            panic!("{case}: {refused:?}");
        };
        for expected in named {
            assert!(message.contains(expected), "{case}: {message}");
        }
        assert_eq!(
            shared
                .scope
                .fixture
                .runs
                .count_runs()
                .await
                .expect("count Runs"),
            before,
            "{case}: the refused admission created no Run"
        );
    }
}

// Catches: a Run that keeps a revision a publish replaced while it was
// being admitted, an admission that keeps failing after the publish, or an
// accepted Run that follows a later publish.
#[tokio::test]
async fn a_publish_during_admission_fails_that_admission_once() {
    let shared = SharedScope::new("user:shared-admission").await;
    let owner = shared.scope.owner.clone();
    let revision = |number: u64| format!("export default async () => {number};");
    shared.save_private(&owner, "draft", &revision(1)).await;
    shared
        .publish(&revision(1), ToolAccessState::Enabled, 0)
        .await;
    let runs = &shared.scope.fixture.runs;
    let program = "return await coral.shared.mail.remind();";
    let before = runs.count_runs().await.expect("count Runs");

    let prepared = runs
        .prepare(
            &shared.member,
            &shared.scope.workspace_id,
            submission(program),
        )
        .await
        .expect("prepare against revision 1");
    shared
        .publish(&revision(2), ToolAccessState::Enabled, 1)
        .await;
    let mut transaction = shared.scope.fixture.database.begin().await.expect("begin");
    let refused = runs.accept_in_transaction(&mut transaction, prepared).await;
    transaction.rollback().await.expect("roll back");
    assert!(
        matches!(
            &refused,
            Err(crate::Error::ProgramAdmission(message))
                if message == "Shared Tool changed during Program admission"
        ),
        "{:?}",
        refused.as_ref().err()
    );
    assert_eq!(
        runs.count_runs().await.expect("count Runs"),
        before,
        "the refused admission created no Run"
    );

    // The next admission links the new revision.
    let (run, value) = shared.run(&shared.member, program).await;
    assert_eq!(value, json!(2));
    assert_eq!(
        shared.linked(&shared.member, &run).await,
        [("mail.remind".to_string(), 2)]
    );

    // A Run already accepted keeps the revision it linked.
    shared
        .publish(&revision(3), ToolAccessState::Enabled, 2)
        .await;
    assert_eq!(
        shared.linked(&shared.member, &run).await,
        [("mail.remind".to_string(), 2)]
    );
}

// Catches: a Shared Tool that ignores the default its publishing Owner
// chose, or a body that runs although its call is disabled.
#[tokio::test]
async fn a_shared_tool_call_follows_the_default_its_owner_chose_at_publish() {
    let shared = SharedScope::new("user:shared-policy").await;
    let owner = shared.scope.owner.clone();
    let source = "export default async () => { await coral.emit('body ran'); return 1; }";
    shared.save_private(&owner, "draft", source).await;
    shared.publish(source, ToolAccessState::Disabled, 0).await;
    let program = "try { return await coral.shared.mail.remind(); } catch (error) {
        if (!(error instanceof coral.ToolError)) throw error;
        return { code: error.code };
    }";

    let (run, value) = shared.run(&shared.member, program).await;
    assert_eq!(value, json!({ "code": "tool_disabled" }));
    let update = shared.scope.terminal(&shared.member, &run).await;
    assert!(
        !update
            .events()
            .iter()
            .any(|event| matches!(event.kind(), RunEventKind::ProgramEmitted { .. })),
        "the body ran: {:?}",
        update.events()
    );

    // The next revision's default applies to the next Run.
    shared.publish(source, ToolAccessState::Enabled, 1).await;
    let (_run, value) = shared.run(&shared.member, program).await;
    assert_eq!(value, json!(1));
}
