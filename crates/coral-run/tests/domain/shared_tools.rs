//! Calls to `coral.shared.*`, over the real V8 engine: every Principal in
//! the Workspace calls a Shared Tool with its own authority, each Run links
//! and records the current revision, a publish or a removal during admission
//! fails that admission, and a removed Shared Tool fails the next one.
//! Publishing and removal rules are tested at their owner, coral-tools.
use super::provider_setup::{AdminScope, final_value};
use super::*;
use crate::{SourceDocument, SourceLocation};
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
    /// The workflow Reef publishes and removes through, which notifies
    /// Universal Search after each commit.
    workflow: coral_administration::SharedToolWorkflow,
    index: coral_search::SearchIndex,
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
        scope.fixture.runs =
            scope
                .fixture
                .runs
                .clone()
                .with_private_tools(crate::PrivateTools::new(
                    private_tools.clone(),
                    index.clone(),
                ));
        let workflow = coral_administration::SharedToolWorkflow::new(
            scope.fixture.database.clone(),
            shared_tools.clone(),
            coral_search::UniversalSearchModule::new(
                scope.fixture.database.clone(),
                scope.fixture.workspaces.clone(),
            ),
            index.clone(),
        );
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
            workflow,
            index,
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
                    AuthoredToolDescription::parse("A private tool").expect("a description"),
                    AuthoredToolSource::parse(source).expect("a source"),
                ),
            )
            .await
            .expect("save the Private Tool");
    }

    /// Publishes `source` at `coral.shared.mail.remind` as the Owner, from
    /// the Owner's Private Tool `draft`, expecting `expected_revision`, as
    /// Reef does: Universal Search hears of it after the commit.
    async fn publish(&self, source: &str, default: ToolAccessState, expected_revision: u64) {
        let review = SharedToolPublish::review(
            SharedToolPath::parse("mail.remind").expect("a path"),
            SharedToolOrigin::PrivateTool {
                owner: self.scope.owner.principal().clone(),
                path: PrivateToolPath::parse("draft").expect("a path"),
            },
            AuthoredToolDescription::parse("Remind someone").expect("a description"),
            AuthoredToolSource::parse(source).expect("a source"),
            default,
            expected_revision,
        );
        // The Owner approves the review Reef shows them.
        Box::pin(self.workflow.publish_as_reviewed(
            &change(self.scope.owner_user(), 70, '6'),
            &self.scope.workspace_id,
            review,
        ))
        .await
        .expect("publish the Shared Tool");
    }

    /// Removes `coral.shared.mail.remind` as the Owner, at its revision
    /// `expected_revision`, as Reef does.
    async fn remove(&self, expected_revision: u64) {
        self.workflow
            .remove(
                &change(self.scope.owner_user(), 71, '6'),
                &self.scope.workspace_id,
                &SharedToolPath::parse("mail.remind").expect("a path"),
                expected_revision,
            )
            .await
            .expect("remove the Shared Tool");
    }

    /// Prepares `program` as the Member, applies `change`, then accepts it:
    /// the refusal of the admission that `change` interrupted.
    async fn admission_interrupted_by(
        &self,
        program: &str,
        change: impl Future<Output = ()>,
    ) -> String {
        let runs = &self.scope.fixture.runs;
        let prepared = runs
            .prepare(&self.member, &self.scope.workspace_id, submission(program))
            .await
            .expect("prepare before the change");
        change.await;
        let mut transaction = self.scope.fixture.database.begin().await.expect("begin");
        let accepted = runs.accept_in_transaction(&mut transaction, prepared).await;
        transaction.rollback().await.expect("roll back");
        match accepted {
            Err(crate::Error::ProgramAdmission(message)) => message,
            other => panic!("the admission was not refused: {:?}", other.err()),
        }
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
        self.snapshots(principal, run)
            .await
            .into_iter()
            .map(|snapshot| (snapshot.path, snapshot.revision))
            .collect()
    }

    /// The Shared Tool snapshots `run` recorded.
    async fn snapshots(
        &self,
        principal: &AuthenticatedPrincipal,
        run: &crate::RunId,
    ) -> Vec<crate::SharedSourceSnapshot> {
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
                summary
                    .parent
                    .map(|parent| (parent.call_path, parent.tool_kind)),
                summary.source_location,
            ));
        }
        calls.sort_by(|left, right| left.0.cmp(&right.0));
        // A call the Shared Tool's body made is located in the snapshot of
        // the revision the Run linked, never in the caller's own source.
        let [snapshot] = shared
            .snapshots(caller, &run)
            .await
            .try_into()
            .expect("one snapshot");
        assert_eq!(snapshot.source, REMIND_SOURCE, "{name}");
        let in_shared_tool = SourceLocation {
            document: SourceDocument::SharedTool {
                shared_tool_id: snapshot.shared_tool_id,
            },
            line: 2,
            column: 45,
        };
        let in_program = SourceLocation {
            document: SourceDocument::Program,
            line: 1,
            column: 32,
        };
        assert_eq!(
            calls,
            [
                (
                    "coral.private.contacts.find".to_string(),
                    crate::ToolKind::Private,
                    Some((REMIND.to_string(), crate::ToolKind::Shared { revision: 1 })),
                    Some(in_shared_tool),
                ),
                (
                    REMIND.to_string(),
                    crate::ToolKind::Shared { revision: 1 },
                    None,
                    Some(in_program),
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

    let refused = shared
        .admission_interrupted_by(
            program,
            shared.publish(&revision(2), ToolAccessState::Enabled, 1),
        )
        .await;
    assert_eq!(refused, "Shared Tool changed during Program admission");
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

// Catches: a Program or a Private Tool that still links a removed Shared
// Tool, an admission that accepts a Shared Tool removed while it was being
// admitted, or a Run already accepted that loses the revision it linked.
#[tokio::test]
async fn a_removal_fails_the_next_admission_and_leaves_an_accepted_run_as_it_was() {
    let shared = SharedScope::new("user:shared-removal").await;
    let owner = shared.scope.owner.clone();
    let source = "export default async () => 1;";
    shared.save_private(&owner, "draft", source).await;
    shared.publish(source, ToolAccessState::Enabled, 0).await;
    shared
        .save_private(
            &shared.member,
            "relay",
            "export default async () => coral.shared.mail.remind();",
        )
        .await;
    let runs = &shared.scope.fixture.runs;
    let program = "return await coral.shared.mail.remind();";
    let (accepted, value) = shared.run(&shared.member, program).await;
    assert_eq!(value, json!(1));
    let linked = async || {
        shared
            .snapshots(&shared.member, &accepted)
            .await
            .into_iter()
            .map(|snapshot| (snapshot.path, snapshot.revision, snapshot.source))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        linked().await,
        [("mail.remind".to_string(), 1, source.to_string())]
    );
    let before = runs.count_runs().await.expect("count Runs");

    let refused = shared
        .admission_interrupted_by(program, shared.remove(1))
        .await;
    assert_eq!(refused, "Shared Tool changed during Program admission");

    // The next admission finds no Tool at the path, in the Program and in
    // a Private Tool that still calls it.
    // (case, the Program, what the refusal names)
    let cases = [
        (
            "a Program calls it",
            program,
            [
                "Program:1:",
                "coral.shared.mail.remind names no Tool that you can call",
            ],
        ),
        (
            "a Private Tool calls it",
            "return await coral.private.relay();",
            [
                "Private Tool coral.private.relay:1:",
                "coral.shared.mail.remind names no Tool that you can call",
            ],
        ),
    ];
    for (case, source, named) in cases {
        let refused = runs
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
    }
    assert_eq!(
        runs.count_runs().await.expect("count Runs"),
        before,
        "no refused admission created a Run"
    );

    // The Run accepted before the removal keeps the revision it linked.
    assert_eq!(
        linked().await,
        [("mail.remind".to_string(), 1, source.to_string())]
    );
}

// Catches: a Run accepted before a removal that fails, or loses its body,
// when it calls the removed Shared Tool afterwards.
#[tokio::test]
async fn a_run_accepted_before_a_removal_still_runs_the_revision_it_linked() {
    let shared = SharedScope::new("user:shared-removal-accepted").await;
    let owner = shared.scope.owner.clone();
    let source = "export default async () => 'linked before the removal';";
    shared.save_private(&owner, "draft", source).await;
    // The call waits for an approval, so the Run is accepted with its body
    // not yet run.
    shared
        .publish(source, ToolAccessState::ApprovalRequired, 0)
        .await;
    let run = shared
        .scope
        .start(
            &shared.member,
            submission("return await coral.shared.mail.remind();"),
        )
        .await;
    let approvals = shared.scope.fixture.approvals();
    let request =
        wait_for_approval(&approvals, &shared.member, &shared.scope.workspace_id, &run).await;

    shared.remove(1).await;

    approvals
        .decide(
            &shared.member,
            &shared.scope.workspace_id,
            &request.id,
            crate::approval::ApprovalDecision::Approve,
            None,
            &TraceId::parse(&"d".repeat(32)).unwrap(),
        )
        .await
        .expect("approve the call");
    let update = shared.scope.terminal(&shared.member, &run).await;
    assert_eq!(final_value(&update), json!("linked before the removal"));
    assert_eq!(
        shared.linked(&shared.member, &run).await,
        [("mail.remind".to_string(), 1)]
    );
}

// Catches: a publish or a removal that a Program's `coral.search` does not
// follow: a Shared Tool that a Member or an Agent cannot find or describe,
// a description that names its publisher or lacks the source and the current
// revision, and a removed Shared Tool that search still returns or describes.
// Ranking, visibility and policy are tested at their owner, coral-search.
#[tokio::test]
async fn every_principal_finds_and_describes_a_shared_tool_until_it_is_removed() {
    let shared = SharedScope::new("user:shared-search").await;
    let owner = shared.scope.owner.clone();
    shared
        .save_private(&owner, "contacts.find", &contacts("owner"))
        .await;
    shared.save_private(&owner, "draft", REMIND_SOURCE).await;
    shared
        .publish(REMIND_SOURCE, ToolAccessState::Enabled, 0)
        .await;
    shared.index.settled().await;
    let callers = [
        ("member", shared.member.clone()),
        ("agent", shared.agent.clone()),
    ];
    let search = "const [found, exact] = await Promise.all([
        coral.search({ query: 'remind someone' }),
        coral.search({ paths: ['coral.shared.mail.remind'] }),
    ]);
    return {
        hits: found.data.hits.map((hit) => hit.call_path),
        descriptions: exact.data.descriptions,
        missing: exact.data.missing.map((missing) => missing.call_path),
    };";

    for (name, caller) in &callers {
        let (_run, found) = shared.run(caller, search).await;
        assert_eq!(
            found.pointer("/hits"),
            Some(&json!([REMIND])),
            "{name}: {found}"
        );
        let Some([described]) = found
            .pointer("/descriptions")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
        else {
            panic!("{name}: one description in {found}");
        };
        for (field, expected) in [
            ("/kind", json!("shared_tool")),
            ("/call_path", json!(REMIND)),
            ("/source", json!(REMIND_SOURCE)),
            ("/revision", json!(1)),
        ] {
            assert_eq!(described.pointer(field), Some(&expected), "{name}: {field}");
        }
        // The publisher is attribution only: no description carries it.
        let publisher = owner.principal().id().as_str();
        assert!(
            !described.to_string().contains(publisher),
            "{name}: {described}"
        );
    }

    shared.remove(1).await;
    shared.index.settled().await;
    for (name, caller) in &callers {
        let (_run, found) = shared.run(caller, search).await;
        assert_eq!(
            found,
            json!({ "hits": [], "descriptions": [], "missing": [REMIND] }),
            "{name}"
        );
    }
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

// Catches: an Owner's exact rule on a Shared Tool that does not override
// its published default for some caller, that a new revision drops, a body
// that runs before its call is approved, or an Approval Request that reads
// as not actionable because the approval view decided the call as the
// Member's own Private Tool.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one scenario follows a rule through an approval and a new revision"
)]
async fn an_owners_rule_on_a_shared_tool_applies_to_every_caller_and_survives_a_new_revision() {
    let shared = SharedScope::new("user:shared-rule").await;
    let owner = shared.scope.owner.clone();
    // The body calls the caller's own Private Tool, so the Run records a
    // second Invocation once the body runs.
    shared
        .save_private(&owner, "contacts.find", &contacts("owner"))
        .await;
    shared.save_private(&owner, "draft", REMIND_SOURCE).await;
    shared
        .publish(REMIND_SOURCE, ToolAccessState::Enabled, 0)
        .await;
    shared
        .save_private(&shared.member, "contacts.find", &contacts("member"))
        .await;
    let tool = shared
        .shared_tools
        .get(
            &owner,
            &shared.scope.workspace_id,
            &SharedToolPath::parse("mail.remind").expect("a path"),
        )
        .await
        .expect("the Shared Tool");
    let database = shared.scope.fixture.database.clone();
    let mut transaction = database.begin_read_snapshot().await.expect("begin");
    let provider = coral_provider::authored_tool_provider_in_transaction(
        &mut transaction,
        &shared.scope.workspace_id,
        coral_catalog::AuthoredToolKind::Shared,
    )
    .await
    .expect("read the Shared Provider")
    .expect("the Shared Provider");
    transaction.commit().await.expect("commit");
    // The Owner's rule, set as Reef sets it, over the published `enabled`.
    let set_rule = |state: ToolAccessState, at: i64| {
        let (policies, context, target) = (
            shared.scope.fixture.policies.clone(),
            change(shared.scope.owner_user(), at, '7'),
            ToolPolicyTarget::new(
                shared.scope.workspace_id.clone(),
                provider.id().clone(),
                tool.id().tool_key(),
            ),
        );
        async move {
            policies
                .set_access_state(&context, target, state)
                .await
                .expect("set the Owner's rule");
        }
    };
    set_rule(ToolAccessState::ApprovalRequired, 80).await;

    let run = shared
        .scope
        .start(
            &shared.member,
            submission("return await coral.shared.mail.remind('Ada');"),
        )
        .await;
    let approvals = shared.scope.fixture.approvals();
    let request =
        wait_for_approval(&approvals, &shared.member, &shared.scope.workspace_id, &run).await;
    assert!(request.actionable, "the Member can decide the request");
    let called = || async {
        shared
            .scope
            .fixture
            .invocation_rows(&shared.scope.workspace_id, &run)
            .await
            .into_iter()
            .map(|row| row.call_path)
            .collect::<Vec<_>>()
    };
    // The call waits on its decision with its body not yet run.
    assert_eq!(called().await, [REMIND]);
    approvals
        .decide(
            &shared.member,
            &shared.scope.workspace_id,
            &request.id,
            crate::approval::ApprovalDecision::Approve,
            None,
            &TraceId::parse(&"d".repeat(32)).unwrap(),
        )
        .await
        .expect("approve the call");
    let update = shared.scope.terminal(&shared.member, &run).await;
    assert_eq!(
        final_value(&update).pointer("/to"),
        Some(&json!("member:Ada"))
    );
    // The approved body ran, and made its own call.
    let mut after = called().await;
    after.sort_unstable();
    assert_eq!(after, ["coral.private.contacts.find", REMIND]);

    // The rule is the Shared Tool's, not a revision's: it still decides
    // every caller's call after a new revision that declares `enabled`.
    set_rule(ToolAccessState::Disabled, 81).await;
    shared
        .publish(REMIND_SOURCE, ToolAccessState::Enabled, 1)
        .await;
    // Admission links the Private Tool the body names for each caller.
    shared
        .save_private(&shared.agent, "contacts.find", &contacts("agent"))
        .await;
    let program = "try { return await coral.shared.mail.remind('Ada'); } catch (error) {
        if (!(error instanceof coral.ToolError)) throw error;
        return { code: error.code };
    }";
    for (name, caller) in [
        ("owner", &owner),
        ("member", &shared.member),
        ("agent", &shared.agent),
    ] {
        let (run, value) = shared.run(caller, program).await;
        assert_eq!(value, json!({ "code": "tool_disabled" }), "{name}");
        assert_eq!(
            shared.linked(caller, &run).await,
            [("mail.remind".to_owned(), 2)],
            "{name}"
        );
        // The body made no call.
        let called: Vec<_> = shared
            .scope
            .fixture
            .invocation_rows(&shared.scope.workspace_id, &run)
            .await
            .into_iter()
            .map(|row| row.call_path)
            .collect();
        assert_eq!(called, [REMIND], "{name}");
    }
}
