//! `coral.addTool`, `coral.updateTool` and `coral.removeTool` author the
//! invoking Principal's Private Tools from inside a Program, over the real
//! V8 engine. Authoring rules are tested at their owner, coral-tools, and
//! the default Access State at coral-policy.
use super::provider_setup::{AdminScope, RunEndsAfterPreparation, final_value, workspace_call};
use super::*;
use coral_policy::ToolAccessState;
use serde_json::{Value, json};

/// The function each Program saves: `saved`, declared at its top level.
const SOURCE: &str = "const saved = ({ name }: { name: string }): string => `hello ${name}`;";

/// An Admin scope whose Runs author Private Tools, with no exact rule on
/// either Tool, and the index each save notifies.
async fn authoring_scope(user: &str) -> (AdminScope, coral_search::SearchIndex) {
    let mut scope = AdminScope::new(user).await;
    let module = private_tool_module(&scope);
    let index =
        coral_search::SearchIndex::new(scope.fixture.database.clone(), Duration::from_secs(3_600));
    scope.fixture.runs = scope
        .fixture
        .runs
        .clone()
        .with_private_tools(crate::PrivateTools::new(module, index.clone()));
    (scope, index)
}

/// The module that stores `scope`'s Private Tools.
fn private_tool_module(scope: &AdminScope) -> PrivateToolModule {
    PrivateToolModule::new(
        scope.fixture.database.clone(),
        scope.fixture.workspaces.clone(),
        crate::IsolateLimits::default(),
        coral_test_support::TEST_COMPILATION_TIMEOUT,
    )
    .unwrap()
}

/// A Workspace member who does not own the Workspace.
async fn member(scope: &AdminScope, user: &str) -> AuthenticatedPrincipal {
    let member = organization_user(
        scope.fixture.database.clone(),
        user,
        OrganizationRole::Member,
    )
    .await;
    scope
        .fixture
        .workspaces
        .set_membership(
            &change(scope.owner_user(), 14, '5'),
            &scope.workspace_id,
            &member,
            WorkspaceRole::Member,
        )
        .await
        .unwrap();
    AuthenticatedPrincipal::user(member)
}

/// A Program that declares `declarations`, which declare `saved`, and adds
/// `saved` at `path`. It returns a Tool error's code and message.
fn add(path: &str, declarations: &str) -> RunSubmission {
    author("addTool", path, declarations, "")
}

fn greet() -> RunSubmission {
    submission("return coral.private.people.greet({ name: 'Ada' });")
}

/// Runs `run` as `actor` and returns its Final Value.
async fn outcome(scope: &AdminScope, actor: &AuthenticatedPrincipal, run: RunSubmission) -> Value {
    let run = scope.start(actor, run).await;
    final_value(&scope.terminal(actor, &run).await)
}

/// Whether admission refuses `run`, as it does a Program that names a
/// Private Tool the caller does not have.
async fn admission_refuses(
    scope: &AdminScope,
    actor: &AuthenticatedPrincipal,
    run: RunSubmission,
) -> bool {
    matches!(
        scope
            .fixture
            .runs
            .start(actor, &scope.workspace_id, run)
            .await,
        Err(crate::Error::ProgramAdmission(_))
    )
}

/// Who reads the Activity and approvals of `principal`'s Runs: a User
/// itself, or the Owner of an Agent.
fn reader<'a>(
    scope: &'a AdminScope,
    principal: &'a AuthenticatedPrincipal,
) -> &'a AuthenticatedPrincipal {
    if principal.authenticated_user().is_some() {
        principal
    } else {
        &scope.owner
    }
}

/// How many Invocations `run` recorded, as Activity shows them.
async fn invocations(
    scope: &AdminScope,
    principal: &AuthenticatedPrincipal,
    run: &crate::RunId,
) -> usize {
    scope
        .fixture
        .runs
        .activity_entries(
            reader(scope, principal),
            &scope.workspace_id,
            run,
            &crate::RunEntryWindow::Start,
            &crate::RunEntryLevel::Every,
            100,
            None,
        )
        .await
        .unwrap()
        .entries
        .iter()
        .filter(|entry| matches!(entry, crate::RunEntry::Invocation(_)))
        .count()
}

// Catches: a Principal that cannot add a tool without an Owner's rule, or
// waits for approval; a tool a later Program cannot call, recorded as
// anything but an Invocation of its Run; a Program adding into another
// Principal's namespace; or a removal that leaves the tool callable.
#[tokio::test]
async fn each_principal_adds_and_removes_its_own_tool_with_no_rule() {
    let (scope, _index) = authoring_scope("user:functions-owner").await;
    let member = member(&scope, "user:functions-member").await;
    let agent =
        registered_agent(&scope.fixture.workspaces, &scope.owner, &scope.workspace_id).await;
    let principals = [scope.owner.clone(), member, agent];
    for principal in &principals {
        let run = scope.start(principal, add("people.greet", SOURCE)).await;
        let saved = final_value(&scope.terminal(principal, &run).await);
        assert_eq!(
            saved.pointer("/data/call_path"),
            Some(&json!("coral.private.people.greet")),
            "{principal:?}: {saved}"
        );
        assert!(
            saved
                .pointer("/data/declaration")
                .and_then(Value::as_str)
                .is_some_and(|declaration| declaration.contains("name: string")),
            "{saved}"
        );
        assert_eq!(
            invocations(&scope, principal, &run).await,
            1,
            "{principal:?}"
        );
        let approvals = scope
            .fixture
            .approvals()
            .list_for_run(reader(&scope, principal), &scope.workspace_id, &run)
            .await
            .unwrap();
        assert!(approvals.is_empty(), "{principal:?}: {approvals:?}");
    }
    // Each Principal saved its own; none replaced another's.
    for principal in &principals {
        assert_eq!(
            outcome(&scope, principal, greet()).await,
            json!("hello Ada"),
            "{principal:?}"
        );
    }
    for principal in &principals {
        let removed = outcome(&scope, principal, remove("people.greet")).await;
        assert_eq!(
            removed.pointer("/data/call_path"),
            Some(&json!("coral.private.people.greet")),
            "{removed}"
        );
        assert!(admission_refuses(&scope, principal, greet()).await);
    }
}

// Catches: a save that throws on an advisory type error, admits a function
// that cannot become a Private Tool, or saves at a path that cannot hold it.
#[tokio::test]
async fn type_errors_are_advisory_and_invalid_private_tools_save_nothing() {
    let (scope, _index) = authoring_scope("user:functions-diagnostics").await;
    let typed = outcome(
        &scope,
        &scope.owner,
        add(
            "people.count",
            "const saved = (): number => { const n: number = 'one'; return n; };",
        ),
    )
    .await;
    assert_eq!(
        typed.pointer("/data/diagnostics/0/category"),
        Some(&json!("error")),
        "{typed}"
    );

    // Admission extracts the function from the Program, so the Program
    // itself is refused.
    for (path, declarations) in [
        ("people.broken", "const saved = ("),
        (
            "people.imports",
            "import x from 'y';\nconst saved = () => x;",
        ),
    ] {
        assert!(
            admission_refuses(&scope, &scope.owner, add(path, declarations)).await,
            "{path}"
        );
    }
    // `people` is a namespace that holds people.count.
    for path in ["1bad", "people"] {
        let refused = outcome(&scope, &scope.owner, add(path, SOURCE)).await;
        assert_eq!(
            refused.get("code"),
            Some(&json!("invalid_input")),
            "{path}: {refused}"
        );
        assert!(
            refused
                .get("message")
                .and_then(Value::as_str)
                .is_some_and(|message| message.contains("`path`")),
            "{path}: {refused}"
        );
    }
    for path in ["people.broken", "people.imports", "1bad", "people"] {
        assert!(
            admission_refuses(
                &scope,
                &scope.owner,
                submission(&format!("return coral.private.{path}();")),
            )
            .await,
            "{path}: nothing was saved"
        );
    }
}

// Catches: a save that an Owner's rule holds for approval and that does not
// compile and commit once approved. coral-policy owns which rule applies.
#[tokio::test]
async fn an_approved_save_compiles_and_commits() {
    let (scope, _index) = authoring_scope("user:functions-rules").await;
    scope
        .set_access_to("coral.addTool", ToolAccessState::ApprovalRequired)
        .await;
    let run = scope.start(&scope.owner, add("people.greet", SOURCE)).await;
    scope.approve(&scope.owner, &run).await;
    let saved = final_value(&scope.terminal(&scope.owner, &run).await);
    assert_eq!(
        saved.pointer("/data/call_path"),
        Some(&json!("coral.private.people.greet")),
        "{saved}"
    );
    assert_eq!(
        outcome(&scope, &scope.owner, greet()).await,
        json!("hello Ada")
    );
}

// Catches: a save undone when its Run fails afterwards, or a function that
// search finds only after a reconcile pass.
#[tokio::test]
async fn a_save_outlives_its_failed_run_and_search_finds_it() {
    let (scope, index) = authoring_scope("user:functions-outlives").await;
    let run = scope
        .start(
            &scope.owner,
            submission(&format!(
                "{SOURCE}\nawait coral.addTool({{ path: 'people.greet', \
                 description: 'Greet one person by name.', source: saved }});\n\
                 throw new Error('after the save');"
            )),
        )
        .await;
    let terminal = scope.terminal(&scope.owner, &run).await;
    assert!(
        matches!(
            terminal.terminal_outcome(),
            Some(RunTerminalOutcome::Failed { .. })
        ),
        "{terminal:?}"
    );
    assert_eq!(
        outcome(&scope, &scope.owner, greet()).await,
        json!("hello Ada")
    );

    index.settled().await;
    let found = outcome(
        &scope,
        &scope.owner,
        workspace_call("search", &json!({ "query": "greet person" }).to_string()),
    )
    .await;
    assert!(
        found
            .pointer("/data/hits")
            .and_then(Value::as_array)
            .is_some_and(|hits| hits
                .iter()
                .any(|hit| { hit.get("call_path") == Some(&json!("coral.private.people.greet")) })),
        "{found}"
    );
}

// Catches: a save committed for a Run that ended while its source compiled.
#[tokio::test]
async fn a_run_that_ends_while_its_save_compiles_saves_nothing() {
    let (mut scope, _index) = authoring_scope("user:functions-ended").await;
    scope.fixture.runs.execution_policy =
        crate::execution::ExecutionPolicy::testing_with_claim_lease(Duration::from_mins(15));
    scope.fixture.runs.invocation_observer = Arc::new(RunEndsAfterPreparation {
        clock: scope.fixture.clock.clone(),
    });
    let run = scope.start(&scope.owner, add("people.greet", SOURCE)).await;
    assert_eq!(
        scope.terminal(&scope.owner, &run).await.state(),
        RunState::TimedOut
    );
    assert!(admission_refuses(&scope, &scope.owner, greet()).await);
}

// Catches: a Program that cannot test the function it saves before saving
// it, or that names it by its path in the same Run, which admission links
// before the save exists.
#[tokio::test]
async fn a_program_calls_its_function_then_saves_it_in_one_run() {
    let (scope, _index) = authoring_scope("user:functions-same-run").await;
    let by_path = format!(
        "{SOURCE}\nawait coral.addTool({{ path: 'people.greet', \
         description: 'Greet one person by name.', source: saved }});\n\
         return coral.private.people.greet({{ name: 'Ada' }});"
    );
    assert!(admission_refuses(&scope, &scope.owner, submission(&by_path)).await);
    assert!(
        admission_refuses(&scope, &scope.owner, greet()).await,
        "the refused Program saved nothing"
    );

    let tested = format!(
        "{SOURCE}\nconst greeting = saved({{ name: 'Ada' }});\n\
         await coral.addTool({{ path: 'people.greet', \
         description: 'Greet one person by name.', source: saved }});\n\
         return greeting;"
    );
    assert_eq!(
        outcome(&scope, &scope.owner, submission(&tested)).await,
        json!("hello Ada")
    );
    assert_eq!(
        outcome(&scope, &scope.owner, greet()).await,
        json!("hello Ada")
    );
}

// Catches: a Private Tool that cannot add a tool from a function it
// declares, or remove one, when a later Program calls it.
#[tokio::test]
async fn a_private_tool_adds_and_removes_tools() {
    let (scope, _index) = authoring_scope("user:functions-from-functions").await;
    let tools = format!(
        "{SOURCE}
async function saver() {{
    return coral.addTool({{ path: 'people.greet', description: 'Greet one person by name.', source: saved }});
}}
async function remover() {{
    return coral.removeTool({{ path: 'people.greet' }});
}}
const saving = await coral.addTool({{ path: 'tools.saver', description: 'Add the greeting.', source: saver }});
const removing = await coral.addTool({{ path: 'tools.remover', description: 'Remove the greeting.', source: remover }});
return [saving.data.diagnostics, removing.data.diagnostics];"
    );
    // A function is the type `source` takes, so neither save has a type error.
    assert_eq!(
        outcome(&scope, &scope.owner, submission(&tools)).await,
        json!([[], []])
    );
    assert!(admission_refuses(&scope, &scope.owner, greet()).await);

    let saved = outcome(
        &scope,
        &scope.owner,
        submission("return await coral.private.tools.saver();"),
    )
    .await;
    assert_eq!(
        saved.pointer("/data/call_path"),
        Some(&json!("coral.private.people.greet")),
        "{saved}"
    );
    assert_eq!(
        outcome(&scope, &scope.owner, greet()).await,
        json!("hello Ada")
    );

    outcome(
        &scope,
        &scope.owner,
        submission("return await coral.private.tools.remover();"),
    )
    .await;
    assert!(admission_refuses(&scope, &scope.owner, greet()).await);
}

// Catches: authoring Tools that run where no Private Tool module is wired.
#[tokio::test]
async fn without_private_tools_the_tools_are_unavailable() {
    let scope = AdminScope::new("user:functions-unwired").await;
    for run in [add("people.greet", SOURCE), remove("people.greet")] {
        let refused = outcome(&scope, &scope.owner, run).await;
        assert_eq!(
            refused.get("code"),
            Some(&json!("tool_unavailable")),
            "{refused}"
        );
    }
}

// Catches: a Built-in Tool or Private Tool that its root on `coral` does not
// reach, or a result or description that reports another root.
#[tokio::test]
async fn programs_reach_the_builtin_and_private_tools_on_coral() {
    let (scope, _index) = authoring_scope("user:functions-roots").await;
    let saved = outcome(&scope, &scope.owner, add("people.greet", SOURCE)).await;
    assert_eq!(
        saved.pointer("/data/call_path"),
        Some(&json!("coral.private.people.greet")),
        "{saved}"
    );
    assert_eq!(
        outcome(&scope, &scope.owner, greet()).await,
        json!("hello Ada")
    );
    let described = outcome(
        &scope,
        &scope.owner,
        submission(
            "const { data } = await coral.search({ paths: \
             ['coral.search', 'coral.private.people.greet'] });\n\
             return data.descriptions.map((description) => description.call_path);",
        ),
    )
    .await;
    assert_eq!(
        described,
        json!(["coral.search", "coral.private.people.greet"])
    );
}

/// The function `addTool` and `updateTool` save in place of `saved`: it
/// says goodbye, so a call shows which version is current.
const OTHER_SOURCE: &str = "const saved = ({ name }: { name: string }): string => `bye ${name}`;";

/// A Program that declares `declarations` and returns `expression`'s
/// value, or a Tool error's code and message.
fn tool_call(declarations: &str, expression: &str) -> RunSubmission {
    submission(&format!(
        "{declarations}
try {{
    return await {expression};
}} catch (error) {{
    if (!(error instanceof coral.ToolError)) throw error;
    return {{ code: error.code, message: error.message }};
}}"
    ))
}

/// `coral.addTool` or `coral.updateTool` saving `saved` from
/// `declarations` at `path`, with `extra` fields after `source`.
fn author(verb: &str, path: &str, declarations: &str, extra: &str) -> RunSubmission {
    tool_call(
        declarations,
        &format!(
            "coral.{verb}({{ path: {path}, description: 'Greet one person by name.', \
             source: saved{extra} }})",
            path = json!(path),
        ),
    )
}

fn remove(path: &str) -> RunSubmission {
    tool_call(
        "",
        &format!("coral.removeTool({{ path: {path} }})", path = json!(path)),
    )
}

/// The actions of `actor`'s lifecycle Events, oldest first.
async fn lifecycle_actions(
    scope: &AdminScope,
    functions: &PrivateToolModule,
    actor: &AuthenticatedPrincipal,
) -> Vec<coral_tools::LifecycleAction> {
    let mut events = functions
        .list_lifecycle_events(actor, &scope.workspace_id)
        .await
        .unwrap();
    events.sort_by_key(coral_tools::AdministrativeLifecycleEvent::occurred_at_unix_nanos);
    events
        .iter()
        .map(coral_tools::AdministrativeLifecycleEvent::action)
        .collect()
}

// Catches: an authoring Tool whose refusal reaches the Program under any
// code but its own, or whose result reports the wrong path or revision.
// coral-tools tests that a refusal changes nothing.
#[tokio::test]
async fn create_only_replace_only_and_revision_conflicts_reach_the_program_with_their_codes() {
    use coral_tools::LifecycleAction::{Created, Deleted, Replaced};
    let (scope, _index) = authoring_scope("user:tools-intents").await;
    let functions = private_tool_module(&scope);
    let before = lifecycle_actions(&scope, &functions, &scope.owner).await;
    let added = outcome(
        &scope,
        &scope.owner,
        author("addTool", "people.greet", SOURCE, ""),
    )
    .await;
    assert_eq!(
        (
            added.pointer("/data/call_path"),
            added.pointer("/data/revision")
        ),
        (Some(&json!("coral.private.people.greet")), Some(&json!(1))),
        "{added}"
    );

    for (run, code) in [
        (
            author("addTool", "people.greet", OTHER_SOURCE, ""),
            "private_tool_exists",
        ),
        (
            author("updateTool", "people.missing", OTHER_SOURCE, ""),
            "private_tool_not_found",
        ),
        (
            author(
                "updateTool",
                "people.greet",
                OTHER_SOURCE,
                ", expectedRevision: 2",
            ),
            "private_tool_revision_conflict",
        ),
        (remove("people.missing"), "private_tool_not_found"),
    ] {
        let refused = outcome(&scope, &scope.owner, run).await;
        assert_eq!(refused.get("code"), Some(&json!(code)), "{refused}");
    }

    for (run, revision) in [
        (
            author(
                "updateTool",
                "people.greet",
                OTHER_SOURCE,
                ", expectedRevision: 1",
            ),
            2,
        ),
        (author("updateTool", "people.greet", SOURCE, ""), 3),
    ] {
        let updated = outcome(&scope, &scope.owner, run).await;
        assert_eq!(
            updated.pointer("/data/revision"),
            Some(&json!(revision)),
            "{updated}"
        );
    }
    assert_eq!(
        outcome(&scope, &scope.owner, greet()).await,
        json!("hello Ada")
    );
    let removed = outcome(&scope, &scope.owner, remove("people.greet")).await;
    assert_eq!(
        removed.pointer("/data/call_path"),
        Some(&json!("coral.private.people.greet")),
        "{removed}"
    );
    assert!(admission_refuses(&scope, &scope.owner, greet()).await);
    let after = lifecycle_actions(&scope, &functions, &scope.owner).await;
    assert_eq!(
        after.get(before.len()..),
        Some(&[Created, Replaced, Replaced, Deleted][..])
    );
}

// Catches: an authoring Tool an Agent or Member cannot reach without a
// rule, or one that adds, updates or removes another Principal's tool.
#[tokio::test]
async fn each_principal_adds_updates_and_removes_only_its_own_tools() {
    let (scope, _index) = authoring_scope("user:tools-own").await;
    let member = member(&scope, "user:tools-member").await;
    let agent =
        registered_agent(&scope.fixture.workspaces, &scope.owner, &scope.workspace_id).await;
    outcome(
        &scope,
        &scope.owner,
        author("addTool", "people.greet", SOURCE, ""),
    )
    .await;
    for principal in [&member, &agent] {
        for (run, code) in [
            (
                author("updateTool", "people.greet", OTHER_SOURCE, ""),
                "private_tool_not_found",
            ),
            (remove("people.greet"), "private_tool_not_found"),
        ] {
            let refused = outcome(&scope, principal, run).await;
            assert_eq!(
                refused.get("code"),
                Some(&json!(code)),
                "{principal:?}: {refused}"
            );
        }
        let added = outcome(
            &scope,
            principal,
            author("addTool", "people.greet", OTHER_SOURCE, ""),
        )
        .await;
        assert_eq!(
            added.pointer("/data/revision"),
            Some(&json!(1)),
            "{principal:?}: {added}"
        );
        assert_eq!(
            outcome(&scope, principal, greet()).await,
            json!("bye Ada"),
            "{principal:?}"
        );
        let removed = outcome(&scope, principal, remove("people.greet")).await;
        assert!(removed.pointer("/data/call_path").is_some(), "{removed}");
    }
    assert_eq!(
        outcome(&scope, &scope.owner, greet()).await,
        json!("hello Ada")
    );
}

/// The Invocations `run` recorded, oldest first, with their detail as the
/// Run's reader inspects it.
async fn invocation_details(
    scope: &AdminScope,
    principal: &AuthenticatedPrincipal,
    run: &crate::RunId,
) -> Vec<crate::InvocationDetail> {
    let reader = reader(scope, principal);
    let entries = scope
        .fixture
        .runs
        .activity_entries(
            reader,
            &scope.workspace_id,
            run,
            &crate::RunEntryWindow::Start,
            &crate::RunEntryLevel::Every,
            100,
            None,
        )
        .await
        .unwrap()
        .entries;
    let mut details = Vec::new();
    for entry in entries {
        if let crate::RunEntry::Invocation(summary) = entry {
            details.push(
                scope
                    .fixture
                    .runs
                    .inspect_invocation(reader, &scope.workspace_id, &summary.id)
                    .await
                    .unwrap(),
            );
        }
    }
    details
}

// Catches: a Private Tool call that is not asynchronous, records no
// Invocation or more than one, wraps its value as a Tool Result, or shares
// a value with its caller instead of copying it as JSON, so that an
// `undefined` array element arrives as anything but null or a change the
// body makes reaches the caller's object.
#[tokio::test]
async fn each_private_tool_call_is_one_invocation_that_copies_data_and_returns_the_raw_value() {
    let (scope, _index) = authoring_scope("user:functions-invocations").await;
    let echo = "const saved = async (input: { values: (number | null)[] }) => {
        const second = input.values[1];
        input.values.push(4);
        return { values: input.values, secondIsNull: second === null, missing: [undefined] };
    };";
    outcome(&scope, &scope.owner, add("data.echo", echo)).await;

    let run = scope
        .start(
            &scope.owner,
            submission(
                "const input = { values: [1, undefined, 3] };
                 const pending = coral.private.data.echo(input);
                 const asynchronous = pending instanceof Promise;
                 const result = await pending;
                 const again = await coral.private.data.echo({ values: [] });
                 return { asynchronous, callerLength: input.values.length, result, again };",
            ),
        )
        .await;
    assert_eq!(
        final_value(&scope.terminal(&scope.owner, &run).await),
        json!({
            "asynchronous": true,
            "callerLength": 3,
            "result": { "values": [1, null, 3, 4], "secondIsNull": true, "missing": [null] },
            "again": { "values": [4], "secondIsNull": false, "missing": [null] },
        })
    );
    let details = invocation_details(&scope, &scope.owner, &run).await;
    let recorded = details
        .iter()
        .map(|detail| {
            (
                detail.summary.call_path.as_str(),
                detail.arguments.clone(),
                detail.outcome.clone(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        recorded,
        [
            (
                "coral.private.data.echo",
                json!({ "values": [1, null, 3] }),
                Some(crate::InvocationOutcome::UpstreamOutcome(
                    json!({ "values": [1, null, 3, 4], "secondIsNull": true, "missing": [null] })
                )),
            ),
            (
                "coral.private.data.echo",
                json!({ "values": [] }),
                Some(crate::InvocationOutcome::UpstreamOutcome(
                    json!({ "values": [4], "secondIsNull": false, "missing": [null] })
                )),
            ),
        ]
    );
}

// Catches: a throw in a Private Tool's body that reaches its caller as
// anything but a Tool Error coded `tool_failed` with its message, a result
// that is not JSON under any code but `invalid_tool_result`, a Tool Error
// from a call the body makes that loses its own code, or an Invocation that
// records any of them as anything but `tool_failed` or `invalid_tool_result`.
#[tokio::test]
async fn a_private_tool_that_throws_or_returns_no_data_fails_its_caller_with_a_tool_error() {
    let (scope, _index) = authoring_scope("user:functions-failures").await;
    for (path, source, caller_code, caller_message, recorded_code) in [
        (
            "fails.error",
            "const saved = async (): Promise<number> => { throw new Error('boom'); };",
            "tool_failed",
            "boom",
            "tool_failed",
        ),
        (
            "fails.text",
            "const saved = async (): Promise<number> => { throw 'plain text'; };",
            "tool_failed",
            "plain text",
            "tool_failed",
        ),
        (
            "fails.result",
            "const saved = async (): Promise<any> => 10n;",
            "invalid_tool_result",
            "Tool result is not a JSON value at `result`: it is a bigint; use a number or a string",
            "invalid_tool_result",
        ),
        (
            "fails.nested",
            "const saved = async () => coral.removeTool({ path: 'nothing.here' });",
            "private_tool_not_found",
            "Private Tool was not found",
            "tool_failed",
        ),
    ] {
        outcome(&scope, &scope.owner, add(path, source)).await;
        let run = scope
            .start(
                &scope.owner,
                tool_call("", &format!("coral.private.{path}()")),
            )
            .await;
        assert_eq!(
            final_value(&scope.terminal(&scope.owner, &run).await),
            json!({ "code": caller_code, "message": caller_message }),
            "{path}"
        );
        let outer = invocation_details(&scope, &scope.owner, &run)
            .await
            .into_iter()
            .find(|detail| detail.summary.call_path == format!("coral.private.{path}"))
            .expect("the call is an Invocation");
        assert!(
            matches!(
                &outer.outcome,
                Some(crate::InvocationOutcome::CoralFailure { code, .. }) if code == recorded_code
            ),
            "{path}: {:?}",
            outer.outcome
        );
    }
}

// Catches: an old root (`coral.workspace`, `coral.functions`, or
// `saveFunction` on `coral`) that admission lets through, in a Program or in
// a stored Private Tool's source, so the Program fails only when it calls
// it; or a refused admission that creates a Run.
#[tokio::test]
async fn old_roots_fail_at_admission_in_a_program_and_in_a_stored_tool() {
    let (scope, _index) = authoring_scope("user:functions-old-roots").await;
    let module = private_tool_module(&scope);
    for (path, body) in [
        ("old.workspace", "coral.workspace.search({ query: 'x' })"),
        (
            "old.functions",
            "coral.functions.people.greet({ name: 'Ada' })",
        ),
    ] {
        // Coral never rewrites a stored source, so one saved before the old
        // roots went can still name them.
        module
            .save(
                &coral_tools::PrivateToolChangeContext::new(
                    scope.owner.clone(),
                    60,
                    coral_workspace::TraceId::current_or_new(),
                ),
                &scope.workspace_id,
                coral_tools::PrivateToolSave::new(
                    coral_tools::PrivateToolPath::parse(path).unwrap(),
                    coral_tools::AuthoredToolDescription::parse("Call an old root").unwrap(),
                    coral_tools::AuthoredToolSource::parse(&format!(
                        "export default async () => {body};"
                    ))
                    .unwrap(),
                ),
            )
            .await
            .unwrap();
    }
    for (source, refusal) in [
        (
            "return await coral.workspace.search({ query: 'x' });",
            "Program:1:14: coral.workspace.search names no Tool that you can call.",
        ),
        (
            "return await coral.saveFunction({ path: 'a' });",
            "Program:1:14: coral.saveFunction names no Tool that you can call.",
        ),
        (
            "return await coral.functions.people.greet({ name: 'Ada' });",
            "Program:1:14: coral.functions.people.greet names no Tool that you can call.",
        ),
        (
            "return await coral.private.old.workspace();",
            "Private Tool coral.private.old.workspace:1:28: coral.workspace.search names no Tool that you can call.",
        ),
        (
            "return await coral.private.old.functions();",
            "Private Tool coral.private.old.functions:1:28: coral.functions.people.greet names no Tool that you can call.",
        ),
    ] {
        let before = scope.fixture.runs.count_runs().await.unwrap();
        let refused = scope
            .fixture
            .runs
            .start(&scope.owner, &scope.workspace_id, submission(source))
            .await;
        assert!(
            matches!(&refused, Err(crate::Error::ProgramAdmission(message)) if message.starts_with(refusal)),
            "{source}: {:?}",
            refused.err()
        );
        assert_eq!(
            scope.fixture.runs.count_runs().await.unwrap(),
            before,
            "{source}"
        );
    }
}

// Catches: a call to a Private Tool the caller does not have refused with
// other words than any path that names no Tool, or without the caller's own
// nearest Private Tools to call instead.
#[tokio::test]
async fn a_missing_private_tool_is_refused_like_any_unknown_path_with_its_nearest_tools() {
    let (scope, _index) = authoring_scope("user:functions-missing").await;
    outcome(
        &scope,
        &scope.owner,
        author(
            "addTool",
            "people.greetPerson",
            "const saved = async ({ name }: { name: string }): Promise<string> => `hello ${name}`;",
            "",
        ),
    )
    .await;
    for (source, refusal) in [
        (
            "return await coral.private.people.personGreet({ name: 'Ada' });",
            "Program:1:14: coral.private.people.personGreet names no Tool that you can call. \
             Nearest: coral.private.people.greetPerson.",
        ),
        (
            "return await coral.private.nobody.here();",
            "Program:1:14: coral.private.nobody.here names no Tool that you can call. \
             Find one with coral.search.",
        ),
    ] {
        let refused = scope
            .fixture
            .runs
            .start(&scope.owner, &scope.workspace_id, submission(source))
            .await;
        assert!(
            matches!(&refused, Err(crate::Error::ProgramAdmission(message)) if message == refusal),
            "{source}: {:?}",
            refused.err()
        );
    }
}

/// A tool whose body makes one call of its own, so a row for that call
/// shows the body ran.
const CALLING_SOURCE: &str =
    "const saved = async ({ name }: { name: string }): Promise<string> => {
    await coral.listProviders();
    return `hello ${name}`;
};";

/// Who authors and calls the tool in a policy row.
#[derive(Debug, Clone, Copy)]
enum Author {
    Owner,
    Agent,
}

/// What an Owner does after the save, before the call.
#[derive(Debug, Clone, Copy)]
enum Gate {
    Nothing,
    /// An Owner's rule on the Private Tool.
    Rule(ToolAccessState),
    /// An Owner's rule on the Private Tool, then its author, an Owner, is
    /// made a Member by another Owner.
    RuleThenDemote(ToolAccessState),
    /// An Owner's rule on the call the body makes.
    InnerRule(ToolAccessState),
    DisableProvider,
}

/// One row: who authors the tool, the policy its save declares, the
/// Owner's gate, the decision on an approval the call waits for, and the
/// call's outcome: its value or its Tool Error's code.
struct PolicyRow {
    author: Author,
    declared: Option<&'static str>,
    gate: Gate,
    decision: Option<crate::approval::ApprovalDecision>,
    outcome: Value,
}

// Catches: a Private Tool whose declared default, or an Owner's rule over
// it, does not decide its call; a rule that still decides it once its
// author is a Member; a call its body makes that does not follow its own
// rule; a disabled Private Provider that leaves a Private Tool callable; an
// outer call that needs approval whose body runs before the decision, or at
// all once declined; or an Agent's call that its Owner cannot approve.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table covers each source of a Private Tool's policy"
)]
async fn a_private_tool_call_follows_its_declared_default_owner_rules_and_the_private_provider() {
    use crate::approval::ApprovalDecision::{Approve, Decline};
    use Gate::{DisableProvider, InnerRule, Nothing, Rule, RuleThenDemote};
    let hello = || json!("hello Ada");
    let code = |code: &str| json!({ "code": code });
    let rows = [
        PolicyRow {
            author: Author::Owner,
            declared: None,
            gate: Nothing,
            decision: None,
            outcome: hello(),
        },
        PolicyRow {
            author: Author::Owner,
            declared: Some("enabled"),
            gate: Nothing,
            decision: None,
            outcome: hello(),
        },
        PolicyRow {
            author: Author::Owner,
            declared: Some("disabled"),
            gate: Nothing,
            decision: None,
            outcome: code("tool_disabled"),
        },
        PolicyRow {
            author: Author::Owner,
            declared: Some("approval_required"),
            gate: Nothing,
            decision: Some(Approve),
            outcome: hello(),
        },
        PolicyRow {
            author: Author::Owner,
            declared: Some("approval_required"),
            gate: Nothing,
            decision: Some(Decline),
            outcome: code("approval_declined"),
        },
        PolicyRow {
            author: Author::Owner,
            declared: Some("approval_required"),
            gate: Rule(ToolAccessState::Enabled),
            decision: None,
            outcome: hello(),
        },
        PolicyRow {
            author: Author::Owner,
            declared: None,
            gate: RuleThenDemote(ToolAccessState::Disabled),
            decision: None,
            outcome: hello(),
        },
        PolicyRow {
            author: Author::Owner,
            declared: None,
            gate: InnerRule(ToolAccessState::Disabled),
            decision: None,
            outcome: code("tool_disabled"),
        },
        PolicyRow {
            author: Author::Owner,
            declared: None,
            gate: DisableProvider,
            decision: None,
            outcome: code("provider_unavailable"),
        },
        PolicyRow {
            author: Author::Agent,
            declared: Some("approval_required"),
            gate: Nothing,
            decision: Some(Approve),
            outcome: hello(),
        },
    ];
    for (index, row) in rows.into_iter().enumerate() {
        let label = format!(
            "row {index}: {:?} {:?} {:?}",
            row.author, row.declared, row.gate
        );
        let (scope, _index) = authoring_scope(&format!("user:private-policy-{index}")).await;
        let author = match row.author {
            Author::Owner => scope.owner.clone(),
            Author::Agent => {
                registered_agent(&scope.fixture.workspaces, &scope.owner, &scope.workspace_id).await
            }
        };
        let policy = row
            .declared
            .map_or_else(String::new, |policy| format!(", policy: {}", json!(policy)));
        let saved = outcome(
            &scope,
            &author,
            author_with("addTool", CALLING_SOURCE, &policy),
        )
        .await;
        assert!(
            saved.pointer("/data/revision").is_some(),
            "{label}: {saved}"
        );
        match row.gate {
            Nothing => {}
            Rule(state) | RuleThenDemote(state) => {
                let tool = private_tool_module(&scope)
                    .get(
                        &author,
                        &scope.workspace_id,
                        &PrivateToolPath::parse("people.greet").unwrap(),
                    )
                    .await
                    .unwrap();
                scope
                    .fixture
                    .policies
                    .set_access_state(
                        &change(scope.owner_user(), 13, '4'),
                        ToolPolicyTarget::new(
                            scope.workspace_id.clone(),
                            private_provider(&scope).await.id().clone(),
                            tool.id().tool_key(),
                        ),
                        state,
                    )
                    .await
                    .expect("set the Owner's rule");
                if matches!(row.gate, RuleThenDemote(_)) {
                    demote_the_owner(&scope, index).await;
                }
            }
            InnerRule(state) => scope.set_access_to("coral.listProviders", state).await,
            DisableProvider => disable_private_provider(&scope).await,
        }

        let run = scope.start(&author, greet_or_code()).await;
        if let Some(decision) = row.decision {
            let approver = reader(&scope, &author);
            let approvals = scope.fixture.approvals();
            let request = wait_for_approval(&approvals, approver, &scope.workspace_id, &run).await;
            // The approver can decide it: the Invocation keeps the default
            // the linked revision declared.
            assert!(request.actionable, "{label}");
            // The call waits on its decision with its body not yet run.
            let paths = |rows: Vec<InvocationRow>| {
                rows.into_iter()
                    .map(|row| row.call_path)
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                paths(
                    scope
                        .fixture
                        .invocation_rows(&scope.workspace_id, &run)
                        .await
                ),
                ["coral.private.people.greet"],
                "{label}"
            );
            approvals
                .decide(
                    approver,
                    &scope.workspace_id,
                    &request.id,
                    decision,
                    None,
                    &TraceId::parse(&"d".repeat(32)).unwrap(),
                )
                .await
                .expect("decide the call");
        }
        let value = final_value(&scope.terminal(&author, &run).await);
        let found = value
            .get("code")
            .map_or_else(|| value.clone(), |code| json!({ "code": code }));
        assert_eq!(found, row.outcome, "{label}: {value}");
        // The body ran, and made its own call, only when the call ran. A
        // disabled Provider admits no call, as for any Provider.
        let mut called: Vec<_> = scope
            .fixture
            .invocation_rows(&scope.workspace_id, &run)
            .await
            .into_iter()
            .map(|row| row.call_path)
            .collect();
        called.sort();
        let expected = match (row.gate, row.outcome == hello()) {
            (DisableProvider, _) => Vec::new(),
            (InnerRule(_), _) | (_, true) => {
                vec!["coral.listProviders", "coral.private.people.greet"]
            }
            (_, false) => vec!["coral.private.people.greet"],
        };
        assert_eq!(called, expected, "{label}");
    }
}

// Catches: a `coral.removeTool` that leaves its Private Tool's exact rule
// among the Workspace's rules or takes another Tool's, a refused one that
// takes a rule, or one that commits although its rule removal failed.
#[tokio::test]
async fn removing_a_tool_removes_its_exact_rule_and_no_other() {
    let (scope, _index) = authoring_scope("user:remove-rule").await;
    let functions = private_tool_module(&scope);
    let provider = private_provider(&scope).await.id().clone();
    let mut keys = Vec::new();
    for path in ["people.greet", "people.part"] {
        let saved = outcome(&scope, &scope.owner, add(path, SOURCE)).await;
        assert!(saved.pointer("/data/revision").is_some(), "{path}: {saved}");
        let tool = functions
            .get(
                &scope.owner,
                &scope.workspace_id,
                &PrivateToolPath::parse(path).unwrap(),
            )
            .await
            .unwrap();
        keys.push(tool.id().tool_key());
    }
    scope
        .fixture
        .policies
        .set_access_states(
            &change(scope.owner_user(), 13, '4'),
            keys.iter()
                .map(|key| {
                    (
                        ToolPolicyTarget::new(
                            scope.workspace_id.clone(),
                            provider.clone(),
                            key.clone(),
                        ),
                        ToolAccessState::Disabled,
                    )
                })
                .collect(),
        )
        .await
        .expect("set the Owner's rules");
    // Whether an exact rule names each Tool: with no policy facts a Tool
    // defaults to `approval_required`, so only its rule disables it.
    let ruled = async || {
        let mut transaction = scope.fixture.database.begin_read_snapshot().await.unwrap();
        let rules =
            coral_policy::exact_tool_rules_in_transaction(&mut transaction, &scope.workspace_id)
                .await
                .unwrap();
        transaction.rollback().await.unwrap();
        keys.iter()
            .map(|key| {
                rules.access_state(&provider, key, coral_catalog::ToolPolicyFacts::default())
                    == ToolAccessState::Disabled
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(ruled().await, [true, true]);

    // An Owner's rule refuses the remove of the ruled Tool itself.
    scope
        .set_access_to("coral.removeTool", ToolAccessState::Disabled)
        .await;
    let refused = outcome(&scope, &scope.owner, remove("people.greet")).await;
    assert_eq!(
        refused.get("code"),
        Some(&json!("tool_disabled")),
        "{refused}"
    );
    assert_eq!(
        ruled().await,
        [true, true],
        "a refused remove takes no rule"
    );
    scope
        .set_access_to("coral.removeTool", ToolAccessState::Enabled)
        .await;

    // While the rule delete fails, the remove fails with it and keeps its
    // Tool: the remove below still finds the Tool.
    let table = "tool_access_rules";
    let database = &scope.fixture.test_database;
    database.fail_deletes(table, true).await.unwrap();
    let run = scope.start(&scope.owner, remove("people.greet")).await;
    let failed = scope.terminal(&scope.owner, &run).await;
    assert!(
        matches!(
            failed.terminal_outcome(),
            Some(RunTerminalOutcome::Interrupted)
        ),
        "{failed:?}"
    );
    assert_eq!(ruled().await, [true, true], "a failed remove takes no rule");
    database.fail_deletes(table, false).await.unwrap();

    let removed = outcome(&scope, &scope.owner, remove("people.greet")).await;
    assert_eq!(
        removed.pointer("/data/call_path"),
        Some(&json!("coral.private.people.greet")),
        "{removed}"
    );
    assert_eq!(ruled().await, [false, true]);
}

/// The Workspace's Private Provider, in any state.
async fn private_provider(scope: &AdminScope) -> coral_provider::Provider {
    let mut transaction = scope.fixture.database.begin().await.unwrap();
    let private = coral_provider::authored_tool_provider_in_transaction(
        &mut transaction,
        &scope.workspace_id,
        coral_catalog::AuthoredToolKind::Private,
    )
    .await
    .unwrap()
    .expect("the Private Provider");
    transaction.rollback().await.unwrap();
    private
}

/// The Owner disables the Workspace's Private Provider.
async fn disable_private_provider(scope: &AdminScope) {
    let private = private_provider(scope).await;
    scope
        .fixture
        .providers
        .disable_provider(
            &change(scope.owner_user(), 17, '8')
                .with_etag(&private.etag())
                .unwrap(),
            &scope.workspace_id,
            private.id(),
        )
        .await
        .expect("disable the Private Provider");
}

/// Makes `scope`'s Owner a Member: another Owner, added for it, changes
/// the role, since a Workspace keeps one Owner.
async fn demote_the_owner(scope: &AdminScope, index: usize) {
    let second = member(scope, &format!("user:private-policy-{index}-second")).await;
    let second = second.authenticated_user().unwrap().clone();
    let workspaces = &scope.fixture.workspaces;
    workspaces
        .set_membership(
            &change(scope.owner_user(), 15, '6'),
            &scope.workspace_id,
            &second,
            WorkspaceRole::Owner,
        )
        .await
        .expect("make a second Owner");
    workspaces
        .set_membership(
            &change(second, 16, '7'),
            &scope.workspace_id,
            &scope.owner_user(),
            WorkspaceRole::Member,
        )
        .await
        .expect("make the first Owner a Member");
}

/// `coral.addTool` or `coral.updateTool` saving `saved` from
/// `declarations` at `people.greet`, with `extra` fields after `source`.
fn author_with(verb: &str, declarations: &str, extra: &str) -> RunSubmission {
    author(verb, "people.greet", declarations, extra)
}

/// Calls `people.greet` and returns its value, or its Tool Error's code
/// and message.
fn greet_or_code() -> RunSubmission {
    tool_call("", "coral.private.people.greet({ name: 'Ada' })")
}

/// One row of [`calls_inside_a_body_follow_their_own_policy_and_the_kill_switch`]:
/// the policy `people.inner` declares, whether the Owner disables the
/// Private Provider while the body waits on its first call, the outcome's
/// code, and the Call Paths the Run records.
struct NestedRow {
    inner_declared: &'static str,
    disable_mid_body: bool,
    code: &'static str,
    called: &'static [&'static str],
}

// Catches: a nested Private Tool call that its own declared default does
// not decide, or a Private Provider disabled while a body runs that leaves
// the body's next Private Tool call to run.
#[tokio::test]
async fn calls_inside_a_body_follow_their_own_policy_and_the_kill_switch() {
    let rows = [
        NestedRow {
            inner_declared: "disabled",
            disable_mid_body: false,
            code: "tool_disabled",
            called: &[
                "coral.listProviders",
                "coral.private.people.greet",
                "coral.private.people.inner",
            ],
        },
        NestedRow {
            inner_declared: "enabled",
            disable_mid_body: true,
            code: "provider_unavailable",
            called: &["coral.listProviders", "coral.private.people.greet"],
        },
    ];
    for (index, row) in rows.into_iter().enumerate() {
        let (scope, _index) = authoring_scope(&format!("user:private-nested-{index}")).await;
        let inner = outcome(
            &scope,
            &scope.owner,
            author(
                "addTool",
                "people.inner",
                "const saved = async ({ name }: { name: string }): Promise<string> => `inner ${name}`;",
                &format!(", policy: {}", json!(row.inner_declared)),
            ),
        )
        .await;
        assert!(inner.pointer("/data/revision").is_some(), "{inner}");
        let greet = outcome(
            &scope,
            &scope.owner,
            author_with(
                "addTool",
                "const saved = async ({ name }: { name: string }): Promise<string> => {
                    await coral.listProviders();
                    return await coral.private.people.inner({ name });
                };",
                "",
            ),
        )
        .await;
        assert!(greet.pointer("/data/revision").is_some(), "{greet}");
        if row.disable_mid_body {
            scope
                .set_access_to("coral.listProviders", ToolAccessState::ApprovalRequired)
                .await;
        }

        let run = scope.start(&scope.owner, greet_or_code()).await;
        if row.disable_mid_body {
            let approvals = scope.fixture.approvals();
            let request =
                wait_for_approval(&approvals, &scope.owner, &scope.workspace_id, &run).await;
            disable_private_provider(&scope).await;
            approvals
                .decide(
                    &scope.owner,
                    &scope.workspace_id,
                    &request.id,
                    crate::approval::ApprovalDecision::Approve,
                    None,
                    &TraceId::parse(&"d".repeat(32)).unwrap(),
                )
                .await
                .expect("approve the body's first call");
        }
        let value = final_value(&scope.terminal(&scope.owner, &run).await);
        assert_eq!(
            value.get("code"),
            Some(&json!(row.code)),
            "row {index}: {value}"
        );
        let rows = scope
            .fixture
            .invocation_rows(&scope.workspace_id, &run)
            .await;
        let greet_id = rows
            .iter()
            .find(|row| row.call_path == "coral.private.people.greet")
            .map(|row| row.id.clone());
        // Each call the body made is a child of the call it ran for.
        for child in rows
            .iter()
            .filter(|row| row.call_path != "coral.private.people.greet")
        {
            assert_eq!(
                child.parent_id, greet_id,
                "row {index}: {}",
                child.call_path
            );
        }
        let mut called: Vec<_> = rows.iter().map(|row| row.call_path.as_str()).collect();
        called.sort_unstable();
        assert_eq!(called, row.called, "row {index}");
    }
}
