//! `coral.workspace.saveFunction` and `deleteFunction` author the invoking
//! Principal's Saved Functions from inside a Program, over the real V8
//! engine. Authoring rules are tested at their owner, coral-functions, and
//! the default Access State at coral-policy.
use super::provider_setup::{AdminScope, RunEndsAfterPreparation, final_value, workspace_call};
use super::*;
use coral_policy::ToolAccessState;
use serde_json::{Value, json};

/// The function each Program saves: `saved`, declared at its top level.
const SOURCE: &str = "const saved = (name: string): string => `hello ${name}`;";

/// An Admin scope whose Runs author Saved Functions, with no exact rule on
/// either Tool, and the index each save notifies.
async fn authoring_scope(user: &str) -> (AdminScope, coral_search::SearchIndex) {
    let mut scope = AdminScope::new(user).await;
    let module = SavedFunctionModule::new(
        scope.fixture.database.clone(),
        scope.fixture.workspaces.clone(),
        crate::IsolateLimits::default(),
        coral_test_support::TEST_COMPILATION_TIMEOUT,
    )
    .unwrap();
    let index =
        coral_search::SearchIndex::new(scope.fixture.database.clone(), Duration::from_secs(3_600));
    scope.fixture.runs = scope
        .fixture
        .runs
        .clone()
        .with_saved_functions(crate::SavedFunctions::new(module, index.clone()));
    (scope, index)
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

/// A Program that declares `declarations`, which declare `saved`, and saves
/// `saved` at `path`. It returns a Tool error's code and message.
fn save(path: &str, declarations: &str) -> RunSubmission {
    submission(&format!(
        "{declarations}
try {{
    return await coral.workspace.saveFunction({{ path: {path}, description: 'Greet one person by name.', source: saved }});
}} catch (error) {{
    if (!(error instanceof coral.ToolError)) throw error;
    return {{ code: error.code, message: error.message }};
}}",
        path = json!(path),
    ))
}

fn delete(path: &str) -> RunSubmission {
    workspace_call("deleteFunction", &json!({ "path": path }).to_string())
}

fn greet() -> RunSubmission {
    submission("return coral.functions.people.greet('Ada');")
}

/// Runs `run` as `actor` and returns its Final Value.
async fn outcome(scope: &AdminScope, actor: &AuthenticatedPrincipal, run: RunSubmission) -> Value {
    let run = scope.start(actor, run).await;
    final_value(&scope.terminal(actor, &run).await)
}

/// Whether admission refuses `run`, as it does a Program that names a
/// Saved Function the caller does not have.
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

// Catches: a Principal that cannot save without an Owner's rule, or waits
// for approval; a save a later Program cannot call, recorded as anything but
// an Invocation of its Run; a Program saving into another Principal's
// namespace; or a delete that leaves the function callable.
#[tokio::test]
async fn each_principal_saves_and_deletes_its_own_function_with_no_rule() {
    let (scope, _index) = authoring_scope("user:functions-owner").await;
    let member = member(&scope, "user:functions-member").await;
    let agent =
        registered_agent(&scope.fixture.workspaces, &scope.owner, &scope.workspace_id).await;
    let principals = [scope.owner.clone(), member, agent];
    for principal in &principals {
        let run = scope.start(principal, save("people.greet", SOURCE)).await;
        let saved = final_value(&scope.terminal(principal, &run).await);
        assert_eq!(
            saved.pointer("/data/call_path"),
            Some(&json!("coral.functions.people.greet")),
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
        let deleted = outcome(&scope, principal, delete("people.greet")).await;
        assert_eq!(
            deleted.pointer("/data/call_path"),
            Some(&json!("coral.functions.people.greet")),
            "{deleted}"
        );
        assert!(admission_refuses(&scope, principal, greet()).await);
        let again = outcome(&scope, principal, delete("people.greet")).await;
        assert_eq!(
            again.get("code"),
            Some(&json!("saved_function_not_found")),
            "{again}"
        );
    }
}

// Catches: a save that throws on an advisory type error, admits a function
// that cannot become a Saved Function, or saves at a path that cannot hold it.
#[tokio::test]
async fn type_errors_are_advisory_and_invalid_functions_save_nothing() {
    let (scope, _index) = authoring_scope("user:functions-diagnostics").await;
    let typed = outcome(
        &scope,
        &scope.owner,
        save(
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
            admission_refuses(&scope, &scope.owner, save(path, declarations)).await,
            "{path}"
        );
    }
    // `people` is a namespace that holds people.count.
    for path in ["1bad", "people"] {
        let refused = outcome(&scope, &scope.owner, save(path, SOURCE)).await;
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
                submission(&format!("return coral.functions.{path}();")),
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
        .set_access_to(
            "coral.workspace.saveFunction",
            ToolAccessState::ApprovalRequired,
        )
        .await;
    let run = scope
        .start(&scope.owner, save("people.greet", SOURCE))
        .await;
    scope.approve(&scope.owner, &run).await;
    let saved = final_value(&scope.terminal(&scope.owner, &run).await);
    assert_eq!(
        saved.pointer("/data/call_path"),
        Some(&json!("coral.functions.people.greet")),
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
                "{SOURCE}\nawait coral.workspace.saveFunction({{ path: 'people.greet', \
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
            .is_some_and(|hits| hits.iter().any(|hit| {
                hit.get("call_path") == Some(&json!("coral.functions.people.greet"))
            })),
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
    let run = scope
        .start(&scope.owner, save("people.greet", SOURCE))
        .await;
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
        "{SOURCE}\nawait coral.workspace.saveFunction({{ path: 'people.greet', \
         description: 'Greet one person by name.', source: saved }});\n\
         return coral.functions.people.greet('Ada');"
    );
    assert!(admission_refuses(&scope, &scope.owner, submission(&by_path)).await);
    assert!(
        admission_refuses(&scope, &scope.owner, greet()).await,
        "the refused Program saved nothing"
    );

    let tested = format!(
        "{SOURCE}\nconst greeting = saved('Ada');\n\
         await coral.workspace.saveFunction({{ path: 'people.greet', \
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

// Catches: a Saved Function that cannot save a function it declares, or
// delete one, when a later Program calls it.
#[tokio::test]
async fn a_saved_function_saves_and_deletes_functions() {
    let (scope, _index) = authoring_scope("user:functions-from-functions").await;
    let tools = format!(
        "{SOURCE}
async function saver() {{
    return coral.workspace.saveFunction({{ path: 'people.greet', description: 'Greet one person by name.', source: saved }});
}}
async function remover() {{
    return coral.workspace.deleteFunction({{ path: 'people.greet' }});
}}
const saving = await coral.workspace.saveFunction({{ path: 'tools.saver', description: 'Save the greeting.', source: saver }});
const removing = await coral.workspace.saveFunction({{ path: 'tools.remover', description: 'Delete the greeting.', source: remover }});
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
        submission("return await coral.functions.tools.saver();"),
    )
    .await;
    assert_eq!(
        saved.pointer("/data/call_path"),
        Some(&json!("coral.functions.people.greet")),
        "{saved}"
    );
    assert_eq!(
        outcome(&scope, &scope.owner, greet()).await,
        json!("hello Ada")
    );

    outcome(
        &scope,
        &scope.owner,
        submission("return await coral.functions.tools.remover();"),
    )
    .await;
    assert!(admission_refuses(&scope, &scope.owner, greet()).await);
}

// Catches: authoring Tools that run where no Saved Function module is wired.
#[tokio::test]
async fn without_saved_functions_the_tools_are_unavailable() {
    let scope = AdminScope::new("user:functions-unwired").await;
    for run in [save("people.greet", SOURCE), delete("people.greet")] {
        let refused = outcome(&scope, &scope.owner, run).await;
        assert_eq!(
            refused.get("code"),
            Some(&json!("tool_unavailable")),
            "{refused}"
        );
    }
}
