//! `coral.workspace.saveFunction` and `deleteFunction` author the invoking
//! Principal's Saved Functions from inside a Program, over the real V8
//! engine. Authoring rules are tested at their owner, coral-functions.
use super::provider_setup::{AdminScope, final_value, workspace_call};
use super::*;
use coral_policy::ToolAccessState;
use serde_json::{Value, json};

const SOURCE: &str = "export default (name: string): string => `hello ${name}`;";

/// An Admin scope whose Runs can author Saved Functions, with both Tools
/// enabled by an exact rule.
async fn authoring_scope(user: &str) -> AdminScope {
    let mut scope = AdminScope::new(user).await;
    let module = SavedFunctionModule::new(
        scope.fixture.database.clone(),
        scope.fixture.workspaces.clone(),
        crate::IsolateLimits::default(),
    )
    .unwrap();
    let index =
        coral_search::SearchIndex::new(scope.fixture.database.clone(), Duration::from_secs(3_600));
    scope.fixture.runs = scope
        .fixture
        .runs
        .clone()
        .with_saved_functions(crate::SavedFunctions::new(module, index));
    for tool in ["saveFunction", "deleteFunction"] {
        scope
            .set_access_to(&format!("coral.workspace.{tool}"), ToolAccessState::Enabled)
            .await;
    }
    scope
}

fn save(path: &str, source: &str) -> RunSubmission {
    workspace_call(
        "saveFunction",
        &json!({ "path": path, "description": "Greet one person by name.", "source": source })
            .to_string(),
    )
}

fn delete(path: &str) -> RunSubmission {
    workspace_call("deleteFunction", &json!({ "path": path }).to_string())
}

/// Runs `run` as `actor` and returns its Final Value.
async fn outcome(scope: &AdminScope, actor: &AuthenticatedPrincipal, run: RunSubmission) -> Value {
    let run = scope.start(actor, run).await;
    final_value(&scope.terminal(actor, &run).await)
}

// Catches: a save a later Program cannot call, a Program saving as anyone
// but itself, or a delete that leaves the function callable.
#[tokio::test]
async fn a_program_saves_and_deletes_its_own_function_for_later_programs() {
    let scope = authoring_scope("user:functions-owner").await;
    let agent =
        registered_agent(&scope.fixture.workspaces, &scope.owner, &scope.workspace_id).await;
    for principal in [scope.owner.clone(), agent] {
        let saved = outcome(&scope, &principal, save("people.greet", SOURCE)).await;
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

        let called = outcome(
            &scope,
            &principal,
            submission("return coral.functions.people.greet('Ada');"),
        )
        .await;
        assert_eq!(called, json!("hello Ada"), "{principal:?}");

        let deleted = outcome(&scope, &principal, delete("people.greet")).await;
        assert_eq!(
            deleted.pointer("/data/call_path"),
            Some(&json!("coral.functions.people.greet")),
            "{deleted}"
        );
        let again = outcome(&scope, &principal, delete("people.greet")).await;
        assert_eq!(
            again.get("code"),
            Some(&json!("saved_function_not_found")),
            "{again}"
        );
        let gone = scope
            .fixture
            .runs
            .start(
                &principal,
                &scope.workspace_id,
                submission("return coral.functions.people.greet('Ada');"),
            )
            .await;
        assert!(
            gone.is_err(),
            "{principal:?}: admission links no deleted function"
        );
    }
}

// Catches: a save that throws on an advisory type error, or saves a source
// that cannot become a function.
#[tokio::test]
async fn type_errors_are_advisory_and_invalid_sources_save_nothing() {
    let scope = authoring_scope("user:functions-diagnostics").await;
    let typed = outcome(
        &scope,
        &scope.owner,
        save(
            "people.count",
            "export default (): number => { const n: number = 'one'; return n; };",
        ),
    )
    .await;
    assert_eq!(
        typed.pointer("/data/diagnostics/0/category"),
        Some(&json!("error")),
        "{typed}"
    );

    for (path, source, field) in [
        ("people.broken", "export default (", "source"),
        (
            "people.imports",
            "import x from 'y'; export default () => x;",
            "source",
        ),
        ("1bad", SOURCE, "path"),
    ] {
        let refused = outcome(&scope, &scope.owner, save(path, source)).await;
        assert_eq!(
            refused.get("code"),
            Some(&json!("invalid_input")),
            "{path}: {refused}"
        );
        assert!(
            refused
                .get("message")
                .and_then(Value::as_str)
                .is_some_and(|message| message.contains(&format!("`{field}`"))),
            "{path}: {refused}"
        );
        let absent = scope
            .fixture
            .runs
            .start(
                &scope.owner,
                &scope.workspace_id,
                submission(&format!("return coral.functions.{path}();")),
            )
            .await;
        assert!(absent.is_err(), "{path}: nothing was saved");
    }
}

// Catches: authoring Tools that run where no Saved Function module is wired.
#[tokio::test]
async fn without_saved_functions_the_tools_are_unavailable() {
    let scope = AdminScope::new("user:functions-unwired").await;
    scope
        .set_access_to("coral.workspace.saveFunction", ToolAccessState::Enabled)
        .await;
    let refused = outcome(&scope, &scope.owner, save("people.greet", SOURCE)).await;
    assert_eq!(
        refused.get("code"),
        Some(&json!("tool_unavailable")),
        "{refused}"
    );
}
