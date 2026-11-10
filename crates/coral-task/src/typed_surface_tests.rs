//! Saved Function save types the Call Paths a source names from what the
//! caller can discover. The compiler's typing rules are tested at
//! `coral-v8` and the visibility rules at `coral-search`; these tests cover
//! that the save command composes them with the caller's authority.

use super::Fixture;
use coral_workspace::{ChangeContext, WorkspaceModule};

/// `repos.get` takes `owner` and `repo` and returns `id`, `full_name` and
/// 4,000 more fields, so typing it tests the deadline. `repos.delete` exists
/// but is disabled, so the caller cannot discover it.
async fn publish_github(fixture: &Fixture) {
    let mut properties = serde_json::Map::new();
    properties.insert("id".into(), serde_json::json!({"type": "integer"}));
    properties.insert("full_name".into(), serde_json::json!({"type": "string"}));
    for index in 0..4_000 {
        properties.insert(
            format!("field_{index:04}_url"),
            serde_json::json!({"type": "string"}),
        );
    }
    let parameters = serde_json::json!([
        {"name": "owner", "in": "path", "required": true, "schema": {"type": "string"}},
        {"name": "repo", "in": "path", "required": true, "schema": {"type": "string"}},
    ]);
    let document = serde_json::json!({
        "openapi": "3.1.0",
        "info": {"title": "GitHub", "version": "1"},
        "servers": [{"url": "https://github.example"}],
        "security": [],
        "paths": {"/repos/{owner}/{repo}": {
            "get": {
                "operationId": "repos/get",
                "parameters": parameters,
                "responses": {"200": {"description": "OK", "content": {"application/json": {"schema": {
                    "type": "object", "properties": properties, "required": ["id", "full_name"],
                }}}}},
            },
            "delete": {
                "operationId": "repos/delete",
                "parameters": parameters,
                "responses": {"204": {"description": "Deleted"}},
            },
        }},
    });
    let provider = fixture.publish_openapi("github", &document).await;
    let database = fixture.test_database.database();
    let workspaces = WorkspaceModule::new(database.clone());
    coral_policy::ToolPolicyModule::new(database, workspaces)
        .set_access_state(
            &ChangeContext::new(fixture.user.clone(), 16, coral_test_support::trace('3')),
            coral_policy::ToolPolicyTarget::new(
                fixture.workspace.clone(),
                provider.access().id().clone(),
                coral_catalog::ToolKey::parse("repos/delete").unwrap(),
            ),
            coral_policy::ToolAccessState::Disabled,
        )
        .await
        .unwrap();
}

/// The diagnostic code for a called path that names no Tool the caller
/// can call.
const UNKNOWN_TOOL: u64 = 100_001;

/// `(code, line, column)` of each diagnostic.
type Located = Vec<(u64, Option<u64>, Option<u64>)>;

fn located(diagnostics: &[coral_functions::SavedFunctionDiagnostic]) -> Located {
    diagnostics
        .iter()
        .map(|diagnostic| (diagnostic.code(), diagnostic.line(), diagnostic.column()))
        .collect()
}

async fn save(fixture: &Fixture, path: &str, source: &str) -> coral_functions::SavedFunction {
    let goal = fixture.goal().await;
    fixture
        .save_function(
            &fixture.call(&goal, "Save a typed function"),
            path,
            "Typed",
            source,
        )
        .await
        .expect("type diagnostics never fail a save")
}

/// Saves `body` as the body of an async default export.
async fn save_diagnostics(
    fixture: &Fixture,
    body: &str,
) -> Vec<coral_functions::SavedFunctionDiagnostic> {
    save(
        fixture,
        "typed.check",
        &format!("export default async function check() {{\n{body}\n}}"),
    )
    .await
    .diagnostics()
    .to_vec()
}

/// Each body is indented two spaces inside a function, so its calls start at
/// line 2, column 3. A disabled Tool reads as an absent one, so typing
/// reveals nothing that `describe` conceals.
#[tokio::test]
async fn save_types_calls_from_what_the_caller_can_discover() {
    let fixture = Fixture::new().await;
    publish_github(&fixture).await;
    save(
        &fixture,
        "helpers.double",
        "export default function double(value: number): number { return value * 2; }",
    )
    .await;
    let cases: &[(&str, &str, Located)] = &[
        (
            "a Tool the caller can discover",
            "  return (await coral.providers.github.repos.get({ owner: \"a\", repo: \"b\" })).data.fullname;",
            vec![(2551, Some(2), Some(83))],
        ),
        (
            "a Saved Function the caller owns",
            "  return coral.functions.helpers.double(\"two\");",
            vec![(2345, Some(2), Some(41))],
        ),
        (
            "a Tool the caller cannot discover",
            "  return coral.providers.github.repos.delete({ owner: 1, nope: true });",
            vec![(UNKNOWN_TOOL, Some(2), Some(10))],
        ),
    ];
    for (name, body, expected) in cases {
        assert_eq!(
            &located(&save_diagnostics(&fixture, body).await),
            expected,
            "{name}"
        );
    }

    let saved = save(
        &fixture,
        "github.repoName",
        "export default async function repoName(owner: string, repo: string) {\n  return (await coral.providers.github.repos.get({ owner, repo })).data.full_name;\n}",
    )
    .await;
    assert!(saved.diagnostics().is_empty(), "{:?}", saved.diagnostics());
    assert_eq!(
        saved.declaration().as_str(),
        "export default function repoName(owner: string, repo: string): Promise<string>;\n",
        "the declaration carries the Tool's result type"
    );
}

/// `coral.workspace.search` declares one overload per mode, so a typed
/// Program reads the result its own mode returns without narrowing, and a
/// call that mixes the modes, or names types of several paths, does not
/// type.
#[tokio::test]
async fn search_types_each_mode_with_the_result_it_returns() {
    let fixture = Fixture::new().await;
    let database = fixture.test_database.database();
    let (installed, _search) = coral_provider::ProviderModule::with_running_catalog(
        coral_outbound::DestinationPolicy::default(),
        database.clone(),
        WorkspaceModule::new(database.clone()),
    )
    .install_missing_builtin(&fixture.workspace, 20)
    .await
    .unwrap()
    .into_parts();
    installed.expect("the Workspace has no Built-in Provider yet");
    let cases: &[(&str, &str, Located)] = &[
        (
            "a query reads its page",
            "  const page = (await coral.workspace.search({ query: \"forecast\", limit: 3 })).data;\n  return [page.hits[0]?.signature_line, page.next_offset];",
            vec![],
        ),
        (
            "paths read their descriptions and missing",
            "  const found = (await coral.workspace.search({ paths: [\"a\", \"b\"] })).data;\n  return [found.descriptions[0]?.typescript_declaration, found.missing[0]?.call_path];",
            vec![],
        ),
        (
            "a missing path reads its suggestions",
            "  const missing = (await coral.workspace.search({ paths: [\"a\"] })).data.missing[0];\n  const nearest: string | undefined = missing?.suggestions[0];\n  return nearest;",
            vec![],
        ),
        (
            "a missing path of a types call reads its suggestions",
            "  const found = (await coral.workspace.search({ paths: [\"a\"], types: [\"T\"] })).data;\n  return found.missing.flatMap((missing) => missing.suggestions);",
            vec![],
        ),
        (
            "a suggestion read as a description",
            "  return (await coral.workspace.search({ paths: [\"a\"] })).data.missing[0]?.suggestions[0]?.call_path;",
            vec![(2339, Some(2), Some(92))],
        ),
        (
            "types read one path's descriptions",
            "  return (await coral.workspace.search({ paths: [\"a\"], types: [\"T\"] })).data.descriptions;",
            vec![],
        ),
        (
            "a query and paths together",
            "  return coral.workspace.search({ query: \"forecast\", paths: [\"a\"] });",
            vec![(2769, Some(2), Some(26))],
        ),
        (
            "types of several paths",
            "  return coral.workspace.search({ paths: [\"a\", \"b\"], types: [\"T\"] });",
            vec![(2769, Some(2), Some(26))],
        ),
        (
            "a page read as descriptions",
            "  return (await coral.workspace.search({ query: \"forecast\" })).data.descriptions;",
            vec![(2339, Some(2), Some(69))],
        ),
    ];
    for (name, body, expected) in cases {
        assert_eq!(
            &located(&save_diagnostics(&fixture, body).await),
            expected,
            "{name}"
        );
    }
}
