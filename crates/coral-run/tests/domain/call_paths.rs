//! A Call Path that names no Tool, over the real V8 engine. Program Admission
//! refuses a literal one before a Run exists; a computed one runs and its
//! call fails with `tool_not_found`. Both name the nearest Call Paths the
//! caller can discover. Which paths are nearest is tested at its owner,
//! coral-search.
use super::provider_setup::AdminScope;
use super::*;
use serde_json::json;

/// Notion's page Tools, named as Notion's operation ids name them.
const NOTION: &str = r#"{
  "openapi": "3.1.0",
  "info": { "title": "Notion", "version": "1.0.0" },
  "servers": [{ "url": "https://api.notion.example" }],
  "security": [],
  "paths": {
    "/pages/{page_id}": {
      "get": {
        "operationId": "retrieve-a-page",
        "summary": "Retrieve a page",
        "parameters": [{ "name": "page_id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "OK" } }
      },
      "patch": {
        "operationId": "update-page",
        "summary": "Update page properties",
        "parameters": [{ "name": "page_id", "in": "path", "required": true, "schema": { "type": "string" } }],
        "responses": { "200": { "description": "OK" } }
      }
    }
  }
}"#;

/// GitHub's repository Tools, namespaced as GitHub's operation ids
/// namespace them.
const GITHUB: &str = r#"{
  "openapi": "3.1.0",
  "info": { "title": "GitHub", "version": "1.0.0" },
  "servers": [{ "url": "https://api.github.example" }],
  "security": [],
  "paths": {
    "/repos/{owner}/{repo}": {
      "get": {
        "operationId": "repos/get",
        "summary": "Get a repository",
        "parameters": [
          { "name": "owner", "in": "path", "required": true, "schema": { "type": "string" } },
          { "name": "repo", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": { "200": { "description": "OK" } }
      }
    },
    "/user": {
      "get": {
        "operationId": "users/get-authenticated",
        "summary": "Get the authenticated user",
        "responses": { "200": { "description": "OK" } }
      }
    }
  }
}"#;

/// The Owner's Workspace with Notion and GitHub, indexed.
async fn scope(user: &str) -> AdminScope {
    let scope = AdminScope::new(user).await;
    for (namespace, document, at) in [("notion", NOTION, 20), ("github", GITHUB, 21)] {
        scope
            .fixture
            .providers
            .publish_openapi_fixture(
                &change(scope.owner_user(), at, '4'),
                &scope.workspace_id,
                OpenApiDocument::parse(document).unwrap(),
                namespace,
                CredentialRoute::None,
            )
            .await
            .expect("publish a Provider");
    }
    coral_search::SearchIndex::new(scope.fixture.database.clone(), Duration::from_mins(5))
        .reconcile()
        .await;
    scope
}

/// The admission failure `source` gets as `principal`, after asserting that
/// it created no Run.
async fn refused(scope: &AdminScope, principal: &AuthenticatedPrincipal, source: &str) -> String {
    let before = scope.fixture.runs.count_runs().await.expect("count Runs");
    let error = scope
        .fixture
        .runs
        .start(principal, &scope.workspace_id, submission(source))
        .await
        .expect_err("a literal path that names no Tool is refused");
    let Error::ProgramAdmission(message) = error else {
        panic!("{source}: {error:?}");
    };
    assert_eq!(
        scope.fixture.runs.count_runs().await.expect("count Runs"),
        before,
        "{source}"
    );
    message
}

// Catches: a guessed literal path, or a call to a namespace of a Tool, that
// runs the Program's earlier calls and only then fails; a refusal that does
// not name the path, where it is, or what to call instead; or one that
// suggests a Tool the caller cannot call.
#[tokio::test]
async fn a_literal_path_that_names_no_tool_is_refused_with_the_nearest_paths() {
    let scope = scope("user:call-paths-literal").await;
    let message = refused(
        &scope,
        &scope.owner,
        "await coral.providers.github.users.getAuthenticated();\n\
         return await coral.providers.notion.retrievePage({ page_id: \"p\" });",
    )
    .await;
    assert!(
        message.starts_with(
            "Program:2:14: coral.providers.notion.retrievePage names no Tool that you can \
             call. Nearest: coral.providers.notion.retrieveAPage"
        ),
        "{message}"
    );
    let message = refused(
        &scope,
        &scope.owner,
        "return await coral.providers.github.getRepo({ owner: \"a\", repo: \"b\" });",
    )
    .await;
    assert!(
        message.starts_with(
            "Program:1:14: coral.providers.github.getRepo names no Tool that you can call. \
             Nearest: coral.providers.github.repos.get"
        ),
        "{message}"
    );
    // A namespace of a Tool is not a Tool, even beside a call to the Tool.
    let message = refused(
        &scope,
        &scope.owner,
        "await coral.providers.github.repos.get({ owner: \"a\", repo: \"b\" });\n\
         return await coral.providers.github.repos({ owner: \"a\", repo: \"b\" });",
    )
    .await;
    assert!(
        message.starts_with(
            "Program:2:14: coral.providers.github.repos names no Tool that you can call. \
             Nearest: coral.providers.github.repos.get"
        ),
        "{message}"
    );
    // Each path once, in source order, and one under no Provider's root.
    let message = refused(
        &scope,
        &scope.owner,
        "await coral.workspace.deleteEverything();\n\
         await coral.providers.absent.get();\n\
         await coral.workspace.deleteEverything();",
    )
    .await;
    assert_eq!(
        message,
        "Program:1:7: coral.workspace.deleteEverything names no Tool that you can call. \
         Find one with coral.workspace.search.\n\
         Program:2:7: coral.providers.absent.get names no Tool that you can call. \
         Find one with coral.workspace.search."
    );

    // A Member reaches no Owner-only Built-in Tool, and is not told of one.
    let member = organization_user(
        scope.fixture.database.clone(),
        "user:call-paths-member",
        OrganizationRole::Member,
    )
    .await;
    scope
        .fixture
        .workspaces
        .set_membership(
            &change(scope.owner_user(), 30, '5'),
            &scope.workspace_id,
            &member,
            WorkspaceRole::Member,
        )
        .await
        .unwrap();
    let member = AuthenticatedPrincipal::user(member);
    let message = refused(
        &scope,
        &member,
        "return await coral.workspace.updatePolicies({});",
    )
    .await;
    assert!(
        message.starts_with(
            "Program:1:14: coral.workspace.updatePolicies names no Tool that you can call."
        ) && !message.contains("coral.workspace.updatePolicy,")
            && !message.contains("coral.workspace.updatePolicy."),
        "{message}"
    );
}

// Catches: a computed path refused at admission, which cannot judge it, or a
// failed call that does not name the path and the nearest ones, to a
// Program that catches it or to the reader of one that does not.
#[tokio::test]
async fn a_computed_path_runs_and_its_tool_error_names_the_nearest_paths() {
    let scope = scope("user:call-paths-computed").await;
    let caught = scope
        .outcome(submission(
            r#"
            const name = ["retrieve", "Page"].join("");
            try {
                return await coral.providers.notion[name]({ page_id: "p" });
            } catch (error) {
                return { code: error.code, message: error.message, suggestions: error.suggestions };
            }
            "#,
        ))
        .await;
    assert_eq!(caught.get("code"), Some(&json!("tool_not_found")));
    assert_eq!(
        caught.pointer("/suggestions/0"),
        Some(&json!("coral.providers.notion.retrieveAPage"))
    );
    let message = caught
        .get("message")
        .and_then(serde_json::Value::as_str)
        .expect("a message");
    assert!(
        message.starts_with(
            "coral.providers.notion.retrievePage names no Tool that you can call. Nearest: \
             coral.providers.notion.retrieveAPage"
        ),
        "{message}"
    );

    let run = scope
        .start(
            &scope.owner,
            submission(
                r#"const name = ["get", "Repo"].join(""); return await coral.providers.github[name]();"#,
            ),
        )
        .await;
    let terminal = scope.terminal(&scope.owner, &run).await;
    let Some(RunTerminalOutcome::Failed(failure)) = terminal.terminal_outcome() else {
        panic!("an uncaught ToolError fails the Run: {terminal:?}");
    };
    let error = failure.error().expect("the uncaught error");
    assert_eq!(
        error.tool.as_ref().map(|tool| tool.code.as_str()),
        Some("tool_not_found")
    );
    assert!(
        error.message.starts_with(
            "coral.providers.github.getRepo names no Tool that you can call. Nearest: \
             coral.providers.github.repos.get"
        ),
        "{}",
        error.message
    );
}

// Catches: admission refusing a call to a disabled Tool, which must run to
// be recorded and refused, or a suggestion of a Tool the caller cannot
// call.
#[tokio::test]
async fn a_disabled_tool_is_admitted_but_never_suggested() {
    let scope = scope("user:call-paths-disabled").await;
    scope
        .set_access_to(
            "coral.providers.notion.retrieveAPage",
            ToolAccessState::Disabled,
        )
        .await;
    let message = refused(
        &scope,
        &scope.owner,
        "return await coral.providers.notion.retrievePage({ page_id: \"p\" });",
    )
    .await;
    assert!(
        message.starts_with(
            "Program:1:14: coral.providers.notion.retrievePage names no Tool that you can call."
        ) && !message.contains("retrieveAPage"),
        "{message}"
    );
    assert_eq!(
        scope
            .outcome(submission(
                r#"try {
                    return await coral.providers.notion.retrieveAPage({ page_id: "p" });
                } catch (error) {
                    return error.code;
                }"#,
            ))
            .await,
        json!("tool_disabled")
    );
    let caught = scope
        .outcome(submission(
            r#"const name = ["retrieve", "Page"].join("");
            try { await coral.providers.notion[name]({ page_id: "p" }); }
            catch (error) { return error.suggestions ?? []; }"#,
        ))
        .await;
    assert!(
        !caught
            .as_array()
            .expect("suggestions")
            .contains(&json!("coral.providers.notion.retrieveAPage")),
        "{caught}"
    );
}
