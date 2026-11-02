//! `coral.workspace.search` discovers from inside a Program, over the real V8
//! engine. A listing and exact paths read the catalog, not the index, so
//! these Programs need no indexing pass. Ranking and visibility are tested at
//! their owner, coral-search.
use super::provider_setup::{AdminScope, final_value, workspace_call};
use super::*;
use serde_json::{Value, json};

/// The Call Paths of a page's hits, or of a lookup's descriptions.
fn call_paths(found: &Value, list: &str) -> Vec<String> {
    found
        .pointer(&format!("/data/{list}"))
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("{list} in {found}"))
        .iter()
        .map(|item| item["call_path"].as_str().expect("a Call Path").to_owned())
        .collect()
}

// Catches: a Built-in search that ignores the reach of the Tools it shows,
// waits for approval, or fails a lookup for one path it cannot describe.
#[tokio::test]
async fn every_principal_searches_without_approval_and_sees_only_what_it_reaches() {
    let scope = AdminScope::new("user:search-owner").await;
    let member = organization_user(
        scope.fixture.database.clone(),
        "user:search-member",
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
    let agent =
        registered_agent(&scope.fixture.workspaces, &scope.owner, &scope.workspace_id).await;
    let paths = json!({
        "paths": [
            "coral.workspace.updatePolicy",
            "coral.providers.absent.tool",
            "coral.workspace.search",
        ],
    });
    for (principal, described, missing) in [
        (
            scope.owner.clone(),
            &["coral.workspace.updatePolicy", "coral.workspace.search"][..],
            &["coral.providers.absent.tool"][..],
        ),
        (
            AuthenticatedPrincipal::user(member),
            &["coral.workspace.search"][..],
            &[
                "coral.workspace.updatePolicy",
                "coral.providers.absent.tool",
            ][..],
        ),
        (
            agent,
            &["coral.workspace.search"][..],
            &[
                "coral.workspace.updatePolicy",
                "coral.providers.absent.tool",
            ][..],
        ),
    ] {
        let run = scope
            .start(&principal, workspace_call("search", &paths.to_string()))
            .await;
        let found = final_value(&scope.terminal(&principal, &run).await);
        assert_eq!(
            call_paths(&found, "descriptions"),
            described,
            "{principal:?}"
        );
        assert_eq!(call_paths(&found, "missing"), missing, "{principal:?}");
        let declaration = found
            .pointer("/data/descriptions/0/typescript_declaration")
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("a declaration in {found}"));
        assert!(
            declaration.contains("export default function ("),
            "{declaration}"
        );

        let run = scope
            .start(
                &principal,
                workspace_call(
                    "search",
                    r#"{ "provider": "coral.workspace", "limit": 50 }"#,
                ),
            )
            .await;
        let listed = final_value(&scope.terminal(&principal, &run).await);
        let reached = call_paths(&listed, "hits");
        assert!(
            reached.contains(&"coral.workspace.search".to_owned()),
            "{listed}"
        );
        assert_eq!(
            reached.contains(&"coral.workspace.updatePolicy".to_owned()),
            described.contains(&"coral.workspace.updatePolicy"),
            "{principal:?}: {listed}"
        );
    }
}

// Catches: a page a Program cannot continue from its own cursor.
#[tokio::test]
async fn a_program_pages_a_listing_with_the_cursor_it_returned() {
    let scope = AdminScope::new("user:search-pages").await;
    let run = scope
        .start(
            &scope.owner,
            submission(
                r#"
                const first = await coral.workspace.search({ provider: "coral.workspace", limit: 2 });
                const next = await coral.workspace.search({
                    provider: "coral.workspace",
                    limit: 2,
                    cursor: first.data.next_cursor,
                });
                const all = await coral.workspace.search({ provider: "coral.workspace", limit: 4 });
                return { first, next, all };
                "#,
            ),
        )
        .await;
    let results = final_value(&scope.terminal(&scope.owner, &run).await);
    let page = |name: &str| results.get(name).cloned().expect("a page");
    assert_eq!(page("first").pointer("/data/has_more"), Some(&json!(true)));
    assert_eq!(
        page("first").get("metadata"),
        Some(&json!({ "kind": "builtin" }))
    );
    let mut both = call_paths(&page("first"), "hits");
    both.extend(call_paths(&page("next"), "hits"));
    assert_eq!(both, call_paths(&page("all"), "hits"));
}

// Catches: a refused argument that the Program cannot tell from the others,
// or that runs a search anyway.
#[tokio::test]
async fn an_invalid_argument_names_what_to_correct() {
    let scope = AdminScope::new("user:search-invalid").await;
    for (arguments, message) in [
        (
            json!({ "query": "providers", "paths": ["coral.workspace.search"] }),
            " at `query`: leave it out with paths",
        ),
        (
            json!({ "types": ["Search"] }),
            " at `types`: pass types with paths",
        ),
        (
            json!({ "paths": ["coral.workspace.search", "coral.workspace.getPolicy"], "types": ["Search"] }),
            " at `types`: pass types with one path",
        ),
        (
            json!({ "query": " " }),
            " at `query`: pass keywords that are not blank, with no null character, in at most 4096 UTF-8 bytes",
        ),
        (json!({ "limit": 51 }), " at `limit`: pass 1 to 50"),
        (
            json!({ "provider": "coral.providers.github" }),
            " at `provider`: pass a namespace from coral.providers.<namespace>, or coral.workspace",
        ),
        (
            json!({ "provider": "github" }),
            " at `provider`: No Provider you can search has this namespace. Use one of: \
             coral.workspace.",
        ),
        (
            json!({ "cursor": "bm90LWEtY3Vyc29y" }),
            " at `cursor`: pass the next_cursor of the previous page, with the same query and \
             provider",
        ),
        (
            json!({ "paths": ["coral.workspace.getPolicy"], "types": ["Missing"] }),
            " at `types`: Type \"Missing\" is not declared by this path. Coral cannot list the \
             types it declares.",
        ),
    ] {
        let run = scope
            .start(
                &scope.owner,
                workspace_call("search", &arguments.to_string()),
            )
            .await;
        assert_eq!(
            final_value(&scope.terminal(&scope.owner, &run).await),
            json!({
                "code": "invalid_input",
                "message": format!("Tool arguments are invalid{message}"),
            }),
            "{arguments}"
        );
    }
}
