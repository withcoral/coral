//! `coral.search` discovers from inside a Program, over the real V8
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
            "coral.updatePolicy",
            "coral.providers.absent.tool",
            "coral.search",
        ],
    });
    for (principal, described, missing) in [
        (
            scope.owner.clone(),
            &["coral.updatePolicy", "coral.search"][..],
            &["coral.providers.absent.tool"][..],
        ),
        (
            AuthenticatedPrincipal::user(member),
            &["coral.search"][..],
            &["coral.updatePolicy", "coral.providers.absent.tool"][..],
        ),
        (
            agent,
            &["coral.search"][..],
            &["coral.updatePolicy", "coral.providers.absent.tool"][..],
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
                workspace_call("search", r#"{ "provider": "coral", "limit": 50 }"#),
            )
            .await;
        let listed = final_value(&scope.terminal(&principal, &run).await);
        let reached = call_paths(&listed, "hits");
        assert!(reached.contains(&"coral.search".to_owned()), "{listed}");
        assert_eq!(
            reached.contains(&"coral.updatePolicy".to_owned()),
            described.contains(&"coral.updatePolicy"),
            "{principal:?}: {listed}"
        );
    }
}

// Catches: a page a Program cannot continue from its own next offset.
#[tokio::test]
async fn a_program_pages_a_listing_with_the_offset_it_returned() {
    let scope = AdminScope::new("user:search-pages").await;
    let run = scope
        .start(
            &scope.owner,
            submission(
                r#"
                const first = await coral.search({ provider: "coral", limit: 2 });
                const next = await coral.search({
                    provider: "coral",
                    limit: 2,
                    offset: first.data.next_offset,
                });
                const all = await coral.search({ provider: "coral", limit: 4 });
                return { first, next, all };
                "#,
            ),
        )
        .await;
    let results = final_value(&scope.terminal(&scope.owner, &run).await);
    let page = |name: &str| results.get(name).cloned().expect("a page");
    assert_eq!(page("first").pointer("/data/next_offset"), Some(&json!(2)));
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
            json!({ "query": "providers", "paths": ["coral.search"] }),
            " at `query`: leave it out with paths",
        ),
        (
            json!({ "types": ["Search"] }),
            " at `types`: pass types with paths",
        ),
        (
            json!({ "paths": ["coral.search", "coral.getPolicy"], "types": ["Search"] }),
            " at `types`: pass types with one path",
        ),
        (
            json!({ "query": " " }),
            " at `query`: pass keywords that are not blank, with no null character, in at most 4096 UTF-8 bytes",
        ),
        (json!({ "limit": 51 }), " at `limit`: pass 1 to 50"),
        (
            json!({ "provider": "coral.providers.github" }),
            " at `provider`: pass a namespace from coral.providers.<namespace>, or coral",
        ),
        (
            json!({ "provider": "github" }),
            " at `provider`: No Provider you can search has this namespace. Use one of: \
             coral.",
        ),
        (
            json!({ "provider": ["coral.workspace", "github"] }),
            " at `provider[1]`: No Provider you can search has this namespace. Use one of: \
             coral.workspace.",
        ),
        (
            json!({ "provider": ["github", "coral.workspace", "slack"] }),
            ": `provider[0]`: No Provider you can search has this namespace; `provider[2]`: No \
             Provider you can search has this namespace; Use one of: coral.workspace.",
        ),
        (
            json!({ "provider": [] }),
            " at `provider`: pass a namespace or an array of 1 to 32 namespaces",
        ),
        (
            json!({ "provider": ["github", 7] }),
            " at `provider`: pass a namespace or an array of 1 to 32 namespaces",
        ),
        (json!({ "offset": -1 }), " at `offset`: expected u32"),
        (
            json!({ "paths": ["coral.getPolicy"], "types": ["Missing"] }),
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

// Catches: a saved diagnostic integer past 2^53 − 1 reaching a Program as a
// number V8 rounds, where MCP `describe` fails instead.
#[tokio::test]
async fn a_lookup_fails_rather_than_round_a_saved_diagnostic_integer() {
    let scope = AdminScope::new("user:search-diagnostics").await;
    PrivateToolModule::new(
        scope.fixture.database.clone(),
        scope.fixture.workspaces.clone(),
        crate::IsolateLimits::default(),
        coral_test_support::TEST_COMPILATION_TIMEOUT,
    )
    .expect("start Private Tool compiler")
    .save(
        &private_tool_change(scope.owner.clone(), 30, 'a'),
        &scope.workspace_id,
        PrivateToolSave::new(
            PrivateToolPath::parse("reports.weekly").expect("path"),
            AuthoredToolDescription::parse("The weekly report").expect("description"),
            AuthoredToolSource::parse("export default async function weekly() { return 1; }")
                .expect("source"),
        ),
    )
    .await
    .expect("save Private Tool");
    // 2^53 + 1, which a JSON reader rounds to 2^53.
    scope
        .fixture
        .test_database
        .execute_fixture_sql(
            "UPDATE private_tool_records SET diagnostics_json = \
             '[{\"category\":\"warning\",\"code\":9007199254740993,\"message\":\"unused\",\
             \"line\":1,\"column\":1}]'",
        )
        .await
        .expect("store an oversized diagnostic code");
    let run = scope
        .start(
            &scope.owner,
            workspace_call("search", r#"{ "paths": ["coral.private.reports.weekly"] }"#),
        )
        .await;
    // Corrupt stored data ends the Run as any other host error in this
    // lookup does, such as diagnostics past their stored bounds.
    let terminal = scope.terminal(&scope.owner, &run).await;
    assert!(
        matches!(
            terminal.terminal_outcome(),
            Some(RunTerminalOutcome::Interrupted)
        ),
        "{terminal:?}"
    );
}
