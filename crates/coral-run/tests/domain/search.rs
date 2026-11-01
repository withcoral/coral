//! `coral.workspace.search` runs Universal Search inside a Program, over the
//! real V8 engine. These calls read the catalog, not the index, so they
//! need no indexing pass.

use serde_json::{Value, json};

use super::provider_setup::{AdminScope, final_value, workspace_call};

async fn scope() -> AdminScope {
    let mut scope = AdminScope::new("user:search").await;
    scope.fixture.runs =
        scope
            .fixture
            .runs
            .clone()
            .with_search(coral_search::UniversalSearchModule::new(
                scope.fixture.database.clone(),
                scope.fixture.workspaces.clone(),
            ));
    scope
}

async fn search(scope: &AdminScope, arguments: &Value) -> Value {
    let run = scope
        .start(
            &scope.owner,
            workspace_call("search", &arguments.to_string()),
        )
        .await;
    final_value(&scope.terminal(&scope.owner, &run).await)
}

// Catches: a Program that cannot read a declaration by its exact Call Path, or
// one bad path failing the paths beside it.
#[tokio::test]
async fn exact_paths_return_descriptions_and_name_what_is_missing() {
    let scope = scope().await;
    let result = search(
        &scope,
        &json!({ "paths": ["coral.workspace.listProviders", "coral.providers.absent.tool"] }),
    )
    .await;
    assert_eq!(
        result.pointer("/data/hits/0/call_path"),
        Some(&json!("coral.workspace.listProviders")),
        "{result}"
    );
    let declaration = result
        .pointer("/data/hits/0/description/typescript_declaration")
        .and_then(Value::as_str)
        .expect("a declaration");
    assert!(
        declaration.contains("export default function"),
        "{declaration}"
    );
    assert_eq!(
        result.pointer("/data/missing"),
        Some(&json!(["coral.providers.absent.tool"]))
    );
    assert_eq!(result.pointer("/data/has_more"), Some(&json!(false)));
}

// Catches: a page that needs a second call to read what the first one found.
#[tokio::test]
async fn a_listing_adds_declarations_only_when_asked() {
    let scope = scope().await;
    let summaries = search(
        &scope,
        &json!({ "provider": "coral.workspace", "limit": 2 }),
    )
    .await;
    assert_eq!(
        summaries
            .pointer("/data/hits")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(2),
        "{summaries}"
    );
    assert!(summaries.pointer("/data/hits/0/description").is_none());
    assert!(summaries.pointer("/data/hits/0/signature_line").is_some());
    // The best hits carry a compact declaration, so one call can be enough.
    assert!(
        summaries.pointer("/data/hits/0/declaration").is_some(),
        "{summaries}"
    );

    let full = search(
        &scope,
        &json!({ "provider": "coral.workspace", "limit": 2, "declarations": true }),
    )
    .await;
    assert!(
        full.pointer("/data/hits/0/description/typescript_declaration")
            .is_some(),
        "{full}"
    );
    assert!(full.pointer("/data/hits/0/signature_line").is_some());
}

// Catches: a Program that cannot see what one Provider offers without guessing
// a query that matches each of its tools.
#[tokio::test]
async fn a_provider_without_a_query_lists_that_providers_tools() {
    let scope = scope().await;
    let result = search(
        &scope,
        &json!({ "provider": "coral.workspace", "limit": 50 }),
    )
    .await;
    let paths = result
        .pointer("/data/hits")
        .and_then(Value::as_array)
        .expect("hits")
        .iter()
        .filter_map(|hit| hit.get("call_path").and_then(Value::as_str))
        .collect::<Vec<_>>();
    assert!(paths.contains(&"coral.workspace.search"), "{result}");
    assert!(paths.contains(&"coral.workspace.listProviders"), "{result}");
    assert!(
        paths
            .iter()
            .all(|path| path.starts_with("coral.workspace.")),
        "{result}"
    );
}

// Catches: compact declarations spreading past the best hits, which would
// grow every page.
#[tokio::test]
async fn only_the_top_hits_carry_a_compact_declaration() {
    let scope = scope().await;
    let result = search(
        &scope,
        &json!({ "provider": "coral.workspace", "limit": 7 }),
    )
    .await;
    let hits = result
        .pointer("/data/hits")
        .and_then(Value::as_array)
        .expect("hits");
    assert!(hits.len() > 3, "{result}");
    let carrying = hits
        .iter()
        .filter(|hit| hit.get("declaration").is_some())
        .count();
    assert_eq!(carrying, 3, "{result}");
}

// Catches: a page of declarations that can flood the agent's context if a
// Program returns it whole.
#[tokio::test]
async fn declarations_stop_at_the_page_cap() {
    let scope = scope().await;
    let result = search(
        &scope,
        &json!({ "provider": "coral.workspace", "limit": 50, "declarations": true }),
    )
    .await;
    let hits = result
        .pointer("/data/hits")
        .and_then(Value::as_array)
        .expect("hits");
    let described: Vec<&Value> = hits
        .iter()
        .take_while(|hit| hit.get("description").is_some())
        .collect();
    assert!(!described.is_empty(), "{result}");
    assert!(
        hits.iter()
            .skip(described.len())
            .all(|hit| hit.get("description").is_none()),
        "descriptions go to the best hits, in order"
    );
    let bytes: usize = described
        .iter()
        .map(|hit| hit["description"].to_string().len())
        .sum();
    assert!(bytes <= 32 * 1024, "{bytes} bytes of descriptions");
}

// Catches: arguments from both modes that quietly drop one of them.
#[tokio::test]
async fn query_and_paths_are_exclusive() {
    let scope = scope().await;
    let result = search(
        &scope,
        &json!({ "query": "providers", "paths": ["coral.workspace.listProviders"] }),
    )
    .await;
    assert_eq!(
        result.get("code"),
        Some(&json!("invalid_input")),
        "{result}"
    );
}
