//! Ranking regressions: queries whose first hits were wrong, each over a
//! small invented catalog that reproduces the defect on both backends.
//!
//! The catalog has three Providers and two Saved Functions. GitHub holds
//! many Tools whose names hold `search`, `issues` and `list`; Slack and
//! Linear hold few, which match a query mostly through their Provider's
//! name. Each Saved Function shares one body word with a query that a Tool
//! matches better.

use coral_workspace::{AuthenticatedPrincipal, WorkspaceId};

use super::{TestModules, organization_user};
use crate::{SearchPage, SearchQuery, SearchRequest};

const USER: &str = "user:search-ranking";

const GITHUB: &str = r#"{
  "openapi": "3.1.0",
  "info": { "title": "GitHub", "version": "1.0.0" },
  "servers": [{ "url": "https://api.github.example" }],
  "security": [],
  "paths": {
    "/search/code": { "get": { "operationId": "search/code", "summary": "Search code",
      "description": "Find code across repositories.",
      "responses": { "200": { "description": "OK" } } } },
    "/search/commits": { "get": { "operationId": "search/commits", "summary": "Search commits",
      "description": "Find commits by the words of their messages.",
      "responses": { "200": { "description": "OK" } } } },
    "/search/issues": { "get": { "operationId": "search/issues",
      "summary": "Search issues and pull requests",
      "description": "Find issues by state and keyword.",
      "responses": { "200": { "description": "OK" } } } },
    "/issues": { "get": { "operationId": "issues/list", "summary": "List repository issues",
      "description": "Returns open issues.",
      "responses": { "200": { "description": "OK" } } } },
    "/issue": { "get": { "operationId": "issues/get", "summary": "Get an issue",
      "responses": { "200": { "description": "OK" } } } },
    "/repo": { "get": { "operationId": "repos/get", "summary": "Get a repository",
      "responses": { "200": { "description": "OK" } } } },
    "/pulls": { "get": { "operationId": "pulls/list", "summary": "List pull requests",
      "responses": { "200": { "description": "OK" } } } }
  }
}"#;

const SLACK: &str = r#"{
  "openapi": "3.1.0",
  "info": { "title": "Slack", "version": "1.0.0" },
  "servers": [{ "url": "https://api.slack.example" }],
  "security": [],
  "paths": {
    "/history": { "get": { "operationId": "conversations/history",
      "summary": "Fetch conversation history",
      "description": "Returns the messages of one channel.",
      "responses": { "200": { "description": "OK" } } } },
    "/users": { "get": { "operationId": "users/list", "summary": "List users",
      "description": "Returns the members of the workspace.",
      "responses": { "200": { "description": "OK" } } } },
    "/post": { "post": { "operationId": "chat/postMessage", "summary": "Send a message",
      "description": "Posts one message to a channel.",
      "responses": { "200": { "description": "OK" } } } }
  }
}"#;

const LINEAR: &str = r#"{
  "openapi": "3.1.0",
  "info": { "title": "Linear", "version": "1.0.0" },
  "servers": [{ "url": "https://api.linear.example" }],
  "security": [],
  "paths": {
    "/issue": { "get": { "operationId": "getIssue", "summary": "Get an issue",
      "description": "Returns one issue by its identifier.",
      "responses": { "200": { "description": "OK" } } } },
    "/teams": { "get": { "operationId": "listTeams", "summary": "List teams",
      "description": "Returns the teams of the organization.",
      "responses": { "200": { "description": "OK" } } } }
  }
}"#;

/// One User's Workspace with the three Providers and two Saved Functions,
/// converged.
async fn catalog() -> (TestModules, AuthenticatedPrincipal, WorkspaceId) {
    let modules = TestModules::new().await;
    let user = organization_user(modules.search.database.clone(), USER).await;
    let principal = AuthenticatedPrincipal::user(user.clone());
    let workspace = modules.create_workspace(&user).await;
    for (document, namespace, at) in [
        (GITHUB, "github", 20),
        (SLACK, "slack", 21),
        (LINEAR, "linear", 22),
    ] {
        modules
            .publish_openapi(&user, &workspace, document, namespace, at)
            .await;
    }
    for (path, description, at) in [
        (
            "digest.weekly",
            "Send the weekly digest of channel messages.",
            30,
        ),
        (
            "standup.notes",
            "Collect standup notes and list them by person.",
            31,
        ),
    ] {
        modules
            .save_function(&principal, &workspace, path, description, at)
            .await;
    }
    modules.converge().await;
    (modules, principal, workspace)
}

fn paths(page: &SearchPage) -> Vec<&str> {
    page.hits()
        .iter()
        .map(|item| item.call_path().as_str())
        .collect()
}

#[tokio::test]
async fn each_query_ranks_its_most_relevant_entity_first() {
    let (modules, principal, workspace) = catalog().await;
    // Each row: the query, an Entity, the most hits that may rank above it,
    // and why.
    let cases = [
        (
            "search slack messages",
            "coral.providers.slack.conversations.history",
            0,
            "the query names Slack, so its Tool that holds `messages` outranks \
             GitHub Tools whose names hold `search`, and a Saved Function that \
             holds only `messages`",
        ),
        (
            "search issues linear",
            "coral.providers.linear.getIssue",
            1,
            "the query names Linear, so its two Tools outrank every GitHub Tool \
             that holds `search` and `issues`. `issues` does not match `issue`, \
             so `getIssue` may follow `listTeams`",
        ),
        (
            "list teams",
            "coral.providers.linear.listTeams",
            0,
            "a Tool whose name and title hold the whole query outranks a Saved \
             Function whose description holds only `list`",
        ),
        (
            "channel messages",
            "coral.functions.digest.weekly",
            0,
            "a Saved Function whose description holds the whole query outranks \
             Tools that hold its words apart",
        ),
    ];
    let mut misranked = Vec::new();
    for (query, expected, most_above, reason) in cases {
        let page = modules
            .search
            .search(
                &principal,
                &workspace,
                &SearchRequest::query(SearchQuery::parse(query).unwrap()),
            )
            .await
            .unwrap();
        let ranked = paths(&page);
        if ranked
            .iter()
            .position(|path| *path == expected)
            .is_none_or(|position| position > most_above)
        {
            misranked.push(format!("{query}: {reason}; order {ranked:?}"));
        }
    }
    assert!(misranked.is_empty(), "{misranked:#?}");
}

#[tokio::test]
async fn a_saved_function_never_splits_a_named_providers_tools_or_passes_a_stronger_tool() {
    let (modules, principal, workspace) = catalog().await;
    // Each row: the query, a Saved Function that holds one of its body
    // words, and the Tools it must follow: every Tool of the Provider the
    // query names, and a Tool that holds the query's words in its name and
    // title.
    let cases = [
        (
            "search slack messages",
            "coral.functions.digest.weekly",
            [
                "coral.providers.slack.conversations.history",
                "coral.providers.slack.users.list",
                "coral.providers.slack.chat.postMessage",
                "coral.providers.github.search.commits",
            ],
        ),
        (
            "slack list",
            "coral.functions.standup.notes",
            [
                "coral.providers.slack.users.list",
                "coral.providers.slack.conversations.history",
                "coral.providers.slack.chat.postMessage",
                "coral.providers.github.pulls.list",
            ],
        ),
    ];
    let mut misranked = Vec::new();
    for (query, function, stronger) in cases {
        let page = modules
            .search
            .search(
                &principal,
                &workspace,
                &SearchRequest::query(SearchQuery::parse(query).unwrap()),
            )
            .await
            .unwrap();
        let ranked = paths(&page);
        let position = |path: &str| ranked.iter().position(|ranked| *ranked == path);
        for tool in stronger {
            let follows = matches!(
                (position(function), position(tool)),
                (Some(function), Some(tool)) if function > tool
            );
            if !follows {
                misranked.push(format!(
                    "{query}: {function} above {tool}; order {ranked:?}"
                ));
            }
        }
    }
    assert!(misranked.is_empty(), "{misranked:#?}");
}
