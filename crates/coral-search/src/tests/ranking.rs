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
async fn a_weak_saved_function_follows_every_strong_tool() {
    let (modules, principal, workspace) = catalog().await;
    // Each row: the query, a Saved Function that shares only a description
    // word with it, and Tools that are strong matches: every Tool of the
    // Provider the query names, and a Tool whose name and title start words
    // with half of the query's content words.
    let cases = [
        (
            "search slack messages",
            "coral.functions.digest.weekly",
            &[
                "coral.providers.slack.conversations.history",
                "coral.providers.slack.users.list",
                "coral.providers.slack.chat.postMessage",
            ][..],
        ),
        (
            "slack list",
            "coral.functions.standup.notes",
            &[
                "coral.providers.slack.users.list",
                "coral.providers.slack.conversations.history",
                "coral.providers.slack.chat.postMessage",
                "coral.providers.github.pulls.list",
            ],
        ),
    ];
    let mut misranked = Vec::new();
    for (query, function, stronger) in cases {
        let ranked = ranked(&modules, &principal, &workspace, query).await;
        let position = |path: &str| ranked.iter().position(|ranked| ranked == path);
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

const OPS: &str = r#"{
  "openapi": "3.1.0",
  "info": { "title": "Ops", "version": "1.0.0" },
  "servers": [{ "url": "https://api.ops.example" }],
  "security": [],
  "paths": {
    "/deployments/statuses": { "get": { "operationId": "deployments/listStatuses",
      "summary": "List deployment statuses",
      "description": "Lists the status of deployments.",
      "responses": { "200": { "description": "OK" } } } },
    "/status": { "get": { "operationId": "status/get", "summary": "Get the pipeline",
      "description": "Returns the deploy state.",
      "responses": { "200": { "description": "OK" } } } }
  }
}"#;

/// Two Tools in one phrase tier whose statement order and whole-word counts
/// disagree, and a Saved Function that holds both query words apart in its
/// description. `deployments.listStatuses` leads its window by its score:
/// its name and title start words with both query words, though neither is
/// a whole word there. `status.get` holds `status` whole in its name. A
/// merge that compares whole-word counts across kinds puts the Saved
/// Function first, since it holds more whole words than the window's head,
/// and so above `status.get`, which holds more than the Saved Function.
#[tokio::test]
async fn a_saved_function_between_two_tools_follows_both_when_it_is_weak() {
    let modules = TestModules::new().await;
    let user = organization_user(modules.search.database.clone(), USER).await;
    let principal = AuthenticatedPrincipal::user(user.clone());
    let workspace = modules.create_workspace(&user).await;
    modules
        .publish_openapi(&user, &workspace, OPS, "ops", 20)
        .await;
    modules
        .save_function(
            &principal,
            &workspace,
            "alerts.notify",
            "Notify the team of each status change after a deploy.",
            30,
        )
        .await;
    modules.converge().await;

    assert_eq!(
        ranked(&modules, &principal, &workspace, "deploy status").await,
        [
            "coral.providers.ops.deployments.listStatuses",
            "coral.providers.ops.status.get",
            "coral.functions.alerts.notify",
        ],
        "both Tools are strong by the Tool rule and keep their statement's \
         order; the Saved Function's path starts no query word, so it is weak \
         and follows them"
    );
}

async fn ranked(
    modules: &TestModules,
    principal: &AuthenticatedPrincipal,
    workspace: &WorkspaceId,
    query: &str,
) -> Vec<String> {
    let page = modules
        .search
        .search(
            principal,
            workspace,
            &SearchRequest::query(SearchQuery::parse(query).unwrap()),
        )
        .await
        .unwrap();
    paths(&page).into_iter().map(str::to_string).collect()
}
