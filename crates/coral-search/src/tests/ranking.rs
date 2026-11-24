//! Ranking regressions: queries whose first hits were wrong, each over a
//! small invented catalog that reproduces the defect on both backends.
//!
//! The catalog has three Providers and two Private Tools. GitHub holds
//! many Tools whose names hold `search`, `issues` and `list`; Slack and
//! Linear hold few, which match a query mostly through their Provider's
//! name. Each Private Tool shares one body word with a query that a Tool
//! matches better.

use coral_policy::ToolAccessState;
use coral_provider::access::CredentialRoute;
use coral_test_support::change;
use coral_tools::{
    AuthoredToolDescription, AuthoredToolSource, PrivateToolPath, SharedToolOrigin, SharedToolPath,
    SharedToolPublish,
};
use coral_workspace::{AuthenticatedPrincipal, IfMatch, WorkspaceId};

use super::{SMALL_TOOL_SOURCE, TestModules, organization_user};
use crate::{SearchQuery, SearchRequest};

const USER: &str = "user:search-ranking";
const OTHER_USER: &str = "user:search-ranking-other";

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

/// One User's Workspace with the three Providers and two Private Tools,
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
            .save_private_tool(&principal, &workspace, path, description, at)
            .await;
    }
    modules.converge().await;
    (modules, principal, workspace)
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
             GitHub Tools whose names hold `search`, and a Private Tool that \
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
            "a Tool whose name and title hold the whole query outranks a Private \
             Tool whose description holds only `list`",
        ),
        (
            "channel messages",
            "coral.private.digest.weekly",
            0,
            "a Private Tool whose description holds the whole query outranks \
             Tools that hold its words apart",
        ),
    ];
    let mut misranked = Vec::new();
    for (query, expected, most_above, reason) in cases {
        let ranked = ranked(&modules, &principal, &workspace, query).await;
        if ranked
            .iter()
            .position(|path| path == expected)
            .is_none_or(|position| position > most_above)
        {
            misranked.push(format!("{query}: {reason}; order {ranked:?}"));
        }
    }
    assert!(misranked.is_empty(), "{misranked:#?}");
}

#[tokio::test]
async fn a_weak_private_tool_follows_every_strong_tool() {
    let (modules, principal, workspace) = catalog().await;
    // Each row: the query, a Private Tool that shares only a description
    // word with it, and Tools that are strong matches: every Tool of the
    // Provider the query names, and a Tool whose name and title start words
    // with half of the query's content words.
    let cases = [
        (
            "search slack messages",
            "coral.private.digest.weekly",
            &[
                "coral.providers.slack.conversations.history",
                "coral.providers.slack.users.list",
                "coral.providers.slack.chat.postMessage",
            ][..],
        ),
        (
            "slack list",
            "coral.private.standup.notes",
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
/// disagree, and a Private Tool that holds both query words apart in its
/// description. `deployments.listStatuses` leads its window by its score:
/// its name and title start words with both query words, though neither is
/// a whole word there. `status.get` holds `status` whole in its name. A
/// merge that compares whole-word counts across kinds puts the Private
/// Tool first, since it holds more whole words than the window's head,
/// and so above `status.get`, which holds more than the Private Tool.
#[tokio::test]
async fn a_private_tool_between_two_tools_follows_both_when_it_is_weak() {
    let modules = TestModules::new().await;
    let user = organization_user(modules.search.database.clone(), USER).await;
    let principal = AuthenticatedPrincipal::user(user.clone());
    let workspace = modules.create_workspace(&user).await;
    modules
        .publish_openapi(&user, &workspace, OPS, "ops", 20)
        .await;
    modules
        .save_private_tool(
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
            "coral.private.alerts.notify",
        ],
        "both Tools are strong by the Tool rule and keep their statement's \
         order; the Private Tool's path starts no query word, so it is weak \
         and follows them"
    );
}

/// Ten Tools. Five hold a word that starts with `list` in their path, and
/// none holds `list` itself. No Tool path holds a word that starts with
/// `invoices`.
const SHOP: &str = r#"{
  "openapi": "3.1.0",
  "info": { "title": "Shop", "version": "1.0.0" },
  "servers": [{ "url": "https://api.shop.example" }],
  "security": [],
  "paths": {
    "/a": { "get": { "operationId": "listings/get", "summary": "Get a listing",
      "responses": { "200": { "description": "OK" } } } },
    "/b": { "get": { "operationId": "listings/archive", "summary": "Archive a listing",
      "responses": { "200": { "description": "OK" } } } },
    "/c": { "get": { "operationId": "listings/count", "summary": "Count the listings",
      "responses": { "200": { "description": "OK" } } } },
    "/d": { "get": { "operationId": "listeners/add", "summary": "Add a webhook listener",
      "responses": { "200": { "description": "OK" } } } },
    "/e": { "get": { "operationId": "listed/orders", "summary": "Orders of listed goods",
      "responses": { "200": { "description": "OK" } } } },
    "/f": { "get": { "operationId": "orders/get", "summary": "Get an order",
      "responses": { "200": { "description": "OK" } } } },
    "/g": { "get": { "operationId": "carts/clear", "summary": "Clear a cart",
      "responses": { "200": { "description": "OK" } } } },
    "/h": { "get": { "operationId": "refunds/create", "summary": "Create a refund",
      "responses": { "200": { "description": "OK" } } } },
    "/i": { "get": { "operationId": "coupons/apply", "summary": "Apply a coupon",
      "responses": { "200": { "description": "OK" } } } },
    "/j": { "get": { "operationId": "stock/count", "summary": "Count the stock",
      "responses": { "200": { "description": "OK" } } } }
  }
}"#;

/// The Shop Tools that are strong for `list invoices` by the Tool rule:
/// their names start a word with one of its two content words.
const SHOP_STRONG: [&str; 5] = [
    "coral.providers.shop.listings.get",
    "coral.providers.shop.listings.archive",
    "coral.providers.shop.listings.count",
    "coral.providers.shop.listeners.add",
    "coral.providers.shop.listed.orders",
];

/// An Authored Tool is strong by a path word only when the word is rare: in
/// under 10% of the Workspace's Provider Tool paths. `list` starts a word of
/// half the Shop paths, so it is common, though no path holds `list` itself.
/// `invoices` starts a word of none. A phrase match stays strong.
#[tokio::test]
async fn an_authored_tool_is_strong_by_a_path_word_only_when_the_word_is_rare() {
    let modules = TestModules::new().await;
    let user = organization_user(modules.search.database.clone(), USER).await;
    let principal = AuthenticatedPrincipal::user(user.clone());
    let workspace = modules.create_workspace(&user).await;
    modules
        .publish_openapi(&user, &workspace, SHOP, "shop", 20)
        .await;
    for (path, description, at) in [
        ("weekly.listAll", "Sum the orders of the week.", 30),
        ("billing.invoices", "Sum the open amounts.", 31),
        ("monthly.listTotals", "List invoices of the month.", 32),
        ("drafts.alpha", "A first draft.", 33),
        ("drafts.beta", "A second draft.", 34),
    ] {
        modules
            .save_private_tool(&principal, &workspace, path, description, at)
            .await;
    }
    for (path, origin) in [
        ("team.listOpen", "drafts.alpha"),
        ("finance.invoicesDue", "drafts.beta"),
    ] {
        modules
            .publish_shared_tool(
                &user,
                &workspace,
                SharedToolPublish::review(
                    SharedToolPath::parse(path).unwrap(),
                    SharedToolOrigin::PrivateTool {
                        owner: principal.principal().clone(),
                        path: PrivateToolPath::parse(origin).unwrap(),
                    },
                    AuthoredToolDescription::parse("Sum the open amounts.").unwrap(),
                    AuthoredToolSource::parse(SMALL_TOOL_SOURCE).unwrap(),
                    ToolAccessState::Enabled,
                    0,
                ),
            )
            .await;
    }
    modules.converge().await;

    let ranked = ranked(&modules, &principal, &workspace, "list invoices").await;
    let mut misranked = Vec::new();
    for (path, strong, reason) in [
        (
            "coral.private.weekly.listAll",
            false,
            "a Private Tool whose path holds only the common word",
        ),
        (
            "coral.private.billing.invoices",
            true,
            "a Private Tool whose path holds the rare word",
        ),
        (
            "coral.private.monthly.listTotals",
            true,
            "a Private Tool whose path holds only the common word, and whose \
             description holds the query's words in order",
        ),
        (
            "coral.shared.team.listOpen",
            false,
            "a Shared Tool whose path holds only the common word",
        ),
        (
            "coral.shared.finance.invoicesDue",
            true,
            "a Shared Tool whose path holds a word that starts with the rare word",
        ),
    ] {
        if before_a_strong_tool(&ranked, path, &SHOP_STRONG) != strong {
            misranked.push(format!(
                "{path}: {reason}, so strong is {strong}; order {ranked:?}"
            ));
        }
    }
    assert!(misranked.is_empty(), "{misranked:#?}");
}

/// Ten Tools. Two are strong for `list invoices` by the Tool rule. No path
/// holds a word that starts with `list`.
const LEDGER: &str = r#"{
  "openapi": "3.1.0",
  "info": { "title": "Ledger", "version": "1.0.0" },
  "servers": [{ "url": "https://api.ledger.example" }],
  "security": [],
  "paths": {
    "/a": { "get": { "operationId": "invoices/find", "summary": "Find invoices",
      "responses": { "200": { "description": "OK" } } } },
    "/b": { "get": { "operationId": "invoices/void", "summary": "Void an invoice",
      "responses": { "200": { "description": "OK" } } } },
    "/c": { "get": { "operationId": "carts/clear", "summary": "Clear a cart",
      "responses": { "200": { "description": "OK" } } } },
    "/d": { "get": { "operationId": "refunds/create", "summary": "Create a refund",
      "responses": { "200": { "description": "OK" } } } },
    "/e": { "get": { "operationId": "coupons/apply", "summary": "Apply a coupon",
      "responses": { "200": { "description": "OK" } } } },
    "/f": { "get": { "operationId": "stock/count", "summary": "Count the stock",
      "responses": { "200": { "description": "OK" } } } },
    "/g": { "get": { "operationId": "orders/get", "summary": "Get an order",
      "responses": { "200": { "description": "OK" } } } },
    "/h": { "get": { "operationId": "orders/cancel", "summary": "Cancel an order",
      "responses": { "200": { "description": "OK" } } } },
    "/i": { "get": { "operationId": "rates/get", "summary": "Get the rates",
      "responses": { "200": { "description": "OK" } } } },
    "/j": { "get": { "operationId": "taxes/get", "summary": "Get the taxes",
      "responses": { "200": { "description": "OK" } } } }
  }
}"#;

/// [`SHOP`] with no path word that starts with `list`.
fn shop_without_list_words() -> serde_json::Value {
    serde_json::from_str(
        &SHOP
            .replace("listings/", "catalog/")
            .replace("listeners/", "hooks/")
            .replace("listed/", "sold/"),
    )
    .unwrap()
}

/// The counts follow the catalog: a Provider install, update and removal
/// change whether `list` is rare. With the Ledger alone no path word starts
/// with `list`; with the Shop as well, five of twenty paths hold one. The
/// counts are per Workspace: a Shop in another Workspace changes nothing.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one catalog goes through each install, update and removal in order"
)]
async fn the_path_word_counts_follow_the_catalog_of_each_workspace() {
    let modules = TestModules::new().await;
    let user = organization_user(modules.search.database.clone(), USER).await;
    let principal = AuthenticatedPrincipal::user(user.clone());
    let workspace = modules.create_workspace(&user).await;
    let other_user = organization_user(modules.search.database.clone(), OTHER_USER).await;
    let other_workspace = modules.create_workspace(&other_user).await;
    modules
        .publish_openapi(&other_user, &other_workspace, SHOP, "shop", 20)
        .await;
    modules
        .publish_openapi(&user, &workspace, LEDGER, "ledger", 21)
        .await;
    modules
        .save_private_tool(
            &principal,
            &workspace,
            "weekly.listAll",
            "Sum the orders of the week.",
            30,
        )
        .await;
    let ledger_strong = [
        "coral.providers.ledger.invoices.find",
        "coral.providers.ledger.invoices.void",
    ];
    // Each stage converges the index first, unless it asks for the index as
    // the catalog change left it.
    let strong = async |stage: &str, converge: bool| {
        if converge {
            modules.converge().await;
            assert_eq!(
                path_word_generations(&modules, &workspace).await,
                complete_generations(&modules, &workspace).await,
                "{stage}: each complete Generation has path words, and no other one has"
            );
        }
        let ranked = ranked(&modules, &principal, &workspace, "list invoices").await;
        let strong = before_a_strong_tool(&ranked, "coral.private.weekly.listAll", &ledger_strong);
        (stage.to_string(), strong)
    };

    let mut stages = vec![strong("the Shop is in another Workspace only", true).await];
    let shop = modules
        .publish_openapi(&user, &workspace, SHOP, "shop", 40)
        .await;
    stages.push(strong("the Shop is installed", true).await);
    modules
        .providers
        .delete_provider(
            &change(user.clone(), 50, '5'),
            &workspace,
            &shop,
            &IfMatch::any(),
        )
        .await
        .expect("delete the Shop")
        .skip_search();
    stages.push(strong("the Shop is removed; the index still holds its rows", false).await);
    stages.push(strong("the Shop is removed", true).await);
    let shop = modules
        .publish_configured(
            &change(user.clone(), 60, '6'),
            &workspace,
            &shop_without_list_words(),
            "mart",
            CredentialRoute::None,
        )
        .await;
    stages.push(strong("a Shop with no `list` word is installed", true).await);
    modules
        .regenerate_workspace_provider(
            &change(user.clone(), 70, '7'),
            &workspace,
            shop.id(),
            &serde_json::from_str(SHOP).unwrap(),
        )
        .await;
    stages.push(strong("that Shop is updated to the `list` words", true).await);
    modules
        .regenerate_workspace_provider(
            &change(user.clone(), 80, '8'),
            &workspace,
            shop.id(),
            &shop_without_list_words(),
        )
        .await;
    stages.push(
        strong(
            "that Shop is updated back; the index still holds the replaced Generation",
            false,
        )
        .await,
    );
    stages.push(strong("that Shop is updated back", true).await);

    assert_eq!(
        stages,
        [
            ("the Shop is in another Workspace only", true),
            ("the Shop is installed", false),
            ("the Shop is removed; the index still holds its rows", true),
            ("the Shop is removed", true),
            ("a Shop with no `list` word is installed", true),
            ("that Shop is updated to the `list` words", false),
            (
                "that Shop is updated back; the index still holds the replaced Generation",
                true
            ),
            ("that Shop is updated back", true),
        ]
        .map(|(stage, strong)| (stage.to_string(), strong)),
        "a Private Tool whose path holds only `list` is strong while `list` is rare in the \
         active Generations"
    );
}

/// Twenty Tools. Only `issues.getIssue` holds a word that starts with `get`,
/// and it holds two: `get` and `getissue`. Two Tools are strong for
/// `get invoices` by `invoices`.
const DESK: &str = r#"{
  "openapi": "3.1.0",
  "info": { "title": "Desk", "version": "1.0.0" },
  "servers": [{ "url": "https://api.desk.example" }],
  "security": [],
  "paths": {
    "/a": { "get": { "operationId": "issues/getIssue", "summary": "Read an issue",
      "responses": { "200": { "description": "OK" } } } },
    "/b": { "get": { "operationId": "invoices/find", "summary": "Find invoices",
      "responses": { "200": { "description": "OK" } } } },
    "/c": { "get": { "operationId": "invoices/void", "summary": "Void an invoice",
      "responses": { "200": { "description": "OK" } } } },
    "/d": { "get": { "operationId": "carts/clear", "summary": "Clear a cart",
      "responses": { "200": { "description": "OK" } } } },
    "/e": { "get": { "operationId": "refunds/create", "summary": "Create a refund",
      "responses": { "200": { "description": "OK" } } } },
    "/f": { "get": { "operationId": "coupons/apply", "summary": "Apply a coupon",
      "responses": { "200": { "description": "OK" } } } },
    "/g": { "get": { "operationId": "stock/count", "summary": "Count the stock",
      "responses": { "200": { "description": "OK" } } } },
    "/h": { "get": { "operationId": "orders/cancel", "summary": "Cancel an order",
      "responses": { "200": { "description": "OK" } } } },
    "/i": { "get": { "operationId": "rates/read", "summary": "Read the rates",
      "responses": { "200": { "description": "OK" } } } },
    "/j": { "get": { "operationId": "taxes/read", "summary": "Read the taxes",
      "responses": { "200": { "description": "OK" } } } },
    "/k": { "get": { "operationId": "teams/add", "summary": "Add a team",
      "responses": { "200": { "description": "OK" } } } },
    "/l": { "get": { "operationId": "teams/remove", "summary": "Remove a team",
      "responses": { "200": { "description": "OK" } } } },
    "/m": { "get": { "operationId": "notes/write", "summary": "Write a note",
      "responses": { "200": { "description": "OK" } } } },
    "/n": { "get": { "operationId": "notes/erase", "summary": "Erase a note",
      "responses": { "200": { "description": "OK" } } } },
    "/o": { "get": { "operationId": "tags/attach", "summary": "Attach a tag",
      "responses": { "200": { "description": "OK" } } } },
    "/p": { "get": { "operationId": "tags/detach", "summary": "Detach a tag",
      "responses": { "200": { "description": "OK" } } } },
    "/q": { "get": { "operationId": "users/invite", "summary": "Invite a user",
      "responses": { "200": { "description": "OK" } } } },
    "/r": { "get": { "operationId": "users/block", "summary": "Block a user",
      "responses": { "200": { "description": "OK" } } } },
    "/s": { "get": { "operationId": "files/upload", "summary": "Upload a file",
      "responses": { "200": { "description": "OK" } } } },
    "/t": { "get": { "operationId": "files/share", "summary": "Share a file",
      "responses": { "200": { "description": "OK" } } } }
  }
}"#;

/// A word's count is the sum of the Tool counts of the stored words that
/// start with it, an upper bound. One of twenty Desk paths holds a word that
/// starts with `get`, but it holds two such words, so `get` counts as in 10%
/// of the paths: not under 10%, so common.
#[tokio::test]
async fn a_path_with_two_words_that_start_with_a_query_word_counts_twice() {
    let modules = TestModules::new().await;
    let user = organization_user(modules.search.database.clone(), USER).await;
    let principal = AuthenticatedPrincipal::user(user.clone());
    let workspace = modules.create_workspace(&user).await;
    modules
        .publish_openapi(&user, &workspace, DESK, "desk", 20)
        .await;
    modules
        .save_private_tool(
            &principal,
            &workspace,
            "weekly.getAll",
            "Sum the orders of the week.",
            30,
        )
        .await;
    modules.converge().await;

    let ranked = ranked(&modules, &principal, &workspace, "get invoices").await;
    assert!(
        !before_a_strong_tool(
            &ranked,
            "coral.private.weekly.getAll",
            &[
                "coral.providers.desk.issues.getIssue",
                "coral.providers.desk.invoices.find",
                "coral.providers.desk.invoices.void",
            ],
        ),
        "`get` counts as common, so the Private Tool is weak; order {ranked:?}"
    );
}

/// In a Workspace with no Provider Tool every path word is rare. The
/// Private Tool with the phrase in its path heads the Private window, so the
/// one whose path holds only `list` is second there. Strong, it comes before
/// the weak Shared Tool at the head of the Shared window; weak, after it.
#[tokio::test]
async fn every_path_word_is_rare_in_a_workspace_with_no_provider_tool() {
    let modules = TestModules::new().await;
    let user = organization_user(modules.search.database.clone(), USER).await;
    let principal = AuthenticatedPrincipal::user(user.clone());
    let workspace = modules.create_workspace(&user).await;
    for (path, description, at) in [
        ("list.invoices", "Sum the open amounts.", 30),
        ("weekly.listAll", "Sum the orders of the week.", 31),
        ("drafts.alpha", "A first draft.", 32),
    ] {
        modules
            .save_private_tool(&principal, &workspace, path, description, at)
            .await;
    }
    modules
        .publish_shared_tool(
            &user,
            &workspace,
            SharedToolPublish::review(
                SharedToolPath::parse("team.totals").unwrap(),
                SharedToolOrigin::PrivateTool {
                    owner: principal.principal().clone(),
                    path: PrivateToolPath::parse("drafts.alpha").unwrap(),
                },
                AuthoredToolDescription::parse("Sum the invoices.").unwrap(),
                AuthoredToolSource::parse(SMALL_TOOL_SOURCE).unwrap(),
                ToolAccessState::Enabled,
                0,
            ),
        )
        .await;
    modules.converge().await;

    assert_eq!(
        ranked(&modules, &principal, &workspace, "list invoices").await,
        [
            "coral.private.list.invoices",
            "coral.private.weekly.listAll",
            "coral.shared.team.totals",
        ],
        "no Provider Tool path holds `list`, so the Private Tool is strong by it"
    );
}

/// The Generations of the Workspace that have path words.
async fn path_word_generations(modules: &TestModules, workspace: &WorkspaceId) -> Vec<String> {
    let mut transaction = modules.search.database.begin_read_snapshot().await.unwrap();
    let generations =
        crate::indexer::path_word_generations_in_workspace(&mut transaction, workspace)
            .await
            .unwrap();
    transaction.rollback().await.unwrap();
    generations
}

/// The Generations of the Workspace whose index state is `complete`, in id
/// order.
async fn complete_generations(modules: &TestModules, workspace: &WorkspaceId) -> Vec<String> {
    let mut transaction = modules.search.database.begin_read_snapshot().await.unwrap();
    let states = crate::indexer::generation_states_in_workspace(&mut transaction, workspace)
        .await
        .unwrap();
    transaction.rollback().await.unwrap();
    states
        .into_iter()
        .filter(|(_provider, _generation, state, _name)| state == "complete")
        .map(|(_provider, generation, _state, _name)| generation)
        .collect()
}

/// Whether `path` ranks before the last of `strong_tools`. The fusion puts
/// a strong Authored Tool at the head of its window before the strong Tools
/// behind the head of theirs, and a weak one after every strong Tool.
fn before_a_strong_tool(ranked: &[String], path: &str, strong_tools: &[&str]) -> bool {
    let position = |path: &str| {
        ranked
            .iter()
            .position(|ranked| ranked == path)
            .unwrap_or_else(|| panic!("{path} is not on the page {ranked:?}"))
    };
    Some(position(path)) < strong_tools.iter().map(|tool| position(tool)).max()
}

/// The Call Paths of the first page for `query`, in rank order.
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
    page.hits()
        .iter()
        .map(|item| item.call_path().as_str().to_string())
        .collect()
}
