//! Relevance inside the database, one retriever per Search Entity kind.
//!
//! Every test builds its world through real Provider publication and real
//! Saved Function saves, converges the index with one reconcile pass, and
//! observes what an agent sees through `UniversalSearchModule::search`.

use coral_catalog::ToolKey;
use coral_db::DatabaseSystem;
use coral_policy::{ToolAccessState, ToolPolicyTarget};
use coral_provider::ProviderId;
use coral_provider::access::ProviderCallRoot;
use coral_workspace::{AuthenticatedPrincipal, AuthenticatedUser, WorkspaceId};

use super::{TestModules, change, organization_user};
use crate::{
    CallPath, EntityKind, RetrieverState, RetrieverStatus, SearchPage, SearchQuery, SearchRequest,
};

const USER: &str = "user:search-retrieval";

/// Two GitHub Tools and two Saved Functions that all match `issues`.
const GITHUB: &str = r#"{
  "openapi": "3.1.0",
  "info": { "title": "GitHub", "version": "1.0.0" },
  "servers": [{ "url": "https://api.github.example" }],
  "security": [],
  "paths": {
    "/issues": {
      "get": {
        "operationId": "issues/list",
        "summary": "List issues",
        "description": "Returns repository tickets.",
        "responses": { "200": { "description": "OK" } }
      }
    },
    "/pulls": {
      "get": {
        "operationId": "pulls/list",
        "summary": "List pull requests",
        "description": "Pull requests are issues with code.",
        "responses": { "200": { "description": "OK" } }
      }
    }
  }
}"#;

/// The GitHub Provider regenerated: pull requests stay, issues become tickets.
const GITHUB_TICKETS: &str = r#"{
  "openapi": "3.1.0",
  "info": { "title": "GitHub", "version": "2.0.0" },
  "servers": [{ "url": "https://api.github.example" }],
  "security": [],
  "paths": {
    "/tickets": {
      "get": {
        "operationId": "tickets/list",
        "summary": "List tickets",
        "description": "Returns repository tickets.",
        "responses": { "200": { "description": "OK" } }
      }
    }
  }
}"#;

/// One User's Workspace with the four-Entity corpus, converged.
struct Corpus {
    modules: TestModules,
    user: AuthenticatedUser,
    principal: AuthenticatedPrincipal,
    workspace: WorkspaceId,
    github: ProviderId,
}

impl Corpus {
    async fn new() -> Self {
        let corpus = Self::unconverged().await;
        corpus.modules.converge().await;
        corpus
    }

    /// The corpus, published and saved, before any index write.
    async fn unconverged() -> Self {
        let modules = TestModules::new().await;
        let user = organization_user(modules.search.database.clone(), USER).await;
        let principal = AuthenticatedPrincipal::user(user.clone());
        let workspace = modules.create_workspace(&user).await;
        let github = modules
            .publish_openapi(&user, &workspace, GITHUB, "github", 20)
            .await;
        modules
            .save_function(
                &principal,
                &workspace,
                "triage.openIssues",
                "Open issues that wait for triage.",
                30,
            )
            .await;
        modules
            .save_function(
                &principal,
                &workspace,
                "reports.weekly",
                "A weekly report of closed issues.",
                40,
            )
            .await;
        Self {
            modules,
            user,
            principal,
            workspace,
            github,
        }
    }

    /// An Agent of the corpus Workspace.
    async fn agent(&self) -> AuthenticatedPrincipal {
        let issued = self
            .modules
            .workspaces
            .create_agent(
                &change(self.user.clone(), 65, '6'),
                &self.workspace,
                coral_workspace::AgentDisplayName::parse("Worker").unwrap(),
                None,
            )
            .await
            .unwrap();
        self.modules
            .workspaces
            .authenticate_agent(&self.workspace, issued.credential.expose(), 66)
            .await
            .unwrap()
    }

    /// Indexes the GitHub Provider's active Generation the way a
    /// notification does, and waits for the write.
    async fn notify_active_generation(&self) {
        let mut transaction = self
            .modules
            .search
            .database
            .begin_read_snapshot()
            .await
            .unwrap();
        let entries = coral_provider::prepare_active_generation_entries_in_transaction(
            &mut transaction,
            &self.workspace,
            &self.github,
        )
        .await
        .unwrap()
        .expect("active Generation");
        transaction.rollback().await.unwrap();
        self.modules.index.index_generation(
            self.workspace.clone(),
            self.github.clone(),
            entries.generation_id().clone(),
        );
        self.modules.index.settled().await;
    }

    fn query(text: &str) -> SearchRequest {
        SearchRequest::query(SearchQuery::parse(text).unwrap())
    }

    async fn search(&self, request: SearchRequest) -> SearchPage {
        self.modules
            .search
            .search(&self.principal, &self.workspace, &request)
            .await
            .expect("search the corpus")
    }
}

fn paths(page: &SearchPage) -> Vec<&str> {
    page.hits()
        .iter()
        .map(|item| item.call_path().as_str())
        .collect()
}

fn status(page: &SearchPage, kind: EntityKind) -> RetrieverStatus {
    *page
        .retriever_statuses()
        .iter()
        .find(|status| status.retriever() == kind)
        .expect("one status per retriever")
}

fn state(page: &SearchPage, kind: EntityKind) -> RetrieverState {
    status(page, kind).state()
}

#[tokio::test]
async fn a_mixed_query_fuses_each_kinds_ranking_by_position() {
    let corpus = Corpus::new().await;
    let issues = SearchRequest::query(SearchQuery::parse("issues").unwrap());

    let page = corpus.search(issues.clone()).await;
    assert_eq!(
        paths(&page),
        [
            "coral.functions.triage.openIssues",
            "coral.providers.github.issues.list",
            "coral.functions.reports.weekly",
            "coral.providers.github.pulls.list",
        ],
        "equal ranks put the Saved Function first"
    );
    assert_eq!(
        state(&page, EntityKind::ProviderTool),
        RetrieverState::ResultsFound
    );
    assert_eq!(
        state(&page, EntityKind::SavedFunction),
        RetrieverState::ResultsFound
    );

    // The Provider filter narrows Tools only: Saved Functions stay, for
    // a query and for a listing.
    let github = ProviderCallRoot::parse("coral.providers.github").unwrap();
    let filtered = corpus.search(issues.with_provider(github.clone())).await;
    assert_eq!(paths(&filtered), paths(&page));
    assert_eq!(
        state(&filtered, EntityKind::SavedFunction),
        RetrieverState::ResultsFound
    );
    let listed = corpus
        .search(SearchRequest::list().with_provider(github))
        .await;
    for function in [
        "coral.functions.reports.weekly",
        "coral.functions.triage.openIssues",
    ] {
        assert!(paths(&listed).contains(&function), "{function}");
    }
}

#[tokio::test]
async fn relevance_waits_for_the_index_while_listing_and_describe_do_not() {
    let corpus = Corpus::unconverged().await;
    let tools = [
        "coral.providers.github.issues.list",
        "coral.providers.github.pulls.list",
    ];

    let listed = corpus.search(SearchRequest::list()).await;
    assert!(
        tools.iter().all(|tool| paths(&listed).contains(tool)),
        "listing reads no index"
    );
    let described = corpus
        .modules
        .search
        .describe(
            &corpus.principal,
            &corpus.workspace,
            &CallPath::parse(tools[0]).unwrap(),
            &[],
        )
        .await
        .expect("describe reads no index");
    assert_eq!(described.authoring().call_path().as_str(), tools[0]);

    let before = corpus.search(Corpus::query("issues")).await;
    assert!(
        paths(&before)
            .iter()
            .all(|path| !path.starts_with("coral.providers.")),
        "relevance omits a Provider whose rows have not landed"
    );
    let tool = status(&before, EntityKind::ProviderTool);
    assert_eq!(tool.state(), RetrieverState::Partial);
    assert_eq!(
        (
            tool.coverage().eligible_units(),
            tool.coverage().searched_units(),
            tool.coverage().failed_units(),
        ),
        (1, 0, 1)
    );

    corpus.modules.converge().await;
    let after = corpus.search(Corpus::query("issues")).await;
    assert!(paths(&after).contains(&tools[0]));
    assert_eq!(
        state(&after, EntityKind::ProviderTool),
        RetrieverState::ResultsFound
    );

    corpus
        .modules
        .regenerate_workspace_provider(
            &change(corpus.user.clone(), 60, '6'),
            &corpus.workspace,
            &corpus.github,
            &serde_json::from_str(GITHUB_TICKETS).unwrap(),
        )
        .await;
    let superseded = corpus.search(Corpus::query("issues")).await;
    assert!(
        paths(&superseded)
            .iter()
            .all(|path| !path.starts_with("coral.providers.")),
        "a superseded Generation's rows never rank, even before they go"
    );
    assert_eq!(
        state(&superseded, EntityKind::ProviderTool),
        RetrieverState::Partial
    );

    // Index the new Generation without the removal a reconcile pass runs
    // first: the old rows stay stored.
    corpus.notify_active_generation().await;
    let list = corpus.search(Corpus::query("list")).await;
    assert_eq!(
        paths(&list)
            .into_iter()
            .filter(|path| path.starts_with("coral.providers."))
            .collect::<Vec<_>>(),
        ["coral.providers.github.tickets.list"],
        "a superseded Generation's stored rows never rank"
    );
    assert_eq!(
        status(&list, EntityKind::ProviderTool)
            .coverage()
            .returned_count(),
        1,
        "the ranked window holds only the active Generation's Tool, not the stored superseded ones"
    );
}

#[tokio::test]
async fn hits_leave_out_what_the_caller_may_not_see() {
    let corpus = Corpus::new().await;
    let agent = corpus.agent().await;
    corpus
        .modules
        .save_function(
            &agent,
            &corpus.workspace,
            "agent.issueDigest",
            "The agent's digest of issues.",
            70,
        )
        .await;
    corpus
        .modules
        .policies
        .set_access_state(
            &change(corpus.user.clone(), 80, '8'),
            ToolPolicyTarget::new(
                corpus.workspace.clone(),
                corpus.github.clone(),
                ToolKey::parse("issues/list").unwrap(),
            ),
            ToolAccessState::Disabled,
        )
        .await
        .unwrap();
    // A caller route: the Agent has no Account, so the Provider is not ready
    // for it.
    corpus
        .modules
        .publish_configured(
            &change(corpus.user.clone(), 90, '9'),
            &corpus.workspace,
            &super::credential_document(),
            "weather",
            coral_provider::access::CredentialRoute::Caller,
        )
        .await;
    corpus.modules.converge().await;

    let page = corpus.search(Corpus::query("issues")).await;
    assert_eq!(
        paths(&page),
        [
            "coral.functions.triage.openIssues",
            "coral.providers.github.pulls.list",
            "coral.functions.reports.weekly",
        ],
        "neither a disabled Tool nor another Principal's Saved Function ranks"
    );

    let weather = Corpus::query("forecast")
        .with_provider(ProviderCallRoot::parse("coral.providers.weather").unwrap());
    assert_eq!(
        paths(&corpus.search(weather).await),
        ["coral.providers.weather.getForecast"],
        "the User may see the caller-route Provider"
    );
    // A Call Root the caller cannot search is refused, and the refusal lists
    // only the Call Roots the caller can search: those `start_task` shows.
    for (principal, call_root, searchable) in [
        (
            &corpus.principal,
            "coral.providers.absent",
            &["coral.providers.github", "coral.providers.weather"][..],
        ),
        (
            &agent,
            "coral.providers.weather",
            &["coral.providers.github"],
        ),
    ] {
        let shown = corpus
            .modules
            .search
            .searchable_providers(principal, &corpus.workspace)
            .await
            .unwrap();
        assert_eq!(
            shown
                .iter()
                .map(ProviderCallRoot::as_str)
                .collect::<Vec<_>>(),
            searchable
        );
        for request in [Corpus::query("forecast"), SearchRequest::list()] {
            let error = corpus
                .modules
                .search
                .search(
                    principal,
                    &corpus.workspace,
                    &request.with_provider(ProviderCallRoot::parse(call_root).unwrap()),
                )
                .await
                .expect_err("an unknown Call Root is refused");
            assert!(
                matches!(
                    &error,
                    crate::Error::UnknownProviderCallRoot { searchable: listed }
                        if listed.iter().map(ProviderCallRoot::as_str).eq(searchable.iter().copied())
                ),
                "{call_root}: {error:?}"
            );
        }
    }
}

#[tokio::test]
async fn a_filtered_search_reads_only_the_named_provider() {
    let corpus = Corpus::new().await;
    corpus
        .modules
        .publish_openapi(&corpus.user, &corpus.workspace, CAMEL, "weather", 90)
        .await;
    corpus.modules.converge().await;
    // A fault in the unrelated Provider's credential contract.
    corpus
        .modules
        .fixture
        .execute_fixture_sql(
            "UPDATE provider_generations SET credential_contract = 'not a contract' \
             WHERE id IN (SELECT active_generation_id FROM provider_records \
                          WHERE namespace = 'coral.providers.weather')",
        )
        .await
        .unwrap();
    assert!(
        matches!(
            corpus
                .modules
                .search
                .search(
                    &corpus.principal,
                    &corpus.workspace,
                    &Corpus::query("issues")
                )
                .await,
            Err(crate::Error::Provider(_))
        ),
        "the fault reaches a search of every Provider"
    );
    let github = ProviderCallRoot::parse("coral.providers.github").unwrap();
    for request in [Corpus::query("issues"), SearchRequest::list()] {
        let page = corpus.search(request.with_provider(github.clone())).await;
        assert!(
            paths(&page).contains(&"coral.providers.github.issues.list"),
            "{:?}",
            paths(&page)
        );
    }
}

#[tokio::test]
async fn a_retriever_that_cannot_rank_keeps_the_others_results() {
    let corpus = Corpus::new().await;
    let search = |kinds: &[EntityKind]| corpus.modules.failing_search(kinds);
    let issues = Corpus::query("issues");

    let page = search(&[EntityKind::ProviderTool])
        .search(&corpus.principal, &corpus.workspace, &issues)
        .await
        .expect("the Saved Function retriever still ranks");
    assert_eq!(
        paths(&page),
        [
            "coral.functions.triage.openIssues",
            "coral.functions.reports.weekly",
        ]
    );
    assert_eq!(
        state(&page, EntityKind::ProviderTool),
        RetrieverState::Error
    );
    assert_eq!(
        state(&page, EntityKind::SavedFunction),
        RetrieverState::ResultsFound
    );

    assert!(matches!(
        search(&[EntityKind::ProviderTool, EntityKind::SavedFunction])
            .search(&corpus.principal, &corpus.workspace, &issues)
            .await,
        Err(crate::Error::ServiceUnavailable)
    ));

    corpus
        .modules
        .fixture
        .execute_fixture_sql(
            "ALTER TABLE saved_function_records RENAME TO saved_function_records_away",
        )
        .await
        .unwrap();
    assert!(
        matches!(
            corpus
                .modules
                .search
                .search(&corpus.principal, &corpus.workspace, &issues)
                .await,
            Err(crate::Error::Functions(coral_functions::Error::Database(_)))
        ),
        "a shared read failure fails the request, not one retriever"
    );
}

/// Short rows of similar length, distinct words per field, no shared
/// prefixes: field weights alone decide each pinned order.
const SAME_ORDER: &str = r#"{
  "openapi": "3.1.0",
  "info": { "title": "Orders", "version": "1.0.0" },
  "servers": [{ "url": "https://orders.example" }],
  "security": [],
  "paths": {
    "/a": { "get": { "operationId": "cobalt/fetch", "summary": "Plain lookup",
      "responses": { "200": { "description": "OK" } } } },
    "/b": { "get": { "operationId": "plain/read", "summary": "Plain lookup",
      "parameters": [{ "name": "cobalt", "in": "query", "schema": { "type": "string" } }],
      "responses": { "200": { "description": "OK" } } } },
    "/c": { "get": { "operationId": "first/item", "summary": "Saffron report",
      "description": "Weekly totals.",
      "responses": { "200": { "description": "OK" } } } },
    "/d": { "get": { "operationId": "second/item", "summary": "Weekly totals",
      "description": "Saffron report.",
      "responses": { "200": { "description": "OK" } } } },
    "/e": { "get": { "operationId": "third/item", "summary": "Violet ledger",
      "responses": { "200": { "description": "OK" } } } },
    "/f": { "get": { "operationId": "fourth/item", "summary": "Ledger for violet",
      "responses": { "200": { "description": "OK" } } } },
    "/g": { "get": { "operationId": "alpha/tie", "summary": "Quartz lookup",
      "responses": { "200": { "description": "OK" } } } },
    "/h": { "get": { "operationId": "gamma/tie", "summary": "Quartz lookup",
      "responses": { "200": { "description": "OK" } } } },
    "/i": { "get": { "operationId": "fifth/item", "summary": "Saffron switch",
      "responses": { "200": { "description": "OK" } } } }
  }
}"#;

/// A Provider published after the index converged, so its rows never land.
const LATE: &str = r#"{
  "openapi": "3.1.0",
  "info": { "title": "Late", "version": "1.0.0" },
  "servers": [{ "url": "https://late.example" }],
  "security": [],
  "paths": {
    "/l": { "get": { "operationId": "late/item", "summary": "Saffron late",
      "responses": { "200": { "description": "OK" } } } }
  }
}"#;

#[tokio::test]
async fn both_backends_rank_one_corpus_in_the_same_order() {
    let modules = TestModules::new().await;
    let user = organization_user(modules.search.database.clone(), USER).await;
    let principal = AuthenticatedPrincipal::user(user.clone());
    let workspace = modules.create_workspace(&user).await;
    let orders = modules
        .publish_openapi(&user, &workspace, SAME_ORDER, "orders", 20)
        .await;
    modules
        .policies
        .set_access_state(
            &change(user.clone(), 30, '3'),
            ToolPolicyTarget::new(
                workspace.clone(),
                orders,
                ToolKey::parse("fifth/item").unwrap(),
            ),
            ToolAccessState::Disabled,
        )
        .await
        .unwrap();
    modules.converge().await;
    modules
        .publish_openapi(&user, &workspace, LATE, "late", 40)
        .await;

    for (query, expected, why) in [
        (
            "cobalt",
            &[
                "coral.providers.orders.cobalt.fetch",
                "coral.providers.orders.plain.read",
            ][..],
            "the qualified name outranks the searchable text",
        ),
        (
            "saffron",
            &[
                "coral.providers.orders.first.item",
                "coral.providers.orders.second.item",
            ],
            "the title outranks the description; disabled and unindexed rows stay out",
        ),
        (
            "violet ledger",
            &[
                "coral.providers.orders.third.item",
                "coral.providers.orders.fourth.item",
            ],
            "a whole-query match in the title ranks first",
        ),
        (
            "quartz",
            &[
                "coral.providers.orders.alpha.tie",
                "coral.providers.orders.gamma.tie",
            ],
            "rows that tie follow Call Path order",
        ),
    ] {
        let page = modules
            .search
            .search(&principal, &workspace, &Corpus::query(query))
            .await
            .unwrap();
        assert_eq!(paths(&page), expected, "{why}");
    }
}

#[tokio::test]
async fn evidence_explains_a_match_by_the_words_of_a_dotted_term() {
    let corpus = Corpus::new().await;
    // `issues.weekly` stays one term; no field holds it whole, but the
    // backends match its words `issues` and `weekly`.
    let page = corpus.search(Corpus::query("issues.weekly")).await;
    let weekly = page
        .hits()
        .iter()
        .find(|item| item.call_path().as_str() == "coral.functions.reports.weekly")
        .expect("a match by words keeps its hit");
    assert_eq!(weekly.evidence().fields(), ["call_path", "description"]);
}

/// Tools whose names differ only inside camelCase. `getOutlook` also
/// holds `outlook` as a word in its description, and `listWarnings` holds it
/// in its title.
const CAMEL: &str = r#"{
  "openapi": "3.1.0",
  "info": { "title": "Weather", "version": "1.0.0" },
  "servers": [{ "url": "https://weather.example" }],
  "security": [],
  "paths": {
    "/alerts": { "get": { "operationId": "getAlerts", "summary": "Tracer getAlerts",
      "responses": { "200": { "description": "OK" } } } },
    "/forecast": { "get": { "operationId": "getForecast", "summary": "Tracer getForecast",
      "responses": { "200": { "description": "OK" } } } },
    "/outlook": { "get": { "operationId": "getOutlook", "summary": "Tracer getOutlook",
      "description": "Read the outlook.",
      "responses": { "200": { "description": "OK" } } } },
    "/warnings": { "get": { "operationId": "listWarnings", "summary": "Warnings for an outlook region",
      "responses": { "200": { "description": "OK" } } } }
  }
}"#;

#[tokio::test]
async fn a_word_inside_a_camel_case_name_ranks() {
    let modules = TestModules::new().await;
    let user = organization_user(modules.search.database.clone(), USER).await;
    let principal = AuthenticatedPrincipal::user(user.clone());
    let workspace = modules.create_workspace(&user).await;
    modules
        .publish_openapi(&user, &workspace, CAMEL, "weather", 20)
        .await;
    modules.converge().await;
    // No field holds either query whole, and every row holds `weather`.
    for (query, expected, reason) in [
        (
            "weather forecast",
            "coral.providers.weather.getForecast",
            "only the part `forecast` of `getForecast` can decide",
        ),
        (
            "weather outlook",
            "coral.providers.weather.getOutlook",
            "the part `outlook` of `getOutlook` counts as a name match even though \
             the description also holds the word, so it outranks the title of \
             `listWarnings`",
        ),
    ] {
        let page = modules
            .search
            .search(&principal, &workspace, &Corpus::query(query))
            .await
            .unwrap();
        assert_eq!(
            paths(&page).first().copied(),
            Some(expected),
            "{query}: {reason}; order {:?}",
            paths(&page)
        );
    }
}

#[tokio::test]
async fn a_stale_match_without_evidence_leaves_an_empty_page() {
    let corpus = Corpus::new().await;
    // Replace the description; nothing reindexes it, so the stored row still
    // holds `closed`.
    corpus
        .modules
        .save_function(
            &corpus.principal,
            &corpus.workspace,
            "reports.weekly",
            "A weekly summary.",
            50,
        )
        .await;

    let page = corpus.search(Corpus::query("closed")).await;
    assert!(page.hits().is_empty(), "the stale match leaves the page");
    assert!(!page.has_more());
}

#[tokio::test]
async fn a_query_without_a_searchable_term_is_empty_even_while_entities_wait() {
    // `ab` has no term of three characters; `...` and `___` hold no letter
    // or digit, so they have no phrase to order by.
    for corpus in [Corpus::unconverged().await, Corpus::new().await] {
        for query in ["ab", "...", "___"] {
            let page = corpus.search(Corpus::query(query)).await;
            for kind in [EntityKind::ProviderTool, EntityKind::SavedFunction] {
                let status = status(&page, kind);
                assert_eq!(status.state(), RetrieverState::Empty, "{query:?}: {kind:?}");
                assert_eq!(status.coverage().failed_units(), 0, "{query:?}: {kind:?}");
            }
        }
    }
}

#[tokio::test]
async fn a_query_of_the_largest_size_still_ranks_by_its_phrase_patterns() {
    let corpus = Corpus::new().await;
    // The phrase patterns grow with each word, and most with short words
    // and filler words, which the whole-field pattern may skip between any
    // two. SQLite's `regex` crate and Postgres both have a size limit.
    for filler in ["ab", "a"] {
        let mut query = "issues".to_string();
        while query.len() + 1 + filler.len() <= crate::SEARCH_QUERY_MAX_BYTES {
            query.push(' ');
            query.push_str(filler);
        }
        let page = corpus.search(Corpus::query(&query)).await;
        for kind in [EntityKind::ProviderTool, EntityKind::SavedFunction] {
            assert_eq!(
                status(&page, kind).coverage().failed_units(),
                0,
                "{filler:?}: {kind:?}"
            );
        }
        assert!(
            paths(&page).contains(&"coral.providers.github.issues.list"),
            "{filler:?}: {:?}",
            paths(&page)
        );
    }
}

/// Names that join words with no case change or punctuation, titles in
/// American spelling, and a name and title that hold `search`.
const JOINED: &str = r#"{
  "openapi": "3.1.0",
  "info": { "title": "Weather", "version": "1.0.0" },
  "servers": [{ "url": "https://weather.example" }],
  "security": [],
  "paths": {
    "/a": { "get": { "operationId": "getalertsdaily", "summary": "Tracer getalertsdaily",
      "responses": { "200": { "description": "OK" } } } },
    "/f": { "get": { "operationId": "getforecastdaily", "summary": "Tracer getforecastdaily",
      "responses": { "200": { "description": "OK" } } } },
    "/l": { "get": { "operationId": "labels/paint", "summary": "Set the color of a label",
      "responses": { "200": { "description": "OK" } } } },
    "/o": { "get": { "operationId": "orders/history", "summary": "Show canceled orders",
      "responses": { "200": { "description": "OK" } } } },
    "/s": { "get": { "operationId": "notionSearch", "summary": "Search pages",
      "responses": { "200": { "description": "OK" } } } }
  }
}"#;

/// Publishes [`JOINED`] as the `weather` Provider, converged.
async fn joined() -> (TestModules, AuthenticatedPrincipal, WorkspaceId) {
    published(JOINED, "weather").await
}

/// One User's Workspace with `document` published as `namespace`, converged.
async fn published(
    document: &str,
    namespace: &str,
) -> (TestModules, AuthenticatedPrincipal, WorkspaceId) {
    let modules = TestModules::new().await;
    let user = organization_user(modules.search.database.clone(), USER).await;
    let principal = AuthenticatedPrincipal::user(user.clone());
    let workspace = modules.create_workspace(&user).await;
    modules
        .publish_openapi(&user, &workspace, document, namespace, 20)
        .await;
    modules.converge().await;
    (modules, principal, workspace)
}

#[tokio::test]
async fn a_word_inside_a_joined_name_selects_its_row_and_ranks_on_sqlite() {
    let (modules, principal, workspace) = joined().await;
    // Both joined names hold `weather` only through the Provider name; only
    // the part `forecast` of `getforecastdaily` can put it above
    // `getalertsdaily`.
    let page = modules
        .search
        .search(&principal, &workspace, &Corpus::query("weather forecast"))
        .await
        .unwrap();
    let ranked = paths(&page);
    let position = |path: &str| {
        ranked
            .iter()
            .position(|ranked| *ranked == path)
            .unwrap_or_else(|| panic!("{path} is on the page: {ranked:?}"))
    };
    let forecast = position("coral.providers.weather.getforecastdaily");
    // On Postgres the substring selects the row, but a word that only sits
    // inside a longer word adds no score.
    if coral_test_support::selected_database().unwrap() == DatabaseSystem::Sqlite {
        assert!(
            forecast < position("coral.providers.weather.getalertsdaily"),
            "{ranked:?}"
        );
    }
}

/// The first page for `query` in the caller's Workspace.
async fn search_page(
    modules: &TestModules,
    principal: &AuthenticatedPrincipal,
    workspace: &WorkspaceId,
    query: &str,
) -> SearchPage {
    modules
        .search
        .search(principal, workspace, &Corpus::query(query))
        .await
        .unwrap()
}

#[tokio::test]
async fn postgres_ranks_a_british_spelling_as_its_american_one() {
    if coral_test_support::selected_database().unwrap() != DatabaseSystem::Postgres {
        return;
    }
    let (modules, principal, workspace) = joined().await;
    let search = |query| search_page(&modules, &principal, &workspace, query);
    // No row holds `colour` or `cancelled`; each is one edit from a word in
    // a title.
    for (british, american, expected) in [
        ("colour", "color", "coral.providers.weather.labels.paint"),
        (
            "cancelled",
            "canceled",
            "coral.providers.weather.orders.history",
        ),
    ] {
        let page = search(british).await;
        let typed = search(american).await;
        assert_eq!(paths(&page), paths(&typed), "{british} ranks as {american}");
        let first = page.hits().first().expect("a hit");
        assert_eq!(first.call_path().as_str(), expected, "{british}");
        assert!(
            first.evidence().fields().contains(&"title".to_string()),
            "{british}: the evidence names the title, which holds {american}"
        );
        assert_eq!(
            first.evidence().fields(),
            typed.hits().first().expect("a hit").evidence().fields(),
            "{british}: the evidence is that of {american}"
        );
    }
}

#[tokio::test]
async fn a_word_that_only_resembles_a_rows_text_selects_nothing() {
    let (modules, principal, workspace) = joined().await;
    let search = |query| search_page(&modules, &principal, &workspace, query);
    // `research` measures 0.56 by trigram word similarity against
    // `notionSearch` and `Search pages`, but it is two edits from `search`.
    assert_eq!(paths(&search("research").await), Vec::<&str>::new());
    assert_eq!(
        paths(&search("weather research").await),
        paths(&search("weather").await),
        "the word changes no order"
    );
}

/// One Provider of invented words whose rows separate the scoring rules.
/// Every row holds `zentro` through the Provider name, so a query of
/// two words with it never meets a phrase; the long rows hold the query
/// words out of the query's order, with no filler word between them, for
/// the same reason.
fn scoring_document() -> String {
    let parameter = |name: String, description: &str| {
        serde_json::json!({
            "name": name, "in": "query",
            "schema": { "type": "string", "description": description }
        })
    };
    // Words no query holds: they make a row long.
    let filler = (1..=30)
        .map(|index| parameter(format!("filler{index}"), "Vash teln ober kip mun dral."))
        .collect::<Vec<_>>();
    let tool =
        |id: &str, summary: &str, description: Option<&str>, parameters: Vec<serde_json::Value>| {
            let mut tool = serde_json::json!({
                "operationId": id, "summary": summary, "parameters": parameters,
                "responses": { "200": { "description": "OK" } }
            });
            if let Some(description) = description {
                tool.as_object_mut()
                    .expect("an operation object")
                    .insert("description".to_string(), serde_json::json!(description));
            }
            serde_json::json!({ "get": tool })
        };
    let schema_repeats = (1..=6)
        .map(|index| parameter(format!("note{index}"), "Frosk the glimmet near a brandle."))
        .chain(filler.iter().cloned())
        .collect();
    serde_json::json!({
        "openapi": "3.1.0",
        "info": { "title": "Zentro", "version": "1.0.0" },
        "servers": [{ "url": "https://zentro.example" }],
        "security": [],
        "paths": {
            "/forms": tool("storeQuib", "Tracer one",
                Some("Plonks, plonked, plonking, plonker and plonkish."), Vec::new()),
            "/plonk": tool("dropNib", "Plonk a nib", None, Vec::new()),
            "/snibs": tool("storeMub", "Snib, snibs, snibbed and snibbing",
                Some("Tracer ten."), Vec::new()),
            "/snib": tool("pickSnib", "Tracer eleven", Some("Snib the pick."), Vec::new()),
            "/gorvex": tool("listGorvex", "Tracer two", None, Vec::new()),
            "/tupa": tool("readTupa", "Tracer three", Some("Read the gorvex."), Vec::new()),
            "/wimble": tool("fetchZQXWimble", "Tracer four", None, Vec::new()),
            "/slib": tool("readSlib", "Tracer five", Some("Read the wimble."), Vec::new()),
            "/short-schema": tool("brandleFrosk", "Tracer six", None, Vec::new()),
            "/long-schema": tool("listYorbs", "Tracer seven", None, schema_repeats),
            "/short-body": tool("quabbleSnorf", "Tracer eight",
                Some("Quabble the snorf."), Vec::new()),
            "/long-body": tool("listDrams", "Tracer nine",
                Some("Zinder of a snorf, quabble. Snorf with zinder, quabble. Zinder, snorf and quabble."),
                filler),
        }
    })
    .to_string()
}

#[tokio::test]
async fn name_and_body_score_apart_and_postgres_splits_camel_case_names() {
    let (modules, principal, workspace) = published(&scoring_document(), "zentro").await;
    let shared = [
        (
            "brandle frosk glimmet",
            "brandleFrosk",
            "a short row whose name holds every query word but one outranks a long \
             row whose schema text holds every query word many times",
        ),
        (
            "quabble snorf zinder",
            "quabbleSnorf",
            "a short row whose name holds every query word but one outranks a long \
             row whose description holds every query word many times",
        ),
    ];
    // Postgres matches forms of a word by prefix and splits camelCase names.
    // SQLite matches substrings, so each form of a word is one more hit.
    // `postgres_vector_weights_each_field_and_splits_names_by_dots_and_camel_case`
    // shows that camelCase in a title, a description or schema text stays
    // one word.
    let postgres_only = [
        (
            "plonk zentro",
            "dropNib",
            "five forms of `plonk` in one description add one saturated term, below \
             `plonk` in a title and a description",
        ),
        (
            "snib zentro",
            "pickSnib",
            "four forms of `snib` in one title add one saturated term, below `snib` \
             in a name and a description",
        ),
        (
            "gorvex zentro",
            "listGorvex",
            "`gorvex` inside `listGorvex` is a name word, above a description",
        ),
        (
            "wimble zentro",
            "fetchZQXWimble",
            "`wimble` after the acronym run in `fetchZQXWimble` is a name word",
        ),
        (
            "listGorvex",
            "listGorvex",
            "a query for a whole identifier that only the name holds still ranks it first",
        ),
        (
            "fetchZQXWimble",
            "fetchZQXWimble",
            "a query for a whole identifier that only the name holds still ranks it first",
        ),
    ];
    let on_postgres = coral_test_support::selected_database().unwrap() == DatabaseSystem::Postgres;
    let mut misranked = Vec::new();
    for (query, expected, reason) in shared
        .into_iter()
        .chain(postgres_only.into_iter().filter(|_| on_postgres))
    {
        let page = search_page(&modules, &principal, &workspace, query).await;
        let expected = format!("coral.providers.zentro.{expected}");
        if paths(&page).first().copied() != Some(expected.as_str()) {
            misranked.push(format!("{query}: {reason}; order {:?}", paths(&page)));
        }
    }
    assert!(misranked.is_empty(), "{misranked:#?}");
}
