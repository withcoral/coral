//! The Tool retriever's vector list on Postgres: what an agent finds by
//! meaning, and what it never finds that way.
//!
//! Every test publishes real Providers, indexes them and lets the Embedder
//! give each Search Entity a vector, then observes pages through
//! `UniversalSearchModule::search`. The model is a fixed-vector stand-in,
//! [`Meanings`], so no test but the one marked `ignore` loads the real one.

use std::sync::Arc;

use coral_catalog::ToolKey;
use coral_db::DatabaseSystem;
use coral_embed::{Embed, EmbedError, MANIFEST};
use coral_policy::{ToolAccessState, ToolPolicyTarget};
use coral_provider::ProviderId;
use coral_provider::access::ProviderCallRoot;
use coral_telemetry::testing::{ExportCapture, SpanCapture, attribute};
use coral_workspace::{AuthenticatedPrincipal, AuthenticatedUser, WorkspaceId};
use opentelemetry_sdk::trace::SpanData;
use tokio::sync::watch;

use super::{TestModules, change, organization_user, paths};
use crate::{
    EntityKind, ModelLoad, SearchLimit, SearchPage, SearchQuery, SearchRequest,
    UniversalSearchModule,
};

const USER: &str = "user:search-vector-retrieval";

/// One weather Tool and one GitHub Tool.
const WEATHER: &str = r#"{
  "openapi": "3.1.0",
  "info": { "title": "Weather", "version": "1.0.0" },
  "servers": [{ "url": "https://weather.example" }],
  "security": [],
  "paths": {
    "/forecast": {
      "get": {
        "operationId": "getForecast",
        "summary": "Get one weather forecast",
        "description": "Returns the forecast for one location.",
        "responses": { "200": { "description": "OK" } }
      }
    }
  }
}"#;

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
    }
  }
}"#;

#[tokio::test]
async fn a_query_with_no_word_of_a_tool_finds_it_by_meaning() {
    coral_test_support::database_only!(Postgres);
    let world = World::new().await;

    let page = world
        .search(&world.by_meaning(), "will it rain tomorrow")
        .await;

    assert_eq!(
        paths(&page).first().copied(),
        Some("coral.providers.weather.getForecast")
    );
    assert!(
        paths(
            &world
                .search(&world.by_words(), "will it rain tomorrow")
                .await
        )
        .is_empty(),
        "no word of the query is in any Tool's text"
    );
}

#[tokio::test]
async fn an_entity_without_a_current_vector_ranks_by_its_words_alone() {
    coral_test_support::database_only!(Postgres);
    let world = World::new().await;
    let forecast = "coral.providers.weather.getForecast";
    let by_meaning = world.by_meaning();
    // Each case leaves the forecast Tool without a vector of the current
    // revision made from the text its row holds.
    for (case, change) in [
        (
            "no vector",
            "DELETE FROM search_entity_embeddings WHERE doc_id = {doc}",
        ),
        (
            "an older Embedder revision",
            "UPDATE search_entity_embeddings SET embedder_revision = 'older' WHERE doc_id = {doc}",
        ),
        (
            "a vector of other text",
            "UPDATE search_entity_embeddings \
             SET source_indexed_at_unix_nanos = source_indexed_at_unix_nanos - 1 \
             WHERE doc_id = {doc}",
        ),
    ] {
        assert_eq!(
            paths(&world.search(&by_meaning, "will it rain tomorrow").await).first(),
            Some(&forecast),
            "{case}: before the change"
        );
        world
            .execute(&change.replace(
                "{doc}",
                &format!("(SELECT doc_id FROM search_entities WHERE call_path = '{forecast}')"),
            ))
            .await;

        assert!(
            !paths(&world.search(&by_meaning, "will it rain tomorrow").await).contains(&forecast),
            "{case}: never found by meaning"
        );
        assert_eq!(
            paths(&world.search(&by_meaning, "forecast").await).first(),
            Some(&forecast),
            "{case}: still found by words"
        );

        world.index_and_embed().await;
    }
}

#[tokio::test]
async fn the_vector_list_holds_only_tools_the_caller_may_see_and_the_filter_names() {
    coral_test_support::database_only!(Postgres);
    let world = World::new().await;
    let agent = world.agent().await;
    // A Provider whose caller route the Agent has no Account for, so it is
    // not discoverable to the Agent, and a forecast Tool that policy hides.
    world
        .modules
        .publish_configured(
            &change(world.user.clone(), 80, '8'),
            &world.workspace,
            &super::credential_document(),
            "skies",
            coral_provider::access::CredentialRoute::Caller,
        )
        .await;
    world
        .modules
        .policies
        .set_access_state(
            &change(world.user.clone(), 90, '9'),
            ToolPolicyTarget::new(
                world.workspace.clone(),
                world.weather.clone(),
                ToolKey::parse("getForecast").unwrap(),
            ),
            ToolAccessState::Disabled,
        )
        .await
        .unwrap();
    world.index_and_embed().await;
    let by_meaning = world.by_meaning();
    let rain = || SearchRequest::query(SearchQuery::parse("will it rain tomorrow").unwrap());

    let page = world.request(&by_meaning, &world.principal, rain()).await;
    assert_eq!(
        paths(&page),
        [
            "coral.providers.skies.getForecast",
            "coral.providers.github.issues.list",
        ],
        "the User sees the Provider of the caller route, and never the hidden Tool"
    );
    assert_eq!(
        tools_returned(&page),
        paths(&page).len() as u64,
        "hydration dropped no Tool: the hidden one was never in the window"
    );
    assert_eq!(
        paths(&world.request(&by_meaning, &agent, rain()).await),
        ["coral.providers.github.issues.list"],
        "the Agent cannot discover the Provider of the caller route"
    );
    assert_eq!(
        paths(
            &world
                .request(
                    &by_meaning,
                    &world.principal,
                    rain()
                        .with_provider(&ProviderCallRoot::parse("coral.providers.github").unwrap()),
                )
                .await
        ),
        ["coral.providers.github.issues.list"],
        "the filter holds"
    );
}

#[tokio::test]
async fn every_limit_and_offset_pages_one_order_with_ties_in_the_vector_list() {
    coral_test_support::database_only!(Postgres);
    let world = World::new().await;
    // Twelve more Tools whose vectors are all the same, as is the GitHub
    // Tool's, so the vector list holds a tie of thirteen.
    let operations = (1..=12)
        .map(|number| {
            (
                format!("/notes/{number}"),
                serde_json::json!({ "get": {
                    "operationId": format!("listNotes{number:02}"),
                    "summary": format!("List notes {number}"),
                    "description": "Returns notes.",
                    "responses": { "200": { "description": "OK" } }
                }}),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    let notes = serde_json::json!({
        "openapi": "3.1.0",
        "info": { "title": "Notes", "version": "1.0.0" },
        "servers": [{ "url": "https://notes.example" }],
        "security": [],
        "paths": operations,
    });
    world
        .modules
        .publish_openapi(
            &world.user,
            &world.workspace,
            &notes.to_string(),
            "notes",
            40,
        )
        .await;
    world.index_and_embed().await;
    let by_meaning = world.by_meaning();
    let page = |limit: u16, offset: u32| {
        SearchRequest::query(SearchQuery::parse("rain notes").unwrap())
            .with_limit(SearchLimit::parse(limit).unwrap())
            .with_offset(offset)
    };

    let all = paths(
        &world
            .request(&by_meaning, &world.principal, page(50, 0))
            .await,
    )
    .into_iter()
    .map(str::to_owned)
    .collect::<Vec<_>>();
    let mut published = (1..=12)
        .map(|number| format!("coral.providers.notes.listNotes{number:02}"))
        .chain([
            "coral.providers.github.issues.list".to_owned(),
            "coral.providers.weather.getForecast".to_owned(),
        ])
        .collect::<Vec<_>>();
    published.sort();
    let mut found = all.clone();
    found.sort();
    assert_eq!(found, published, "one page holds every Tool");
    let tools = u16::try_from(all.len()).unwrap();
    for limit in 1..=tools {
        assert_eq!(
            paths(
                &world
                    .request(&by_meaning, &world.principal, page(limit, 0))
                    .await
            ),
            all.iter()
                .take(usize::from(limit))
                .map(String::as_str)
                .collect::<Vec<_>>(),
            "limit {limit}"
        );
    }
    let mut paged = Vec::new();
    for offset in (0..u32::from(tools)).step_by(3) {
        paged.extend(
            paths(
                &world
                    .request(&by_meaning, &world.principal, page(3, offset))
                    .await,
            )
            .into_iter()
            .map(str::to_owned),
        );
    }
    assert_eq!(paged, all, "offset pages of three");
}

#[tokio::test]
async fn the_setting_off_and_each_vector_side_failure_give_the_lexical_page() {
    let world = World::new().await;
    let postgres = world.modules.search.database.system() == DatabaseSystem::Postgres;
    let lexical = world.search(&world.by_words(), "rain issues").await;
    assert_eq!(paths(&lexical), ["coral.providers.github.issues.list"]);
    let fused = world.search(&world.by_meaning(), "rain issues").await;
    if postgres {
        assert_eq!(
            paths(&fused),
            [
                "coral.providers.github.issues.list",
                "coral.providers.weather.getForecast",
            ]
        );
    } else {
        assert_eq!(fused, lexical, "SQLite keeps no vectors yet");
    }

    let cases: [(&str, UniversalSearchModule, Option<&str>); 4] = [
        (
            "the model is not ready",
            world.with_model(ModelLoad::Pending),
            None,
        ),
        (
            "the model fails on the query",
            world.with_model(ModelLoad::Ready(Arc::new(FailingQueries))),
            None,
        ),
        (
            "the vector query fails",
            world.with_model(ModelLoad::Ready(Arc::new(ShortQueries))),
            None,
        ),
        (
            "the vector table is missing",
            world.by_meaning(),
            postgres.then_some("DROP TABLE search_entity_embeddings"),
        ),
    ];
    for (case, search, change) in cases {
        if let Some(change) = change {
            world.execute(change).await;
        }
        assert_eq!(
            world.search(&search, "rain issues").await,
            lexical,
            "{case}"
        );
    }
}

#[tokio::test]
async fn a_search_while_the_model_is_absent_exports_the_tool_retriever_as_lexical_only() {
    coral_test_support::database_only!(Postgres);
    let world = World::new().await;
    let exported = ExportCapture::default();
    let spans = SpanCapture::exporting_to(&exported);
    let _traced = tracing::subscriber::set_default(spans.subscriber());

    world
        .search(&world.with_model(ModelLoad::Pending), "rain issues")
        .await;

    let rank = exported_tool_rank(&exported);
    assert_eq!(
        attribute(&rank, "coral.search.semantic").as_deref(),
        Some("lexical_only")
    );
    assert_eq!(
        attribute(&rank, "coral.search.lexical_only.reason").as_deref(),
        Some("model_absent")
    );
    assert_eq!(
        coral_keys(&rank),
        [
            "coral.search.lexical_only.reason",
            "coral.search.retriever",
            "coral.search.semantic",
        ],
        "the export filter passes the new fields and nothing else new"
    );
}

#[tokio::test]
async fn a_fused_search_exports_the_query_embedding_and_each_ranking_under_the_search() {
    coral_test_support::database_only!(Postgres);
    let world = World::new().await;
    let exported = ExportCapture::default();
    let spans = SpanCapture::exporting_to(&exported);
    let _traced = tracing::subscriber::set_default(spans.subscriber());

    world.search(&world.by_meaning(), "rain issues").await;

    let spans = exported.spans();
    let named = |name: &str| {
        spans
            .iter()
            .filter(|span| span.name == name)
            .collect::<Vec<_>>()
    };
    let [search] = named("coral.search")[..] else {
        panic!("one search span: {spans:#?}");
    };
    let [embed] = named("coral.search.embed_query")[..] else {
        panic!("one query embedding span: {spans:#?}");
    };
    assert_eq!(embed.parent_span_id, search.span_context.span_id());
    assert!(
        coral_keys(embed).is_empty() && coral_keys(search).is_empty(),
        "neither span carries a field: {embed:#?} {search:#?}"
    );
    let ranks = named("coral.search.rank");
    assert_eq!(ranks.len(), 3, "one span for each retriever");
    assert!(
        ranks
            .iter()
            .all(|rank| rank.parent_span_id == search.span_context.span_id())
    );
}

#[tokio::test]
async fn each_reason_the_tool_retriever_ranks_by_words_alone_is_on_its_span() {
    coral_test_support::database_only!(Postgres);
    let world = World::new().await;
    let exported = ExportCapture::default();
    let spans = SpanCapture::exporting_to(&exported);
    let _traced = tracing::subscriber::set_default(spans.subscriber());

    let cases = [
        ("fused", world.by_meaning(), None, "fused", None),
        (
            "the setting off",
            world.by_words(),
            None,
            "lexical_only",
            Some("disabled"),
        ),
        (
            "the model failed to load",
            world.with_model(ModelLoad::Failed),
            None,
            "lexical_only",
            Some("model_failed"),
        ),
        (
            "a CPU below the floor",
            world.with_model(ModelLoad::CpuTooOld),
            None,
            "lexical_only",
            Some("cpu_floor"),
        ),
        (
            "the model fails on the query",
            world.with_model(ModelLoad::Ready(Arc::new(FailingQueries))),
            None,
            "lexical_only",
            Some("embed_failed"),
        ),
        (
            "the vector query fails",
            world.with_model(ModelLoad::Ready(Arc::new(ShortQueries))),
            None,
            "lexical_only",
            Some("vector_query_failed"),
        ),
        // During a rolling update, an older Coral may have dropped the table.
        (
            "the vector table is missing",
            world.by_meaning(),
            Some("DROP TABLE search_entity_embeddings"),
            "lexical_only",
            Some("vector_query_failed"),
        ),
    ];
    for (case, search, change, semantic, reason) in cases {
        if let Some(change) = change {
            world.execute(change).await;
        }

        world.search(&search, "rain issues").await;

        let rank = exported_tool_rank(&exported);
        assert_eq!(
            (
                attribute(&rank, "coral.search.semantic").as_deref(),
                attribute(&rank, "coral.search.lexical_only.reason").as_deref(),
            ),
            (Some(semantic), reason),
            "{case}"
        );
    }
}

#[tokio::test]
async fn the_vector_query_reads_the_vector_table_alone() {
    coral_test_support::database_only!(Postgres);
    let world = World::new().await;
    let mut transaction = world
        .modules
        .search
        .database
        .begin_read_snapshot()
        .await
        .unwrap();
    let scope: Vec<(String, String)> = transaction
        .fetch_all_sql(
            "SELECT DISTINCT provider_id, provider_generation_id FROM search_entities \
             WHERE workspace_id = $1 AND kind = 'provider_tool'",
            sea_query::Values(vec![world.workspace.to_string().into()]),
        )
        .await
        .unwrap();
    let providers = scope
        .iter()
        .map(|(provider, _)| ProviderId::parse(provider).unwrap())
        .collect::<Vec<_>>();
    let generations = scope
        .iter()
        .map(|(_, generation)| coral_catalog::ProviderGenerationId::parse(generation).unwrap())
        .collect::<Vec<_>>();
    let excluded = [(
        world.weather.clone(),
        ToolKey::parse("getForecast").unwrap(),
    )];

    let plan = crate::retriever::tools::explain_vector_query(
        &mut transaction,
        &world.workspace,
        &providers,
        &generations,
        &excluded,
    )
    .await
    .unwrap();

    transaction.rollback().await.unwrap();
    assert!(
        plan.iter()
            .any(|line| line.contains(" on search_entity_embeddings ")),
        "{plan:#?}"
    );
    assert!(
        !plan.iter().any(|line| line.contains("search_entities")),
        "the vector query joins the entity table: {plan:#?}"
    );
}

/// The Tool retriever's span of the last search, as it left the host.
fn exported_tool_rank(exported: &ExportCapture) -> SpanData {
    exported
        .spans()
        .into_iter()
        .rev()
        .find(|span| {
            span.name == "coral.search.rank"
                && attribute(span, "coral.search.retriever").as_deref() == Some("provider_tool")
        })
        .expect("the Tool retriever's span left the host")
}

/// The keys of an exported span, in order, less the `code.*` and `thread.*`
/// keys the tracing bridge adds to every span.
fn coral_keys(span: &SpanData) -> Vec<&str> {
    let mut keys = span
        .attributes
        .iter()
        .map(|attribute| attribute.key.as_str())
        .filter(|key| !key.starts_with("code.") && !key.starts_with("thread."))
        .collect::<Vec<_>>();
    keys.sort_unstable();
    keys
}

/// How many Tools the Tool retriever handed the engine for `page`.
fn tools_returned(page: &SearchPage) -> u64 {
    page.retriever_statuses()
        .iter()
        .find(|status| status.retriever() == EntityKind::ProviderTool)
        .expect("the Tool retriever's status")
        .coverage()
        .returned_count()
}

/// The whole path once with the real model: index, embed, search. Run it
/// with the model's files and a Postgres:
///
/// ```text
/// CORAL_EMBED_MODEL_DIR=<folder with model.onnx and tokenizer.json> \
/// CORAL_TEST_DATABASE=postgres CORAL_TEST_POSTGRES_URL=<url> \
///   cargo nextest run -p coral-search --run-ignored only -E 'test(the_real_model)'
/// ```
#[tokio::test]
#[ignore = "loads the model; set CORAL_EMBED_MODEL_DIR"]
#[expect(
    clippy::disallowed_methods,
    reason = "the test reads where the developer put the model's files"
)]
async fn the_real_model_finds_a_tool_by_meaning() {
    coral_test_support::database_only!(Postgres);
    let dir = std::env::var_os("CORAL_EMBED_MODEL_DIR")
        .expect("set CORAL_EMBED_MODEL_DIR to a folder with model.onnx and tokenizer.json");
    let model = coral_embed::Model::load(std::path::Path::new(&dir)).expect("the model loads");
    let world = World::with(Arc::new(model)).await;

    let page = world
        .search(&world.by_meaning(), "will it rain tomorrow")
        .await;

    assert_eq!(
        paths(&page).first().copied(),
        Some("coral.providers.weather.getForecast"),
        "{:?}",
        paths(&page)
    );
    assert!(
        paths(
            &world
                .search(&world.by_words(), "will it rain tomorrow")
                .await
        )
        .is_empty(),
        "no word of the query is in any Tool's text"
    );
}

/// A model that gives a text the vector of the first meaning whose word it
/// holds, and one shared vector to every other text. Texts of one meaning
/// are as close as two vectors can be; texts of two meanings are as far
/// apart as texts of none.
#[derive(Clone)]
struct Meanings(Arc<[(&'static str, usize)]>);

impl Meanings {
    fn weather() -> Self {
        Self(Arc::new([("forecast", 0), ("rain", 0)]))
    }

    fn vector(&self, text: &str) -> Vec<f32> {
        let slot = self
            .0
            .iter()
            .find(|(word, _slot)| text.contains(word))
            .map_or(MANIFEST.dimension - 1, |(_word, slot)| *slot);
        let mut vector = vec![0.0; MANIFEST.dimension];
        if let Some(value) = vector.get_mut(slot) {
            *value = 1.0;
        }
        vector
    }
}

impl Embed for Meanings {
    fn embed_query(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
        Ok(self.vector(text))
    }

    fn embed_document(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
        Ok(self.vector(text))
    }
}

/// A model that fails on every query.
struct FailingQueries;

impl Embed for FailingQueries {
    fn embed_query(&self, _text: &str) -> Result<Vec<f32>, EmbedError> {
        Err(EmbedError::Output("injected query fault".to_owned()))
    }

    fn embed_document(&self, _text: &str) -> Result<Vec<f32>, EmbedError> {
        Err(EmbedError::Output("injected document fault".to_owned()))
    }
}

/// A model whose query vectors are shorter than the stored ones, so the
/// vector query fails in the database.
struct ShortQueries;

impl Embed for ShortQueries {
    fn embed_query(&self, _text: &str) -> Result<Vec<f32>, EmbedError> {
        Ok(vec![1.0, 0.0, 0.0])
    }

    fn embed_document(&self, _text: &str) -> Result<Vec<f32>, EmbedError> {
        Ok(vec![1.0, 0.0, 0.0])
    }
}

/// A Workspace with the Weather and GitHub Providers, indexed, each Search
/// Entity with a vector of its model, [`Meanings::weather`] unless a test
/// names another.
struct World {
    modules: TestModules,
    user: AuthenticatedUser,
    principal: AuthenticatedPrincipal,
    workspace: WorkspaceId,
    weather: ProviderId,
    model: Arc<dyn Embed>,
}

impl World {
    async fn new() -> Self {
        Self::with(Arc::new(Meanings::weather())).await
    }

    async fn with(model: Arc<dyn Embed>) -> Self {
        let modules = TestModules::new().await;
        let user = organization_user(modules.search.database.clone(), USER).await;
        let principal = AuthenticatedPrincipal::user(user.clone());
        let workspace = modules.create_workspace(&user).await;
        let weather = modules
            .publish_openapi(&user, &workspace, WEATHER, "weather", 20)
            .await;
        modules
            .publish_openapi(&user, &workspace, GITHUB, "github", 30)
            .await;
        modules.prepare_vectors().await;
        let world = Self {
            modules,
            user,
            principal,
            workspace,
            weather,
            model,
        };
        world.index_and_embed().await;
        world
    }

    /// Indexes the Workspace and waits until each Search Entity has a
    /// vector. The Embedder stops then, so a test's change to the vector
    /// table stays.
    async fn index_and_embed(&self) {
        let index = self.modules.index_with(Arc::clone(&self.model));
        let background = index.spawn_reconcile_task();
        index.reconcile().await;
        index.settled().await;
        index.embedded().await;
        background.abort();
    }

    /// Discovery whose engine embeds each query with the World's model.
    fn by_meaning(&self) -> UniversalSearchModule {
        self.with_model(ModelLoad::Ready(Arc::clone(&self.model)))
    }

    /// Discovery with the vector side off.
    fn by_words(&self) -> UniversalSearchModule {
        UniversalSearchModule::new(
            self.modules.search.database.clone(),
            self.modules.workspaces.clone(),
        )
    }

    /// Discovery whose model slot holds `model`.
    fn with_model(&self, model: ModelLoad) -> UniversalSearchModule {
        let (_ready, model) = watch::channel(model);
        self.by_words().with_model(model)
    }

    async fn execute(&self, sql: &str) {
        self.modules
            .fixture
            .execute_fixture_sql(sql)
            .await
            .expect("change the index");
    }

    /// An Agent of the Workspace.
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

    async fn search(&self, search: &UniversalSearchModule, query: &str) -> SearchPage {
        self.request(
            search,
            &self.principal,
            SearchRequest::query(SearchQuery::parse(query).unwrap()),
        )
        .await
    }

    async fn request(
        &self,
        search: &UniversalSearchModule,
        principal: &AuthenticatedPrincipal,
        request: SearchRequest,
    ) -> SearchPage {
        search
            .search(principal, &self.workspace, &request)
            .await
            .expect("search")
    }
}
