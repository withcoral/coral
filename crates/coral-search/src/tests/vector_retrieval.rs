//! The retrievers' vector lists on Postgres: what an agent finds by
//! meaning, and what it never finds that way.
//!
//! Every test publishes real Providers, saves and publishes real Authored
//! Tools where it needs them, indexes them and lets the Embedder give each
//! Search Entity a vector, then observes pages through
//! `UniversalSearchModule::search`. The model is a fixed-vector stand-in,
//! [`Meanings`], so no test but the one marked `ignore` loads the real one.

use std::sync::Arc;

use coral_catalog::{AuthoredToolKind, ToolKey};
use coral_db::DatabaseSystem;
use coral_embed::{Embed, EmbedError, MANIFEST};
use coral_policy::{ToolAccessState, ToolPolicyTarget};
use coral_provider::ProviderId;
use coral_provider::access::ProviderCallRoot;
use coral_tools::{
    AuthoredToolDescription, AuthoredToolId, AuthoredToolSource, PrivateTool, SharedTool,
    SharedToolOrigin, SharedToolPath, SharedToolPublish,
};
use coral_workspace::{AuthenticatedPrincipal, AuthenticatedUser, WorkspaceId};
use tokio::sync::watch;

use super::{SMALL_TOOL_SOURCE, TestModules, change, organization_user, paths};
use crate::{
    EntityKind, SearchHit, SearchLimit, SearchPage, SearchQuery, SearchRequest,
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

/// The description of a harbour's tide times: a text about tides, with no
/// word of the queries `moon` and `moon anchor`.
const HARBOUR: &str = "List high and low water for one harbour";

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
async fn the_vector_list_holds_only_the_private_and_shared_tools_the_caller_may_see() {
    coral_test_support::database_only!(Postgres);
    let harbour = Harbour::new().await;

    let page = harbour
        .world
        .request(
            &harbour.world.by_meaning(),
            &harbour.member,
            Harbour::query(),
        )
        .await;

    assert_eq!(
        of_kind(&page, EntityKind::PrivateTool),
        ["coral.private.harbour.tideTimes"],
        "the Member finds its own Private Tool, never the User's"
    );
    assert_eq!(
        of_kind(&page, EntityKind::SharedTool),
        ["coral.shared.harbour.tideTimes"],
        "no Shared Tool that policy hides"
    );
    // The hidden Tools are nearer to the query than the Member's Tools. If
    // the vector list held them, they would take its strong places. Then
    // the Member's Tools would follow every strong Tool.
    assert_eq!(
        paths(&page).get(1..3),
        Some(
            [
                "coral.private.harbour.tideTimes",
                "coral.shared.harbour.tideTimes",
            ]
            .as_slice()
        ),
        "{:?}",
        paths(&page)
    );
    let user_page = harbour
        .world
        .request(
            &harbour.world.by_meaning(),
            &harbour.world.principal,
            Harbour::query(),
        )
        .await;
    let mut nearest = of_kind(&user_page, EntityKind::PrivateTool);
    nearest.truncate(5);
    nearest.sort_unstable();
    assert_eq!(
        nearest,
        Harbour::QUAYS,
        "the User's own quay Tools come first for the User"
    );
}

#[tokio::test]
async fn a_page_of_three_kinds_puts_the_strong_first_in_one_order_for_every_limit() {
    coral_test_support::database_only!(Postgres);
    let harbour = Harbour::new().await;
    let by_meaning = harbour.world.by_meaning();
    let page = async |limit: u16, offset: u32| {
        harbour
            .world
            .request(
                &by_meaning,
                &harbour.world.principal,
                Harbour::query()
                    .with_limit(SearchLimit::parse(limit).unwrap())
                    .with_offset(offset),
            )
            .await
    };

    let all = page(50, 0).await;

    // Each kind's vector list marks its top five strong: both Tools, the
    // five quay Private Tools and the one Shared Tool the User may see. The
    // two archive Private Tools are sixth and seventh, so weak: they follow
    // every strong entry.
    assert_eq!(
        all.hits().iter().map(SearchHit::kind).collect::<Vec<_>>(),
        [
            EntityKind::ProviderTool,
            EntityKind::PrivateTool,
            EntityKind::SharedTool,
            EntityKind::ProviderTool,
            EntityKind::PrivateTool,
            EntityKind::PrivateTool,
            EntityKind::PrivateTool,
            EntityKind::PrivateTool,
            EntityKind::PrivateTool,
            EntityKind::PrivateTool,
        ],
        "{:?}",
        paths(&all)
    );
    let mut weak = paths(&all).split_off(8);
    weak.sort_unstable();
    assert_eq!(
        weak,
        [
            "coral.private.notes.archive01",
            "coral.private.notes.archive02"
        ]
    );
    assert_pages_of_one_order(&paths(&all), page).await;
}

#[tokio::test]
async fn a_private_or_shared_tool_is_found_by_its_words_at_once_and_by_meaning_after_the_embedder()
{
    coral_test_support::database_only!(Postgres);
    let world = World::with(Arc::new(Meanings::tides())).await;
    let by_meaning = world.by_meaning();
    let tides = [
        (EntityKind::PrivateTool, "coral.private.harbour.tideTimes"),
        (EntityKind::SharedTool, "coral.shared.harbour.tideTimes"),
    ];
    let archive = world
        .save(&world.principal, "notes.archive", "Archive old notes")
        .await;
    world
        .publish(
            "notes.archive",
            "Archive old notes",
            &archive,
            ToolAccessState::Enabled,
        )
        .await;
    world
        .save(&world.principal, "harbour.tideTimes", HARBOUR)
        .await;
    world
        .publish(
            "harbour.tideTimes",
            HARBOUR,
            &archive,
            ToolAccessState::Enabled,
        )
        .await;

    // An index pass with no Embedder: the rows hold the words, and no
    // vector.
    world.modules.converge().await;

    let harbour = world.search(&by_meaning, "harbour").await;
    let moon = world.search(&by_meaning, "moon").await;
    for (kind, path) in tides {
        assert_eq!(of_kind(&harbour, kind), [path], "{kind:?}: by its words");
        assert!(of_kind(&moon, kind).is_empty(), "{kind:?}: no vector yet");
    }

    world.index_and_embed().await;

    let moon = world.search(&by_meaning, "moon").await;
    let by_words = world.search(&world.by_words(), "moon").await;
    for (kind, path) in tides {
        assert_eq!(
            of_kind(&moon, kind).first(),
            Some(&path),
            "{kind:?}: by meaning"
        );
        assert!(
            of_kind(&by_words, kind).is_empty(),
            "{kind:?}: no word of the query is in a text of the kind"
        );
    }
}

#[tokio::test]
async fn an_entity_without_a_current_vector_ranks_by_its_words_alone() {
    coral_test_support::database_only!(Postgres);
    let world = World::new().await;
    let outlook = world
        .save(&world.principal, "outlook.daily", "Daily forecast summary")
        .await;
    world
        .publish(
            "outlook.daily",
            "Daily forecast summary",
            &outlook,
            ToolAccessState::Enabled,
        )
        .await;
    world.index_and_embed().await;
    let entities = [
        (
            EntityKind::ProviderTool,
            "coral.providers.weather.getForecast",
        ),
        (EntityKind::PrivateTool, "coral.private.outlook.daily"),
        (EntityKind::SharedTool, "coral.shared.outlook.daily"),
    ];
    let docs = format!(
        "(SELECT doc_id FROM search_entities WHERE call_path IN ({}))",
        entities
            .iter()
            .map(|(_kind, path)| format!("'{path}'"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let by_meaning = world.by_meaning();
    // Each case leaves the forecast Tool, the Private Tool and the Shared
    // Tool without a vector of the current revision made from the text
    // their rows hold.
    for (case, change) in [
        (
            "no vector",
            "DELETE FROM search_entity_embeddings WHERE doc_id IN {docs}",
        ),
        (
            "an older Embedder revision",
            "UPDATE search_entity_embeddings SET embedder_revision = 'older' \
             WHERE doc_id IN {docs}",
        ),
        (
            "a vector of other text",
            "UPDATE search_entity_embeddings \
             SET source_indexed_at_unix_nanos = source_indexed_at_unix_nanos - 1 \
             WHERE doc_id IN {docs}",
        ),
    ] {
        let rain = world.search(&by_meaning, "will it rain tomorrow").await;
        for (kind, path) in entities {
            assert_eq!(
                of_kind(&rain, kind).first(),
                Some(&path),
                "{case}: before the change"
            );
        }
        world.execute(&change.replace("{docs}", &docs)).await;

        let rain = world.search(&by_meaning, "will it rain tomorrow").await;
        let forecast = world.search(&by_meaning, "forecast").await;
        for (kind, path) in entities {
            assert!(
                !paths(&rain).contains(&path),
                "{case}: {path} never found by meaning"
            );
            assert_eq!(
                of_kind(&forecast, kind).first(),
                Some(&path),
                "{case}: {path} still found by words"
            );
        }

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
    let page = async |limit: u16, offset: u32| {
        world
            .request(
                &by_meaning,
                &world.principal,
                SearchRequest::query(SearchQuery::parse("rain notes").unwrap())
                    .with_limit(SearchLimit::parse(limit).unwrap())
                    .with_offset(offset),
            )
            .await
    };

    let all = page(50, 0).await;
    let all = paths(&all);
    let mut published = (1..=12)
        .map(|number| format!("coral.providers.notes.listNotes{number:02}"))
        .chain([
            "coral.providers.github.issues.list".to_owned(),
            "coral.providers.weather.getForecast".to_owned(),
        ])
        .collect::<Vec<_>>();
    published.sort();
    let mut found = all.clone();
    found.sort_unstable();
    assert_eq!(found, published, "one page holds every Tool");
    assert_pages_of_one_order(&all, page).await;
}

#[tokio::test]
async fn the_setting_off_and_each_vector_side_failure_give_the_lexical_page() {
    let world = World::new().await;
    // A Private Tool and a Shared Tool near the query by meaning alone.
    let outlook = world
        .save(&world.principal, "outlook.daily", "Daily forecast summary")
        .await;
    world
        .publish(
            "outlook.daily",
            "Daily forecast summary",
            &outlook,
            ToolAccessState::Enabled,
        )
        .await;
    world.index_and_embed().await;
    let postgres = world.modules.search.database.system() == DatabaseSystem::Postgres;
    let lexical = world.search(&world.by_words(), "rain issues").await;
    assert_eq!(paths(&lexical), ["coral.providers.github.issues.list"]);
    let fused = world.search(&world.by_meaning(), "rain issues").await;
    if postgres {
        assert_eq!(
            paths(&fused),
            [
                "coral.providers.github.issues.list",
                "coral.private.outlook.daily",
                "coral.shared.outlook.daily",
                "coral.providers.weather.getForecast",
            ]
        );
    } else {
        assert_eq!(fused, lexical, "SQLite keeps no vectors yet");
    }

    let cases: [(&str, UniversalSearchModule, Option<&str>); 4] = [
        ("the model is not ready", world.with_model(None), None),
        (
            "the model fails on the query",
            world.with_model(Some(Arc::new(FailingQueries))),
            None,
        ),
        (
            "the vector query fails",
            world.with_model(Some(Arc::new(ShortQueries))),
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

    let mut plans = vec![(
        EntityKind::ProviderTool,
        crate::retriever::tools::explain_vector_query(
            &mut transaction,
            &world.workspace,
            &providers,
            &generations,
            &excluded,
        )
        .await
        .unwrap(),
    )];
    for kind in [AuthoredToolKind::Private, AuthoredToolKind::Shared] {
        let ids = [AuthoredToolId::parse(kind, "00000000-0000-4000-8000-000000000001").unwrap()];
        plans.push((
            EntityKind::authored(kind),
            crate::retriever::authored_tools::explain_vector_query(
                &mut transaction,
                &world.workspace,
                world.principal.principal(),
                kind,
                &ids,
            )
            .await
            .unwrap(),
        ));
    }

    transaction.rollback().await.unwrap();
    for (kind, plan) in plans {
        assert!(
            plan.iter()
                .any(|line| line.contains(" on search_entity_embeddings ")),
            "{kind:?}: {plan:#?}"
        );
        assert!(
            !plan.iter().any(|line| line.contains("search_entities")),
            "{kind:?}: the vector query joins the entity table: {plan:#?}"
        );
    }
}

/// Asserts that `page`, one query's page at a limit and an offset, pages
/// `all`, the query's hits, in one order: each limit gives the first hits
/// of `all`, and offset pages of three join to give `all`.
async fn assert_pages_of_one_order(all: &[&str], page: impl AsyncFn(u16, u32) -> SearchPage) {
    let hits = u16::try_from(all.len()).unwrap();
    for limit in 1..=hits {
        assert_eq!(
            paths(&page(limit, 0).await),
            all.iter()
                .take(usize::from(limit))
                .copied()
                .collect::<Vec<_>>(),
            "limit {limit}"
        );
    }
    let mut paged = Vec::new();
    for offset in (0..u32::from(hits)).step_by(3) {
        paged.extend(paths(&page(3, offset).await).into_iter().map(str::to_owned));
    }
    assert_eq!(paged, all, "offset pages of three");
}

/// The Call Paths of the hits of `kind` on `page`, in page order.
fn of_kind(page: &SearchPage, kind: EntityKind) -> Vec<&str> {
    page.hits()
        .iter()
        .filter(|hit| hit.kind() == kind)
        .map(|hit| hit.call_path().as_str())
        .collect()
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

/// A model that gives a text one axis for each meaning whose word it holds,
/// and one shared axis to every text of no meaning. Texts of the same
/// meanings are as close as two vectors can be; a text that shares one of
/// two meanings with the query is farther; texts that share none are as far
/// apart as texts of none.
#[derive(Clone)]
struct Meanings(Arc<[(&'static str, usize)]>);

impl Meanings {
    fn weather() -> Self {
        Self(Arc::new([("forecast", 0), ("rain", 0)]))
    }

    /// [`Self::weather`], and the sea: a text about tides means what the
    /// query word `moon` means, and a text about a quay what `anchor` means.
    fn tides() -> Self {
        Self(Arc::new([
            ("forecast", 0),
            ("rain", 0),
            ("tide", 1),
            ("moon", 1),
            ("quay", 2),
            ("anchor", 2),
        ]))
    }

    fn vector(&self, text: &str) -> Vec<f32> {
        let mut vector = vec![0.0; MANIFEST.dimension];
        for (_word, slot) in self.0.iter().filter(|(word, _slot)| text.contains(word)) {
            if let Some(value) = vector.get_mut(*slot) {
                *value = 1.0;
            }
        }
        if vector.iter().all(|value| *value == 0.0)
            && let Some(value) = vector.last_mut()
        {
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

/// A [`World`] of [`Meanings::tides`] whose User and Member hold Private
/// Tools and whose User published Shared Tools, each with a vector. For the
/// query `moon anchor`, the User's five quay Tools, Private and Shared, are
/// as near as a Tool can be. The harbour Tools are nearer than every Tool
/// but those. Policy hides the five quay Shared Tools: four by the default
/// their publish chose, and one by an Owner's rule.
struct Harbour {
    world: World,
    member: AuthenticatedPrincipal,
}

impl Harbour {
    /// The User's Private Tools about a quay's tides.
    const QUAYS: [&str; 5] = [
        "coral.private.quay.tides01",
        "coral.private.quay.tides02",
        "coral.private.quay.tides03",
        "coral.private.quay.tides04",
        "coral.private.quay.tides05",
    ];

    async fn new() -> Self {
        let world = World::with(Arc::new(Meanings::tides())).await;
        let member = world.member().await;
        let mut origin = None;
        for number in 1..=5 {
            let path = format!("quay.tides{number:02}");
            let quay = world
                .save(&world.principal, &path, "Tables of the water at one quay")
                .await;
            let by_rule = number == 5;
            let shared = world
                .publish(
                    &path,
                    "Tables of the water at one quay",
                    &quay,
                    if by_rule {
                        ToolAccessState::Enabled
                    } else {
                        ToolAccessState::Disabled
                    },
                )
                .await;
            if by_rule {
                world.disable(&shared).await;
            }
            origin.get_or_insert(quay);
        }
        for number in 1..=2 {
            world
                .save(
                    &world.principal,
                    &format!("notes.archive{number:02}"),
                    "Archive old notes",
                )
                .await;
        }
        world
            .publish(
                "harbour.tideTimes",
                HARBOUR,
                &origin.expect("a quay Tool"),
                ToolAccessState::Enabled,
            )
            .await;
        world.save(&member, "harbour.tideTimes", HARBOUR).await;
        world.index_and_embed().await;
        Self { world, member }
    }

    fn query() -> SearchRequest {
        SearchRequest::query(SearchQuery::parse("moon anchor").unwrap())
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
        self.with_model(Some(Arc::clone(&self.model)))
    }

    /// Discovery with the vector side off.
    fn by_words(&self) -> UniversalSearchModule {
        UniversalSearchModule::new(
            self.modules.search.database.clone(),
            self.modules.workspaces.clone(),
        )
    }

    /// Discovery whose model slot holds `model`.
    fn with_model(&self, model: Option<Arc<dyn Embed>>) -> UniversalSearchModule {
        let (_ready, model) = watch::channel(model);
        self.by_words().with_model(model)
    }

    /// Saves a Private Tool of `principal` at `path` with `description`,
    /// whose one input is `limit`. The index learns of it at its next pass.
    async fn save(
        &self,
        principal: &AuthenticatedPrincipal,
        path: &str,
        description: &str,
    ) -> PrivateTool {
        self.modules
            .save_private_tool(principal, &self.workspace, path, description, 70)
            .await
    }

    /// Publishes the Shared Tool at `path` with `description` from the
    /// User's `origin`. Policy decides its calls by `default` until an
    /// Owner sets a rule. Its one input is `limit`. The index learns of it
    /// at its next pass.
    async fn publish(
        &self,
        path: &str,
        description: &str,
        origin: &PrivateTool,
        default: ToolAccessState,
    ) -> SharedTool {
        self.modules
            .publish_shared_tool(
                &self.user,
                &self.workspace,
                SharedToolPublish::review(
                    SharedToolPath::parse(path).unwrap(),
                    SharedToolOrigin::PrivateTool {
                        owner: self.principal.principal().clone(),
                        path: origin.path().clone(),
                    },
                    AuthoredToolDescription::parse(description).unwrap(),
                    AuthoredToolSource::parse(SMALL_TOOL_SOURCE).unwrap(),
                    default,
                    0,
                ),
            )
            .await
    }

    /// Disables `shared` by the User's rule as Workspace Owner.
    async fn disable(&self, shared: &SharedTool) {
        let mut transaction = self
            .modules
            .search
            .database
            .begin_read_snapshot()
            .await
            .unwrap();
        let provider = coral_provider::authored_tool_provider_in_transaction(
            &mut transaction,
            &self.workspace,
            AuthoredToolKind::Shared,
        )
        .await
        .unwrap()
        .expect("the Workspace's Shared Provider");
        transaction.rollback().await.unwrap();
        self.modules
            .policies
            .set_access_state(
                &change(self.user.clone(), 80, '8'),
                ToolPolicyTarget::new(
                    self.workspace.clone(),
                    provider.id().clone(),
                    shared.id().tool_key(),
                ),
                ToolAccessState::Disabled,
            )
            .await
            .expect("set the Owner's rule");
    }

    /// A Member of the Workspace.
    async fn member(&self) -> AuthenticatedPrincipal {
        self.modules
            .add_member(&self.user, &self.workspace, "user:vector-member")
            .await
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
