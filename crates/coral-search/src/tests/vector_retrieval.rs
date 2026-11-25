//! The retrievers' vector lists on both databases: what an agent finds by
//! semantic search, and what it never finds that way.
//!
//! Every test publishes real Providers, saves and publishes real Authored
//! Tools where it needs them, indexes them and lets the Embedder give each
//! Search Entity a vector, then observes pages through
//! `UniversalSearchModule::search`; two tests read the vector queries'
//! plans instead, one per database. The model is a fixed-vector stand-in,
//! [`Meanings`], so no test but the one marked `ignore` loads the real one.

use std::sync::Arc;

use coral_catalog::{AuthoredToolKind, ToolKey};
use coral_db::{DatabaseSystem, Transaction};
use coral_embed::{Embed, EmbedError, MANIFEST};
use coral_policy::{ToolAccessState, ToolPolicyTarget};
use coral_provider::ProviderId;
use coral_provider::access::ProviderCallRoot;
use coral_telemetry::testing::{ExportCapture, SpanCapture, attribute};
use coral_tools::{
    AuthoredToolDescription, AuthoredToolId, AuthoredToolSource, PrivateTool, SharedTool,
    SharedToolOrigin, SharedToolPath, SharedToolPublish,
};
use coral_workspace::{AuthenticatedPrincipal, AuthenticatedUser, RequestId, WorkspaceId};
use opentelemetry_sdk::trace::SpanData;
use tokio::sync::watch;

use super::{
    SMALL_TOOL_SOURCE, TestModules, change, eventually, exported_keys, organization_user, paths,
};
use crate::retriever::authored_tools::AuthoredToolRetriever;
use crate::retriever::tools::ToolRetriever;
use crate::retriever::{
    Answer, EntityRef, EntityRetriever, Hydrated, Listing, QueryError, QueryRequest, RequestContext,
};
use crate::{
    CallPath, EntityKind, Error, ExactAuthoringDescription, ModelLoad, ProviderFilter, SearchHit,
    SearchLimit, SearchPage, SearchQuery, SearchRequest, UniversalSearchModule,
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

/// The description of a quay's tide tables: a text about a quay, with no
/// word of the query `moon anchor`. At a path of tides, it means what the
/// whole query means.
const QUAY: &str = "Tables of the water at one quay";

/// One Tool about tides, whose text holds no word of the query `moon anchor`.
const TIDES: &str = r#"{
  "openapi": "3.1.0",
  "info": { "title": "Tides", "version": "1.0.0" },
  "servers": [{ "url": "https://tides.example" }],
  "security": [],
  "paths": {
    "/tables": {
      "get": {
        "operationId": "getTides",
        "summary": "Get high and low water",
        "description": "Returns the water levels of one day.",
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
async fn a_query_with_no_word_of_a_tool_finds_it_by_semantic_search() {
    let world = World::new().await;

    let page = world
        .search(&world.semantic(), "will it rain tomorrow")
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
    let harbour = Harbour::new().await;

    let page = harbour
        .world
        .request(&harbour.world.semantic(), &harbour.member, Harbour::query())
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
            &harbour.world.semantic(),
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
async fn a_page_of_three_kinds_puts_strong_weak_and_last_in_one_order_for_every_limit() {
    let harbour = Harbour::new().await;
    // Twelve more Tools of no meaning, so the Tool vector list holds weak
    // Tools.
    harbour.world.publish_notes().await;
    harbour.world.index_and_embed().await;
    let semantic = harbour.world.semantic();
    let page = async |limit: u16, offset: u32| {
        harbour
            .world
            .request(
                &semantic,
                &harbour.world.principal,
                Harbour::query()
                    .with_limit(SearchLimit::parse(limit).unwrap())
                    .with_offset(offset),
            )
            .await
    };

    let all = page(50, 0).await;

    // Every Tool means nothing of the query, so all fourteen are as near
    // to it: the Tool vector list marks five strong and nine weak. The five
    // quay Private Tools and the harbour Shared Tool are nearer, so
    // strong. The two archive Private Tools are as near as the Tools, so
    // last: they follow every weak Tool.
    assert_eq!(
        kinds(&all),
        [
            [
                EntityKind::ProviderTool,
                EntityKind::PrivateTool,
                EntityKind::SharedTool,
            ]
            .as_slice(),
            &[EntityKind::ProviderTool, EntityKind::PrivateTool].repeat(4),
            &[EntityKind::ProviderTool; 9],
            &[EntityKind::PrivateTool; 2],
        ]
        .concat(),
        "{:?}",
        paths(&all)
    );
    let mut last = paths(&all).split_off(20);
    last.sort_unstable();
    assert_eq!(
        last,
        [
            "coral.private.notes.archive01",
            "coral.private.notes.archive02"
        ]
    );
    assert_pages_of_one_order(&paths(&all), page).await;
}

#[tokio::test]
async fn a_private_or_shared_tool_is_found_by_words_at_once_and_by_semantic_search_after_embedding()
{
    let world = World::with(Arc::new(Meanings::tides())).await;
    let semantic = world.semantic();
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

    let harbour = world.search(&semantic, "harbour").await;
    let moon = world.search(&semantic, "moon").await;
    for (kind, path) in tides {
        assert_eq!(of_kind(&harbour, kind), [path], "{kind:?}: by its words");
        assert!(of_kind(&moon, kind).is_empty(), "{kind:?}: no vector yet");
    }

    world.index_and_embed().await;

    let moon = world.search(&semantic, "moon").await;
    let by_words = world.search(&world.by_words(), "moon").await;
    for (kind, path) in tides {
        assert_eq!(
            of_kind(&moon, kind).first(),
            Some(&path),
            "{kind:?}: by semantic search"
        );
        assert!(
            of_kind(&by_words, kind).is_empty(),
            "{kind:?}: no word of the query is in a text of the kind"
        );
    }
}

#[tokio::test]
async fn an_entity_without_a_current_vector_ranks_by_its_words_alone() {
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
    let semantic = world.semantic();
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
        let rain = world.search(&semantic, "will it rain tomorrow").await;
        for (kind, path) in entities {
            assert_eq!(
                of_kind(&rain, kind).first(),
                Some(&path),
                "{case}: before the change"
            );
        }
        world.execute(&change.replace("{docs}", &docs)).await;

        let rain = world.search(&semantic, "will it rain tomorrow").await;
        let forecast = world.search(&semantic, "forecast").await;
        for (kind, path) in entities {
            assert!(
                !paths(&rain).contains(&path),
                "{case}: {path} never found by semantic search"
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
    let semantic = world.semantic();
    let rain = || SearchRequest::query(SearchQuery::parse("will it rain tomorrow").unwrap());

    let page = world.request(&semantic, &world.principal, rain()).await;
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
        paths(&world.request(&semantic, &agent, rain()).await),
        ["coral.providers.github.issues.list"],
        "the Agent cannot discover the Provider of the caller route"
    );
    assert_eq!(
        paths(
            &world
                .request(
                    &semantic,
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
async fn authored_tools_farther_than_the_nearest_provider_tool_follow_every_weak_provider_tool() {
    let world = World::new().await;
    // Fourteen Provider Tools: the forecast Tool, nearest to `rain`, and
    // thirteen of no meaning, so their vector list marks five strong and
    // nine weak.
    world.publish_notes().await;
    // Six Private Tools and one Shared Tool of no meaning and no word of
    // the query: each is farther from it than the forecast Tool.
    let mut archives = Vec::new();
    for number in 1..=6 {
        archives.push(
            world
                .save(
                    &world.principal,
                    &format!("notes.archive{number:02}"),
                    "Archive old notes",
                )
                .await,
        );
    }
    world
        .publish(
            "notes.archive",
            "Archive old notes",
            archives.first().expect("an archive Tool"),
            ToolAccessState::Enabled,
        )
        .await;
    world.index_and_embed().await;
    let semantic = world.semantic();
    let rain = |limit: u16| {
        SearchRequest::query(SearchQuery::parse("rain").unwrap())
            .with_limit(SearchLimit::parse(limit).unwrap())
    };

    let first = world.request(&semantic, &world.principal, rain(10)).await;
    let all = world.request(&semantic, &world.principal, rain(50)).await;

    assert_eq!(
        kinds(&first),
        [EntityKind::ProviderTool; 10],
        "Provider Tools fill the first page: {:?}",
        paths(&first)
    );
    assert_eq!(
        after_every_provider_tool(&all),
        [
            "coral.private.notes.archive01",
            "coral.private.notes.archive02",
            "coral.private.notes.archive03",
            "coral.private.notes.archive04",
            "coral.private.notes.archive05",
            "coral.private.notes.archive06",
            "coral.shared.notes.archive",
        ],
        "with room, the Authored Tools follow every weak Provider Tool: {:?}",
        paths(&all)
    );
}

#[tokio::test]
async fn an_authored_tool_is_strong_only_when_strictly_nearer_than_the_nearest_provider_tool_in_scope()
 {
    let world = World::anchorage().await;
    // No word of `moon anchor` is in these Authored Tools' texts. The quay
    // Tools are as near to it as a Tool can be. The harbour Tools mean what
    // the Tides Tool means, so they are exactly as near as it, and nearer
    // than every Notes Tool.
    world.save_and_publish("quay.tides01", QUAY).await;
    world.save_and_publish("harbour.tideTimes", HARBOUR).await;
    world.index_and_embed().await;
    let semantic = world.semantic();
    // Each case: the Provider filter, and the Authored Tools that follow
    // every Provider Tool. An Authored Tool is strong or last, and each case
    // has weak Provider Tools, so one that follows none of them is strong.
    let cases = [
        (
            "no filter: an equal distance is not strong, so the harbour Tools \
             follow every weak Provider Tool",
            None,
            vec![
                "coral.private.harbour.tideTimes",
                "coral.shared.harbour.tideTimes",
            ],
        ),
        (
            "the Notes filter: the harbour Tools are nearer than every Provider Tool in it",
            Some("notes"),
            vec![],
        ),
    ];
    for (case, filter, last) in cases {
        let page = world.anchor(&semantic, filter).await;

        assert_eq!(
            after_every_provider_tool(&page),
            last,
            "{case}: {:?}",
            paths(&page)
        );
        assert_eq!(
            paths(&page)
                .into_iter()
                .filter(|path| !path.starts_with("coral.providers."))
                .collect::<Vec<_>>(),
            [
                "coral.private.quay.tides01",
                "coral.shared.quay.tides01",
                "coral.private.harbour.tideTimes",
                "coral.shared.harbour.tideTimes",
            ],
            "{case}"
        );
    }
}

#[tokio::test]
async fn a_rare_path_word_makes_an_authored_tool_strong_within_the_tenth_nearest_provider_tool() {
    let world = World::with(Arc::new(Meanings::tides())).await;
    // For `moon anchor`, three distances in rank order: eight Tides Tools
    // and the Coast tide Tool mean what a text about a tide, a forecast and
    // a quay means, the nearest nine; the Tides level Tool means only what
    // a text about tides means, the tenth; the Tides outlook Tool means a
    // tide and a forecast, the eleventh. The other Coast Tools and the
    // forecast and GitHub Tools mean nothing of the query.
    let tides = (1..=8)
        .map(|number| {
            (
                format!("quay{number}"),
                format!("The tide forecast at the quay {number}"),
            )
        })
        .chain([
            ("level".to_owned(), "Water level".to_owned()),
            ("outlook".to_owned(), "The forecast of the water".to_owned()),
        ])
        .collect::<Vec<_>>();
    world.publish_operations("tides", &tides, 50).await;
    // Eight Coast Tools, fewer than ten, so that a filter on Coast leaves
    // fewer than ten Provider distances and weak Provider Tools.
    let coast = std::iter::once((
        "tideTables".to_owned(),
        "The tide forecast at the quay".to_owned(),
    ))
    .chain((1..=7).map(|number| (format!("listBuoys{number}"), format!("List buoys {number}"))))
    .collect::<Vec<_>>();
    world.publish_operations("coast", &coast, 51).await;
    // `anchor` starts a word of both paths and of no Provider Tool path, so
    // it is rare. The depths Tool is exactly as near as the tenth nearest
    // Provider Tool, and the forecast Tool, which also means what
    // `forecast` means, as the eleventh.
    world
        .save(&world.principal, "anchor.depths", "Depth below the keel")
        .await;
    world
        .save(&world.principal, "anchor.forecast", "Wind for one berth")
        .await;
    world.index_and_embed().await;
    let semantic = world.semantic();
    let forecast_vector = "DELETE FROM search_entity_embeddings WHERE doc_id IN \
        (SELECT doc_id FROM search_entities WHERE call_path = 'coral.private.anchor.forecast')";
    // Each case: the Provider filter, a change to the index before it, and
    // the Authored Tools that follow every Provider Tool.
    let cases = [
        (
            "no filter: the depths Tool is as near as the tenth nearest Provider Tool, \
             the forecast Tool as the eleventh",
            None,
            None,
            vec!["coral.private.anchor.forecast"],
        ),
        (
            "the Coast filter: with fewer than ten Provider distances, the farthest counts",
            Some("coast"),
            None,
            vec![],
        ),
        (
            "the forecast Tool has no distance of its own: the rare word decides alone",
            None,
            Some(forecast_vector),
            vec![],
        ),
    ];
    for (case, filter, change, last) in cases {
        if let Some(change) = change {
            world.execute(change).await;
        }

        let page = world.anchor(&semantic, filter).await;

        assert_eq!(
            of_kind(&page, EntityKind::PrivateTool).len(),
            2,
            "{case}: {:?}",
            paths(&page)
        );
        assert_eq!(
            after_every_provider_tool(&page),
            last,
            "{case}: {:?}",
            paths(&page)
        );
    }
}

#[tokio::test]
async fn the_cut_offs_come_from_the_vector_list_when_the_window_loses_the_nearest_provider_tool() {
    let world = World::anchorage().await;
    // No word of `moon anchor`. The Tool means what a text about tides and a
    // forecast means: farther from the query than the Tides Tool, the nearest
    // Provider Tool, and nearer than every other Provider Tool.
    world
        .save(
            &world.principal,
            "harbour.tideForecast",
            "The forecast for one berth",
        )
        .await;
    world.index_and_embed().await;
    let last = ["coral.private.harbour.tideForecast".to_owned()];

    let whole = world.anchor(&world.semantic(), None).await;
    let cut = world.anchor(&world.losing_nearest_tool(), None).await;

    assert_eq!(
        after_every_provider_tool(&whole),
        last,
        "not nearer than the nearest Provider Tool: {:?}",
        paths(&whole)
    );
    assert!(
        !paths(&cut).contains(&"coral.providers.tides.getTides"),
        "the window lost the nearest Provider Tool: {:?}",
        paths(&cut)
    );
    assert_eq!(
        after_every_provider_tool(&cut),
        last,
        "the vector list still holds the nearest Provider Tool: {:?}",
        paths(&cut)
    );
}

#[tokio::test]
async fn without_a_provider_distance_no_authored_tool_is_strong_by_distance() {
    let world = World::anchorage().await;
    // For `moon anchor water`: an Authored Tool whose text holds the query,
    // one whose path starts a word with `anchor`, which no Provider Tool path
    // does, and the quay Tool, which shares only `water` with it and is as
    // near to it as a Tool can be. The Tides Tool shares `water`, so every
    // page holds it.
    world
        .save_and_publish("watch.log", "Log of moon anchor water")
        .await;
    world
        .save_and_publish("anchor.depths", "Depth below the keel")
        .await;
    world.save_and_publish("quay.tides01", QUAY).await;
    world.index_and_embed().await;
    let provider_tool_docs = "(SELECT doc_id FROM search_entities WHERE kind = 'provider_tool')";
    let quay = [
        "coral.private.quay.tides01".to_owned(),
        "coral.shared.quay.tides01".to_owned(),
    ];
    // Each case: the search, a change to the index before it, and the
    // Authored Tools that follow every Provider Tool. A phrase keeps its
    // Authored Tools strong in every case. So does the rare path word: with a
    // Provider distance the anchor Tool is near enough, and without one the
    // rare word decides alone.
    let cases: [(&str, UniversalSearchModule, Option<String>, &[String]); 6] = [
        ("a Provider Tool distance", world.semantic(), None, &[]),
        ("the setting off", world.by_words(), None, &quay),
        (
            "the model not ready",
            world.with_model(ModelLoad::Pending),
            None,
            &quay,
        ),
        (
            "the vector queries fail",
            world.with_model(ModelLoad::Ready(Arc::new(ShortQueries))),
            None,
            &quay,
        ),
        (
            "no Provider Tool vector",
            world.semantic(),
            Some(format!(
                "DELETE FROM search_entity_embeddings WHERE doc_id IN {provider_tool_docs}"
            )),
            &quay,
        ),
        (
            "Provider Tool vectors of other text",
            world.semantic(),
            Some(format!(
                "UPDATE search_entity_embeddings \
                 SET source_indexed_at_unix_nanos = source_indexed_at_unix_nanos - 1 \
                 WHERE doc_id IN {provider_tool_docs}"
            )),
            &quay,
        ),
    ];
    for (case, search, change, last) in cases {
        if let Some(change) = change {
            world.execute(&change).await;
        }

        let page = world
            .request(
                &search,
                &world.principal,
                SearchRequest::query(SearchQuery::parse("moon anchor water").unwrap())
                    .with_limit(SearchLimit::parse(50).unwrap()),
            )
            .await;

        let mut authored = [EntityKind::PrivateTool, EntityKind::SharedTool]
            .into_iter()
            .flat_map(|kind| of_kind(&page, kind))
            .collect::<Vec<_>>();
        authored.sort_unstable();
        assert_eq!(
            authored,
            [
                "coral.private.anchor.depths",
                "coral.private.quay.tides01",
                "coral.private.watch.log",
                "coral.shared.anchor.depths",
                "coral.shared.quay.tides01",
                "coral.shared.watch.log",
            ],
            "{case}: {:?}",
            paths(&page)
        );
        assert_eq!(
            after_every_provider_tool(&page),
            last,
            "{case}: {:?}",
            paths(&page)
        );
        world.index_and_embed().await;
    }
}

#[tokio::test]
async fn every_limit_and_offset_pages_one_order_with_ties_in_the_vector_list() {
    let world = World::new().await;
    // Twelve more Tools whose vectors are all the same, as is the GitHub
    // Tool's, so the vector list holds a tie of thirteen.
    world.publish_notes().await;
    world.index_and_embed().await;
    let semantic = world.semantic();
    let page = async |limit: u16, offset: u32| {
        world
            .request(
                &semantic,
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
    // A Private Tool and a Shared Tool that only semantic search finds for the query.
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
    let lexical = world.search(&world.by_words(), "rain issues").await;
    assert_eq!(paths(&lexical), ["coral.providers.github.issues.list"]);
    let fused = world.search(&world.semantic(), "rain issues").await;
    // The outlook Tools are as near to the query as the forecast Tool, so
    // not strong: they follow every Tool.
    assert_eq!(
        paths(&fused),
        [
            "coral.providers.github.issues.list",
            "coral.providers.weather.getForecast",
            "coral.private.outlook.daily",
            "coral.shared.outlook.daily",
        ]
    );
    let drop_vector_table = match world.modules.search.database.system() {
        DatabaseSystem::Postgres => "DROP TABLE search_entity_embeddings",
        DatabaseSystem::Sqlite => {
            "DROP TABLE search_entity_embedding_vectors; DROP TABLE search_entity_embeddings"
        }
    };

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
            world.semantic(),
            Some(drop_vector_table),
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
async fn a_fused_search_exports_the_query_embedding_and_each_ranking_under_the_search() {
    let world = World::new().await;
    let exported = ExportCapture::default();
    let spans = SpanCapture::exporting_to(&exported);
    let _traced = tracing::subscriber::set_default(spans.subscriber());

    world.search(&world.semantic(), "rain issues").await;

    // SQLite's connection thread holds the span of each statement until
    // after it has answered it. The search span is the span of the last
    // statement, so the search can return before that span ends.
    let search_left = eventually(std::time::Duration::from_secs(30), async || {
        exported
            .spans()
            .iter()
            .any(|span| span.name == "coral.search")
    })
    .await;
    assert!(search_left, "the search span leaves the host");
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
        exported_keys(embed).is_empty(),
        "the query embedding's span carries no field: {embed:#?}"
    );
    assert_eq!(
        exported_keys(search),
        [
            "coral.search.authored_strong.distance",
            "coral.search.authored_strong.phrase",
            "coral.search.authored_strong.rare_path_word",
            "coral.search.provider_tool_distance_present",
        ],
        "the export filter passes the search span's counts and nothing else"
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
async fn the_search_span_counts_the_authored_tools_each_rule_makes_strong() {
    let world = World::anchorage().await;
    // For `moon anchor water`: a phrase, a rare path word, the quay Tool
    // nearer than every Tool, and the harbour Tool, as near as the Tides
    // Tool. The quay and harbour Tools share only `water` with the query.
    for (path, description) in [
        ("watch.log", "Log of moon anchor water"),
        ("anchor.depths", "Depth below the keel"),
        ("quay.tides01", QUAY),
        ("harbour.tideTimes", HARBOUR),
    ] {
        world.save_and_publish(path, description).await;
    }
    world.index_and_embed().await;
    let exported = ExportCapture::default();
    let spans = SpanCapture::exporting_to(&exported);
    let _traced = tracing::subscriber::set_default(spans.subscriber());
    // Each case: the Authored Tools made strong by a phrase, by a rare path
    // word and by distance, Private and Shared together, and whether a Tool
    // distance was there.
    let cases = [
        ("a fused search", world.semantic(), ["2", "2", "2", "true"]),
        (
            "the setting off",
            world.by_words(),
            ["2", "2", "0", "false"],
        ),
    ];
    for (case, search, expected) in cases {
        let before = exported.spans().len();

        world.search(&search, "moon anchor water").await;

        let left = eventually(std::time::Duration::from_secs(30), async || {
            exported
                .spans()
                .iter()
                .skip(before)
                .any(|span| span.name == "coral.search")
        })
        .await;
        assert!(left, "{case}: the search span leaves the host");
        let spans = exported.spans();
        let [search] = spans
            .iter()
            .skip(before)
            .filter(|span| span.name == "coral.search")
            .collect::<Vec<_>>()[..]
        else {
            panic!("{case}: one search span");
        };
        assert_eq!(
            [
                "coral.search.authored_strong.phrase",
                "coral.search.authored_strong.rare_path_word",
                "coral.search.authored_strong.distance",
                "coral.search.provider_tool_distance_present",
            ]
            .map(|key| attribute(search, key)),
            expected.map(|value| Some(value.to_owned())),
            "{case}"
        );
    }
}

#[tokio::test]
async fn each_reason_a_retriever_ranks_by_words_alone_leaves_the_host_on_its_span() {
    let world = World::new().await;
    let drop_vector_table = match world.modules.search.database.system() {
        DatabaseSystem::Postgres => "DROP TABLE search_entity_embeddings",
        DatabaseSystem::Sqlite => {
            "DROP TABLE search_entity_embedding_vectors; DROP TABLE search_entity_embeddings"
        }
    };
    // A Private Tool and a Shared Tool, so each retriever runs its vector
    // query.
    let tides = world
        .save(&world.principal, "harbour.tideTimes", HARBOUR)
        .await;
    world
        .publish(
            "harbour.tideTimes",
            HARBOUR,
            &tides,
            ToolAccessState::Enabled,
        )
        .await;
    world.index_and_embed().await;
    let exported = ExportCapture::default();
    let spans = SpanCapture::exporting_to(&exported);
    let _traced = tracing::subscriber::set_default(spans.subscriber());
    // Each case's reason; `None` is a fused search.
    let cases = [
        ("a fused search", world.semantic(), None, None),
        ("the setting off", world.by_words(), None, Some("disabled")),
        (
            "the model not ready yet",
            world.with_model(ModelLoad::Pending),
            None,
            Some("model_absent"),
        ),
        (
            "the model failed to load",
            world.with_model(ModelLoad::Failed),
            None,
            Some("model_failed"),
        ),
        (
            "a CPU below the floor",
            world.with_model(ModelLoad::CpuTooOld),
            None,
            Some("cpu_floor"),
        ),
        (
            "the model fails on the query",
            world.with_model(ModelLoad::Ready(Arc::new(FailingQueries))),
            None,
            Some("embed_failed"),
        ),
        (
            "the vector query fails",
            world.with_model(ModelLoad::Ready(Arc::new(ShortQueries))),
            None,
            Some("vector_query_failed"),
        ),
        // During a rolling update, an older Coral may have dropped the table.
        (
            "the vector table is missing",
            world.semantic(),
            Some(drop_vector_table),
            Some("vector_query_failed"),
        ),
    ];
    for (case, search, change, reason) in cases {
        if let Some(change) = change {
            world.execute(change).await;
        }
        let before = exported.spans().len();

        world.search(&search, "rain issues").await;

        let this_search = exported
            .spans()
            .into_iter()
            .skip(before)
            .collect::<Vec<_>>();
        for retriever in ["provider_tool", "private_tool", "shared_tool"] {
            assert_semantic(rank(&this_search, retriever, case), reason, case);
        }
        let embed_errors = this_search
            .iter()
            .filter(|span| span.name == "coral.search.embed_query")
            .filter_map(|span| attribute(span, "error.type"))
            .collect::<Vec<_>>();
        let expected: &[&str] = if reason == Some("embed_failed") {
            &["embed_failed"]
        } else {
            &[]
        };
        assert_eq!(
            embed_errors, expected,
            "{case}: the query embedding's span says when the model failed"
        );
    }
}

#[tokio::test]
async fn the_vector_query_reads_the_vector_table_alone() {
    coral_test_support::database_only!(Postgres);
    let world = World::new().await;

    for (kind, plan) in vector_query_plans(&world).await {
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

/// The kind of each hit on `page`, in page order.
fn kinds(page: &SearchPage) -> Vec<EntityKind> {
    page.hits().iter().map(SearchHit::kind).collect()
}

/// The Call Paths of the hits that follow every Tool on `page`, sorted.
fn after_every_provider_tool(page: &SearchPage) -> Vec<String> {
    let start = page
        .hits()
        .iter()
        .rposition(|hit| hit.kind() == EntityKind::ProviderTool)
        .map_or(0, |last| last + 1);
    let mut after = page
        .hits()
        .iter()
        .skip(start)
        .map(|hit| hit.call_path().as_str().to_owned())
        .collect::<Vec<_>>();
    after.sort_unstable();
    after
}

/// The Call Paths of the hits of `kind` on `page`, in page order.
fn of_kind(page: &SearchPage, kind: EntityKind) -> Vec<&str> {
    page.hits()
        .iter()
        .filter(|hit| hit.kind() == kind)
        .map(|hit| hit.call_path().as_str())
        .collect()
}

#[tokio::test]
async fn on_sqlite_each_vector_query_gives_the_eligibility_filter_to_sqlite_vec() {
    coral_test_support::database_only!(Sqlite);
    let world = World::new().await;

    for (kind, plan) in vector_query_plans(&world).await {
        // sqlite-vec names its plan in the index string: `3` is a nearest
        // neighbour scan, and `[` is a `rowid IN (...)` it reads before any
        // distance.
        let vectors = plan
            .iter()
            .find(|line| line.starts_with("SCAN v VIRTUAL TABLE INDEX "))
            .unwrap_or_else(|| panic!("{kind:?}: no scan of the vectors: {plan:#?}"));
        let index = vectors.rsplit_once(':').map_or_default(|(_, index)| index);
        assert!(
            index.starts_with('3') && index.contains('['),
            "{kind:?}: sqlite-vec does not get the rowids: {plan:#?}"
        );
        assert!(
            plan.iter().any(|line| line.starts_with("LIST SUBQUERY")),
            "{kind:?}: the eligibility filter is not one list: {plan:#?}"
        );
    }
}

/// The plan of each retriever's vector query, by the kind it ranks. It
/// reads them on a read snapshot, which runs on a query-only reader on
/// `SQLite`, as every search does.
async fn vector_query_plans(world: &World) -> Vec<(EntityKind, Vec<String>)> {
    let mut transaction = world
        .modules
        .search
        .database
        .begin_read_snapshot()
        .await
        .unwrap();
    let (providers, generations) =
        searched_providers_and_generations(&mut transaction, &world.workspace).await;
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
    plans
}

/// The Providers and active Generations of the Workspace's Tool rows.
async fn searched_providers_and_generations(
    transaction: &mut coral_db::Transaction<'_>,
    workspace: &WorkspaceId,
) -> (Vec<ProviderId>, Vec<coral_catalog::ProviderGenerationId>) {
    let sql = match transaction.system() {
        DatabaseSystem::Postgres => {
            "SELECT DISTINCT provider_id, provider_generation_id FROM search_entities \
             WHERE workspace_id = $1 AND kind = 'provider_tool'"
        }
        DatabaseSystem::Sqlite => {
            "SELECT DISTINCT provider_id, provider_generation_id FROM search_entities \
             WHERE workspace_id = ?1 AND kind = 'provider_tool'"
        }
    };
    let scope: Vec<(String, String)> = transaction
        .fetch_all_sql(sql, sea_query::Values(vec![workspace.to_string().into()]))
        .await
        .unwrap();
    (
        scope
            .iter()
            .map(|(provider, _)| ProviderId::parse(provider).unwrap())
            .collect(),
        scope
            .iter()
            .map(|(_, generation)| coral_catalog::ProviderGenerationId::parse(generation).unwrap())
            .collect(),
    )
}

/// Asserts that `rank` says `fused` when `reason` is `None`, and otherwise
/// `lexical_only` with `reason`, and that it exports no other new key.
fn assert_semantic(rank: &SpanData, reason: Option<&str>, case: &str) {
    let retriever = attribute(rank, "coral.search.retriever");
    assert_eq!(
        (
            attribute(rank, "coral.search.semantic").as_deref(),
            attribute(rank, "coral.search.lexical_only.reason").as_deref(),
        ),
        (
            Some(if reason.is_some() {
                "lexical_only"
            } else {
                "fused"
            }),
            reason
        ),
        "{case}: {retriever:?}"
    );
    let mut keys = vec!["coral.search.retriever", "coral.search.semantic"];
    if reason.is_some() {
        keys.insert(0, "coral.search.lexical_only.reason");
    }
    assert_eq!(
        exported_keys(rank),
        keys,
        "{case}: {retriever:?}: the export filter passes the new fields and nothing else new"
    );
}

/// The one rank span of `retriever` among `spans`.
fn rank<'a>(spans: &'a [SpanData], retriever: &str, case: &str) -> &'a SpanData {
    let ranks = spans
        .iter()
        .filter(|span| {
            span.name == "coral.search.rank"
                && attribute(span, "coral.search.retriever").as_deref() == Some(retriever)
        })
        .collect::<Vec<_>>();
    let [rank] = ranks[..] else {
        panic!("{case}: one {retriever} span left the host, not {ranks:#?}");
    };
    rank
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
/// with the model's files, on `SQLite` or, with the two `CORAL_TEST_*`
/// variables, on Postgres:
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
async fn the_real_model_finds_a_tool_by_semantic_search() {
    let dir = std::env::var_os("CORAL_EMBED_MODEL_DIR")
        .expect("set CORAL_EMBED_MODEL_DIR to a folder with model.onnx and tokenizer.json");
    let model = coral_embed::Model::load(std::path::Path::new(&dir), std::num::NonZeroUsize::MIN)
        .expect("the model loads");
    let world = World::with(Arc::new(model)).await;

    let page = world
        .search(&world.semantic(), "will it rain tomorrow")
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

/// The Tool retriever, whose window loses the nearest Provider Tool while
/// its vector list keeps it: what the merge does when a hundred entries
/// outrank that Tool's place in the vector list alone.
struct LosesNearestTool(ToolRetriever);

#[async_trait::async_trait]
impl EntityRetriever for LosesNearestTool {
    fn kind(&self) -> EntityKind {
        self.0.kind()
    }

    async fn list(
        &self,
        transaction: &mut Transaction<'_>,
        context: RequestContext<'_>,
        provider_filter: &ProviderFilter,
    ) -> Result<Listing, Error> {
        self.0.list(transaction, context, provider_filter).await
    }

    async fn query(
        &self,
        transaction: &mut Transaction<'_>,
        context: RequestContext<'_>,
        request: &QueryRequest<'_>,
    ) -> Result<Answer, QueryError> {
        let mut answer = self.0.query(transaction, context, request).await?;
        let nearest = answer
            .window
            .iter()
            .enumerate()
            .filter_map(|(index, entry)| entry.distance.map(|distance| (index, distance)))
            .min_by(|left, right| left.1.order(right.1))
            .map(|(index, _distance)| index);
        if let Some(index) = nearest {
            answer.window.remove(index);
        }
        Ok(answer)
    }

    async fn hydrate(
        &self,
        transaction: &mut Transaction<'_>,
        context: RequestContext<'_>,
        entity: &EntityRef,
    ) -> Result<Hydrated, Error> {
        self.0.hydrate(transaction, context, entity).await
    }

    async fn describe(
        &self,
        transaction: &mut Transaction<'_>,
        context: RequestContext<'_>,
        call_path: &CallPath,
    ) -> Result<Option<ExactAuthoringDescription>, Error> {
        self.0.describe(transaction, context, call_path).await
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

    /// A World of [`Meanings::tides`] with the Notes and Tides Providers.
    /// For the query `moon anchor`, the Tides Tool is the nearest Tool, and
    /// as near as a text about tides alone can be. The fourteen other Tools
    /// mean nothing of the query: the vector list marks four of them
    /// strong and ten weak.
    async fn anchorage() -> Self {
        let world = Self::with(Arc::new(Meanings::tides())).await;
        world.publish_notes().await;
        world
            .modules
            .publish_openapi(&world.user, &world.workspace, TIDES, "tides", 50)
            .await;
        world.index_and_embed().await;
        world
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

    /// Publishes the Notes Provider: twelve Tools of no meaning, whose
    /// vectors are all the same, as is the GitHub Tool's. The index learns
    /// of it at its next pass.
    async fn publish_notes(&self) {
        let operations = (1..=12)
            .map(|number| {
                (
                    format!("listNotes{number:02}"),
                    format!("List notes {number}"),
                )
            })
            .collect::<Vec<_>>();
        self.publish_operations("notes", &operations, 40).await;
    }

    /// Publishes the Provider `namespace` with one Tool per operation id
    /// and summary, each described as `Returns notes.`. The index learns of
    /// it at its next pass.
    async fn publish_operations(&self, namespace: &str, operations: &[(String, String)], at: i64) {
        let paths = operations
            .iter()
            .enumerate()
            .map(|(number, (operation, summary))| {
                (
                    format!("/{namespace}/{number}"),
                    serde_json::json!({ "get": {
                        "operationId": operation,
                        "summary": summary,
                        "description": "Returns notes.",
                        "responses": { "200": { "description": "OK" } }
                    }}),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        let document = serde_json::json!({
            "openapi": "3.1.0",
            "info": { "title": namespace, "version": "1.0.0" },
            "servers": [{ "url": format!("https://{namespace}.example") }],
            "security": [],
            "paths": paths,
        });
        self.modules
            .publish_openapi(
                &self.user,
                &self.workspace,
                &document.to_string(),
                namespace,
                at,
            )
            .await;
    }

    /// Discovery whose engine embeds each query with the World's model.
    fn semantic(&self) -> UniversalSearchModule {
        self.with_model(ModelLoad::Ready(Arc::clone(&self.model)))
    }

    /// Discovery with the vector side off.
    fn by_words(&self) -> UniversalSearchModule {
        UniversalSearchModule::new(
            self.modules.search.database.clone(),
            self.modules.workspaces.clone(),
        )
    }

    /// Discovery whose engine embeds each query with the World's model, and
    /// whose Tool retriever's window loses its nearest Provider Tool.
    fn losing_nearest_tool(&self) -> UniversalSearchModule {
        let (_ready, model) = watch::channel(ModelLoad::Ready(Arc::clone(&self.model)));
        UniversalSearchModule::with_retrievers(
            self.modules.search.database.clone(),
            self.modules.workspaces.clone(),
            Arc::new(LosesNearestTool(ToolRetriever::default())),
            AuthoredToolRetriever::new,
        )
        .with_model(model)
    }

    /// Discovery whose model slot holds `model`.
    fn with_model(&self, model: ModelLoad) -> UniversalSearchModule {
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

    /// Saves the User's Private Tool at `path` with `description`, and
    /// publishes it as the Shared Tool at the same path, which policy lets
    /// every caller call.
    async fn save_and_publish(&self, path: &str, description: &str) {
        let origin = self.save(&self.principal, path, description).await;
        self.publish(path, description, &origin, ToolAccessState::Enabled)
            .await;
    }

    /// The User's page for `moon anchor` with room for every hit, through
    /// `filter` when it names a Provider namespace.
    async fn anchor(&self, search: &UniversalSearchModule, filter: Option<&str>) -> SearchPage {
        let mut request = SearchRequest::query(SearchQuery::parse("moon anchor").unwrap())
            .with_limit(SearchLimit::parse(50).unwrap());
        if let Some(namespace) = filter {
            request = request.with_provider(
                &ProviderCallRoot::parse(&format!("coral.providers.{namespace}")).unwrap(),
            );
        }
        self.request(search, &self.principal, request).await
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
                &RequestId::generate(),
                &self.workspace,
                coral_workspace::AgentDisplayName::parse("Worker").unwrap(),
                None,
            )
            .await
            .unwrap();
        self.modules
            .workspaces
            .authenticate_agent(
                &self.workspace,
                issued.credential.expose(),
                coral_db::UnixNanos::new(66),
            )
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
