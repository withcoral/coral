//! The Embedder: it gives each Search Entity a vector of the current
//! Embedder revision and source stamp, in the background, on both
//! databases.
//!
//! Every test indexes through `SearchIndex`, waits for the index to settle and
//! for the Embedder to drain, then reads the vector table beside the entity
//! table. The model is a fixed-vector stand-in, so no test loads the real one.
//!
//! A test that runs reconcile passes of its own starts the Embedder alone,
//! with `spawn_embedder`. The reconcile task starts a pass as it starts, and
//! nothing the test awaits waits for that pass: on a loaded host it runs
//! between the test's own steps and indexes a row the test left stale.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use coral_embed::{Embed, EmbedError, MANIFEST};
use coral_telemetry::testing::{ExportCapture, MetricCapture, SpanCapture, attribute};
use coral_workspace::{AuthenticatedUser, IfMatch, WorkspaceId};
use sea_query::{Alias, Expr, ExprTrait as _, Query};
use tokio::sync::watch;
use tracing_subscriber::layer::SubscriberExt as _;
use wiremock::MockServer;

use super::{TestModules, change, exported_keys, organization_user};
use crate::{ModelLoad, SearchIndex};

const USER: &str = "user:search-embedder";

const RECONCILE_INTERVAL: std::time::Duration = std::time::Duration::from_mins(5);

#[tokio::test]
async fn the_embedder_embeds_the_text_the_bench_measured() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let model = FixedVectors::default();
    let index = world.index_with(&model);
    let _background = Background(index.spawn_reconcile_task());

    index.index_active_generation(world.workspace.clone(), world.tracer.clone());
    index.settled().await;
    index.embedded().await;

    // The harness's `path_title_desc_params` gives this text for the same
    // row: the Provider's display name with the words of the path, the
    // title, the description with its guidance, and the field names.
    assert_eq!(
        model.documents(),
        ["Weather Tracer get forecast\n\
          Get one weather forecast\n\
          Fetch the current conditions for one forecast location.\n\
          Use a stable location identifier when one is available.\n\
          data temperature c"]
    );
}

#[tokio::test]
async fn a_private_tool_gets_a_vector_of_its_path_description_and_input_names() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let principal = coral_workspace::AuthenticatedPrincipal::user(world.user.clone());
    let saved = world
        .modules
        .save_source(
            &principal,
            &world.workspace,
            "internal.summarizeTides",
            "Summarize the tides",
            "export default function run(input: { limit: number }): { highWater: number } \
             { return { highWater: input.limit }; }",
            60,
        )
        .await;
    let model = FixedVectors::default();
    let index = world.index_with(&model);
    let _background = Background(index.spawn_reconcile_task());

    index.sync_authored_tool(world.workspace.clone(), saved.id().clone());
    index.settled().await;
    index.embedded().await;

    assert!(world.vectors().await.contains_key(&saved.id().to_string()));
    // A Private Tool has no Provider and no title. Its text names its input,
    // `limit`, and not its result, `highWater`.
    assert!(
        model
            .documents()
            .contains(&"internal summarize tides\nSummarize the tides\nlimit".to_owned()),
        "{:?}",
        model.documents()
    );
}

#[tokio::test]
async fn a_row_an_older_coral_changed_gets_no_vector_until_a_pass_indexes_it_again() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let principal = coral_workspace::AuthenticatedPrincipal::user(world.user.clone());
    let saved = world
        .modules
        .save_private_tool(
            &principal,
            &world.workspace,
            "internal.summarizeTides",
            "Summarize the tides",
            60,
        )
        .await;
    let doc_id = saved.id().to_string();
    let model = FixedVectors::default();
    let index = world.index_with(&model);
    let _background = Background(index.spawn_embedder());
    index.sync_authored_tool(world.workspace.clone(), saved.id().clone());
    index.settled().await;
    index.embedded().await;
    let stamp = world
        .entity_stamps()
        .await
        .get(&doc_id)
        .copied()
        .expect("the Private Tool's row");

    // During a rolling update, the previous release writes a new revision
    // of the row. Its upsert does not know the field names, so it leaves
    // those of the revision before. The ledger's first index write then
    // wakes the Embedder before any reconcile pass.
    world
        .execute(&format!(
            "UPDATE search_entities SET description = 'Summarize the tides and currents', \
             fingerprint = '2.2', indexed_at_unix_nanos = indexed_at_unix_nanos + 1 \
             WHERE doc_id = '{doc_id}'"
        ))
        .await;
    let ledger = world.publish_ledger().await;
    index.index_active_generation(world.workspace.clone(), ledger.clone());
    index.settled().await;
    index.embedded().await;
    assert!(
        !world.vectors_of(&ledger).await.is_empty(),
        "the ledger's write woke the Embedder"
    );

    assert_eq!(
        model.documents_with("currents"),
        0,
        "no text joins one revision's description to another's field names"
    );
    assert_eq!(
        world
            .vectors()
            .await
            .get(&doc_id)
            .map(|(_, source)| *source),
        Some(stamp),
        "the vector keeps the stamp it was made from"
    );

    index.reconcile().await;
    index.settled().await;
    index.embedded().await;
    world.assert_every_vector_current().await;
}

#[tokio::test]
async fn a_missing_vector_another_revision_or_a_changed_row_is_embedded_again_and_a_current_row_is_not()
 {
    // Each case changes what the Embedder finds, and names the rows it
    // embeds again: one of the ledger's three, all three, or none.
    let cases: [(&str, Staleness, usize); 4] = [
        ("current rows", Staleness::None, 0),
        ("a missing vector", Staleness::MissingVector, 1),
        ("another revision", Staleness::OtherRevision, 1),
        ("changed rows", Staleness::ChangedRows, 3),
    ];
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let ledger = world.publish_ledger().await;
    let model = FixedVectors::default();
    let index = world.index_with(&model);
    let _background = Background(index.spawn_embedder());
    world
        .index_and_embed(&index, &[&world.tracer, &ledger])
        .await;
    // Each case starts and ends with every vector current.
    for (case, staleness, embedded_again) in cases {
        let before = model.documents().len();

        world.make_stale(&index, &ledger, staleness).await;
        index.reconcile().await;
        index.settled().await;
        index.embedded().await;

        assert_eq!(
            model.documents().len() - before,
            embedded_again,
            "{case}: rows embedded again"
        );
        world.assert_every_vector_current().await;
    }
}

#[tokio::test]
async fn a_row_written_again_with_the_same_text_keeps_its_vector_and_the_model_does_not_run() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let ledger = world.publish_ledger().await;
    let model = FixedVectors::default();
    let index = world.index_with(&model);
    let _background = Background(index.spawn_embedder());
    world
        .index_and_embed(&index, &[&world.tracer, &ledger])
        .await;
    let stamps = world.entity_stamps().await;
    let embeddings = world.embeddings().await;
    let embedded = model.documents().len();

    // An older indexer revision wrote other words for lexical search. The
    // current revision writes each row again: new words, so a new stamp,
    // and the same text for the model.
    world
        .execute(
            "UPDATE search_entities SET searchable_text = 'words of an older indexer'; \
             UPDATE search_index_states SET indexer_revision = 0 WHERE kind = 'provider_tool'",
        )
        .await;
    index.reconcile().await;
    index.settled().await;
    index.embedded().await;

    assert_ne!(
        world.entity_stamps().await,
        stamps,
        "the pass wrote the rows again"
    );
    assert_eq!(model.documents().len(), embedded, "the model did not run");
    assert_eq!(
        world.embeddings().await,
        embeddings,
        "each row keeps its vector"
    );
    world.assert_every_vector_current().await;
}

#[tokio::test]
async fn a_new_generation_runs_the_model_only_for_the_tools_whose_text_changed() {
    // Each case publishes a new Generation of the ledger and names the
    // Tools whose text changed. The model runs for those alone, and every
    // other Tool keeps its vector.
    let cases: [(&str, String, &[&str]); 2] = [
        (
            "a new server, which no Tool text holds",
            ledger_on_another_server(),
            &[],
        ),
        (
            "one Tool of three changed",
            LEDGER.replace("List ledger entries", "List every entry"),
            &["listEntries"],
        ),
    ];
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let ledger = world.publish_ledger().await;
    let model = FixedVectors::default();
    let index = world.index_with(&model);
    let _background = Background(index.spawn_embedder());
    world
        .index_and_embed(&index, &[&world.tracer, &ledger])
        .await;
    for (at, (case, document, changed)) in (61..).zip(cases) {
        let before = world.tool_embeddings(&ledger).await;
        let embedded = model.documents().len();

        world
            .publish_generation(&index, &ledger, &document, at)
            .await;

        assert_eq!(
            model.documents().len() - embedded,
            changed.len(),
            "{case}: model runs"
        );
        // The pass removes the replaced Generation's rows and vectors.
        index.reconcile().await;
        index.embedded().await;
        world.assert_every_vector_current().await;
        let after = world.tool_embeddings(&ledger).await;
        assert_eq!(
            after.keys().collect::<Vec<_>>(),
            before.keys().collect::<Vec<_>>(),
            "{case}"
        );
        for (tool, (generation, vector)) in after {
            let (old_generation, old_vector) = before.get(&tool).expect("the Tool before");
            assert_ne!(&generation, old_generation, "{case}: a new Generation");
            assert_eq!(
                &vector != old_vector,
                changed.contains(&tool.as_str()),
                "{case}: {tool} has a new vector exactly when its text changed"
            );
        }
    }
}

#[tokio::test]
async fn a_vector_is_a_source_only_of_the_current_revision_and_with_its_digest_written_beside_it() {
    // Each case changes the vectors of a replaced Generation whose rows are
    // gone, and names how many of the new Generation's three Tools, with
    // the same text, run the model. A digest beside a vector of another
    // stamp has its own test: an older Coral's write.
    let cases = [
        ("a vector of a replaced Generation", None, 0),
        (
            "a vector of another Embedder revision",
            Some("embedder_revision = 'older'"),
            3,
        ),
        (
            "a vector an older Coral wrote without a digest",
            Some("text_sha256 = NULL, text_sha256_source_indexed_at_unix_nanos = NULL"),
            3,
        ),
    ];
    for (case, change, embedded_again) in cases {
        let upstream = MockServer::start().await;
        let world = World::publish_tracer(&upstream).await;
        let ledger = world.publish_ledger().await;
        let model = FixedVectors::default();
        let index = world.index_with(&model);
        let _background = Background(index.spawn_embedder());
        world
            .index_and_embed(&index, &[&world.tracer, &ledger])
            .await;
        let replaced = world.vectors_of(&ledger).await;
        world
            .execute(&format!(
                "DELETE FROM search_entities WHERE provider_id = '{ledger}'"
            ))
            .await;
        if let Some(change) = change {
            world
                .execute(&format!(
                    "UPDATE search_entity_embeddings SET {change} WHERE doc_id IN ('{}')",
                    replaced.join("', '")
                ))
                .await;
        }
        let embedded = model.documents().len();

        world
            .publish_generation(&index, &ledger, &ledger_on_another_server(), 61)
            .await;

        assert_eq!(
            model.documents().len() - embedded,
            embedded_again,
            "{case}: model runs"
        );
        index.reconcile().await;
        index.embedded().await;
        world.assert_every_vector_current().await;
    }
}

#[tokio::test]
async fn a_vector_an_older_coral_writes_with_its_column_list_is_kept_and_is_no_source() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let ledger = world.publish_ledger().await;
    let model = FixedVectors::default();
    let index = world.index_with(&model);
    let _background = Background(index.spawn_embedder());
    world
        .index_and_embed(&index, &[&world.tracer, &ledger])
        .await;
    let list_entries = world.newest_doc_id_titled("List ledger entries").await;

    // During a rolling update, an older Coral writes other text for the
    // Tool and the vector of that text, with the column list it knows. The
    // digest of the text before stays on the vector row.
    world
        .execute(&format!(
            "UPDATE search_entities SET description = 'Words of an older Coral', \
             indexed_at_unix_nanos = indexed_at_unix_nanos + 1 WHERE doc_id = '{list_entries}'"
        ))
        .await;
    world
        .write_as_an_older_coral(
            &list_entries,
            &FixedVectors::vector("Words of an older Coral"),
        )
        .await;
    world.assert_every_vector_current().await;
    let embedded = model.documents().len();

    // The new Generation's Tool has the text that stale digest names.
    world
        .publish_generation(&index, &ledger, &ledger_on_another_server(), 61)
        .await;

    assert_eq!(
        model.documents().len() - embedded,
        1,
        "the older Coral's vector is no source; the other two Tools reuse theirs"
    );
}

#[tokio::test]
async fn a_reused_vector_does_not_reach_a_row_that_changed_or_went_before_the_write() {
    // In each case one batch holds the ledger's three rows: the model holds
    // the one whose text changed, and the other two reuse a vector. Before
    // the write, one of those two changes and the other goes.
    for case in ["a copy of another row's vector", "a row's own vector"] {
        let upstream = MockServer::start().await;
        let world = World::publish_tracer(&upstream).await;
        let ledger = world.publish_ledger().await;
        {
            let index = world.index_with(&FixedVectors::default());
            let _background = Background(index.spawn_embedder());
            world
                .index_and_embed(&index, &[&world.tracer, &ledger])
                .await;
        }
        let held = Arc::new(std::sync::Barrier::new(2));
        let model = FixedVectors::default().holding_first_text_at(&held);
        let index = world.index_with(&model);
        let _background = Background(index.spawn_embedder());
        if case == "a copy of another row's vector" {
            world
                .regenerate(
                    &ledger,
                    &LEDGER.replace("List ledger entries", "List every entry"),
                    61,
                )
                .await;
            index.index_active_generation(world.workspace.clone(), ledger.clone());
            index.settled().await;
        } else {
            // New words for lexical search give each row a new stamp; one
            // row gets new text too.
            world
                .execute(&format!(
                    "UPDATE search_entities SET searchable_text = 'new words', \
                     indexed_at_unix_nanos = indexed_at_unix_nanos + 1 \
                     WHERE provider_id = '{ledger}'; \
                     UPDATE search_entities SET description = 'List every entry' \
                     WHERE title = 'List ledger entries'"
                ))
                .await;
            index.reconcile().await;
        }
        meet(&held).await;
        let changed = world.newest_doc_id_titled("Record a ledger entry").await;
        let gone = world.newest_doc_id_titled("Delete a ledger entry").await;
        let gone_vector = world.vectors().await.get(&gone).cloned();

        world
            .execute(&format!(
                "UPDATE search_entities SET description = 'Changed while the model ran', \
                 indexed_at_unix_nanos = indexed_at_unix_nanos + 1 WHERE doc_id = '{changed}'; \
                 DELETE FROM search_entities WHERE doc_id = '{gone}'"
            ))
            .await;
        meet(&held).await;
        index.embedded().await;

        let documents = model.documents();
        let [changed_text] = documents
            .iter()
            .filter(|text| text.contains("Changed while the model ran"))
            .collect::<Vec<_>>()[..]
        else {
            panic!("{case}: the model embeds the changed row's new text once");
        };
        assert_eq!(
            world.embeddings().await.get(&changed),
            Some(&FixedVectors::vector(changed_text)),
            "{case}: the changed row's vector is of its new text"
        );
        assert_eq!(
            world.vectors().await.get(&changed).map(|(_, stamp)| *stamp),
            world.entity_stamps().await.get(&changed).copied(),
            "{case}: the changed row's vector is current"
        );
        assert_eq!(
            world.vectors().await.get(&gone).cloned(),
            gone_vector,
            "{case}: no write touched the vector row of the row that went"
        );
    }
}

#[tokio::test]
async fn each_write_exports_the_vectors_it_reused_and_those_the_model_made() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let ledger = world.publish_ledger().await;
    let exported = ExportCapture::default();
    let spans = SpanCapture::exporting_to(&exported);
    let _traced = tracing::subscriber::set_default(spans.subscriber());
    let index = world.index_with(&FixedVectors::default());
    let _background = Background(index.spawn_embedder());
    world
        .index_and_embed(&index, &[&world.tracer, &ledger])
        .await;
    let before = exported.spans().len();

    world
        .publish_generation(
            &index,
            &ledger,
            &LEDGER.replace("List ledger entries", "List every entry"),
            61,
        )
        .await;

    let batches = exported
        .spans()
        .into_iter()
        .skip(before)
        .filter(|span| span.name == "coral.search.embed_batch")
        .collect::<Vec<_>>();
    let [batch] = &batches[..] else {
        panic!("one write of the new Generation: {batches:#?}");
    };
    assert_eq!(
        exported_keys(batch),
        [
            "coral.search.embedder.rows",
            "coral.search.embedder.vectors_embedded",
            "coral.search.embedder.vectors_reused",
            "coral.search.embedder.vectors_written",
            "coral.workspace.id",
        ],
        "no text and no digest leaves the host"
    );
    let count = |key: &str| attribute(batch, key);
    assert_eq!(
        [
            count("coral.search.embedder.rows"),
            count("coral.search.embedder.vectors_reused"),
            count("coral.search.embedder.vectors_embedded"),
            count("coral.search.embedder.vectors_written"),
        ],
        [3, 2, 1, 3].map(|count: u8| Some(count.to_string())),
        "rows, reused, embedded, written"
    );
}

#[tokio::test]
async fn the_same_text_in_another_workspace_runs_the_model() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let ledger = world.publish_ledger().await;
    let other = world.modules.create_workspace(&world.user).await;
    let model = FixedVectors::default();
    let index = world.index_with(&model);
    let _background = Background(index.spawn_embedder());
    world
        .index_and_embed(&index, &[&world.tracer, &ledger])
        .await;
    let embedded = model.documents();

    let other_ledger = world
        .modules
        .publish_openapi(&world.user, &other, LEDGER, "ledger", 50)
        .await;
    index.index_active_generation(other.clone(), other_ledger);
    index.settled().await;
    index.embedded().await;

    let added = model.documents().split_off(embedded.len());
    assert_eq!(
        added.len(),
        3,
        "the model ran for each of the ledger's Tools"
    );
    assert!(
        added.iter().all(|text| embedded.contains(text)),
        "each text has a vector in the first Workspace: {added:?}"
    );
    assert_eq!(world.vectors_in(&other).await.len(), 3);
}

#[tokio::test]
async fn two_embedders_on_the_same_rows_write_each_vector_once() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let ledger = world.publish_ledger().await;
    world.index_without_vectors(&[&world.tracer, &ledger]).await;
    let spans = SpanCapture::default();
    let _traced = tracing::subscriber::set_default(spans.subscriber());
    // Each model waits at its first text until the other reaches its own,
    // so both Embedders read the same stale rows before either writes.
    let both_embedding = Arc::new(std::sync::Barrier::new(2));
    let first = FixedVectors::default().waiting_once_at(&both_embedding);
    let second = FixedVectors::default().waiting_once_at(&both_embedding);
    let indexes = [world.index_with(&first), world.index_with(&second)];
    let _background = indexes
        .iter()
        .map(|index| Background(index.spawn_reconcile_task()))
        .collect::<Vec<_>>();

    for index in &indexes {
        index.embedded().await;
    }

    world.assert_every_vector_current().await;
    assert!(
        !first.documents().is_empty() && !second.documents().is_empty(),
        "both Embedders embedded"
    );
    let written = spans
        .finished()
        .iter()
        .filter(|span| span.name == "coral.search.embed_batch")
        .filter_map(|span| attribute(span, "coral.search.embedder.vectors_written"))
        .map(|written| written.parse::<usize>().unwrap())
        .sum::<usize>();
    assert_eq!(written, world.entity_stamps().await.len());
}

#[tokio::test]
async fn a_reconcile_pass_removes_the_vectors_of_a_deleted_provider_and_keeps_the_rest() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let ledger = world.publish_ledger().await;
    let model = FixedVectors::default();
    let index = world.index_with(&model);
    let _background = Background(index.spawn_embedder());
    world
        .index_and_embed(&index, &[&world.tracer, &ledger])
        .await;
    let ledger_vectors = world.vectors_of(&ledger).await;
    assert!(!ledger_vectors.is_empty(), "the ledger has vectors");
    let mut tracer_vectors = world.vectors().await;
    tracer_vectors.retain(|doc_id, _| !ledger_vectors.contains(doc_id));

    world
        .modules
        .providers
        .delete_provider(
            &change(world.user.clone(), 80, '9'),
            &world.workspace,
            &ledger,
            &IfMatch::any(),
        )
        .await
        .expect("delete the ledger")
        .skip_search();
    index.reconcile().await;

    assert_eq!(world.vectors().await, tracer_vectors);
    assert_eq!(
        world.unkeyed_vectors().await,
        0,
        "the ledger's vectors are gone"
    );
}

#[tokio::test]
async fn a_batch_that_fails_with_the_model_or_the_database_down_skips_nothing_and_the_next_pass_embeds_it()
 {
    for down in ["the model", "the database"] {
        let upstream = MockServer::start().await;
        let world = World::publish_tracer(&upstream).await;
        let ledger = world.publish_ledger().await;
        let model = FixedVectors::default();
        if down == "the model" {
            model.fail_texts_with("");
        } else {
            world.refuse_vector_writes(true).await;
        }
        let index = world.index_with(&model);
        let _background = Background(index.spawn_embedder());
        for provider in [&world.tracer, &ledger] {
            index.index_active_generation(world.workspace.clone(), provider.clone());
        }
        index.settled().await;
        index.embedded().await;
        assert_eq!(world.vectors().await, BTreeMap::new(), "{down} is down");

        model.fail_texts_with_nothing();
        world.refuse_vector_writes(false).await;
        index.reconcile().await;
        index.embedded().await;

        world.assert_every_vector_current().await;
    }
}

#[tokio::test]
async fn a_row_that_always_fails_is_skipped_until_its_row_changes_and_holds_back_no_other_row() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    // More Tools than one batch holds; the first in document order fails.
    let many = world
        .modules
        .publish_openapi(&world.user, &world.workspace, &many_tools(40), "many", 60)
        .await;
    let model = FixedVectors::default();
    model.fail_texts_with(BROKEN);
    let index = world.index_with(&model);
    let _background = Background(index.spawn_embedder());
    index.index_active_generation(world.workspace.clone(), many.clone());
    index.index_active_generation(world.workspace.clone(), world.tracer.clone());
    index.settled().await;
    index.embedded().await;
    let broken = world.broken_doc_id().await;
    let mut vectors = world.vectors().await;
    assert_eq!(
        vectors.remove(&broken),
        None,
        "the broken row has no vector"
    );
    assert_eq!(vectors.len(), world.entity_stamps().await.len() - 1);

    // The model would embed it now, but the row did not change.
    model.fail_texts_with_nothing();
    let tried = model.documents_with(BROKEN);
    index.reconcile().await;
    index.embedded().await;
    assert_eq!(
        model.documents_with(BROKEN),
        tried,
        "a skipped row is not tried"
    );
    assert!(!world.vectors().await.contains_key(&broken));

    world.rename(&many, "Many More").await;
    index.index_active_generation(world.workspace.clone(), many.clone());
    index.settled().await;
    index.embedded().await;
    world.assert_every_vector_current().await;
}

#[tokio::test]
async fn the_backlog_goes_to_zero_once_the_embedder_drains_a_provider() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let metrics = MetricCapture::default();
    let _measured = tracing::subscriber::set_default(metrics.subscriber());
    let (model, index) = world.index_waiting_for_model();
    let _background = Background(index.spawn_reconcile_task());
    index.index_active_generation(world.workspace.clone(), world.tracer.clone());
    index.settled().await;
    index.observe_backlog().await;
    let backlog = || gauge(&metrics, "coral.search.embed.backlog");
    let entities = world.entity_stamps().await.len();
    assert!(entities > 0, "the tracer has Search Entities");
    assert_eq!(
        backlog(),
        Some(f64::from(u32::try_from(entities).unwrap())),
        "each row waits for its vector while the model is pending"
    );

    model.send_replace(ModelLoad::Ready(Arc::new(FixedVectors::default())));
    index.embedded().await;
    index.observe_backlog().await;

    assert_eq!(backlog(), Some(0.0));
}

#[tokio::test]
async fn a_row_on_the_skip_list_shows_in_the_skipped_rows_and_its_batch_as_failed() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let many = world
        .modules
        .publish_openapi(&world.user, &world.workspace, &many_tools(40), "many", 60)
        .await;
    let metrics = MetricCapture::default();
    let _measured = tracing::subscriber::set_default(metrics.subscriber());
    let model = FixedVectors::default();
    model.fail_texts_with(BROKEN);
    let index = world.index_with(&model);
    let _background = Background(index.spawn_reconcile_task());

    index.index_active_generation(world.workspace.clone(), many);
    index.settled().await;
    index.embedded().await;
    index.observe_backlog().await;

    assert_eq!(gauge(&metrics, "coral.search.embed.skipped"), Some(1.0));
    assert_eq!(gauge(&metrics, "coral.search.embed.backlog"), Some(1.0));
    assert_eq!(
        batch_outcomes(&metrics),
        ["failed".to_owned(), "ok".to_owned()].into(),
        "the batch with the broken row failed, the rows tried alone after it were written"
    );
}

#[tokio::test]
async fn a_model_panic_fails_its_batch_and_a_new_thread_embeds_the_rows() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let metrics = MetricCapture::default();
    let spans = SpanCapture::default();
    let _measured = tracing::subscriber::set_default(
        tracing_subscriber::Registry::default()
            .with(metrics.layer())
            .with(spans.layer()),
    );
    let index = world.index_with(&FixedVectors::default().panicking_once());
    let _background = Background(index.spawn_reconcile_task());

    world.index_and_embed(&index, &[&world.tracer]).await;

    assert_eq!(
        batch_outcomes(&metrics),
        ["failed".to_owned(), "ok".to_owned()].into(),
        "the batch the model panicked in failed, the new thread's batch was written"
    );
    let failed = metrics
        .named("coral.search.embed.batch.duration")
        .into_iter()
        .filter(|point| point.attributes.get("outcome").map(String::as_str) == Some("failed"))
        .map(|point| point.count)
        .sum::<u64>();
    assert_eq!(failed, 1, "one batch failed: the one the model panicked in");
    let failures = spans
        .finished()
        .iter()
        .filter(|span| span.name == "coral.search.embed_batch")
        .filter_map(|span| attribute(span, "error.type"))
        .collect::<Vec<_>>();
    assert_eq!(failures, ["thread_gone"]);
}

/// The outcomes of the Embedder's batches.
fn batch_outcomes(metrics: &MetricCapture) -> std::collections::BTreeSet<String> {
    metrics
        .named("coral.search.embed.batch.duration")
        .into_iter()
        .filter_map(|point| point.attributes.get("outcome").cloned())
        .collect()
}

/// The last value of the gauge `name`, when one was observed.
fn gauge(metrics: &MetricCapture, name: &str) -> Option<f64> {
    metrics.values(name).last().copied()
}

#[tokio::test]
async fn a_failing_row_alone_in_its_batch_is_skipped_and_holds_back_no_other_workspace() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let other = world.modules.create_workspace(&world.user).await;
    let ledger = world
        .modules
        .publish_openapi(&world.user, &other, LEDGER, "ledger", 50)
        .await;
    let model = FixedVectors::default();
    model.fail_texts_with("Get one weather forecast");
    let index = world.index_with(&model);
    let _background = Background(index.spawn_embedder());
    // The tracer's row is older, so its batch, of one row, comes first.
    index.index_active_generation(world.workspace.clone(), world.tracer.clone());
    index.settled().await;
    index.index_active_generation(other.clone(), ledger);
    index.settled().await;
    index.embedded().await;
    assert_eq!(world.vectors().await, BTreeMap::new());
    assert_eq!(world.vectors_in(&other).await.len(), 3, "the ledger's rows");

    model.fail_texts_with_nothing();
    index.reconcile().await;
    index.embedded().await;

    assert_eq!(
        world.vectors().await,
        BTreeMap::new(),
        "the row stays skipped"
    );
}

#[tokio::test]
async fn an_index_write_does_not_wait_for_the_embedder() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let ledger = world.publish_ledger().await;
    let held = Arc::new(std::sync::Barrier::new(2));
    let model = FixedVectors::default().holding_first_text_at(&held);
    let index = world.index_with(&model);
    let _background = Background(index.spawn_reconcile_task());
    index.index_active_generation(world.workspace.clone(), world.tracer.clone());
    index.settled().await;
    meet(&held).await;

    // The Embedder holds its first text while the ledger is published,
    // renamed and indexed.
    world.rename(&ledger, "Day Book").await;
    index.index_active_generation(world.workspace.clone(), ledger.clone());
    tokio::time::timeout(std::time::Duration::from_secs(30), index.settled())
        .await
        .expect("the index write finishes while the Embedder holds its text");
    assert!(
        world.vectors().await.is_empty(),
        "the Embedder still holds its first text"
    );

    meet(&held).await;
    index.embedded().await;
    world.assert_every_vector_current().await;
}

/// Waits at `barrier` on a blocking thread, beside the Embedder's.
async fn meet(barrier: &Arc<std::sync::Barrier>) {
    let barrier = barrier.clone();
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        tokio::task::spawn_blocking(move || {
            barrier.wait();
        }),
    )
    .await
    .expect("the Embedder reaches the barrier")
    .unwrap();
}

/// The summary of the Tool of [`many_tools`] that comes first in document
/// order.
const BROKEN: &str = "Broken tool";

/// An OpenAPI document with `count` Tools, `op00` first.
fn many_tools(count: usize) -> String {
    let paths = (0..count)
        .map(|tool| {
            let summary = if tool == 0 {
                BROKEN.to_owned()
            } else {
                format!("Tool number {tool}")
            };
            format!(
                r#""/items{tool}": {{ "get": {{ "operationId": "op{tool:02}", "summary": "{summary}", "responses": {{ "200": {{ "description": "OK" }} }} }} }}"#
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        r#"{{ "openapi": "3.1.0", "info": {{ "title": "Many", "version": "1.0.0" }}, "servers": [{{ "url": "https://many.example" }}], "paths": {{ {paths} }} }}"#
    )
}

#[derive(Clone, Copy)]
enum Staleness {
    None,
    MissingVector,
    OtherRevision,
    ChangedRows,
}

/// [`LEDGER`] on another server: a new Generation whose Tool texts, which
/// name no server, stay the same.
fn ledger_on_another_server() -> String {
    LEDGER.replace("https://ledger.example", "https://books.example")
}

/// A Provider with three Tools, one with parameters.
const LEDGER: &str = r#"{
  "openapi": "3.1.0",
  "info": { "title": "Ledger", "version": "1.0.0" },
  "servers": [{ "url": "https://ledger.example" }],
  "paths": {
    "/entries": {
      "get": {
        "operationId": "listEntries",
        "summary": "List ledger entries",
        "responses": { "200": { "description": "OK" } }
      },
      "post": {
        "operationId": "createEntry",
        "summary": "Record a ledger entry",
        "requestBody": {
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "properties": {
                  "amountCents": { "type": "integer" },
                  "memo": { "type": "string" }
                }
              }
            }
          }
        },
        "responses": { "200": { "description": "OK" } }
      }
    },
    "/entries/{entryId}": {
      "delete": {
        "operationId": "deleteEntry",
        "summary": "Delete a ledger entry",
        "parameters": [
          { "name": "entryId", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": { "200": { "description": "OK" } }
      }
    }
  }
}"#;

/// A barrier and how many times the first text waits at it.
type FirstTextWait = (Arc<std::sync::Barrier>, usize);

/// A model that gives each text a fixed vector and records each text it
/// embeds.
#[derive(Clone, Default)]
struct FixedVectors {
    documents: Arc<Mutex<Vec<String>>>,
    /// A barrier the first text waits at, and how many times.
    first_text: Arc<Mutex<Option<FirstTextWait>>>,
    /// Texts that hold this fail; `None` fails none.
    failing: Arc<Mutex<Option<String>>>,
    /// The next text panics, which ends the Embedder's thread.
    panic_next: Arc<AtomicBool>,
}

impl FixedVectors {
    /// This model waits at `barrier` once, before its first text.
    fn waiting_once_at(self, barrier: &Arc<std::sync::Barrier>) -> Self {
        *self.first_text.lock().unwrap() = Some((barrier.clone(), 1));
        self
    }

    /// This model meets the test at `barrier` when its first text starts,
    /// and again before it embeds that text.
    fn holding_first_text_at(self, barrier: &Arc<std::sync::Barrier>) -> Self {
        *self.first_text.lock().unwrap() = Some((barrier.clone(), 2));
        self
    }

    /// This model panics on its first text, once.
    fn panicking_once(self) -> Self {
        self.panic_next.store(true, Ordering::SeqCst);
        self
    }

    /// Fails each later text that holds `part`; an empty `part` fails
    /// every text.
    fn fail_texts_with(&self, part: &str) {
        *self.failing.lock().unwrap() = Some(part.to_owned());
    }

    fn fail_texts_with_nothing(&self) {
        *self.failing.lock().unwrap() = None;
    }

    /// The texts embedded or tried that hold `part`.
    fn documents_with(&self, part: &str) -> usize {
        self.documents()
            .iter()
            .filter(|text| text.contains(part))
            .count()
    }

    fn documents(&self) -> Vec<String> {
        self.documents.lock().unwrap().clone()
    }

    /// A vector of two slots that a hash of the whole text picks, so two
    /// texts of a test rarely share one. Both values are exact in `halfvec`.
    fn vector(text: &str) -> Vec<f32> {
        let mut vector = vec![0.0; MANIFEST.dimension];
        let hash = text.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
        });
        let dimension = u64::try_from(MANIFEST.dimension).unwrap();
        for (bits, value) in [(hash, 1.0), (hash >> 32, 0.5)] {
            let slot = usize::try_from(bits % dimension).unwrap();
            if let Some(slot) = vector.get_mut(slot) {
                *slot += value;
            }
        }
        vector
    }
}

impl Embed for FixedVectors {
    fn embed_query(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
        Ok(Self::vector(text))
    }

    #[expect(
        clippy::panic_in_result_fn,
        reason = "the model panics on purpose, to end the Embedder's thread"
    )]
    fn embed_document(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
        assert!(
            !self.panic_next.swap(false, Ordering::SeqCst),
            "injected model panic"
        );
        let barrier = lock(&self.first_text).take();
        if let Some((barrier, times)) = barrier {
            for _ in 0..times {
                barrier.wait();
            }
        }
        lock(&self.documents).push(text.to_owned());
        let failing = lock(&self.failing).clone();
        if failing.is_some_and(|part| text.contains(&part)) {
            return Err(EmbedError::Output("injected embedding fault".to_owned()));
        }
        Ok(Self::vector(text))
    }
}

/// Locks `mutex`, poisoned or not: a test's assertion fails first.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Aborts the background task when the test ends.
struct Background(tokio::task::JoinHandle<()>);

impl Drop for Background {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct World {
    modules: TestModules,
    workspace: WorkspaceId,
    tracer: coral_provider::ProviderId,
    user: AuthenticatedUser,
}

impl World {
    async fn publish_tracer(upstream: &MockServer) -> Self {
        let modules = TestModules::new().await;
        let user = organization_user(modules.search.database.clone(), USER).await;
        let workspace = modules.create_workspace(&user).await;
        let tracer = modules
            .publish_tracer(&user, &workspace, &upstream.uri())
            .await
            .provider_id;
        modules.prepare_vectors().await;
        Self {
            modules,
            workspace,
            tracer,
            user,
        }
    }

    /// Publishes [`LEDGER`].
    async fn publish_ledger(&self) -> coral_provider::ProviderId {
        self.modules
            .publish_openapi(&self.user, &self.workspace, LEDGER, "ledger", 50)
            .await
    }

    /// Indexes each of `providers` with an index whose vector side is off.
    async fn index_without_vectors(&self, providers: &[&coral_provider::ProviderId]) {
        let index = SearchIndex::new(self.modules.search.database.clone(), RECONCILE_INTERVAL);
        for provider in providers {
            index.index_active_generation(self.workspace.clone(), (*provider).clone());
        }
        index.settled().await;
    }

    /// Indexes each of `providers`, then waits for the index and the
    /// Embedder.
    async fn index_and_embed(
        &self,
        index: &SearchIndex,
        providers: &[&coral_provider::ProviderId],
    ) {
        for provider in providers {
            index.index_active_generation(self.workspace.clone(), (*provider).clone());
        }
        index.settled().await;
        index.embedded().await;
        self.assert_every_vector_current().await;
    }

    /// Makes the Embedder find `provider`'s rows stale as `staleness` says.
    async fn make_stale(
        &self,
        index: &SearchIndex,
        provider: &coral_provider::ProviderId,
        staleness: Staleness,
    ) {
        let one_vector = format!(
            "WHERE doc_id = (SELECT min(doc_id) FROM search_entities \
             WHERE workspace_id = '{}' AND provider_id = '{provider}')",
            self.workspace
        );
        match staleness {
            Staleness::None => {}
            Staleness::MissingVector => {
                self.execute(&format!(
                    "DELETE FROM search_entity_embeddings {one_vector}"
                ))
                .await;
            }
            Staleness::OtherRevision => {
                self.execute(&format!(
                    "UPDATE search_entity_embeddings SET embedder_revision = 'older' {one_vector}"
                ))
                .await;
            }
            Staleness::ChangedRows => {
                self.rename(provider, "Day Book").await;
                index.index_active_generation(self.workspace.clone(), provider.clone());
                index.settled().await;
            }
        }
    }

    /// The document ids of the vectors of `provider`'s Tools.
    async fn vectors_of(&self, provider: &coral_provider::ProviderId) -> Vec<String> {
        let rows: Vec<(String,)> = self
            .modules
            .search
            .database
            .fetch_all(
                Query::select()
                    .column(Alias::new("doc_id"))
                    .from(Alias::new("search_entity_embeddings"))
                    .and_where(Expr::col(Alias::new("workspace_id")).eq(self.workspace.to_string()))
                    .and_where(
                        Expr::col(Alias::new("doc_id")).in_subquery(
                            Query::select()
                                .column(Alias::new("doc_id"))
                                .from(Alias::new("search_entities"))
                                .and_where(
                                    Expr::col(Alias::new("workspace_id"))
                                        .eq(self.workspace.to_string()),
                                )
                                .and_where(
                                    Expr::col(Alias::new("provider_id")).eq(provider.to_string()),
                                )
                                .to_owned(),
                        ),
                    )
                    .to_owned(),
            )
            .await
            .expect("read the vectors of a Provider");
        rows.into_iter().map(|(doc_id,)| doc_id).collect()
    }

    /// Publishes `document` as `provider`'s new Generation at `at`, indexes
    /// it, and waits for the Embedder.
    async fn publish_generation(
        &self,
        index: &SearchIndex,
        provider: &coral_provider::ProviderId,
        document: &str,
        at: i64,
    ) {
        self.regenerate(provider, document, at).await;
        index.index_active_generation(self.workspace.clone(), provider.clone());
        index.settled().await;
        index.embedded().await;
    }

    /// Publishes `document` as `provider`'s new Generation, at `at`.
    async fn regenerate(&self, provider: &coral_provider::ProviderId, document: &str, at: i64) {
        self.modules
            .regenerate_workspace_provider(
                &change(self.user.clone(), at, '6'),
                &self.workspace,
                provider,
                &serde_json::from_str(document).expect("an OpenAPI document"),
            )
            .await;
    }

    /// The vector of each of `provider`'s Tools, with the Generation of its
    /// row, by Tool Key.
    async fn tool_embeddings(
        &self,
        provider: &coral_provider::ProviderId,
    ) -> BTreeMap<String, (String, Vec<f32>)> {
        let tools = self.vectors_of(provider).await;
        self.embeddings()
            .await
            .into_iter()
            .filter(|(doc_id, _)| tools.contains(doc_id))
            .map(|(doc_id, vector)| {
                let (generation, tool) = doc_id
                    .split_once(':')
                    .expect("a Provider Tool's document id");
                (tool.to_owned(), (generation.to_owned(), vector))
            })
            .collect()
    }

    /// Renames `provider` to `display_name` in the catalog.
    async fn rename(&self, provider: &coral_provider::ProviderId, display_name: &str) {
        self.modules
            .providers
            .rename_provider(
                &change(self.user.clone(), 70, '7'),
                &self.workspace,
                provider,
                &IfMatch::any(),
                coral_provider::ProviderDisplayName::parse(display_name).unwrap(),
            )
            .await
            .unwrap()
            .skip_search();
    }

    /// The document id of the [`BROKEN`] Tool.
    async fn broken_doc_id(&self) -> String {
        let rows: Vec<(String,)> = self
            .modules
            .search
            .database
            .fetch_all(
                Query::select()
                    .column(Alias::new("doc_id"))
                    .from(Alias::new("search_entities"))
                    .and_where(Expr::col(Alias::new("workspace_id")).eq(self.workspace.to_string()))
                    .and_where(Expr::col(Alias::new("title")).eq(BROKEN))
                    .to_owned(),
            )
            .await
            .expect("read the broken row");
        let [(doc_id,)] = rows.as_slice() else {
            panic!("one broken row: {rows:?}");
        };
        doc_id.clone()
    }

    /// The document id of the Workspace's newest row titled `title`.
    async fn newest_doc_id_titled(&self, title: &str) -> String {
        let rows: Vec<(String,)> = self
            .modules
            .search
            .database
            .fetch_all(
                Query::select()
                    .column(Alias::new("doc_id"))
                    .from(Alias::new("search_entities"))
                    .and_where(Expr::col(Alias::new("workspace_id")).eq(self.workspace.to_string()))
                    .and_where(Expr::col(Alias::new("title")).eq(title))
                    .order_by(Alias::new("indexed_at_unix_nanos"), sea_query::Order::Desc)
                    .to_owned(),
            )
            .await
            .expect("read the rows by title");
        rows.into_iter()
            .next()
            .map_or_else(|| panic!("a row titled {title:?}"), |(doc_id,)| doc_id)
    }

    /// Writes `vector` for the row of `doc_id` as a Coral older than vector
    /// reuse writes it: its statements, with the columns it knows.
    async fn write_as_an_older_coral(&self, doc_id: &str, vector: &[f32]) {
        let workspace = &self.workspace;
        let revision = MANIFEST.embedder_revision();
        let vector = crate::storage::vector_literal(vector);
        let sql = match self.modules.search.database.system() {
            coral_db::DatabaseSystem::Postgres => format!(
                "INSERT INTO search_entity_embeddings (\
                    workspace_id, doc_id, kind, provider_id, provider_generation_id, tool_key, \
                    embedder_revision, source_indexed_at_unix_nanos, embedding) \
                 SELECT s.workspace_id, s.doc_id, s.kind, s.provider_id, \
                    s.provider_generation_id, s.tool_key, '{revision}', \
                    s.indexed_at_unix_nanos, '{vector}'::halfvec \
                 FROM search_entities s \
                 WHERE s.workspace_id = '{workspace}' AND s.doc_id = '{doc_id}' \
                 ON CONFLICT (workspace_id, doc_id) DO UPDATE SET \
                    kind = EXCLUDED.kind, \
                    provider_id = EXCLUDED.provider_id, \
                    provider_generation_id = EXCLUDED.provider_generation_id, \
                    tool_key = EXCLUDED.tool_key, \
                    embedder_revision = EXCLUDED.embedder_revision, \
                    source_indexed_at_unix_nanos = EXCLUDED.source_indexed_at_unix_nanos, \
                    embedding = EXCLUDED.embedding \
                 WHERE search_entity_embeddings.embedder_revision <> EXCLUDED.embedder_revision \
                 OR search_entity_embeddings.source_indexed_at_unix_nanos \
                     <> EXCLUDED.source_indexed_at_unix_nanos"
            ),
            coral_db::DatabaseSystem::Sqlite => format!(
                "INSERT INTO search_entity_embeddings (\
                    workspace_id, doc_id, embedder_revision, source_indexed_at_unix_nanos) \
                 SELECT s.workspace_id, s.doc_id, '{revision}', s.indexed_at_unix_nanos \
                 FROM search_entities AS s \
                 WHERE s.workspace_id = '{workspace}' AND s.doc_id = '{doc_id}' \
                 ON CONFLICT (workspace_id, doc_id) DO UPDATE SET \
                    embedder_revision = excluded.embedder_revision, \
                    source_indexed_at_unix_nanos = excluded.source_indexed_at_unix_nanos \
                 WHERE search_entity_embeddings.embedder_revision <> excluded.embedder_revision \
                 OR search_entity_embeddings.source_indexed_at_unix_nanos \
                     <> excluded.source_indexed_at_unix_nanos; \
                 DELETE FROM search_entity_embedding_vectors WHERE rowid = (\
                     SELECT vector_rowid FROM search_entity_embeddings \
                     WHERE workspace_id = '{workspace}' AND doc_id = '{doc_id}'); \
                 INSERT INTO search_entity_embedding_vectors (rowid, embedding) VALUES ((\
                     SELECT vector_rowid FROM search_entity_embeddings \
                     WHERE workspace_id = '{workspace}' AND doc_id = '{doc_id}'), '{vector}')"
            ),
        };
        self.execute(&sql).await;
    }

    /// Makes each vector write fail while `refuse`, and lets them through
    /// again when not: a database fault.
    async fn refuse_vector_writes(&self, refuse: bool) {
        let sql = match (self.modules.search.database.system(), refuse) {
            (coral_db::DatabaseSystem::Postgres, true) => {
                "CREATE FUNCTION refuse_vector_write() RETURNS trigger LANGUAGE plpgsql AS \
                     $$ BEGIN RAISE EXCEPTION 'injected vector write fault'; END $$; \
                 CREATE TRIGGER refuse_vector_write \
                     BEFORE INSERT OR UPDATE ON search_entity_embeddings \
                     FOR EACH ROW EXECUTE FUNCTION refuse_vector_write()"
            }
            (coral_db::DatabaseSystem::Postgres, false) => {
                "DROP TRIGGER IF EXISTS refuse_vector_write ON search_entity_embeddings; \
                 DROP FUNCTION IF EXISTS refuse_vector_write()"
            }
            (coral_db::DatabaseSystem::Sqlite, true) => {
                "CREATE TRIGGER refuse_vector_insert BEFORE INSERT ON search_entity_embeddings \
                     BEGIN SELECT RAISE(ABORT, 'injected vector write fault'); END; \
                 CREATE TRIGGER refuse_vector_update BEFORE UPDATE ON search_entity_embeddings \
                     BEGIN SELECT RAISE(ABORT, 'injected vector write fault'); END"
            }
            (coral_db::DatabaseSystem::Sqlite, false) => {
                "DROP TRIGGER IF EXISTS refuse_vector_insert; \
                 DROP TRIGGER IF EXISTS refuse_vector_update"
            }
        };
        self.execute(sql).await;
    }

    async fn execute(&self, sql: &str) {
        self.modules
            .fixture
            .execute_fixture_sql(sql)
            .await
            .expect("change the vector table");
    }

    /// Asserts that each Search Entity of the Workspace has a vector of the
    /// current revision and of its row's stamp, and no other vector exists.
    async fn assert_every_vector_current(&self) {
        let revision = MANIFEST.embedder_revision().to_string();
        let expected = self
            .entity_stamps()
            .await
            .into_iter()
            .map(|(doc_id, stamp)| (doc_id, (revision.clone(), stamp)))
            .collect::<BTreeMap<_, _>>();
        assert!(!expected.is_empty(), "the Workspace has Search Entities");
        assert_eq!(self.vectors().await, expected);
    }

    /// An index whose Embedder embeds with `model`, ready at once.
    fn index_with(&self, model: &(impl Embed + Clone + 'static)) -> SearchIndex {
        self.modules.index_with(Arc::new(model.clone()))
    }

    /// An index whose Embedder waits until the test sends it a model.
    fn index_waiting_for_model(&self) -> (watch::Sender<ModelLoad>, SearchIndex) {
        let (model, slot) = watch::channel(ModelLoad::Pending);
        let index = SearchIndex::new(self.modules.search.database.clone(), RECONCILE_INTERVAL)
            .with_embedder(slot);
        (model, index)
    }

    /// Each Search Entity of the Workspace with its source stamp.
    async fn entity_stamps(&self) -> BTreeMap<String, i64> {
        let rows: Vec<(String, i64)> = self
            .modules
            .search
            .database
            .fetch_all(
                Query::select()
                    .columns(["doc_id", "indexed_at_unix_nanos"].map(Alias::new))
                    .from(Alias::new("search_entities"))
                    .and_where(Expr::col(Alias::new("workspace_id")).eq(self.workspace.to_string()))
                    .to_owned(),
            )
            .await
            .expect("read the Search Entities");
        rows.into_iter().collect()
    }

    /// Each vector of the Workspace with its Embedder revision and source
    /// stamp.
    async fn vectors(&self) -> BTreeMap<String, (String, i64)> {
        self.vectors_in(&self.workspace).await
    }

    /// Each vector of `workspace` with its Embedder revision and source
    /// stamp.
    /// On `SQLite`, a key row counts only beside its vector.
    async fn vectors_in(&self, workspace: &WorkspaceId) -> BTreeMap<String, (String, i64)> {
        let sql = match self.modules.search.database.system() {
            coral_db::DatabaseSystem::Postgres => {
                "SELECT doc_id, embedder_revision, source_indexed_at_unix_nanos \
                 FROM search_entity_embeddings WHERE workspace_id = $1"
            }
            coral_db::DatabaseSystem::Sqlite => {
                "SELECT k.doc_id, k.embedder_revision, k.source_indexed_at_unix_nanos \
                 FROM search_entity_embeddings AS k \
                 JOIN search_entity_embedding_vectors AS v ON v.rowid = k.vector_rowid \
                 WHERE k.workspace_id = ?1"
            }
        };
        let rows: Vec<(String, String, i64)> =
            self.read(sql, vec![workspace.to_string().into()]).await;
        rows.into_iter()
            .map(|(doc_id, revision, stamp)| (doc_id, (revision, stamp)))
            .collect()
    }

    /// Each vector of the Workspace, by document id.
    async fn embeddings(&self) -> BTreeMap<String, Vec<f32>> {
        let sql = match self.modules.search.database.system() {
            coral_db::DatabaseSystem::Postgres => {
                "SELECT doc_id, embedding::text FROM search_entity_embeddings \
                 WHERE workspace_id = $1"
            }
            coral_db::DatabaseSystem::Sqlite => {
                "SELECT k.doc_id, vec_to_json(v.embedding) \
                 FROM search_entity_embeddings AS k \
                 JOIN search_entity_embedding_vectors AS v ON v.rowid = k.vector_rowid \
                 WHERE k.workspace_id = ?1"
            }
        };
        let rows: Vec<(String, String)> = self
            .read(sql, vec![self.workspace.to_string().into()])
            .await;
        rows.into_iter()
            .map(|(doc_id, vector)| {
                let values = vector
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .split(',')
                    .map(|value| value.parse::<f32>().expect("a vector value"))
                    .collect();
                (doc_id, values)
            })
            .collect()
    }

    /// How many stored vectors no key row names: on `SQLite`, rows of the
    /// sqlite-vec table whose key row is gone. Postgres keeps each vector in
    /// its key row.
    async fn unkeyed_vectors(&self) -> i64 {
        match self.modules.search.database.system() {
            coral_db::DatabaseSystem::Postgres => 0,
            coral_db::DatabaseSystem::Sqlite => {
                let rows: Vec<(i64,)> = self
                    .read(
                        "SELECT count(*) FROM search_entity_embedding_vectors \
                         WHERE rowid NOT IN (SELECT vector_rowid FROM search_entity_embeddings)",
                        Vec::new(),
                    )
                    .await;
                rows.first().map_or(0, |(count,)| *count)
            }
        }
    }

    /// Rows of one statement written for the test database's backend.
    async fn read<T>(&self, sql: &str, values: Vec<sea_query::Value>) -> Vec<T>
    where
        T: Send + Unpin,
        for<'row> T: sqlx::FromRow<'row, sqlx::sqlite::SqliteRow>,
        for<'row> T: sqlx::FromRow<'row, sqlx::postgres::PgRow>,
    {
        let mut transaction = self
            .modules
            .search
            .database
            .begin_read_snapshot()
            .await
            .expect("begin a read");
        let rows = transaction
            .fetch_all_sql(sql, sea_query::Values(values))
            .await
            .expect("read the vector tables");
        transaction.rollback().await.expect("end the read");
        rows
    }
}
