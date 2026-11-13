//! The Embedder: it gives each Search Entity a vector of the current
//! Embedder revision and source stamp, in the background, on Postgres.
//!
//! Every test indexes through `SearchIndex`, waits for the index to settle and
//! for the Embedder to drain, then reads the vector table beside the entity
//! table. The model is a fixed-vector stand-in, so no test loads the real one.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use coral_embed::{Embed, EmbedError, MANIFEST};
use coral_telemetry::testing::{MetricCapture, SpanCapture, attribute};
use coral_workspace::{AuthenticatedUser, WorkspaceId};
use sea_query::{Alias, Expr, ExprTrait as _, Query};
use tokio::sync::watch;
use wiremock::MockServer;

use super::{TestModules, change, organization_user};
use crate::{ModelLoad, SearchIndex};

const USER: &str = "user:search-embedder";

const RECONCILE_INTERVAL: std::time::Duration = std::time::Duration::from_mins(5);

#[tokio::test]
async fn the_embedder_embeds_the_text_the_bench_measured() {
    coral_test_support::database_only!(Postgres);
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
    coral_test_support::database_only!(Postgres);
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
    coral_test_support::database_only!(Postgres);
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
    let _background = Background(index.spawn_reconcile_task());
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
    coral_test_support::database_only!(Postgres);
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
    let _background = Background(index.spawn_reconcile_task());
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
async fn two_embedders_on_the_same_rows_write_each_vector_once() {
    coral_test_support::database_only!(Postgres);
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
    coral_test_support::database_only!(Postgres);
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let ledger = world.publish_ledger().await;
    let model = FixedVectors::default();
    let index = world.index_with(&model);
    let _background = Background(index.spawn_reconcile_task());
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
        )
        .await
        .expect("delete the ledger")
        .skip_search();
    index.reconcile().await;

    assert_eq!(world.vectors().await, tracer_vectors);
}

#[tokio::test]
async fn a_batch_that_fails_with_the_model_or_the_database_down_skips_nothing_and_the_next_pass_embeds_it()
 {
    coral_test_support::database_only!(Postgres);
    for down in ["the model", "the database"] {
        let upstream = MockServer::start().await;
        let world = World::publish_tracer(&upstream).await;
        let ledger = world.publish_ledger().await;
        let model = FixedVectors::default();
        if down == "the model" {
            model.fail_texts_with("");
        } else {
            world.execute(REFUSE_VECTOR_WRITES).await;
        }
        let index = world.index_with(&model);
        let _background = Background(index.spawn_reconcile_task());
        for provider in [&world.tracer, &ledger] {
            index.index_active_generation(world.workspace.clone(), provider.clone());
        }
        index.settled().await;
        index.embedded().await;
        assert_eq!(world.vectors().await, BTreeMap::new(), "{down} is down");

        model.fail_texts_with_nothing();
        world.execute(ALLOW_VECTOR_WRITES).await;
        index.reconcile().await;
        index.embedded().await;

        world.assert_every_vector_current().await;
    }
}

/// A database fault: each vector write fails.
const REFUSE_VECTOR_WRITES: &str = "\
    CREATE FUNCTION refuse_vector_write() RETURNS trigger LANGUAGE plpgsql AS \
        $$ BEGIN RAISE EXCEPTION 'injected vector write fault'; END $$; \
    CREATE TRIGGER refuse_vector_write BEFORE INSERT OR UPDATE ON search_entity_embeddings \
        FOR EACH ROW EXECUTE FUNCTION refuse_vector_write()";

const ALLOW_VECTOR_WRITES: &str = "\
    DROP TRIGGER IF EXISTS refuse_vector_write ON search_entity_embeddings; \
    DROP FUNCTION IF EXISTS refuse_vector_write()";

#[tokio::test]
async fn a_row_that_always_fails_is_skipped_until_its_row_changes_and_holds_back_no_other_row() {
    coral_test_support::database_only!(Postgres);
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
    let _background = Background(index.spawn_reconcile_task());
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
    coral_test_support::database_only!(Postgres);
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let metrics = MetricCapture::default();
    let _measured = tracing::subscriber::set_default(metrics.subscriber());
    let (model, index) = world.index_waiting_for_model();
    let _background = Background(index.spawn_reconcile_task());
    index.index_active_generation(world.workspace.clone(), world.tracer.clone());
    index.settled().await;
    let backlog = || gauge(&metrics, "coral.search.embed.backlog");
    observed(|| backlog().is_some_and(|stale| stale >= 1.0)).await;

    model.send_replace(ModelLoad::Ready(Arc::new(FixedVectors::default())));
    index.embedded().await;

    observed(|| backlog() == Some(0.0)).await;
}

#[tokio::test]
async fn a_row_on_the_skip_list_shows_in_the_skipped_rows_and_its_batch_as_failed() {
    coral_test_support::database_only!(Postgres);
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

    observed(|| {
        gauge(&metrics, "coral.search.embed.skipped") == Some(1.0)
            && gauge(&metrics, "coral.search.embed.backlog") == Some(1.0)
    })
    .await;
    let outcomes = metrics
        .named("coral.search.embed.batch.duration")
        .into_iter()
        .filter_map(|point| point.attributes.get("outcome").cloned())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        outcomes,
        ["failed".to_owned(), "ok".to_owned()].into(),
        "the batch with the broken row failed, the rows tried alone after it were written"
    );
}

/// The last value of the gauge `name`, when one was observed.
fn gauge(metrics: &MetricCapture, name: &str) -> Option<f64> {
    metrics.values(name).last().copied()
}

/// Waits until `ready` holds: the backlog observer samples in the
/// background.
async fn observed(ready: impl Fn() -> bool) {
    let waited = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !ready() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(waited.is_ok(), "the backlog observation converges");
}

#[tokio::test]
async fn a_failing_row_alone_in_its_batch_is_skipped_and_holds_back_no_other_workspace() {
    coral_test_support::database_only!(Postgres);
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
    let _background = Background(index.spawn_reconcile_task());
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
    coral_test_support::database_only!(Postgres);
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

#[tokio::test]
async fn on_sqlite_the_embedder_never_runs() {
    coral_test_support::database_only!(Sqlite);
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let model = FixedVectors::default();
    let index = world.index_with(&model);
    let _background = Background(index.spawn_reconcile_task());

    index.index_active_generation(world.workspace.clone(), world.tracer.clone());
    index.settled().await;
    index.embedded().await;

    assert!(
        !world.entity_stamps().await.is_empty(),
        "the index has rows"
    );
    assert_eq!(model.documents(), Vec::<String>::new());
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

    fn vector(text: &str) -> Vec<f32> {
        let mut vector = vec![0.0; MANIFEST.dimension];
        let slot = text.len() % MANIFEST.dimension;
        if let Some(value) = vector.get_mut(slot) {
            *value = 1.0;
        }
        vector
    }
}

impl Embed for FixedVectors {
    fn embed_query(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
        Ok(Self::vector(text))
    }

    fn embed_document(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
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
            "WHERE doc_id = (SELECT min(doc_id) FROM search_entity_embeddings \
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
                    .and_where(Expr::col(Alias::new("provider_id")).eq(provider.to_string()))
                    .to_owned(),
            )
            .await
            .expect("read the vectors of a Provider");
        rows.into_iter().map(|(doc_id,)| doc_id).collect()
    }

    /// Renames `provider` to `display_name` in the catalog.
    async fn rename(&self, provider: &coral_provider::ProviderId, display_name: &str) {
        self.modules
            .providers
            .rename_provider(
                &change(self.user.clone(), 70, '7'),
                &self.workspace,
                provider,
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
    async fn vectors_in(&self, workspace: &WorkspaceId) -> BTreeMap<String, (String, i64)> {
        let rows: Vec<(String, String, i64)> = self
            .modules
            .search
            .database
            .fetch_all(
                Query::select()
                    .columns(
                        [
                            "doc_id",
                            "embedder_revision",
                            "source_indexed_at_unix_nanos",
                        ]
                        .map(Alias::new),
                    )
                    .from(Alias::new("search_entity_embeddings"))
                    .and_where(Expr::col(Alias::new("workspace_id")).eq(workspace.to_string()))
                    .to_owned(),
            )
            .await
            .expect("read the vectors");
        rows.into_iter()
            .map(|(doc_id, revision, stamp)| (doc_id, (revision, stamp)))
            .collect()
    }
}
