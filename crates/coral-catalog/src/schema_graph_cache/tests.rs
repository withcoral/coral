use coral_outbound::DestinationPolicy;
use coral_test_support::TestDatabase;

use super::*;
use crate::{
    MaterializationOutcome, McpDiscoverySnapshot, OpenApiDocument, ProviderSetupInput,
    ProviderSetupInputDigest,
};

/// One published Generation, with the graph it stores and the weight the
/// cache gives it.
struct Published {
    id: ProviderGenerationId,
    graph: SchemaGraph,
    weight_bytes: usize,
}

fn http_input() -> ProviderSetupInput {
    OpenApiDocument::parse(include_str!(
        "../../tests/fixtures/openapi-http-corpus.yaml"
    ))
    .expect("HTTP input")
    .into()
}

/// A smaller graph than [`http_input`]'s.
fn mcp_input() -> ProviderSetupInput {
    McpDiscoverySnapshot::parse(include_str!("../../tests/fixtures/mcp-tool-corpus.json"))
        .expect("MCP input")
        .into()
}

/// Publishes one new Generation of each input, in order.
async fn publish(fixture: &TestDatabase, inputs: &[ProviderSetupInput]) -> Vec<Published> {
    let database = fixture.database();
    let mut transaction = database.begin().await.unwrap();
    let mut published = Vec::new();
    for input in inputs {
        let MaterializationOutcome::Ready(prepared) =
            crate::materialize(DestinationPolicy::default(), input, &[])
        else {
            panic!("the fixture materializes");
        };
        let graph = prepared.schema_graph().clone();
        // The cache weighs the graph it parses from storage.
        let weight_bytes = weight_bytes(
            &SchemaGraph::from_storage_json(&graph.to_storage_json().unwrap()).unwrap(),
        );
        let id = crate::publish_generation_in_transaction(
            &mut transaction,
            input,
            &ProviderSetupInputDigest::of_setup_input(input),
            prepared,
            1,
        )
        .await
        .unwrap();
        published.push(Published {
            id,
            graph,
            weight_bytes,
        });
    }
    transaction.commit().await.unwrap();
    published
}

/// Stores `json` as the schema graph of `only`, or of every Generation.
async fn store_graph_json(fixture: &TestDatabase, only: Option<&ProviderGenerationId>, json: &str) {
    let json = json.replace('\'', "''");
    let scope = only.map_or_else(String::new, |id| format!(" WHERE id = '{id}'"));
    let changed = fixture
        .execute_fixture_sql(&format!(
            "UPDATE provider_generations SET schema_graph_json = '{json}'{scope}"
        ))
        .await
        .unwrap();
    assert!(changed > 0, "the rewrite matched a Generation");
}

/// The cached read, in a transaction of its own.
async fn cached(
    fixture: &TestDatabase,
    cache: &SchemaGraphCache,
    id: &ProviderGenerationId,
) -> Result<Arc<SchemaGraph>, Error> {
    let database = fixture.database();
    let mut transaction = database.begin_read_snapshot().await.unwrap();
    let graph = cache
        .schema_graph_in_transaction(&mut transaction, id)
        .await;
    transaction.rollback().await.unwrap();
    graph
}

/// The read that no cache serves, in a transaction of its own.
async fn uncached(fixture: &TestDatabase, id: &ProviderGenerationId) -> Result<SchemaGraph, Error> {
    let database = fixture.database();
    let mut transaction = database.begin_read_snapshot().await.unwrap();
    let graph = crate::schema_graph_in_transaction(&mut transaction, id).await;
    transaction.rollback().await.unwrap();
    graph
}

fn is_corrupt_graph<T>(result: &Result<T, Error>) -> bool {
    matches!(
        result,
        Err(Error::CorruptData(message))
            if message == "Provider schema graph is not valid canonical JSON"
    )
}

// Catches: a cached graph that still costs a read of its row, and a reader
// that does not use the cache losing the corruption check.
#[tokio::test]
async fn a_cached_graph_is_served_without_reading_its_row() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let generation = publish(&fixture, &[http_input()])
        .await
        .pop()
        .expect("one Generation");
    let cache = SchemaGraphCache::with_default_budget();

    let first = cached(&fixture, &cache, &generation.id).await.unwrap();
    assert_eq!(*first, generation.graph);

    // The stored graph is corrupted after this Coral Instance cached it.
    store_graph_json(&fixture, Some(&generation.id), "not a graph").await;
    let second = cached(&fixture, &cache, &generation.id).await.unwrap();
    assert!(
        Arc::ptr_eq(&first, &second),
        "the cache serves the graph it holds"
    );
    assert!(
        Arc::ptr_eq(
            &first,
            &cached(&fixture, &cache.clone(), &generation.id)
                .await
                .unwrap()
        ),
        "a clone is the same cache"
    );
    let direct = uncached(&fixture, &generation.id).await;
    assert!(is_corrupt_graph(&direct), "{direct:?}");
}

// Catches: a failed load leaving an entry behind, so that a corrupt graph is
// served, or its error is served after the row is whole again.
#[tokio::test]
async fn a_corrupt_stored_graph_is_reported_and_not_cached() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let generation = publish(&fixture, &[http_input()])
        .await
        .pop()
        .expect("one Generation");
    let cache = SchemaGraphCache::with_default_budget();
    store_graph_json(&fixture, Some(&generation.id), "not a graph").await;

    for attempt in ["first", "second"] {
        let graph = cached(&fixture, &cache, &generation.id).await;
        assert!(is_corrupt_graph(&graph), "{attempt}: {graph:?}");
    }

    let stored = generation.graph.to_storage_json().unwrap();
    store_graph_json(&fixture, Some(&generation.id), &stored).await;
    assert_eq!(
        *cached(&fixture, &cache, &generation.id).await.unwrap(),
        generation.graph
    );
}

// Catches: a graph that the cache keeps having no place in the cache's store,
// so that every Tool call on it bundles and compiles its response schema,
// and a graph read without the cache keeping validators that no budget
// counts.
#[tokio::test]
async fn a_cached_graph_keeps_its_validators_and_a_graph_read_without_the_cache_keeps_none() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let generation = publish(&fixture, &[http_input()])
        .await
        .pop()
        .expect("one Generation");
    let cache = SchemaGraphCache::with_default_budget();
    let (root, _schema) = generation.graph.schemas().next().expect("a schema");
    let direction = crate::types::SchemaValidationDirection::Output;
    let builds = |graph: &SchemaGraph| {
        let built = std::cell::Cell::new(0);
        for _use in 0..2 {
            let validator = graph.validators().validator(root, direction, || {
                built.set(built.get() + 1);
                Some(serde_json::json!({"type": "object"}))
            });
            assert!(validator.is_some());
        }
        built.get()
    };

    let kept = cached(&fixture, &cache, &generation.id).await.unwrap();
    assert_eq!(builds(&kept), 1, "the cached graph builds a validator once");
    let again = cached(&fixture, &cache, &generation.id).await.unwrap();
    assert_eq!(builds(&again), 0, "a later call finds it kept");

    let direct = uncached(&fixture, &generation.id).await.unwrap();
    assert_eq!(builds(&direct), 2, "a graph read without the cache");
    assert_eq!(builds(&generation.graph), 2, "a materialized graph");
    assert_eq!(
        builds(&SchemaGraph::clone(&kept)),
        2,
        "a copy of the cached graph"
    );
}

struct BudgetCase {
    name: &'static str,
    /// The budget, from the weights of an HTTP graph and the smaller MCP graph.
    budget_bytes: fn(usize, usize) -> usize,
    /// Cached reads in order: 0, 1 and 2 are HTTP Generations, 3 and 4 MCP ones.
    loads: &'static [usize],
    kept: &'static [usize],
}

const BUDGET_CASES: [BudgetCase; 8] = [
    BudgetCase {
        name: "two graphs within the budget",
        budget_bytes: |http, _mcp| 2 * http,
        loads: &[0, 1],
        kept: &[0, 1],
    },
    BudgetCase {
        name: "a second graph over the budget evicts the first",
        budget_bytes: |http, _mcp| 2 * http - 1,
        loads: &[0, 1],
        kept: &[1],
    },
    BudgetCase {
        name: "a graph read again outlives one that was not",
        budget_bytes: |http, _mcp| 2 * http,
        loads: &[0, 1, 0, 2],
        kept: &[0, 2],
    },
    BudgetCase {
        name: "one graph evicts as many as it needs room for",
        budget_bytes: |http, mcp| http + mcp - 1,
        loads: &[3, 4, 0],
        kept: &[0],
    },
    BudgetCase {
        name: "a graph over the whole budget evicts the rest and is kept while most recent",
        budget_bytes: |http, _mcp| http - 1,
        loads: &[3, 0, 0],
        kept: &[0],
    },
    BudgetCase {
        name: "a graph over the whole budget leaves when another graph is used",
        budget_bytes: |http, _mcp| http - 1,
        loads: &[0, 3],
        kept: &[3],
    },
    BudgetCase {
        name: "a graph over the whole budget replaces another",
        budget_bytes: |http, _mcp| http - 1,
        loads: &[0, 1],
        kept: &[1],
    },
    BudgetCase {
        name: "a budget of zero keeps only the most recent graph",
        budget_bytes: |_http, _mcp| 0,
        loads: &[3, 4],
        kept: &[4],
    },
];

// Catches: a cache that grows past its budget, one that evicts an entry still
// in use before an idle one, and one that the largest graph makes useless:
// that does not keep a graph over its whole budget while it is the one in
// use, or keeps it once another graph is.
#[tokio::test]
async fn the_byte_budget_evicts_the_entries_used_longest_ago_but_never_the_newest() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    for case in BUDGET_CASES {
        let generations = publish(
            &fixture,
            &[
                http_input(),
                http_input(),
                http_input(),
                mcp_input(),
                mcp_input(),
            ],
        )
        .await;
        let generation = |index: usize| {
            generations
                .get(index)
                .expect("the case names a published Generation")
        };
        let (http, mcp) = (generation(0).weight_bytes, generation(3).weight_bytes);
        assert!(mcp < http, "the MCP graph is the smaller one");
        let cache = SchemaGraphCache::new((case.budget_bytes)(http, mcp));

        for &load in case.loads {
            let generation = generation(load);
            assert_eq!(
                *cached(&fixture, &cache, &generation.id).await.unwrap(),
                generation.graph,
                "{}: every read is served",
                case.name
            );
        }

        // Only a graph the cache kept can still be read.
        store_graph_json(&fixture, None, "not a graph").await;
        for (index, generation) in generations.iter().enumerate() {
            let graph = cached(&fixture, &cache, &generation.id).await;
            if case.kept.contains(&index) {
                assert!(
                    graph.is_ok_and(|kept| *kept == generation.graph),
                    "{}: Generation {index} is kept",
                    case.name
                );
            } else {
                assert!(
                    is_corrupt_graph(&graph),
                    "{}: Generation {index} is not kept: {:?}",
                    case.name,
                    graph.map(|_graph| "a graph")
                );
            }
        }
    }
}

/// A graph of one schema, `schema`.
fn one_schema_graph(schema: Value) -> SchemaGraph {
    let source = "#/components/schemas/Only";
    let id = SchemaId::for_source(source);
    SchemaGraph::new(
        crate::SchemaSemantics::JsonSchema2020_12,
        BTreeMap::from([(id.clone(), schema)]),
        BTreeMap::from([(source.to_string(), id)]),
    )
    .unwrap()
}

// Catches: a weight taken from the length of the stored JSON, which weighs a
// graph of many small values, such as Microsoft Graph's, like one long string
// and so keeps far more than the budget or nothing of the largest Provider.
#[test]
fn a_graph_weighs_what_its_values_hold_not_its_stored_length() {
    let properties = (0..200)
        .map(|index| {
            (
                format!("p{index:03}"),
                serde_json::json!({"type": "string"}),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    let structure = one_schema_graph(serde_json::json!({"properties": properties}));
    let stored_bytes = structure.to_storage_json().unwrap().len();
    let padding = stored_bytes
        - one_schema_graph(serde_json::json!({"description": ""}))
            .to_storage_json()
            .unwrap()
            .len();
    let text = one_schema_graph(serde_json::json!({"description": "a".repeat(padding)}));
    assert_eq!(text.to_storage_json().unwrap().len(), stored_bytes);

    let parsed = |graph: &SchemaGraph| {
        SchemaGraph::from_storage_json(&graph.to_storage_json().unwrap()).unwrap()
    };
    let (structure, text) = (
        weight_bytes(&parsed(&structure)),
        weight_bytes(&parsed(&text)),
    );
    assert!(
        text >= stored_bytes,
        "a graph weighs at least its text: {text} for {stored_bytes} stored"
    );
    assert!(
        structure > 8 * text,
        "200 small objects weigh {structure}; one string as long when stored weighs {text}"
    );
}

// Catches: the entry slots an object is weighed at drifting from what its
// index map really holds room for, as an upgrade of `indexmap` could cause.
// The slots are about nine tenths of the room an object is weighed at, and
// every weight rests on them: measure the graphs again before changing
// `object_room`.
//
// Not checked: the bucket count of the index table. Neither `serde_json` nor
// `indexmap` reports it, so `object_room` derives it from the same doubling.
#[test]
fn an_object_is_weighed_at_the_entries_its_index_map_holds_room_for() {
    assert_eq!(object_room(0), (0, 0));
    let mut object = indexmap::IndexMap::<String, Value>::new();
    for len in 1..=1024 {
        object.insert(len.to_string(), Value::Null);
        let (slots, _buckets) = object_room(len);
        assert_eq!(slots, object.capacity(), "{len} entries");
    }
}

/// A graph of one schema that no other graph here equals.
fn titled_graph(title: &str) -> SchemaGraph {
    one_schema_graph(serde_json::json!({"title": title}))
}

// Catches: a load that lost the race for a Generation replacing the entry
// that a caller already holds, weighing the Generation twice, or dropping
// its own graph inside the lock.
#[test]
fn a_second_graph_for_a_cached_generation_is_released_and_the_first_stays() {
    let (budget, weight) = (25, 10);
    let mut entries = Entries::default();
    let [contended, idle, next] = [(); 3].map(|()| ProviderGenerationId::generate());
    let first = Arc::new(titled_graph("first"));
    let other = Arc::new(titled_graph("other"));
    let (kept, released) = entries.keep(&contended, Arc::clone(&first), weight, budget);
    assert!(Arc::ptr_eq(&kept, &first));
    assert_eq!(released.len(), 0, "the first graph displaces nothing");
    let (_kept, released) = entries.keep(&idle, Arc::clone(&other), weight, budget);
    assert_eq!(released.len(), 0, "both graphs are within the budget");

    // A concurrent load of the first Generation finishes late, with its own parse.
    let late = Arc::new(titled_graph("late"));
    let (kept, released) = entries.keep(&contended, Arc::clone(&late), weight, budget);
    assert!(
        Arc::ptr_eq(&kept, &first),
        "the first graph is the one in use"
    );
    assert!(
        matches!(&released[..], [only] if Arc::ptr_eq(only, &late)),
        "the caller drops the late graph, and nothing else"
    );
    assert_eq!(entries.weight_bytes, 2 * weight);
    assert_eq!(
        (entries.by_generation.len(), entries.by_last_use.len()),
        (2, 2),
        "each entry is in the use order once"
    );

    // The late load used the entry as a hit does, so the idle one is evicted first.
    let (_kept, released) = entries.keep(&next, Arc::new(titled_graph("next")), weight, budget);
    assert!(matches!(&released[..], [only] if Arc::ptr_eq(only, &other)));
    assert!(entries.by_generation.contains_key(&contended));
}

// Catches: a load returning the graph it parsed when another load cached the
// Generation first, which leaves two graphs of one Generation in use.
#[test]
fn a_load_that_finds_its_generation_cached_returns_the_cached_graph() {
    let cache = SchemaGraphCache::with_default_budget();
    let generation = ProviderGenerationId::generate();
    let first = cache.keep_loaded(&generation, titled_graph("first"));
    assert_eq!(*first, titled_graph("first"));
    let late = cache.keep_loaded(&generation, titled_graph("late"));
    assert!(Arc::ptr_eq(&first, &late));
}

/// What a test sees of the loads of an observed cache, and the hold it has
/// on each parse.
struct Loads {
    /// One signal for each caller that found its Generation not cached, sent
    /// before it waits for its turn to load.
    awaiting_turn: tokio::sync::mpsc::UnboundedReceiver<()>,
    /// One signal for each parse, sent before it starts: where it runs.
    parsing: tokio::sync::mpsc::UnboundedReceiver<ParseThread>,
    /// Each parse on a thread of its own waits here for one message before
    /// it starts.
    release: std::sync::mpsc::Sender<()>,
}

/// The thread a parse runs on.
#[derive(Debug, PartialEq)]
enum ParseThread {
    /// A thread of the load's own.
    Own,
    /// A thread of the Tokio runtime: one of its workers, or one of its
    /// blocking pool.
    Runtime,
}

struct LoadSignals {
    awaiting_turn: tokio::sync::mpsc::UnboundedSender<()>,
    parsing: tokio::sync::mpsc::UnboundedSender<ParseThread>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}

impl LoadObserver for LoadSignals {
    fn awaiting_turn(&self) {
        let _test_gone = self.awaiting_turn.send(()).is_err();
    }

    fn before_parse(&self) {
        if tokio::runtime::Handle::try_current().is_ok() {
            // Not held: on the test's own worker, the test could not let go.
            let _test_gone = self.parsing.send(ParseThread::Runtime).is_err();
            return;
        }
        let _test_gone = self.parsing.send(ParseThread::Own).is_err();
        // Blocks the load's own thread, which runs no Tokio task. A test
        // that is gone has dropped the sender, and holds nothing.
        let release = self.release.lock().unwrap_or_else(PoisonError::into_inner);
        let _test_gone = release.recv().is_err();
    }
}

/// A cache whose loads the test observes and holds.
fn observed_cache() -> (SchemaGraphCache, Loads) {
    let (awaiting_turn, awaiting_turn_seen) = tokio::sync::mpsc::unbounded_channel();
    let (parsing, parsing_seen) = tokio::sync::mpsc::unbounded_channel();
    let (release, released) = std::sync::mpsc::channel();
    let cache = SchemaGraphCache::observed(
        SchemaGraphCache::DEFAULT_BUDGET_BYTES,
        Arc::new(LoadSignals {
            awaiting_turn,
            parsing,
            release: Mutex::new(released),
        }),
    );
    let loads = Loads {
        awaiting_turn: awaiting_turn_seen,
        parsing: parsing_seen,
        release,
    };
    (cache, loads)
}

/// The cached read as a task of its own, in a read snapshot of its own, as a
/// Tool call makes it.
fn spawn_cached(
    fixture: &TestDatabase,
    cache: &SchemaGraphCache,
    id: &ProviderGenerationId,
) -> tokio::task::JoinHandle<Result<Arc<SchemaGraph>, Error>> {
    let (database, cache, id) = (fixture.database(), cache.clone(), id.clone());
    tokio::spawn(async move {
        let mut transaction = database.begin_read_snapshot().await.unwrap();
        let graph = cache
            .schema_graph_in_transaction(&mut transaction, &id)
            .await;
        transaction.rollback().await.unwrap();
        graph
    })
}

/// What the stored row holds.
#[derive(Clone, Copy, PartialEq)]
enum Row {
    Whole,
    Corrupt,
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Outcome {
    Graph,
    CorruptGraph,
    Canceled,
}

struct ConcurrentCase {
    name: &'static str,
    /// The row when the first caller reads it.
    first_read: Row,
    /// The row from then on: the test rewrites it while the first parse is
    /// held, after every other caller has begun to wait for its turn.
    later_reads: Row,
    /// Whether the first caller is canceled while its parse is held.
    cancel_first: bool,
    /// What the first caller and each of the others end with.
    first: Outcome,
    others: Outcome,
    /// The parses that the callers cause together.
    parses: fn(usize) -> usize,
}

const CONCURRENT_CASES: [ConcurrentCase; 4] = [
    ConcurrentCase {
        name: "the callers share one load",
        first_read: Row::Whole,
        later_reads: Row::Corrupt,
        cancel_first: false,
        first: Outcome::Graph,
        others: Outcome::Graph,
        parses: |_callers| 1,
    },
    ConcurrentCase {
        name: "a canceled caller's load still serves the others",
        first_read: Row::Whole,
        later_reads: Row::Corrupt,
        cancel_first: true,
        first: Outcome::Canceled,
        others: Outcome::Graph,
        parses: |_callers| 1,
    },
    ConcurrentCase {
        name: "a failed load fails its caller alone, and the next caller loads",
        first_read: Row::Corrupt,
        later_reads: Row::Whole,
        cancel_first: false,
        first: Outcome::CorruptGraph,
        others: Outcome::Graph,
        parses: |_callers| 2,
    },
    ConcurrentCase {
        name: "each caller of a corrupt graph loads in its turn and fails",
        first_read: Row::Corrupt,
        later_reads: Row::Corrupt,
        cancel_first: false,
        first: Outcome::CorruptGraph,
        others: Outcome::CorruptGraph,
        parses: |callers| callers,
    },
];

// Catches: concurrent first calls that each read and parse the Generation, a
// waiting caller that reads the row again once the load it waited for has
// cached the graph, a load that fails or loses its caller leaving the others
// waiting, failing them too, or caching something, and a parse on a thread of
// the runtime, which then runs none of its other tasks.
#[tokio::test]
async fn concurrent_first_loads_of_a_generation_take_turns_and_share_the_first_graph() {
    const CALLERS: usize = 5;
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    for case in CONCURRENT_CASES {
        let generation = publish(&fixture, &[http_input()])
            .await
            .pop()
            .expect("one Generation");
        let whole = generation.graph.to_storage_json().unwrap();
        let store = |row: Row| {
            let json = match row {
                Row::Whole => whole.as_str(),
                Row::Corrupt => "not a graph",
            };
            store_graph_json(&fixture, Some(&generation.id), json)
        };
        let (cache, mut loads) = observed_cache();

        store(case.first_read).await;
        let first = spawn_cached(&fixture, &cache, &generation.id);
        assert_eq!(
            loads.parsing.recv().await,
            Some(ParseThread::Own),
            "{}: the first parse is held, on a thread of its own",
            case.name
        );
        let others = [(); CALLERS - 1].map(|()| spawn_cached(&fixture, &cache, &generation.id));
        for _caller in 0..CALLERS {
            loads.awaiting_turn.recv().await.expect("a caller waits");
        }
        if case.later_reads != case.first_read {
            store(case.later_reads).await;
        }
        if case.cancel_first {
            first.abort();
        }
        // Enough for every parse a broken cache could start.
        for _parse in 0..CALLERS {
            loads.release.send(()).expect("the cache is alive");
        }

        let outcome = |joined: Result<Result<Arc<SchemaGraph>, Error>, tokio::task::JoinError>| {
            match joined {
                Ok(Ok(graph)) => (Outcome::Graph, Some(graph)),
                Ok(graph @ Err(_)) => {
                    assert!(is_corrupt_graph(&graph), "{}: {graph:?}", case.name);
                    (Outcome::CorruptGraph, None)
                }
                Err(canceled) => {
                    assert!(canceled.is_cancelled(), "{}: {canceled}", case.name);
                    (Outcome::Canceled, None)
                }
            }
        };
        let (first, first_graph) = outcome(first.await);
        assert_eq!(first, case.first, "{}: the first caller", case.name);
        let mut shared = first_graph;
        for other in others {
            let (other, graph) = outcome(other.await);
            assert_eq!(other, case.others, "{}: another caller", case.name);
            if let (Some(shared), Some(graph)) = (&shared, &graph) {
                assert!(
                    Arc::ptr_eq(shared, graph),
                    "{}: every caller uses one graph",
                    case.name
                );
            }
            shared = shared.or(graph);
        }

        let mut parses = 1;
        while let Ok(thread) = loads.parsing.try_recv() {
            assert_eq!(thread, ParseThread::Own, "{}: a later parse", case.name);
            parses += 1;
        }
        assert_eq!(parses, (case.parses)(CALLERS), "{}: parses", case.name);
        assert_eq!(
            cache.shared.turns().len(),
            0,
            "{}: no turn outlives its callers",
            case.name
        );

        // A failure cached nothing: the next call reads the row as it is now.
        store(Row::Whole).await;
        loads.release.send(()).expect("the cache is alive");
        let next = cached(&fixture, &cache, &generation.id).await.unwrap();
        assert_eq!(*next, generation.graph, "{}", case.name);
        if let Some(shared) = &shared {
            assert!(
                Arc::ptr_eq(shared, &next),
                "{}: the shared graph is the cached one",
                case.name
            );
        }
    }
}
