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
    let cache = SchemaGraphCache::new(SchemaGraphCache::DEFAULT_BUDGET_BYTES);

    let first = cached(&fixture, &cache, &generation.id).await.unwrap();
    assert_eq!(*first, generation.graph);

    // The stored graph is corrupted after this process cached it.
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
    let cache = SchemaGraphCache::new(SchemaGraphCache::DEFAULT_BUDGET_BYTES);
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

// Catches: the room an object is weighed at drifting from what its index map
// really grows to, as an upgrade of `indexmap` could cause. Every weight
// rests on it: measure the graphs again before changing `object_room`.
#[test]
fn an_object_is_weighed_at_the_room_its_index_map_grows_to() {
    assert_eq!(object_room(0), (0, 0));
    let mut object = indexmap::IndexMap::<String, Value>::new();
    for len in 1..=1024 {
        object.insert(len.to_string(), Value::Null);
        let (slots, buckets) = object_room(len);
        assert_eq!(slots, object.capacity(), "{len} entries");
        assert!(buckets > slots, "{len} entries");
    }
}

// Catches: two loads of one uncached Generation leaving two graphs in use,
// or the later insert replacing the entry a caller already holds.
#[tokio::test]
async fn concurrent_loads_of_one_generation_share_one_graph() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let generation = publish(&fixture, &[http_input()])
        .await
        .pop()
        .expect("one Generation");
    let cache = SchemaGraphCache::new(SchemaGraphCache::DEFAULT_BUDGET_BYTES);
    let database = fixture.database();

    // Both transactions are open before either load starts, so the second
    // load looks the Generation up while the first is still reading it.
    let mut left = database.begin_read_snapshot().await.unwrap();
    let mut right = database.begin_read_snapshot().await.unwrap();
    let (first, second) = tokio::join!(
        cache.schema_graph_in_transaction(&mut left, &generation.id),
        cache.schema_graph_in_transaction(&mut right, &generation.id),
    );
    left.rollback().await.unwrap();
    right.rollback().await.unwrap();

    let (first, second) = (first.unwrap(), second.unwrap());
    assert!(Arc::ptr_eq(&first, &second));
    assert!(Arc::ptr_eq(
        &first,
        &cached(&fixture, &cache, &generation.id).await.unwrap()
    ));
}
