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
        let weight_bytes = weight_bytes(graph.to_storage_json().unwrap().len());
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

// Catches: a cache that grows past its budget, evicts an entry still in use
// before an idle one, keeps a graph larger than its budget, or evicts others
// to make room for a graph it will not keep.
#[tokio::test]
async fn the_byte_budget_evicts_the_entries_used_longest_ago() {
    struct Case {
        name: &'static str,
        /// The budget, from the weights of an HTTP graph and the smaller MCP graph.
        budget_bytes: fn(usize, usize) -> usize,
        /// Cached reads in order: 0, 1 and 2 are HTTP Generations, 3 and 4 MCP ones.
        loads: &'static [usize],
        kept: &'static [usize],
    }
    let cases = [
        Case {
            name: "two graphs within the budget",
            budget_bytes: |http, _mcp| 2 * http,
            loads: &[0, 1],
            kept: &[0, 1],
        },
        Case {
            name: "a second graph over the budget evicts the first",
            budget_bytes: |http, _mcp| 2 * http - 1,
            loads: &[0, 1],
            kept: &[1],
        },
        Case {
            name: "a graph read again outlives one that was not",
            budget_bytes: |http, _mcp| 2 * http,
            loads: &[0, 1, 0, 2],
            kept: &[0, 2],
        },
        Case {
            name: "one graph evicts as many as it needs room for",
            budget_bytes: |http, mcp| http + mcp - 1,
            loads: &[3, 4, 0],
            kept: &[0],
        },
        Case {
            name: "a graph over the whole budget is not kept and evicts nothing",
            budget_bytes: |http, _mcp| http - 1,
            loads: &[3, 0],
            kept: &[3],
        },
        Case {
            name: "a budget of zero keeps nothing",
            budget_bytes: |_http, _mcp| 0,
            loads: &[3, 3],
            kept: &[],
        },
    ];
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    for case in cases {
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
