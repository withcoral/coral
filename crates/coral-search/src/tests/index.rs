//! Indexing a Provider Generation through `SearchIndex`.
//!
//! Every test publishes through the real Provider Module, then either notifies
//! `SearchIndex` with the Generation's ids, as publication does after its
//! commit, or lets a reconcile pass find the Generation. Until the read
//! side lands, the tests observe the index
//! through a test-only read of the search-owned tables: membership, text
//! columns, and state, never counts, scores, or weights.

use std::collections::BTreeMap;
use std::sync::Arc;

use coral_db::DatabaseSystem;
use coral_provider::{ActiveGeneration, GenerationEntries};
use coral_telemetry::testing::{SpanCapture, attribute};
use coral_workspace::{AuthenticatedPrincipal, AuthenticatedUser, WorkspaceId};
use opentelemetry::trace::SpanId;
use opentelemetry_sdk::trace::SpanData;
use sea_query::{Alias, Expr, ExprTrait as _, Order, Query};
use tracing_subscriber::layer::SubscriberExt as _;
use wiremock::MockServer;

use super::{TestModules, change, organization_user};
use crate::SearchIndex;
use crate::indexer::INDEXER_REVISION;
use crate::indexer::tools::ToolIndexer;

const USER: &str = "user:search-index";

const INJECTED_FAULT: &str = "injected index fault";

/// The reconcile interval of the tests' index: long enough that a `failed`
/// state a test just wrote is newer than it.
const RECONCILE_INTERVAL: std::time::Duration = std::time::Duration::from_mins(5);

#[tokio::test]
async fn indexing_reports_visibility_lag_once_and_backlog_includes_failed_retries() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let metrics = coral_telemetry::testing::MetricCapture::default();
    let _measured = tracing::subscriber::set_default(metrics.subscriber());
    world.reject_complete_states().await;
    world.reconcile_and_settle().await;
    // Start sampling only after the failed attempt has committed its state.
    let background = world.index.spawn_reconcile_task();
    let backlog = || {
        metrics
            .named("coral.search.index.backlog")
            .iter()
            .map(|point| point.value)
            .sum::<f64>()
    };
    wait_for_observation(|| backlog() >= 1.0).await;
    assert!(
        metrics
            .named("coral.search.publication.visibility.duration")
            .is_empty(),
        "a failed commit reports no visibility"
    );
    world.reconcile_and_settle().await;
    assert!(backlog() >= 1.0, "a retry not yet due is still backlog");
    world.allow_complete_states().await;
    world.make_retries_due().await;
    world.reconcile_and_settle().await;
    wait_for_observation(|| {
        !metrics.named("coral.search.index.backlog").is_empty() && backlog() < 1.0
    })
    .await;
    let samples = || {
        metrics
            .named("coral.search.publication.visibility.duration")
            .iter()
            .map(|point| point.count)
            .sum::<u64>()
    };
    let first = samples();
    assert!(first > 0);
    assert!(
        metrics
            .named("coral.search.publication.visibility.duration")
            .iter()
            .all(|point| point.attributes.get("outcome").map(String::as_str) == Some("retried"))
    );
    world.reconcile_and_settle().await;
    assert_eq!(
        samples(),
        first,
        "an unchanged pass does not repeat publication latency"
    );
    background.abort();
    let _stopped = background.await;
}

#[tokio::test]
async fn complete_rewrites_and_failed_rebuilds_do_not_repeat_first_visibility() {
    for rename in [true, false] {
        let upstream = MockServer::start().await;
        let world = World::publish_tracer(&upstream).await;
        let metrics = coral_telemetry::testing::MetricCapture::default();
        let _measured = tracing::subscriber::set_default(metrics.subscriber());
        world.reconcile_and_settle().await;
        let visibility = || metrics.named("coral.search.publication.visibility.duration");
        let recovery = || metrics.named("coral.search.index.recovery.duration");
        let samples = || visibility().iter().map(|point| point.count).sum::<u64>();
        let first = samples();
        assert!(first > 0);
        assert!(
        visibility()
            .iter()
            .all(|point| point.attributes.get("outcome").map(String::as_str) == Some("initial"))
    );

        // An older writer can complete a projection without clearing the new
        // outage column. The current complete state must remain authoritative.
        world.set_outage_start(1).await;
        // Exercise a real complete rewrite, not a reconcile pass that skips it.
        world
            .index
            .index_active_generation(world.workspace.clone(), world.tracer.clone());
        world.index.settled().await;
        assert_eq!(samples(), first);

        assert!(recovery().is_empty());
        world.set_outage_start(1).await;

        if rename {
            world
                .modules
                .providers
                .rename_provider(
                    &change(world.user.clone(), 70, '7'),
                    &world.workspace,
                    &world.tracer,
                    coral_provider::ProviderDisplayName::parse("Storm Watch").unwrap(),
                )
                .await
                .unwrap();
        } else {
            world
                .modules
                .fixture
                .execute_fixture_sql(
                    "UPDATE search_index_states SET indexer_revision = 0 WHERE kind = 'provider_tool'",
                )
                .await
                .unwrap();
        }
        world.reject_complete_states().await;
        world.reconcile_and_settle().await;
        let states = world.search_index_states(Some(&world.tracer)).await;
        assert!(
            matches!(
                states.get(&world.tracer),
                Some(crate::SearchIndexState::Failed { .. })
            ),
            "rebuild failed: {states:?}"
        );
        let background = world.index.spawn_reconcile_task();
        wait_for_observation(|| {
            !metrics
                .named("coral.search.index.first_visibility.age")
                .is_empty()
        })
        .await;
        assert!(
            metrics
                .values("coral.search.index.first_visibility.age")
                .iter()
                .all(|age| *age < 0.001),
            "a previously visible catalog is not awaiting first visibility"
        );
        assert!(
            metrics
                .values("coral.search.index.backlog")
                .iter()
                .any(|count| *count >= 1.0),
            "rebuild remains backlog (rename: {rename})"
        );
        background.abort();
        let _stopped = background.await;

        // Repeated failures retain the original outage clock.
        let five_seconds_ago = crate::unix_now_nanos().unwrap() - 5_000_000_000;
        world.set_outage_start(five_seconds_ago).await;
        world.make_retries_due().await;
        world.reconcile_and_settle().await;
        // A fresh index handle reads the durable history on recovery.
        world.allow_complete_states().await;
        world.make_retries_due().await;
        let restarted = SearchIndex::new(world.modules.search.database.clone(), RECONCILE_INTERVAL);
        restarted.reconcile().await;
        restarted.settled().await;
        assert_eq!(samples(), first);
        let recovery = recovery();
        assert!(!recovery.is_empty());
        assert!(
            recovery
                .iter()
                .all(|point| point.value >= 5.0 && point.value < 60.0),
            "recovery measures this outage rather than the original publication's age"
        );
    }
}

#[tokio::test]
async fn upgrading_preserves_visibility_even_when_an_older_writer_later_records_failure() {
    let upstream = MockServer::start().await;
    let modules = TestModules::with_plan(&coral_test_support::migration_plan_before(48)).await;
    let world = World::publish_with_modules(modules, &upstream).await;
    world.modules.fixture.execute_fixture_sql(
        "INSERT INTO search_index_states (workspace_id, target_id, kind, provider_id, provider_generation_id, provider_display_name, state, attempted_at_unix_nanos, indexer_revision) SELECT workspace_id, active_generation_id, 'provider_tool', id, active_generation_id, display_name, 'complete', 1, 1 FROM provider_records WHERE active_generation_id IS NOT NULL"
    ).await.unwrap();
    world
        .modules
        .search
        .database
        .migrate(coral_db::migration_plan())
        .await
        .unwrap();
    // An older writer updates only columns it knows. Backfilled visibility
    // survives, but no precise outage clock can be invented for that writer.
    world.modules.fixture.execute_fixture_sql(&format!(
        "UPDATE search_index_states SET state = 'failed', error = 'older writer', failed_attempts = 1, retry_after_unix_nanos = 9223372036854775807, failed_coral_version = '{}' WHERE kind = 'provider_tool'", env!("CARGO_PKG_VERSION")
    )).await.unwrap();
    let metrics = coral_telemetry::testing::MetricCapture::default();
    let _measured = tracing::subscriber::set_default(metrics.subscriber());
    let background = world.index.spawn_reconcile_task();
    wait_for_observation(|| {
        !metrics
            .named("coral.search.index.first_visibility.age")
            .is_empty()
    })
    .await;
    assert!(
        metrics
            .values("coral.search.index.first_visibility.age")
            .iter()
            .all(|value| value.abs() < f64::EPSILON)
    );
    assert!(
        metrics
            .values("coral.search.index.backlog")
            .iter()
            .any(|value| *value >= 1.0)
    );
    background.abort();
    let _stopped = background.await;
    world
        .index
        .index_active_generation(world.workspace.clone(), world.tracer.clone());
    world.index.settled().await;
    assert!(
        metrics
            .named("coral.search.publication.visibility.duration")
            .is_empty()
    );
    assert!(
        metrics
            .named("coral.search.index.recovery.duration")
            .is_empty()
    );
}

#[tokio::test]
async fn legacy_failed_projections_keep_visibility_unknown_until_a_successful_commit() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    world.reject_complete_states().await;
    world.reconcile_and_settle().await;
    // Older releases left failed rows without first-visibility evidence.
    world
        .modules
        .fixture
        .execute_fixture_sql(
            "UPDATE search_index_states SET ever_visible = NULL WHERE state = 'failed'",
        )
        .await
        .unwrap();
    let metrics = coral_telemetry::testing::MetricCapture::default();
    let _measured = tracing::subscriber::set_default(metrics.subscriber());
    let background = world.index.spawn_reconcile_task();
    wait_for_observation(|| !metrics.named("coral.search.index.backlog").is_empty()).await;
    assert!(
        metrics
            .values("coral.search.index.backlog")
            .iter()
            .any(|value| *value >= 1.0)
    );
    assert!(
        metrics
            .named("coral.search.index.first_visibility.age")
            .is_empty()
    );
    background.abort();
    let _stopped = background.await;
    world.allow_complete_states().await;
    world.make_retries_due().await;
    world.reconcile_and_settle().await;
    assert!(
        metrics
            .named("coral.search.publication.visibility.duration")
            .is_empty(),
        "unknown history must not invent first visibility"
    );
    assert!(
        metrics
            .named("coral.search.index.recovery.duration")
            .is_empty(),
        "unknown history must not invent an outage start"
    );
    let restarted = SearchIndex::new(world.modules.search.database.clone(), RECONCILE_INTERVAL);
    let background = restarted.spawn_reconcile_task();
    wait_for_observation(|| {
        !metrics
            .named("coral.search.index.first_visibility.age")
            .is_empty()
    })
    .await;
    assert!(
        metrics
            .values("coral.search.index.first_visibility.age")
            .iter()
            .all(|value| value.abs() < f64::EPSILON)
    );
    background.abort();
    let _stopped = background.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backlog_sampling_continues_while_synchronous_index_work_is_held() {
    let upstream = MockServer::start().await;
    let mut world = World::publish_tracer(&upstream).await;
    let metrics = coral_telemetry::testing::MetricCapture::default();
    let _measured = tracing::subscriber::set_default(metrics.subscriber());
    let hold = world.index.hold_row_building();
    let background = world.index.spawn_reconcile_task();
    tokio::time::timeout(std::time::Duration::from_secs(5), hold.entered())
        .await
        .expect("row work starts");
    // Any snapshot taken before the hold is now too old to satisfy this.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let fresh = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if metrics
                .values("coral.search.index.backlog.sample.age")
                .iter()
                .any(|age| *age < 0.15)
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    drop(hold);
    background.abort();
    let _stopped = background.await;
    fresh.expect("synchronous reconciliation cannot stop the observer from refreshing");
}

#[tokio::test]
async fn an_observer_panic_does_not_stop_reconciliation_and_the_observer_restarts() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    world
        .modules
        .fixture
        .execute_fixture_sql("ALTER TABLE search_index_states RENAME TO unavailable_index_states")
        .await
        .unwrap();
    let metrics = coral_telemetry::testing::MetricCapture::default();
    let logs = CapturedLogs::default();
    let armed = Arc::new(AtomicBool::new(true));
    let _measured = tracing::subscriber::set_default(
        tracing_subscriber::Registry::default()
            .with(metrics.layer())
            .with(logs.clone())
            .with(PanicObserverOnce(armed.clone())),
    );
    let index = SearchIndex::new(
        world.modules.search.database.clone(),
        std::time::Duration::from_millis(20),
    );
    let background = index.spawn_reconcile_task();
    wait_for_observation(|| !armed.load(Ordering::SeqCst)).await;
    world
        .modules
        .fixture
        .execute_fixture_sql("ALTER TABLE unavailable_index_states RENAME TO search_index_states")
        .await
        .unwrap();
    let reconciled = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let states = world.search_index_states(Some(&world.tracer)).await;
            if matches!(
                states.get(&world.tracer),
                Some(crate::SearchIndexState::Indexed)
            ) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    if reconciled.is_ok() {
        wait_for_observation(|| !metrics.named("coral.search.index.backlog").is_empty()).await;
    }
    background.abort();
    let _stopped = background.await;
    reconciled.expect("observer failure must not stop reconciliation");
    assert!(
        logs.events()
            .iter()
            .any(|event| event.fields.get("error.type").map(String::as_str)
                == Some("\"background_task_panicked\"")
                && event
                    .fields
                    .get("coral.search.background.task")
                    .map(String::as_str)
                    == Some("\"observer\"")),
        "the supervisor identifies the task and safe failure kind"
    );
}

/// Fault injection at the subscriber boundary, as an exporter or layer can
/// panic while handling a real failed snapshot. No production panic hook.
struct PanicObserverOnce(Arc<std::sync::atomic::AtomicBool>);
impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for PanicObserverOnce {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _context: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if event.metadata().target().ends_with("index::telemetry")
            && self.0.swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            panic!("injected observer subscriber failure");
        }
    }
}

#[tokio::test]
async fn an_unreadable_backlog_reports_unknown_and_recovers_after_the_next_snapshot() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let metrics = coral_telemetry::testing::MetricCapture::default();
    let logs = CapturedLogs::default();
    let _measured = tracing::subscriber::set_default(
        tracing_subscriber::Registry::default()
            .with(metrics.layer())
            .with(logs.clone()),
    );
    let background = world.index.spawn_reconcile_task();
    world.reconcile_and_settle().await;
    wait_for_observation(|| !metrics.named("coral.search.index.backlog").is_empty()).await;
    world
        .modules
        .fixture
        .execute_fixture_sql("ALTER TABLE search_index_states RENAME TO unavailable_index_states")
        .await
        .unwrap();
    world.reconcile_and_settle().await;
    wait_for_observation(|| metrics.named("coral.search.index.backlog").is_empty()).await;
    assert!(
        metrics
            .named("coral.search.index.first_visibility.age")
            .is_empty()
    );
    assert!(
        logs.events()
            .iter()
            .any(|event| event.target.ends_with("index::telemetry")
                && event.fields.get("error.type").map(String::as_str) == Some("\"database\"")),
        "the missing snapshot has a safe diagnostic reason"
    );
    world
        .modules
        .fixture
        .execute_fixture_sql("ALTER TABLE unavailable_index_states RENAME TO search_index_states")
        .await
        .unwrap();
    world.reconcile_and_settle().await;
    wait_for_observation(|| !metrics.named("coral.search.index.backlog").is_empty()).await;
    assert!(
        metrics
            .named("coral.search.index.backlog")
            .iter()
            .all(|point| point.value < 1.0)
    );
    assert!(
        !metrics.named("coral.search.index.backlog").is_empty(),
        "a successful empty snapshot is a known zero"
    );
    background.abort();
    let _stopped = background.await;
    wait_for_observation(|| metrics.named("coral.search.index.backlog").is_empty()).await;
    assert!(
        metrics
            .named("coral.search.index.backlog.sample.age")
            .is_empty(),
        "a stopped observer must not keep publishing its last snapshot"
    );
}

async fn wait_for_observation(ready: impl Fn() -> bool) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !ready() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("background backlog observation converges");
}

#[tokio::test]
async fn one_reconcile_pass_indexes_a_published_generation_and_a_second_changes_nothing() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let generation = world
        .stored_entries(&world.tracer)
        .await
        .generation_id()
        .to_string();

    world.index.reconcile().await;

    assert_eq!(
        world.indexed_generations().await,
        std::slice::from_ref(&generation)
    );
    assert_eq!(
        world.states().await,
        [StateRow::complete(
            &world.tracer,
            &generation,
            "Weather Tracer"
        )]
    );
    let stamps = world.write_stamps().await;
    let versions = world.row_versions().await;
    world.index.reconcile().await;
    assert_eq!(
        world.write_stamps().await,
        stamps,
        "a second pass rewrites nothing"
    );
    assert_eq!(
        world.row_versions().await,
        versions,
        "a second pass writes no row"
    );
}

/// `SQLite` writers queue in arrival order for one connection, so a pass that
/// took the writer to find nothing would hold back every write in the process
/// on each interval.
#[tokio::test]
async fn a_pass_that_finds_nothing_to_do_does_not_wait_for_the_writer() {
    coral_test_support::database_only!(Sqlite);
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    world.index.reconcile().await;

    let writer = world.modules.search.database.begin().await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), world.index.reconcile())
        .await
        .expect("a pass with nothing to do reads and returns");
    writer.rollback().await.unwrap();
}

/// A pass reads what is stale at its start, and under load the notifications
/// for the same Generations arrive while it writes. Neither side may index a
/// Generation the other just finished: the repeat would queue for the writer
/// again and lengthen the backlog that made the two overlap.
#[tokio::test]
async fn a_pass_and_its_overlapping_notifications_index_each_generation_once() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let ledger = world.publish_ledger().await;
    let tracer_entries = world.stored_entries(&world.tracer).await;
    let ledger_entries = world.stored_entries(&ledger).await;
    let index = world
        .index
        .clone()
        .with_stall_before_write(std::time::Duration::from_millis(50));

    let spans = CapturedSpans::default();
    {
        let _subscriber = tracing::subscriber::set_default(
            tracing_subscriber::Registry::default().with(spans.clone()),
        );
        tokio::join!(index.reconcile(), async {
            notify(&index, &tracer_entries);
            notify(&index, &ledger_entries);
        });
        index.settled().await;
    }

    let writes = spans
        .recorded()
        .into_iter()
        .filter(|span| span.fields.contains_key("coral.search.writer_wait_ms"))
        .count();
    assert_eq!(writes, 2, "one write per Generation");
    assert_eq!(world.indexed_generations().await.len(), 2);
}

#[tokio::test]
async fn a_notice_for_an_indexed_generation_writes_nothing() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let entries = world.stored_entries(&world.tracer).await;
    world.index.reconcile().await;

    let write = world.traced_notice(|index| notify(index, &entries)).await;

    assert_eq!(
        attribute(&write, "coral.search.skipped").as_deref(),
        Some("already_indexed")
    );
}

/// A pass reads its stale functions once and releases the mutex between
/// items, so a notified write of a newer revision can land first. Writing the
/// pass's older copy after it would put the old text back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pass_leaves_a_function_that_a_notified_write_indexed_at_a_newer_revision() {
    let upstream = MockServer::start().await;
    let mut world = World::publish_tracer(&upstream).await;
    world.index.reconcile().await;
    let principal = AuthenticatedPrincipal::user(world.user.clone());
    for (path, description, at) in [
        ("internal.first", "First function", 60),
        ("internal.second", "Older text", 61),
    ] {
        world
            .modules
            .save_function(&principal, &world.workspace, path, description, at)
            .await;
    }
    let hold = world.index.hold_row_building();
    let index = world.index.clone();
    let pass = tokio::spawn(async move { index.reconcile().await });
    tokio::time::timeout(std::time::Duration::from_secs(5), hold.entered())
        .await
        .expect("the pass reaches its first function");

    let newer = world
        .modules
        .save_function(
            &principal,
            &world.workspace,
            "internal.second",
            "Newer text",
            70,
        )
        .await;
    world
        .index
        .index_function(coral_functions::SavedFunctionEntry::from_saved(&newer));
    // The notified write queues for the mutex ahead of the pass's next item.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    drop(hold);
    pass.await.expect("the pass ends");
    world.index.settled().await;

    let descriptions: Vec<(String,)> = world
        .modules
        .search
        .database
        .fetch_all(
            Query::select()
                .column(Alias::new("description"))
                .from(Alias::new("search_entities"))
                .and_where(Expr::col(Alias::new("qualified_name")).eq("internal.second"))
                .to_owned(),
        )
        .await
        .expect("read the function's row");
    assert_eq!(descriptions, [("Newer text".to_string(),)]);
}

#[tokio::test]
async fn a_generation_replaced_after_a_pass_read_it_is_no_longer_stale() {
    async fn active(world: &World, provider: &coral_provider::ProviderId) -> ActiveGeneration {
        let database = &world.modules.search.database;
        let mut transaction = database.begin_read_snapshot().await.unwrap();
        coral_provider::workspace_active_generations_in_transaction(
            &mut transaction,
            &world.workspace,
            Some(provider),
        )
        .await
        .unwrap()
        .pop()
        .expect("the Provider's active Generation")
    }
    async fn stale(world: &World, generation: &ActiveGeneration) -> bool {
        let database = &world.modules.search.database;
        let mut transaction = database.begin_read_snapshot().await.unwrap();
        ToolIndexer
            .still_needs_indexing(
                &mut transaction,
                generation,
                crate::unix_now_nanos().unwrap(),
            )
            .await
            .unwrap()
    }
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let ledger = world.publish_ledger().await;
    let read_by_the_pass = active(&world, &ledger).await;
    assert!(stale(&world, &read_by_the_pass).await);

    world
        .modules
        .regenerate_workspace_provider(
            &change(world.user.clone(), 60, '6'),
            &world.workspace,
            &ledger,
            &serde_json::from_str(&LEDGER.replace("List ledger entries", "List every entry"))
                .unwrap(),
        )
        .await;

    assert!(!stale(&world, &read_by_the_pass).await);
    assert!(stale(&world, &active(&world, &ledger).await).await);
}

#[tokio::test]
async fn a_transient_failure_waits_twice_as_long_after_each_attempt() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let entries = world.stored_entries(&world.tracer).await;
    world.reject_complete_states().await;

    world.index.reconcile().await;
    assert_eq!(world.retries().await, [Retry::after(1, RECONCILE_INTERVAL)]);
    // A notified write always tries, whatever the retry rule.
    notify(&world.index, &entries);
    world.index.settled().await;
    assert_eq!(
        world.retries().await,
        [Retry::after(2, RECONCILE_INTERVAL * 2)]
    );

    world.allow_complete_states().await;
    world.index.reconcile().await;
    assert!(
        world.tool_rows().await.is_empty(),
        "a pass before the retry time leaves the failure"
    );
    world.make_retries_due().await;
    world.index.reconcile().await;
    assert_eq!(
        world.states().await,
        [StateRow::complete(
            &world.tracer,
            &entries.generation_id().to_string(),
            "Weather Tracer"
        )]
    );
    assert_eq!(world.retries().await, [Retry::none()]);
}

#[tokio::test]
async fn a_timeout_after_the_workspace_lock_waits_for_a_newer_coral_version() {
    // `NULL` is the version of a row written before Coral recorded one.
    for older_version in ["'0.0.0'", "NULL"] {
        let upstream = MockServer::start().await;
        let world = World::publish_tracer(&upstream).await;
        let entries = world.stored_entries(&world.tracer).await;
        let stalled = world
            .index
            .clone()
            .with_write_limit(std::time::Duration::from_millis(100))
            .with_stall_after_lock(std::time::Duration::from_hours(1));

        notify(&stalled, &entries);
        stalled.settled().await;
        let [state] = world.states().await.try_into().expect("one state row");
        assert_eq!(state.state, "failed");
        let error = state.error.expect("a failed state carries its error");
        assert!(error.contains("ran past its"), "{error}");
        assert_eq!(world.retries().await, [Retry::on_new_version(1)]);

        world.make_retries_due().await;
        let stamps = world.attempt_stamps().await;
        world.index.reconcile().await;
        assert_eq!(
            world.attempt_stamps().await,
            stamps,
            "a pass under the same Coral version leaves the failure"
        );

        world
            .modules
            .fixture
            .execute_fixture_sql(&format!(
                "UPDATE search_index_states SET failed_coral_version = {older_version}"
            ))
            .await
            .expect("record an older Coral version");
        world.index.reconcile().await;
        assert_eq!(
            world.states().await,
            [StateRow::complete(
                &world.tracer,
                &entries.generation_id().to_string(),
                "Weather Tracer"
            )],
            "older version: {older_version}"
        );
    }
}

#[tokio::test]
async fn a_timeout_waiting_for_the_writer_is_transient() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let entries = world.stored_entries(&world.tracer).await;
    let index = world
        .index
        .clone()
        .with_write_limit(std::time::Duration::from_secs(1));
    let database = world.modules.search.database.clone();

    // SQLite's writer, or Postgres's lock on the Workspace's index writes.
    let mut blocker = database.begin().await.expect("hold the writer");
    crate::indexer::lock_workspace(&mut blocker, &world.workspace)
        .await
        .expect("hold the Workspace's index lock");
    notify(&index, &entries);
    // Released half a limit after the write gives up, so the failure records
    // its state within its own limit, with room on both sides.
    tokio::join!(index.settled(), async {
        tokio::time::sleep(std::time::Duration::from_millis(1_500)).await;
        blocker.rollback().await.expect("release the writer");
    });

    let [state] = world.states().await.try_into().expect("one state row");
    assert_eq!(state.state, "failed");
    let error = state.error.expect("a failed state carries its error");
    assert!(error.contains("waited more than"), "{error}");
    assert_eq!(world.retries().await, [Retry::after(1, RECONCILE_INTERVAL)]);
}

#[tokio::test]
async fn another_write_can_take_the_writer_while_a_write_builds_its_rows() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let entries = world.stored_entries(&world.tracer).await;
    // The pause stands in for a slow row build.
    let index = world
        .index
        .clone()
        .with_stall_before_write(std::time::Duration::from_secs(2));
    let database = world.modules.search.database.clone();

    notify(&index, &entries);
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    // SQLite's writer, or Postgres's lock on the Workspace's index writes.
    let probe = tokio::time::timeout(std::time::Duration::from_millis(500), async {
        let mut probe = database.begin().await.expect("begin the probe");
        crate::indexer::lock_workspace(&mut probe, &world.workspace)
            .await
            .expect("take the Workspace's index lock");
        probe
    })
    .await
    .expect("the writer is free while the rows build");
    probe.rollback().await.expect("release the writer");
    index.settled().await;

    assert_eq!(
        world.states().await,
        [StateRow::complete(
            &world.tracer,
            &entries.generation_id().to_string(),
            "Weather Tracer"
        )]
    );
}

#[tokio::test]
async fn a_write_that_waited_for_the_writer_still_has_its_whole_limit() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let entries = world.stored_entries(&world.tracer).await;
    // The wait and the write each take 60% of the limit: together they run
    // past it, each alone stays within it.
    let index = world
        .index
        .clone()
        .with_write_limit(std::time::Duration::from_secs(2))
        .with_stall_after_lock(std::time::Duration::from_millis(1_200));
    let database = world.modules.search.database.clone();

    let mut blocker = database.begin().await.expect("hold the writer");
    crate::indexer::lock_workspace(&mut blocker, &world.workspace)
        .await
        .expect("hold the Workspace's index lock");
    notify(&index, &entries);
    tokio::join!(index.settled(), async {
        tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;
        blocker.rollback().await.expect("release the writer");
    });

    assert_eq!(
        world.states().await,
        [StateRow::complete(
            &world.tracer,
            &entries.generation_id().to_string(),
            "Weather Tracer"
        )]
    );
}

#[tokio::test]
async fn a_failed_removal_leaves_the_superseded_generation_and_still_indexes_the_active_one() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let ledger = world.publish_ledger().await;
    world.index.reconcile().await;
    let superseded = world
        .stored_entries(&ledger)
        .await
        .generation_id()
        .to_string();
    world
        .modules
        .regenerate_workspace_provider(
            &change(world.user.clone(), 60, '6'),
            &world.workspace,
            &ledger,
            &serde_json::from_str(&LEDGER.replace("List ledger entries", "List every entry"))
                .unwrap(),
        )
        .await;
    let current = world
        .stored_entries(&ledger)
        .await
        .generation_id()
        .to_string();
    world.reject_state_removals().await;

    let logs = CapturedLogs::default();
    {
        let _subscriber = tracing::subscriber::set_default(
            tracing_subscriber::Registry::default().with(logs.clone()),
        );
        world.index.reconcile().await;
    }

    let ledger_states = |states: Vec<StateRow>| {
        states
            .into_iter()
            .filter(|state| state.provider_id == ledger.to_string())
            .map(|state| (state.generation_id, state.state))
            .collect::<std::collections::BTreeSet<_>>()
    };
    assert_eq!(
        ledger_states(world.states().await),
        [
            (superseded.clone(), "complete".to_string()),
            (current.clone(), "complete".to_string()),
        ]
        .into_iter()
        .collect()
    );
    assert!(
        logs.events()
            .iter()
            .any(|event| event.level == tracing::Level::ERROR
                && event.fields.get("error.type").map(String::as_str) == Some("\"database\"")),
        "the failed removal is logged"
    );

    world.allow_state_removals().await;
    world.index.reconcile().await;
    assert_eq!(
        ledger_states(world.states().await),
        [(current, "complete".to_string())].into_iter().collect()
    );
}

#[tokio::test]
async fn after_a_regeneration_a_pass_alone_replaces_the_superseded_generation() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let ledger = world.publish_ledger().await;
    world.index.reconcile().await;
    let tracer = world
        .stored_entries(&world.tracer)
        .await
        .generation_id()
        .to_string();
    let superseded = world
        .stored_entries(&ledger)
        .await
        .generation_id()
        .to_string();

    world
        .modules
        .regenerate_workspace_provider(
            &change(world.user.clone(), 60, '6'),
            &world.workspace,
            &ledger,
            &serde_json::from_str(&LEDGER.replace("List ledger entries", "List every entry"))
                .unwrap(),
        )
        .await;
    let current = world
        .stored_entries(&ledger)
        .await
        .generation_id()
        .to_string();
    assert_ne!(
        current, superseded,
        "a changed document is a new Generation"
    );
    world.index.reconcile().await;

    let generations = [tracer.clone(), current.clone()]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        world
            .indexed_generations()
            .await
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>(),
        generations
    );
    assert_eq!(
        world
            .states()
            .await
            .into_iter()
            .map(|state| (state.generation_id, state.state))
            .collect::<std::collections::BTreeSet<_>>(),
        generations
            .into_iter()
            .map(|generation| (generation, "complete".to_string()))
            .collect()
    );
}

#[tokio::test]
async fn a_pass_reindexes_a_renamed_provider_under_its_new_name() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    world.index.reconcile().await;

    world
        .modules
        .providers
        .rename_provider(
            &change(world.user.clone(), 70, '7'),
            &world.workspace,
            &world.tracer,
            coral_provider::ProviderDisplayName::parse("Storm Watch").unwrap(),
        )
        .await
        .expect("rename the Provider");
    world.index.reconcile().await;

    let generation = world
        .stored_entries(&world.tracer)
        .await
        .generation_id()
        .to_string();
    assert_eq!(
        world
            .tool_rows()
            .await
            .into_iter()
            .map(|row| row.provider_name)
            .collect::<Vec<_>>(),
        ["weather Storm Watch"]
    );
    assert_eq!(
        world.states().await,
        [StateRow::complete(
            &world.tracer,
            &generation,
            "Storm Watch"
        )]
    );
    world.assert_exact_statistics().await;
}

#[tokio::test]
async fn a_pass_reindexes_a_generation_that_an_older_indexer_revision_wrote() {
    for (revision, reindexed, writer) in [
        (None, true, "a Coral without indexer revisions"),
        (
            Some(INDEXER_REVISION - 1),
            true,
            "an older indexer revision",
        ),
        (
            Some(INDEXER_REVISION + 1),
            false,
            "a newer indexer revision during a rolling update",
        ),
    ] {
        let upstream = MockServer::start().await;
        let world = World::publish_tracer(&upstream).await;
        world.index.reconcile().await;
        let indexed = world.tool_rows().await;
        world.leave_rows_as_written_by(revision).await;
        let left = world.tool_rows().await;

        world.index.reconcile().await;

        if reindexed {
            assert_eq!(world.tool_rows().await, indexed, "{writer}");
            world.assert_exact_statistics().await;
            // The rewrite records this revision, so the next pass finds
            // nothing to index.
            let stamps = world.write_stamps().await;
            world.index.reconcile().await;
            assert_eq!(
                world.write_stamps().await,
                stamps,
                "{writer}: a second pass"
            );
        } else {
            assert_eq!(world.tool_rows().await, left, "{writer}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_concurrent_passes_converge_on_the_rows_one_pass_leaves() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let ledger = world.publish_ledger().await;
    // Each process builds its own index, with its own mutex.
    let other_process = SearchIndex::new(world.modules.search.database.clone(), RECONCILE_INTERVAL);

    tokio::join!(world.index.reconcile(), other_process.reconcile());

    let mut expected = Vec::new();
    for (provider, name) in [(&world.tracer, "Weather Tracer"), (&ledger, "Ledger")] {
        let generation = world
            .stored_entries(provider)
            .await
            .generation_id()
            .to_string();
        expected.push(StateRow::complete(provider, &generation, name));
    }
    expected.sort_by(|left, right| left.generation_id.cmp(&right.generation_id));
    assert_eq!(world.states().await, expected);
    let stamps = world.write_stamps().await;
    world.index.reconcile().await;
    assert_eq!(
        world.write_stamps().await,
        stamps,
        "the concurrent passes left nothing for a later pass"
    );
    world.assert_exact_statistics().await;
}

#[tokio::test]
async fn a_pass_corrects_the_statistics_a_saved_function_delete_leaves() {
    coral_test_support::database_only!(Postgres);
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let principal = AuthenticatedPrincipal::user(world.user.clone());
    world
        .modules
        .save_function(
            &principal,
            &world.workspace,
            "internal.summarizeTides",
            "Summarize the tides",
            60,
        )
        .await;
    world.index.reconcile().await;
    assert!(world.has_term("tides").await);
    world.assert_exact_statistics().await;

    // The function's row cascades with it, and no index write follows.
    world
        .modules
        .delete_function(&principal, &world.workspace, "internal.summarizeTides", 70)
        .await;
    assert!(world.has_term("tides").await, "nothing recounted yet");
    world.index.reconcile().await;

    assert!(!world.has_term("tides").await);
    world.assert_exact_statistics().await;
}

#[tokio::test]
async fn a_pass_traces_only_the_writes_it_makes_and_records_its_work() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let principal = AuthenticatedPrincipal::user(world.user.clone());
    world
        .modules
        .save_function(
            &principal,
            &world.workspace,
            "internal.summarizeTides",
            "Summarize the tides",
            60,
        )
        .await;

    let (pass, writes) = split_pass(world.traced_pass().await);
    assert_eq!(
        pass.iter().map(|span| span.name).collect::<Vec<_>>(),
        [
            "coral.search.reconcile",
            "coral.search.reconcile.remove_superseded",
            "coral.search.reconcile.generations",
            "coral.search.reconcile.functions",
            "coral.search.reconcile.statistics",
        ]
    );
    for span in &pass {
        assert_eq!(span.level, tracing::Level::DEBUG, "{}", span.name);
        assert_eq!(failure(span), None, "{} did not fail", span.name);
    }
    for span in &writes {
        assert_eq!(span.level, tracing::Level::INFO, "{}", span.name);
        assert_eq!(failure(span), None, "{} did not fail", span.name);
        assert_eq!(
            span.fields
                .get("coral.search.write.trigger")
                .map(String::as_str),
            Some("\"reconcile\""),
            "{}",
            span.name
        );
    }
    assert_eq!(
        write_names(&writes),
        [
            "coral.search.index_generation",
            "coral.search.index_function"
        ],
        "the first pass indexes the Provider and the function"
    );
    assert_eq!(item_work(&pass, "generations"), (1, 0));
    assert_eq!(item_work(&pass, "functions"), (1, 0));
    // Only Postgres keeps statistics of its own. Each index write counts its
    // Workspace, so the statistics step finds nothing to count after them.
    let checked = match world.modules.search.database.system() {
        DatabaseSystem::Postgres => 1,
        DatabaseSystem::Sqlite => 0,
    };
    assert_eq!(statistics_work(&pass), (checked, 0, false));
    let (pass, writes) = split_pass(world.traced_pass().await);
    assert!(writes.is_empty(), "a pass that changed nothing: {writes:?}");
    assert_eq!(item_work(&pass, "generations"), (0, 0));

    world
        .modules
        .delete_function(&principal, &world.workspace, "internal.summarizeTides", 70)
        .await;
    let (pass, writes) = split_pass(world.traced_pass().await);
    assert_eq!(
        statistics_work(&pass),
        (checked, checked, checked > 0),
        "the pass after a cascade counts the Workspace again"
    );
    assert_eq!(
        write_names(&writes).len(),
        usize::try_from(checked).expect("a count"),
        "a recount is a write of its own: {writes:?}"
    );
    let (pass, writes) = split_pass(world.traced_pass().await);
    assert_eq!(
        statistics_work(&pass),
        (checked, 0, false),
        "a pass with no changes counts nothing"
    );
    assert!(writes.is_empty());
}

#[tokio::test]
async fn a_failed_step_marks_its_span_and_the_pass() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    world
        .modules
        .fixture
        .execute_fixture_sql("ALTER TABLE search_index_states RENAME TO hidden_states")
        .await
        .expect("hide the index states");

    let (pass, _) = split_pass(world.traced_pass().await);

    for name in [
        "coral.search.reconcile",
        "coral.search.reconcile.generations",
        "coral.search.reconcile.functions",
    ] {
        let span = pass
            .iter()
            .find(|span| span.name == name)
            .unwrap_or_else(|| panic!("{name} span"));
        assert_eq!(failure(span), Some("\"reconcile_failed\""), "{name}");
    }
}

/// A Generation whose indexing fails is recorded for a retry and the pass
/// goes on, but its own span, exported by default, names the error, and the
/// pass counts it as failed.
#[tokio::test]
async fn a_pass_counts_and_traces_a_generation_that_fails_to_index() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    world
        .modules
        .fixture
        .execute_fixture_sql("ALTER TABLE materialized_tools RENAME TO hidden_tools")
        .await
        .expect("hide the catalog's Tools");

    let (pass, writes) = split_pass(world.traced_pass().await);

    let [generation] = writes.try_into().expect("one Generation span");
    assert_eq!(generation.name, "coral.search.index_generation");
    assert_eq!(
        generation
            .fields
            .get("coral.provider.id")
            .map(String::as_str),
        Some(world.tracer.to_string().as_str())
    );
    assert_eq!(failure(&generation), Some("\"provider\""));
    assert_eq!(item_work(&pass, "generations"), (0, 1));
    assert_eq!(
        failure(pass.first().expect("the pass span")),
        Some("\"reconcile_failed\""),
        "the pass fails with its Generation"
    );
}

/// Splits a pass's spans into the `coral.search.reconcile` span and its
/// steps, and the spans of the writes it made, each in start order.
fn split_pass(recorded: Vec<CapturedSpan>) -> (Vec<CapturedSpan>, Vec<CapturedSpan>) {
    recorded
        .into_iter()
        .partition(|span| span.name.starts_with("coral.search.reconcile"))
}

fn write_names(writes: &[CapturedSpan]) -> Vec<&'static str> {
    writes.iter().map(|span| span.name).collect()
}

/// The error type a failed span records.
fn failure(span: &CapturedSpan) -> Option<&str> {
    span.fields.get("error.type").map(String::as_str)
}

/// The `items` the pass's span counts as indexed and as failed.
fn item_work(pass: &[CapturedSpan], items: &str) -> (u64, u64) {
    let count = |outcome: &str| {
        pass.first()
            .expect("the pass span")
            .u64_field(&format!("coral.search.reconcile.{items}_{outcome}"))
            .unwrap_or_else(|| panic!("{items} {outcome}"))
    };
    (count("indexed"), count("failed"))
}

/// The statistics work a pass's spans record: the Workspaces checked and
/// recounted, and whether any row was written. The pass's span and its
/// statistics step's span record the same work.
fn statistics_work(recorded: &[CapturedSpan]) -> (u64, u64, bool) {
    let [pass, .., statistics] = recorded else {
        panic!("a pass and its steps");
    };
    let work = |span: &CapturedSpan| {
        (
            span.u64_field("coral.search.reconcile.workspaces_checked")
                .expect("Workspaces checked"),
            span.u64_field("coral.search.reconcile.workspaces_recounted")
                .expect("Workspaces recounted"),
            span.u64_field("coral.search.reconcile.rows_written")
                .expect("rows written")
                > 0,
        )
    };
    assert_eq!(work(pass), work(statistics));
    work(pass)
}

#[tokio::test]
async fn a_published_generation_indexes_its_tools_and_a_complete_state() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let entries = world.stored_entries(&world.tracer).await;

    notify(&world.index, &entries);
    world.index.settled().await;

    let generation = entries.generation_id().to_string();
    assert_eq!(
        world.tool_rows().await,
        [ToolRow {
            doc_id: format!("{generation}:getForecast"),
            provider_id: world.tracer.to_string(),
            generation_id: generation.clone(),
            tool_key: "getForecast".to_string(),
            call_path: "coral.providers.weather.getForecast".to_string(),
            qualified_name: "getForecast".to_string(),
            title: "Get one weather forecast".to_string(),
            description: "Fetch the current conditions for one forecast location.\n\
                          Use a stable location identifier when one is available."
                .to_string(),
            provider_name: "weather Weather Tracer".to_string(),
            searchable_text: "forecast weather result data object temperatureC number key \
                              getoneweatherforecast weathertracer"
                .to_string(),
            deprecated: true,
            fingerprint: generation.clone(),
        }]
    );
    assert_eq!(
        world.states().await,
        [StateRow::complete(
            &world.tracer,
            &generation,
            "Weather Tracer"
        )]
    );
}

/// One Tool whose response reaches the shared `Run` schema by two
/// paths. The tags and the enum values repeat words in another case.
const RUNS: &str = r##"{
  "openapi": "3.1.0",
  "info": { "title": "Bench Runs", "version": "1.0.0" },
  "servers": [{ "url": "https://bench.example" }],
  "security": [],
  "paths": { "/runs": { "get": { "operationId": "listRuns", "summary": "List runs",
    "tags": ["Runs", "runs"],
    "responses": { "200": { "description": "OK", "content": { "application/json": {
      "schema": { "type": "object", "properties": {
        "latest": { "$ref": "#/components/schemas/Run" },
        "previous": { "$ref": "#/components/schemas/Run" }
      } } } } } } } } },
  "components": { "schemas": { "Run": { "type": "object", "properties": {
    "benchmark_run_id": { "type": "string" },
    "Status": { "type": "string", "enum": ["in-progress", "DONE", "done"] }
  } } } }
}"##;

#[tokio::test]
async fn schema_text_holds_each_word_once_in_its_first_spelling() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    world
        .modules
        .publish_openapi(&world.user, &world.workspace, RUNS, "bench", 50)
        .await;

    world.index.reconcile().await;

    let text = world
        .tool_rows()
        .await
        .into_iter()
        .find(|row| row.call_path == "coral.providers.bench.listRuns")
        .expect("the Tool row")
        .searchable_text;
    // The words split as the query planner splits them, so `_` stays in a
    // word and `-` ends one. Each word appears once: the second path to
    // `Run`, the second tag and `done` add nothing.
    assert_eq!(
        text,
        "Runs result data object latest Status string in progress DONE \
         benchmark_run_id key previous listruns benchruns"
    );
}

#[tokio::test]
async fn a_generation_without_tools_indexes_only_a_complete_state() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let callbacks_only = world
        .modules
        .publish_openapi(&world.user, &world.workspace, CALLBACKS_ONLY, "hooks", 40)
        .await;
    let entries = world.stored_entries(&callbacks_only).await;
    assert!(entries.tools().next().is_none(), "no Tool is available");

    notify(&world.index, &entries);
    world.index.settled().await;

    assert!(world.tool_rows().await.is_empty());
    assert_eq!(
        world.states().await,
        [StateRow::complete(
            &callbacks_only,
            &entries.generation_id().to_string(),
            "Callbacks"
        )]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn visibility_read_pressure_does_not_hold_up_later_index_writes() {
    // SQLite has distinct read and write pools: telemetry can wait for the
    // former while a catalog-removal write must still use the latter.
    coral_test_support::database_only!(Sqlite);
    let upstream = MockServer::start().await;
    let mut world = World::publish_tracer(&upstream).await;
    let ledger = world.publish_ledger().await;
    let old = world.stored_entries(&ledger).await;
    notify(&world.index, &old);
    world.index.settled().await;
    world
        .modules
        .regenerate_workspace_provider(
            &change(world.user.clone(), 60, '6'),
            &world.workspace,
            &ledger,
            &serde_json::from_str(
                &LEDGER.replace("List ledger entries", "List every ledger entry"),
            )
            .unwrap(),
        )
        .await;
    let current = world.stored_entries(&ledger).await;
    let metrics = coral_telemetry::testing::MetricCapture::default();
    let spans = SpanCapture::default();
    let _measured = tracing::subscriber::set_default(
        tracing_subscriber::Registry::default()
            .with(metrics.layer())
            .with(spans.layer()),
    );
    let hold = world.index.hold_row_building();
    notify(&world.index, &current);
    tokio::time::timeout(std::time::Duration::from_secs(5), hold.entered())
        .await
        .unwrap();
    let mut readers = Vec::new();
    for _ in 0..sqlx::sqlite::SqlitePoolOptions::new().get_max_connections() - 1 {
        readers.push(
            world
                .modules
                .search
                .database
                .begin_read_snapshot()
                .await
                .unwrap(),
        );
    }
    // A rolled-back preparation connection may still be returning to its
    // pool on the worker paused above. Queue the final reader before release.
    let last = world.modules.search.database.begin_read_snapshot();
    tokio::pin!(last);
    let ready = tokio::time::timeout(std::time::Duration::from_millis(20), &mut last).await;
    drop(hold);
    readers.push(match ready {
        Ok(reader) => reader.unwrap(),
        Err(_) => last.await.unwrap(),
    });
    wait_for_observation(|| {
        metrics
            .named("coral.persistence.transaction.duration")
            .iter()
            .any(|point| point.attributes.get("outcome").map(String::as_str) == Some("committed"))
    })
    .await;
    world.index.remove_generation(old.generation_id().clone());
    let removed = tokio::time::timeout(std::time::Duration::from_millis(500), async {
        loop {
            if spans
                .finished()
                .iter()
                .any(|span| span.name == "coral.search.remove_generation")
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await;
    for reader in readers {
        reader.rollback().await.unwrap();
    }
    world.index.settled().await;
    removed.expect("the next write proceeds while a visibility read is queued");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_catalog_replaced_during_indexing_emits_no_visibility_sample() {
    let upstream = MockServer::start().await;
    let mut world = World::publish_tracer(&upstream).await;
    let ledger = world.publish_ledger().await;
    let old = world.stored_entries(&ledger).await;
    let metrics = coral_telemetry::testing::MetricCapture::default();
    let _measured = tracing::subscriber::set_default(metrics.subscriber());
    let hold = world.index.hold_row_building();
    notify(&world.index, &old);
    tokio::time::timeout(std::time::Duration::from_secs(5), hold.entered())
        .await
        .unwrap();
    world
        .modules
        .regenerate_workspace_provider(
            &change(world.user.clone(), 60, '6'),
            &world.workspace,
            &ledger,
            &serde_json::from_str(
                &LEDGER.replace("List ledger entries", "List every ledger entry"),
            )
            .unwrap(),
        )
        .await;
    let current = world.stored_entries(&ledger).await;
    assert_ne!(old.generation_id(), current.generation_id());
    drop(hold);
    world.index.settled().await;
    assert!(
        metrics
            .named("coral.search.publication.visibility.duration")
            .is_empty(),
        "a superseded projection never becomes searchable"
    );
    notify(&world.index, &current);
    world.index.settled().await;
    assert_eq!(
        metrics
            .named("coral.search.publication.visibility.duration")
            .iter()
            .map(|point| point.count)
            .sum::<u64>(),
        1,
        "only the current catalog contributes first visibility"
    );
}

#[tokio::test]
async fn a_notice_for_a_replaced_generation_writes_nothing() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let ledger = world.publish_ledger().await;
    let replaced = world.stored_entries(&ledger).await;
    let document: serde_json::Value =
        serde_json::from_str(&LEDGER.replace("List ledger entries", "List every ledger entry"))
            .unwrap();
    world
        .modules
        .regenerate_workspace_provider(
            &change(world.user.clone(), 60, '6'),
            &world.workspace,
            &ledger,
            &document,
        )
        .await;
    assert_ne!(
        world.stored_entries(&ledger).await.generation_id(),
        replaced.generation_id()
    );

    let write = world.traced_notice(|index| notify(index, &replaced)).await;

    assert!(world.tool_rows().await.is_empty());
    assert!(world.states().await.is_empty());
    assert_eq!(
        attribute(&write, "coral.search.skipped").as_deref(),
        Some("generation_replaced")
    );
}

/// A notified write is the root of a trace of its own, linked to the span
/// that notified it, such as the request that published a Generation. Its
/// span names the Generation it wrote and how long it waited for the writer
/// and wrote, and a failed write marks it with its error's kind.
#[tokio::test]
async fn a_notified_write_traces_itself_and_links_to_its_notifier() {
    type Notice = fn(&SearchIndex, &GenerationEntries);
    let by_generation: Notice = notify;
    let by_provider: Notice = |index, entries| {
        index.index_active_generation(
            entries.workspace_id().clone(),
            entries.provider_id().clone(),
        );
    };
    let removal: Notice = |index, entries| {
        index.remove_provider(
            entries.workspace_id().clone(),
            entries.provider_id().clone(),
        );
    };
    for (case, notice, fails, name) in [
        (
            "a Generation",
            by_generation,
            false,
            "coral.search.index_generation",
        ),
        (
            "a renamed Provider",
            by_provider,
            false,
            "coral.search.index_generation",
        ),
        (
            "a failed Generation",
            by_generation,
            true,
            "coral.search.index_generation",
        ),
        ("a removal", removal, false, "coral.search.remove_provider"),
        (
            "a failed removal",
            removal,
            true,
            "coral.search.remove_provider",
        ),
    ] {
        let upstream = MockServer::start().await;
        let world = World::publish_tracer(&upstream).await;
        let entries = world.stored_entries(&world.tracer).await;
        let removes = name == "coral.search.remove_provider";
        if removes {
            notify(&world.index, &entries);
            world.index.settled().await;
        }
        if fails && removes {
            world.reject_state_removals().await;
        } else if fails {
            world.reject_complete_states().await;
        }

        let write = world.traced_notice(|index| notice(index, &entries)).await;

        assert_eq!(write.name, name, "{case}");
        assert_eq!(
            attribute(&write, "error.type"),
            fails.then(|| "database".to_string()),
            "{case}"
        );
        if !removes {
            assert_eq!(
                attribute(&write, "coral.search.generation.id"),
                Some(entries.generation_id().to_string()),
                "{case}"
            );
            for field in ["coral.search.writer_wait_ms", "coral.search.write_ms"] {
                assert!(
                    attribute(&write, field).is_some_and(|ms| ms.parse::<u64>().is_ok()),
                    "{case}: {field}"
                );
            }
        }
    }
}

#[tokio::test]
async fn a_bounded_walk_indexes_the_tool_and_logs_a_warning() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let properties = (0..8_300)
        .map(|index| {
            (
                format!("f{index:04}"),
                serde_json::json!({"type": "boolean"}),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    let document = serde_json::json!({
        "openapi": "3.1.0",
        "info": {"title": "Wide", "version": "1"},
        "servers": [{"url": "https://wide.example"}],
        "paths": {"/wide": {"get": {
            "operationId": "wide",
            "responses": {"200": {
                "description": "ok",
                "content": {"application/json": {"schema": {
                    "type": "object",
                    "properties": properties,
                }}},
            }},
        }}},
    })
    .to_string();
    let wide = world
        .modules
        .publish_openapi(&world.user, &world.workspace, &document, "wide", 40)
        .await;
    let entries = world.stored_entries(&wide).await;

    let logs = CapturedLogs::default();
    {
        let _subscriber = tracing::subscriber::set_default(
            tracing_subscriber::Registry::default().with(logs.clone()),
        );
        notify(&world.index, &entries);
    }
    world.index.settled().await;

    assert_eq!(
        world
            .tool_rows()
            .await
            .iter()
            .map(|row| row.tool_key.as_str())
            .collect::<Vec<_>>(),
        ["wide"]
    );
    let [event] = logs
        .events()
        .into_iter()
        .filter(|event| event.level == tracing::Level::WARN)
        .collect::<Vec<_>>()
        .try_into()
        .expect("one warning");
    assert_eq!(event.target, "coral_search::index");
    for (field, value) in [
        ("coral.workspace.id", world.workspace.to_string()),
        ("coral.provider.id", wide.to_string()),
        (
            "coral.search.generation.id",
            entries.generation_id().to_string(),
        ),
        (
            "coral.search.warning.code",
            "tool_search_material_truncated".to_string(),
        ),
    ] {
        assert_eq!(event.fields.get(field), Some(&value), "{field}");
    }
}

#[tokio::test]
async fn a_failed_attempt_leaves_only_a_failed_state_and_logs_the_error() {
    // A first attempt rolls back its own rows; a failed rewrite also removes
    // the rows an earlier attempt wrote, because a failed Generation counts
    // as not searched.
    for indexed_before in [false, true] {
        let upstream = MockServer::start().await;
        let world = World::publish_tracer(&upstream).await;
        let entries = world.stored_entries(&world.tracer).await;
        if indexed_before {
            notify(&world.index, &entries);
            world.index.settled().await;
            assert!(!world.tool_rows().await.is_empty());
            // A rename is what rewrites an indexed Generation.
            world
                .modules
                .providers
                .rename_provider(
                    &change(world.user.clone(), 70, '7'),
                    &world.workspace,
                    &world.tracer,
                    coral_provider::ProviderDisplayName::parse("Storm Watch").unwrap(),
                )
                .await
                .expect("rename the Provider");
        }
        world.reject_complete_states().await;

        let logs = CapturedLogs::default();
        {
            let _subscriber = tracing::subscriber::set_default(
                tracing_subscriber::Registry::default().with(logs.clone()),
            );
            notify(&world.index, &entries);
        }
        world.index.settled().await;

        assert!(
            world.tool_rows().await.is_empty(),
            "indexed before: {indexed_before}"
        );
        let [state] = world.states().await.try_into().expect("one state row");
        assert_eq!(
            (state.generation_id.as_str(), state.state.as_str()),
            (entries.generation_id().to_string().as_str(), "failed")
        );
        let error = state.error.expect("a failed state carries its error");
        assert!(error.contains(INJECTED_FAULT), "{error}");

        let [event] = logs.events().try_into().expect("the write's error");
        assert_eq!(event.level, tracing::Level::ERROR);
        assert_eq!(event.target, "coral_search::index");
        for (field, value) in [
            ("coral.workspace.id", world.workspace.to_string()),
            ("coral.provider.id", world.tracer.to_string()),
            (
                "coral.search.generation.id",
                entries.generation_id().to_string(),
            ),
            ("error.type", "\"database\"".to_string()),
        ] {
            assert_eq!(event.fields.get(field), Some(&value), "{field}");
        }
    }
}

#[tokio::test]
async fn a_failed_preparation_after_a_notice_leaves_a_failed_state() {
    // Without the state, the Provider reads as pending until a reconcile
    // pass fails the same way and records it.
    for by_generation in [true, false] {
        let upstream = MockServer::start().await;
        let world = World::publish_tracer(&upstream).await;
        let generation = world
            .stored_entries(&world.tracer)
            .await
            .generation_id()
            .clone();
        world
            .modules
            .fixture
            .execute_fixture_sql("ALTER TABLE materialized_tools RENAME TO hidden_tools")
            .await
            .expect("hide the catalog's Tools");

        if by_generation {
            world.index.index_generation(
                world.workspace.clone(),
                world.tracer.clone(),
                generation.clone(),
            );
        } else {
            world
                .index
                .index_active_generation(world.workspace.clone(), world.tracer.clone());
        }
        world.index.settled().await;

        let [state] = world.states().await.try_into().expect("one state row");
        assert_eq!(
            (state.generation_id.as_str(), state.state.as_str()),
            (generation.to_string().as_str(), "failed"),
            "by generation: {by_generation}"
        );
    }
}

#[tokio::test]
async fn a_no_op_regeneration_keeps_the_indexed_generation_and_a_rewrite_leaves_the_same_rows() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let ledger = world.publish_ledger().await;
    let indexed = world.stored_entries(&ledger).await;
    notify(&world.index, &indexed);
    world.index.settled().await;
    let rows = world.tool_rows().await;
    let states = world.states().await;
    assert_eq!(
        rows.iter()
            .map(|row| row.tool_key.as_str())
            .collect::<Vec<_>>(),
        ["listEntries"]
    );

    world
        .modules
        .regenerate_workspace_provider(
            &change(world.user.clone(), 60, '6'),
            &world.workspace,
            &ledger,
            &serde_json::from_str(LEDGER).unwrap(),
        )
        .await;
    let regenerated = world.stored_entries(&ledger).await;
    assert_eq!(
        regenerated.generation_id(),
        indexed.generation_id(),
        "an unchanged document reuses the active Generation, so nothing is notified"
    );
    assert_eq!(world.tool_rows().await, rows);
    assert_eq!(world.states().await, states);

    notify(&world.index, &regenerated);
    world.index.settled().await;
    assert_eq!(
        world.tool_rows().await,
        rows,
        "a rewrite leaves the same rows"
    );
    assert_eq!(world.states().await, states);
}

#[tokio::test]
async fn a_rewrite_removes_rows_its_entries_no_longer_hold() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let entries = world.stored_entries(&world.tracer).await;
    notify(&world.index, &entries);
    world.index.settled().await;
    assert!(!world.tool_rows().await.is_empty());

    let generation_id = entries.generation_id().clone();
    world
        .index
        .write_generation(&entries.without_tools())
        .await
        .expect("write the Generation without its Tools");
    world.index.settled().await;

    assert!(world.tool_rows().await.is_empty());
    assert_eq!(
        world.states().await,
        [StateRow::complete(
            &world.tracer,
            &generation_id.to_string(),
            "Weather Tracer"
        )]
    );
}

#[tokio::test]
async fn a_renamed_provider_rewrites_its_generation_under_the_new_name() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let entries = world.stored_entries(&world.tracer).await;
    notify(&world.index, &entries);
    world.index.settled().await;

    world
        .modules
        .providers
        .rename_provider(
            &change(world.user.clone(), 70, '7'),
            &world.workspace,
            &world.tracer,
            coral_provider::ProviderDisplayName::parse("Storm Watch").unwrap(),
        )
        .await
        .expect("rename the Provider");
    world
        .index
        .index_active_generation(world.workspace.clone(), world.tracer.clone());
    world.index.settled().await;

    let generation = entries.generation_id().to_string();
    let rows = world.tool_rows().await;
    assert_eq!(
        rows.iter()
            .map(|row| (
                row.provider_name.as_str(),
                row.searchable_text.ends_with("stormwatch")
            ))
            .collect::<Vec<_>>(),
        [("weather Storm Watch", true)]
    );
    assert_eq!(
        world.states().await,
        [StateRow::complete(
            &world.tracer,
            &generation,
            "Storm Watch"
        )]
    );
    world.assert_exact_statistics().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn notified_writes_land_in_notification_order() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let entries = world.stored_entries(&world.tracer).await;

    for _ in 0..100 {
        notify(&world.index, &entries);
        world
            .index
            .remove_generation(entries.generation_id().clone());
    }
    world.index.settled().await;

    assert!(world.tool_rows().await.is_empty());
    assert!(world.states().await.is_empty());
}

#[tokio::test]
async fn removals_remove_only_the_generation_or_provider_they_name() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let ledger = world.publish_ledger().await;
    let tracer_entries = world.stored_entries(&world.tracer).await;
    let ledger_entries = world.stored_entries(&ledger).await;
    notify(&world.index, &tracer_entries);
    notify(&world.index, &ledger_entries);
    world.index.settled().await;
    let ledger_generation = ledger_entries.generation_id().to_string();

    world
        .index
        .remove_generation(tracer_entries.generation_id().clone());
    world.index.settled().await;
    assert_eq!(
        world.indexed_generations().await,
        std::slice::from_ref(&ledger_generation)
    );
    assert_eq!(
        world.states().await,
        [StateRow::complete(&ledger, &ledger_generation, "Ledger")]
    );

    notify(&world.index, &tracer_entries);
    world
        .index
        .remove_provider(world.workspace.clone(), ledger.clone());
    world.index.settled().await;
    let tracer_generation = tracer_entries.generation_id().to_string();
    assert_eq!(
        world.indexed_generations().await,
        std::slice::from_ref(&tracer_generation)
    );
    assert_eq!(
        world.states().await,
        [StateRow::complete(
            &world.tracer,
            &tracer_generation,
            "Weather Tracer"
        )]
    );
}

#[tokio::test]
async fn postgres_statistics_follow_each_write_and_removal_of_the_workspace() {
    coral_test_support::database_only!(Postgres);
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let ledger = world.publish_ledger().await;

    let tracer_entries = world.stored_entries(&world.tracer).await;
    let tracer_generation = tracer_entries.generation_id().clone();
    notify(&world.index, &tracer_entries);
    world.index.settled().await;
    let after_tracer = world.statistics().await.expect("statistics after a write");
    assert!(world.has_term("forecast").await);
    assert!(!world.has_term("ledger").await);
    world.assert_exact_statistics().await;

    notify(&world.index, &world.stored_entries(&ledger).await);
    world.index.settled().await;
    let after_ledger = world.statistics().await.expect("statistics after a write");
    assert_ne!(after_ledger, after_tracer);
    assert!(world.has_term("forecast").await);
    assert!(world.has_term("ledger").await);
    world.assert_exact_statistics().await;

    world
        .index
        .remove_provider(world.workspace.clone(), ledger.clone());
    world.index.settled().await;
    assert_eq!(world.statistics().await, Some(after_tracer));
    assert!(world.has_term("forecast").await);
    assert!(!world.has_term("ledger").await);
    world.assert_exact_statistics().await;

    world.index.remove_generation(tracer_generation);
    world.index.settled().await;
    assert!(!world.has_term("forecast").await);
    world.assert_exact_statistics().await;
}

#[tokio::test]
async fn a_rewrite_of_an_unchanged_generation_writes_no_search_row() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    world
        .index
        .index_active_generation(world.workspace.clone(), world.tracer.clone());
    world.index.settled().await;
    let stamps = world.row_stamps().await;
    let versions = world.search_row_versions().await;
    // Only Postgres has row versions and statistics rows; on `SQLite` the
    // stamps show that no row changed.
    let postgres = world.modules.search.database.system() == DatabaseSystem::Postgres;
    if postgres {
        assert!(
            versions.iter().any(|(table, ..)| table == "search_terms"),
            "the write counted the terms"
        );
    }

    world
        .index
        .index_active_generation(world.workspace.clone(), world.tracer.clone());
    world.index.settled().await;

    assert_eq!(world.row_stamps().await, stamps, "no row changed");
    if postgres {
        assert_eq!(
            world.search_row_versions().await,
            versions,
            "no row, term, or totals row was written"
        );
    }
    world.assert_exact_statistics().await;
}

#[tokio::test]
async fn a_write_stalled_past_its_limit_rolls_back_and_frees_later_writes() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let entries = world.stored_entries(&world.tracer).await;
    let index = world
        .index
        .clone()
        .with_write_limit(std::time::Duration::from_millis(200));
    let database = world.modules.search.database.clone();

    let logs = CapturedLogs::default();
    let mut blocker = database.begin().await.expect("hold the writer");
    if database.system() == DatabaseSystem::Postgres {
        blocker
            .execute_sql(
                "LOCK TABLE search_entities IN ACCESS EXCLUSIVE MODE",
                sea_query::Values(Vec::new()),
            )
            .await
            .expect("lock the search table");
    }
    {
        let _subscriber = tracing::subscriber::set_default(
            tracing_subscriber::Registry::default().with(logs.clone()),
        );
        notify(&index, &entries);
    }
    index.settled().await;
    blocker.rollback().await.expect("release the writer");

    assert!(world.tool_rows().await.is_empty());
    let first = logs
        .events()
        .into_iter()
        .find(|event| event.level == tracing::Level::ERROR)
        .expect("the stalled write is logged");
    // SQLite's writer stalls the write before the Workspace's lock;
    // Postgres's table lock stalls it after.
    let expected = match database.system() {
        DatabaseSystem::Sqlite => "\"writer_busy\"",
        DatabaseSystem::Postgres => "\"overran\"",
    };
    assert_eq!(
        first.fields.get("error.type").map(String::as_str),
        Some(expected),
        "{first:?}"
    );

    notify(&index, &entries);
    index.settled().await;
    assert_eq!(
        world.states().await,
        [StateRow::complete(
            &world.tracer,
            &entries.generation_id().to_string(),
            "Weather Tracer"
        )],
        "the stalled write released the mutex"
    );
}

#[tokio::test]
async fn a_failure_waits_for_a_busy_writer_to_record_its_state() {
    // A write that runs past its limit keeps SQLite's one writer until its
    // statement ends. The failure it leaves must still be recorded once the
    // writer is free, or the Provider reads as pending until a reconcile
    // pass. Postgres has no single writer: a blocked write waits on locks.
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    if world.modules.search.database.system() == DatabaseSystem::Postgres {
        return;
    }
    let entries = world.stored_entries(&world.tracer).await;
    let database = Arc::new(
        world
            .modules
            .fixture
            .connect_peer_with_writer_wait(std::time::Duration::from_millis(50))
            .await
            .expect("open a handle with a short writer wait"),
    );
    let index = SearchIndex::new(database.clone(), RECONCILE_INTERVAL)
        .with_write_limit(std::time::Duration::from_secs(1));

    let blocker = database.begin().await.expect("hold the writer");
    notify(&index, &entries);
    tokio::join!(index.settled(), async {
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        blocker.rollback().await.expect("release the writer");
    });

    let [state] = world.states().await.try_into().expect("one state row");
    assert_eq!(
        (state.generation_id.as_str(), state.state.as_str()),
        (entries.generation_id().to_string().as_str(), "failed")
    );
}

/// A small Provider with one Tool and no credentials.
const LEDGER: &str = r#"{
  "openapi": "3.1.0",
  "info": { "title": "Ledger", "version": "1.0.0" },
  "servers": [{ "url": "https://ledger.example" }],
  "paths": {
    "/entries": {
      "get": {
        "operationId": "listEntries",
        "summary": "List ledger entries",
        "tags": ["ledger"],
        "responses": { "200": { "description": "OK" } }
      }
    }
  }
}"#;

/// An OpenAPI document whose one Tool declares callbacks, which Coral
/// cannot call, so its Generation has no available Tool.
const CALLBACKS_ONLY: &str = r#"{
  "openapi": "3.1.0",
  "info": { "title": "Callbacks", "version": "1.0.0" },
  "servers": [{ "url": "https://hooks.example" }],
  "paths": {
    "/subscribe": {
      "post": {
        "operationId": "subscribe",
        "callbacks": {},
        "responses": { "200": { "description": "OK" } }
      }
    }
  }
}"#;

/// One Workspace, one User, and the tracer Provider, published for real.
#[tokio::test]
async fn an_inert_index_writes_nothing_when_notified() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let entries = world.stored_entries(&world.tracer).await;
    let inert = SearchIndex::inert(world.modules.search.database.clone());

    notify(&inert, &entries);
    inert.settled().await;

    assert_eq!(world.indexed_generations().await, Vec::<String>::new());
    assert_eq!(world.states().await, []);
}

#[tokio::test]
async fn search_index_states_follow_each_providers_active_generation() {
    use crate::SearchIndexFailure;
    use crate::SearchIndexState::{Failed, Indexed, Pending};

    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let ledger = world.publish_ledger().await;
    assert_eq!(
        world.search_index_states(None).await,
        BTreeMap::from([(world.tracer.clone(), Pending), (ledger.clone(), Pending)]),
        "a Generation no attempt has reached"
    );

    world.fail_an_attempt().await;
    let failed = || Failed {
        reason: Some(SearchIndexFailure::Database),
    };
    assert_eq!(
        world.search_index_states(None).await,
        BTreeMap::from([(world.tracer.clone(), failed()), (ledger.clone(), failed())]),
        "the injected fault is a database error"
    );

    notify(&world.index, &world.stored_entries(&world.tracer).await);
    world.index.settled().await;
    assert_eq!(
        world.search_index_states(None).await,
        BTreeMap::from([(world.tracer.clone(), Indexed), (ledger.clone(), failed())])
    );

    world
        .modules
        .regenerate_workspace_provider(
            &change(world.user.clone(), 60, '6'),
            &world.workspace,
            &ledger,
            &serde_json::from_str(&LEDGER.replace("List ledger entries", "List every entry"))
                .unwrap(),
        )
        .await;
    assert_eq!(
        world.search_index_states(None).await,
        BTreeMap::from([(world.tracer.clone(), Indexed), (ledger.clone(), Pending)]),
        "a superseded Generation's failure is not the Provider's state"
    );
    assert_eq!(
        world.search_index_states(Some(&world.tracer)).await,
        BTreeMap::from([(world.tracer.clone(), Indexed)])
    );
}

#[tokio::test]
async fn a_failure_without_a_known_kind_reports_no_reason() {
    // A row written before Coral recorded the kind holds none; a row written
    // by a later Coral may hold a kind this one does not know.
    for kind in ["NULL", "'a_later_kind'"] {
        let upstream = MockServer::start().await;
        let world = World::publish_tracer(&upstream).await;
        world.fail_an_attempt().await;
        world
            .modules
            .fixture
            .execute_fixture_sql(&format!(
                "UPDATE search_index_states SET error_kind = {kind}"
            ))
            .await
            .expect("replace the recorded kind");

        assert_eq!(
            world.search_index_states(None).await,
            BTreeMap::from([(
                world.tracer.clone(),
                crate::SearchIndexState::Failed { reason: None }
            )]),
            "error_kind = {kind}"
        );
    }
}

#[tokio::test]
async fn a_failed_state_read_reports_no_states_and_logs_the_error() {
    // Search state is information only (ADR 0017): a read that fails must not
    // fail the Provider inspection that asked for it.
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    world
        .modules
        .fixture
        .execute_fixture_sql("ALTER TABLE search_index_states RENAME TO hidden_index_states")
        .await
        .expect("hide the state rows");

    let logs = CapturedLogs::default();
    let states = {
        let _subscriber = tracing::subscriber::set_default(
            tracing_subscriber::Registry::default().with(logs.clone()),
        );
        world
            .modules
            .search
            .inspect_search_index_states(&world.user, &world.workspace, None)
            .await
            .expect("a failed state read never fails the inspection")
    };

    assert!(states.is_empty(), "{states:?}");
    let [event] = logs.events().try_into().expect("one log event");
    assert_eq!(event.level, tracing::Level::WARN);
    assert_eq!(
        event.fields.get("coral.workspace.id"),
        Some(&world.workspace.to_string())
    );
}

#[tokio::test]
async fn search_index_states_require_workspace_inspection() {
    let upstream = MockServer::start().await;
    let world = World::publish_tracer(&upstream).await;
    let outsider = organization_user(world.modules.search.database.clone(), "user:outsider").await;
    world.fail_an_attempt().await;
    let before = world.states().await;

    let denied = world
        .modules
        .search
        .inspect_search_index_states(&outsider, &world.workspace, None)
        .await;

    assert!(
        matches!(
            denied,
            Err(crate::Error::Workspace(
                coral_workspace::Error::WorkspaceNotFound
            ))
        ),
        "{denied:?}"
    );
    assert_eq!(world.states().await, before, "the denial changed nothing");
}

/// Notifies the index that the Generation the entries name became active, as
/// publication does after its commit: with the ids alone.
fn notify(index: &SearchIndex, entries: &GenerationEntries) {
    index.index_generation(
        entries.workspace_id().clone(),
        entries.provider_id().clone(),
        entries.generation_id().clone(),
    );
}

struct World {
    modules: TestModules,
    user: AuthenticatedUser,
    workspace: WorkspaceId,
    tracer: coral_provider::ProviderId,
    index: SearchIndex,
}

impl World {
    async fn publish_tracer(upstream: &MockServer) -> Self {
        Self::publish_with_modules(TestModules::new().await, upstream).await
    }

    async fn publish_with_modules(modules: TestModules, upstream: &MockServer) -> Self {
        let user = organization_user(modules.search.database.clone(), USER).await;
        let workspace = modules.create_workspace(&user).await;
        let tracer = modules
            .publish_tracer(&user, &workspace, &upstream.uri())
            .await
            .provider_id;
        let index = SearchIndex::new(modules.search.database.clone(), RECONCILE_INTERVAL);
        Self {
            modules,
            user,
            workspace,
            tracer,
            index,
        }
    }

    async fn reconcile_and_settle(&self) {
        self.index.reconcile().await;
        self.index.settled().await;
    }

    /// The active Generation's entries, prepared by the owner from what the
    /// catalog stores.
    async fn stored_entries(&self, provider: &coral_provider::ProviderId) -> GenerationEntries {
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
            provider,
        )
        .await
        .unwrap()
        .expect("published Provider");
        transaction.rollback().await.unwrap();
        entries
    }

    /// Each Provider's search index state, as the owner inspects it.
    async fn search_index_states(
        &self,
        provider: Option<&coral_provider::ProviderId>,
    ) -> BTreeMap<coral_provider::ProviderId, crate::SearchIndexState> {
        self.modules
            .search
            .inspect_search_index_states(&self.user, &self.workspace, provider)
            .await
            .expect("inspect the search index states")
    }

    /// Publishes a second Provider, "ledger", from [`LEDGER`].
    async fn publish_ledger(&self) -> coral_provider::ProviderId {
        self.modules
            .publish_openapi(&self.user, &self.workspace, LEDGER, "ledger", 50)
            .await
    }

    /// The Generations that have Tool rows, in order.
    async fn indexed_generations(&self) -> Vec<String> {
        let mut generations = self
            .tool_rows()
            .await
            .into_iter()
            .map(|row| row.generation_id)
            .collect::<Vec<_>>();
        generations.dedup();
        generations
    }

    /// The Workspace's Postgres statistics row: document count and mean
    /// length.
    async fn statistics(&self) -> Option<(i64, f64)> {
        self.modules
            .search
            .database
            .fetch_optional(
                Query::select()
                    .columns([Alias::new("total_docs"), Alias::new("avgdl")])
                    .from(Alias::new("search_stats"))
                    .and_where(Expr::col(Alias::new("workspace_id")).eq(self.workspace.to_string()))
                    .to_owned(),
            )
            .await
            .expect("read the statistics")
    }

    /// Whether the Workspace's Postgres term statistics hold `term`.
    async fn has_term(&self, term: &str) -> bool {
        let row: Option<(String,)> = self
            .modules
            .search
            .database
            .fetch_optional(
                Query::select()
                    .column(Alias::new("term"))
                    .from(Alias::new("search_terms"))
                    .and_where(Expr::col(Alias::new("workspace_id")).eq(self.workspace.to_string()))
                    .and_where(Expr::col(Alias::new("term")).eq(term))
                    .to_owned(),
            )
            .await
            .expect("read the term statistics");
        row.is_some()
    }

    /// Sends `notice` in a request span, waits for the write it starts, and
    /// returns the write's span. Asserts what every notified write's span
    /// shows: it is a root, marked `notified`, that links to the request and
    /// does not hold the request's span open.
    async fn traced_notice(&self, notice: impl FnOnce(&SearchIndex)) -> SpanData {
        let capture = SpanCapture::default();
        {
            let _subscriber = tracing::subscriber::set_default(capture.subscriber());
            tracing::info_span!("request").in_scope(|| notice(&self.index));
        }
        self.index.settled().await;
        // A database driver may hold the write's span for a moment after the
        // write returns.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        let spans = loop {
            let spans = capture.finished();
            if spans
                .iter()
                .any(|span| span.name.starts_with("coral.search."))
                || tokio::time::Instant::now() > deadline
            {
                break spans;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        };
        let request = spans
            .iter()
            .find(|span| span.name == "request")
            .expect("the request span")
            .clone();
        let [write] = spans
            .into_iter()
            .filter(|span| span.name.starts_with("coral.search."))
            .collect::<Vec<_>>()
            .try_into()
            .expect("one write span");
        assert_eq!(write.parent_span_id, SpanId::INVALID, "a root");
        assert_ne!(
            write.span_context.trace_id(),
            request.span_context.trace_id()
        );
        assert!(
            request.end_time < write.end_time,
            "the write does not hold its notifier's span open"
        );
        assert_eq!(
            write
                .links
                .iter()
                .map(|link| (link.span_context.trace_id(), link.span_context.span_id()))
                .collect::<Vec<_>>(),
            [(
                request.span_context.trace_id(),
                request.span_context.span_id()
            )],
            "the write links to its notifier"
        );
        assert_eq!(
            attribute(&write, "coral.search.write.trigger").as_deref(),
            Some("notified")
        );
        write
    }

    /// Runs one pass and returns the spans it started.
    async fn traced_pass(&self) -> Vec<CapturedSpan> {
        let spans = CapturedSpans::default();
        {
            let _subscriber = tracing::subscriber::set_default(
                tracing_subscriber::Registry::default().with(spans.clone()),
            );
            self.index.reconcile().await;
        }
        spans.recorded()
    }

    /// The version of each of the Workspace's rows in the search-owned tables,
    /// by table and key: Postgres's `xmin`, which every write of a row
    /// changes. `SQLite` keeps no row version, so it has none.
    async fn row_versions(&self) -> Vec<(String, String, String)> {
        let database = &self.modules.search.database;
        if database.system() != DatabaseSystem::Postgres {
            return Vec::new();
        }
        let mut transaction = database.begin_read_snapshot().await.unwrap();
        let versions = transaction
            .fetch_all_sql(
                "SELECT 'search_entities'::text, doc_id, xmin::text \
                 FROM search_entities WHERE workspace_id = $1 \
                 UNION ALL SELECT 'search_index_states', target_id, xmin::text \
                 FROM search_index_states WHERE workspace_id = $1 \
                 UNION ALL SELECT 'search_terms', term, xmin::text \
                 FROM search_terms WHERE workspace_id = $1 \
                 UNION ALL SELECT 'search_stats', workspace_id, xmin::text \
                 FROM search_stats WHERE workspace_id = $1 \
                 ORDER BY 1, 2",
                sea_query::Values(vec![self.workspace.to_string().into()]),
            )
            .await
            .expect("read the row versions");
        transaction.rollback().await.unwrap();
        versions
    }

    /// The versions of the Workspace's Search Entity rows and statistics
    /// rows, as [`Self::row_versions`] reads them: every row but the state
    /// rows, whose attempt time each write changes.
    async fn search_row_versions(&self) -> Vec<(String, String, String)> {
        self.row_versions()
            .await
            .into_iter()
            .filter(|(table, ..)| table != "search_index_states")
            .collect()
    }

    /// When each of the Workspace's Search Entity rows last changed, by
    /// document id.
    async fn row_stamps(&self) -> Vec<(String, i64)> {
        self.modules
            .search
            .database
            .fetch_all(
                Query::select()
                    .columns(["doc_id", "indexed_at_unix_nanos"].map(Alias::new))
                    .from(Alias::new("search_entities"))
                    .and_where(Expr::col(Alias::new("workspace_id")).eq(self.workspace.to_string()))
                    .order_by(Alias::new("doc_id"), Order::Asc)
                    .to_owned(),
            )
            .await
            .expect("read the row stamps")
    }

    /// On Postgres, asserts that the Workspace's stored statistics equal
    /// those counted again from its rows. `SQLite`'s FTS5 keeps its own
    /// statistics, so there is nothing to compare.
    async fn assert_exact_statistics(&self) {
        if self.modules.search.database.system() != DatabaseSystem::Postgres {
            return;
        }
        assert_eq!(
            self.stored_statistics().await,
            self.recounted_statistics().await,
            "the stored statistics match the rows"
        );
    }

    /// The Workspace's Postgres statistics as stored: each term's document
    /// count, then the statistics row's document count and mean length.
    async fn stored_statistics(&self) -> Statistics {
        let mut transaction = self
            .modules
            .search
            .database
            .begin_read_snapshot()
            .await
            .unwrap();
        let terms = transaction
            .fetch_all_sql(
                "SELECT term, ndoc FROM search_terms WHERE workspace_id = $1 ORDER BY term",
                sea_query::Values(vec![self.workspace.to_string().into()]),
            )
            .await
            .expect("read the term statistics");
        transaction.rollback().await.unwrap();
        (terms, self.statistics().await)
    }

    /// The Workspace's Postgres statistics counted again from its rows' vectors,
    /// by the rules the indexer follows: what [`Self::stored_statistics`]
    /// holds when the statistics are exact. The mean length counts each
    /// vector's lexeme positions again, so a stale `doc_len` shows.
    async fn recounted_statistics(&self) -> Statistics {
        let mut transaction = self
            .modules
            .search
            .database
            .begin_read_snapshot()
            .await
            .unwrap();
        let workspace = || sea_query::Values(vec![self.workspace.to_string().into()]);
        let terms = transaction
            .fetch_all_sql(
                "SELECT word, ndoc FROM ts_stat(format( \
                     'SELECT tsv FROM search_entities WHERE workspace_id = %L', $1::text \
                 )) ORDER BY word",
                workspace(),
            )
            .await
            .expect("count the terms");
        let totals: Vec<(i64, f64)> = transaction
            .fetch_all_sql(
                "SELECT count(*), coalesce(avg(coalesce(( \
                     SELECT sum(coalesce(array_length(u.positions, 1), 1)) \
                     FROM unnest(d.tsv) AS u(lexeme, positions, weights) \
                 ), 0)), 1.0)::float8 \
                 FROM search_entities AS d WHERE d.workspace_id = $1",
                workspace(),
            )
            .await
            .expect("count the rows");
        transaction.rollback().await.unwrap();
        (terms, totals.into_iter().next())
    }

    /// Runs one reconcile pass whose every Generation attempt fails with a
    /// database error, then lets later attempts succeed.
    async fn fail_an_attempt(&self) {
        self.reject_complete_states().await;
        self.index.reconcile().await;
        self.allow_complete_states().await;
    }

    /// Injects a persisted outage clock, including stale data an older writer
    /// can leave when it does not know the new columns.
    async fn set_outage_start(&self, at: i64) {
        self.modules.fixture.execute_fixture_sql(&format!(
            "UPDATE search_index_states SET unavailable_since_unix_nanos = {at} WHERE ever_visible = TRUE"
        )).await.unwrap();
    }

    /// Makes every `complete` state row fail to insert, so an attempt fails
    /// after it wrote its Tool rows.
    async fn reject_complete_states(&self) {
        let sql = match self.modules.search.database.system() {
            DatabaseSystem::Sqlite => format!(
                "CREATE TRIGGER inject_index_fault BEFORE INSERT ON search_index_states \
                 WHEN NEW.state = 'complete' \
                 BEGIN SELECT RAISE(ABORT, '{INJECTED_FAULT}'); END;"
            ),
            DatabaseSystem::Postgres => format!(
                "CREATE FUNCTION inject_index_fault() RETURNS trigger LANGUAGE plpgsql \
                 AS $$ BEGIN RAISE EXCEPTION '{INJECTED_FAULT}'; END $$; \
                 CREATE TRIGGER inject_index_fault BEFORE INSERT ON search_index_states \
                 FOR EACH ROW WHEN (NEW.state = 'complete') \
                 EXECUTE FUNCTION inject_index_fault();"
            ),
        };
        self.modules
            .fixture
            .execute_fixture_sql(&sql)
            .await
            .expect("install the index fault");
    }

    /// Makes every state row fail to delete, so a removal fails.
    async fn reject_state_removals(&self) {
        let sql = match self.modules.search.database.system() {
            DatabaseSystem::Sqlite => format!(
                "CREATE TRIGGER inject_removal_fault BEFORE DELETE ON search_index_states \
                 BEGIN SELECT RAISE(ABORT, '{INJECTED_FAULT}'); END;"
            ),
            DatabaseSystem::Postgres => format!(
                "CREATE FUNCTION inject_removal_fault() RETURNS trigger LANGUAGE plpgsql \
                 AS $$ BEGIN RAISE EXCEPTION '{INJECTED_FAULT}'; END $$; \
                 CREATE TRIGGER inject_removal_fault BEFORE DELETE ON search_index_states \
                 FOR EACH ROW EXECUTE FUNCTION inject_removal_fault();"
            ),
        };
        self.modules
            .fixture
            .execute_fixture_sql(&sql)
            .await
            .expect("install the removal fault");
    }

    /// Removes the fault [`Self::reject_state_removals`] installed.
    async fn allow_state_removals(&self) {
        let sql = match self.modules.search.database.system() {
            DatabaseSystem::Sqlite => "DROP TRIGGER inject_removal_fault;",
            DatabaseSystem::Postgres => {
                "DROP TRIGGER inject_removal_fault ON search_index_states; \
                 DROP FUNCTION inject_removal_fault();"
            }
        };
        self.modules
            .fixture
            .execute_fixture_sql(sql)
            .await
            .expect("remove the removal fault");
    }

    /// Removes the fault [`Self::reject_complete_states`] installed.
    async fn allow_complete_states(&self) {
        let sql = match self.modules.search.database.system() {
            DatabaseSystem::Sqlite => "DROP TRIGGER inject_index_fault;",
            DatabaseSystem::Postgres => {
                "DROP TRIGGER inject_index_fault ON search_index_states; \
                 DROP FUNCTION inject_index_fault();"
            }
        };
        self.modules
            .fixture
            .execute_fixture_sql(sql)
            .await
            .expect("remove the index fault");
    }

    /// The Workspace's Tool rows, in document order.
    async fn tool_rows(&self) -> Vec<ToolRow> {
        let rows: Vec<ToolColumns> = self
            .modules
            .search
            .database
            .fetch_all(
                Query::select()
                    .columns(
                        [
                            "doc_id",
                            "provider_id",
                            "provider_generation_id",
                            "tool_key",
                            "call_path",
                            "qualified_name",
                            "title",
                            "description",
                            "provider_name",
                            "searchable_text",
                            "deprecated",
                            "fingerprint",
                        ]
                        .map(Alias::new),
                    )
                    .from(Alias::new("search_entities"))
                    .and_where(Expr::col(Alias::new("workspace_id")).eq(self.workspace.to_string()))
                    .and_where(Expr::col(Alias::new("kind")).eq("provider_tool"))
                    .order_by(Alias::new("doc_id"), Order::Asc)
                    .to_owned(),
            )
            .await
            .expect("read the Tool rows");
        rows.into_iter().map(ToolRow::from).collect()
    }

    /// Leaves the Workspace's Generations as an indexer of `revision` would
    /// have written them, `None` for a Coral without indexer revisions: each
    /// Tool row holds other text, and each state row names `revision`.
    async fn leave_rows_as_written_by(&self, revision: Option<i64>) {
        self.modules
            .search
            .database
            .execute(
                Query::update()
                    .table(Alias::new("search_entities"))
                    .value(Alias::new("searchable_text"), "text of an older indexer")
                    .and_where(Expr::col(Alias::new("workspace_id")).eq(self.workspace.to_string()))
                    .and_where(Expr::col(Alias::new("kind")).eq("provider_tool"))
                    .to_owned(),
            )
            .await
            .expect("rewrite the Tool rows");
        self.modules
            .search
            .database
            .execute(
                Query::update()
                    .table(Alias::new("search_index_states"))
                    .value(Alias::new("indexer_revision"), revision)
                    .and_where(Expr::col(Alias::new("workspace_id")).eq(self.workspace.to_string()))
                    .and_where(Expr::col(Alias::new("kind")).eq("provider_tool"))
                    .to_owned(),
            )
            .await
            .expect("rewrite the Generation state rows");
    }

    /// When each of the Workspace's rows last changed and each of its state
    /// rows was last attempted, by document or target id. A write that
    /// changes no row still records its attempt in the state row.
    async fn write_stamps(&self) -> Vec<(String, i64)> {
        let mut stamps = self.row_stamps().await;
        let states: Vec<(String, i64)> = self
            .modules
            .search
            .database
            .fetch_all(
                Query::select()
                    .columns(["target_id", "attempted_at_unix_nanos"].map(Alias::new))
                    .from(Alias::new("search_index_states"))
                    .and_where(Expr::col(Alias::new("workspace_id")).eq(self.workspace.to_string()))
                    .to_owned(),
            )
            .await
            .expect("read the state stamps");
        stamps.extend(states);
        stamps.sort();
        stamps
    }

    /// Each Generation state row's retry rule, by Generation.
    async fn retries(&self) -> Vec<Retry> {
        let rows: Vec<RetryColumns> = self
            .modules
            .search
            .database
            .fetch_all(
                Query::select()
                    .columns(
                        [
                            "attempted_at_unix_nanos",
                            "failed_attempts",
                            "retry_after_unix_nanos",
                            "failed_coral_version",
                        ]
                        .map(Alias::new),
                    )
                    .from(Alias::new("search_index_states"))
                    .and_where(Expr::col(Alias::new("workspace_id")).eq(self.workspace.to_string()))
                    .and_where(Expr::col(Alias::new("kind")).eq("provider_tool"))
                    .order_by(Alias::new("provider_generation_id"), Order::Asc)
                    .to_owned(),
            )
            .await
            .expect("read the retry rules");
        rows.into_iter()
            .map(|(attempted_at, attempts, retry_after, version)| Retry {
                attempts,
                wait_nanos: retry_after.map(|retry_after| retry_after - attempted_at),
                version_recorded: version
                    .is_some_and(|version| version == env!("CARGO_PKG_VERSION")),
            })
            .collect()
    }

    /// Each Generation state row's attempt time, by Generation.
    async fn attempt_stamps(&self) -> Vec<i64> {
        let rows: Vec<(i64,)> = self
            .modules
            .search
            .database
            .fetch_all(
                Query::select()
                    .column(Alias::new("attempted_at_unix_nanos"))
                    .from(Alias::new("search_index_states"))
                    .and_where(Expr::col(Alias::new("workspace_id")).eq(self.workspace.to_string()))
                    .order_by(Alias::new("target_id"), Order::Asc)
                    .to_owned(),
            )
            .await
            .expect("read the attempt times");
        rows.into_iter().map(|(stamp,)| stamp).collect()
    }

    /// Moves every transient failure's retry time into the past.
    async fn make_retries_due(&self) {
        self.modules
            .fixture
            .execute_fixture_sql(
                "UPDATE search_index_states SET retry_after_unix_nanos = 0 \
                 WHERE retry_after_unix_nanos IS NOT NULL",
            )
            .await
            .expect("make the retries due");
    }

    /// The Workspace's Generation state rows, by Generation.
    async fn states(&self) -> Vec<StateRow> {
        let rows: Vec<StateColumns> = self
            .modules
            .search
            .database
            .fetch_all(
                Query::select()
                    .columns(
                        [
                            "provider_id",
                            "provider_generation_id",
                            "state",
                            "provider_display_name",
                            "error",
                        ]
                        .map(Alias::new),
                    )
                    .from(Alias::new("search_index_states"))
                    .and_where(Expr::col(Alias::new("workspace_id")).eq(self.workspace.to_string()))
                    .and_where(Expr::col(Alias::new("kind")).eq("provider_tool"))
                    .order_by(Alias::new("provider_generation_id"), Order::Asc)
                    .to_owned(),
            )
            .await
            .expect("read the index states");
        rows.into_iter()
            .map(
                |(provider_id, generation_id, state, display_name, error)| StateRow {
                    provider_id,
                    generation_id,
                    state,
                    display_name,
                    error,
                },
            )
            .collect()
    }
}

type RetryColumns = (i64, Option<i64>, Option<i64>, Option<String>);

type StateColumns = (String, String, String, Option<String>, Option<String>);

/// A Workspace's Postgres statistics: each term with its document count, in
/// term order, and the statistics row's document count and mean length.
type Statistics = (Vec<(String, i32)>, Option<(i64, f64)>);

type ToolColumns = (
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    bool,
    String,
);

#[derive(Debug, PartialEq, Eq)]
struct ToolRow {
    doc_id: String,
    provider_id: String,
    generation_id: String,
    tool_key: String,
    call_path: String,
    qualified_name: String,
    title: String,
    description: String,
    provider_name: String,
    searchable_text: String,
    deprecated: bool,
    fingerprint: String,
}

impl From<ToolColumns> for ToolRow {
    fn from(
        (
            doc_id,
            provider_id,
            generation_id,
            tool_key,
            call_path,
            qualified_name,
            title,
            description,
            provider_name,
            searchable_text,
            deprecated,
            fingerprint,
        ): ToolColumns,
    ) -> Self {
        Self {
            doc_id,
            provider_id,
            generation_id,
            tool_key,
            call_path,
            qualified_name,
            title,
            description,
            provider_name,
            searchable_text,
            deprecated,
            fingerprint,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct StateRow {
    provider_id: String,
    generation_id: String,
    state: String,
    display_name: Option<String>,
    error: Option<String>,
}

impl StateRow {
    fn complete(
        provider: &coral_provider::ProviderId,
        generation_id: &str,
        display_name: &str,
    ) -> Self {
        Self {
            provider_id: provider.to_string(),
            generation_id: generation_id.to_string(),
            state: "complete".to_string(),
            display_name: Some(display_name.to_string()),
            error: None,
        }
    }
}

/// A state row's retry rule: the failures in a row, how long after the last
/// one a pass may retry it, and whether this Coral version recorded it.
#[derive(Debug, PartialEq, Eq)]
struct Retry {
    attempts: Option<i64>,
    wait_nanos: Option<i64>,
    version_recorded: bool,
}

impl Retry {
    /// A transient failure a pass retries `wait` after it.
    fn after(attempts: i64, wait: std::time::Duration) -> Self {
        Self {
            attempts: Some(attempts),
            wait_nanos: Some(i64::try_from(wait.as_nanos()).expect("a short wait")),
            version_recorded: true,
        }
    }

    /// A failure a pass retries only under another Coral version.
    const fn on_new_version(attempts: i64) -> Self {
        Self {
            attempts: Some(attempts),
            wait_nanos: None,
            version_recorded: true,
        }
    }

    /// A row with no failure to retry.
    const fn none() -> Self {
        Self {
            attempts: None,
            wait_nanos: None,
            version_recorded: false,
        }
    }
}

/// Records every event of this crate with its level, target, and fields.
#[derive(Clone, Default)]
struct CapturedLogs(Arc<std::sync::Mutex<Vec<CapturedEvent>>>);

#[derive(Debug)]
struct CapturedEvent {
    level: tracing::Level,
    target: String,
    fields: BTreeMap<String, String>,
}

impl CapturedLogs {
    fn events(&self) -> Vec<CapturedEvent> {
        std::mem::take(&mut *self.0.lock().expect("captured logs"))
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CapturedLogs {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _context: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if !event.metadata().target().starts_with("coral_search") {
            return;
        }
        let mut fields = FieldRecorder::default();
        event.record(&mut fields);
        self.0.lock().expect("captured logs").push(CapturedEvent {
            level: *event.metadata().level(),
            target: event.metadata().target().to_string(),
            fields: fields.0,
        });
    }
}

/// Records every span of this crate with its name and the fields recorded on
/// it so far, in the order the spans start. It does not wait for a span to
/// close: a database driver may still hold a span for a moment after the
/// work in it returns.
#[derive(Clone, Default)]
struct CapturedSpans(Arc<std::sync::Mutex<Vec<CapturedSpan>>>);

#[derive(Debug)]
struct CapturedSpan {
    name: &'static str,
    level: tracing::Level,
    fields: BTreeMap<String, String>,
}

/// A captured span's position in [`CapturedSpans`].
struct CapturedSpanIndex(usize);

impl CapturedSpans {
    fn recorded(&self) -> Vec<CapturedSpan> {
        std::mem::take(&mut *self.0.lock().expect("captured spans"))
    }
}

impl CapturedSpan {
    fn u64_field(&self, name: &str) -> Option<u64> {
        self.fields.get(name).and_then(|value| value.parse().ok())
    }
}

impl<S> tracing_subscriber::Layer<S> for CapturedSpans
where
    S: tracing::Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    fn on_new_span(
        &self,
        attributes: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        context: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let Some(span) = context.span(id) else {
            return;
        };
        if !span.metadata().target().starts_with("coral_search") {
            return;
        }
        let mut fields = FieldRecorder::default();
        attributes.record(&mut fields);
        let mut spans = self.0.lock().expect("captured spans");
        span.extensions_mut().insert(CapturedSpanIndex(spans.len()));
        spans.push(CapturedSpan {
            name: span.metadata().name(),
            level: *span.metadata().level(),
            fields: fields.0,
        });
    }

    fn on_record(
        &self,
        id: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        context: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let Some(span) = context.span(id) else {
            return;
        };
        let Some(index) = span
            .extensions()
            .get::<CapturedSpanIndex>()
            .map(|index| index.0)
        else {
            return;
        };
        let mut fields = FieldRecorder::default();
        values.record(&mut fields);
        if let Some(captured) = self.0.lock().expect("captured spans").get_mut(index) {
            captured.fields.extend(fields.0);
        }
    }
}

#[derive(Default)]
struct FieldRecorder(BTreeMap<String, String>);

impl tracing::field::Visit for FieldRecorder {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().to_string(), format!("{value:?}"));
    }
}
