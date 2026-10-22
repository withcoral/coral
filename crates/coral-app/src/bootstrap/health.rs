//! Application-owned readiness: database reachability with a bounded, cached,
//! single-flight probe shared with listener composition, and the process's
//! shutdown, from which on it is never ready. Transport modules only project
//! its boolean answer. Workspace catalog readiness is a domain concern.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::watch;

use coral_db::Database;

/// How long one readiness answer is reused.
///
/// The health service is unauthenticated by design, so without this every caller
/// that can reach the port could drive a database round trip per request. It
/// also keeps a normal orchestrator poll interval from doing the work every
/// time.
const READINESS_CACHE_TTL: Duration = Duration::from_secs(2);

/// How long a caller waits on the running probe before answering unready.
///
/// Without a deadline the health RPC hangs on exactly the fault it exists to
/// report: a database that accepts the round trip and never answers it would
/// wedge the health answer instead of reporting `NotServing`. This bounds only
/// how long an answer is waited for — the check itself runs to completion in
/// the background — so a slow database converges to ready over successive polls
/// rather than restarting a doomed check on every one.
const READINESS_PROBE_TIMEOUT: Duration = Duration::from_millis(1_500);

type ReadinessFuture = Pin<Box<dyn Future<Output = bool> + Send>>;

/// Whether the server can still reach the state it serves out of.
///
/// The health service is the only unauthenticated RPC on the gRPC surface, which
/// makes it the one place a readiness probe can reach without a bearer token.
/// Answering from the database here is what keeps an authenticated `/readyz`
/// from degenerating into a port check: the alternative — probing a data-plane
/// RPC from outside — either needs a token or an authentication bypass.
#[derive(Clone)]
pub struct EngineReadiness {
    probe: Arc<dyn Fn() -> ReadinessFuture + Send + Sync>,
    cached: Arc<Mutex<Option<(Instant, bool)>>>,
    /// The check currently running, shared by everyone waiting on it.
    ///
    /// The TTL alone bounds the steady-state rate but not concurrency: every
    /// caller arriving in the instant the cache expires would otherwise launch
    /// its own check, letting unauthenticated health traffic fan out into
    /// database work. Occupying this slot for the length of one probe is what
    /// collapses that burst into a single round trip.
    in_flight: Arc<Mutex<Option<watch::Receiver<Option<bool>>>>>,
    /// The process is shutting down: its Shutdown Drain lets the live Runs
    /// end while the orchestrator routes new traffic elsewhere.
    stopping: Arc<AtomicBool>,
}

impl EngineReadiness {
    fn new(probe: Arc<dyn Fn() -> ReadinessFuture + Send + Sync>) -> Self {
        Self {
            probe,
            cached: Arc::new(Mutex::new(None)),
            in_flight: Arc::new(Mutex::new(None)),
            stopping: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Reports not ready from now on, on every listener that shares this
    /// readiness: the process is shutting down.
    pub(crate) fn stop_serving(&self) {
        self.stopping.store(true, Ordering::SeqCst);
    }

    /// Reports readiness from one round trip to the database this server serves
    /// out of.
    ///
    /// Every RPC worth routing here reads or writes that database, so a server
    /// that cannot reach it can answer nothing and belongs out of rotation;
    /// one that can reach it is as ready as an instance-wide probe can
    /// establish.
    ///
    /// Readiness asks nothing about a Workspace. Resolving a Workspace's
    /// catalog would report more, but what it reports is a property of that
    /// Workspace and its Providers — which Workspaces exist at all is a
    /// tenancy question, and an Upstream Service failing is not
    /// this replica being unfit to serve. Neither is something a probe that
    /// empties a fleet can act on.
    #[must_use]
    pub fn from_database(database: Arc<Database>) -> Self {
        Self::new(Arc::new(move || {
            let database = Arc::clone(&database);
            Box::pin(async move { database.ping().await.is_ok() })
        }))
    }

    /// Returns the callback used by HTTP listener composition.
    #[must_use]
    pub fn probe(&self) -> coral_mcp::ReadinessProbe {
        let readiness = self.clone();
        Arc::new(move || {
            let readiness = readiness.clone();
            Box::pin(async move { readiness.is_ready().await })
        })
    }

    /// Reads the cached answer while it is fresh, else waits on one probe.
    ///
    /// Exactly one probe runs at a time and every caller arriving during it
    /// shares that one answer, so a burst of health traffic costs one database
    /// round trip rather than one per request.
    ///
    /// The deadline bounds this *answer*, not the check behind it. A database
    /// that legitimately takes longer than one poll allows keeps answering
    /// after this returns unready and records what it found, so the next poll
    /// reports ready — whereas cancelling it would discard the work and restart
    /// from nothing every time, and a database slower than the deadline could
    /// never report ready at all.
    pub async fn is_ready(&self) -> bool {
        if self.stopping.load(Ordering::SeqCst) {
            return false;
        }
        if let Some(ready) = self.fresh_answer() {
            return ready;
        }
        let mut answer = self.in_flight_probe();
        let settled = tokio::time::timeout(READINESS_PROBE_TIMEOUT, async move {
            loop {
                if let Some(ready) = *answer.borrow_and_update() {
                    return ready;
                }
                // The sender is dropped without a value only if the probe task
                // panicked, which is not a service that can answer.
                if answer.changed().await.is_err() {
                    return false;
                }
            }
        });
        settled.await.unwrap_or(false)
    }

    /// Subscribes to the running probe, starting one if none is running.
    ///
    /// The probe is spawned rather than awaited in place so that neither this
    /// caller's deadline nor its disconnection can cancel a check already under
    /// way: whoever asks next still gets the answer it paid for.
    ///
    /// Whether to probe is decided under the slot, cache included, because a
    /// caller reads the cache before it reaches this lock. A probe finishing in
    /// between records its answer and vacates the slot, so that caller arrives
    /// to an empty slot holding a fresh answer; without re-reading the cache
    /// here it would check the database again anyway. Deciding under the lock
    /// is what keeps the TTL a bound on how often unauthenticated health
    /// traffic can drive a check, rather than only on the steady state.
    fn in_flight_probe(&self) -> watch::Receiver<Option<bool>> {
        let Ok(mut slot) = self.in_flight.lock() else {
            // A poisoned slot costs single-flight, not correctness.
            let (_, receiver) = watch::channel(Some(false));
            return receiver;
        };
        if let Some(running) = slot.as_ref() {
            return running.clone();
        }
        // Ordering note: this is the only place the two locks nest, and it
        // takes them slot-then-cache. The probe below records its answer and
        // clears the slot in separate critical sections, never holding both.
        if let Some(ready) = self.fresh_answer() {
            let (_, receiver) = watch::channel(Some(ready));
            return receiver;
        }
        let (sender, receiver) = watch::channel(None);
        let probing = self.clone();
        tokio::spawn(async move {
            let ready = (probing.probe)().await;
            // Recorded before the slot is released, so the re-read above never
            // sees a vacated slot without the answer that vacated it.
            if let Ok(mut cached) = probing.cached.lock() {
                *cached = Some((Instant::now(), ready));
            }
            if let Ok(mut slot) = probing.in_flight.lock() {
                *slot = None;
            }
            // Fails only when every waiter already gave up on this probe, which
            // the cache above has answered for.
            let _delivered = sender.send(Some(ready));
        });
        *slot = Some(receiver.clone());
        receiver
    }

    fn fresh_answer(&self) -> Option<bool> {
        let cached = self.cached.lock().ok()?;
        cached
            .filter(|(recorded, _)| recorded.elapsed() < READINESS_CACHE_TTL)
            .map(|(_, ready)| ready)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::{EngineReadiness, READINESS_PROBE_TIMEOUT};
    use coral_test_support::TestDatabase;

    /// One instance's own migrated database, with the readiness probe the
    /// server mounts over it.
    struct ReadinessFixture {
        database: TestDatabase,
        readiness: EngineReadiness,
    }

    async fn readiness_fixture() -> ReadinessFixture {
        let database = TestDatabase::new(coral_db::migration_plan())
            .await
            .expect("migrated database");
        ReadinessFixture {
            readiness: EngineReadiness::from_database(database.database()),
            database,
        }
    }

    /// A fresh install owns no Workspace or Provider. Readiness is a question
    /// about this service, not about what has been put in it, so an empty
    /// instance whose database answers is ready to serve.
    #[tokio::test]
    async fn an_instance_with_a_reachable_database_reports_ready() {
        let fixture = readiness_fixture().await;

        assert!(fixture.readiness.is_ready().await);
    }

    /// The fault this probe exists to report: every RPC worth routing here
    /// reads or writes the database, so a server that cannot reach it can
    /// answer nothing and must leave the rotation.
    #[tokio::test]
    async fn an_unreachable_database_reports_unready() {
        let fixture = readiness_fixture().await;
        fixture.database.database().close().await;

        assert!(
            !fixture.readiness.is_ready().await,
            "a server that cannot reach its database is not ready to serve"
        );
    }

    /// Once shutdown begins the instance leaves the rotation at once, on
    /// every listener sharing the readiness, whatever the cached answer says.
    #[tokio::test]
    async fn a_stopping_instance_reports_unready_despite_a_cached_ready_answer() {
        let fixture = readiness_fixture().await;
        let shared = fixture.readiness.clone();
        assert!(fixture.readiness.is_ready().await);

        fixture.readiness.stop_serving();

        assert!(!fixture.readiness.is_ready().await);
        assert!(!shared.is_ready().await, "clones share the shutdown");
    }

    #[tokio::test]
    async fn readiness_probes_the_database_once_within_the_cache_window() {
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&calls);
        let readiness = EngineReadiness::new(Arc::new(move || {
            counted.fetch_add(1, Ordering::SeqCst);
            Box::pin(std::future::ready(true))
        }));

        for _ in 0..5 {
            assert!(readiness.is_ready().await);
        }

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "the unauthenticated readiness check must not hit the database per request"
        );
    }

    #[tokio::test]
    async fn concurrent_readiness_checks_share_one_probe() {
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&calls);
        // Yields inside the probe so every task is queued before the first
        // one finishes: without shared in-flight probing each would run its own.
        let readiness = EngineReadiness::new(Arc::new(move || {
            let counted = Arc::clone(&counted);
            Box::pin(async move {
                counted.fetch_add(1, Ordering::SeqCst);
                tokio::task::yield_now().await;
                true
            })
        }));

        let (first, second, third, fourth) = tokio::join!(
            readiness.is_ready(),
            readiness.is_ready(),
            readiness.is_ready(),
            readiness.is_ready(),
        );
        assert!(first && second && third && fourth);

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "concurrent public health traffic must not fan out into parallel database work"
        );
    }

    // Paused time lets the probe deadline elapse without a wall-clock wait.
    #[tokio::test(start_paused = true)]
    async fn a_wedged_probe_answers_unready_instead_of_hanging() {
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&calls);
        // Never resolves: a database that never answers is precisely the fault
        // this service exists to report, so it must not also silence it.
        let readiness = EngineReadiness::new(Arc::new(move || {
            counted.fetch_add(1, Ordering::SeqCst);
            Box::pin(std::future::pending::<bool>())
        }));

        assert!(
            !readiness.is_ready().await,
            "a wedged probe must answer NotServing rather than hold the health RPC"
        );
        assert!(!readiness.is_ready().await);

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "a wedged probe still occupies the slot, so polling must not pile up checks"
        );
    }

    /// The regression that makes a deadline dangerous: cancelling the check to
    /// answer on time would discard the work, so a database slower than the
    /// deadline would restart from nothing every poll and never report ready.
    /// The answer is bounded; the check is not.
    #[tokio::test(start_paused = true)]
    async fn a_slow_database_reports_ready_on_a_later_poll() {
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&calls);
        let slow = READINESS_PROBE_TIMEOUT + Duration::from_millis(200);
        let readiness = EngineReadiness::new(Arc::new(move || {
            counted.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                tokio::time::sleep(slow).await;
                true
            })
        }));

        assert!(
            !readiness.is_ready().await,
            "the first poll gives up waiting before the check finishes"
        );

        // Let the check the first poll walked away from finish.
        tokio::time::sleep(Duration::from_millis(400)).await;

        assert!(
            readiness.is_ready().await,
            "the check must survive the deadline and record what it found"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "the second poll must read the first probe's answer, not start another"
        );
    }
}
