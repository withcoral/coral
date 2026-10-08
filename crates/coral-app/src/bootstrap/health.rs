//! Implementation of `grpc.health.v1.Health` for the gRPC surface.
//!
//! The service separates the two questions orchestrators ask, because they
//! demand opposite failure responses. The empty service name — the gRPC
//! convention for overall server health, and what off-the-shelf probers query by
//! default — answers process liveness from a constant: a live process must not
//! report `NotServing` just because its engine is degraded, or a liveness prober
//! restarts the container instead of removing it from rotation. Service
//! readiness is a separate registered service, [`READINESS_SERVICE_NAME`], so a
//! readiness probe can ask for it explicitly.
//!
//! Readiness asks whether this service can serve at all: it is up, and the
//! database it reads and writes answers. It deliberately says nothing about any
//! one workspace's catalog. A catalog answer would be a stronger signal, but it
//! is a signal about a workspace and its sources rather than about this
//! instance, and a probe that pulls a replica out of rotation cannot act on it.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::watch;

use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_server::Health;
use tonic_health::pb::{HealthCheckRequest, HealthCheckResponse};

use crate::state::db::CoralDb;

/// Health service name reporting whether the service can reach its database.
///
/// Readiness lives under its own name so the default (empty-name) check stays a
/// constant-time liveness answer. Probes that want service health ask for this.
pub const READINESS_SERVICE_NAME: &str = "coral.readiness";

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
///
/// Keep it below coral-mcp's `READINESS_TIMEOUT`, the client-side deadline the
/// authenticated `/readyz` applies to this RPC, so the answer comes from here
/// rather than from the client giving up. Changing either means revisiting both.
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
pub(super) struct EngineReadiness {
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
}

impl EngineReadiness {
    fn new(probe: Arc<dyn Fn() -> ReadinessFuture + Send + Sync>) -> Self {
        Self {
            probe,
            cached: Arc::new(Mutex::new(None)),
            in_flight: Arc::new(Mutex::new(None)),
        }
    }

    /// Reports readiness from one round trip to the database this server serves
    /// out of.
    ///
    /// Every RPC worth routing here reads or writes that database, so a server
    /// that cannot reach it can answer nothing and belongs out of rotation;
    /// one that can reach it is as ready as an instance-wide probe can
    /// establish.
    ///
    /// Readiness asks nothing about a workspace. Resolving a workspace's
    /// catalog would report more, but what it reports is a property of that
    /// workspace and the sources it names — which workspaces exist at all is a
    /// tenancy question, and a source outside this deployment failing is not
    /// this replica being unfit to serve. Neither is something a probe that
    /// empties a fleet can act on.
    pub(super) fn from_database(database: Arc<CoralDb>) -> Self {
        Self::new(Arc::new(move || {
            let database = Arc::clone(&database);
            Box::pin(async move { database.ping().await.is_ok() })
        }))
    }

    #[cfg(test)]
    pub(super) fn fixed(ready: bool) -> Self {
        Self::new(Arc::new(move || Box::pin(std::future::ready(ready))))
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
    async fn is_ready(&self) -> bool {
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

pub(super) struct AggregateHealthService {
    readiness: EngineReadiness,
}

impl AggregateHealthService {
    pub(super) fn new(readiness: EngineReadiness) -> Self {
        Self { readiness }
    }
}

impl std::fmt::Debug for AggregateHealthService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AggregateHealthService")
            .finish_non_exhaustive()
    }
}

pub(super) type HealthWatchStream = Pin<
    Box<
        dyn tonic::codegen::tokio_stream::Stream<Item = Result<HealthCheckResponse, tonic::Status>>
            + Send,
    >,
>;

#[tonic::async_trait]
impl Health for AggregateHealthService {
    async fn check(
        &self,
        request: tonic::Request<HealthCheckRequest>,
    ) -> Result<tonic::Response<HealthCheckResponse>, tonic::Status> {
        let status = match request.get_ref().service.as_str() {
            // Process liveness: answered from a constant so a degraded service
            // never tells a liveness prober to restart the process.
            "" => ServingStatus::Serving,
            READINESS_SERVICE_NAME if self.readiness.is_ready().await => ServingStatus::Serving,
            READINESS_SERVICE_NAME => ServingStatus::NotServing,
            _ => return Err(tonic::Status::not_found("service not registered")),
        };
        Ok(tonic::Response::new(HealthCheckResponse {
            status: status as i32,
        }))
    }

    type WatchStream = HealthWatchStream;

    async fn watch(
        &self,
        _request: tonic::Request<HealthCheckRequest>,
    ) -> Result<tonic::Response<Self::WatchStream>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "health watch is not supported",
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use tempfile::TempDir;
    use tonic_health::pb::HealthCheckRequest;
    use tonic_health::pb::health_check_response::ServingStatus;
    use tonic_health::pb::health_server::Health as _;

    use super::{
        AggregateHealthService, EngineReadiness, READINESS_PROBE_TIMEOUT, READINESS_SERVICE_NAME,
    };
    use crate::state::AppStateLayout;
    use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig};

    /// One instance's own migrated database, with the readiness probe the
    /// server mounts over it.
    struct ReadinessFixture {
        _temp: TempDir,
        db: Arc<CoralDb>,
        readiness: EngineReadiness,
    }

    async fn readiness_fixture() -> ReadinessFixture {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(&layout).expect("db config")
        else {
            panic!("the default test database is sqlite")
        };
        let db = Arc::new(
            CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
                .await
                .expect("open sqlite"),
        );
        db.migrate().await.expect("migrate sqlite");
        ReadinessFixture {
            _temp: temp,
            readiness: EngineReadiness::from_database(Arc::clone(&db)),
            db,
        }
    }

    /// A fresh install owns no workspace and no source. Readiness is a question
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
        fixture.db.close_for_tests().await;

        assert!(
            !fixture.readiness.is_ready().await,
            "a server that cannot reach its database is not ready to serve"
        );
    }

    async fn check(service: &str, ready: bool) -> Result<i32, tonic::Status> {
        AggregateHealthService::new(EngineReadiness::fixed(ready))
            .check(tonic::Request::new(HealthCheckRequest {
                service: service.to_string(),
            }))
            .await
            .map(|response| response.into_inner().status)
    }

    #[tokio::test]
    async fn liveness_stays_serving_while_the_service_is_unready() {
        for ready in [true, false] {
            assert_eq!(
                check("", ready).await.expect("liveness check"),
                ServingStatus::Serving as i32,
                "liveness must not follow service readiness"
            );
        }
    }

    #[tokio::test]
    async fn the_readiness_service_reports_service_readiness() {
        assert_eq!(
            check(READINESS_SERVICE_NAME, true)
                .await
                .expect("readiness check"),
            ServingStatus::Serving as i32
        );
        assert_eq!(
            check(READINESS_SERVICE_NAME, false)
                .await
                .expect("readiness check"),
            ServingStatus::NotServing as i32
        );
    }

    #[tokio::test]
    async fn other_service_names_are_not_registered() {
        let error = check("coral.v1.QueryService", true)
            .await
            .expect_err("unknown services must not resolve");
        assert_eq!(error.code(), tonic::Code::NotFound);
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
    async fn a_caller_racing_probe_completion_reuses_the_answer_it_missed() {
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&calls);
        let readiness = EngineReadiness::new(Arc::new(move || {
            counted.fetch_add(1, Ordering::SeqCst);
            Box::pin(std::future::ready(true))
        }));

        // Leaves the cache fresh and the slot empty — the state a caller sees
        // when it reads the cache just before a probe records its answer and
        // then reaches the slot just after that probe vacates it. Calling
        // `in_flight_probe` directly is that interleaving without the timing.
        assert!(readiness.is_ready().await);
        let mut answer = readiness.in_flight_probe();

        assert_eq!(
            *answer.borrow_and_update(),
            Some(true),
            "the racing caller must be handed the answer already in the cache"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "losing the race to a finished probe must not buy a second database round trip"
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
