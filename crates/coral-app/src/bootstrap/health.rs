//! Implementation of `grpc.health.v1.Health` for the gRPC surface.
//!
//! The service separates the two questions orchestrators ask, because they
//! demand opposite failure responses. The empty service name — the gRPC
//! convention for overall server health, and what off-the-shelf probers query by
//! default — answers process liveness from a constant: a live process must not
//! report `NotServing` just because its engine is degraded, or a liveness prober
//! restarts the container instead of removing it from rotation. Engine readiness
//! is a separate registered service, [`READINESS_SERVICE_NAME`], so a readiness
//! probe can ask for it explicitly.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::watch;

use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_server::Health;
use tonic_health::pb::{HealthCheckRequest, HealthCheckResponse};

use crate::catalog::discovery::CatalogDiscovery;
use crate::query::QueryAttribution;
use crate::query::manager::{QueryManager, QueryManagerError};
use crate::transport::query_status;
use crate::workspaces::WorkspaceName;

/// Health service name reporting whether the engine can answer for its catalog.
///
/// Readiness lives under its own name so the default (empty-name) check stays a
/// constant-time liveness answer. Probes that want engine health ask for this.
pub const READINESS_SERVICE_NAME: &str = "coral.readiness";

/// How long one engine readiness answer is reused.
///
/// The health service is unauthenticated by design, so without this every caller
/// that can reach the port could drive a catalog resolution per request. It also
/// keeps a normal orchestrator poll interval from doing the work every time.
const READINESS_CACHE_TTL: Duration = Duration::from_secs(2);

/// How long a caller waits on the running probe before answering unready.
///
/// Without a deadline the health RPC hangs on exactly the fault it exists to
/// report: catalog resolution connects to every configured source and has no
/// bound of its own, so a wedged source would wedge the answer instead of
/// reporting `NotServing`. This bounds only how long an answer is waited for —
/// the resolution itself runs to completion in the background — so a slow
/// engine converges to ready over successive polls rather than restarting a
/// doomed resolution on every one.
///
/// It bounds the asynchronous part of that work. A probe stalled inside a
/// blocking call — `load_query_sources` takes the state lock with a blocking
/// `flock` — cannot be preempted, so it holds a runtime thread until it
/// returns; this deadline still frees the RPC to answer.
///
/// Keep it below coral-mcp's `READINESS_TIMEOUT`, the client-side deadline the
/// authenticated `/readyz` applies to this RPC, so the answer comes from here
/// rather than from the client giving up. Changing either means revisiting both.
const READINESS_PROBE_TIMEOUT: Duration = Duration::from_millis(1_500);

type ReadinessFuture = Pin<Box<dyn Future<Output = bool> + Send>>;

/// Whether the server can still answer for its catalog.
///
/// The health service is the only unauthenticated RPC on the gRPC surface, which
/// makes it the one place a readiness probe can reach without a bearer token.
/// Answering from the engine here is what keeps an authenticated `/readyz` from
/// degenerating into a port check: the alternative — probing a data-plane RPC
/// from outside — either needs a token or an authentication bypass.
#[derive(Clone)]
pub(super) struct EngineReadiness {
    probe: Arc<dyn Fn() -> ReadinessFuture + Send + Sync>,
    cached: Arc<Mutex<Option<(Instant, bool)>>>,
    /// The resolution currently running, shared by everyone waiting on it.
    ///
    /// The TTL alone bounds the steady-state rate but not concurrency: every
    /// caller arriving in the instant the cache expires would otherwise launch
    /// its own resolution, letting unauthenticated health traffic fan out into
    /// arbitrary engine work. Occupying this slot for the length of one probe is
    /// what collapses that burst into a single catalog resolution.
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

    pub(super) fn from_query_manager(queries: QueryManager) -> Self {
        Self::new(Arc::new(move || {
            let catalog = CatalogDiscovery::new(queries.clone());
            Box::pin(async move {
                readiness_from_catalog(
                    catalog
                        .catalog_info(
                            &WorkspaceName::default(),
                            None,
                            None,
                            &QueryAttribution::new(None),
                        )
                        .await,
                )
            })
        }))
    }

    #[cfg(test)]
    pub(super) fn fixed(ready: bool) -> Self {
        Self::new(Arc::new(move || Box::pin(std::future::ready(ready))))
    }

    /// Reads the cached answer while it is fresh, else waits on one probe.
    ///
    /// Exactly one probe runs at a time and every caller arriving during it
    /// shares that one answer, so a burst of health traffic resolves the catalog
    /// once rather than once per request.
    ///
    /// The deadline bounds this *answer*, not the resolution behind it. A
    /// catalog that legitimately takes longer than one poll allows keeps
    /// resolving after this returns unready and records what it finds, so the
    /// next poll reports ready — whereas cancelling it would discard the work
    /// and restart from nothing every time, and an engine slower than the
    /// deadline could never report ready at all.
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
                // panicked, which is not an engine that can answer.
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
    /// caller's deadline nor its disconnection can cancel a resolution already
    /// under way: whoever asks next still gets the answer it paid for.
    ///
    /// Whether to probe is decided under the slot, cache included, because a
    /// caller reads the cache before it reaches this lock. A probe finishing in
    /// between records its answer and vacates the slot, so that caller arrives
    /// to an empty slot holding a fresh answer; without re-reading the cache
    /// here it would resolve the catalog again anyway. Deciding under the lock
    /// is what keeps the TTL a bound on how often unauthenticated health
    /// traffic can drive a resolution, rather than only on the steady state.
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

/// Turns one catalog resolution into a readiness answer.
///
/// A rejection is not automatically an unready instance.
fn readiness_from_catalog<T>(outcome: Result<T, QueryManagerError>) -> bool {
    match outcome {
        Ok(_) => true,
        Err(error) => !matches!(
            query_status(error).code(),
            tonic::Code::Cancelled
                | tonic::Code::Unknown
                | tonic::Code::DeadlineExceeded
                | tonic::Code::Unimplemented
                | tonic::Code::Internal
                | tonic::Code::Unavailable
                | tonic::Code::DataLoss
        ),
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
            // Process liveness: answered from a constant so a degraded engine
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

    use tonic_health::pb::HealthCheckRequest;
    use tonic_health::pb::health_check_response::ServingStatus;
    use tonic_health::pb::health_server::Health as _;

    use coral_engine::CoreError;

    use super::{
        AggregateHealthService, EngineReadiness, READINESS_PROBE_TIMEOUT, READINESS_SERVICE_NAME,
        readiness_from_catalog,
    };
    use crate::bootstrap::AppError;
    use crate::query::manager::QueryManagerError;

    async fn check(service: &str, ready: bool) -> Result<i32, tonic::Status> {
        AggregateHealthService::new(EngineReadiness::fixed(ready))
            .check(tonic::Request::new(HealthCheckRequest {
                service: service.to_string(),
            }))
            .await
            .map(|response| response.into_inner().status)
    }

    #[tokio::test]
    async fn liveness_stays_serving_while_the_engine_is_unready() {
        for ready in [true, false] {
            assert_eq!(
                check("", ready).await.expect("liveness check"),
                ServingStatus::Serving as i32,
                "liveness must not follow engine readiness"
            );
        }
    }

    #[tokio::test]
    async fn the_readiness_service_reports_engine_readiness() {
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
    async fn readiness_probes_the_engine_once_within_the_cache_window() {
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
            "the unauthenticated readiness check must not resolve the catalog per request"
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
            "losing the race to a finished probe must not buy a second catalog resolution"
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
            "concurrent public health traffic must not fan out into parallel engine work"
        );
    }

    // Paused time lets the probe deadline elapse without a wall-clock wait.
    #[tokio::test(start_paused = true)]
    async fn a_wedged_probe_answers_unready_instead_of_hanging() {
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&calls);
        // Never resolves: the catalog resolution wedging is precisely the fault
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
            "a wedged probe still occupies the slot, so polling must not pile up resolutions"
        );
    }

    /// The regression that makes a deadline dangerous: cancelling the
    /// resolution to answer on time would discard the work, so an engine slower
    /// than the deadline would restart from nothing every poll and never report
    /// ready. The answer is bounded; the resolution is not.
    #[tokio::test(start_paused = true)]
    async fn a_slow_engine_reports_ready_on_a_later_poll() {
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
            "the first poll gives up waiting before the catalog finishes"
        );

        // Let the resolution the first poll walked away from finish.
        tokio::time::sleep(Duration::from_millis(400)).await;

        assert!(
            readiness.is_ready().await,
            "the resolution must survive the deadline and record what it found"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "the second poll must read the first probe's answer, not start another"
        );
    }

    #[test]
    fn a_resolved_catalog_reports_ready() {
        assert!(readiness_from_catalog(Ok(())));
    }

    #[test]
    fn instance_wide_catalog_rejections_still_report_ready() {
        for error in [
            AppError::WorkspaceNotFound("default".to_string()),
            AppError::InvalidInput(
                "catalog runtime schema 'public' is owned by both 'a' and 'b'".to_string(),
            ),
            AppError::FailedPrecondition("source 'a' is missing credentials".to_string()),
        ] {
            let described = error.to_string();
            assert!(
                readiness_from_catalog::<()>(Err(QueryManagerError::App(error))),
                "request-shaped rejection must not report the instance unready: {described}"
            );
        }
    }

    #[test]
    fn infrastructure_faults_report_unready() {
        assert!(
            !readiness_from_catalog::<()>(Err(QueryManagerError::App(AppError::Internal(
                "config store lock poisoned".to_string()
            )))),
            "an engine that cannot answer at all is not ready"
        );
        assert!(!readiness_from_catalog::<()>(Err(QueryManagerError::Core(
            CoreError::Unavailable("backend down".to_string())
        ))),);
    }
}
