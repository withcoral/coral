//! gRPC health encoding; application composition owns readiness calculation.
use std::{future::Future, pin::Pin, sync::Arc};
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_server::Health;
use tonic_health::pb::{HealthCheckRequest, HealthCheckResponse};

/// Readiness callback supplied by the application, with its own caching and deadline.
pub type ReadinessProbe = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = bool> + Send>> + Send + Sync>;

/// Named readiness check; the empty service name always reports process liveness.
pub const READINESS_SERVICE_NAME: &str = "coral.readiness";

pub(super) struct AggregateHealthService {
    readiness: ReadinessProbe,
}

impl AggregateHealthService {
    pub(super) fn new(readiness: ReadinessProbe) -> Self {
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
            READINESS_SERVICE_NAME if (self.readiness)().await => ServingStatus::Serving,
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
