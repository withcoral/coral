//! Bounded observed-values writer lifecycle and durable queue handoff.

use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use serde::Serialize;
use tokio::sync::mpsc::{self, Receiver, Sender, error::TrySendError};
use uuid::Uuid;

use crate::search::observed::sqlite_queue::{
    ObservedValueCandidate, ObservedValuesEnqueueResult, ObservedValuesEpoch,
    ObservedValuesQueueJob, ObservedValuesQueuePayload, ObservedValuesSurfaceKind,
};
use crate::search::observed::sqlite_store::SqliteObservedValuesStore;
use crate::workspaces::WorkspaceName;

const OBSERVED_VALUES_WRITE_QUEUE_CAPACITY: usize = 128;

#[derive(Debug, Clone)]
pub(super) struct ObservedValuesWriter {
    shared: Arc<ObservedValuesWriterShared>,
}

#[derive(Debug)]
struct ObservedValuesWriterShared {
    sender: Mutex<Option<Sender<ObservedValuesWrite>>>,
    join_handle: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Debug)]
pub(super) struct ObservedValuesWrite {
    pub(super) workspace_name: WorkspaceName,
    pub(super) owner_source_name: String,
    pub(super) source_name: String,
    pub(super) source_scope_id: String,
    pub(super) surface_kind: ObservedValuesSurfaceKind,
    pub(super) surface_name: String,
    pub(super) payload_json: String,
    pub(super) epoch: ObservedValuesEpoch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ObservedValuesTryReserveError {
    Full,
    Disconnected,
}

pub(super) struct ObservedValuesWritePermit {
    permit: mpsc::OwnedPermit<ObservedValuesWrite>,
}

impl ObservedValuesWritePermit {
    pub(super) fn send(self, write: ObservedValuesWrite) {
        self.permit.send(write);
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum ObservedValuesWriterShutdownError {
    #[error("writer mutex poisoned")]
    MutexPoisoned,
    #[error("writer thread panicked")]
    Panicked,
}

impl ObservedValuesWriter {
    pub(super) fn start(store: SqliteObservedValuesStore) -> Self {
        let (sender, receiver) = mpsc::channel(OBSERVED_VALUES_WRITE_QUEUE_CAPACITY);
        let join_handle = match std::thread::Builder::new()
            .name("coral-observed-values-writer".to_string())
            .spawn(move || run_observed_values_writer(&store, receiver))
        {
            Ok(join_handle) => Some(join_handle),
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "failed to start observed-values background writer"
                );
                None
            }
        };
        Self {
            shared: Arc::new(ObservedValuesWriterShared {
                sender: Mutex::new(Some(sender)),
                join_handle: Mutex::new(join_handle),
            }),
        }
    }

    pub(super) fn try_reserve(
        &self,
    ) -> Result<ObservedValuesWritePermit, ObservedValuesTryReserveError> {
        let Ok(sender) = self.shared.sender.lock() else {
            return Err(ObservedValuesTryReserveError::Disconnected);
        };
        let Some(sender) = sender.as_ref() else {
            return Err(ObservedValuesTryReserveError::Disconnected);
        };
        sender
            .clone()
            .try_reserve_owned()
            .map(|permit| ObservedValuesWritePermit { permit })
            .map_err(|error| match error {
                TrySendError::Full(_) => ObservedValuesTryReserveError::Full,
                TrySendError::Closed(_) => ObservedValuesTryReserveError::Disconnected,
            })
    }

    pub(super) fn shutdown(&self) -> Result<(), ObservedValuesWriterShutdownError> {
        let mut sender = self
            .shared
            .sender
            .lock()
            .map_err(|_poisoned| ObservedValuesWriterShutdownError::MutexPoisoned)?;
        drop(sender.take());
        drop(sender);
        let join_handle = self
            .shared
            .join_handle
            .lock()
            .map_err(|_poisoned| ObservedValuesWriterShutdownError::MutexPoisoned)?
            .take();
        if let Some(join_handle) = join_handle {
            join_handle
                .join()
                .map_err(|_panic_payload| ObservedValuesWriterShutdownError::Panicked)?;
        }
        Ok(())
    }
}

fn run_observed_values_writer(
    store: &SqliteObservedValuesStore,
    mut receiver: Receiver<ObservedValuesWrite>,
) {
    while let Some(write) = receiver.blocking_recv() {
        let job = ObservedValuesQueueJob {
            owner_source_name: write.owner_source_name,
            source_name: write.source_name,
            source_scope_id: write.source_scope_id,
            surface_kind: write.surface_kind,
            surface_name: write.surface_name,
            payload_json: write.payload_json,
        };
        match store.enqueue_if_current(&write.workspace_name, &job, write.epoch) {
            Ok(ObservedValuesEnqueueResult::Enqueued { .. }) => {}
            Ok(ObservedValuesEnqueueResult::StaleEpoch) => {
                tracing::debug!(
                    workspace = %write.workspace_name.as_str(),
                    source = %job.source_name,
                    surface = %job.surface_name,
                    "dropping stale observed-values source-scan observation"
                );
            }
            Ok(ObservedValuesEnqueueResult::QueueFull) => {
                tracing::debug!(
                    workspace = %write.workspace_name.as_str(),
                    source = %job.source_name,
                    surface = %job.surface_name,
                    "dropping observed-values source-scan observation because durable queue is full"
                );
            }
            Ok(ObservedValuesEnqueueResult::StorageLimitReached) => {
                tracing::debug!(
                    workspace = %write.workspace_name.as_str(),
                    source = %job.source_name,
                    surface = %job.surface_name,
                    "dropping observed-values source-scan observation because workspace search storage is full"
                );
            }
            Err(error) => {
                tracing::debug!(
                    workspace = %write.workspace_name.as_str(),
                    source = %job.source_name,
                    surface = %job.surface_name,
                    error = %error,
                    "failed to enqueue observed-values source-scan observation"
                );
            }
        }
    }
}

#[derive(Serialize)]
struct ObservedValuesQueuePayloadRef<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    search_origin: Option<Uuid>,
    values: &'a [ObservedValueCandidate],
}

#[cfg(test)]
pub(super) fn payload_json_with_budget(
    payload: ObservedValuesQueuePayload,
    max_job_bytes: usize,
) -> Result<Option<String>, String> {
    payload_json_with_budget_and_origin(payload, max_job_bytes, None)
}

pub(super) fn payload_json_with_budget_and_origin(
    payload: ObservedValuesQueuePayload,
    max_job_bytes: usize,
    search_origin: Option<Uuid>,
) -> Result<Option<String>, String> {
    if payload.is_empty() {
        return Ok(None);
    }
    let values = payload.values;
    let payload_json = payload_json_for_values(&values, search_origin)?;
    if payload_json.len() <= max_job_bytes {
        return Ok(Some(payload_json));
    }

    let mut best = None;
    let mut low = 0_usize;
    let mut high = values.len();
    while low < high {
        let candidate_len = (low + high).div_ceil(2);
        let candidate_values = values
            .get(..candidate_len)
            .expect("binary-search prefix length stays within observed-values payload bounds");
        let candidate_json = payload_json_for_values(candidate_values, search_origin)?;
        if candidate_json.len() <= max_job_bytes {
            best = Some(candidate_json);
            low = candidate_len;
        } else {
            high = candidate_len.saturating_sub(1);
        }
    }
    Ok(best)
}

fn payload_json_for_values(
    values: &[ObservedValueCandidate],
    search_origin: Option<Uuid>,
) -> Result<String, String> {
    serde_json::to_string(&ObservedValuesQueuePayloadRef {
        search_origin,
        values,
    })
    .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use uuid::Uuid;

    use super::{
        ObservedValuesTryReserveError, ObservedValuesWrite, ObservedValuesWriter,
        ObservedValuesWriterShared, payload_json_with_budget, payload_json_with_budget_and_origin,
    };
    use crate::search::observed::sqlite_queue::{
        ObservedValueCandidate, ObservedValuesEpoch, ObservedValuesQueueEnvelope,
        ObservedValuesQueuePayload, ObservedValuesSurfaceKind,
    };
    use crate::workspaces::WorkspaceName;

    #[test]
    fn writer_capacity_is_reserved_before_a_write_is_built() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let writer = ObservedValuesWriter {
            shared: Arc::new(ObservedValuesWriterShared {
                sender: Mutex::new(Some(sender)),
                join_handle: Mutex::new(None),
            }),
        };

        let permit = writer.try_reserve().expect("first reservation");
        assert!(matches!(
            writer.try_reserve(),
            Err(ObservedValuesTryReserveError::Full)
        ));

        permit.send(test_write());
        assert!(matches!(
            writer.try_reserve(),
            Err(ObservedValuesTryReserveError::Full)
        ));
        receiver.try_recv().expect("reserved write");
        writer
            .try_reserve()
            .expect("capacity should return after dequeue");
    }

    #[test]
    fn ordinary_queue_payload_omits_origin_and_legacy_json_defaults_to_none() {
        let payload_json = payload_json_with_budget(test_payload(), usize::MAX)
            .expect("serialize payload")
            .expect("non-empty payload");

        assert_eq!(
            payload_json,
            r#"{"values":[{"column_name":"title","display_value":"Payment outage","search_text":"payment outage","value_key":"payment-outage"}]}"#
        );
        let envelope = serde_json::from_str::<ObservedValuesQueueEnvelope>(&payload_json)
            .expect("deserialize legacy-compatible envelope");
        assert_eq!(envelope.search_origin, None);
        assert_eq!(envelope.payload.values.len(), 1);
    }

    #[test]
    fn selected_queue_payload_serializes_search_origin() {
        let search_origin = Uuid::from_u128(42);
        let payload_json =
            payload_json_with_budget_and_origin(test_payload(), usize::MAX, Some(search_origin))
                .expect("serialize payload")
                .expect("non-empty payload");

        let envelope = serde_json::from_str::<ObservedValuesQueueEnvelope>(&payload_json)
            .expect("deserialize origin envelope");
        assert_eq!(envelope.search_origin, Some(search_origin));
        assert_eq!(envelope.payload.values.len(), 1);
    }

    fn test_payload() -> ObservedValuesQueuePayload {
        ObservedValuesQueuePayload {
            values: vec![ObservedValueCandidate {
                column_name: "title".to_string(),
                display_value: "Payment outage".to_string(),
                search_text: "payment outage".to_string(),
                value_key: "payment-outage".to_string(),
            }],
        }
    }

    fn test_write() -> ObservedValuesWrite {
        ObservedValuesWrite {
            workspace_name: WorkspaceName::default(),
            owner_source_name: "github".to_string(),
            source_name: "github".to_string(),
            source_scope_id: "scope".to_string(),
            surface_kind: ObservedValuesSurfaceKind::Table,
            surface_name: "issues".to_string(),
            payload_json: r#"{"values":[]}"#.to_string(),
            epoch: ObservedValuesEpoch::ZERO,
        }
    }
}
