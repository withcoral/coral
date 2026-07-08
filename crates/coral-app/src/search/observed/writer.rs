//! Bounded observed-values writer lifecycle and durable queue handoff.

use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use serde::Serialize;

use crate::search::observed::sqlite_queue::{
    ObservedValueCandidate, ObservedValuesEnqueueResult, ObservedValuesGeneration,
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
    sender: Mutex<Option<SyncSender<ObservedValuesWrite>>>,
    join_handle: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Debug)]
pub(super) struct ObservedValuesWrite {
    pub(super) workspace_name: WorkspaceName,
    pub(super) source_name: String,
    pub(super) source_scope_id: String,
    pub(super) surface_kind: ObservedValuesSurfaceKind,
    pub(super) surface_name: String,
    pub(super) payload: ObservedValuesQueuePayload,
    pub(super) max_job_bytes: usize,
    pub(super) generation: ObservedValuesGeneration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ObservedValuesTryEnqueueError {
    Full,
    Disconnected,
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
        let (sender, receiver) = sync_channel(OBSERVED_VALUES_WRITE_QUEUE_CAPACITY);
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

    pub(super) fn try_enqueue(
        &self,
        write: ObservedValuesWrite,
    ) -> Result<(), ObservedValuesTryEnqueueError> {
        let Ok(sender) = self.shared.sender.lock() else {
            return Err(ObservedValuesTryEnqueueError::Disconnected);
        };
        let Some(sender) = sender.as_ref() else {
            return Err(ObservedValuesTryEnqueueError::Disconnected);
        };
        sender.try_send(write).map_err(|error| match error {
            TrySendError::Full(_) => ObservedValuesTryEnqueueError::Full,
            TrySendError::Disconnected(_) => ObservedValuesTryEnqueueError::Disconnected,
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
    receiver: Receiver<ObservedValuesWrite>,
) {
    for write in receiver {
        let payload_json = match payload_json_with_budget(write.payload, write.max_job_bytes) {
            Ok(Some(payload_json)) => payload_json,
            Ok(None) => {
                tracing::debug!(
                    workspace = %write.workspace_name.as_str(),
                    source = %write.source_name,
                    surface = %write.surface_name,
                    "dropping observed-values source-scan observation because serialized payload exceeds job budget"
                );
                continue;
            }
            Err(error) => {
                tracing::debug!(
                    workspace = %write.workspace_name.as_str(),
                    source = %write.source_name,
                    surface = %write.surface_name,
                    error = %error,
                    "failed to serialize observed-values source-scan observation"
                );
                continue;
            }
        };
        let job = ObservedValuesQueueJob {
            source_name: write.source_name,
            source_scope_id: write.source_scope_id,
            surface_kind: write.surface_kind,
            surface_name: write.surface_name,
            payload_json,
        };
        match store.enqueue_source_scan(&write.workspace_name, &job, write.generation) {
            Ok(ObservedValuesEnqueueResult::Enqueued { .. }) => {}
            Ok(ObservedValuesEnqueueResult::StaleGeneration) => {
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
    values: &'a [ObservedValueCandidate],
}

pub(super) fn payload_json_with_budget(
    payload: ObservedValuesQueuePayload,
    max_job_bytes: usize,
) -> Result<Option<String>, String> {
    if payload.is_empty() {
        return Ok(None);
    }
    let values = payload.values;
    let payload_json = payload_json_for_values(&values)?;
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
        let candidate_json = payload_json_for_values(candidate_values)?;
        if candidate_json.len() <= max_job_bytes {
            best = Some(candidate_json);
            low = candidate_len;
        } else {
            high = candidate_len.saturating_sub(1);
        }
    }
    Ok(best)
}

fn payload_json_for_values(values: &[ObservedValueCandidate]) -> Result<String, String> {
    serde_json::to_string(&ObservedValuesQueuePayloadRef { values })
        .map_err(|error| error.to_string())
}
