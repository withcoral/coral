//! In-memory MCP result-handle store.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use coral_client::CollectedQueryResult;

const RESULT_ID_PREFIX: &str = "res_";
const DEFAULT_RESULT_TTL: Duration = Duration::from_mins(30);
const DEFAULT_MAX_RESULTS: usize = 64;
const DEFAULT_MAX_BYTES: usize = 128 * 1024 * 1024;

#[derive(Clone)]
pub(crate) struct ResultStore {
    state: Arc<Mutex<ResultStoreState>>,
    clock: Arc<dyn ResultClock>,
    limits: ResultStoreLimits,
}

#[derive(Debug, Clone, Copy)]
struct ResultStoreLimits {
    ttl: Duration,
    max_results: usize,
    max_bytes: usize,
}

impl Default for ResultStoreLimits {
    fn default() -> Self {
        Self {
            ttl: DEFAULT_RESULT_TTL,
            max_results: DEFAULT_MAX_RESULTS,
            max_bytes: DEFAULT_MAX_BYTES,
        }
    }
}

impl ResultStoreLimits {
    #[cfg(test)]
    fn new(ttl: Duration, max_results: usize, max_bytes: usize) -> Self {
        Self {
            ttl,
            max_results,
            max_bytes,
        }
    }
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub(crate) enum ResultStoreError {
    #[error("result handle '{0}' was not found")]
    NotFound(String),
    #[error("result handle '{0}' has expired")]
    Expired(String),
    #[error("result is too large to store ({estimated_bytes} bytes, max {max_bytes} bytes)")]
    TooLarge {
        estimated_bytes: usize,
        max_bytes: usize,
    },
    #[error("result handle store is unavailable")]
    Unavailable,
}

trait ResultClock: Send + Sync + std::fmt::Debug {
    fn now(&self) -> Instant;
}

#[derive(Debug)]
struct SystemClock;

impl ResultClock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

#[derive(Default)]
struct ResultStoreState {
    entries: HashMap<String, ResultStoreEntry>,
    total_bytes: usize,
    next_id: u64,
    access_seq: u64,
}

struct ResultStoreEntry {
    result: Arc<CollectedQueryResult>,
    estimated_bytes: usize,
    // Monotonic access order for LRU eviction; Instant ties between entries
    // touched at the same clock reading would make eviction nondeterministic.
    last_accessed_seq: u64,
    expires_at: Instant,
}

impl ResultStore {
    pub(crate) fn new() -> Self {
        Self::with_limits(ResultStoreLimits::default())
    }

    fn with_limits(limits: ResultStoreLimits) -> Self {
        Self {
            state: Arc::new(Mutex::new(ResultStoreState::default())),
            clock: Arc::new(SystemClock),
            limits,
        }
    }

    #[cfg(test)]
    fn with_clock(limits: ResultStoreLimits, clock: Arc<dyn ResultClock>) -> Self {
        Self {
            state: Arc::new(Mutex::new(ResultStoreState::default())),
            clock,
            limits,
        }
    }

    pub(crate) fn insert(
        &self,
        result: Arc<CollectedQueryResult>,
        estimated_bytes: usize,
    ) -> Result<String, ResultStoreError> {
        if self.limits.max_results == 0 || estimated_bytes > self.limits.max_bytes {
            return Err(ResultStoreError::TooLarge {
                estimated_bytes,
                max_bytes: self.limits.max_bytes,
            });
        }

        let mut guard = self
            .state
            .lock()
            .map_err(|_error| ResultStoreError::Unavailable)?;
        let now = self.clock.now();
        let result_id = guard.next_result_id();
        guard.purge_expired(now);
        let last_accessed_seq = guard.next_access_seq();
        guard.total_bytes = guard.total_bytes.saturating_add(estimated_bytes);
        guard.entries.insert(
            result_id.clone(),
            ResultStoreEntry {
                result,
                estimated_bytes,
                last_accessed_seq,
                expires_at: now + self.limits.ttl,
            },
        );
        guard.evict_until_within_limits(self.limits);
        Ok(result_id)
    }

    pub(crate) fn get(
        &self,
        result_id: &str,
    ) -> Result<Arc<CollectedQueryResult>, ResultStoreError> {
        let now = self.clock.now();
        let mut guard = self
            .state
            .lock()
            .map_err(|_error| ResultStoreError::Unavailable)?;
        let last_accessed_seq = guard.next_access_seq();
        let Some(entry) = guard.entries.get_mut(result_id) else {
            return Err(ResultStoreError::NotFound(result_id.to_string()));
        };
        if now >= entry.expires_at {
            let estimated_bytes = entry.estimated_bytes;
            guard.entries.remove(result_id);
            guard.total_bytes = guard.total_bytes.saturating_sub(estimated_bytes);
            return Err(ResultStoreError::Expired(result_id.to_string()));
        }
        entry.last_accessed_seq = last_accessed_seq;
        Ok(Arc::clone(&entry.result))
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.state
            .lock()
            .expect("result store mutex poisoned")
            .entries
            .len()
    }
}

impl Default for ResultStore {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ResultStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let len = self.state.lock().map(|guard| guard.entries.len()).ok();
        f.debug_struct("ResultStore")
            .field("entries", &len)
            .field("clock", &self.clock)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl ResultStoreState {
    fn next_access_seq(&mut self) -> u64 {
        self.access_seq += 1;
        self.access_seq
    }

    fn next_result_id(&mut self) -> String {
        loop {
            self.next_id = self.next_id.checked_add(1).unwrap_or(1);
            let result_id = format!("{RESULT_ID_PREFIX}{}", self.next_id);
            if !self.entries.contains_key(&result_id) {
                return result_id;
            }
        }
    }

    fn purge_expired(&mut self, now: Instant) {
        let expired = self
            .entries
            .iter()
            .filter_map(|(id, entry)| (now >= entry.expires_at).then_some(id.clone()))
            .collect::<Vec<_>>();
        for id in expired {
            if let Some(entry) = self.entries.remove(&id) {
                self.total_bytes = self.total_bytes.saturating_sub(entry.estimated_bytes);
            }
        }
    }

    fn evict_until_within_limits(&mut self, limits: ResultStoreLimits) {
        while self.entries.len() > limits.max_results || self.total_bytes > limits.max_bytes {
            let Some(oldest_id) = self
                .entries
                .iter()
                .min_by_key(|(_id, entry)| entry.last_accessed_seq)
                .map(|(id, _entry)| id.clone())
            else {
                break;
            };
            if let Some(entry) = self.entries.remove(&oldest_id) {
                self.total_bytes = self.total_bytes.saturating_sub(entry.estimated_bytes);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use coral_client::CollectedQueryResult;

    use super::{ResultClock, ResultStore, ResultStoreError, ResultStoreLimits};

    #[derive(Debug)]
    struct ManualClock {
        now: Mutex<Instant>,
    }

    impl ManualClock {
        fn new(now: Instant) -> Self {
            Self {
                now: Mutex::new(now),
            }
        }

        fn advance(&self, duration: Duration) {
            let mut guard = self.now.lock().expect("clock mutex poisoned");
            *guard += duration;
        }
    }

    impl ResultClock for ManualClock {
        fn now(&self) -> Instant {
            *self.now.lock().expect("clock mutex poisoned")
        }
    }

    fn result(value: i64) -> Arc<CollectedQueryResult> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![value])) as _],
        )
        .expect("batch");
        Arc::new(CollectedQueryResult::new(schema, vec![batch], 1).expect("result"))
    }

    fn store_with_limits(max_results: usize, max_bytes: usize) -> ResultStore {
        ResultStore::with_limits(ResultStoreLimits::new(
            Duration::from_secs(30),
            max_results,
            max_bytes,
        ))
    }

    fn stored_id(insert: Result<String, ResultStoreError>) -> String {
        insert.expect("expected stored result")
    }

    #[test]
    fn clones_share_entries() {
        let store = ResultStore::new();
        let clone = store.clone();
        let result_id = stored_id(store.insert(result(1), 1));
        assert_eq!(clone.get(&result_id).expect("stored").row_count(), 1);
    }

    #[test]
    fn result_ids_are_short_process_local_handles() {
        let store = ResultStore::new();

        assert_eq!(stored_id(store.insert(result(1), 1)), "res_1");
        assert_eq!(stored_id(store.insert(result(2), 1)), "res_2");
    }

    #[test]
    fn oversized_results_are_rejected() {
        let store = store_with_limits(10, 5);
        assert_eq!(
            store.insert(result(1), 6),
            Err(ResultStoreError::TooLarge {
                estimated_bytes: 6,
                max_bytes: 5
            })
        );
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn store_evicts_least_recently_accessed_entry_by_count() {
        let store = store_with_limits(2, 100);
        let first = stored_id(store.insert(result(1), 1));
        let second = stored_id(store.insert(result(2), 1));
        store.get(&first).expect("touch first so second is oldest");
        let third = stored_id(store.insert(result(3), 1));

        store.get(&first).expect("first should still be stored");
        assert!(matches!(
            store.get(&second),
            Err(ResultStoreError::NotFound(id)) if id == second
        ));
        store.get(&third).expect("third should be stored");
    }

    #[test]
    fn eviction_is_deterministic_when_clock_does_not_advance() {
        let clock = Arc::new(ManualClock::new(Instant::now()));
        let store = ResultStore::with_clock(
            ResultStoreLimits::new(Duration::from_secs(30), 2, 100),
            clock,
        );
        let first = stored_id(store.insert(result(1), 1));
        let second = stored_id(store.insert(result(2), 1));
        let third = stored_id(store.insert(result(3), 1));

        assert!(matches!(
            store.get(&first),
            Err(ResultStoreError::NotFound(id)) if id == first
        ));
        store.get(&second).expect("second should still be stored");
        store
            .get(&third)
            .expect("just-inserted third should never be evicted");
    }

    #[test]
    fn store_evicts_by_total_bytes() {
        let store = store_with_limits(10, 3);
        let first = stored_id(store.insert(result(1), 2));
        let second = stored_id(store.insert(result(2), 2));
        assert!(matches!(
            store.get(&first),
            Err(ResultStoreError::NotFound(id)) if id == first
        ));
        store.get(&second).expect("second should still be stored");
    }

    #[test]
    fn expired_entry_is_removed_without_sleeping() {
        let clock = Arc::new(ManualClock::new(Instant::now()));
        let store = ResultStore::with_clock(
            ResultStoreLimits::new(Duration::from_secs(5), 10, 100),
            clock.clone(),
        );
        let result_id = stored_id(store.insert(result(1), 1));
        clock.advance(Duration::from_secs(5));
        assert!(matches!(
            store.get(&result_id),
            Err(ResultStoreError::Expired(id)) if id == result_id
        ));
        assert_eq!(store.len(), 0);
    }
}
