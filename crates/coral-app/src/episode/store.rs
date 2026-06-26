//! Per-workspace episode store.
//!
//! Persists `{ episode_id -> intent, parent }` for an episode (one task-attempt),
//! written once by `OpenEpisode` into app storage. Writes are bounded by a
//! per-workspace **byte ceiling with oldest-out eviction** — when a new record
//! would exceed the limit, the oldest records are dropped to make room, so storage
//! keeps the most recent episodes within budget. The always-registered
//! `OpenEpisode` route makes an unbounded store a real fill-the-disk surface,
//! which this prevents. Queryable `coral.episodes` lands in a later PR.

use std::time::{SystemTime, UNIX_EPOCH};

use coral_api::CORAL_EPISODE_INTENT_MAX_CHARS;
use serde::Serialize;

use super::EpisodeId;
use crate::storage::app::{AppStorageError, AppStore, OpenEpisodeResult, StoredEpisode};
use crate::workspaces::WorkspaceName;

/// Per-workspace byte ceiling on the raw log. A new record that would push the log
/// over this evicts the oldest records first (the newest is always kept, even if it
/// alone exceeds the ceiling). Generous — bounds disk without losing recent history;
/// becomes a `[episodes]` config knob alongside JSONL→Parquet compaction later.
const MAX_EPISODE_BYTES_PER_WORKSPACE: u64 = 256 * 1024 * 1024;

/// A registered episode — one task-attempt's intent and lineage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Episode {
    /// Client-minted, validated episode id.
    pub(crate) id: EpisodeId,
    /// Workspace that scopes the episode (per-tenant isolation).
    pub(crate) workspace: WorkspaceName,
    /// Natural-language task description — the retrieval handle.
    pub(crate) intent: String,
    /// Parent episode id for a child sub-task; `None` for a root.
    pub(crate) parent_episode_id: Option<EpisodeId>,
    /// Registration time, unix nanoseconds.
    pub(crate) created_at_unix_nanos: u128,
}

/// Stable serialization shape for byte-budget accounting.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct PersistedEpisode {
    id: String,
    workspace: String,
    intent: String,
    parent_episode_id: Option<String>,
    created_at_unix_nanos: u128,
}

impl PersistedEpisode {
    /// Builds the on-disk record, storing the already-normalized `intent` (rather
    /// than `episode.intent`) so persisted/compared values stay consistent.
    fn from_episode(episode: &Episode, intent: &str) -> Self {
        Self {
            id: episode.id.as_str().to_string(),
            workspace: episode.workspace.as_str().to_string(),
            intent: intent.to_string(),
            parent_episode_id: episode
                .parent_episode_id
                .as_ref()
                .map(|parent| parent.as_str().to_string()),
            created_at_unix_nanos: episode.created_at_unix_nanos,
        }
    }

    fn record_bytes(&self) -> Result<u64, EpisodeStoreError> {
        Ok(serde_json::to_vec(self)?.len() as u64 + 1)
    }
}

/// Errors from the episode store.
#[derive(Debug, thiserror::Error)]
pub(crate) enum EpisodeStoreError {
    /// Filesystem error reading legacy test fixtures or writing the store.
    #[error("episode store io: {0}")]
    Io(#[from] std::io::Error),
    /// JSON (de)serialization error.
    #[error("episode store serialization: {0}")]
    Serde(#[from] serde_json::Error),
    /// The `episode_id` is already registered with a different intent/parent.
    #[error("episode {episode_id:?} is already open with a different intent/parent")]
    Conflict {
        /// The conflicting episode id.
        episode_id: String,
    },
    /// The intent is empty or exceeds the maximum length.
    #[error("episode intent must be non-empty and at most {max} characters")]
    InvalidIntent {
        /// The configured maximum intent length, in characters.
        max: usize,
    },
    /// The app storage backend failed.
    #[error(transparent)]
    Storage(#[from] AppStorageError),
}

/// Per-workspace episode repository, bounded by a byte ceiling with oldest-out
/// eviction. Durable writes go through [`AppStore`].
#[derive(Clone)]
pub(crate) struct EpisodeStore {
    store: AppStore,
    max_bytes: u64,
}

impl EpisodeStore {
    /// Creates a store that persists through `store` with the default byte ceiling.
    pub(crate) fn new(store: AppStore) -> Self {
        Self {
            store,
            max_bytes: MAX_EPISODE_BYTES_PER_WORKSPACE,
        }
    }

    /// Overrides the per-workspace byte ceiling so tests can exercise eviction with a
    /// tiny value; production uses [`MAX_EPISODE_BYTES_PER_WORKSPACE`].
    #[cfg(test)]
    pub(crate) fn with_max_bytes(mut self, max_bytes: u64) -> Self {
        self.max_bytes = max_bytes;
        self
    }

    /// Registers `episode`, idempotently.
    ///
    /// Re-opening the same `episode_id` with an identical `{ intent, parent }` is a
    /// no-op; a different intent/parent returns [`EpisodeStoreError::Conflict`]
    /// (intent is immutable per episode — a change is a new child/sibling).
    ///
    /// The conflict guarantee covers records still **retained** in the log. Because
    /// the byte ceiling evicts the oldest records (below), an id whose record has been
    /// evicted is treated as new rather than conflicting — bounding the log without
    /// unbounded id-tracking inherently means forgetting old ids. Episode ids are
    /// client-minted and unique per attempt (ULID/UUID), so an evicted id is never
    /// reused in practice and the guarantee holds for all real callers.
    ///
    /// A new record is appended within the per-workspace byte ceiling: if it would
    /// push storage over budget, the oldest records are evicted to make room (a
    /// byte-bounded FIFO; the newest record is always kept). The idempotency check
    /// and evict-and-insert happen in one app-storage unit of work. The id is
    /// already validated ([`EpisodeId`]); intent is checked here as a safety net for
    /// callers that bypass the service boundary.
    pub(crate) fn open_episode(&self, episode: &Episode) -> Result<(), EpisodeStoreError> {
        // Normalize intent once so surrounding whitespace never becomes part of the
        // immutable key — a retry with an accidental trailing space must stay
        // idempotent, not conflict.
        let intent = episode.intent.trim();
        if intent.is_empty() || intent.chars().count() > CORAL_EPISODE_INTENT_MAX_CHARS {
            return Err(EpisodeStoreError::InvalidIntent {
                max: CORAL_EPISODE_INTENT_MAX_CHARS,
            });
        }
        let persisted = PersistedEpisode::from_episode(episode, intent);
        let stored = StoredEpisode {
            workspace: persisted.workspace.clone(),
            id: persisted.id.clone(),
            intent: persisted.intent.clone(),
            parent_episode_id: persisted.parent_episode_id.clone(),
            created_at_unix_nanos: i64::try_from(persisted.created_at_unix_nanos).map_err(
                |_error| AppStorageError::ValueOutOfRange {
                    field: "created_at_unix_nanos",
                    value: persisted.created_at_unix_nanos.to_string(),
                },
            )?,
            record_bytes: persisted.record_bytes()?,
        };
        let mut uow = self.store.begin_write()?;
        let result = {
            let mut episodes = uow.episodes();
            episodes.open_episode(&stored, self.max_bytes)?
        };
        match result {
            OpenEpisodeResult::Opened | OpenEpisodeResult::AlreadyOpen => {
                uow.commit()?;
                Ok(())
            }
            OpenEpisodeResult::Conflict => Err(EpisodeStoreError::Conflict {
                episode_id: episode.id.as_str().to_string(),
            }),
        }
    }

    #[cfg(test)]
    fn read_episode(
        &self,
        workspace: &WorkspaceName,
        episode_id: &str,
    ) -> Result<Option<StoredEpisode>, AppStorageError> {
        self.store.test_read_episode(workspace.as_str(), episode_id)
    }

    #[cfg(test)]
    fn count_episodes(&self, workspace: &WorkspaceName) -> Result<usize, AppStorageError> {
        self.store.test_count_episodes(workspace.as_str())
    }

    #[cfg(test)]
    fn episode_bytes(&self, workspace: &WorkspaceName) -> Result<u64, AppStorageError> {
        self.store.test_episode_bytes(workspace.as_str())
    }
}

/// Unix-nanoseconds timestamp for `created_at_unix_nanos`.
pub(crate) fn now_unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos())
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::{
        CORAL_EPISODE_INTENT_MAX_CHARS, Episode, EpisodeId, EpisodeStore, EpisodeStoreError,
        PersistedEpisode, now_unix_nanos,
    };
    use crate::state::AppStateLayout;
    use crate::storage::app::AppStore;
    use crate::workspaces::WorkspaceName;

    /// On-disk byte size of one record (serialized JSON + newline), for sizing the
    /// byte ceiling in eviction tests.
    fn record_bytes(workspace: &WorkspaceName, id: &str, intent: &str) -> u64 {
        let record = PersistedEpisode::from_episode(&episode(workspace, id, intent, None), intent);
        record.record_bytes().expect("record bytes")
    }

    fn store() -> (TempDir, EpisodeStore) {
        let dir = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(dir.path().join("coral-config")))
            .expect("layout should resolve");
        let app_store = AppStore::sqlite(layout.app_database_file()).expect("sqlite app store");
        (dir, EpisodeStore::new(app_store))
    }

    fn episode(workspace: &WorkspaceName, id: &str, intent: &str, parent: Option<&str>) -> Episode {
        Episode {
            id: EpisodeId::parse(id).expect("valid episode id"),
            workspace: workspace.clone(),
            intent: intent.to_string(),
            parent_episode_id: parent.map(|parent| EpisodeId::parse(parent).expect("valid parent")),
            created_at_unix_nanos: now_unix_nanos(),
        }
    }

    #[test]
    fn open_rejects_blank_and_overlong_intent() {
        let (_dir, store) = store();
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        let blank = store
            .open_episode(&episode(&workspace, "ep_blank", "   ", None))
            .expect_err("blank intent must be rejected");
        assert!(matches!(blank, EpisodeStoreError::InvalidIntent { .. }));
        let overlong = "x".repeat(CORAL_EPISODE_INTENT_MAX_CHARS + 1);
        let too_long = store
            .open_episode(&episode(&workspace, "ep_long", &overlong, None))
            .expect_err("overlong intent must be rejected");
        assert!(matches!(too_long, EpisodeStoreError::InvalidIntent { .. }));
    }

    #[test]
    fn intent_whitespace_is_normalized_and_does_not_fork_the_key() {
        let (_dir, store) = store();
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        store
            .open_episode(&episode(&workspace, "ep_1", "find the form", None))
            .expect("open");
        // A retry with accidental surrounding whitespace stays idempotent.
        store
            .open_episode(&episode(&workspace, "ep_1", "  find the form  ", None))
            .expect("whitespace-only difference is idempotent");
        let read = store
            .read_episode(&workspace, "ep_1")
            .expect("read")
            .expect("present");
        assert_eq!(read.intent, "find the form", "stored intent is normalized");
        assert_eq!(
            store.count_episodes(&workspace).expect("count"),
            1,
            "whitespace-only difference must not insert a second record"
        );
    }

    #[test]
    fn open_then_read_round_trips() {
        let (_dir, store) = store();
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        let ep = episode(&workspace, "ep_1", "find the HR onboarding form", None);
        store.open_episode(&ep).expect("open");
        let read = store
            .read_episode(&workspace, "ep_1")
            .expect("read")
            .expect("episode should be present");
        assert_eq!(read.id, "ep_1");
        assert_eq!(read.workspace, "acme");
        assert_eq!(read.intent, "find the HR onboarding form");
        assert_eq!(read.parent_episode_id, None);
        assert_eq!(
            read.created_at_unix_nanos,
            i64::try_from(ep.created_at_unix_nanos).expect("timestamp")
        );
    }

    #[test]
    fn reopen_identical_is_idempotent() {
        let (_dir, store) = store();
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        let ep = episode(&workspace, "ep_1", "same intent", None);
        store.open_episode(&ep).expect("first open");
        store.open_episode(&ep).expect("idempotent reopen");
        assert_eq!(
            store.count_episodes(&workspace).expect("count"),
            1,
            "idempotent reopen must not insert a duplicate"
        );
    }

    #[test]
    fn reopen_with_different_intent_conflicts() {
        let (_dir, store) = store();
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        store
            .open_episode(&episode(&workspace, "ep_1", "intent A", None))
            .expect("first open");
        let error = store
            .open_episode(&episode(&workspace, "ep_1", "intent B", None))
            .expect_err("changed intent must conflict");
        assert!(matches!(error, EpisodeStoreError::Conflict { .. }));
    }

    #[test]
    fn open_child_round_trips_parent() {
        let (_dir, store) = store();
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        store
            .open_episode(&episode(&workspace, "root_ep", "root task", None))
            .expect("open root");
        let child = episode(&workspace, "child_ep", "sub task", Some("root_ep"));
        store.open_episode(&child).expect("open child");
        let read = store
            .read_episode(&workspace, "child_ep")
            .expect("read")
            .expect("child should be present");
        assert_eq!(
            read.parent_episode_id.as_deref(),
            Some("root_ep"),
            "the parent link must persist and round-trip"
        );
        assert_eq!(read.intent, "sub task");
    }

    #[test]
    fn reopen_child_with_identical_parent_is_idempotent() {
        let (_dir, store) = store();
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        let child = episode(&workspace, "child_ep", "sub task", Some("root_ep"));
        store.open_episode(&child).expect("first open");
        store
            .open_episode(&child)
            .expect("identical reopen (same id/intent/parent) is idempotent");
        assert_eq!(
            store.count_episodes(&workspace).expect("count"),
            1,
            "reopening with an identical parent must not insert a duplicate"
        );
    }

    #[test]
    fn reopen_same_intent_different_parent_conflicts() {
        let (_dir, store) = store();
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        store
            .open_episode(&episode(
                &workspace,
                "child_ep",
                "sub task",
                Some("parent_a"),
            ))
            .expect("first open");
        // Same id and intent, but a different parent. The parent is half of the
        // idempotency key, so this must conflict rather than silently re-parent
        // the child's lineage.
        let error = store
            .open_episode(&episode(
                &workspace,
                "child_ep",
                "sub task",
                Some("parent_b"),
            ))
            .expect_err("changed parent must conflict");
        assert!(matches!(error, EpisodeStoreError::Conflict { .. }));
    }

    #[test]
    fn same_id_in_different_workspaces_is_isolated() {
        let (_dir, store) = store();
        let acme = WorkspaceName::parse("acme").expect("workspace");
        let globex = WorkspaceName::parse("globex").expect("workspace");
        store
            .open_episode(&episode(&acme, "ep_1", "in acme", None))
            .expect("acme");
        // Same id, different workspace → isolated, no conflict.
        store
            .open_episode(&episode(&globex, "ep_1", "in globex", None))
            .expect("globex");
        assert_eq!(
            store.read_episode(&acme, "ep_1").unwrap().unwrap().intent,
            "in acme"
        );
        assert_eq!(
            store.read_episode(&globex, "ep_1").unwrap().unwrap().intent,
            "in globex"
        );
    }

    #[test]
    fn open_evicts_oldest_to_stay_under_the_byte_ceiling() {
        let (_dir, store) = store();
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        // Size the ceiling to hold exactly two records (ids and intent are uniform, so
        // every record serializes to the same length).
        let one = record_bytes(&workspace, "ep_0", "task");
        let store = store.with_max_bytes(one * 2);
        for id in ["ep_1", "ep_2", "ep_3"] {
            store
                .open_episode(&episode(&workspace, id, "task", None))
                .expect("open");
        }

        assert!(
            store.read_episode(&workspace, "ep_1").unwrap().is_none(),
            "the oldest record must be evicted"
        );
        assert!(store.read_episode(&workspace, "ep_2").unwrap().is_some());
        assert!(store.read_episode(&workspace, "ep_3").unwrap().is_some());
        assert!(
            store.episode_bytes(&workspace).unwrap() <= one * 2,
            "the episode rows must stay within the byte ceiling"
        );
    }

    #[test]
    fn the_newest_record_is_kept_even_when_it_alone_exceeds_the_ceiling() {
        let (_dir, store) = store();
        // A ceiling smaller than any record: each open evicts everything older, so the
        // store always holds exactly the most recent episode — never rejects.
        let store = store.with_max_bytes(1);
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        store
            .open_episode(&episode(&workspace, "ep_1", "first", None))
            .expect("first kept");
        store
            .open_episode(&episode(&workspace, "ep_2", "second", None))
            .expect("second evicts first, never rejects");

        assert!(store.read_episode(&workspace, "ep_1").unwrap().is_none());
        assert_eq!(
            store
                .read_episode(&workspace, "ep_2")
                .unwrap()
                .unwrap()
                .intent,
            "second"
        );
        assert_eq!(
            store.count_episodes(&workspace).expect("count"),
            1,
            "only the newest record remains"
        );
    }

    #[test]
    fn reopening_an_evicted_id_is_a_fresh_episode() {
        let (_dir, store) = store();
        let store = store.with_max_bytes(1);
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        store
            .open_episode(&episode(&workspace, "ep_1", "old intent", None))
            .expect("open ep_1");
        // ep_1 is evicted to make room for ep_2.
        store
            .open_episode(&episode(&workspace, "ep_2", "other", None))
            .expect("open ep_2 evicts ep_1");
        // Re-using ep_1's id with a different intent is a fresh episode, not a
        // conflict — its old record is gone.
        store
            .open_episode(&episode(&workspace, "ep_1", "new intent", None))
            .expect("evicted id is reusable");
        let read = store
            .read_episode(&workspace, "ep_1")
            .unwrap()
            .expect("present");
        assert_eq!(read.intent, "new intent");
    }

    #[test]
    fn eviction_preserves_idempotency_for_surviving_records() {
        let (_dir, store) = store();
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        let one = record_bytes(&workspace, "ep_0", "task");
        let store = store.with_max_bytes(one * 2);
        for id in ["ep_1", "ep_2", "ep_3"] {
            store
                .open_episode(&episode(&workspace, id, "task", None))
                .expect("open");
        }
        // ep_2 survived the eviction of ep_1, so reopening it is still idempotent and a
        // changed intent still conflicts.
        store
            .open_episode(&episode(&workspace, "ep_2", "task", None))
            .expect("surviving record is idempotent");
        let conflict = store
            .open_episode(&episode(&workspace, "ep_2", "different", None))
            .expect_err("surviving record still conflicts on a changed intent");
        assert!(matches!(conflict, EpisodeStoreError::Conflict { .. }));
    }
}
