//! Per-workspace, append-only episode store.
//!
//! Persists `{ episode_id -> intent, parent }` for an episode (one task-attempt),
//! written once by `OpenEpisode` into the per-workspace episode log resolved by
//! [`AppStateLayout`]: a private (0600) JSONL append, fronted by a per-workspace
//! in-memory index (`episode_id -> { intent, parent }`) hydrated once from the log.
//! The index serves the idempotency/conflict check and the size cap in O(1), so an
//! open no longer scans the whole log under the shared state lock. A per-workspace
//! ceiling bounds the raw log until JSONL→Parquet compaction, retention/eviction,
//! and the queryable `coral.episodes` surface land in a later PR.

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use coral_api::CORAL_EPISODE_INTENT_MAX_CHARS;
use serde::{Deserialize, Serialize};

use super::EpisodeId;
use crate::state::AppStateLayout;
use crate::storage::fs::{self as storage_fs, FileLock};
use crate::workspaces::WorkspaceName;

/// Per-workspace backstop on the raw episode log size. The `OpenEpisode` route is
/// always registered, so without a ceiling a caller could grow the log without
/// bound. Interim protection until JSONL→Parquet compaction + retention land
/// (which will make this configurable under `[episodes]`); generous enough never
/// to bite a real workspace during the experimental phase.
const MAX_EPISODES_PER_WORKSPACE: usize = 100_000;

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

/// On-disk JSONL shape for an [`Episode`]. The workspace is also encoded in the
/// per-workspace file path; it is persisted here too so each record is
/// self-describing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
}

/// Errors from the episode store.
#[derive(Debug, thiserror::Error)]
pub(crate) enum EpisodeStoreError {
    /// Filesystem error reading or writing the store.
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
    /// The workspace has reached the maximum number of episodes the raw log holds
    /// before compaction/retention is enabled.
    #[error("workspace episode store is full ({max} episodes)")]
    CapacityExceeded {
        /// The configured per-workspace episode ceiling.
        max: usize,
    },
}

/// One workspace's write-side index: `episode_id -> { intent, parent }`, hydrated
/// once from the on-disk log and thereafter authoritative for the idempotency /
/// conflict check and the size cap.
#[derive(Default)]
struct WorkspaceIndex {
    /// Whether the index has been populated from disk yet.
    hydrated: bool,
    /// Latest `{ intent, parent }` per episode id (normalized intent, as persisted).
    entries: HashMap<String, IndexEntry>,
}

/// The indexed half of a persisted episode — enough to answer idempotency,
/// conflict, and the size cap without re-reading the log.
struct IndexEntry {
    /// Normalized (trimmed) intent, matching what is persisted.
    intent: String,
    /// Parent episode id, or `None` for a root.
    parent_episode_id: Option<String>,
}

/// Append-only, per-workspace JSONL episode store. Paths and the shared state lock
/// are resolved through [`AppStateLayout`]; an in-memory index per workspace fronts
/// the log so opens are O(1). Cloning shares the index (the store is cloned per
/// request).
#[derive(Clone)]
pub(crate) struct EpisodeStore {
    layout: AppStateLayout,
    max_episodes_per_workspace: usize,
    /// Shared across clones. The outer lock guards the per-workspace map and is held
    /// only to fetch/create an entry; each workspace's own lock serializes its opens
    /// without blocking other workspaces.
    indexes: Arc<Mutex<HashMap<WorkspaceName, Arc<Mutex<WorkspaceIndex>>>>>,
}

impl EpisodeStore {
    /// Creates a store that persists under `layout`.
    pub(crate) fn new(layout: AppStateLayout) -> Self {
        Self {
            layout,
            max_episodes_per_workspace: MAX_EPISODES_PER_WORKSPACE,
            indexes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Returns the index handle for `workspace`, creating an empty (unhydrated) one
    /// on first use. Holds the outer lock only briefly.
    fn workspace_index(&self, workspace: &WorkspaceName) -> Arc<Mutex<WorkspaceIndex>> {
        // Recover from a poisoned lock rather than panic: a prior open that panicked
        // mid-critical-section leaves the index self-healing (it re-hydrates from the
        // log on the next open), so one failure must not brick the whole store.
        let mut indexes = self
            .indexes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(
            indexes
                .entry(workspace.clone())
                .or_insert_with(|| Arc::new(Mutex::new(WorkspaceIndex::default()))),
        )
    }

    /// Registers `episode`, idempotently.
    ///
    /// Re-opening the same `episode_id` with an identical `{ intent, parent }` is a
    /// no-op; a different intent/parent returns [`EpisodeStoreError::Conflict`]
    /// (intent is immutable per episode — a change is a new child/sibling). A new id
    /// past the per-workspace ceiling returns [`EpisodeStoreError::CapacityExceeded`].
    ///
    /// The idempotency/conflict check and the cap are served from the per-workspace
    /// in-memory index (hydrated from the log on first use), so an open is O(1)
    /// rather than a full-log scan. The per-workspace index lock makes the
    /// check-then-append atomic against concurrent opens for the *same* workspace
    /// without serializing others; the cross-process state lock is held only briefly
    /// around the append. Intent may be PII, so the log is written 0600. The id is
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

        let path = self.layout.episodes_file(&episode.workspace);
        let index = self.workspace_index(&episode.workspace);
        let mut index = index
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !index.hydrated {
            hydrate_index(&path, &mut index.entries)?;
            index.hydrated = true;
        }

        if let Some(existing) = index.entries.get(episode.id.as_str()) {
            return if existing.intent == intent
                && existing.parent_episode_id.as_deref()
                    == episode.parent_episode_id.as_ref().map(EpisodeId::as_str)
            {
                Ok(())
            } else {
                Err(EpisodeStoreError::Conflict {
                    episode_id: episode.id.as_str().to_string(),
                })
            };
        }

        if index.entries.len() >= self.max_episodes_per_workspace {
            return Err(EpisodeStoreError::CapacityExceeded {
                max: self.max_episodes_per_workspace,
            });
        }

        // Append under the cross-process state lock (atomic file write + torn-tail
        // handling), then mirror the record into the in-memory index.
        {
            let _lock = FileLock::exclusive(self.layout.state_lock())?;
            append_record(&path, &PersistedEpisode::from_episode(episode, intent))?;
        }
        index.entries.insert(
            episode.id.as_str().to_string(),
            IndexEntry {
                intent: intent.to_string(),
                parent_episode_id: episode
                    .parent_episode_id
                    .as_ref()
                    .map(|parent| parent.as_str().to_string()),
            },
        );
        Ok(())
    }

    /// Overrides the per-workspace episode ceiling so tests can exercise the cap
    /// without writing the full default.
    #[cfg(test)]
    pub(crate) fn with_max_episodes_per_workspace(mut self, max: usize) -> Self {
        self.max_episodes_per_workspace = max;
        self
    }
}

/// Unix-nanoseconds timestamp for `created_at_unix_nanos`.
pub(crate) fn now_unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos())
}

/// Parses every intact record in `contents`, in file order, skipping blank lines
/// and torn records.
///
/// Splits on newlines so a torn record is skipped whether the crash broke JSON *or*
/// UTF-8 (a crash can truncate a multi-byte intent mid-character) — one torn write
/// must never brick reads for the workspace. An unacknowledged torn write is simply
/// re-appended when the client retries `OpenEpisode`.
fn parse_records(contents: &[u8]) -> impl Iterator<Item = PersistedEpisode> + '_ {
    contents
        .split(|&byte| byte == b'\n')
        .filter_map(|raw_line| {
            let Ok(line) = std::str::from_utf8(raw_line) else {
                tracing::warn!("skipping episode record with invalid UTF-8 (likely a torn write)");
                return None;
            };
            if line.trim().is_empty() {
                return None;
            }
            match serde_json::from_str::<PersistedEpisode>(line) {
                Ok(record) => Some(record),
                Err(error) => {
                    tracing::warn!(%error, "skipping unparsable episode record");
                    None
                }
            }
        })
}

/// Populates `entries` from the on-disk log, last-write-wins per id, so the
/// in-memory index reflects the persisted state before the first open is served.
fn hydrate_index(
    path: &Path,
    entries: &mut HashMap<String, IndexEntry>,
) -> Result<(), EpisodeStoreError> {
    let contents = match std::fs::read(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    for record in parse_records(&contents) {
        entries.insert(
            record.id,
            IndexEntry {
                intent: record.intent,
                parent_episode_id: record.parent_episode_id,
            },
        );
    }
    Ok(())
}

/// Appends one record to `path` as a private (0600) JSONL line.
fn append_record(path: &Path, record: &PersistedEpisode) -> Result<(), EpisodeStoreError> {
    let encoded = serde_json::to_vec(record)?;
    let mut bytes = Vec::with_capacity(encoded.len() + 2);
    // A prior torn append can leave the file without a trailing newline; start this
    // record on its own line so the incomplete one stays separately skippable
    // instead of merging with (and corrupting) this record.
    if file_needs_leading_newline(path)? {
        bytes.push(b'\n');
    }
    bytes.extend_from_slice(&encoded);
    bytes.push(b'\n');
    storage_fs::append_file_private(path, &bytes)?;
    Ok(())
}

/// Returns the most recent record for `episode_id` in `path`, if any. Test-only:
/// production reads go through the in-memory index.
#[cfg(test)]
fn read_episode(
    path: &Path,
    episode_id: &str,
) -> Result<Option<PersistedEpisode>, EpisodeStoreError> {
    let contents = match std::fs::read(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    Ok(parse_records(&contents)
        .filter(|record| record.id == episode_id)
        .last())
}

/// Whether an append to `path` must start with a newline: the file exists, is
/// non-empty, and does not already end in one (e.g. after a torn append). Keeps
/// an incomplete trailing record on its own line so it never merges with the
/// next record.
fn file_needs_leading_newline(path: &Path) -> Result<bool, EpisodeStoreError> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    if file.seek(SeekFrom::End(0))? == 0 {
        return Ok(false);
    }
    file.seek(SeekFrom::End(-1))?;
    let mut last = [0_u8; 1];
    file.read_exact(&mut last)?;
    Ok(last[0] != b'\n')
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use super::{
        CORAL_EPISODE_INTENT_MAX_CHARS, Episode, EpisodeId, EpisodeStore, EpisodeStoreError,
        now_unix_nanos, read_episode,
    };
    use crate::state::AppStateLayout;
    use crate::workspaces::WorkspaceName;

    fn layout() -> (TempDir, AppStateLayout) {
        let dir = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(dir.path().join("coral-config")))
            .expect("layout should resolve");
        (dir, layout)
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
        let (_dir, layout) = layout();
        let store = EpisodeStore::new(layout);
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
        let (_dir, layout) = layout();
        let store = EpisodeStore::new(layout.clone());
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        store
            .open_episode(&episode(&workspace, "ep_1", "find the form", None))
            .expect("open");
        // A retry with accidental surrounding whitespace stays idempotent.
        store
            .open_episode(&episode(&workspace, "ep_1", "  find the form  ", None))
            .expect("whitespace-only difference is idempotent");
        let read = read_episode(&layout.episodes_file(&workspace), "ep_1")
            .expect("read")
            .expect("present");
        assert_eq!(read.intent, "find the form", "stored intent is normalized");
        let lines = fs::read_to_string(layout.episodes_file(&workspace))
            .expect("read file")
            .lines()
            .filter(|line| !line.trim().is_empty())
            .count();
        assert_eq!(
            lines, 1,
            "whitespace-only difference must not append a record"
        );
    }

    #[test]
    fn read_tolerates_a_torn_trailing_record() {
        use std::io::Write as _;

        let (_dir, layout) = layout();
        let store = EpisodeStore::new(layout.clone());
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        let path = layout.episodes_file(&workspace);
        store
            .open_episode(&episode(&workspace, "ep_1", "first intent", None))
            .expect("open");
        // Simulate a crash mid-append: a complete record followed by a torn line
        // with no trailing newline.
        fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open for append")
            .write_all(b"{\"id\":\"ep_2\",\"workspace\":\"acme\"")
            .expect("write torn record");
        // The torn line is skipped, not fatal.
        let read = read_episode(&path, "ep_1")
            .expect("read tolerates the torn tail")
            .expect("ep_1 present");
        assert_eq!(read.intent, "first intent");
        // The next append starts on a fresh line (no merge), so the new record is
        // readable and the torn one stays isolated.
        store
            .open_episode(&episode(&workspace, "ep_2", "second intent", None))
            .expect("open after torn tail");
        let recovered = read_episode(&path, "ep_2")
            .expect("read")
            .expect("ep_2 present after recovery");
        assert_eq!(recovered.intent, "second intent");
    }

    #[test]
    fn read_tolerates_a_torn_record_with_invalid_utf8() {
        use std::io::Write as _;

        let (_dir, layout) = layout();
        let store = EpisodeStore::new(layout.clone());
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        let path = layout.episodes_file(&workspace);
        store
            .open_episode(&episode(&workspace, "ep_1", "first intent", None))
            .expect("open");
        // A crash can truncate a multi-byte (non-ASCII) intent mid-character,
        // leaving invalid UTF-8 at the tail — `{` then the first two bytes of a
        // 4-byte emoji.
        fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("open for append")
            .write_all(&[b'{', 0xF0, 0x9F])
            .expect("write torn invalid-utf8 record");
        let read = read_episode(&path, "ep_1")
            .expect("read tolerates an invalid-UTF-8 tail")
            .expect("ep_1 present");
        assert_eq!(read.intent, "first intent");
    }

    #[test]
    fn open_then_read_round_trips() {
        let (_dir, layout) = layout();
        let store = EpisodeStore::new(layout.clone());
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        let ep = episode(&workspace, "ep_1", "find the HR onboarding form", None);
        store.open_episode(&ep).expect("open");
        let read = read_episode(&layout.episodes_file(&workspace), "ep_1")
            .expect("read")
            .expect("episode should be present");
        assert_eq!(read.id, "ep_1");
        assert_eq!(read.workspace, "acme");
        assert_eq!(read.intent, "find the HR onboarding form");
        assert_eq!(read.parent_episode_id, None);
        assert_eq!(read.created_at_unix_nanos, ep.created_at_unix_nanos);
    }

    #[test]
    fn reopen_identical_is_idempotent() {
        let (_dir, layout) = layout();
        let store = EpisodeStore::new(layout.clone());
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        let ep = episode(&workspace, "ep_1", "same intent", None);
        store.open_episode(&ep).expect("first open");
        store.open_episode(&ep).expect("idempotent reopen");
        let lines = fs::read_to_string(layout.episodes_file(&workspace))
            .expect("read file")
            .lines()
            .filter(|line| !line.trim().is_empty())
            .count();
        assert_eq!(lines, 1, "idempotent reopen must not append a duplicate");
    }

    #[test]
    fn reopen_with_different_intent_conflicts() {
        let (_dir, layout) = layout();
        let store = EpisodeStore::new(layout);
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
        let (_dir, layout) = layout();
        let store = EpisodeStore::new(layout.clone());
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        store
            .open_episode(&episode(&workspace, "root_ep", "root task", None))
            .expect("open root");
        let child = episode(&workspace, "child_ep", "sub task", Some("root_ep"));
        store.open_episode(&child).expect("open child");
        let read = read_episode(&layout.episodes_file(&workspace), "child_ep")
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
        let (_dir, layout) = layout();
        let store = EpisodeStore::new(layout.clone());
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        let child = episode(&workspace, "child_ep", "sub task", Some("root_ep"));
        store.open_episode(&child).expect("first open");
        store
            .open_episode(&child)
            .expect("identical reopen (same id/intent/parent) is idempotent");
        let lines = fs::read_to_string(layout.episodes_file(&workspace))
            .expect("read file")
            .lines()
            .filter(|line| !line.trim().is_empty())
            .count();
        assert_eq!(
            lines, 1,
            "reopening with an identical parent must not append a duplicate"
        );
    }

    #[test]
    fn reopen_same_intent_different_parent_conflicts() {
        let (_dir, layout) = layout();
        let store = EpisodeStore::new(layout);
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
        let (_dir, layout) = layout();
        let store = EpisodeStore::new(layout.clone());
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
            read_episode(&layout.episodes_file(&acme), "ep_1")
                .unwrap()
                .unwrap()
                .intent,
            "in acme"
        );
        assert_eq!(
            read_episode(&layout.episodes_file(&globex), "ep_1")
                .unwrap()
                .unwrap()
                .intent,
            "in globex"
        );
    }

    #[test]
    fn a_fresh_store_hydrates_existing_episodes_from_disk() {
        let (_dir, layout) = layout();
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        {
            // First store persists an episode, then is dropped (its index with it).
            let store = EpisodeStore::new(layout.clone());
            store
                .open_episode(&episode(&workspace, "ep_1", "intent A", None))
                .expect("open");
        }
        // A second store starts cold and must rebuild its index from the log, so it
        // still sees ep_1: an identical reopen is idempotent and a changed intent
        // conflicts — neither would hold if the cold index ignored the persisted log.
        let store = EpisodeStore::new(layout.clone());
        store
            .open_episode(&episode(&workspace, "ep_1", "intent A", None))
            .expect("identical reopen is idempotent after hydrating from disk");
        let conflict = store
            .open_episode(&episode(&workspace, "ep_1", "intent B", None))
            .expect_err("hydrated index must detect the conflict");
        assert!(matches!(conflict, EpisodeStoreError::Conflict { .. }));
        let lines = fs::read_to_string(layout.episodes_file(&workspace))
            .expect("read file")
            .lines()
            .filter(|line| !line.trim().is_empty())
            .count();
        assert_eq!(
            lines, 1,
            "the idempotent reopen must not append a duplicate"
        );
    }

    #[test]
    fn open_rejects_new_episodes_past_the_workspace_cap() {
        let (_dir, layout) = layout();
        let store = EpisodeStore::new(layout).with_max_episodes_per_workspace(2);
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        store
            .open_episode(&episode(&workspace, "ep_1", "first", None))
            .expect("first under cap");
        store
            .open_episode(&episode(&workspace, "ep_2", "second", None))
            .expect("second at cap");
        let full = store
            .open_episode(&episode(&workspace, "ep_3", "third", None))
            .expect_err("a new id past the cap must be rejected");
        assert!(matches!(full, EpisodeStoreError::CapacityExceeded { .. }));
        // The cap bounds new ids only — re-opening an existing one stays idempotent.
        store
            .open_episode(&episode(&workspace, "ep_1", "first", None))
            .expect("idempotent reopen is allowed at the cap");
    }

    #[test]
    fn cap_is_per_workspace() {
        let (_dir, layout) = layout();
        let store = EpisodeStore::new(layout).with_max_episodes_per_workspace(1);
        let acme = WorkspaceName::parse("acme").expect("workspace");
        let globex = WorkspaceName::parse("globex").expect("workspace");
        store
            .open_episode(&episode(&acme, "ep_1", "in acme", None))
            .expect("acme at cap");
        // A different workspace has its own budget, so its first open still succeeds.
        store
            .open_episode(&episode(&globex, "ep_1", "in globex", None))
            .expect("globex has its own cap");
        // acme is full, though.
        let full = store
            .open_episode(&episode(&acme, "ep_2", "more acme", None))
            .expect_err("acme is at its cap");
        assert!(matches!(full, EpisodeStoreError::CapacityExceeded { .. }));
    }
}
