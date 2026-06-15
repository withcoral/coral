//! Per-workspace, append-only episode store.
//!
//! Persists `{ episode_id -> intent, parent }` for an episode (one task-attempt),
//! written once by `OpenEpisode` into the per-workspace episode log resolved by
//! [`AppStateLayout`]. Minimal by design for PR 1: a private, locked JSONL append.
//! JSONL→Parquet compaction, retention/eviction, and the queryable
//! `coral.episodes` surface land in a later PR.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use coral_api::CORAL_EPISODE_INTENT_MAX_CHARS;
use serde::{Deserialize, Serialize};

use super::EpisodeId;
use crate::state::AppStateLayout;
use crate::storage::fs::{self as storage_fs, FileLock};
use crate::workspaces::WorkspaceName;

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
}

/// Append-only, per-workspace JSONL episode store. Paths and the shared state
/// lock are resolved through [`AppStateLayout`].
#[derive(Clone)]
pub(crate) struct EpisodeStore {
    layout: AppStateLayout,
}

impl EpisodeStore {
    /// Creates a store that persists under `layout`.
    pub(crate) fn new(layout: AppStateLayout) -> Self {
        Self { layout }
    }

    /// Registers `episode`, idempotently.
    ///
    /// Re-opening the same `episode_id` with an identical `{ intent, parent }` is a
    /// no-op; a different intent/parent returns [`EpisodeStoreError::Conflict`]
    /// (intent is immutable per episode — a change is a new child/sibling).
    ///
    /// The shared state lock is held across the read-then-append so the
    /// idempotency/conflict check and the write are atomic against concurrent
    /// opens. Intent may be PII, so the log is written with private (0600)
    /// permissions. The id is already validated ([`EpisodeId`]); intent is
    /// checked here as a safety net for callers that bypass the service boundary.
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
        let _lock = FileLock::exclusive(self.layout.state_lock())?;
        let path = self.layout.episodes_file(&episode.workspace);
        if let Some(existing) = read_episode(&path, episode.id.as_str())? {
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
        let record = serde_json::to_vec(&PersistedEpisode::from_episode(episode, intent))?;
        let mut bytes = Vec::with_capacity(record.len() + 2);
        // A prior torn append can leave the file without a trailing newline; start
        // this record on its own line so the incomplete one stays separately
        // skippable instead of merging with (and corrupting) this record.
        if file_needs_leading_newline(&path)? {
            bytes.push(b'\n');
        }
        bytes.extend_from_slice(&record);
        bytes.push(b'\n');
        storage_fs::append_file_private(&path, &bytes)?;
        Ok(())
    }
}

/// Unix-nanoseconds timestamp for `created_at_unix_nanos`.
pub(crate) fn now_unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos())
}

/// Returns the most recent record for `episode_id` in `path`, if any.
///
/// Reads raw bytes and splits on newlines so a torn final record is skipped
/// whether the crash broke JSON *or* UTF-8 (it can truncate a multi-byte intent
/// mid-character) — one torn write must never brick reads for the workspace.
fn read_episode(
    path: &Path,
    episode_id: &str,
) -> Result<Option<PersistedEpisode>, EpisodeStoreError> {
    let contents = match std::fs::read(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut found = None;
    for raw_line in contents.split(|&byte| byte == b'\n') {
        // A torn final record can be invalid UTF-8 (e.g. a truncated non-ASCII
        // intent); skip it rather than failing the whole read.
        let Ok(line) = std::str::from_utf8(raw_line) else {
            tracing::warn!("skipping episode record with invalid UTF-8 (likely a torn write)");
            continue;
        };
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<PersistedEpisode>(line) {
            Ok(episode) => {
                if episode.id == episode_id {
                    found = Some(episode);
                }
            }
            // A crash mid-append can also leave a syntactically torn (but valid
            // UTF-8) record; skip it too. An unacknowledged torn write is simply
            // re-appended when the client retries `OpenEpisode`.
            Err(error) => {
                tracing::warn!(%error, "skipping unparsable episode record");
            }
        }
    }
    Ok(found)
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
}
