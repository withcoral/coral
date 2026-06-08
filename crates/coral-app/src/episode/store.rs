//! Per-workspace, append-only episode store.
//!
//! Persists `{ episode_id -> intent, parent }` for an episode (one task-attempt),
//! written once by `OpenEpisode` into the per-workspace episode log resolved by
//! [`AppStateLayout`]. Minimal by design for PR 1: a private, locked JSONL append.
//! JSONL→Parquet compaction, retention/eviction, and the queryable
//! `coral.episodes` surface land in PR 2.
#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the store is consumed by the OpenEpisode handler — next PR in the stack"
    )
)]

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use super::EpisodeId;
use crate::state::AppStateLayout;
use crate::storage::fs::{self as storage_fs, FileLock};
use crate::workspaces::WorkspaceName;

/// Maximum intent length, in characters — generous for a task description while
/// bounding the per-record size of the append-only log.
const MAX_INTENT_CHARS: usize = 4096;

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
    fn from_episode(episode: &Episode) -> Self {
        Self {
            id: episode.id.as_str().to_string(),
            workspace: episode.workspace.as_str().to_string(),
            intent: episode.intent.clone(),
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
        if episode.intent.trim().is_empty() || episode.intent.chars().count() > MAX_INTENT_CHARS {
            return Err(EpisodeStoreError::InvalidIntent {
                max: MAX_INTENT_CHARS,
            });
        }
        let _lock = FileLock::exclusive(self.layout.state_lock())?;
        let path = self.layout.episodes_file(&episode.workspace);
        if let Some(existing) = read_episode(&path, episode.id.as_str())? {
            return if existing.intent == episode.intent
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
        let mut line = serde_json::to_vec(&PersistedEpisode::from_episode(episode))?;
        line.push(b'\n');
        storage_fs::append_file_private(&path, &line)?;
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
fn read_episode(
    path: &Path,
    episode_id: &str,
) -> Result<Option<PersistedEpisode>, EpisodeStoreError> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut found = None;
    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let episode: PersistedEpisode = serde_json::from_str(&line)?;
        if episode.id == episode_id {
            found = Some(episode);
        }
    }
    Ok(found)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use super::{
        Episode, EpisodeId, EpisodeStore, EpisodeStoreError, MAX_INTENT_CHARS, now_unix_nanos,
        read_episode,
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
        let overlong = "x".repeat(MAX_INTENT_CHARS + 1);
        let too_long = store
            .open_episode(&episode(&workspace, "ep_long", &overlong, None))
            .expect_err("overlong intent must be rejected");
        assert!(matches!(too_long, EpisodeStoreError::InvalidIntent { .. }));
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
