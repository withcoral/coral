//! Per-workspace, append-only episode store.
//!
//! Persists `{ episode_id -> intent, parent }` for an episode (one task-attempt),
//! written once by `OpenEpisode` into the per-workspace episode log resolved by
//! [`AppStateLayout`]: a private (0600), locked JSONL append, bounded by a
//! per-workspace **byte ceiling with oldest-out eviction** — when a new record
//! would exceed the limit, the oldest records are dropped to make room, so the log
//! keeps the most recent episodes within budget (a byte-bounded FIFO; no time
//! component). The idempotency scan already reads every record, so eviction folds
//! into the same pass. The always-registered `OpenEpisode` route makes an unbounded
//! log a real fill-the-disk surface, which this prevents. JSONL→Parquet compaction
//! and the queryable `coral.episodes` surface land in a later PR.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use coral_api::CORAL_EPISODE_INTENT_MAX_CHARS;
use serde::{Deserialize, Serialize};

use super::EpisodeId;
use crate::bootstrap::AppError;
use crate::state::AppStateLayout;
use crate::state::db::{CoralDb, DbRepos, EpisodeRecord};
use crate::storage::fs::{self as storage_fs, FileLock};
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
    /// Durable database persistence failed.
    #[error("episode store persistence: {0}")]
    Persistence(#[from] AppError),
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

impl From<crate::state::db::DbError> for EpisodeStoreError {
    fn from(error: crate::state::db::DbError) -> Self {
        Self::Persistence(AppError::from(error))
    }
}

/// Append-only, per-workspace JSONL episode store, bounded by a byte ceiling with
/// oldest-out eviction. Paths and the shared state lock are resolved through
/// [`AppStateLayout`].
#[derive(Clone)]
pub(crate) struct EpisodeStore {
    layout: AppStateLayout,
    max_bytes: u64,
    catalog_db: Option<Arc<CoralDb>>,
}

impl EpisodeStore {
    /// Creates a store that persists under `layout` with the default byte ceiling.
    pub(crate) fn new(layout: AppStateLayout) -> Self {
        Self {
            layout,
            max_bytes: MAX_EPISODE_BYTES_PER_WORKSPACE,
            catalog_db: None,
        }
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "DB-backed episode writes are exercised by tests here and wired to production after legacy episode import in the next stack PR."
        )
    )]
    pub(crate) fn with_db(layout: AppStateLayout, catalog_db: Arc<CoralDb>) -> Self {
        Self {
            layout,
            max_bytes: MAX_EPISODE_BYTES_PER_WORKSPACE,
            catalog_db: Some(catalog_db),
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
    /// push the log over, the oldest records are evicted to make room (the log is a
    /// byte-bounded FIFO; the newest record is always kept). The shared state lock is
    /// held across read-then-write so the idempotency check and the
    /// evict-and-append are atomic against concurrent opens. Intent may be PII, so
    /// the log is written 0600. The id is already validated ([`EpisodeId`]); intent
    /// is checked here as a safety net for callers that bypass the service boundary.
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
        if let Some(db) = self.catalog_db.clone() {
            return open_episode_in_db(db, episode, intent, self.max_bytes);
        }
        let path = self.layout.episodes_file(&episode.workspace);

        // One read serves both the idempotency check and eviction (file order is
        // chronological — oldest first).
        let records = read_all_records(&path)?;
        if let Some(existing) = records
            .iter()
            .rfind(|record| record.id == episode.id.as_str())
        {
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

        append_within_budget(
            &path,
            records,
            PersistedEpisode::from_episode(episode, intent),
            self.max_bytes,
        )
    }
}

fn open_episode_in_db(
    db: Arc<CoralDb>,
    episode: &Episode,
    intent: &str,
    max_bytes: u64,
) -> Result<(), EpisodeStoreError> {
    let workspace = episode.workspace.clone();
    let id = episode.id.as_str().to_string();
    let parent_episode_id = episode
        .parent_episode_id
        .as_ref()
        .map(EpisodeId::as_str)
        .map(str::to_string);
    let intent = intent.to_string();
    let workspace_created_at_unix_nanos = db_timestamp_from_unix_nanos(now_unix_nanos())?;

    run_episode_db_operation(async move {
        let mut tx = db.begin().await?;
        tx.workspaces()
            .ensure_write_locked(workspace.as_str(), workspace_created_at_unix_nanos)
            .await?;
        let mut episodes = tx.episodes().list_workspace_episodes(&workspace).await?;
        let (created_at_unix_nanos_raw, created_at_unix_nanos) =
            next_db_episode_created_at(&episodes)?;
        let record_bytes = episode_record_bytes(
            &workspace,
            &id,
            intent.as_str(),
            parent_episode_id.as_deref(),
            created_at_unix_nanos_raw,
        )?;
        let episode_record = EpisodeRecord {
            id: id.clone(),
            intent,
            parent_episode_id,
            created_at_unix_nanos,
            record_bytes,
        };
        if let Some(existing) = episodes.iter().find(|existing| existing.id == id) {
            return if existing.intent == episode_record.intent
                && existing.parent_episode_id == episode_record.parent_episode_id
            {
                tx.commit().await?;
                Ok(())
            } else {
                Err(EpisodeStoreError::Conflict {
                    episode_id: id.clone(),
                })
            };
        }
        episodes.push(episode_record);
        let kept = retain_episode_records_within_budget(episodes, max_bytes);
        tx.episodes()
            .replace_workspace_episodes(&workspace, &kept)
            .await?;
        tx.commit().await?;
        Ok(())
    })
}

fn next_db_episode_created_at(
    episodes: &[EpisodeRecord],
) -> Result<(u128, i64), EpisodeStoreError> {
    let now = db_timestamp_from_unix_nanos(now_unix_nanos())?;
    let created_at_unix_nanos = match episodes
        .iter()
        .map(|episode| episode.created_at_unix_nanos)
        .max()
    {
        Some(max_existing) => now.max(max_existing.checked_add(1).ok_or_else(|| {
            EpisodeStoreError::Persistence(AppError::FailedPrecondition(
                "episode timestamp space exhausted".to_string(),
            ))
        })?),
        None => now,
    };
    let raw = u128::try_from(created_at_unix_nanos).map_err(|error| {
        EpisodeStoreError::Persistence(AppError::FailedPrecondition(format!(
            "episode timestamp is negative: {error}"
        )))
    })?;
    Ok((raw, created_at_unix_nanos))
}

fn db_timestamp_from_unix_nanos(nanos: u128) -> Result<i64, EpisodeStoreError> {
    i64::try_from(nanos).map_err(|error| {
        EpisodeStoreError::Persistence(AppError::FailedPrecondition(format!(
            "episode timestamp exceeds i64 nanoseconds: {error}"
        )))
    })
}

fn retain_episode_records_within_budget(
    mut episodes: Vec<EpisodeRecord>,
    max_bytes: u64,
) -> Vec<EpisodeRecord> {
    episodes.sort_by(|left, right| {
        left.created_at_unix_nanos
            .cmp(&right.created_at_unix_nanos)
            .then_with(|| left.id.cmp(&right.id))
    });
    let mut total: u64 = episodes
        .iter()
        .map(|episode| u64::try_from(episode.record_bytes).unwrap_or(u64::MAX))
        .sum();
    while episodes.len() > 1 && total > max_bytes {
        let removed = episodes.remove(0);
        total = total.saturating_sub(u64::try_from(removed.record_bytes).unwrap_or(u64::MAX));
    }
    episodes
}

fn episode_record_bytes(
    workspace: &WorkspaceName,
    id: &str,
    intent: &str,
    parent_episode_id: Option<&str>,
    created_at_unix_nanos: u128,
) -> Result<i64, EpisodeStoreError> {
    let record = PersistedEpisode {
        id: id.to_string(),
        workspace: workspace.as_str().to_string(),
        intent: intent.to_string(),
        parent_episode_id: parent_episode_id.map(str::to_string),
        created_at_unix_nanos,
    };
    let bytes = serde_json::to_vec(&record)?.len() + 1;
    i64::try_from(bytes).map_err(|error| {
        EpisodeStoreError::Persistence(AppError::FailedPrecondition(format!(
            "episode record size exceeds i64 bytes: {error}"
        )))
    })
}

fn run_episode_db_operation<T, F>(operation: F) -> Result<T, EpisodeStoreError>
where
    T: Send + 'static,
    F: std::future::Future<Output = Result<T, EpisodeStoreError>> + Send + 'static,
{
    fn run_on_runtime<T, F>(operation: F) -> Result<T, EpisodeStoreError>
    where
        F: std::future::Future<Output = Result<T, EpisodeStoreError>>,
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| {
                EpisodeStoreError::Persistence(AppError::FailedPrecondition(format!(
                    "failed to create episode database runtime: {error}"
                )))
            })?;
        runtime.block_on(operation)
    }

    if tokio::runtime::Handle::try_current().is_ok() {
        return std::thread::spawn(move || run_on_runtime(operation))
            .join()
            .map_err(|_panic| {
                EpisodeStoreError::Persistence(AppError::FailedPrecondition(
                    "episode database operation thread panicked".to_string(),
                ))
            })?;
    }

    run_on_runtime(operation)
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

/// Reads every intact record in `path`, in file order (empty if the log is absent).
fn read_all_records(path: &Path) -> Result<Vec<PersistedEpisode>, EpisodeStoreError> {
    let contents = match std::fs::read(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    Ok(parse_records(&contents).collect())
}

/// Appends `record` to `path`, evicting the oldest records first if needed to keep
/// the log within `max_bytes`. `existing` is the current log in file order (oldest
/// first). The newest record is always kept, even if it alone exceeds `max_bytes`.
fn append_within_budget(
    path: &Path,
    existing: Vec<PersistedEpisode>,
    record: PersistedEpisode,
    max_bytes: u64,
) -> Result<(), EpisodeStoreError> {
    let line_len = serde_json::to_vec(&record)?.len() as u64 + 1;
    // Fast path: it fits on top of what's already on disk. `file_len` counts physical
    // bytes (so any torn-record bytes are included conservatively); add the separator
    // newline `append_record` will prepend when the file lacks a trailing one, so a
    // torn-tailed file at the boundary doesn't slip one byte over the ceiling.
    let leading = u64::from(file_needs_leading_newline(path)?);
    if file_len(path)?
        .saturating_add(line_len)
        .saturating_add(leading)
        <= max_bytes
    {
        return append_record(path, &record);
    }

    // Over budget: rebuild the log keeping the newest records (and always `record`)
    // that fit, dropping the oldest. Rewriting also compacts away torn records.
    let mut kept = existing;
    kept.push(record);
    let encoded: Vec<Vec<u8>> = kept
        .iter()
        .map(serde_json::to_vec)
        .collect::<Result<_, _>>()?;
    let record_count = encoded.len();
    let mut total: u64 = encoded.iter().map(|line| line.len() as u64 + 1).sum();
    // Drop oldest (front) records until within budget, but never the newest (last).
    let mut dropped = 0;
    for line in &encoded {
        if total <= max_bytes || dropped + 1 >= record_count {
            break;
        }
        total -= line.len() as u64 + 1;
        dropped += 1;
    }
    let mut bytes = Vec::new();
    for line in encoded.iter().skip(dropped) {
        bytes.extend_from_slice(line);
        bytes.push(b'\n');
    }
    // `write_atomic` (temp-file + rename) does not create the parent dir, so ensure
    // it for the first-ever write. Crash-safe, so a crash mid-eviction never
    // truncates the log.
    if let Some(parent) = path.parent() {
        storage_fs::ensure_dir(parent)?;
    }
    storage_fs::write_atomic(path, &bytes)?;
    Ok(())
}

/// Current on-disk size of `path` in bytes, or 0 if the log does not exist yet.
fn file_len(path: &Path) -> Result<u64, EpisodeStoreError> {
    match std::fs::metadata(path) {
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error.into()),
    }
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
/// production reads go through [`read_all_records`].
#[cfg(test)]
fn read_episode(
    path: &Path,
    episode_id: &str,
) -> Result<Option<PersistedEpisode>, EpisodeStoreError> {
    Ok(read_all_records(path)?
        .into_iter()
        .rfind(|record| record.id == episode_id))
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
    use std::{fs, sync::Arc, time::Duration};

    use tempfile::TempDir;

    use super::{
        CORAL_EPISODE_INTENT_MAX_CHARS, Episode, EpisodeId, EpisodeStore, EpisodeStoreError,
        PersistedEpisode, now_unix_nanos, read_episode,
    };
    use crate::bootstrap;
    use crate::state::AppStateLayout;
    use crate::state::db::{CoralDb, DatabaseConfig, DbRepos, ResolvedDatabaseConfig};
    use crate::workspaces::WorkspaceName;

    /// On-disk byte size of one record (serialized JSON + newline), for sizing the
    /// byte ceiling in eviction tests.
    fn record_bytes(workspace: &WorkspaceName, id: &str, intent: &str) -> u64 {
        let record = PersistedEpisode::from_episode(&episode(workspace, id, intent, None), intent);
        serde_json::to_vec(&record).expect("serialize").len() as u64 + 1
    }

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

    fn unique_workspace(prefix: &str) -> WorkspaceName {
        WorkspaceName::parse(&format!("{prefix}{}", uuid::Uuid::new_v4().simple()))
            .expect("workspace")
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
    fn open_evicts_oldest_to_stay_under_the_byte_ceiling() {
        let (_dir, layout) = layout();
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        // Size the ceiling to hold exactly two records (ids and intent are uniform, so
        // every record serializes to the same length).
        let one = record_bytes(&workspace, "ep_0", "task");
        let store = EpisodeStore::new(layout.clone()).with_max_bytes(one * 2);
        for id in ["ep_1", "ep_2", "ep_3"] {
            store
                .open_episode(&episode(&workspace, id, "task", None))
                .expect("open");
        }

        let path = layout.episodes_file(&workspace);
        assert!(
            read_episode(&path, "ep_1").unwrap().is_none(),
            "the oldest record must be evicted"
        );
        assert!(read_episode(&path, "ep_2").unwrap().is_some());
        assert!(read_episode(&path, "ep_3").unwrap().is_some());
        assert!(
            fs::metadata(&path).unwrap().len() <= one * 2,
            "the log must stay within the byte ceiling"
        );
    }

    #[tokio::test]
    async fn db_store_round_trips_idempotency_and_conflict() {
        let (_dir, layout) = layout();
        let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(&layout).expect("db config")
        else {
            panic!("default episode test DB should be sqlite");
        };
        let db = Arc::new(
            CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
                .await
                .expect("open sqlite"),
        );
        db.migrate().await.expect("migrate sqlite");
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        let store = EpisodeStore::with_db(layout.clone(), Arc::clone(&db));
        let ep = episode(&workspace, "ep_1", " same intent ", Some("root_ep"));

        store.open_episode(&ep).expect("open");
        store.open_episode(&ep).expect("idempotent reopen");
        let conflict = store
            .open_episode(&episode(&workspace, "ep_1", "different", Some("root_ep")))
            .expect_err("changed intent should conflict");

        assert!(matches!(conflict, EpisodeStoreError::Conflict { .. }));
        let mut session = db.as_ref();
        let episodes = session
            .episodes()
            .list_workspace_episodes(&workspace)
            .await
            .expect("list episodes");
        assert_eq!(episodes.len(), 1);
        let episode = episodes.first().expect("episode");
        assert_eq!(episode.intent, "same intent");
        assert_eq!(episode.parent_episode_id.as_deref(), Some("root_ep"));
        assert!(
            !layout.episodes_file(&workspace).exists(),
            "DB-backed episode store should not append legacy JSONL"
        );
    }

    #[tokio::test]
    async fn db_store_reopen_same_intent_different_parent_conflicts() {
        let (_dir, layout) = layout();
        let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(&layout).expect("db config")
        else {
            panic!("default episode test DB should be sqlite");
        };
        let db = Arc::new(
            CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
                .await
                .expect("open sqlite"),
        );
        db.migrate().await.expect("migrate sqlite");
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        let store = EpisodeStore::with_db(layout, Arc::clone(&db));

        store
            .open_episode(&episode(
                &workspace,
                "child_ep",
                "sub task",
                Some("parent_a"),
            ))
            .expect("first open");
        let error = store
            .open_episode(&episode(
                &workspace,
                "child_ep",
                "sub task",
                Some("parent_b"),
            ))
            .expect_err("changed parent must conflict");

        assert!(matches!(error, EpisodeStoreError::Conflict { .. }));
        let mut session = db.as_ref();
        let episodes = session
            .episodes()
            .list_workspace_episodes(&workspace)
            .await
            .expect("list episodes");
        assert_eq!(episodes.len(), 1);
        assert_eq!(
            episodes
                .first()
                .expect("episode")
                .parent_episode_id
                .as_deref(),
            Some("parent_a")
        );
    }

    #[tokio::test]
    async fn db_store_evicts_oldest_to_stay_under_byte_ceiling() {
        let (_dir, layout) = layout();
        let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(&layout).expect("db config")
        else {
            panic!("default episode test DB should be sqlite");
        };
        let db = Arc::new(
            CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
                .await
                .expect("open sqlite"),
        );
        db.migrate().await.expect("migrate sqlite");
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        let store = EpisodeStore::with_db(layout, Arc::clone(&db)).with_max_bytes(1);
        let mut first = episode(&workspace, "ep_1", "task", None);
        first.created_at_unix_nanos = now_unix_nanos() + 1_000_000_000;
        let mut second = episode(&workspace, "ep_2", "task", None);
        second.created_at_unix_nanos = 1;

        store.open_episode(&first).expect("open first");
        store.open_episode(&second).expect("open second");

        let mut session = db.as_ref();
        assert!(
            session
                .episodes()
                .get(&workspace, "ep_1")
                .await
                .expect("get ep_1")
                .is_none(),
            "the oldest DB episode should be evicted"
        );
        assert!(
            session
                .episodes()
                .get(&workspace, "ep_2")
                .await
                .expect("get ep_2")
                .is_some()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn db_store_concurrent_opens_do_not_drop_each_other() {
        let (_dir, layout) = layout();
        let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(&layout).expect("db config")
        else {
            panic!("default episode test DB should be sqlite");
        };
        let db = Arc::new(
            CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
                .await
                .expect("open sqlite"),
        );
        db.migrate().await.expect("migrate sqlite");
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        let store = Arc::new(EpisodeStore::with_db(layout, Arc::clone(&db)));

        let mut handles = Vec::new();
        for index in 0..16 {
            let store = Arc::clone(&store);
            let workspace = workspace.clone();
            handles.push(tokio::task::spawn_blocking(move || {
                store
                    .open_episode(&episode(
                        &workspace,
                        &format!("ep_{index}"),
                        &format!("task {index}"),
                        None,
                    ))
                    .expect("open episode");
            }));
        }
        for handle in handles {
            handle.await.expect("join open task");
        }

        let mut session = db.as_ref();
        let episodes = session
            .episodes()
            .list_workspace_episodes(&workspace)
            .await
            .expect("list episodes");
        assert_eq!(
            episodes.len(),
            16,
            "concurrent DB opens must not lose episodes"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the DB-backed episode runtime path against Postgres"]
    async fn open_episode_in_db_runtime_path_against_postgres() {
        let Some(url) = bootstrap::env_var("CORAL_TEST_POSTGRES_URL") else {
            return;
        };
        let db = Arc::new(
            CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
                .await
                .expect("open postgres"),
        );
        db.migrate().await.expect("migrate postgres");
        let (_dir, store_layout) = layout();
        let workspace = unique_workspace("episodepg");
        let store = EpisodeStore::with_db(store_layout, Arc::clone(&db)).with_max_bytes(1);

        store
            .open_episode(&episode(&workspace, "ep_old", "task", None))
            .expect("open first");
        store
            .open_episode(&episode(&workspace, "ep_new", "task", None))
            .expect("open second");
        store
            .open_episode(&episode(&workspace, "ep_new", "task", None))
            .expect("idempotent reopen");
        let conflict = store
            .open_episode(&episode(&workspace, "ep_new", "different", None))
            .expect_err("changed intent should conflict");
        assert!(matches!(conflict, EpisodeStoreError::Conflict { .. }));
        let episodes = {
            let mut session = db.as_ref();
            session
                .episodes()
                .list_workspace_episodes(&workspace)
                .await
                .expect("list retained episodes")
        };
        assert_eq!(episodes.len(), 1);
        assert_eq!(episodes.first().expect("episode").id, "ep_new");

        let race_workspace = unique_workspace("episodepgrace");
        let (_first_dir, first_layout) = layout();
        let (_second_dir, second_layout) = layout();
        let first_store = Arc::new(EpisodeStore::with_db(first_layout, Arc::clone(&db)));
        let second_store = Arc::new(EpisodeStore::with_db(second_layout, Arc::clone(&db)));
        let mut tx = db.begin().await.expect("begin lock tx");
        tx.workspaces()
            .ensure_write_locked(race_workspace.as_str(), 7)
            .await
            .expect("hold workspace lock");
        let (first_started, first) =
            spawn_open(first_store, race_workspace.clone(), "ep_a", "task a");
        let (second_started, second) =
            spawn_open(second_store, race_workspace.clone(), "ep_b", "task b");
        first_started.await.expect("first open started");
        second_started.await.expect("second open started");
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(
            !first.is_finished(),
            "first open should wait on the row lock"
        );
        assert!(
            !second.is_finished(),
            "second open should wait on the row lock"
        );
        tx.commit().await.expect("release workspace lock");
        first.await.expect("join first").expect("open first");
        second.await.expect("join second").expect("open second");

        let mut session = db.as_ref();
        let episodes = session
            .episodes()
            .list_workspace_episodes(&race_workspace)
            .await
            .expect("list raced episodes");
        let ids: Vec<_> = episodes.iter().map(|episode| episode.id.as_str()).collect();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&"ep_a"));
        assert!(ids.contains(&"ep_b"));
    }

    fn spawn_open(
        store: Arc<EpisodeStore>,
        workspace: WorkspaceName,
        id: &'static str,
        intent: &'static str,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::task::JoinHandle<Result<(), EpisodeStoreError>>,
    ) {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::task::spawn_blocking(move || {
            let _receiver_waiting = started_tx.send(()).is_ok();
            store.open_episode(&episode(&workspace, id, intent, None))
        });
        (started_rx, handle)
    }

    #[test]
    fn the_newest_record_is_kept_even_when_it_alone_exceeds_the_ceiling() {
        let (_dir, layout) = layout();
        // A ceiling smaller than any record: each open evicts everything older, so the
        // log always holds exactly the most recent episode — never rejects.
        let store = EpisodeStore::new(layout.clone()).with_max_bytes(1);
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        store
            .open_episode(&episode(&workspace, "ep_1", "first", None))
            .expect("first kept");
        store
            .open_episode(&episode(&workspace, "ep_2", "second", None))
            .expect("second evicts first, never rejects");

        let path = layout.episodes_file(&workspace);
        assert!(read_episode(&path, "ep_1").unwrap().is_none());
        assert_eq!(
            read_episode(&path, "ep_2").unwrap().unwrap().intent,
            "second"
        );
        let lines = fs::read_to_string(&path)
            .unwrap()
            .lines()
            .filter(|line| !line.trim().is_empty())
            .count();
        assert_eq!(lines, 1, "only the newest record remains");
    }

    #[test]
    fn reopening_an_evicted_id_is_a_fresh_episode() {
        let (_dir, layout) = layout();
        let store = EpisodeStore::new(layout.clone()).with_max_bytes(1);
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
        let read = read_episode(&layout.episodes_file(&workspace), "ep_1")
            .unwrap()
            .expect("present");
        assert_eq!(read.intent, "new intent");
    }

    #[test]
    fn byte_ceiling_holds_when_the_log_has_a_torn_tail() {
        // Regression: the fast-path budget must account for the separator newline
        // `append_record` prepends to a file with no trailing newline, or a torn-tailed
        // file sitting at the boundary slips one byte over the ceiling.
        let (_dir, layout) = layout();
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        let one = record_bytes(&workspace, "ep_1", "task");
        let path = layout.episodes_file(&workspace);

        // Seed a single valid record with NO trailing newline (a torn tail), so its
        // on-disk size is `one - 1`.
        let seed = serde_json::to_vec(&PersistedEpisode::from_episode(
            &episode(&workspace, "ep_1", "task", None),
            "task",
        ))
        .expect("serialize");
        fs::create_dir_all(path.parent().unwrap()).expect("mkdir");
        fs::write(&path, &seed).expect("seed torn-tail log");

        // Ceiling = exactly the torn record + one new record. The old fast path would
        // append (adding a leading newline) and land at `2*one`, one over.
        let store = EpisodeStore::new(layout).with_max_bytes(2 * one - 1);
        store
            .open_episode(&episode(&workspace, "ep_2", "task", None))
            .expect("open within budget");
        assert!(
            fs::metadata(&path).unwrap().len() < 2 * one,
            "the log must not exceed the ceiling even when it had a torn tail"
        );
    }

    #[test]
    fn eviction_preserves_idempotency_for_surviving_records() {
        let (_dir, layout) = layout();
        let workspace = WorkspaceName::parse("acme").expect("workspace");
        let one = record_bytes(&workspace, "ep_0", "task");
        let store = EpisodeStore::new(layout.clone()).with_max_bytes(one * 2);
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
