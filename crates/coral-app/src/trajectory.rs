//! Local trajectory-memory capture, consensus indexing, and exact retrieval.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use coral_engine::QueryFingerprint;
use rusqlite::{Connection, OptionalExtension as _, params};
use serde_json::Value as JsonValue;
use sha2::{Digest as _, Sha256};

use crate::episode::store::{Episode, EpisodeStore, EpisodeStoreError};
use crate::query::manager::{QueryManager, QueryManagerError};
use crate::state::AppStateLayout;
use crate::storage::fs as storage_fs;
use crate::telemetry::local_store::{
    StoredTraceStatus, TraceDetailRecord, TraceSpanRecord, TraceStore, TraceStoreError,
};
use crate::workspaces::WorkspaceName;

const TRACE_PAGE_SIZE: usize = 1_000;
pub(crate) const DEFAULT_MIN_QUERY_CONSENSUS: u32 = 2;

#[derive(Debug, thiserror::Error)]
pub(crate) enum TrajectoryError {
    #[error("trajectory store io: {0}")]
    Io(#[from] std::io::Error),
    #[error("trajectory store sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("trajectory store json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("episode store: {0}")]
    Episode(#[from] EpisodeStoreError),
    #[error("trace store: {0}")]
    Trace(#[from] TraceStoreError),
    #[error("query fingerprint failed: {0}")]
    Query(String),
}

/// Internal local trajectory-memory facade.
#[derive(Clone)]
pub(crate) struct TrajectoryMemory {
    layout: AppStateLayout,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GoldenPath {
    pub(crate) workspace: String,
    pub(crate) intent: String,
    pub(crate) path_key: String,
    pub(crate) steps: Vec<String>,
    pub(crate) relations: Vec<String>,
    pub(crate) query_consensus: u32,
    pub(crate) path_consensus: u32,
    pub(crate) episode_count: u32,
}

#[derive(Debug, Clone)]
struct ObservedStep {
    workspace: String,
    episode_id: String,
    intent: String,
    parent_episode_id: Option<String>,
    trace_id: String,
    span_id: String,
    sql: String,
    status: StoredTraceStatus,
    row_count: u64,
    row_count_recorded: bool,
    start_time_unix_nanos: i64,
    end_time_unix_nanos: i64,
    attributes_json: String,
}

#[derive(Debug, Clone)]
struct IndexedStep {
    observed: ObservedStep,
    step_index: u32,
    fingerprint: Option<QueryFingerprint>,
}

#[derive(Debug, Clone)]
struct StoredStep {
    episode_id: String,
    intent: String,
    step_index: u32,
    sql: String,
    status: StoredTraceStatus,
    relations: Vec<String>,
    shape_hash: Option<String>,
    exact_key: Option<String>,
}

#[derive(Debug, Clone)]
struct DistilledEpisode {
    episode_id: String,
    intent: String,
    steps: Vec<StoredStep>,
}

impl TrajectoryMemory {
    pub(crate) fn new(layout: AppStateLayout) -> Self {
        Self { layout }
    }

    /// Rebuilds one workspace's derived trajectory store from episode and trace logs.
    pub(crate) async fn rebuild_workspace(
        &self,
        query_manager: &QueryManager,
        workspace_name: &WorkspaceName,
    ) -> Result<(), TrajectoryError> {
        let observed = collect_observed_steps(&self.layout, workspace_name).await?;
        let fingerprint_sqls = observed
            .iter()
            .filter(|step| step.status == StoredTraceStatus::Ok && !step.sql.trim().is_empty())
            .map(|step| step.sql.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let fingerprint_values = if fingerprint_sqls.is_empty() {
            Vec::new()
        } else {
            query_manager
                .fingerprint_sql_batch(workspace_name, &fingerprint_sqls)
                .await
                .map_err(|error| query_error(&error))?
        };
        let fingerprints = fingerprint_sqls
            .into_iter()
            .zip(fingerprint_values)
            .collect::<HashMap<_, _>>();
        let mut indexed = Vec::with_capacity(observed.len());
        for step in observed {
            let fingerprint = if step.status == StoredTraceStatus::Ok && !step.sql.trim().is_empty()
            {
                fingerprints.get(&step.sql).cloned()
            } else {
                None
            };
            indexed.push(IndexedStep {
                observed: step,
                step_index: 0,
                fingerprint,
            });
        }
        assign_step_indices(&mut indexed);
        let intents = indexed
            .iter()
            .map(|step| step.observed.intent.clone())
            .collect::<BTreeSet<_>>();
        let mut store = TrajectoryStore::open(&self.layout.trajectory_memory_db(workspace_name))?;
        store.replace_workspace_steps(workspace_name.as_str(), &indexed)?;
        store.rebuild_exact_intent_index(workspace_name.as_str())?;
        for intent in intents {
            let path = store.retrieve_exact(
                workspace_name.as_str(),
                &intent,
                DEFAULT_MIN_QUERY_CONSENSUS,
            )?;
            store.record_shadow_lookup(workspace_name.as_str(), &intent, path.as_ref())?;
        }
        Ok(())
    }

    /// Rebuilds and retrieves the strongest exact-intent path for one workspace.
    pub(crate) async fn rebuild_and_retrieve_exact(
        &self,
        query_manager: &QueryManager,
        workspace_name: &WorkspaceName,
        intent: &str,
        min_query_consensus: u32,
    ) -> Result<Option<GoldenPath>, TrajectoryError> {
        self.rebuild_workspace(query_manager, workspace_name)
            .await?;
        self.retrieve_exact(workspace_name, intent, min_query_consensus)
    }

    fn retrieve_exact(
        &self,
        workspace_name: &WorkspaceName,
        intent: &str,
        min_query_consensus: u32,
    ) -> Result<Option<GoldenPath>, TrajectoryError> {
        let store = TrajectoryStore::open(&self.layout.trajectory_memory_db(workspace_name))?;
        store.retrieve_exact(workspace_name.as_str(), intent, min_query_consensus)
    }
}

async fn collect_observed_steps(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
) -> Result<Vec<ObservedStep>, TrajectoryError> {
    let episode_store = EpisodeStore::new(layout.clone());
    let episodes = episode_store.list_episodes(workspace_name)?;
    let episodes_by_id = episodes
        .into_iter()
        .map(|episode| (episode.id.as_str().to_string(), episode))
        .collect::<HashMap<_, _>>();
    if episodes_by_id.is_empty() {
        return Ok(Vec::new());
    }

    let trace_store = TraceStore::new(layout.local_trace_store_dir());
    let mut offset = 0;
    let mut observed = Vec::new();
    loop {
        let summaries = trace_store.list_traces(TRACE_PAGE_SIZE, offset).await?;
        if summaries.is_empty() {
            break;
        }
        for summary in &summaries {
            let detail = trace_store.get_trace(summary.trace_id.clone()).await?;
            observed.extend(observed_steps_from_trace(
                workspace_name,
                &episodes_by_id,
                &detail,
            )?);
        }
        offset = offset.saturating_add(summaries.len());
        if summaries.len() < TRACE_PAGE_SIZE {
            break;
        }
    }
    observed.sort_by(|left, right| {
        left.episode_id
            .cmp(&right.episode_id)
            .then_with(|| left.start_time_unix_nanos.cmp(&right.start_time_unix_nanos))
            .then_with(|| left.span_id.cmp(&right.span_id))
    });
    Ok(observed)
}

fn observed_steps_from_trace(
    workspace_name: &WorkspaceName,
    episodes_by_id: &HashMap<String, Episode>,
    trace: &TraceDetailRecord,
) -> Result<Vec<ObservedStep>, TrajectoryError> {
    let mut steps = Vec::new();
    for span in &trace.spans {
        if span.name != "coral.query" {
            continue;
        }
        let attributes = parse_attributes(&span.attributes_json)?;
        let Some(episode_id) = attr_string(&attributes, "episode.id") else {
            continue;
        };
        let Some(episode) = episodes_by_id.get(&episode_id) else {
            continue;
        };
        let Some(sql) = attr_string(&attributes, "sql") else {
            continue;
        };
        if attr_string(&attributes, "workspace").as_deref() != Some(workspace_name.as_str()) {
            continue;
        }
        let row_count = attr_u64(&attributes, "row_count");
        steps.push(ObservedStep {
            workspace: workspace_name.as_str().to_string(),
            episode_id,
            intent: episode.intent.clone(),
            parent_episode_id: episode
                .parent_episode_id
                .as_ref()
                .map(|parent| parent.as_str().to_string()),
            trace_id: span.trace_id.clone(),
            span_id: span.span_id.clone(),
            sql,
            status: status_from_span(span, &attributes),
            row_count: row_count.unwrap_or_default(),
            row_count_recorded: row_count.is_some(),
            start_time_unix_nanos: span.start_time_unix_nanos,
            end_time_unix_nanos: span.end_time_unix_nanos,
            attributes_json: span.attributes_json.clone(),
        });
    }
    Ok(steps)
}

fn assign_step_indices(steps: &mut [IndexedStep]) {
    steps.sort_by(|left, right| {
        left.observed
            .episode_id
            .cmp(&right.observed.episode_id)
            .then_with(|| {
                left.observed
                    .start_time_unix_nanos
                    .cmp(&right.observed.start_time_unix_nanos)
            })
            .then_with(|| left.observed.span_id.cmp(&right.observed.span_id))
    });
    let mut current_episode = None::<String>;
    let mut next_index = 0_u32;
    for step in steps {
        if current_episode.as_deref() != Some(step.observed.episode_id.as_str()) {
            current_episode = Some(step.observed.episode_id.clone());
            next_index = 0;
        }
        step.step_index = next_index;
        next_index = next_index.saturating_add(1);
    }
}

struct TrajectoryStore {
    conn: Connection,
}

impl TrajectoryStore {
    fn open(path: &Path) -> Result<Self, TrajectoryError> {
        if let Some(parent) = path.parent() {
            storage_fs::ensure_private_dir(parent)?;
        }
        let conn = Connection::open(path)?;
        storage_fs::set_file_permissions_private(path)?;
        let store = Self { conn };
        store.initialize()?;
        Ok(store)
    }

    fn initialize(&self) -> Result<(), TrajectoryError> {
        self.conn.execute_batch(
            r"
            PRAGMA foreign_keys = ON;
            CREATE TABLE IF NOT EXISTS trajectory_steps (
                workspace TEXT NOT NULL,
                episode_id TEXT NOT NULL,
                intent TEXT NOT NULL,
                parent_episode_id TEXT,
                trace_id TEXT NOT NULL,
                span_id TEXT NOT NULL,
                step_index INTEGER NOT NULL,
                sql TEXT NOT NULL,
                status TEXT NOT NULL,
                row_count INTEGER NOT NULL,
                row_count_recorded INTEGER NOT NULL,
                start_time_unix_nanos INTEGER NOT NULL,
                end_time_unix_nanos INTEGER NOT NULL,
                attributes_json TEXT NOT NULL,
                relations_json TEXT NOT NULL,
                shape_hash TEXT,
                exact_key TEXT,
                PRIMARY KEY (workspace, episode_id, trace_id, span_id)
            );
            CREATE INDEX IF NOT EXISTS trajectory_steps_workspace_intent
                ON trajectory_steps(workspace, intent, episode_id, step_index);
            CREATE TABLE IF NOT EXISTS golden_paths (
                workspace TEXT NOT NULL,
                intent TEXT NOT NULL,
                path_key TEXT NOT NULL,
                steps_json TEXT NOT NULL,
                relations_json TEXT NOT NULL,
                query_consensus INTEGER NOT NULL,
                path_consensus INTEGER NOT NULL,
                episode_count INTEGER NOT NULL,
                step_count INTEGER NOT NULL,
                selected_at_unix_nanos INTEGER NOT NULL,
                PRIMARY KEY (workspace, intent, path_key)
            );
            CREATE INDEX IF NOT EXISTS golden_paths_lookup
                ON golden_paths(workspace, intent, query_consensus DESC, path_consensus DESC, step_count ASC);
            CREATE TABLE IF NOT EXISTS trajectory_shadow_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workspace TEXT NOT NULL,
                intent TEXT NOT NULL,
                matched INTEGER NOT NULL,
                path_key TEXT,
                query_consensus INTEGER,
                path_consensus INTEGER,
                step_count INTEGER,
                created_at_unix_nanos INTEGER NOT NULL
            );
            ",
        )?;
        Ok(())
    }

    fn replace_workspace_steps(
        &mut self,
        workspace: &str,
        steps: &[IndexedStep],
    ) -> Result<(), TrajectoryError> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "DELETE FROM trajectory_steps WHERE workspace = ?1",
            params![workspace],
        )?;
        {
            let mut insert = tx.prepare(
                r"
                INSERT INTO trajectory_steps (
                    workspace, episode_id, intent, parent_episode_id, trace_id, span_id,
                    step_index, sql, status, row_count, row_count_recorded,
                    start_time_unix_nanos, end_time_unix_nanos, attributes_json,
                    relations_json, shape_hash, exact_key
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)
                ",
            )?;
            for step in steps {
                let fingerprint = step.fingerprint.as_ref();
                let relations: &[String] =
                    fingerprint.map_or_else(|| &[], QueryFingerprint::relations);
                let relations_json = serde_json::to_string(relations)?;
                insert.execute(params![
                    step.observed.workspace,
                    step.observed.episode_id,
                    step.observed.intent,
                    step.observed.parent_episode_id,
                    step.observed.trace_id,
                    step.observed.span_id,
                    i64::from(step.step_index),
                    step.observed.sql,
                    stored_status(step.observed.status),
                    i64::try_from(step.observed.row_count).unwrap_or(i64::MAX),
                    bool_to_i64(step.observed.row_count_recorded),
                    step.observed.start_time_unix_nanos,
                    step.observed.end_time_unix_nanos,
                    step.observed.attributes_json,
                    relations_json,
                    fingerprint.map(QueryFingerprint::shape_hash),
                    fingerprint.map(QueryFingerprint::exact_key),
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn rebuild_exact_intent_index(&mut self, workspace: &str) -> Result<(), TrajectoryError> {
        let steps = self.workspace_steps(workspace)?;
        let golden_paths = build_golden_paths(workspace, &steps);
        let tx = self.conn.transaction()?;
        tx.execute(
            "DELETE FROM golden_paths WHERE workspace = ?1",
            params![workspace],
        )?;
        {
            let mut insert = tx.prepare(
                r"
                INSERT INTO golden_paths (
                    workspace, intent, path_key, steps_json, relations_json,
                    query_consensus, path_consensus, episode_count, step_count,
                    selected_at_unix_nanos
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                ",
            )?;
            let now = unix_nanos_now();
            for path in &golden_paths {
                insert.execute(params![
                    path.workspace,
                    path.intent,
                    path.path_key,
                    serde_json::to_string(&path.steps)?,
                    serde_json::to_string(&path.relations)?,
                    i64::from(path.query_consensus),
                    i64::from(path.path_consensus),
                    i64::from(path.episode_count),
                    i64::try_from(path.steps.len()).unwrap_or(i64::MAX),
                    now,
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn retrieve_exact(
        &self,
        workspace: &str,
        intent: &str,
        min_query_consensus: u32,
    ) -> Result<Option<GoldenPath>, TrajectoryError> {
        self.conn
            .query_row(
                r"
                SELECT workspace, intent, path_key, steps_json, relations_json,
                       query_consensus, path_consensus, episode_count
                FROM golden_paths
                WHERE workspace = ?1
                  AND intent = ?2
                  AND query_consensus >= ?3
                  AND path_consensus >= ?3
                ORDER BY query_consensus DESC, path_consensus DESC, step_count ASC, path_key ASC
                LIMIT 1
                ",
                params![workspace, intent, i64::from(min_query_consensus)],
                |row| {
                    let steps_json: String = row.get(3)?;
                    let relations_json: String = row.get(4)?;
                    Ok(GoldenPath {
                        workspace: row.get(0)?,
                        intent: row.get(1)?,
                        path_key: row.get(2)?,
                        steps: serde_json::from_str(&steps_json).map_err(json_sql_error)?,
                        relations: serde_json::from_str(&relations_json).map_err(json_sql_error)?,
                        query_consensus: i64_to_u32(row.get(5)?),
                        path_consensus: i64_to_u32(row.get(6)?),
                        episode_count: i64_to_u32(row.get(7)?),
                    })
                },
            )
            .optional()
            .map_err(TrajectoryError::from)
    }

    fn record_shadow_lookup(
        &mut self,
        workspace: &str,
        intent: &str,
        path: Option<&GoldenPath>,
    ) -> Result<(), TrajectoryError> {
        self.conn.execute(
            r"
            INSERT INTO trajectory_shadow_events (
                workspace, intent, matched, path_key, query_consensus,
                path_consensus, step_count, created_at_unix_nanos
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ",
            params![
                workspace,
                intent,
                bool_to_i64(path.is_some()),
                path.map(|path| path.path_key.as_str()),
                path.map(|path| i64::from(path.query_consensus)),
                path.map(|path| i64::from(path.path_consensus)),
                path.map(|path| i64::try_from(path.steps.len()).unwrap_or(i64::MAX)),
                unix_nanos_now(),
            ],
        )?;
        Ok(())
    }

    fn workspace_steps(&self, workspace: &str) -> Result<Vec<StoredStep>, TrajectoryError> {
        let mut statement = self.conn.prepare(
            r"
            SELECT episode_id, intent, step_index, sql, status, relations_json, shape_hash, exact_key
            FROM trajectory_steps
            WHERE workspace = ?1
            ORDER BY intent ASC, episode_id ASC, step_index ASC
            ",
        )?;
        let rows = statement.query_map(params![workspace], |row| {
            let relations_json: String = row.get(5)?;
            Ok(StoredStep {
                episode_id: row.get(0)?,
                intent: row.get(1)?,
                step_index: i64_to_u32(row.get(2)?),
                sql: row.get(3)?,
                status: stored_status_from_str(&row.get::<_, String>(4)?),
                relations: serde_json::from_str(&relations_json).map_err(json_sql_error)?,
                shape_hash: row.get(6)?,
                exact_key: row.get(7)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(TrajectoryError::from)
    }
}

fn build_golden_paths(workspace: &str, steps: &[StoredStep]) -> Vec<GoldenPath> {
    let episodes = distill_episodes(steps);
    let mut by_intent: BTreeMap<String, Vec<DistilledEpisode>> = BTreeMap::new();
    for episode in episodes {
        by_intent
            .entry(episode.intent.clone())
            .or_default()
            .push(episode);
    }

    let mut golden_paths = Vec::new();
    for (intent, episodes) in by_intent {
        let episode_count = usize_to_u32(episodes.len());
        let query_counts = query_consensus_counts(&episodes);
        let path_counts = path_consensus_counts(&episodes);
        let mut representatives = BTreeMap::<String, &DistilledEpisode>::new();
        for episode in &episodes {
            representatives
                .entry(path_key(&episode.steps))
                .and_modify(|current| {
                    if episode.steps.len() < current.steps.len()
                        || (episode.steps.len() == current.steps.len()
                            && episode.episode_id < current.episode_id)
                    {
                        *current = episode;
                    }
                })
                .or_insert(episode);
        }
        for (path_key, episode) in representatives {
            let query_consensus = episode
                .steps
                .iter()
                .filter_map(|step| step.shape_hash.as_deref())
                .filter_map(|shape_hash| query_counts.get(shape_hash))
                .copied()
                .max()
                .unwrap_or_default();
            let path_consensus = path_counts.get(&path_key).copied().unwrap_or_default();
            golden_paths.push(GoldenPath {
                workspace: workspace.to_string(),
                intent: intent.clone(),
                path_key,
                steps: episode.steps.iter().map(|step| step.sql.clone()).collect(),
                relations: relations_for_steps(&episode.steps),
                query_consensus,
                path_consensus,
                episode_count,
            });
        }
    }
    golden_paths.sort_by(|left, right| {
        left.workspace
            .cmp(&right.workspace)
            .then_with(|| left.intent.cmp(&right.intent))
            .then_with(|| right.query_consensus.cmp(&left.query_consensus))
            .then_with(|| right.path_consensus.cmp(&left.path_consensus))
            .then_with(|| left.steps.len().cmp(&right.steps.len()))
            .then_with(|| left.path_key.cmp(&right.path_key))
    });
    golden_paths
}

fn distill_episodes(steps: &[StoredStep]) -> Vec<DistilledEpisode> {
    let mut grouped = BTreeMap::<(String, String), Vec<StoredStep>>::new();
    for step in steps {
        grouped
            .entry((step.intent.clone(), step.episode_id.clone()))
            .or_default()
            .push(step.clone());
    }

    let mut episodes = Vec::new();
    for ((intent, episode_id), mut steps) in grouped {
        steps.sort_by_key(|step| step.step_index);
        let mut seen_exact_keys = HashSet::new();
        let distilled = steps
            .into_iter()
            .filter(|step| step.status == StoredTraceStatus::Ok)
            .filter(|step| step.exact_key.is_some() && step.shape_hash.is_some())
            .filter(|step| match &step.exact_key {
                Some(exact_key) => seen_exact_keys.insert(exact_key.clone()),
                None => false,
            })
            .collect::<Vec<_>>();
        if !distilled.is_empty() {
            episodes.push(DistilledEpisode {
                episode_id,
                intent,
                steps: distilled,
            });
        }
    }
    episodes
}

fn query_consensus_counts(episodes: &[DistilledEpisode]) -> BTreeMap<String, u32> {
    let mut counts = BTreeMap::new();
    for episode in episodes {
        let mut seen = HashSet::new();
        for step in &episode.steps {
            if let Some(shape_hash) = &step.shape_hash
                && seen.insert(shape_hash)
            {
                *counts.entry(shape_hash.clone()).or_insert(0) += 1;
            }
        }
    }
    counts
}

fn path_consensus_counts(episodes: &[DistilledEpisode]) -> BTreeMap<String, u32> {
    let mut counts = BTreeMap::new();
    for episode in episodes {
        *counts.entry(path_key(&episode.steps)).or_insert(0) += 1;
    }
    counts
}

fn path_key(steps: &[StoredStep]) -> String {
    let mut hasher = Sha256::new();
    for step in steps {
        if let Some(exact_key) = &step.exact_key {
            hasher.update(exact_key.as_bytes());
            hasher.update([0]);
        }
    }
    format!("{:x}", hasher.finalize())
}

fn relations_for_steps(steps: &[StoredStep]) -> Vec<String> {
    let mut relations = BTreeSet::new();
    for step in steps {
        relations.extend(step.relations.iter().cloned());
    }
    relations.into_iter().collect()
}

fn parse_attributes(attributes_json: &str) -> Result<JsonValue, TrajectoryError> {
    Ok(serde_json::from_str(attributes_json)?)
}

fn attr_string(attributes: &JsonValue, key: &str) -> Option<String> {
    match attributes.get(key)? {
        JsonValue::String(value) => Some(value.clone()),
        JsonValue::Number(value) => Some(value.to_string()),
        JsonValue::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

fn attr_u64(attributes: &JsonValue, key: &str) -> Option<u64> {
    match attributes.get(key)? {
        JsonValue::Number(value) => value
            .as_u64()
            .or_else(|| value.as_i64().and_then(|value| u64::try_from(value).ok())),
        JsonValue::String(value) => value.parse().ok(),
        _ => None,
    }
}

fn status_from_span(span: &TraceSpanRecord, attributes: &JsonValue) -> StoredTraceStatus {
    match attr_string(attributes, "status").as_deref() {
        Some("ok") => StoredTraceStatus::Ok,
        Some("error") => StoredTraceStatus::Error,
        _ => span.status,
    }
}

fn stored_status(status: StoredTraceStatus) -> &'static str {
    match status {
        StoredTraceStatus::Unspecified => "unspecified",
        StoredTraceStatus::Ok => "ok",
        StoredTraceStatus::Error => "error",
    }
}

fn stored_status_from_str(status: &str) -> StoredTraceStatus {
    match status {
        "ok" => StoredTraceStatus::Ok,
        "error" => StoredTraceStatus::Error,
        _ => StoredTraceStatus::Unspecified,
    }
}

fn bool_to_i64(value: bool) -> i64 {
    i64::from(value)
}

fn i64_to_u32(value: i64) -> u32 {
    u32::try_from(value).unwrap_or_default()
}

fn usize_to_u32(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn json_sql_error(error: serde_json::Error) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(error))
}

fn unix_nanos_now() -> i64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    i64::try_from(nanos).unwrap_or(i64::MAX)
}

fn query_error(error: &QueryManagerError) -> TrajectoryError {
    TrajectoryError::Query(format!("{error:?}"))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::json;
    use tempfile::TempDir;

    use super::{
        IndexedStep, ObservedStep, StoredTraceStatus, TrajectoryStore, assign_step_indices,
        build_golden_paths,
    };
    use crate::episode::EpisodeId;
    use crate::episode::store::{Episode, EpisodeStore, now_unix_nanos};
    use crate::state::AppStateLayout;
    use crate::telemetry::local_store::TraceSpanRecord;
    use crate::workspaces::WorkspaceName;
    use coral_engine::QueryFingerprint;

    fn layout(temp: &TempDir) -> AppStateLayout {
        AppStateLayout::discover(Some(temp.path().join("state"))).expect("layout")
    }

    fn workspace_name(name: &str) -> WorkspaceName {
        WorkspaceName::parse(name).expect("workspace")
    }

    fn observed(
        workspace: &WorkspaceName,
        episode_id: &str,
        intent: &str,
        span_id: &str,
        sql: &str,
        start: i64,
        status: StoredTraceStatus,
    ) -> IndexedStep {
        IndexedStep {
            observed: ObservedStep {
                workspace: workspace.as_str().to_string(),
                episode_id: episode_id.to_string(),
                intent: intent.to_string(),
                parent_episode_id: None,
                trace_id: format!("trace-{episode_id}"),
                span_id: span_id.to_string(),
                sql: sql.to_string(),
                status,
                row_count: 1,
                row_count_recorded: true,
                start_time_unix_nanos: start,
                end_time_unix_nanos: start + 1,
                attributes_json: "{}".to_string(),
            },
            step_index: 0,
            fingerprint: (status == StoredTraceStatus::Ok).then(|| {
                QueryFingerprint::new(
                    vec!["notion.search_objects".to_string()],
                    format!("shape-{sql}"),
                    format!("exact-{sql}"),
                )
            }),
        }
    }

    #[test]
    fn assign_step_indices_orders_steps_within_each_episode() {
        let workspace = workspace_name("acme");
        let mut steps = vec![
            observed(
                &workspace,
                "ep_b",
                "intent",
                "span_b2",
                "SELECT 2",
                20,
                StoredTraceStatus::Ok,
            ),
            observed(
                &workspace,
                "ep_a",
                "intent",
                "span_a1",
                "SELECT 1",
                10,
                StoredTraceStatus::Ok,
            ),
            observed(
                &workspace,
                "ep_b",
                "intent",
                "span_b1",
                "SELECT 1",
                10,
                StoredTraceStatus::Ok,
            ),
        ];

        assign_step_indices(&mut steps);

        let by_span = steps
            .iter()
            .map(|step| (step.observed.span_id.as_str(), step.step_index))
            .collect::<Vec<_>>();
        assert_eq!(by_span, [("span_a1", 0), ("span_b1", 0), ("span_b2", 1)]);
    }

    #[test]
    fn exact_intent_index_is_cluster_scoped_and_thresholded() {
        let temp = TempDir::new().expect("temp dir");
        let workspace = workspace_name("acme");
        let mut store =
            TrajectoryStore::open(&layout(&temp).trajectory_memory_db(&workspace)).expect("store");
        let mut steps = vec![
            observed(
                &workspace,
                "ep_1",
                "find onboarding",
                "a",
                "SELECT onboarding",
                1,
                StoredTraceStatus::Ok,
            ),
            observed(
                &workspace,
                "ep_2",
                "find onboarding",
                "b",
                "SELECT onboarding",
                2,
                StoredTraceStatus::Ok,
            ),
            observed(
                &workspace,
                "ep_3",
                "find payroll",
                "c",
                "SELECT onboarding",
                3,
                StoredTraceStatus::Ok,
            ),
        ];
        assign_step_indices(&mut steps);
        store
            .replace_workspace_steps(workspace.as_str(), &steps)
            .expect("replace steps");
        store
            .rebuild_exact_intent_index(workspace.as_str())
            .expect("rebuild index");

        let hit = store
            .retrieve_exact(workspace.as_str(), "find onboarding", 2)
            .expect("retrieve")
            .expect("hit");
        assert_eq!(hit.query_consensus, 2);
        assert_eq!(hit.steps, ["SELECT onboarding"]);
        assert!(
            store
                .retrieve_exact(workspace.as_str(), "find payroll", 2)
                .expect("retrieve")
                .is_none(),
            "same query in another intent must not borrow consensus"
        );
    }

    #[test]
    fn exact_intent_retrieval_requires_path_consensus() {
        let temp = TempDir::new().expect("temp dir");
        let workspace = workspace_name("acme");
        let mut store =
            TrajectoryStore::open(&layout(&temp).trajectory_memory_db(&workspace)).expect("store");
        let mut steps = vec![
            observed(
                &workspace,
                "ep_1",
                "find onboarding",
                "a",
                "SELECT shared",
                1,
                StoredTraceStatus::Ok,
            ),
            observed(
                &workspace,
                "ep_1",
                "find onboarding",
                "b",
                "SELECT branch one",
                2,
                StoredTraceStatus::Ok,
            ),
            observed(
                &workspace,
                "ep_2",
                "find onboarding",
                "c",
                "SELECT shared",
                3,
                StoredTraceStatus::Ok,
            ),
            observed(
                &workspace,
                "ep_2",
                "find onboarding",
                "d",
                "SELECT branch two",
                4,
                StoredTraceStatus::Ok,
            ),
        ];
        assign_step_indices(&mut steps);
        store
            .replace_workspace_steps(workspace.as_str(), &steps)
            .expect("replace steps");
        store
            .rebuild_exact_intent_index(workspace.as_str())
            .expect("rebuild index");

        let hit = store
            .retrieve_exact(workspace.as_str(), "find onboarding", 2)
            .expect("retrieve");
        assert!(
            hit.is_none(),
            "shared query consensus without exact path consensus is not enough"
        );
    }

    #[test]
    fn distillation_drops_errors_and_duplicate_exact_steps() {
        let workspace = workspace_name("acme");
        let mut steps = vec![
            observed(
                &workspace,
                "ep_1",
                "intent",
                "a",
                "SELECT kept",
                1,
                StoredTraceStatus::Ok,
            ),
            observed(
                &workspace,
                "ep_1",
                "intent",
                "b",
                "SELECT kept",
                2,
                StoredTraceStatus::Ok,
            ),
            observed(
                &workspace,
                "ep_1",
                "intent",
                "c",
                "SELECT errored",
                3,
                StoredTraceStatus::Error,
            ),
        ];
        assign_step_indices(&mut steps);
        let stored = steps
            .into_iter()
            .map(|step| super::StoredStep {
                episode_id: step.observed.episode_id,
                intent: step.observed.intent,
                step_index: step.step_index,
                sql: step.observed.sql,
                status: step.observed.status,
                relations: step
                    .fingerprint
                    .as_ref()
                    .map(|fingerprint| fingerprint.relations().to_vec())
                    .unwrap_or_default(),
                shape_hash: step
                    .fingerprint
                    .as_ref()
                    .map(|fingerprint| fingerprint.shape_hash().to_string()),
                exact_key: step
                    .fingerprint
                    .as_ref()
                    .map(|fingerprint| fingerprint.exact_key().to_string()),
            })
            .collect::<Vec<_>>();

        let paths = build_golden_paths(workspace.as_str(), &stored);

        assert_eq!(paths.len(), 1);
        assert_eq!(paths.first().expect("one path").steps, ["SELECT kept"]);
    }

    #[tokio::test]
    async fn observed_steps_join_episode_intent_to_query_spans() {
        let temp = TempDir::new().expect("temp dir");
        let layout = layout(&temp);
        let workspace = workspace_name("acme");
        let other_workspace = workspace_name("other");
        let store = EpisodeStore::new(layout.clone());
        store
            .open_episode(&Episode {
                id: EpisodeId::parse("ep_1").expect("episode id"),
                workspace: workspace.clone(),
                intent: "find onboarding".to_string(),
                parent_episode_id: None,
                created_at_unix_nanos: now_unix_nanos(),
            })
            .expect("open episode");
        store
            .open_episode(&Episode {
                id: EpisodeId::parse("ep_1").expect("episode id"),
                workspace: other_workspace,
                intent: "other intent".to_string(),
                parent_episode_id: None,
                created_at_unix_nanos: now_unix_nanos(),
            })
            .expect("open other episode");
        let trace_dir = layout.local_trace_store_dir();
        fs::create_dir_all(&trace_dir).expect("trace dir");
        let span = trace_span(
            "trace_1",
            "span_1",
            &json!({
                "workspace": "acme",
                "episode.id": "ep_1",
                "sql": "SELECT id FROM notion.search_objects(query => 'onboarding')",
                "status": "ok",
                "row_count": 1
            }),
        );
        let untagged = trace_span(
            "trace_2",
            "span_2",
            &json!({
                "workspace": "acme",
                "sql": "SELECT 2",
                "status": "ok"
            }),
        );
        let lines = [span, untagged]
            .into_iter()
            .map(|span| serde_json::to_string(&span).expect("span json"))
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(trace_dir.join("spans-test.jsonl"), format!("{lines}\n")).expect("write spans");

        let observed = super::collect_observed_steps(&layout, &workspace)
            .await
            .expect("collect observed");

        assert_eq!(observed.len(), 1);
        let observed = observed.first().expect("one observed step");
        assert_eq!(observed.intent, "find onboarding");
        assert_eq!(observed.episode_id, "ep_1");
        assert_eq!(observed.row_count, 1);
        assert!(observed.row_count_recorded);
    }

    fn trace_span(
        trace_id: &str,
        span_id: &str,
        attributes: &serde_json::Value,
    ) -> TraceSpanRecord {
        TraceSpanRecord {
            trace_id: trace_id.to_string(),
            span_id: span_id.to_string(),
            parent_span_id: None,
            parent_span_is_remote: false,
            name: "coral.query".to_string(),
            kind: "internal".to_string(),
            status: StoredTraceStatus::Ok,
            status_message: None,
            start_time_unix_nanos: 1,
            end_time_unix_nanos: 2,
            duration_nanos: 1,
            attributes_json: attributes.to_string(),
            events_json: "[]".to_string(),
            links_json: "[]".to_string(),
            resource_json: "{}".to_string(),
            scope_name: "test".to_string(),
            scope_version: None,
            scope_schema_url: None,
            scope_attributes_json: "{}".to_string(),
            trace_flags: 0,
            trace_state: String::new(),
            is_remote: false,
        }
    }
}
