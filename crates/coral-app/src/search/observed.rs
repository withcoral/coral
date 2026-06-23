//! Passive observed-value indexing for source-scan observations.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::thread;
use std::time::Duration as StdDuration;
use std::time::Instant as StdInstant;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use chrono::{Duration, SecondsFormat, Utc};
use coral_engine::{
    QuerySource, RuntimeSourceComponent, SourceObservationPublisher, SourceObservationScope,
    SourceObservationSurfaceKind, SourceScanObservation,
};
use coral_spec::{ColumnSpec, ManifestInputKind};
use serde_json::{Map, Value};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::credentials::{OAUTH_INTERNAL_KEY_PREFIX, is_internal_material_key};
use crate::search::index::{
    ObservedValueRecord, ObservedValueSuggestedOperator, ObservedValueSurfaceKind,
    SearchIndexError, SearchIndexStore,
};
use crate::sources::SourceName;
use crate::state::AppStateLayout;
use crate::workspaces::WorkspaceName;

const CATALOG_FINGERPRINT_FILE_NAME: &str = "catalog.sha256";
const DEFAULT_OBSERVED_QUEUE_FOREGROUND_DRAIN_MS: u64 = 1_000;
const DEFAULT_OBSERVED_MAX_STORAGE_MB: u64 = 256;
const DEFAULT_OBSERVED_COLLECTION_MAX_CANDIDATES: usize = 10_000;
const DEFAULT_OBSERVED_COLLECTION_MAX_CANDIDATE_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_OBSERVED_COLLECTION_MAX_JSON_DEPTH: usize = 8;
const DEFAULT_OBSERVED_VALUE_STALE_AFTER_DAYS: u64 = 90;
const DEFAULT_OBSERVED_VALUES_ENABLED: bool = true;
const OBSERVED_SOURCE_SCAN_QUEUE_CAPACITY: usize = 128;
const OBSERVED_SOURCE_SCAN_INDEX_ATTEMPTS: usize = 8;
const OBSERVED_SOURCE_SCAN_INDEX_RETRY_MS: u64 = 25;
const BYTES_PER_MIB: u64 = 1024 * 1024;
const SOURCE_GENERATION_DIR_NAME: &str = "source-generations";

#[derive(Debug, Default, serde::Deserialize)]
struct ObservedSearchConfigFile {
    #[serde(default)]
    search: ObservedSearchConfig,
}

#[derive(Debug, Default, serde::Deserialize)]
struct ObservedSearchConfig {
    #[serde(rename = "observed_values_enabled")]
    values_enabled: Option<bool>,
    #[serde(default)]
    #[serde(rename = "observed_value_excluded_sources")]
    excluded_sources: Vec<String>,
    #[serde(default)]
    #[serde(rename = "observed_value_excluded_surfaces")]
    excluded_surfaces: Vec<String>,
    #[serde(default)]
    #[serde(rename = "observed_value_excluded_columns")]
    excluded_columns: Vec<String>,
    #[serde(rename = "observed_queue_foreground_drain_ms")]
    queue_foreground_drain_ms: Option<u64>,
    #[serde(rename = "observed_max_storage_mb")]
    storage_mb: Option<u64>,
    #[serde(rename = "observed_collection_max_candidates")]
    collection_candidates: Option<usize>,
    #[serde(rename = "observed_collection_max_candidate_bytes")]
    collection_candidate_bytes: Option<usize>,
    #[serde(rename = "observed_collection_max_json_depth")]
    collection_json_depth: Option<usize>,
    #[serde(rename = "observed_value_stale_after_days")]
    value_stale_after_days: Option<u64>,
}

/// Writes source-scan values into search index storage.
struct ObservedValueIndexer {
    layout: AppStateLayout,
    workspace_name: WorkspaceName,
    surfaces: Vec<ObservedSurface>,
    source_generations: BTreeMap<String, Option<String>>,
    collection_budget: ObservedCollectionBudget,
    storage_budget_bytes: u64,
}

#[derive(Clone)]
pub(crate) struct ObservedSourceScanIndexer {
    layout: AppStateLayout,
    sender: SyncSender<ObservedSourceScanWorkerMessage>,
    dropped_observations: Arc<AtomicU64>,
}

impl ObservedValueIndexer {
    #[cfg(test)]
    fn new(
        layout: AppStateLayout,
        workspace_name: WorkspaceName,
        selected_sources: &[QuerySource],
    ) -> Self {
        let source_scope_ids =
            observed_source_scopes_from_query_sources(selected_sources, &BTreeMap::new());
        Self::new_with_source_scopes(layout, workspace_name, selected_sources, &source_scope_ids)
    }

    fn new_with_source_scopes(
        layout: AppStateLayout,
        workspace_name: WorkspaceName,
        selected_sources: &[QuerySource],
        source_scope_ids: &BTreeMap<String, String>,
    ) -> Self {
        let config = observed_search_config_or_default(&layout);
        let policy = ObservedValuePolicy::from_config(&config.search);
        let surfaces = observed_surfaces(selected_sources, source_scope_ids, &policy);
        let source_generations = observed_source_generations(&layout, &workspace_name, &surfaces);
        Self {
            layout,
            workspace_name,
            surfaces,
            source_generations,
            collection_budget: observed_collection_budget_from_config(&config.search),
            storage_budget_bytes: observed_storage_budget_bytes_from_config(&config.search),
        }
    }

    fn index_source_scan_observation_inner(
        &self,
        observation: &OwnedSourceScanObservation,
    ) -> Result<(), ObservedValueIndexError> {
        let Some(surface) = self.source_scan_surface(observation) else {
            tracing::debug!(
                workspace = %self.workspace_name,
                source = %observation.source_name,
                surface = %observation.surface_name,
                "skipping source-scan observed-value indexing because the observed surface is not selected"
            );
            return Ok(());
        };
        let schema = observation.batch.schema();
        let provenance = schema
            .fields()
            .iter()
            .map(|field| direct_field_provenance(surface, field.name(), field.name()))
            .collect::<Vec<_>>();
        if provenance.iter().all(Option::is_none) {
            return Ok(());
        }
        if self.source_generations_changed(&provenance)? {
            tracing::debug!(
                workspace = %self.workspace_name,
                source = %observation.source_name,
                "skipping source-scan observed-value indexing because a source changed while the query was running"
            );
            return Ok(());
        }

        let collection = observed_records_from_batches(
            schema.as_ref(),
            std::slice::from_ref(&observation.batch),
            &provenance,
            self.collection_budget,
        )?;
        if collection.budget_exhausted {
            tracing::debug!(
                workspace = %self.workspace_name,
                source = %observation.source_name,
                surface = %observation.surface_name,
                scope = ?observation.observation_scope,
                accepted_candidates = collection.accepted_candidates,
                accepted_candidate_bytes = collection.accepted_candidate_bytes,
                skipped_oversize_candidates = collection.skipped_oversize_candidates,
                "source-scan observed-value collection budget exhausted; enqueueing bounded chunks"
            );
        }
        self.enqueue_record_collection(collection)
    }

    fn source_scan_surface(
        &self,
        observation: &OwnedSourceScanObservation,
    ) -> Option<&ObservedSurface> {
        let surface_kind = observed_value_surface_kind(observation.surface_kind);
        let matches = self
            .surfaces
            .iter()
            .filter(|surface| surface.surface_kind == surface_kind)
            .filter(|surface| same_identifier(&surface.source_name, &observation.source_name))
            .filter(|surface| same_identifier(&surface.surface_name, &observation.surface_name))
            .collect::<Vec<_>>();
        let [surface] = matches.as_slice() else {
            return None;
        };
        Some(*surface)
    }

    fn enqueue_record_collection(
        &self,
        collection: ObservedRecordCollection,
    ) -> Result<(), ObservedValueIndexError> {
        if collection.is_empty() {
            return Ok(());
        }

        if !SearchIndexStore::workspace_index_is_usable(&self.layout, &self.workspace_name) {
            clear_catalog_fingerprint(&self.layout, &self.workspace_name)?;
        }
        let store = SearchIndexStore::open_workspace(&self.layout, &self.workspace_name)?;
        let enforcement = store.enforce_observed_storage_budget(self.storage_budget_bytes)?;
        if enforcement.budget_exceeded {
            tracing::warn!(
                workspace = %self.workspace_name,
                storage_bytes = enforcement.storage_bytes,
                max_storage_bytes = self.storage_budget_bytes,
                "pausing observed-value enqueue because storage budget is exhausted"
            );
            return Ok(());
        }
        for records in collection.record_chunks {
            let enforcement = store.enforce_observed_storage_budget(self.storage_budget_bytes)?;
            if enforcement.budget_exceeded {
                tracing::warn!(
                    workspace = %self.workspace_name,
                    storage_bytes = enforcement.storage_bytes,
                    max_storage_bytes = self.storage_budget_bytes,
                    "pausing observed-value enqueue because storage budget is exhausted"
                );
                break;
            }
            store.enqueue_observed_values(&self.workspace_name, records)?;
        }
        store.enforce_observed_storage_budget(self.storage_budget_bytes)?;
        Ok(())
    }

    fn source_generations_changed(
        &self,
        provenance: &[Option<FieldProvenance>],
    ) -> Result<bool, ObservedValueIndexError> {
        let source_names = provenance
            .iter()
            .filter_map(|provenance| provenance.as_ref())
            .map(|provenance| provenance.source_name.as_str())
            .collect::<BTreeSet<_>>();
        for source_name in source_names {
            let generation =
                read_observed_source_generation(&self.layout, &self.workspace_name, source_name)?;
            let expected = self
                .source_generations
                .get(source_name)
                .cloned()
                .unwrap_or(None);
            if generation != expected {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

impl ObservedSourceScanIndexer {
    pub(crate) fn spawn(layout: AppStateLayout) -> Self {
        let (sender, receiver) = std::sync::mpsc::sync_channel(OBSERVED_SOURCE_SCAN_QUEUE_CAPACITY);
        let worker = ObservedSourceScanWorker { receiver };
        if let Err(error) = thread::Builder::new()
            .name("coral-observed-source-scan-indexer".to_string())
            .spawn(move || worker.run())
        {
            tracing::warn!(
                error = %error,
                "failed to spawn observed source-scan indexer; source observations will be dropped"
            );
        }
        Self {
            layout,
            sender,
            dropped_observations: Arc::new(AtomicU64::new(0)),
        }
    }

    #[cfg(test)]
    fn publisher(
        &self,
        workspace_name: WorkspaceName,
        selected_sources: &[QuerySource],
    ) -> Arc<dyn SourceObservationPublisher> {
        let source_scope_ids =
            observed_source_scopes_from_query_sources(selected_sources, &BTreeMap::new());
        self.publisher_with_source_scopes(workspace_name, selected_sources, &source_scope_ids)
    }

    pub(crate) fn publisher_with_source_scopes(
        &self,
        workspace_name: WorkspaceName,
        selected_sources: &[QuerySource],
        source_scope_ids: &BTreeMap<String, String>,
    ) -> Arc<dyn SourceObservationPublisher> {
        let indexer = Arc::new(ObservedValueIndexer::new_with_source_scopes(
            self.layout.clone(),
            workspace_name,
            selected_sources,
            source_scope_ids,
        ));
        Arc::new(ObservedSourceScanPublisher {
            sender: self.sender.clone(),
            indexer,
            dropped_observations: Arc::clone(&self.dropped_observations),
        })
    }

    pub(crate) fn drain_for(&self, budget: StdDuration) -> bool {
        if budget.is_zero() {
            return false;
        }
        let started_at = StdInstant::now();
        let (ack_sender, ack_receiver) = std::sync::mpsc::sync_channel(1);
        let mut message = ObservedSourceScanWorkerMessage::Flush(ack_sender);
        loop {
            match self.sender.try_send(message) {
                Ok(()) => break,
                Err(TrySendError::Full(returned_message)) => {
                    let elapsed = started_at.elapsed();
                    if elapsed >= budget {
                        tracing::debug!(
                            "source-scan observed-value worker did not accept drain marker before foreground budget expired"
                        );
                        return false;
                    }
                    message = returned_message;
                    thread::sleep(
                        budget
                            .saturating_sub(elapsed)
                            .min(StdDuration::from_millis(5)),
                    );
                }
                Err(TrySendError::Disconnected(_message)) => {
                    tracing::debug!(
                        "source-scan observed-value worker stopped before foreground drain"
                    );
                    return false;
                }
            }
        }

        match ack_receiver.recv_timeout(budget.saturating_sub(started_at.elapsed())) {
            Ok(()) => true,
            Err(RecvTimeoutError::Timeout) => {
                tracing::debug!(
                    "source-scan observed-value worker did not finish foreground drain before budget expired"
                );
                false
            }
            Err(RecvTimeoutError::Disconnected) => {
                tracing::debug!(
                    "source-scan observed-value worker stopped during foreground drain"
                );
                false
            }
        }
    }
}

struct ObservedSourceScanPublisher {
    sender: SyncSender<ObservedSourceScanWorkerMessage>,
    indexer: Arc<ObservedValueIndexer>,
    dropped_observations: Arc<AtomicU64>,
}

impl SourceObservationPublisher for ObservedSourceScanPublisher {
    fn publish_source_scan(&self, observation: SourceScanObservation<'_>) {
        let job = ObservedSourceScanJob {
            indexer: Arc::clone(&self.indexer),
            observation: OwnedSourceScanObservation {
                source_name: observation.source_name.to_string(),
                surface_kind: observation.surface_kind,
                surface_name: observation.surface_name.to_string(),
                observation_scope: observation.observation_scope,
                batch: observation.batch.clone(),
            },
        };
        let source_name = job.observation.source_name.clone();
        let surface_name = job.observation.surface_name.clone();
        match self
            .sender
            .try_send(ObservedSourceScanWorkerMessage::Observation(job))
        {
            Ok(()) => {}
            Err(TrySendError::Full(_message)) => {
                let dropped = self
                    .dropped_observations
                    .fetch_add(1, Ordering::Relaxed)
                    .saturating_add(1);
                if dropped == 1 || dropped.is_power_of_two() {
                    tracing::debug!(
                        dropped_observations = dropped,
                        source = %source_name,
                        surface = %surface_name,
                        "dropping source-scan observation because observed-value indexing is behind"
                    );
                }
            }
            Err(TrySendError::Disconnected(_message)) => {
                tracing::debug!(
                    source = %source_name,
                    surface = %surface_name,
                    "dropping source-scan observation because observed-value indexer is stopped"
                );
            }
        }
    }
}

struct ObservedSourceScanWorker {
    receiver: Receiver<ObservedSourceScanWorkerMessage>,
}

impl ObservedSourceScanWorker {
    fn run(self) {
        while let Ok(message) = self.receiver.recv() {
            match message {
                ObservedSourceScanWorkerMessage::Observation(job) => {
                    index_source_scan_job(&job);
                }
                ObservedSourceScanWorkerMessage::Flush(ack_sender) => {
                    if ack_sender.send(()).is_err() {}
                }
            }
        }
    }
}

fn index_source_scan_job(job: &ObservedSourceScanJob) {
    for attempt in 1..=OBSERVED_SOURCE_SCAN_INDEX_ATTEMPTS {
        match job
            .indexer
            .index_source_scan_observation_inner(&job.observation)
        {
            Ok(()) => return,
            Err(error) if observed_index_is_temporarily_busy(&error) => {
                if attempt == OBSERVED_SOURCE_SCAN_INDEX_ATTEMPTS {
                    tracing::debug!(
                        source = %job.observation.source_name,
                        surface = %job.observation.surface_name,
                        error = %error,
                        attempts = attempt,
                        "source-scan observed-value indexing remained busy; dropping observation"
                    );
                    return;
                }
                thread::sleep(StdDuration::from_millis(
                    OBSERVED_SOURCE_SCAN_INDEX_RETRY_MS,
                ));
            }
            Err(error) => {
                tracing::warn!(
                    source = %job.observation.source_name,
                    surface = %job.observation.surface_name,
                    error = %error,
                    "source-scan observed-value indexing failed; dropping observation"
                );
                return;
            }
        }
    }
}

fn observed_index_is_temporarily_busy(error: &ObservedValueIndexError) -> bool {
    matches!(
        error,
        ObservedValueIndexError::SearchIndex(SearchIndexError::RedbDatabase(
            redb::DatabaseError::DatabaseAlreadyOpen
        ))
    )
}

struct ObservedSourceScanJob {
    indexer: Arc<ObservedValueIndexer>,
    observation: OwnedSourceScanObservation,
}

enum ObservedSourceScanWorkerMessage {
    Observation(ObservedSourceScanJob),
    Flush(SyncSender<()>),
}

struct OwnedSourceScanObservation {
    source_name: String,
    surface_kind: SourceObservationSurfaceKind,
    surface_name: String,
    observation_scope: SourceObservationScope,
    batch: RecordBatch,
}

#[derive(Debug, thiserror::Error)]
enum ObservedValueIndexError {
    #[error(transparent)]
    SearchIndex(#[from] SearchIndexError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Toml(#[from] toml::de::Error),
}

fn clear_catalog_fingerprint(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
) -> Result<(), ObservedValueIndexError> {
    let path = layout
        .search_dir(workspace_name)
        .join(CATALOG_FINGERPRINT_FILE_NAME);
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn mark_observed_source_generation(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
) -> Result<(), std::io::Error> {
    let path = observed_source_generation_file(layout, workspace_name, source_name.as_str());
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, format!("{}\n", Uuid::new_v4()))
}

fn observed_source_generations(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
    surfaces: &[ObservedSurface],
) -> BTreeMap<String, Option<String>> {
    surfaces
        .iter()
        .map(|surface| {
            let generation =
                match read_observed_source_generation(layout, workspace_name, &surface.source_name)
                {
                    Ok(generation) => generation,
                    Err(error) => {
                        tracing::warn!(
                            workspace = %workspace_name,
                            source = %surface.source_name,
                            error = %error,
                            "failed to read observed-value source generation"
                        );
                        None
                    }
                };
            (surface.source_name.clone(), generation)
        })
        .collect()
}

fn read_observed_source_generation(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
    source_name: &str,
) -> Result<Option<String>, std::io::Error> {
    let path = observed_source_generation_file(layout, workspace_name, source_name);
    match fs::read_to_string(path) {
        Ok(generation) => Ok(Some(generation.trim().to_string())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn observed_source_generation_file(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
    source_name: &str,
) -> std::path::PathBuf {
    layout
        .search_dir(workspace_name)
        .join(SOURCE_GENERATION_DIR_NAME)
        .join(source_name)
}

pub(crate) fn observed_queue_foreground_drain_budget(layout: &AppStateLayout) -> StdDuration {
    let config = observed_search_config_or_default(layout);
    StdDuration::from_millis(
        config
            .search
            .queue_foreground_drain_ms
            .unwrap_or(DEFAULT_OBSERVED_QUEUE_FOREGROUND_DRAIN_MS),
    )
}

pub(crate) fn observed_storage_budget_bytes(layout: &AppStateLayout) -> u64 {
    let config = observed_search_config_or_default(layout);
    observed_storage_budget_bytes_from_config(&config.search)
}

pub(crate) fn observed_value_staleness_cutoff(layout: &AppStateLayout) -> String {
    let config = observed_search_config_or_default(layout);
    let days = config
        .search
        .value_stale_after_days
        .unwrap_or(DEFAULT_OBSERVED_VALUE_STALE_AFTER_DAYS)
        .max(1);
    let days = i64::try_from(days).unwrap_or(i64::MAX);
    (Utc::now() - Duration::days(days)).to_rfc3339_opts(SecondsFormat::Millis, true)
}

pub(crate) fn observed_values_enabled(layout: &AppStateLayout) -> bool {
    let config = observed_search_config_or_default(layout);
    config
        .search
        .values_enabled
        .unwrap_or(DEFAULT_OBSERVED_VALUES_ENABLED)
}

fn observed_source_scope_id(source: &QuerySource) -> String {
    observed_source_scope_id_with_credential_generation(source, None)
}

fn observed_source_scope_id_with_credential_generation(
    source: &QuerySource,
    credential_generation_id: Option<&str>,
) -> String {
    let mut hasher = Sha256::new();
    update_scope_hash(&mut hasher, "scope_version", "1");
    update_scope_hash(&mut hasher, "source_name", source.source_name());
    update_scope_hash(&mut hasher, "version", source.version().unwrap_or(""));
    update_scope_hash(
        &mut hasher,
        "credential_generation_id",
        credential_generation_id.unwrap_or(""),
    );
    update_declared_input_scope_hashes(&mut hasher, source);
    update_runtime_input_scope_hashes(&mut hasher, source);
    update_component_scope_hashes(&mut hasher, source);
    format!("{:x}", hasher.finalize())
}

fn update_declared_input_scope_hashes(hasher: &mut Sha256, source: &QuerySource) {
    for input in source.declared_inputs() {
        update_scope_hash(hasher, "input.key", &input.key);
        update_scope_hash(
            hasher,
            "input.kind",
            match input.kind {
                ManifestInputKind::Variable => "variable",
                ManifestInputKind::Secret => "secret",
            },
        );
        update_scope_hash(
            hasher,
            "input.required",
            if input.required { "true" } else { "false" },
        );
        update_scope_hash(hasher, "input.default", &input.default_value);
        update_scope_hash(hasher, "input.hint", input.hint.as_deref().unwrap_or(""));
        if let Some(credential) = input.credential.as_ref() {
            for method in &credential.methods {
                update_scope_hash(
                    hasher,
                    "input.credential.method.kind",
                    match method.kind {
                        coral_spec::ManifestCredentialMethodKind::SourceConfig => "source_config",
                        coral_spec::ManifestCredentialMethodKind::OAuth => "oauth",
                    },
                );
                update_scope_hash(
                    hasher,
                    "input.credential.method.hint",
                    method.hint.as_deref().unwrap_or(""),
                );
            }
        }
    }
}

fn update_runtime_input_scope_hashes(hasher: &mut Sha256, source: &QuerySource) {
    for (key, value) in source.variables() {
        update_scope_hash(hasher, "variable.key", key);
        update_scope_hash(hasher, "variable.value", value);
    }
    for (key, value) in source.secrets() {
        if !is_internal_material_key(key) {
            update_scope_hash(hasher, "secret.key", key);
            continue;
        }
        if is_safe_oauth_scope_metadata_key(key) {
            update_scope_hash(hasher, "oauth.metadata.key", key);
            update_scope_hash(hasher, "oauth.metadata.value", value);
        }
    }
}

fn update_component_scope_hashes(hasher: &mut Sha256, source: &QuerySource) {
    for component in source.components() {
        match component {
            RuntimeSourceComponent::Http(http) => {
                update_scope_hash(hasher, "component.kind", "http");
                for table in &http.tables {
                    update_surface_scope_hash(hasher, "table", table.name(), table.columns());
                }
                for function in &http.functions {
                    update_surface_scope_hash(
                        hasher,
                        "table_function",
                        &function.name,
                        &function.columns,
                    );
                }
            }
            RuntimeSourceComponent::File(file) => {
                update_scope_hash(hasher, "component.kind", "file");
                for table in &file.tables {
                    update_surface_scope_hash(hasher, "table", table.name(), table.columns());
                }
            }
            RuntimeSourceComponent::Mcp(mcp) => {
                update_scope_hash(hasher, "component.kind", "mcp");
                for table in &mcp.tables {
                    update_surface_scope_hash(hasher, "table", table.name(), table.columns());
                }
                for function in &mcp.functions {
                    update_surface_scope_hash(
                        hasher,
                        "table_function",
                        function.name(),
                        function.columns(),
                    );
                }
            }
        }
    }
}

pub(crate) fn observed_source_scopes_from_query_sources(
    selected_sources: &[QuerySource],
    credential_generation_ids: &BTreeMap<String, Option<String>>,
) -> BTreeMap<String, String> {
    selected_sources
        .iter()
        .map(|source| {
            let credential_generation_id = credential_generation_ids
                .get(source.source_name())
                .and_then(Option::as_deref);
            (
                source.source_name().to_string(),
                observed_source_scope_id_with_credential_generation(
                    source,
                    credential_generation_id,
                ),
            )
        })
        .collect()
}

fn update_surface_scope_hash(
    hasher: &mut Sha256,
    surface_kind: &str,
    surface_name: &str,
    columns: &[ColumnSpec],
) {
    update_scope_hash(hasher, "surface.kind", surface_kind);
    update_scope_hash(hasher, "surface.name", surface_name);
    for column in columns {
        update_scope_hash(hasher, "surface.column.name", &column.name);
        update_scope_hash(hasher, "surface.column.type", &column.data_type);
    }
}

fn update_scope_hash(hasher: &mut Sha256, label: &str, value: &str) {
    hasher.update(label.len().to_le_bytes());
    hasher.update(label.as_bytes());
    hasher.update(value.len().to_le_bytes());
    hasher.update(value.as_bytes());
}

fn is_safe_oauth_scope_metadata_key(key: &str) -> bool {
    let Some(rest) = key.strip_prefix(OAUTH_INTERNAL_KEY_PREFIX) else {
        return false;
    };
    [
        ".method",
        ".token_type",
        ".scope",
        ".client_id",
        ".token_url",
        ".client_secret_transport",
    ]
    .into_iter()
    .any(|suffix| rest.ends_with(suffix))
}

fn observed_search_config_or_default(layout: &AppStateLayout) -> ObservedSearchConfigFile {
    match load_observed_search_config(layout) {
        Ok(config) => config,
        Err(error) => {
            tracing::warn!(
                error = %error,
                "failed to load observed-value search config; using defaults"
            );
            ObservedSearchConfigFile::default()
        }
    }
}

fn observed_storage_budget_bytes_from_config(config: &ObservedSearchConfig) -> u64 {
    config
        .storage_mb
        .unwrap_or(DEFAULT_OBSERVED_MAX_STORAGE_MB)
        .saturating_mul(BYTES_PER_MIB)
}

fn observed_collection_budget_from_config(
    config: &ObservedSearchConfig,
) -> ObservedCollectionBudget {
    ObservedCollectionBudget {
        candidates: config
            .collection_candidates
            .unwrap_or(DEFAULT_OBSERVED_COLLECTION_MAX_CANDIDATES),
        candidate_bytes: config
            .collection_candidate_bytes
            .unwrap_or(DEFAULT_OBSERVED_COLLECTION_MAX_CANDIDATE_BYTES),
        json_depth: config
            .collection_json_depth
            .unwrap_or(DEFAULT_OBSERVED_COLLECTION_MAX_JSON_DEPTH),
    }
}

fn load_observed_search_config(
    layout: &AppStateLayout,
) -> Result<ObservedSearchConfigFile, ObservedValueIndexError> {
    if !layout.config_file().exists() {
        return Ok(ObservedSearchConfigFile::default());
    }

    let raw = std::fs::read_to_string(layout.config_file())?;
    Ok(toml::from_str(&raw)?)
}

#[derive(Debug, Clone)]
struct ObservedSurface {
    source_scope_id: String,
    source_name: String,
    surface_kind: ObservedValueSurfaceKind,
    surface_name: String,
    column_names: BTreeSet<String>,
    column_policy: ObservedSurfaceColumnPolicy,
}

impl ObservedSurface {
    fn allows_column(&self, column_name: &str) -> bool {
        (self.column_names.is_empty()
            || self
                .column_names
                .contains(&normalize_identifier(column_name)))
            && !self.column_policy.denies_column(column_name)
    }
}

#[derive(Debug, Clone)]
struct FieldProvenance {
    source_scope_id: String,
    source_name: String,
    surface_kind: ObservedValueSurfaceKind,
    surface_name: String,
    column_name: String,
    column_policy: ObservedSurfaceColumnPolicy,
}

#[derive(Debug, Clone, Default)]
struct ObservedValuePolicy {
    sources: BTreeSet<String>,
    surfaces: BTreeSet<String>,
    columns: BTreeSet<String>,
}

impl ObservedValuePolicy {
    fn from_config(config: &ObservedSearchConfig) -> Self {
        Self {
            sources: config
                .excluded_sources
                .iter()
                .map(|source| normalize_identifier(source))
                .filter(|source| !source.is_empty())
                .collect(),
            surfaces: config
                .excluded_surfaces
                .iter()
                .map(|surface| normalize_policy_path(surface))
                .filter(|surface| !surface.is_empty())
                .collect(),
            columns: config
                .excluded_columns
                .iter()
                .map(|column| normalize_policy_path(column))
                .filter(|column| !column.is_empty())
                .collect(),
        }
    }

    fn allows_source(&self, source_name: &str) -> bool {
        !self.sources.contains(&normalize_identifier(source_name))
    }

    fn allows_surface(&self, source_name: &str, surface_name: &str) -> bool {
        let surface = policy_path([surface_name]);
        let qualified_surface = policy_path([source_name, surface_name]);
        !self.surfaces.contains(&surface) && !self.surfaces.contains(&qualified_surface)
    }

    fn surface_column_policy(
        &self,
        source_name: &str,
        surface_name: &str,
    ) -> ObservedSurfaceColumnPolicy {
        ObservedSurfaceColumnPolicy {
            source_name: normalize_identifier(source_name),
            surface_name: normalize_identifier(surface_name),
            excluded_columns: self.columns.clone(),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct ObservedSurfaceColumnPolicy {
    source_name: String,
    surface_name: String,
    excluded_columns: BTreeSet<String>,
}

impl ObservedSurfaceColumnPolicy {
    fn denies_column(&self, field_path: &str) -> bool {
        if self.excluded_columns.is_empty() {
            return false;
        }
        let field_path = normalize_policy_path(field_path);
        if field_path.is_empty() {
            return false;
        }
        let surface_field_path = policy_path([self.surface_name.as_str(), field_path.as_str()]);
        let qualified_field_path = policy_path([
            self.source_name.as_str(),
            self.surface_name.as_str(),
            field_path.as_str(),
        ]);
        [&field_path, &surface_field_path, &qualified_field_path]
            .into_iter()
            .any(|candidate| self.excluded_columns.contains(candidate))
    }
}

fn observed_surfaces(
    selected_sources: &[QuerySource],
    source_scope_ids: &BTreeMap<String, String>,
    policy: &ObservedValuePolicy,
) -> Vec<ObservedSurface> {
    let mut surfaces = Vec::new();
    for source in selected_sources {
        let source_name = source.source_name().to_string();
        if !policy.allows_source(&source_name) {
            tracing::debug!(
                source = %source_name,
                "skipping observed-value indexing for source because policy excludes it"
            );
            continue;
        }
        let source_scope_id = source_scope_ids
            .get(source.source_name())
            .cloned()
            .unwrap_or_else(|| observed_source_scope_id(source));
        for component in source.components() {
            match component {
                RuntimeSourceComponent::Http(http) => {
                    for table in &http.tables {
                        if !policy.allows_surface(&source_name, table.name()) {
                            tracing::debug!(
                                source = %source_name,
                                surface = %table.name(),
                                "skipping observed-value indexing for surface because policy excludes it"
                            );
                            continue;
                        }
                        surfaces.push(observed_surface(
                            &source_scope_id,
                            &source_name,
                            ObservedValueSurfaceKind::Table,
                            table.name(),
                            table.columns(),
                            policy,
                        ));
                    }
                    for function in &http.functions {
                        if !policy.allows_surface(&source_name, &function.name) {
                            tracing::debug!(
                                source = %source_name,
                                surface = %function.name,
                                "skipping observed-value indexing for surface because policy excludes it"
                            );
                            continue;
                        }
                        surfaces.push(observed_surface(
                            &source_scope_id,
                            &source_name,
                            ObservedValueSurfaceKind::TableFunction,
                            &function.name,
                            &function.columns,
                            policy,
                        ));
                    }
                }
                RuntimeSourceComponent::File(_) => {}
                RuntimeSourceComponent::Mcp(mcp) => {
                    for table in &mcp.tables {
                        if !policy.allows_surface(&source_name, table.name()) {
                            tracing::debug!(
                                source = %source_name,
                                surface = %table.name(),
                                "skipping observed-value indexing for surface because policy excludes it"
                            );
                            continue;
                        }
                        surfaces.push(observed_surface(
                            &source_scope_id,
                            &source_name,
                            ObservedValueSurfaceKind::Table,
                            table.name(),
                            table.columns(),
                            policy,
                        ));
                    }
                    for function in &mcp.functions {
                        if !policy.allows_surface(&source_name, function.name()) {
                            tracing::debug!(
                                source = %source_name,
                                surface = %function.name(),
                                "skipping observed-value indexing for surface because policy excludes it"
                            );
                            continue;
                        }
                        surfaces.push(observed_surface(
                            &source_scope_id,
                            &source_name,
                            ObservedValueSurfaceKind::TableFunction,
                            function.name(),
                            function.columns(),
                            policy,
                        ));
                    }
                }
            }
        }
    }
    surfaces
}

fn observed_surface(
    source_scope_id: &str,
    source_name: &str,
    surface_kind: ObservedValueSurfaceKind,
    surface_name: &str,
    columns: &[ColumnSpec],
    policy: &ObservedValuePolicy,
) -> ObservedSurface {
    ObservedSurface {
        source_scope_id: source_scope_id.to_string(),
        source_name: source_name.to_string(),
        surface_kind,
        surface_name: surface_name.to_string(),
        column_policy: policy.surface_column_policy(source_name, surface_name),
        column_names: columns
            .iter()
            .map(|column| normalize_identifier(&column.name))
            .collect(),
    }
}

fn observed_value_surface_kind(
    surface_kind: SourceObservationSurfaceKind,
) -> ObservedValueSurfaceKind {
    match surface_kind {
        SourceObservationSurfaceKind::Table => ObservedValueSurfaceKind::Table,
        SourceObservationSurfaceKind::Function => ObservedValueSurfaceKind::TableFunction,
    }
}

fn direct_field_provenance(
    surface: &ObservedSurface,
    column_name: &str,
    output_field_name: &str,
) -> Option<FieldProvenance> {
    if !surface.allows_column(column_name) || is_sensitive_field_path(column_name) {
        return None;
    }
    if output_field_name.trim().is_empty() {
        return None;
    }
    Some(FieldProvenance {
        source_scope_id: surface.source_scope_id.clone(),
        source_name: surface.source_name.clone(),
        surface_kind: surface.surface_kind,
        surface_name: surface.surface_name.clone(),
        column_name: column_name.to_string(),
        column_policy: surface.column_policy.clone(),
    })
}

fn observed_records_from_batches(
    schema: &Schema,
    batches: &[RecordBatch],
    provenance: &[Option<FieldProvenance>],
    budget: ObservedCollectionBudget,
) -> Result<ObservedRecordCollection, ObservedValueIndexError> {
    let mut accumulator = ObservedRecordAccumulator::new(budget);
    for batch in batches {
        for row in record_batch_rows(batch)? {
            for (field_index, field_provenance) in provenance.iter().enumerate() {
                let Some(field_provenance) = field_provenance else {
                    continue;
                };
                let Some(field) = schema.fields().get(field_index) else {
                    continue;
                };
                let Some(value) = row.get(field.name()) else {
                    continue;
                };
                let candidates =
                    observed_candidate_values(field_provenance, value, budget.json_depth);
                if candidates.depth_exhausted {
                    accumulator.mark_budget_exhausted();
                }
                for candidate in &candidates.values {
                    accumulator.push(field_provenance, candidate);
                }
            }
        }
    }
    Ok(accumulator.finish())
}

fn record_batch_rows(
    batch: &RecordBatch,
) -> Result<Vec<Map<String, Value>>, ObservedValueIndexError> {
    let mut bytes = Vec::new();
    {
        let mut writer = arrow::json::ArrayWriter::new(&mut bytes);
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(serde_json::from_slice(&bytes)?)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservedCandidateValue {
    field_path: String,
    display_value: String,
    searchable_text: String,
    normalized_value_key: String,
}

#[derive(Debug, Clone)]
struct ObservedCandidateCollection {
    values: Vec<ObservedCandidateValue>,
    depth_exhausted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ObservedCollectionBudget {
    candidates: usize,
    candidate_bytes: usize,
    json_depth: usize,
}

impl Default for ObservedCollectionBudget {
    fn default() -> Self {
        Self {
            candidates: DEFAULT_OBSERVED_COLLECTION_MAX_CANDIDATES,
            candidate_bytes: DEFAULT_OBSERVED_COLLECTION_MAX_CANDIDATE_BYTES,
            json_depth: DEFAULT_OBSERVED_COLLECTION_MAX_JSON_DEPTH,
        }
    }
}

#[derive(Debug, Clone)]
struct ObservedRecordCollection {
    record_chunks: Vec<Vec<ObservedValueRecord>>,
    budget_exhausted: bool,
    accepted_candidates: usize,
    accepted_candidate_bytes: usize,
    skipped_oversize_candidates: usize,
}

impl ObservedRecordCollection {
    fn is_empty(&self) -> bool {
        self.record_chunks.iter().all(Vec::is_empty)
    }
}

#[derive(Debug)]
struct ObservedRecordAccumulator {
    budget: ObservedCollectionBudget,
    records: BTreeMap<ObservedValueRecordKey, ObservedValueRecord>,
    record_chunks: Vec<Vec<ObservedValueRecord>>,
    window_candidates: usize,
    window_candidate_bytes: usize,
    accepted_candidates: usize,
    accepted_candidate_bytes: usize,
    skipped_oversize_candidates: usize,
    budget_exhausted: bool,
}

impl ObservedRecordAccumulator {
    fn new(budget: ObservedCollectionBudget) -> Self {
        Self {
            budget,
            records: BTreeMap::new(),
            record_chunks: Vec::new(),
            window_candidates: 0,
            window_candidate_bytes: 0,
            accepted_candidates: 0,
            accepted_candidate_bytes: 0,
            skipped_oversize_candidates: 0,
            budget_exhausted: false,
        }
    }

    fn mark_budget_exhausted(&mut self) {
        self.budget_exhausted = true;
    }

    fn push(&mut self, provenance: &FieldProvenance, candidate: &ObservedCandidateValue) {
        let candidate_bytes = candidate_bytes(candidate);
        if self.budget.candidates == 0
            || self.budget.candidate_bytes == 0
            || candidate_bytes > self.budget.candidate_bytes
        {
            self.budget_exhausted = true;
            self.skipped_oversize_candidates = self.skipped_oversize_candidates.saturating_add(1);
            return;
        }

        if self.window_candidates >= self.budget.candidates
            || self.window_candidate_bytes.saturating_add(candidate_bytes)
                > self.budget.candidate_bytes
        {
            self.budget_exhausted = true;
            self.flush_current_chunk();
        }

        self.window_candidates = self.window_candidates.saturating_add(1);
        self.window_candidate_bytes = self.window_candidate_bytes.saturating_add(candidate_bytes);
        self.accepted_candidates = self.accepted_candidates.saturating_add(1);
        self.accepted_candidate_bytes = self
            .accepted_candidate_bytes
            .saturating_add(candidate_bytes);
        self.insert(provenance, candidate);
    }

    fn insert(&mut self, provenance: &FieldProvenance, candidate: &ObservedCandidateValue) {
        let key = ObservedValueRecordKey {
            source_scope_id: provenance.source_scope_id.clone(),
            source_name: provenance.source_name.clone(),
            surface_kind: provenance.surface_kind,
            surface_name: provenance.surface_name.clone(),
            column_name: candidate.field_path.clone(),
            normalized_value_key: candidate.normalized_value_key.clone(),
        };
        self.records
            .entry(key)
            .and_modify(|record| {
                record.observed_count = record.observed_count.saturating_add(1);
            })
            .or_insert_with(|| ObservedValueRecord {
                source_scope_id: provenance.source_scope_id.clone(),
                source_name: provenance.source_name.clone(),
                surface_kind: provenance.surface_kind,
                surface_name: provenance.surface_name.clone(),
                column_name: candidate.field_path.clone(),
                normalized_value_key: candidate.normalized_value_key.clone(),
                display_value: candidate.display_value.clone(),
                searchable_text: candidate.searchable_text.clone(),
                suggested_operator: ObservedValueSuggestedOperator::Exact,
                observed_count: 1,
            });
    }

    fn flush_current_chunk(&mut self) {
        if !self.records.is_empty() {
            self.record_chunks
                .push(std::mem::take(&mut self.records).into_values().collect());
        }
        self.window_candidates = 0;
        self.window_candidate_bytes = 0;
    }

    fn finish(mut self) -> ObservedRecordCollection {
        self.flush_current_chunk();
        ObservedRecordCollection {
            record_chunks: self.record_chunks,
            budget_exhausted: self.budget_exhausted,
            accepted_candidates: self.accepted_candidates,
            accepted_candidate_bytes: self.accepted_candidate_bytes,
            skipped_oversize_candidates: self.skipped_oversize_candidates,
        }
    }
}

fn candidate_bytes(candidate: &ObservedCandidateValue) -> usize {
    candidate
        .field_path
        .len()
        .saturating_add(candidate.display_value.len())
        .saturating_add(candidate.searchable_text.len())
}

fn observed_candidate_values(
    provenance: &FieldProvenance,
    value: &Value,
    max_json_depth: usize,
) -> ObservedCandidateCollection {
    let mut candidates = BTreeMap::<(String, String), ObservedCandidateValue>::new();
    let mut depth_exhausted = false;
    collect_observed_candidates(
        provenance,
        &provenance.column_name,
        value,
        0,
        max_json_depth,
        &mut depth_exhausted,
        &mut candidates,
    );
    ObservedCandidateCollection {
        values: candidates.into_values().collect(),
        depth_exhausted,
    }
}

fn collect_observed_candidates(
    provenance: &FieldProvenance,
    field_path: &str,
    value: &Value,
    depth: usize,
    max_json_depth: usize,
    depth_exhausted: &mut bool,
    candidates: &mut BTreeMap<(String, String), ObservedCandidateValue>,
) {
    if depth > max_json_depth {
        *depth_exhausted = true;
        return;
    }

    match value {
        Value::Null => {}
        Value::String(value) => {
            collect_string_candidates(
                provenance,
                field_path,
                value,
                depth,
                max_json_depth,
                depth_exhausted,
                candidates,
            );
        }
        Value::Bool(value) => push_observed_candidate(
            provenance,
            field_path,
            if *value { "true" } else { "false" },
            candidates,
        ),
        Value::Number(value) => {
            push_observed_candidate(provenance, field_path, &value.to_string(), candidates);
        }
        Value::Array(items) => {
            if !contains_sensitive_observed_path(field_path, value)
                && !contains_denied_observed_path(&provenance.column_policy, field_path, value)
                && !json_depth_exceeds(value, depth, max_json_depth)
                && let Ok(display_value) = serde_json::to_string(value)
            {
                push_observed_candidate(provenance, field_path, &display_value, candidates);
            }
            for item in items {
                collect_observed_candidates(
                    provenance,
                    field_path,
                    item,
                    depth.saturating_add(1),
                    max_json_depth,
                    depth_exhausted,
                    candidates,
                );
            }
        }
        Value::Object(object) => {
            if !contains_sensitive_observed_path(field_path, value)
                && !contains_denied_observed_path(&provenance.column_policy, field_path, value)
                && !json_depth_exceeds(value, depth, max_json_depth)
                && let Ok(display_value) = serde_json::to_string(value)
            {
                push_observed_candidate(provenance, field_path, &display_value, candidates);
            }
            for (key, value) in object {
                let child_path = observed_field_path(field_path, key);
                collect_observed_candidates(
                    provenance,
                    &child_path,
                    value,
                    depth.saturating_add(1),
                    max_json_depth,
                    depth_exhausted,
                    candidates,
                );
            }
        }
    }
}

fn collect_string_candidates(
    provenance: &FieldProvenance,
    field_path: &str,
    value: &str,
    depth: usize,
    max_json_depth: usize,
    depth_exhausted: &mut bool,
    candidates: &mut BTreeMap<(String, String), ObservedCandidateValue>,
) {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return;
    }

    let parsed_json = looks_like_json_container(trimmed)
        .then(|| serde_json::from_str::<Value>(trimmed).ok())
        .flatten();
    let key_value_pairs = key_value_pairs(trimmed);
    let raw_contains_sensitive_child = parsed_json
        .as_ref()
        .is_some_and(|value| contains_sensitive_observed_path(field_path, value))
        || contains_sensitive_raw_value(trimmed)
        || key_value_pairs
            .iter()
            .any(|pair| is_sensitive_field_path(&observed_field_path(field_path, &pair.key)));
    let raw_contains_denied_child = parsed_json.as_ref().is_some_and(|value| {
        contains_denied_observed_path(&provenance.column_policy, field_path, value)
    }) || key_value_pairs.iter().any(|pair| {
        provenance
            .column_policy
            .denies_column(&observed_field_path(field_path, &pair.key))
    });
    let raw_exceeds_json_depth = parsed_json
        .as_ref()
        .is_some_and(|value| json_depth_exceeds(value, depth, max_json_depth));

    if !raw_contains_sensitive_child && !raw_contains_denied_child && !raw_exceeds_json_depth {
        push_observed_candidate(provenance, field_path, trimmed, candidates);
    }

    if let Some(parsed) = parsed_json {
        collect_observed_candidates(
            provenance,
            field_path,
            &parsed,
            depth,
            max_json_depth,
            depth_exhausted,
            candidates,
        );
    }

    for pair in key_value_pairs {
        if depth.saturating_add(1) > max_json_depth {
            *depth_exhausted = true;
            break;
        }
        let child_path = observed_field_path(field_path, &pair.key);
        push_observed_candidate(provenance, &child_path, &pair.value, candidates);
    }
}

fn push_observed_candidate(
    provenance: &FieldProvenance,
    field_path: &str,
    display_value: &str,
    candidates: &mut BTreeMap<(String, String), ObservedCandidateValue>,
) {
    let display_value = display_value.trim();
    if field_path.is_empty()
        || display_value.is_empty()
        || !display_value.chars().any(char::is_alphanumeric)
        || provenance.column_policy.denies_column(field_path)
        || is_sensitive_field_path(field_path)
        || is_sensitive_value(display_value)
    {
        return;
    }

    let normalized_value_key = normalized_value_key(display_value);
    let key = (field_path.to_string(), normalized_value_key.clone());
    if candidates.contains_key(&key) {
        return;
    }

    let searchable_text = observed_searchable_text(provenance, field_path, display_value);
    candidates.insert(
        key,
        ObservedCandidateValue {
            field_path: field_path.to_string(),
            display_value: display_value.to_string(),
            searchable_text,
            normalized_value_key,
        },
    );
}

fn observed_searchable_text(
    provenance: &FieldProvenance,
    field_path: &str,
    display_value: &str,
) -> String {
    [
        provenance.source_name.as_str(),
        provenance.surface_name.as_str(),
        provenance.column_name.as_str(),
        field_path,
        display_value,
    ]
    .into_iter()
    .filter(|part| !part.trim().is_empty())
    .collect::<Vec<_>>()
    .join(" ")
}

fn observed_field_path(parent: &str, child: &str) -> String {
    if parent.is_empty() {
        child.to_string()
    } else {
        format!("{parent}.{child}")
    }
}

fn contains_sensitive_observed_path(field_path: &str, value: &Value) -> bool {
    match value {
        Value::Object(object) => object.iter().any(|(key, value)| {
            let child_path = observed_field_path(field_path, key);
            is_sensitive_field_path(&child_path)
                || contains_sensitive_observed_path(&child_path, value)
        }),
        Value::Array(items) => items
            .iter()
            .any(|item| contains_sensitive_observed_path(field_path, item)),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => false,
    }
}

fn contains_denied_observed_path(
    policy: &ObservedSurfaceColumnPolicy,
    field_path: &str,
    value: &Value,
) -> bool {
    match value {
        Value::Object(object) => object.iter().any(|(key, value)| {
            let child_path = observed_field_path(field_path, key);
            policy.denies_column(&child_path)
                || contains_denied_observed_path(policy, &child_path, value)
        }),
        Value::Array(items) => items
            .iter()
            .any(|item| contains_denied_observed_path(policy, field_path, item)),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => false,
    }
}

fn json_depth_exceeds(value: &Value, depth: usize, max_depth: usize) -> bool {
    if depth > max_depth {
        return true;
    }
    match value {
        Value::Array(items) => items
            .iter()
            .any(|item| json_depth_exceeds(item, depth.saturating_add(1), max_depth)),
        Value::Object(object) => object
            .values()
            .any(|child| json_depth_exceeds(child, depth.saturating_add(1), max_depth)),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => false,
    }
}

fn contains_sensitive_raw_value(value: &str) -> bool {
    contains_sensitive_assignment_key(value) || is_sensitive_value(value)
}

fn contains_sensitive_assignment_key(value: &str) -> bool {
    value
        .char_indices()
        .filter(|(_index, character)| matches!(character, ':' | '='))
        .filter_map(|(separator_index, _separator)| {
            sensitive_key_before_separator(value, separator_index)
        })
        .any(is_sensitive_field_path)
}

fn sensitive_key_before_separator(value: &str, separator_index: usize) -> Option<&str> {
    value
        .get(..separator_index)?
        .rsplit(|character: char| !is_key_char(character))
        .find(|part| !part.is_empty() && is_key_candidate(part))
}

fn starts_with_credential_scheme(value: &str) -> bool {
    let lower = value.trim_start().to_ascii_lowercase();
    lower.starts_with("bearer ") || lower.starts_with("basic ")
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct KeyValuePair {
    key: String,
    value: String,
}

fn key_value_pairs(value: &str) -> Vec<KeyValuePair> {
    let Some(pairs) = parse_key_value_pairs(value) else {
        return Vec::new();
    };
    if pairs.len() >= 2 || looks_like_single_key_value_bag(value, &pairs) {
        pairs
    } else {
        Vec::new()
    }
}

fn parse_key_value_pairs(value: &str) -> Option<Vec<KeyValuePair>> {
    let mut pairs = Vec::new();
    let mut remaining = value.trim();
    while !remaining.is_empty() {
        remaining = trim_pair_separator_prefix(remaining);
        if remaining.is_empty() {
            break;
        }

        let key_len = remaining
            .char_indices()
            .take_while(|(_index, character)| is_key_char(*character))
            .map(|(index, character)| index + character.len_utf8())
            .last()
            .unwrap_or(0);
        if key_len == 0 {
            return None;
        }
        let key = remaining.get(..key_len)?;
        if !is_key_candidate(key) {
            return None;
        }

        remaining = remaining.get(key_len..)?.trim_start();
        let separator = remaining.chars().next()?;
        if !matches!(separator, ':' | '=') {
            return None;
        }
        remaining = remaining.get(separator.len_utf8()..)?.trim_start();
        let (pair_value, next) = parse_key_value_pair_value(remaining)?;
        if pair_value.trim().is_empty() || !pair_value.chars().any(char::is_alphanumeric) {
            return None;
        }
        pairs.push(KeyValuePair {
            key: key.to_string(),
            value: pair_value.trim().to_string(),
        });
        remaining = next;
    }
    (!pairs.is_empty()).then_some(pairs)
}

fn trim_pair_separator_prefix(value: &str) -> &str {
    value.trim_start_matches(|character: char| {
        character.is_whitespace() || matches!(character, ',' | ';')
    })
}

fn parse_key_value_pair_value(value: &str) -> Option<(&str, &str)> {
    if let Some(quote) = value
        .chars()
        .next()
        .filter(|character| matches!(character, '"' | '\''))
    {
        let after_quote = value.get(quote.len_utf8()..)?;
        let end_index = after_quote.find(quote)?;
        let pair_value = after_quote.get(..end_index)?;
        let next = after_quote.get(end_index + quote.len_utf8()..)?;
        return Some((pair_value, next));
    }

    let value_len = value
        .char_indices()
        .take_while(|(_index, character)| {
            !character.is_whitespace() && !matches!(character, ',' | ';' | '\n' | '\r' | '\t')
        })
        .map(|(index, character)| index + character.len_utf8())
        .last()
        .unwrap_or(0);
    (value_len > 0).then(|| {
        (
            value
                .get(..value_len)
                .expect("value_len is a char boundary"),
            value
                .get(value_len..)
                .expect("value_len is a char boundary"),
        )
    })
}

fn looks_like_single_key_value_bag(value: &str, pairs: &[KeyValuePair]) -> bool {
    pairs.len() == 1 && !value.chars().any(char::is_whitespace) && !looks_like_url(value)
}

fn looks_like_json_container(value: &str) -> bool {
    (value.starts_with('{') && value.ends_with('}'))
        || (value.starts_with('[') && value.ends_with(']'))
}

fn is_key_char(character: char) -> bool {
    character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.')
}

fn is_key_candidate(key: &str) -> bool {
    key.chars()
        .next()
        .is_some_and(|character| character.is_ascii_alphabetic() || character == '_')
        && key.chars().any(char::is_alphabetic)
}

fn looks_like_url(value: &str) -> bool {
    value.contains("://") || value.starts_with("www.")
}

fn normalized_value_key(display_value: &str) -> String {
    let normalized = display_value.trim().to_ascii_lowercase();
    let digest = Sha256::digest(normalized.as_bytes());
    format!("{digest:x}")
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ObservedValueRecordKey {
    source_scope_id: String,
    source_name: String,
    surface_kind: ObservedValueSurfaceKind,
    surface_name: String,
    column_name: String,
    normalized_value_key: String,
}

fn is_sensitive_field_path(field_path: &str) -> bool {
    let tokens = field_path_tokens(field_path);
    if tokens.is_empty() {
        return false;
    }
    let token_refs = tokens.iter().map(String::as_str).collect::<Vec<_>>();
    if token_refs.iter().any(|token| {
        matches!(
            *token,
            "authorization"
                | "cookie"
                | "cookies"
                | "credentials"
                | "credential"
                | "jwt"
                | "passkey"
                | "password"
                | "passwd"
                | "pat"
                | "pem"
                | "pwd"
                | "signature"
                | "ssn"
                | "totp"
                | "cvc"
                | "cvv"
        )
    }) {
        return true;
    }
    if sensitive_marker_token(&token_refs, "token")
        || sensitive_marker_token(&token_refs, "secret")
        || token_refs == ["auth"]
    {
        return true;
    }
    [
        &["api", "key"][..],
        &["x", "api", "key"][..],
        &["access", "key"][..],
        &["access", "token"][..],
        &["auth", "token"][..],
        &["backup", "code"][..],
        &["bearer", "token"][..],
        &["card", "number"][..],
        &["card", "num"][..],
        &["client", "secret"][..],
        &["credit", "card"][..],
        &["csrf", "token"][..],
        &["debit", "card"][..],
        &["drivers", "license"][..],
        &["driver", "license"][..],
        &["id", "token"][..],
        &["mfa", "code"][..],
        &["oauth", "token"][..],
        &["one", "time", "password"][..],
        &["passport", "number"][..],
        &["personal", "access", "token"][..],
        &["private", "key"][..],
        &["refresh", "token"][..],
        &["recovery", "code"][..],
        &["recovery", "codes"][..],
        &["secret", "key"][..],
        &["signing", "secret"][..],
        &["session", "id"][..],
        &["session", "token"][..],
        &["ssh", "key"][..],
        &["ssh", "private", "key"][..],
        &["set", "cookie"][..],
        &["social", "security"][..],
        &["social", "security", "number"][..],
        &["tax", "id"][..],
        &["tax", "identification", "number"][..],
        &["taxpayer", "id"][..],
        &["tin", "number"][..],
        &["totp", "secret"][..],
        &["webhook", "secret"][..],
    ]
    .into_iter()
    .any(|phrase| contains_token_phrase(&token_refs, phrase))
}

fn is_sensitive_value(value: &str) -> bool {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return false;
    }
    starts_with_credential_scheme(trimmed)
        || contains_private_key_block(trimmed)
        || contains_url_credentials(trimmed)
        || credential_tokens(trimmed).any(|token| {
            looks_like_jwt(token)
                || has_known_secret_prefix(token)
                || looks_like_long_credential_token(token)
        })
}

fn sensitive_marker_token(tokens: &[&str], marker: &str) -> bool {
    tokens.iter().enumerate().any(|(index, token)| {
        *token == marker
            && tokens
                .get(index.saturating_add(1))
                .is_none_or(|next| !matches!(*next, "count" | "name" | "type"))
    })
}

fn field_path_tokens(field_path: &str) -> Vec<String> {
    normalize_identifier(field_path)
        .split('_')
        .filter(|token| !token.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn contains_token_phrase(tokens: &[&str], phrase: &[&str]) -> bool {
    phrase.len() <= tokens.len() && tokens.windows(phrase.len()).any(|window| window == phrase)
}

fn contains_private_key_block(value: &str) -> bool {
    let upper = value.to_ascii_uppercase();
    upper.contains("-----BEGIN ") && upper.contains(" PRIVATE KEY-----")
}

fn contains_url_credentials(value: &str) -> bool {
    value
        .split_whitespace()
        .filter(|part| part.contains("://"))
        .filter_map(|part| part.split_once("://").map(|(_scheme, rest)| rest))
        .any(|rest| {
            let authority = rest.split(['/', '?', '#']).next().unwrap_or(rest);
            authority.contains('@')
                && authority.split('@').next().is_some_and(|userinfo| {
                    userinfo.contains(':') && userinfo.chars().any(char::is_alphanumeric)
                })
        })
}

fn credential_tokens(value: &str) -> impl Iterator<Item = &str> {
    value
        .split(|character: char| {
            !(character.is_ascii_alphanumeric()
                || matches!(character, '_' | '-' | '.' | '=' | '+' | '/' | ':'))
        })
        .map(|token| token.trim_matches(|character: char| matches!(character, ':' | '"' | '\'')))
        .filter(|token| !token.is_empty())
}

fn looks_like_jwt(token: &str) -> bool {
    let mut parts = token.split('.');
    let Some(header) = parts.next() else {
        return false;
    };
    let Some(payload) = parts.next() else {
        return false;
    };
    let Some(signature) = parts.next() else {
        return false;
    };
    parts.next().is_none()
        && header.len() >= 8
        && payload.len() >= 8
        && signature.len() >= 8
        && [header, payload, signature]
            .into_iter()
            .all(|part| part.chars().all(is_base64url_char))
}

fn is_base64url_char(character: char) -> bool {
    character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
}

fn has_known_secret_prefix(token: &str) -> bool {
    let lower = token.to_ascii_lowercase();
    (lower.starts_with("ghp_") && token.len() >= 20)
        || (lower.starts_with("gho_") && token.len() >= 20)
        || (lower.starts_with("ghu_") && token.len() >= 20)
        || (lower.starts_with("ghs_") && token.len() >= 20)
        || (lower.starts_with("ghr_") && token.len() >= 20)
        || (lower.starts_with("github_pat_") && token.len() >= 30)
        || (lower.starts_with("glpat-") && token.len() >= 20)
        || (lower.starts_with("gloas-") && token.len() >= 20)
        || (lower.starts_with("gldt-") && token.len() >= 20)
        || (lower.starts_with("npm_") && token.len() >= 20)
        || (lower.starts_with("hf_") && token.len() >= 20)
        || (lower.starts_with("sk_live_") && token.len() >= 20)
        || (lower.starts_with("sk_test_") && token.len() >= 20)
        || (lower.starts_with("sk-proj-") && token.len() >= 32)
        || (lower.starts_with("rk_live_") && token.len() >= 20)
        || (lower.starts_with("rk_test_") && token.len() >= 20)
        || (lower.starts_with("whsec_") && token.len() >= 20)
        || (lower.starts_with("xoxb-") && token.len() >= 20)
        || (lower.starts_with("xoxp-") && token.len() >= 20)
        || (lower.starts_with("xoxa-") && token.len() >= 20)
        || (lower.starts_with("ya29.") && token.len() >= 20)
        || (token.starts_with("AIza") && token.len() >= 30)
        || (token.starts_with("SG.") && token.len() >= 20)
        || (token.starts_with("AKIA")
            && token.len() == 20
            && token
                .chars()
                .all(|character| character.is_ascii_uppercase() || character.is_ascii_digit()))
}

fn looks_like_long_credential_token(token: &str) -> bool {
    if token.len() < 32
        || looks_like_url(token)
        || token.chars().any(char::is_whitespace)
        || !token.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '=' | '+' | '/')
        })
        || token.chars().all(|character| character.is_ascii_hexdigit())
    {
        return false;
    }
    let has_upper = token
        .chars()
        .any(|character| character.is_ascii_uppercase());
    let has_lower = token
        .chars()
        .any(|character| character.is_ascii_lowercase());
    let has_digit = token.chars().any(|character| character.is_ascii_digit());
    let has_symbol = token
        .chars()
        .any(|character| matches!(character, '_' | '-' | '=' | '+' | '/'));
    let class_count = [has_upper, has_lower, has_digit, has_symbol]
        .into_iter()
        .filter(|present| *present)
        .count();
    let unique_chars = token.chars().collect::<BTreeSet<_>>().len();
    class_count >= 3 && unique_chars >= 16
}

fn same_identifier(left: &str, right: &str) -> bool {
    normalize_identifier(left) == normalize_identifier(right)
}

fn policy_path<'a>(parts: impl IntoIterator<Item = &'a str>) -> String {
    parts
        .into_iter()
        .flat_map(|part| part.split('.'))
        .map(normalize_identifier)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(".")
}

fn normalize_policy_path(value: &str) -> String {
    policy_path([value])
}

fn normalize_identifier(value: &str) -> String {
    let mut normalized = String::new();
    let mut previous_was_separator = true;
    let mut previous_was_lower_or_digit = false;

    for ch in value.trim().chars() {
        if ch.is_ascii_alphanumeric() {
            if ch.is_ascii_uppercase()
                && previous_was_lower_or_digit
                && !previous_was_separator
                && !normalized.ends_with('_')
            {
                normalized.push('_');
            }
            normalized.push(ch.to_ascii_lowercase());
            previous_was_separator = false;
            previous_was_lower_or_digit = ch.is_ascii_lowercase() || ch.is_ascii_digit();
        } else if !normalized.is_empty() && !normalized.ends_with('_') {
            normalized.push('_');
            previous_was_separator = true;
            previous_was_lower_or_digit = false;
        } else {
            previous_was_separator = true;
            previous_was_lower_or_digit = false;
        }
    }

    normalized.trim_matches('_').to_string()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field};
    use chrono::{DateTime, Duration, Utc};
    use coral_engine::{
        QuerySource, SourceObservationScope, SourceObservationSurfaceKind, SourceScanObservation,
    };
    use coral_spec::parse_source_manifest_value;
    use serde_json::json;
    use tempfile::tempdir;

    use crate::search::index::ObservedValueSearchHit;

    use super::*;

    #[test]
    fn observed_value_staleness_cutoff_loads_search_config() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        std::fs::write(
            layout.config_file(),
            r"
version = 1

[search]
observed_value_stale_after_days = 7
",
        )
        .expect("write config");
        let before = Utc::now() - Duration::days(7) - Duration::seconds(1);

        let cutoff = DateTime::parse_from_rfc3339(&observed_value_staleness_cutoff(&layout))
            .expect("cutoff timestamp")
            .with_timezone(&Utc);

        let after = Utc::now() - Duration::days(7) + Duration::seconds(1);
        assert!(cutoff >= before, "cutoff {cutoff} before {before}");
        assert!(cutoff <= after, "cutoff {cutoff} after {after}");
    }

    #[test]
    fn observed_source_scope_changes_with_credential_generation() {
        let source = http_query_source("fixture");

        let first = observed_source_scope_id_with_credential_generation(
            &source,
            Some("credential-generation-1"),
        );
        let second = observed_source_scope_id_with_credential_generation(
            &source,
            Some("credential-generation-2"),
        );

        assert_ne!(first, second);
    }

    #[test]
    fn observed_policy_excludes_sources_surfaces_and_columns() {
        let config = ObservedSearchConfig {
            excluded_sources: vec!["github".to_string()],
            excluded_surfaces: vec!["linear.issues".to_string(), "users".to_string()],
            excluded_columns: vec![
                "payload.sha".to_string(),
                "fixture.messages.tags.kube_deployment".to_string(),
            ],
            ..ObservedSearchConfig::default()
        };
        let policy = ObservedValuePolicy::from_config(&config);

        assert!(!policy.allows_source("github"));
        assert!(policy.allows_source("linear"));
        assert!(!policy.allows_surface("linear", "issues"));
        assert!(!policy.allows_surface("github", "users"));
        assert!(policy.allows_surface("github", "issues"));

        let column_policy = policy.surface_column_policy("fixture", "messages");
        assert!(column_policy.denies_column("payload.sha"));
        assert!(column_policy.denies_column("tags.kube_deployment"));
        assert!(!column_policy.denies_column("payload.event"));
        assert!(!column_policy.denies_column("tags.service"));
    }

    #[test]
    fn observed_policy_filters_surface_discovery() {
        let source_policy = ObservedValuePolicy::from_config(&ObservedSearchConfig {
            excluded_sources: vec!["fixture".to_string()],
            ..ObservedSearchConfig::default()
        });
        assert!(
            observed_surfaces(
                &[http_query_source("fixture")],
                &BTreeMap::new(),
                &source_policy
            )
            .is_empty()
        );

        let surface_policy = ObservedValuePolicy::from_config(&ObservedSearchConfig {
            excluded_surfaces: vec!["fixture.messages".to_string()],
            ..ObservedSearchConfig::default()
        });
        assert!(
            observed_surfaces(
                &[http_query_source("fixture")],
                &BTreeMap::new(),
                &surface_policy,
            )
            .is_empty()
        );

        let allowed_policy = ObservedValuePolicy::default();
        let surfaces = observed_surfaces(
            &[http_query_source("fixture")],
            &BTreeMap::new(),
            &allowed_policy,
        );
        assert_eq!(surfaces.len(), 1);
        let surface = surfaces.first().expect("allowed observed surface");
        assert_eq!(surface.source_name, "fixture");
        assert_eq!(surface.surface_name, "messages");
    }

    #[test]
    fn sensitivity_classifier_is_token_aware() {
        for benign_field in ["author", "token_count", "session_name", "auth_status"] {
            assert!(
                !is_sensitive_field_path(benign_field),
                "{benign_field} should not be classified as sensitive"
            );
        }
        for sensitive_field in [
            "access_token",
            "user.token",
            "privateKey",
            "headers.Authorization",
            "session_id",
            "credentials.password",
        ] {
            assert!(
                is_sensitive_field_path(sensitive_field),
                "{sensitive_field} should be classified as sensitive"
            );
        }
    }

    #[test]
    fn sensitivity_classifier_covers_common_provider_tokens() {
        for sensitive_field in [
            "x_api_key",
            "apiKey",
            "clientSecret",
            "csrf_token",
            "personal_access_token",
            "webhook_secret",
            "signing_secret",
            "ssh_private_key",
            "totp_secret",
            "mfa_code",
            "recovery_codes",
            "oauth.id_token",
            "cookies.session",
            "jwt",
            "pat",
        ] {
            assert!(
                is_sensitive_field_path(sensitive_field),
                "{sensitive_field} should be classified as sensitive"
            );
        }

        for benign_field in [
            "author_name",
            "authentication_status",
            "session_title",
            "token_count",
            "secretary",
            "github_path",
        ] {
            assert!(
                !is_sensitive_field_path(benign_field),
                "{benign_field} should not be classified as sensitive"
            );
        }

        let sensitive_values = [
            [
                "github",
                "_pat_",
                "11AA222bb333CC444dd555EE666ff777GG888hh999II",
            ]
            .concat(),
            ["gl", "pat-", "1234567890abcdefghijkl"].concat(),
            ["npm", "_", "abCdEfGhIjKlMnOpQrStUvWxYz123456"].concat(),
            ["h", "f_", "abcdefghijklmnopqrstuvwxyz1234567890"].concat(),
            ["rk", "_live_", "1234567890abcdefghijklmnop"].concat(),
            ["wh", "sec_", "1234567890abcdefghijklmnop"].concat(),
            ["ya", "29.", "a0AfH6SMBabcdefghijklmnop"].concat(),
            ["AI", "zaSyA1234567890abcdefghijklmnopqrstu"].concat(),
            [
                "S",
                "G.abcdefghijklmnopqrstuvwxyz.1234567890abcdefghijklmnop",
            ]
            .concat(),
        ];
        for sensitive_value in &sensitive_values {
            assert!(
                is_sensitive_value(sensitive_value),
                "{sensitive_value} should be classified as sensitive"
            );
        }

        for benign_value in [
            "abc123",
            "0123456789abcdef0123456789abcdef",
            "https://example.com/path/to/resource",
            "release-v2026-06-23",
        ] {
            assert!(
                !is_sensitive_value(benign_value),
                "{benign_value} should not be classified as sensitive"
            );
        }
    }

    #[test]
    fn indexes_direct_table_values_including_long_text_and_json_strings() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = http_query_source("fixture");
        let schema = Arc::new(Schema::new(vec![
            Field::new("service", DataType::Utf8, true),
            Field::new("body", DataType::Utf8, true),
            Field::new("payload", DataType::Utf8, true),
            Field::new("tags", DataType::Utf8, true),
            Field::new("api_token", DataType::Utf8, true),
            Field::new("privateKey", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["payments-api", "billing-worker"])),
                Arc::new(StringArray::from(vec![
                    "short incident note",
                    "very long incident body with retry budget exhausted and deploy rollback context",
                ])),
                Arc::new(StringArray::from(vec![
                    r#"{"error":"timeout","region":"us-east-1","api_token":"nested-secret","privateKey":"nested-private-key","private-key":"nested-hyphen-private-key"}"#,
                    r#"{"error":"deploy_failed","sha":"abc123"}"#,
                ])),
                Arc::new(StringArray::from(vec![
                    "env:prod,kube_deployment:titaness-worker,service:titaness-worker",
                    "env=prod service=billing-worker status=error",
                ])),
                Arc::new(StringArray::from(vec!["secret-token", "another-secret"])),
                Arc::new(StringArray::from(vec![
                    "direct-private-key",
                    "another-direct-private-key",
                ])),
            ],
        )
        .expect("batch");

        index_fixture_source_scan(&layout, &workspace, source, batch);

        let store = SearchIndexStore::open_workspace(&layout, &workspace).expect("store");
        drain_observed_queue(&store);
        let hits = store
            .search_observed_values(&workspace, &["deploy_failed".to_string()], 10)
            .expect("search observed");
        assert!(hits.iter().any(|hit| hit.column_name == "payload.error"));

        let hits = store
            .search_observed_values(&workspace, &["abc123".to_string()], 10)
            .expect("search observed");
        assert!(hits.iter().any(|hit| hit.column_name == "payload.sha"));
        assert!(hits.iter().any(|hit| hit.column_name == "payload"));

        let hits = store
            .search_observed_values(&workspace, &["titaness-worker".to_string()], 10)
            .expect("search observed");
        assert!(hits.iter().any(|hit| hit.column_name == "tags.service"));
        assert!(
            hits.iter()
                .any(|hit| hit.column_name == "tags.kube_deployment")
        );
        assert!(hits.iter().any(|hit| hit.column_name == "tags"));

        let hits = store
            .search_observed_values(&workspace, &["rollback".to_string()], 10)
            .expect("search observed");
        assert!(hits.iter().any(|hit| hit.column_name == "body"));

        let hits = store
            .search_observed_values(&workspace, &["secret-token".to_string()], 10)
            .expect("search sensitive");
        assert!(hits.is_empty());

        let hits = store
            .search_observed_values(&workspace, &["nested-secret".to_string()], 10)
            .expect("search nested sensitive");
        assert!(hits.is_empty());

        let hits = store
            .search_observed_values(&workspace, &["direct-private-key".to_string()], 10)
            .expect("search camel-case sensitive");
        assert!(hits.is_empty());

        let hits = store
            .search_observed_values(&workspace, &["nested-private-key".to_string()], 10)
            .expect("search nested camel-case sensitive");
        assert!(hits.is_empty());

        let hits = store
            .search_observed_values(&workspace, &["nested-hyphen-private-key".to_string()], 10)
            .expect("search nested hyphenated sensitive");
        assert!(hits.is_empty());
    }

    #[test]
    fn source_scan_publisher_indexes_full_source_batch_values() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = http_query_source("fixture");
        let source_scan_indexer = ObservedSourceScanIndexer::spawn(layout.clone());
        let publisher = source_scan_indexer.publisher(workspace.clone(), &[source]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("service", DataType::Utf8, true),
            Field::new("payload", DataType::Utf8, true),
            Field::new("api_token", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["projection-visible"])),
                Arc::new(StringArray::from(vec![
                    r#"{"event":"source-only-json","sha":"source-only-sha"}"#,
                ])),
                Arc::new(StringArray::from(vec!["source-secret-token"])),
            ],
        )
        .expect("batch");

        publisher.publish_source_scan(SourceScanObservation {
            source_name: "fixture",
            surface_kind: SourceObservationSurfaceKind::Table,
            surface_name: "messages",
            observation_scope: SourceObservationScope::MappedRowsBeforeProjection,
            batch: &batch,
        });

        eventually_observed_hit(&layout, &workspace, "source-only-json", |hit| {
            hit.column_name == "payload.event" && hit.source_name == "fixture"
        });
        eventually_observed_hit(&layout, &workspace, "source-only-sha", |hit| {
            hit.column_name == "payload.sha" && hit.source_name == "fixture"
        });

        let store = SearchIndexStore::open_workspace(&layout, &workspace).expect("store");
        drain_observed_queue(&store);
        let hits = store
            .search_observed_values(&workspace, &["source-secret-token".to_string()], 10)
            .expect("search sensitive source value");
        assert!(hits.is_empty());
    }

    #[test]
    fn observed_policy_excludes_configured_columns_and_parent_containers() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        std::fs::write(
            layout.config_file(),
            r#"
version = 1

[search]
observed_value_excluded_columns = [
  "fixture.messages.payload.sha",
  "tags.kube_deployment",
]
"#,
        )
        .expect("write config");
        let config = observed_search_config_or_default(&layout);
        assert_eq!(
            config.search.excluded_columns,
            vec![
                "fixture.messages.payload.sha".to_string(),
                "tags.kube_deployment".to_string(),
            ]
        );
        let policy = ObservedValuePolicy::from_config(&config.search);
        let column_policy = policy.surface_column_policy("fixture", "messages");
        assert!(column_policy.denies_column("payload.sha"));
        assert!(column_policy.denies_column("tags.kube_deployment"));

        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = http_query_source("fixture");
        let schema = Arc::new(Schema::new(vec![
            Field::new("payload", DataType::Utf8, true),
            Field::new("tags", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    r#"{"event":"deploy_ready","sha":"blocked-sha"}"#,
                ])),
                Arc::new(StringArray::from(vec![
                    "service=billing-worker kube_deployment=kube-worker",
                ])),
            ],
        )
        .expect("batch");

        index_fixture_source_scan(&layout, &workspace, source, batch);

        let store = SearchIndexStore::open_workspace(&layout, &workspace).expect("store");
        drain_observed_queue(&store);
        assert!(
            !store
                .search_observed_values(&workspace, &["deploy_ready".to_string()], 10)
                .expect("search allowed payload field")
                .is_empty()
        );
        assert!(
            !store
                .search_observed_values(&workspace, &["billing-worker".to_string()], 10)
                .expect("search allowed tags field")
                .is_empty()
        );
        for excluded_value in ["blocked-sha", "kube-worker"] {
            assert!(
                store
                    .search_observed_values(&workspace, &[excluded_value.to_string()], 10)
                    .expect("search excluded value")
                    .is_empty(),
                "{excluded_value} should not be indexed directly or through a parent container"
            );
        }
    }

    #[test]
    fn source_scan_drain_waits_for_worker_observations() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = http_query_source("fixture");
        let source_scan_indexer = ObservedSourceScanIndexer::spawn(layout.clone());
        let publisher = source_scan_indexer.publisher(workspace.clone(), &[source]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "service",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["drained-source-scan"]))],
        )
        .expect("batch");

        publisher.publish_source_scan(SourceScanObservation {
            source_name: "fixture",
            surface_kind: SourceObservationSurfaceKind::Table,
            surface_name: "messages",
            observation_scope: SourceObservationScope::MappedRowsBeforeProjection,
            batch: &batch,
        });

        assert!(source_scan_indexer.drain_for(StdDuration::from_secs(5)));
        let store = SearchIndexStore::open_workspace(&layout, &workspace).expect("store");
        let drain = store
            .drain_observed_value_queue_for(StdDuration::from_secs(1))
            .expect("drain observed queue");
        assert_eq!(drain.pending_jobs, 0);
        assert!(
            !store
                .search_observed_values(&workspace, &["drained-source-scan".to_string()], 10)
                .expect("search drained source value")
                .is_empty()
        );
    }

    #[test]
    fn source_scan_publisher_drops_when_bounded_channel_is_full() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = http_query_source("fixture");
        let (sender, _paused_receiver) = std::sync::mpsc::sync_channel(1);
        let source_scan_indexer = ObservedSourceScanIndexer {
            layout,
            sender,
            dropped_observations: Arc::new(AtomicU64::new(0)),
        };
        let publisher = source_scan_indexer.publisher(workspace.clone(), &[source]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "service",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["backpressure-value"]))],
        )
        .expect("batch");
        let observation = SourceScanObservation {
            source_name: "fixture",
            surface_kind: SourceObservationSurfaceKind::Table,
            surface_name: "messages",
            observation_scope: SourceObservationScope::MappedRowsBeforeProjection,
            batch: &batch,
        };

        publisher.publish_source_scan(observation);
        assert_eq!(
            source_scan_indexer
                .dropped_observations
                .load(Ordering::Relaxed),
            0
        );
        for _ in 0..8 {
            publisher.publish_source_scan(observation);
        }

        assert_eq!(
            source_scan_indexer
                .dropped_observations
                .load(Ordering::Relaxed),
            8
        );
        assert!(!source_scan_indexer.drain_for(StdDuration::from_millis(10)));
    }

    #[test]
    fn source_scan_publisher_skips_stale_source_generation() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = http_query_source("fixture");
        let source_scan_indexer = ObservedSourceScanIndexer::spawn(layout.clone());
        let publisher = source_scan_indexer.publisher(workspace.clone(), &[source]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "service",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["stale-source-value"]))],
        )
        .expect("batch");
        mark_observed_source_generation(
            &layout,
            &workspace,
            &SourceName::parse("fixture").expect("source"),
        )
        .expect("mark source generation");
        publisher.publish_source_scan(SourceScanObservation {
            source_name: "fixture",
            surface_kind: SourceObservationSurfaceKind::Table,
            surface_name: "messages",
            observation_scope: SourceObservationScope::MappedRowsBeforeProjection,
            batch: &batch,
        });

        for _attempt in 0..16 {
            if let Some(store) =
                SearchIndexStore::open_existing_workspace(&layout, &workspace).expect("open store")
            {
                drain_observed_queue(&store);
                assert!(
                    store
                        .search_observed_values(&workspace, &["stale-source-value".to_string()], 10)
                        .expect("search stale source value")
                        .is_empty()
                );
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }
    }

    #[test]
    fn skips_obvious_pii_and_payment_columns() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = http_query_source("fixture");
        let schema = Arc::new(Schema::new(vec![
            Field::new("ssn", DataType::Utf8, true),
            Field::new("social_security_number", DataType::Utf8, true),
            Field::new("credit_card", DataType::Utf8, true),
            Field::new("card_number", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["123-45-6789"])),
                Arc::new(StringArray::from(vec!["987-65-4321"])),
                Arc::new(StringArray::from(vec!["4111111111111111"])),
                Arc::new(StringArray::from(vec!["5555555555554444"])),
            ],
        )
        .expect("batch");

        index_fixture_source_scan(&layout, &workspace, source, batch);

        let store = SearchIndexStore::open_workspace(&layout, &workspace).expect("store");
        drain_observed_queue(&store);
        for sensitive_value in [
            "123-45-6789",
            "987-65-4321",
            "4111111111111111",
            "5555555555554444",
        ] {
            assert!(
                store
                    .search_observed_values(&workspace, &[sensitive_value.to_string()], 10)
                    .expect("search sensitive value")
                    .is_empty()
            );
        }
    }

    #[test]
    fn skips_raw_credential_strings_in_non_sensitive_columns() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = http_query_source("fixture");
        let schema = Arc::new(Schema::new(vec![
            Field::new("http_response", DataType::Utf8, true),
            Field::new("params", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    "Authorization: Bearer header-secret-token",
                    "status=ok latency_ms=12",
                ])),
                Arc::new(StringArray::from(vec![
                    "access_key=raw-access-key region=us-east-1",
                    "region=us-east-1 status=ok",
                ])),
            ],
        )
        .expect("batch");

        index_fixture_source_scan(&layout, &workspace, source, batch);

        let store = SearchIndexStore::open_workspace(&layout, &workspace).expect("store");
        drain_observed_queue(&store);
        let hits = store
            .search_observed_values(&workspace, &["header-secret-token".to_string()], 10)
            .expect("search raw header secret");
        assert!(hits.is_empty());

        let hits = store
            .search_observed_values(&workspace, &["raw-access-key".to_string()], 10)
            .expect("search raw access key");
        assert!(hits.is_empty());
    }

    #[test]
    fn skips_credential_shaped_values_in_non_sensitive_columns() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = http_query_source("fixture");
        let schema = Arc::new(Schema::new(vec![
            Field::new("author", DataType::Utf8, true),
            Field::new("token_count", DataType::Utf8, true),
            Field::new("session_name", DataType::Utf8, true),
            Field::new("notes", DataType::Utf8, true),
            Field::new("reference", DataType::Utf8, true),
        ]));
        let jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c";
        let long_token = "aB3dE5fG7hI9jK1lM2nO4pQ6rS8tU0vW";
        let private_key =
            "-----BEGIN PRIVATE KEY-----\nPRIVATEKEYPAYLOAD\n-----END PRIVATE KEY-----";
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["Grace Hopper", "Ada Lovelace"])),
                Arc::new(StringArray::from(vec!["42", "17"])),
                Arc::new(StringArray::from(vec![
                    "planning-session",
                    "review-session",
                ])),
                Arc::new(StringArray::from(vec![jwt, "benign release note"])),
                Arc::new(StringArray::from(vec![long_token, private_key])),
            ],
        )
        .expect("batch");

        index_fixture_source_scan(&layout, &workspace, source, batch);

        let store = SearchIndexStore::open_workspace(&layout, &workspace).expect("store");
        drain_observed_queue(&store);
        for benign_value in [
            "Grace Hopper",
            "42",
            "planning-session",
            "benign release note",
        ] {
            assert!(
                !store
                    .search_observed_values(&workspace, &[benign_value.to_string()], 10)
                    .expect("search benign value")
                    .is_empty(),
                "{benign_value} should be indexed"
            );
        }
        for sensitive_value in [
            "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c",
            long_token,
            "PRIVATEKEYPAYLOAD",
        ] {
            assert!(
                store
                    .search_observed_values(&workspace, &[sensitive_value.to_string()], 10)
                    .expect("search sensitive value")
                    .is_empty(),
                "{sensitive_value} should not be indexed"
            );
        }
    }

    #[test]
    fn skips_observation_when_source_generation_changes_before_enqueue() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = http_query_source("fixture");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "service",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["payments-api"]))],
        )
        .expect("batch");
        let indexer = ObservedValueIndexer::new(layout.clone(), workspace.clone(), &[source]);

        mark_observed_source_generation(
            &layout,
            &workspace,
            &SourceName::parse("fixture").expect("source"),
        )
        .expect("mark source generation");
        let observation = OwnedSourceScanObservation {
            source_name: "fixture".to_string(),
            surface_kind: SourceObservationSurfaceKind::Table,
            surface_name: "messages".to_string(),
            observation_scope: SourceObservationScope::MappedRowsBeforeProjection,
            batch,
        };
        indexer
            .index_source_scan_observation_inner(&observation)
            .expect("stale source-scan observation is skipped without failing");

        let store = SearchIndexStore::open_workspace(&layout, &workspace).expect("store");
        drain_observed_queue(&store);
        let hits = store
            .search_observed_values(&workspace, &["payments-api".to_string()], 10)
            .expect("search observed");
        assert!(hits.is_empty());
    }

    #[test]
    fn duplicate_values_are_aggregated_before_storage() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = http_query_source("fixture");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "service",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                "payments-api",
                "payments-api",
                "payments-api",
            ]))],
        )
        .expect("batch");

        index_fixture_source_scan(&layout, &workspace, source, batch);

        let store = SearchIndexStore::open_workspace(&layout, &workspace).expect("store");
        drain_observed_queue(&store);
        let hits = store
            .search_observed_values(&workspace, &["payments-api".to_string()], 10)
            .expect("search observed");
        assert_eq!(hits.len(), 1);
        assert_eq!(
            store
                .observed_count_for_test("payments-api")
                .expect("observed state")
                .expect("observed count"),
            3
        );
    }

    #[test]
    fn clears_catalog_fingerprint_when_building_search_index() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = http_query_source("fixture");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "service",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["payments-api"]))],
        )
        .expect("batch");
        let fingerprint_path = layout
            .search_dir(&workspace)
            .join(CATALOG_FINGERPRINT_FILE_NAME);
        fs::create_dir_all(fingerprint_path.parent().expect("fingerprint parent"))
            .expect("search dir");
        fs::write(&fingerprint_path, "stale-fingerprint\n").expect("fingerprint");

        index_fixture_source_scan(&layout, &workspace, source, batch);

        assert!(!fingerprint_path.exists());
        let store = SearchIndexStore::open_existing_workspace(&layout, &workspace)
            .expect("open existing search index")
            .expect("search index exists");
        drain_observed_queue(&store);
        let hits = store
            .search_observed_values(&workspace, &["payments-api".to_string()], 10)
            .expect("search observed");
        assert!(hits.iter().any(|hit| hit.column_name == "service"));
    }

    #[test]
    fn observed_queue_foreground_drain_budget_loads_search_config() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        std::fs::write(
            layout.config_file(),
            r"
version = 1

[search]
observed_queue_foreground_drain_ms = 250
observed_max_storage_mb = 12
observed_collection_max_candidates = 3
observed_collection_max_candidate_bytes = 42
observed_collection_max_json_depth = 2
",
        )
        .expect("write config");

        assert_eq!(
            observed_queue_foreground_drain_budget(&layout),
            StdDuration::from_millis(250)
        );
        assert_eq!(observed_storage_budget_bytes(&layout), 12 * BYTES_PER_MIB);
        assert!(observed_values_enabled(&layout));

        let config = observed_search_config_or_default(&layout);
        assert_eq!(
            observed_collection_budget_from_config(&config.search),
            ObservedCollectionBudget {
                candidates: 3,
                candidate_bytes: 42,
                json_depth: 2
            }
        );
    }

    #[test]
    fn observed_values_enabled_defaults_on_and_loads_search_config() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");

        assert!(observed_values_enabled(&layout));

        std::fs::write(
            layout.config_file(),
            r"
version = 1

[search]
observed_values_enabled = false
",
        )
        .expect("write config");

        assert!(!observed_values_enabled(&layout));
    }

    #[test]
    fn observed_collection_candidate_budget_flushes_overflow_to_queue() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        std::fs::write(
            layout.config_file(),
            r"
version = 1

[search]
observed_collection_max_candidates = 1
",
        )
        .expect("write config");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = http_query_source("fixture");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "service",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                "budget-first",
                "budget-second",
            ]))],
        )
        .expect("batch");

        index_fixture_source_scan(&layout, &workspace, source, batch);

        let store = SearchIndexStore::open_workspace(&layout, &workspace).expect("store");
        drain_observed_queue(&store);
        assert!(
            !store
                .search_observed_values(&workspace, &["budget-first".to_string()], 10)
                .expect("search first")
                .is_empty()
        );
        assert!(
            !store
                .search_observed_values(&workspace, &["budget-second".to_string()], 10)
                .expect("search second")
                .is_empty()
        );
    }

    #[test]
    fn observed_collection_depth_budget_does_not_index_deep_json_via_container() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        std::fs::write(
            layout.config_file(),
            r"
version = 1

[search]
observed_collection_max_json_depth = 1
",
        )
        .expect("write config");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = http_query_source("fixture");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                r#"{"top":"budget-top","outer":{"inner":"budget-deep"}}"#,
            ]))],
        )
        .expect("batch");

        index_fixture_source_scan(&layout, &workspace, source, batch);

        let store = SearchIndexStore::open_workspace(&layout, &workspace).expect("store");
        drain_observed_queue(&store);
        assert!(
            !store
                .search_observed_values(&workspace, &["budget-top".to_string()], 10)
                .expect("search top value")
                .is_empty()
        );
        assert!(
            store
                .search_observed_values(&workspace, &["budget-deep".to_string()], 10)
                .expect("search deep value")
                .is_empty()
        );
    }

    fn drain_observed_queue(store: &SearchIndexStore) {
        for _attempt in 0..64 {
            let drain = match store.drain_observed_value_queue_for(StdDuration::from_secs(1)) {
                Ok(drain) => drain,
                Err(SearchIndexError::RedbDatabase(redb::DatabaseError::DatabaseAlreadyOpen)) => {
                    std::thread::sleep(StdDuration::from_millis(10));
                    continue;
                }
                Err(error) => panic!("drain observed queue: {error}"),
            };
            if drain.pending_jobs == 0 {
                return;
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }
        panic!("observed queue still has pending jobs after test drain attempts");
    }

    fn index_fixture_source_scan(
        layout: &AppStateLayout,
        workspace: &WorkspaceName,
        source: QuerySource,
        batch: RecordBatch,
    ) {
        let indexer = ObservedValueIndexer::new(layout.clone(), workspace.clone(), &[source]);
        let observation = OwnedSourceScanObservation {
            source_name: "fixture".to_string(),
            surface_kind: SourceObservationSurfaceKind::Table,
            surface_name: "messages".to_string(),
            observation_scope: SourceObservationScope::MappedRowsBeforeProjection,
            batch,
        };
        indexer
            .index_source_scan_observation_inner(&observation)
            .expect("source-scan observed-value indexing succeeds");
    }

    fn eventually_observed_hit(
        layout: &AppStateLayout,
        workspace: &WorkspaceName,
        term: &str,
        predicate: impl Fn(&ObservedValueSearchHit) -> bool,
    ) {
        for _attempt in 0..64 {
            if let Some(store) =
                SearchIndexStore::open_existing_workspace(layout, workspace).expect("open store")
            {
                drain_observed_queue(&store);
                let hits = store
                    .search_observed_values(workspace, &[term.to_string()], 10)
                    .expect("search observed values");
                if hits.iter().any(&predicate) {
                    return;
                }
            }
            std::thread::sleep(StdDuration::from_millis(10));
        }
        panic!("expected observed hit for {term}");
    }

    fn http_query_source(name: &str) -> QuerySource {
        let manifest = parse_source_manifest_value(json!({
            "name": name,
            "version": "0.1.0",
            "dsl_version": 3,
            "backend": "http",
            "base_url": "https://example.com",
            "tables": [{
                "name": "messages",
                "description": "Messages fixture",
                "request": {
                    "path": "/messages"
                },
                "response": {
                    "rows_path": []
                },
                "columns": [
                    {"name": "service", "type": "Utf8"},
                    {"name": "body", "type": "Utf8"},
                    {"name": "payload", "type": "Utf8"},
                    {"name": "tags", "type": "Utf8"},
                    {"name": "http_response", "type": "Utf8"},
                    {"name": "params", "type": "Utf8"},
                    {"name": "author", "type": "Utf8"},
                    {"name": "token_count", "type": "Utf8"},
                    {"name": "session_name", "type": "Utf8"},
                    {"name": "notes", "type": "Utf8"},
                    {"name": "reference", "type": "Utf8"},
                    {"name": "api_token", "type": "Utf8"},
                    {"name": "privateKey", "type": "Utf8"},
                    {"name": "ssn", "type": "Utf8"},
                    {"name": "social_security_number", "type": "Utf8"},
                    {"name": "credit_card", "type": "Utf8"},
                    {"name": "card_number", "type": "Utf8"}
                ]
            }]
        }))
        .expect("manifest");
        QuerySource::new(manifest, BTreeMap::new(), BTreeMap::new())
    }
}
