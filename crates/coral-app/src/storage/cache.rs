//! Query-result and metadata cache for short-lived and persistent reuse.

use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use coral_engine::{CatalogInfo, QueryExecution, QueryPlan, SourceValidationReport};
use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

use crate::bootstrap::AppError;
use crate::state::{AppStateLayout, CacheConfig};
use crate::storage::fs as storage_fs;

#[derive(Debug, Clone)]
pub(crate) struct CacheSettings {
    ttl_seconds: Option<u64>,
    persistent: bool,
    max_entries: usize,
}

impl From<CacheConfig> for CacheSettings {
    fn from(value: CacheConfig) -> Self {
        Self {
            ttl_seconds: value.ttl_seconds(),
            persistent: value.persistent(),
            max_entries: value.max_entries(),
        }
    }
}

impl CacheSettings {
    fn is_enabled(&self) -> bool {
        self.ttl_seconds.is_some_and(|ttl| ttl > 0)
    }

    fn ttl(&self) -> Duration {
        Duration::from_secs(self.ttl_seconds.unwrap_or(0))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct QueryCache {
    layout: AppStateLayout,
    settings: CacheSettings,
    memory: Arc<Mutex<MemoryCache>>,
}

impl QueryCache {
    pub(crate) fn new(layout: AppStateLayout, settings: CacheSettings) -> Self {
        let memory = MemoryCache::new(settings.max_entries);
        Self {
            layout,
            settings,
            memory: Arc::new(Mutex::new(memory)),
        }
    }

    pub(crate) fn clear_all(&self) -> Result<(), AppError> {
        self.memory
            .lock()
            .expect("cache lock poisoned during clear")
            .clear();
        let dir = self.layout.cache_entries_dir();
        if dir.exists() {
            fs::remove_dir_all(dir)?;
        }
        Ok(())
    }

    pub(crate) fn clear_source(&self, source_name: &str) -> Result<(), AppError> {
        self.memory
            .lock()
            .expect("cache lock poisoned during clear")
            .retain(|entry| !entry.source_names.iter().any(|name| name == source_name));
        self.clear_persistent_source(source_name)?;
        Ok(())
    }

    pub(crate) fn get_query_execution(&self, key: &str) -> Result<Option<QueryExecution>, AppError> {
        let Some(entry) = self.lookup_entry(key, CacheValueKind::QueryExecution)? else {
            return Ok(None);
        };
        match STANDARD
            .decode(&entry.payload)
            .ok()
            .and_then(|payload| decode_query_execution_ipc(&payload).ok())
        {
            Some(execution) => Ok(Some(execution)),
            None => {
                self.evict_corrupt_entry(key);
                Ok(None)
            }
        }
    }

    pub(crate) fn put_query_execution(
        &self,
        key: &str,
        scope: CacheScope,
        execution: &QueryExecution,
    ) -> Result<(), AppError> {
        let mut bytes = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut bytes, execution.arrow_schema())?;
            for batch in execution.batches() {
                writer.write(batch)?;
            }
            writer.finish()?;
        }
        self.put_raw(
            key,
            scope,
            CacheValueKind::QueryExecution,
            bytes,
        )
    }

    pub(crate) fn get_json<T: DeserializeOwned>(
        &self,
        key: &str,
        kind: CacheValueKind,
    ) -> Result<Option<T>, AppError> {
        self.get_typed::<T>(key, kind)
    }

    pub(crate) fn put_json<T: Serialize>(
        &self,
        key: &str,
        scope: CacheScope,
        kind: CacheValueKind,
        value: &T,
    ) -> Result<(), AppError> {
        let bytes = serde_json::to_vec(value)?;
        self.put_raw(key, scope, kind, bytes)
    }

    fn put_raw(
        &self,
        key: &str,
        scope: CacheScope,
        kind: CacheValueKind,
        payload: Vec<u8>,
    ) -> Result<(), AppError> {
        if !self.settings.is_enabled() {
            return Ok(());
        }
        let entry = StoredCacheEntry::new(key, scope, kind, payload, self.settings.ttl());
        self.memory
            .lock()
            .expect("cache lock poisoned during put")
            .put(entry.clone());
        if self.settings.persistent {
            self.write_entry(&entry)?;
        }
        Ok(())
    }

    fn get_typed<T: DeserializeOwned>(
        &self,
        key: &str,
        kind: CacheValueKind,
    ) -> Result<Option<T>, AppError> {
        if !self.settings.is_enabled() {
            return Ok(None);
        }

        let Some(entry) = self.lookup_entry(key, kind)? else {
            return Ok(None);
        };
        match STANDARD
            .decode(&entry.payload)
            .ok()
            .and_then(|payload| serde_json::from_slice(&payload).ok())
        {
            Some(value) => Ok(Some(value)),
            None => {
                self.evict_corrupt_entry(key);
                Ok(None)
            }
        }
    }

    fn lookup_entry(
        &self,
        key: &str,
        kind: CacheValueKind,
    ) -> Result<Option<StoredCacheEntry>, AppError> {
        if !self.settings.is_enabled() {
            return Ok(None);
        }

        if let Some(entry) = self
            .memory
            .lock()
            .expect("cache lock poisoned during get")
            .get(key)
            .filter(|entry| entry.kind == kind)
            .cloned()
        {
            if entry.is_expired() {
                self.memory
                    .lock()
                    .expect("cache lock poisoned during expiry cleanup")
                    .remove(key);
                return Ok(None);
            }
            return Ok(Some(entry));
        }

        if !self.settings.persistent {
            return Ok(None);
        }

        let Some(entry) = self.read_entry(key)? else {
            return Ok(None);
        };
        if entry.kind != kind {
            return Ok(None);
        }
        if entry.is_expired() {
            self.remove_entry_file(key)?;
            return Ok(None);
        }
        self.memory
            .lock()
            .expect("cache lock poisoned during disk load")
            .put(entry.clone());
        Ok(Some(entry))
    }

    fn evict_corrupt_entry(&self, key: &str) {
        self.memory
            .lock()
            .expect("cache lock poisoned during corruption eviction")
            .remove(key);
        let _ = self.remove_entry_file(key);
    }

    fn cache_dir(&self) -> PathBuf {
        self.layout.cache_entries_dir()
    }

    fn entry_path(&self, key: &str) -> PathBuf {
        self.cache_dir().join(format!("{key}.json"))
    }

    fn read_entry(&self, key: &str) -> Result<Option<StoredCacheEntry>, AppError> {
        let path = self.entry_path(key);
        if !path.exists() {
            return Ok(None);
        }
        let raw = fs::read_to_string(path)?;
        Ok(Some(serde_json::from_str(&raw)?))
    }

    fn write_entry(&self, entry: &StoredCacheEntry) -> Result<(), AppError> {
        let dir = self.cache_dir();
        storage_fs::ensure_dir(&dir)?;
        let path = self.entry_path(&entry.key);
        let raw = serde_json::to_vec_pretty(entry)?;
        storage_fs::write_atomic(&path, &raw)?;
        Ok(())
    }

    fn remove_entry_file(&self, key: &str) -> Result<(), AppError> {
        let path = self.entry_path(key);
        if path.exists() {
            fs::remove_file(path)?;
        }
        Ok(())
    }

    fn clear_persistent_source(&self, source_name: &str) -> Result<(), AppError> {
        let dir = self.cache_dir();
        if !dir.exists() {
            return Ok(());
        }
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let raw = fs::read_to_string(entry.path())?;
            let stored: StoredCacheEntry = serde_json::from_str(&raw)?;
            if stored.source_names.iter().any(|name| name == source_name) {
                fs::remove_file(entry.path())?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CacheScope {
    workspace_name: String,
    source_names: Vec<String>,
}

impl CacheScope {
    pub(crate) fn new(workspace_name: impl Into<String>, source_names: Vec<String>) -> Self {
        Self {
            workspace_name: workspace_name.into(),
            source_names,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum CacheValueKind {
    QueryExecution,
    QueryPlan,
    CatalogInfo,
    TableInfoList,
    SourceValidationReport,
}

impl CacheValueKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::QueryExecution => "query_execution",
            Self::QueryPlan => "query_plan",
            Self::CatalogInfo => "catalog_info",
            Self::TableInfoList => "table_info_list",
            Self::SourceValidationReport => "source_validation_report",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredCacheEntry {
    key: String,
    kind: CacheValueKind,
    workspace_name: String,
    source_names: Vec<String>,
    created_at_unix_seconds: u64,
    expires_at_unix_seconds: Option<u64>,
    payload: String,
}

impl StoredCacheEntry {
    fn new(
        key: &str,
        scope: CacheScope,
        kind: CacheValueKind,
        payload: Vec<u8>,
        ttl: Duration,
    ) -> Self {
        let created_at_unix_seconds = current_unix_seconds();
        let expires_at_unix_seconds = ttl.as_secs().checked_add(created_at_unix_seconds);
        Self {
            key: key.to_string(),
            kind,
            workspace_name: scope.workspace_name,
            source_names: scope.source_names,
            created_at_unix_seconds,
            expires_at_unix_seconds,
            payload: STANDARD.encode(payload),
        }
    }

    fn is_expired(&self) -> bool {
        self.expires_at_unix_seconds
            .is_some_and(|expires_at| current_unix_seconds() >= expires_at)
    }

}

#[derive(Debug, Clone)]
struct MemoryCache {
    capacity: usize,
    entries: HashMap<String, StoredCacheEntry>,
    order: VecDeque<String>,
}

impl MemoryCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
    }

    fn retain(&mut self, mut keep: impl FnMut(&StoredCacheEntry) -> bool) {
        self.entries.retain(|key, entry| {
            let retain = keep(entry);
            if !retain {
                self.order.retain(|candidate| candidate != key);
            }
            retain
        });
    }

    fn remove(&mut self, key: &str) {
        self.entries.remove(key);
        self.order.retain(|candidate| candidate != key);
    }

    fn get(&mut self, key: &str) -> Option<&StoredCacheEntry> {
        if self.entries.contains_key(key) {
            self.order.retain(|candidate| candidate != key);
            self.order.push_back(key.to_string());
        }
        self.entries.get(key)
    }

    fn put(&mut self, entry: StoredCacheEntry) {
        if self.capacity == 0 {
            return;
        }
        self.order.retain(|candidate| candidate != &entry.key);
        let entry_key = entry.key.clone();
        self.entries.insert(entry.key.clone(), entry);
        self.order.push_back(entry_key);
        while self.entries.len() > self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.entries.remove(&oldest);
            }
        }
    }
}

fn current_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn decode_query_execution_ipc(bytes: &[u8]) -> Result<QueryExecution, arrow::error::ArrowError> {
    let cursor = Cursor::new(bytes);
    let mut reader = StreamReader::try_new(cursor, None)?;
    let arrow_schema = Arc::new(reader.schema().as_ref().clone());
    let mut batches = Vec::new();
    for batch in &mut reader {
        batches.push(batch?);
    }
    Ok(QueryExecution::new(arrow_schema, batches))
}

pub(crate) fn query_cache_key(prefix: &str, fingerprint: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(prefix.as_bytes());
    hasher.update(b":");
    hasher.update(fingerprint.as_bytes());
    format!("{:x}", hasher.finalize())
}

pub(crate) fn normalize_cache_key_input(parts: &[&str]) -> String {
    parts
        .iter()
        .map(|part| part.trim())
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("|")
}

pub(crate) fn encode_query_execution(
    schema: &arrow::datatypes::SchemaRef,
    batches: &[RecordBatch],
) -> Result<Vec<u8>, arrow::error::ArrowError> {
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, schema)?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()?;
    }
    Ok(bytes)
}

pub(crate) fn cache_settings_from_config(config: CacheConfig) -> CacheSettings {
    CacheSettings::from(config)
}

pub(crate) fn cache_scope(workspace_name: impl Into<String>, source_names: Vec<String>) -> CacheScope {
    CacheScope::new(workspace_name, source_names)
}

pub(crate) fn cache_fingerprint_digest(parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update(b"\0");
    }
    format!("{:x}", hasher.finalize())
}

pub(crate) fn cache_value_kind_from_query_operation(_operation: &str) -> CacheValueKind {
    CacheValueKind::QueryExecution
}

#[allow(dead_code)]
fn _keep_contract_types_used(_: (&CatalogInfo, &QueryPlan, &SourceValidationReport)) {}
