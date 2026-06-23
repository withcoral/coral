//! Workspace-scoped Tantivy storage for Universal Search retrieval.

use std::collections::BTreeMap as StdBTreeMap;
use std::fs;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, TryLockError};
use std::time::{Duration as StdDuration, Instant as StdInstant};

use chrono::{SecondsFormat, Utc};
use coral_engine::{
    CatalogInfo, ColumnInfo, TableFilterInfo, TableFunctionArgumentInfo, TableFunctionInfo,
    TableFunctionResultColumnInfo, TableInfo,
};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, BoostQuery, Occur, Query, QueryParser, RangeQuery, TermQuery};
use tantivy::schema::{
    Field, IndexRecordOption, STORED, STRING, Schema, TextFieldIndexing, TextOptions, Value as _,
};
use tantivy::tokenizer::{LowerCaser, NgramTokenizer, TextAnalyzer};
use tantivy::{Index, IndexWriter, ReloadPolicy, TantivyDocument, Term};
use uuid::Uuid;

use crate::sources::SourceName;
use crate::state::AppStateLayout;
use crate::storage::fs::ensure_dir;
use crate::workspaces::WorkspaceName;

const OBSERVED_STATE_FILE_NAME: &str = "observed_values.redb";
const OBSERVED_RECORDS_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("observed_records_v1");
const OBSERVED_SOURCE_INDEX_TABLE: TableDefinition<&str, &str> =
    TableDefinition::new("observed_source_index");
const OBSERVED_LAST_OBSERVED_INDEX_TABLE: TableDefinition<&str, &str> =
    TableDefinition::new("observed_last_observed_index");
const OBSERVED_QUEUE_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("observed_queue_v1");
const TRIGRAM_TOKENIZER: &str = "coral_trigram";
const TANTIVY_VERSION: &str = "0.26.1";
const WRITER_MEMORY_BUDGET_BYTES: usize = 50_000_000;
const MAX_RANK_SCORE: u32 = 1_000;
const OBSERVED_RECORD_ENCODING_RAW: u8 = 0;
const OBSERVED_RECORD_ENCODING_ZSTD: u8 = 1;
const OBSERVED_RECORD_ZSTD_LEVEL: i32 = 3;
const OBSERVED_RECORD_ZSTD_LENGTH_BYTES: usize = 8;
const OBSERVED_QUEUE_JOB_RECORD_LIMIT: usize = 256;

static INDEX_MUTATION_LOCKS: OnceLock<Mutex<StdBTreeMap<PathBuf, Arc<Mutex<()>>>>> =
    OnceLock::new();

#[derive(Debug, Clone)]
pub(crate) struct SearchIndexStore {
    path: PathBuf,
    index: Index,
    fields: SearchIndexFields,
    capabilities: TantivySearchCapabilities,
    mutation_lock: Arc<Mutex<()>>,
}

impl SearchIndexStore {
    pub(crate) fn replace_workspace_catalog(
        layout: &AppStateLayout,
        workspace_name: &WorkspaceName,
        catalog: &CatalogInfo,
    ) -> Result<Self, SearchIndexError> {
        Self::replace_catalog_index(layout.search_index_dir(workspace_name), catalog)
    }

    pub(crate) fn replace_catalog_index(
        path: impl Into<PathBuf>,
        catalog: &CatalogInfo,
    ) -> Result<Self, SearchIndexError> {
        let path = path.into();
        replace_catalog_index_at(&path, catalog)?;
        Self::open(path)
    }

    pub(crate) fn open_workspace(
        layout: &AppStateLayout,
        workspace_name: &WorkspaceName,
    ) -> Result<Self, SearchIndexError> {
        Self::open(layout.search_index_dir(workspace_name))
    }

    pub(crate) fn workspace_index_is_usable(
        layout: &AppStateLayout,
        workspace_name: &WorkspaceName,
    ) -> bool {
        index_is_usable(&layout.search_index_dir(workspace_name))
    }

    pub(crate) fn open_existing_workspace(
        layout: &AppStateLayout,
        workspace_name: &WorkspaceName,
    ) -> Result<Option<Self>, SearchIndexError> {
        let path = layout.search_index_dir(workspace_name);
        let Some(index) = open_existing_index(&path)? else {
            return Ok(None);
        };
        Self::from_index(&path, index).map(Some)
    }

    pub(crate) fn discard_observed_values_for_source(
        layout: &AppStateLayout,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<(), SearchIndexError> {
        let path = layout.search_index_dir(workspace_name);
        let mutation_lock = index_mutation_lock(&path);
        let _guard = mutation_lock
            .lock()
            .map_err(|_poisoned| SearchIndexError::MutationLockPoisoned)?;

        remove_observed_queue_records_for_source_at(&path, source_name.as_str())?;
        let removed = remove_observed_records_for_source_at(&path, source_name.as_str())?;
        if removed.is_empty() {
            return Ok(());
        }

        let index = match open_existing_index(&path) {
            Ok(Some(index)) => index,
            Ok(None) => return Ok(()),
            Err(error) => {
                tracing::warn!(
                    workspace = %workspace_name,
                    source = %source_name,
                    path = %path.display(),
                    error = %error,
                    "deleted durable observed values but could not open Tantivy projection"
                );
                return Ok(());
            }
        };
        let store = Self::from_index(&path, index)?;
        store.delete_observed_projection_records(&removed)
    }

    pub(crate) fn discard_observed_values(
        layout: &AppStateLayout,
        workspace_name: &WorkspaceName,
    ) -> Result<(), SearchIndexError> {
        let path = layout.search_index_dir(workspace_name);
        let mutation_lock = index_mutation_lock(&path);
        let _guard = mutation_lock
            .lock()
            .map_err(|_poisoned| SearchIndexError::MutationLockPoisoned)?;

        match fs::remove_file(observed_state_file_for_index(&path)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }

        let Some(index) = open_existing_index(&path)? else {
            return Ok(());
        };
        let store = Self::from_index(&path, index)?;
        store.delete_observed_projection_documents()
    }

    pub(crate) fn open(path: impl Into<PathBuf>) -> Result<Self, SearchIndexError> {
        let path = path.into();
        ensure_dir(&path)?;

        let index = open_or_create_index(&path)?;
        Self::from_index(&path, index)
    }

    fn from_index(path: &Path, index: Index) -> Result<Self, SearchIndexError> {
        register_tokenizers(&index)?;
        let fields = SearchIndexFields::from_schema(&index.schema())?;
        Ok(Self {
            path: path.to_path_buf(),
            index,
            fields,
            capabilities: TantivySearchCapabilities {
                tantivy_version: TANTIVY_VERSION.to_string(),
                tokenizer: TRIGRAM_TOKENIZER.to_string(),
            },
            mutation_lock: index_mutation_lock(path),
        })
    }

    #[cfg(test)]
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn capabilities(&self) -> &TantivySearchCapabilities {
        &self.capabilities
    }

    #[cfg(test)]
    pub(crate) fn search_catalog(
        &self,
        workspace_name: &WorkspaceName,
        terms: &[String],
        limit: usize,
    ) -> Result<Vec<CatalogSearchHit>, SearchIndexError> {
        Ok(self.search_catalog_page(workspace_name, terms, limit)?.hits)
    }

    pub(crate) fn search_catalog_page(
        &self,
        _workspace_name: &WorkspaceName,
        terms: &[String],
        limit: usize,
    ) -> Result<CatalogSearchPage, SearchIndexError> {
        let Some(query) = self.scoped_query("catalog", terms) else {
            return Ok(CatalogSearchPage {
                hits: Vec::new(),
                has_more: false,
            });
        };
        let mut docs = self.search_documents(&query, limit.saturating_add(1))?;
        let has_more = docs.len() > limit;
        docs.truncate(limit);
        let mut hits = docs
            .into_iter()
            .filter(|doc| doc_text(doc, self.fields.entity_kind) == "catalog")
            .map(|doc| CatalogSearchHit {
                entity_key: doc_text(&doc, self.fields.doc_key),
                result_type: CatalogSearchResultType::from_str(&doc_text(
                    &doc,
                    self.fields.result_type,
                )),
                surface_kind: CatalogSearchSurfaceKind::from_str(&doc_text(
                    &doc,
                    self.fields.surface_kind,
                )),
                field_role: CatalogSearchFieldRole::from_str(&doc_text(
                    &doc,
                    self.fields.field_role,
                )),
                schema_name: doc_text(&doc, self.fields.schema_name),
                surface_name: doc_text(&doc, self.fields.surface_name),
                name: doc_text(&doc, self.fields.name),
                data_type: doc_text(&doc, self.fields.data_type),
                required: doc_text(&doc, self.fields.required) == "true",
                description: doc_text(&doc, self.fields.description),
                matched_fields: matched_fields(
                    terms,
                    [
                        ("name", doc_text(&doc, self.fields.name).as_str()),
                        (
                            "qualified_name",
                            doc_text(&doc, self.fields.qualified_name).as_str(),
                        ),
                        (
                            "description",
                            doc_text(&doc, self.fields.description).as_str(),
                        ),
                        (
                            "searchable_text",
                            doc_text(&doc, self.fields.searchable_text).as_str(),
                        ),
                    ],
                ),
                score: 0,
            })
            .collect::<Vec<_>>();
        assign_rank_scores(&mut hits, |hit, score| hit.score = score);
        Ok(CatalogSearchPage { hits, has_more })
    }

    #[cfg(test)]
    pub(crate) fn upsert_observed_values(
        &self,
        _workspace_name: &WorkspaceName,
        records: Vec<ObservedValueRecord>,
    ) -> Result<(), SearchIndexError> {
        if records.is_empty() {
            return Ok(());
        }

        let _guard = self
            .mutation_lock
            .lock()
            .map_err(|_poisoned| SearchIndexError::MutationLockPoisoned)?;
        let mut writer = self.writer()?;
        let updated = self.upsert_observed_records(records)?;
        for record in &updated {
            let key = record.doc_key();
            let _opstamp = writer.delete_term(Term::from_field_text(self.fields.doc_key, &key));
            writer.add_document(self.observed_document(record))?;
        }
        writer.commit()?;
        Ok(())
    }

    pub(crate) fn enqueue_observed_values(
        &self,
        _workspace_name: &WorkspaceName,
        records: Vec<ObservedValueRecord>,
    ) -> Result<(), SearchIndexError> {
        if records.is_empty() {
            return Ok(());
        }

        let database = self.observed_database()?;
        let write_txn = database.begin_write()?;
        {
            let mut queue = write_txn.open_table(OBSERVED_QUEUE_TABLE)?;
            let mut chunk = Vec::with_capacity(OBSERVED_QUEUE_JOB_RECORD_LIMIT);
            for record in records {
                chunk.push(ObservedValueQueuedRecord::from(record));
                if chunk.len() < OBSERVED_QUEUE_JOB_RECORD_LIMIT {
                    continue;
                }
                let job = ObservedValueQueueJob {
                    state: ObservedValueQueueJobState::Pending {
                        records: std::mem::take(&mut chunk),
                    },
                    created_at: now_timestamp(),
                    last_attempt_at: None,
                    attempts: 0,
                };
                let encoded = encode_observed_queue_job(&job)?;
                let key = observed_queue_job_key();
                queue.insert(key.as_str(), encoded.as_slice())?;
            }
            if !chunk.is_empty() {
                let job = ObservedValueQueueJob {
                    state: ObservedValueQueueJobState::Pending { records: chunk },
                    created_at: now_timestamp(),
                    last_attempt_at: None,
                    attempts: 0,
                };
                let encoded = encode_observed_queue_job(&job)?;
                let key = observed_queue_job_key();
                queue.insert(key.as_str(), encoded.as_slice())?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    pub(crate) fn drain_observed_value_queue_for(
        &self,
        budget: StdDuration,
    ) -> Result<ObservedQueueDrain, SearchIndexError> {
        let _guard = match self.mutation_lock.try_lock() {
            Ok(guard) => guard,
            Err(TryLockError::WouldBlock) => {
                let pending_jobs = self.observed_queue_job_count()?;
                return Ok(ObservedQueueDrain {
                    processed_jobs: 0,
                    pending_jobs,
                    budget_exhausted: pending_jobs > 0,
                    lock_busy: true,
                    storage_budget_exceeded: false,
                });
            }
            Err(TryLockError::Poisoned(_poisoned)) => {
                return Err(SearchIndexError::MutationLockPoisoned);
            }
        };

        if budget.is_zero() {
            let pending_jobs = self.observed_queue_job_count()?;
            return Ok(ObservedQueueDrain {
                processed_jobs: 0,
                pending_jobs,
                budget_exhausted: pending_jobs > 0,
                lock_busy: false,
                storage_budget_exceeded: false,
            });
        }

        let started_at = StdInstant::now();
        let mut processed_jobs = 0;
        while started_at.elapsed() < budget {
            let Some(prepared) = self.prepare_next_observed_queue_job()? else {
                return Ok(ObservedQueueDrain {
                    processed_jobs,
                    pending_jobs: 0,
                    budget_exhausted: false,
                    lock_busy: false,
                    storage_budget_exceeded: false,
                });
            };

            let mut writer = self.writer()?;
            for record in &prepared.records {
                let key = record.doc_key();
                let _opstamp = writer.delete_term(Term::from_field_text(self.fields.doc_key, &key));
                writer.add_document(self.observed_document(record))?;
            }
            writer.commit()?;
            self.remove_observed_queue_job(&prepared.key)?;
            processed_jobs += 1;
        }

        let pending_jobs = self.observed_queue_job_count()?;
        Ok(ObservedQueueDrain {
            processed_jobs,
            pending_jobs,
            budget_exhausted: pending_jobs > 0,
            lock_busy: false,
            storage_budget_exceeded: false,
        })
    }

    pub(crate) fn observed_storage_bytes(&self) -> Result<u64, SearchIndexError> {
        Ok(observed_database_payload_bytes_for_index(&self.path)?
            .saturating_add(observed_search_tree_file_bytes_for_index(&self.path)?))
    }

    pub(crate) fn enforce_observed_storage_budget(
        &self,
        max_bytes: u64,
    ) -> Result<ObservedStorageBudgetEnforcement, SearchIndexError> {
        let _guard = self
            .mutation_lock
            .lock()
            .map_err(|_poisoned| SearchIndexError::MutationLockPoisoned)?;
        let mut storage_bytes = self.observed_storage_bytes()?;
        let mut removed_queue_jobs = 0;
        let mut removed_records = Vec::new();
        let mut removed_record_count = 0;

        while storage_bytes > max_bytes {
            if remove_oldest_observed_queue_job_at(&self.path)? {
                removed_queue_jobs += 1;
                storage_bytes = self.observed_storage_bytes()?;
                continue;
            }

            let Some(record) = remove_oldest_observed_record_at(&self.path)? else {
                break;
            };
            removed_record_count += 1;
            if let RemovedObservedRecord::Decoded(record) = record {
                removed_records.push(*record);
            }
            storage_bytes = self.observed_storage_bytes()?;
        }

        self.delete_observed_projection_records(&removed_records)?;
        storage_bytes = self.observed_storage_bytes()?;
        Ok(ObservedStorageBudgetEnforcement {
            removed_queue_jobs,
            removed_records: removed_record_count,
            storage_bytes,
            budget_exceeded: storage_bytes > max_bytes,
        })
    }

    #[cfg(test)]
    pub(crate) fn search_observed_values(
        &self,
        _workspace_name: &WorkspaceName,
        terms: &[String],
        limit: usize,
    ) -> Result<Vec<ObservedValueSearchHit>, SearchIndexError> {
        let Some(query) = self.scoped_query("observed_value", terms) else {
            return Ok(Vec::new());
        };
        let docs = self.search_documents(&query, limit)?;
        let mut hits = self.observed_hits_from_documents(docs);
        assign_rank_scores(&mut hits, |hit, score| hit.score = score);
        Ok(hits)
    }

    pub(crate) fn search_observed_values_filtered(
        &self,
        terms: &[String],
        limit: usize,
        live_source_scopes: &StdBTreeMap<String, String>,
        staleness_cutoff: &str,
    ) -> Result<Vec<ObservedValueSearchHit>, SearchIndexError> {
        if live_source_scopes.is_empty() {
            return Ok(Vec::new());
        }
        let Some(query) = self.scoped_query("observed_value", terms) else {
            return Ok(Vec::new());
        };
        let source_query = self.observed_live_source_query(live_source_scopes);
        let staleness_query = Box::new(RangeQuery::new(
            Bound::Included(Term::from_field_text(
                self.fields.last_observed_at,
                staleness_cutoff,
            )),
            Bound::Unbounded,
        ));
        let query = BooleanQuery::new(vec![
            (Occur::Must, query),
            (Occur::Must, source_query),
            (Occur::Must, staleness_query),
        ]);
        let docs = self.search_documents(&query, limit)?;
        let mut hits = self.observed_hits_from_documents(docs);
        assign_rank_scores(&mut hits, |hit, score| hit.score = score);
        Ok(hits)
    }

    fn observed_live_source_query(
        &self,
        live_source_scopes: &StdBTreeMap<String, String>,
    ) -> Box<dyn Query> {
        let mut clauses = live_source_scopes
            .iter()
            .map(|(source_name, source_scope_id)| {
                (
                    Occur::Should,
                    Box::new(BooleanQuery::new(vec![
                        (
                            Occur::Must,
                            Box::new(TermQuery::new(
                                Term::from_field_text(self.fields.source_name, source_name),
                                IndexRecordOption::Basic,
                            )) as Box<dyn Query>,
                        ),
                        (
                            Occur::Must,
                            Box::new(TermQuery::new(
                                Term::from_field_text(self.fields.source_scope_id, source_scope_id),
                                IndexRecordOption::Basic,
                            )) as Box<dyn Query>,
                        ),
                    ])) as Box<dyn Query>,
                )
            })
            .collect::<Vec<_>>();
        if clauses.len() == 1 {
            return clauses.pop().expect("one source query").1;
        }
        Box::new(BooleanQuery::new(clauses))
    }

    fn observed_hits_from_documents(
        &self,
        docs: Vec<TantivyDocument>,
    ) -> Vec<ObservedValueSearchHit> {
        docs.into_iter()
            .filter(|doc| doc_text(doc, self.fields.entity_kind) == "observed_value")
            .map(|doc| ObservedValueSearchHit {
                source_name: doc_text(&doc, self.fields.source_name),
                surface_kind: ObservedValueSurfaceKind::from_str(&doc_text(
                    &doc,
                    self.fields.surface_kind,
                ))
                .unwrap_or(ObservedValueSurfaceKind::Table),
                surface_name: doc_text(&doc, self.fields.surface_name),
                column_name: doc_text(&doc, self.fields.column_name),
                normalized_value_key: doc_text(&doc, self.fields.normalized_value_key),
                display_value: doc_text(&doc, self.fields.display_value),
                last_observed_at: doc_text(&doc, self.fields.last_observed_at),
                score: 0,
            })
            .collect::<Vec<_>>()
    }

    #[cfg(test)]
    pub(crate) fn delete_observed_values_for_source(
        &self,
        _workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<(), SearchIndexError> {
        let _guard = self
            .mutation_lock
            .lock()
            .map_err(|_poisoned| SearchIndexError::MutationLockPoisoned)?;
        self.remove_observed_queue_records_for_source(source_name.as_str())?;
        let removed = self.remove_observed_records_for_source(source_name.as_str())?;
        self.delete_observed_projection_records(&removed)
    }

    pub(crate) fn purge_observed_values_before(
        &self,
        cutoff: &str,
    ) -> Result<(), SearchIndexError> {
        let _guard = self
            .mutation_lock
            .lock()
            .map_err(|_poisoned| SearchIndexError::MutationLockPoisoned)?;
        let removed = self.remove_observed_records_before(cutoff)?;
        self.delete_observed_projection_records(&removed)
    }

    fn delete_observed_projection_records(
        &self,
        removed: &[ObservedValueStoredRecord],
    ) -> Result<(), SearchIndexError> {
        if removed.is_empty() {
            return Ok(());
        }
        let mut writer = self.writer()?;
        for record in removed {
            let _opstamp = writer.delete_term(Term::from_field_text(
                self.fields.doc_key,
                &record.doc_key(),
            ));
        }
        writer.commit()?;
        Ok(())
    }

    fn delete_observed_projection_documents(&self) -> Result<(), SearchIndexError> {
        let mut writer = self.writer()?;
        writer.delete_query(Box::new(TermQuery::new(
            Term::from_field_text(self.fields.entity_kind, "observed_value"),
            IndexRecordOption::Basic,
        )))?;
        writer.commit()?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn observed_count_for_test(
        &self,
        display_value: &str,
    ) -> Result<Option<u64>, SearchIndexError> {
        Ok(self
            .load_observed_records()?
            .into_iter()
            .find(|record| record.display_value == display_value)
            .map(|record| record.observed_count))
    }

    fn writer(&self) -> Result<IndexWriter, SearchIndexError> {
        Ok(self.index.writer(WRITER_MEMORY_BUDGET_BYTES)?)
    }

    fn scoped_query(&self, entity_kind: &'static str, terms: &[String]) -> Option<Box<dyn Query>> {
        let content_query = self.catalog_content_query(entity_kind, terms)?;
        let kind_query = Box::new(TermQuery::new(
            Term::from_field_text(self.fields.entity_kind, entity_kind),
            IndexRecordOption::Basic,
        ));
        Some(Box::new(BooleanQuery::new(vec![
            (Occur::Must, kind_query),
            (Occur::Must, content_query),
        ])))
    }

    fn catalog_content_query(
        &self,
        entity_kind: &'static str,
        terms: &[String],
    ) -> Option<Box<dyn Query>> {
        let mut clauses = Vec::<(Occur, Box<dyn Query>)>::new();
        if let Some(query_text) = tantivy_query_text(terms) {
            let mut parser = QueryParser::for_index(
                &self.index,
                vec![
                    self.fields.name_text,
                    self.fields.qualified_name_text,
                    self.fields.description_text,
                    self.fields.searchable_text_text,
                    self.fields.value_text,
                ],
            );
            parser.set_field_boost(self.fields.name_text, 4.0);
            parser.set_field_boost(self.fields.qualified_name_text, 5.0);
            parser.set_field_boost(self.fields.description_text, 2.0);
            parser.set_field_boost(self.fields.value_text, 4.0);
            let (parsed_query, errors) = parser.parse_query_lenient(&query_text);
            if !errors.is_empty() {
                tracing::debug!(
                    entity_kind,
                    query = query_text,
                    errors = ?errors,
                    "Tantivy query parser ignored invalid search clauses"
                );
            }
            clauses.push((Occur::Should, parsed_query));
        }
        clauses.extend(
            self.exact_identifier_queries(terms)
                .into_iter()
                .map(|query| (Occur::Should, query)),
        );
        match clauses.len() {
            0 => None,
            1 => clauses.pop().map(|(_occur, query)| query),
            _ => Some(Box::new(BooleanQuery::new(clauses))),
        }
    }

    fn exact_identifier_queries(&self, terms: &[String]) -> Vec<Box<dyn Query>> {
        let fields = [
            (self.fields.schema_name, 20.0),
            (self.fields.source_name, 20.0),
            (self.fields.surface_name, 16.0),
            (self.fields.column_name, 12.0),
            (self.fields.name, 12.0),
            (self.fields.qualified_name, 10.0),
            (self.fields.normalized_value_key, 10.0),
            (self.fields.display_value_exact, 10.0),
            (self.fields.data_type, 4.0),
        ];
        terms
            .iter()
            .flat_map(|term| {
                exact_term_variants(term).into_iter().flat_map(move |term| {
                    fields.into_iter().map(move |(field, boost)| {
                        let query = Box::new(TermQuery::new(
                            Term::from_field_text(field, &term),
                            IndexRecordOption::Basic,
                        ));
                        Box::new(BoostQuery::new(query, boost)) as Box<dyn Query>
                    })
                })
            })
            .collect()
    }

    fn search_documents(
        &self,
        query: &dyn Query,
        limit: usize,
    ) -> Result<Vec<TantivyDocument>, SearchIndexError> {
        let reader = self
            .index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        reader.reload()?;
        let searcher = reader.searcher();
        let top_docs = searcher.search(query, &TopDocs::with_limit(limit).order_by_score())?;
        let mut documents = Vec::with_capacity(top_docs.len());
        for (_score, address) in top_docs {
            documents.push(searcher.doc(address)?);
        }
        Ok(documents)
    }

    fn catalog_document(&self, record: &CatalogEntityRecord) -> TantivyDocument {
        let mut doc = TantivyDocument::default();
        doc.add_text(self.fields.doc_key, &record.entity_key);
        doc.add_text(self.fields.entity_kind, "catalog");
        doc.add_text(self.fields.result_type, record.result_type.as_str());
        doc.add_text(self.fields.surface_kind, record.surface_kind.as_str());
        doc.add_text(self.fields.field_role, record.field_role.as_str());
        doc.add_text(self.fields.schema_name, &record.schema_name);
        doc.add_text(self.fields.source_name, &record.schema_name);
        doc.add_text(self.fields.surface_name, &record.surface_name);
        doc.add_text(self.fields.name, &record.name);
        doc.add_text(self.fields.qualified_name, &record.qualified_name);
        doc.add_text(self.fields.data_type, &record.data_type);
        doc.add_text(
            self.fields.required,
            if record.required { "true" } else { "false" },
        );
        doc.add_text(self.fields.description, &record.description);
        doc.add_text(self.fields.searchable_text, &record.searchable_text);
        doc.add_text(self.fields.name_text, &record.name);
        doc.add_text(self.fields.qualified_name_text, &record.qualified_name);
        doc.add_text(self.fields.description_text, &record.description);
        doc.add_text(self.fields.searchable_text_text, &record.searchable_text);
        doc
    }

    fn observed_document(&self, record: &ObservedValueStoredRecord) -> TantivyDocument {
        let mut doc = TantivyDocument::default();
        doc.add_text(self.fields.doc_key, record.doc_key());
        doc.add_text(self.fields.entity_kind, "observed_value");
        doc.add_text(self.fields.source_scope_id, &record.source_scope_id);
        doc.add_text(self.fields.source_name, &record.source_name);
        doc.add_text(self.fields.schema_name, &record.source_name);
        doc.add_text(self.fields.surface_kind, &record.surface_kind);
        doc.add_text(self.fields.surface_name, &record.surface_name);
        doc.add_text(self.fields.column_name, &record.column_name);
        doc.add_text(self.fields.name, &record.column_name);
        doc.add_text(
            self.fields.qualified_name,
            format!(
                "{}.{}.{}",
                record.source_name, record.surface_name, record.column_name
            ),
        );
        doc.add_text(
            self.fields.normalized_value_key,
            &record.normalized_value_key,
        );
        doc.add_text(self.fields.display_value, &record.display_value);
        doc.add_text(self.fields.display_value_exact, &record.display_value);
        doc.add_text(self.fields.searchable_text, &record.searchable_text);
        doc.add_text(self.fields.last_observed_at, &record.last_observed_at);
        doc.add_text(
            self.fields.observed_count,
            record.observed_count.to_string(),
        );
        doc.add_text(self.fields.name_text, &record.column_name);
        doc.add_text(
            self.fields.qualified_name_text,
            format!(
                "{}.{}.{}",
                record.source_name, record.surface_name, record.column_name
            ),
        );
        doc.add_text(self.fields.searchable_text_text, &record.searchable_text);
        doc.add_text(self.fields.value_text, &record.display_value);
        doc
    }

    #[cfg(test)]
    fn observed_state_file(&self) -> PathBuf {
        observed_state_file_for_index(&self.path)
    }

    #[cfg(test)]
    fn load_observed_records(&self) -> Result<Vec<ObservedValueStoredRecord>, SearchIndexError> {
        load_observed_records_for_index(&self.path)
    }

    fn observed_database(&self) -> Result<Database, SearchIndexError> {
        observed_database_for_index(&self.path)
    }

    fn observed_queue_job_count(&self) -> Result<usize, SearchIndexError> {
        let database = self.observed_database()?;
        let read_txn = database.begin_read()?;
        let queue = read_txn.open_table(OBSERVED_QUEUE_TABLE)?;
        let mut count = 0;
        for entry in queue.iter()? {
            let _entry = entry?;
            count += 1;
        }
        Ok(count)
    }

    fn prepare_next_observed_queue_job(
        &self,
    ) -> Result<Option<PreparedObservedQueueJob>, SearchIndexError> {
        loop {
            let database = self.observed_database()?;
            let write_txn = database.begin_write()?;
            let next = {
                let queue = write_txn.open_table(OBSERVED_QUEUE_TABLE)?;
                queue
                    .iter()?
                    .next()
                    .transpose()?
                    .map(|(key, value)| (key.value().to_string(), value.value().to_vec()))
            };
            let Some((key, value)) = next else {
                write_txn.commit()?;
                return Ok(None);
            };

            let mut job = match decode_observed_queue_job(&value) {
                Ok(job) => job,
                Err(error) => {
                    tracing::warn!(
                        queue_key = %key,
                        error = %error,
                        "discarding malformed observed-value queue job"
                    );
                    {
                        let mut queue = write_txn.open_table(OBSERVED_QUEUE_TABLE)?;
                        queue.remove(key.as_str())?;
                    }
                    write_txn.commit()?;
                    continue;
                }
            };
            job.attempts = job.attempts.saturating_add(1);
            job.last_attempt_at = Some(now_timestamp());

            let records = match job.state {
                ObservedValueQueueJobState::Pending { records } => {
                    let records = records.into_iter().map(ObservedValueRecord::from).collect();
                    let stored = Self::upsert_observed_records_in_txn(&write_txn, records)?;
                    job.state = ObservedValueQueueJobState::ProjectionPending {
                        records: stored.clone(),
                    };
                    let encoded = encode_observed_queue_job(&job)?;
                    let mut queue = write_txn.open_table(OBSERVED_QUEUE_TABLE)?;
                    queue.insert(key.as_str(), encoded.as_slice())?;
                    stored
                }
                ObservedValueQueueJobState::ProjectionPending { records } => {
                    job.state = ObservedValueQueueJobState::ProjectionPending {
                        records: records.clone(),
                    };
                    let encoded = encode_observed_queue_job(&job)?;
                    let mut queue = write_txn.open_table(OBSERVED_QUEUE_TABLE)?;
                    queue.insert(key.as_str(), encoded.as_slice())?;
                    records
                }
            };

            write_txn.commit()?;
            return Ok(Some(PreparedObservedQueueJob { key, records }));
        }
    }

    fn remove_observed_queue_job(&self, key: &str) -> Result<(), SearchIndexError> {
        let database = self.observed_database()?;
        let write_txn = database.begin_write()?;
        {
            let mut queue = write_txn.open_table(OBSERVED_QUEUE_TABLE)?;
            queue.remove(key)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    #[cfg(test)]
    fn remove_observed_queue_records_for_source(
        &self,
        source_name: &str,
    ) -> Result<(), SearchIndexError> {
        remove_observed_queue_records_for_source_at(&self.path, source_name)
    }

    #[cfg(test)]
    fn upsert_observed_records(
        &self,
        records: Vec<ObservedValueRecord>,
    ) -> Result<Vec<ObservedValueStoredRecord>, SearchIndexError> {
        let database = self.observed_database()?;
        let write_txn = database.begin_write()?;
        let updated = Self::upsert_observed_records_in_txn(&write_txn, records)?;
        write_txn.commit()?;
        Ok(updated)
    }

    fn upsert_observed_records_in_txn(
        write_txn: &redb::WriteTransaction,
        records: Vec<ObservedValueRecord>,
    ) -> Result<Vec<ObservedValueStoredRecord>, SearchIndexError> {
        let now = now_timestamp();
        let mut updated = Vec::new();
        let mut old_last_index_keys = Vec::new();

        {
            let mut table = write_txn.open_table(OBSERVED_RECORDS_TABLE)?;
            for record in records {
                let key = observed_doc_key(
                    &record.source_name,
                    &record.source_scope_id,
                    record.surface_kind,
                    &record.surface_name,
                    &record.column_name,
                    &record.normalized_value_key,
                );
                let stored = match table.get(key.as_str())? {
                    Some(existing) => match decode_observed_record(existing.value()) {
                        Ok(mut existing) => {
                            old_last_index_keys
                                .push(observed_last_observed_index_key(&existing, &key));
                            existing.display_value = record.display_value;
                            existing.searchable_text = record.searchable_text;
                            existing.suggested_operator =
                                record.suggested_operator.as_str().to_string();
                            existing.last_observed_at.clone_from(&now);
                            existing.observed_count = existing
                                .observed_count
                                .saturating_add(record.observed_count);
                            existing
                        }
                        Err(error) => {
                            tracing::warn!(
                                record_key = %key,
                                error = %error,
                                "discarding malformed observed-value state record during upsert"
                            );
                            new_observed_stored_record(record, &now)
                        }
                    },
                    None => new_observed_stored_record(record, &now),
                };
                let encoded = encode_observed_record(&stored)?;
                table.insert(key.as_str(), encoded.as_slice())?;
                updated.push(stored);
            }
        }

        {
            let mut source_index = write_txn.open_table(OBSERVED_SOURCE_INDEX_TABLE)?;
            for record in &updated {
                source_index.insert(
                    observed_source_index_key(record).as_str(),
                    record.doc_key().as_str(),
                )?;
            }
        }

        {
            let mut last_observed_index =
                write_txn.open_table(OBSERVED_LAST_OBSERVED_INDEX_TABLE)?;
            for key in old_last_index_keys {
                last_observed_index.remove(key.as_str())?;
            }
            for record in &updated {
                last_observed_index.insert(
                    observed_last_observed_index_key(record, &record.doc_key()).as_str(),
                    record.doc_key().as_str(),
                )?;
            }
        }

        Ok(updated)
    }

    #[cfg(test)]
    fn remove_observed_records_for_source(
        &self,
        source_name: &str,
    ) -> Result<Vec<ObservedValueStoredRecord>, SearchIndexError> {
        remove_observed_records_for_source_at(&self.path, source_name)
    }

    fn remove_observed_records_before(
        &self,
        cutoff: &str,
    ) -> Result<Vec<ObservedValueStoredRecord>, SearchIndexError> {
        remove_observed_records_before_at(&self.path, cutoff)
    }
}

fn remove_observed_queue_records_for_source_at(
    path: &Path,
    source_name: &str,
) -> Result<(), SearchIndexError> {
    let database = observed_database_for_index(path)?;
    let write_txn = database.begin_write()?;
    let jobs = {
        let queue = write_txn.open_table(OBSERVED_QUEUE_TABLE)?;
        queue
            .iter()?
            .map(|entry| {
                entry.map(|(key, value)| (key.value().to_string(), value.value().to_vec()))
            })
            .collect::<Result<Vec<_>, _>>()?
    };

    if jobs.is_empty() {
        write_txn.commit()?;
        return Ok(());
    }

    {
        let mut queue = write_txn.open_table(OBSERVED_QUEUE_TABLE)?;
        for (key, value) in jobs {
            let Ok(job) = decode_observed_queue_job(&value) else {
                queue.remove(key.as_str())?;
                continue;
            };
            let filtered = job.without_source(source_name);
            if filtered.is_empty() {
                queue.remove(key.as_str())?;
            } else {
                let encoded = encode_observed_queue_job(&filtered)?;
                queue.insert(key.as_str(), encoded.as_slice())?;
            }
        }
    }

    write_txn.commit()?;
    Ok(())
}

fn remove_observed_records_for_source_at(
    path: &Path,
    source_name: &str,
) -> Result<Vec<ObservedValueStoredRecord>, SearchIndexError> {
    let prefix = observed_source_index_prefix(source_name);
    remove_observed_records_from_index_range_at(path, OBSERVED_SOURCE_INDEX_TABLE, &prefix, true)
}

fn remove_observed_records_before_at(
    path: &Path,
    cutoff: &str,
) -> Result<Vec<ObservedValueStoredRecord>, SearchIndexError> {
    remove_observed_records_from_index_range_at(
        path,
        OBSERVED_LAST_OBSERVED_INDEX_TABLE,
        cutoff,
        false,
    )
}

fn remove_observed_records_from_index_range_at(
    path: &Path,
    index_table_definition: TableDefinition<&str, &str>,
    prefix_or_cutoff: &str,
    prefix_range: bool,
) -> Result<Vec<ObservedValueStoredRecord>, SearchIndexError> {
    let database = observed_database_for_index(path)?;
    let write_txn = database.begin_write()?;
    let index_entries = {
        let index = write_txn.open_table(index_table_definition)?;
        let (lower_bound, upper_bound) = if prefix_range {
            (
                prefix_or_cutoff.to_string(),
                prefix_range_end(prefix_or_cutoff),
            )
        } else {
            (String::new(), format!("{prefix_or_cutoff}\0"))
        };
        index
            .range(lower_bound.as_str()..upper_bound.as_str())?
            .map(|entry| {
                entry.map(|(index_key, doc_key)| {
                    (index_key.value().to_string(), doc_key.value().to_string())
                })
            })
            .collect::<Result<Vec<_>, _>>()?
    };
    if index_entries.is_empty() {
        return Ok(Vec::new());
    }

    let mut removed = Vec::new();
    {
        let mut records = write_txn.open_table(OBSERVED_RECORDS_TABLE)?;
        for (_index_key, key) in &index_entries {
            if let Some(record) = records.remove(key.as_str())? {
                match decode_observed_record(record.value()) {
                    Ok(record) => removed.push(record),
                    Err(error) => {
                        tracing::warn!(
                            record_key = %key,
                            error = %error,
                            "discarding malformed observed-value state record during removal"
                        );
                    }
                }
            }
        }
    }
    {
        let mut index = write_txn.open_table(index_table_definition)?;
        for (index_key, _doc_key) in &index_entries {
            index.remove(index_key.as_str())?;
        }
    }
    {
        let mut source_index = write_txn.open_table(OBSERVED_SOURCE_INDEX_TABLE)?;
        if !prefix_range {
            for record in &removed {
                source_index.remove(observed_source_index_key(record).as_str())?;
            }
        }
    }
    {
        let mut last_observed_index = write_txn.open_table(OBSERVED_LAST_OBSERVED_INDEX_TABLE)?;
        if prefix_range {
            for record in &removed {
                last_observed_index
                    .remove(observed_last_observed_index_key(record, &record.doc_key()).as_str())?;
            }
        }
    }

    write_txn.commit()?;
    Ok(removed)
}

impl SearchIndexStore {
    #[cfg(test)]
    fn set_last_observed_at_for_test(
        &self,
        display_value: &str,
        last_observed_at: &str,
    ) -> Result<(), SearchIndexError> {
        let database = self.observed_database()?;
        let write_txn = database.begin_write()?;
        let mut updated = None;
        let mut old_last_index_key = None;
        {
            let mut table = write_txn.open_table(OBSERVED_RECORDS_TABLE)?;
            let mut records = Vec::new();
            for entry in table.iter()? {
                let (key, value) = entry?;
                records.push((
                    key.value().to_string(),
                    decode_observed_record(value.value()).map_err(|error| {
                        SearchIndexError::ObservedStateDecode {
                            record_key: key.value().to_string(),
                            error: error.to_string(),
                        }
                    })?,
                ));
            }
            for (key, mut record) in records {
                if record.display_value == display_value {
                    old_last_index_key = Some(observed_last_observed_index_key(&record, &key));
                    record.last_observed_at = last_observed_at.to_string();
                    let encoded = encode_observed_record(&record)?;
                    table.insert(key.as_str(), encoded.as_slice())?;
                    updated = Some((key, record));
                    break;
                }
            }
        }
        let Some((key, record)) = updated else {
            write_txn.commit()?;
            return Ok(());
        };
        {
            let mut last_observed_index =
                write_txn.open_table(OBSERVED_LAST_OBSERVED_INDEX_TABLE)?;
            if let Some(old_key) = old_last_index_key {
                last_observed_index.remove(old_key.as_str())?;
            }
            last_observed_index.insert(
                observed_last_observed_index_key(&record, &key).as_str(),
                key.as_str(),
            )?;
        }
        write_txn.commit()?;

        let mut writer = self.writer()?;
        let _opstamp = writer.delete_term(Term::from_field_text(self.fields.doc_key, &key));
        writer.add_document(self.observed_document(&record))?;
        writer.commit()?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TantivySearchCapabilities {
    pub(crate) tantivy_version: String,
    pub(crate) tokenizer: String,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SearchIndexError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Tantivy(#[from] tantivy::TantivyError),
    #[error(transparent)]
    RedbDatabase(#[from] redb::DatabaseError),
    #[error(transparent)]
    RedbStorage(#[from] redb::StorageError),
    #[error(transparent)]
    RedbTable(#[from] redb::TableError),
    #[error(transparent)]
    RedbTransaction(#[from] redb::TransactionError),
    #[error(transparent)]
    RedbCommit(#[from] redb::CommitError),
    #[error(transparent)]
    Encode(#[from] bincode::error::EncodeError),
    #[error(transparent)]
    Decode(#[from] ObservedRecordDecodeError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("search index mutation lock is poisoned")]
    MutationLockPoisoned,
    #[cfg(test)]
    #[error(
        "observed-value state record '{record_key}' references malformed encoded data: {error}"
    )]
    ObservedStateDecode { record_key: String, error: String },
    #[error("Tantivy search index schema is missing required field '{field}'")]
    MissingField { field: &'static str },
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ObservedRecordDecodeError {
    #[error(transparent)]
    Bincode(#[from] bincode::error::DecodeError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("compressed observed-value state record is missing its length header")]
    MissingCompressedLength,
    #[error("compressed observed-value state record length does not fit this platform: {len}")]
    CompressedLengthTooLarge { len: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ObservedQueueDrain {
    pub(crate) processed_jobs: usize,
    pub(crate) pending_jobs: usize,
    pub(crate) budget_exhausted: bool,
    pub(crate) lock_busy: bool,
    pub(crate) storage_budget_exceeded: bool,
}

impl ObservedQueueDrain {
    pub(crate) fn has_pending(&self) -> bool {
        self.pending_jobs > 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ObservedStorageBudgetEnforcement {
    pub(crate) removed_queue_jobs: usize,
    pub(crate) removed_records: usize,
    pub(crate) storage_bytes: u64,
    pub(crate) budget_exceeded: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct CatalogSearchHit {
    pub(crate) entity_key: String,
    pub(crate) result_type: Option<CatalogSearchResultType>,
    pub(crate) surface_kind: Option<CatalogSearchSurfaceKind>,
    pub(crate) field_role: Option<CatalogSearchFieldRole>,
    pub(crate) schema_name: String,
    pub(crate) surface_name: String,
    pub(crate) name: String,
    pub(crate) data_type: String,
    pub(crate) required: bool,
    pub(crate) description: String,
    pub(crate) matched_fields: Vec<String>,
    pub(crate) score: u32,
}

#[derive(Debug, Clone)]
pub(crate) struct CatalogSearchPage {
    pub(crate) hits: Vec<CatalogSearchHit>,
    pub(crate) has_more: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CatalogSearchResultType {
    CatalogTable,
    CatalogTableFunction,
    ColumnHint,
    NativeSearchPath,
}

impl CatalogSearchResultType {
    fn as_str(self) -> &'static str {
        match self {
            Self::CatalogTable => "catalog_table",
            Self::CatalogTableFunction => "catalog_table_function",
            Self::ColumnHint => "column_hint",
            Self::NativeSearchPath => "native_search_path",
        }
    }

    fn from_str(value: &str) -> Option<Self> {
        match value {
            "catalog_table" => Some(Self::CatalogTable),
            "catalog_table_function" => Some(Self::CatalogTableFunction),
            "column_hint" => Some(Self::ColumnHint),
            "native_search_path" => Some(Self::NativeSearchPath),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum CatalogSearchSurfaceKind {
    Table,
    TableFunction,
}

impl CatalogSearchSurfaceKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::TableFunction => "table_function",
        }
    }

    fn from_str(value: &str) -> Option<Self> {
        match value {
            "table" => Some(Self::Table),
            "table_function" => Some(Self::TableFunction),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CatalogSearchFieldRole {
    Unspecified,
    TableColumn,
    TableFilter,
    TableFunctionArgument,
    TableFunctionResultColumn,
}

impl CatalogSearchFieldRole {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::TableColumn => "table_column",
            Self::TableFilter => "table_filter",
            Self::TableFunctionArgument => "table_function_argument",
            Self::TableFunctionResultColumn => "table_function_result_column",
        }
    }

    fn from_str(value: &str) -> Option<Self> {
        match value {
            "unspecified" => Some(Self::Unspecified),
            "table_column" => Some(Self::TableColumn),
            "table_filter" => Some(Self::TableFilter),
            "table_function_argument" => Some(Self::TableFunctionArgument),
            "table_function_result_column" => Some(Self::TableFunctionResultColumn),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ObservedValueRecord {
    pub(crate) source_scope_id: String,
    pub(crate) source_name: String,
    pub(crate) surface_kind: ObservedValueSurfaceKind,
    pub(crate) surface_name: String,
    pub(crate) column_name: String,
    pub(crate) normalized_value_key: String,
    pub(crate) display_value: String,
    pub(crate) searchable_text: String,
    pub(crate) suggested_operator: ObservedValueSuggestedOperator,
    pub(crate) observed_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ObservedValueQueuedRecord {
    #[serde(default)]
    source_scope_id: String,
    source_name: String,
    surface_kind: String,
    surface_name: String,
    column_name: String,
    normalized_value_key: String,
    display_value: String,
    searchable_text: String,
    suggested_operator: String,
    observed_count: u64,
}

impl From<ObservedValueRecord> for ObservedValueQueuedRecord {
    fn from(record: ObservedValueRecord) -> Self {
        Self {
            source_scope_id: record.source_scope_id,
            source_name: record.source_name,
            surface_kind: record.surface_kind.as_str().to_string(),
            surface_name: record.surface_name,
            column_name: record.column_name,
            normalized_value_key: record.normalized_value_key,
            display_value: record.display_value,
            searchable_text: record.searchable_text,
            suggested_operator: record.suggested_operator.as_str().to_string(),
            observed_count: record.observed_count,
        }
    }
}

impl From<ObservedValueQueuedRecord> for ObservedValueRecord {
    fn from(record: ObservedValueQueuedRecord) -> Self {
        Self {
            source_scope_id: record.source_scope_id,
            source_name: record.source_name,
            surface_kind: ObservedValueSurfaceKind::from_str(&record.surface_kind)
                .unwrap_or(ObservedValueSurfaceKind::Table),
            surface_name: record.surface_name,
            column_name: record.column_name,
            normalized_value_key: record.normalized_value_key,
            display_value: record.display_value,
            searchable_text: record.searchable_text,
            suggested_operator: ObservedValueSuggestedOperator::from_str(
                &record.suggested_operator,
            )
            .unwrap_or(ObservedValueSuggestedOperator::Exact),
            observed_count: record.observed_count,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ObservedValueSearchHit {
    pub(crate) source_name: String,
    pub(crate) surface_kind: ObservedValueSurfaceKind,
    pub(crate) surface_name: String,
    pub(crate) column_name: String,
    pub(crate) normalized_value_key: String,
    pub(crate) display_value: String,
    pub(crate) last_observed_at: String,
    pub(crate) score: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ObservedValueSurfaceKind {
    Table,
    TableFunction,
}

impl ObservedValueSurfaceKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::TableFunction => "table_function",
        }
    }

    fn from_str(value: &str) -> Option<Self> {
        match value {
            "table" => Some(Self::Table),
            "table_function" => Some(Self::TableFunction),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum ObservedValueSuggestedOperator {
    Exact,
}

impl ObservedValueSuggestedOperator {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
        }
    }

    fn from_str(value: &str) -> Option<Self> {
        match value {
            "exact" => Some(Self::Exact),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ObservedValueStoredRecord {
    #[serde(default)]
    source_scope_id: String,
    source_name: String,
    surface_kind: String,
    surface_name: String,
    column_name: String,
    normalized_value_key: String,
    display_value: String,
    searchable_text: String,
    suggested_operator: String,
    first_observed_at: String,
    last_observed_at: String,
    observed_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ObservedValueQueueJob {
    state: ObservedValueQueueJobState,
    created_at: String,
    last_attempt_at: Option<String>,
    attempts: u32,
}

impl ObservedValueQueueJob {
    fn without_source(mut self, source_name: &str) -> Self {
        match &mut self.state {
            ObservedValueQueueJobState::Pending { records } => {
                records.retain(|record| record.source_name != source_name);
            }
            ObservedValueQueueJobState::ProjectionPending { records } => {
                records.retain(|record| record.source_name != source_name);
            }
        }
        self
    }

    fn is_empty(&self) -> bool {
        match &self.state {
            ObservedValueQueueJobState::Pending { records } => records.is_empty(),
            ObservedValueQueueJobState::ProjectionPending { records } => records.is_empty(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum ObservedValueQueueJobState {
    Pending {
        records: Vec<ObservedValueQueuedRecord>,
    },
    ProjectionPending {
        records: Vec<ObservedValueStoredRecord>,
    },
}

struct PreparedObservedQueueJob {
    key: String,
    records: Vec<ObservedValueStoredRecord>,
}

impl ObservedValueStoredRecord {
    fn doc_key(&self) -> String {
        observed_doc_key(
            &self.source_name,
            &self.source_scope_id,
            match self.surface_kind.as_str() {
                "table_function" => ObservedValueSurfaceKind::TableFunction,
                _ => ObservedValueSurfaceKind::Table,
            },
            &self.surface_name,
            &self.column_name,
            &self.normalized_value_key,
        )
    }
}

fn new_observed_stored_record(record: ObservedValueRecord, now: &str) -> ObservedValueStoredRecord {
    ObservedValueStoredRecord {
        source_name: record.source_name,
        source_scope_id: record.source_scope_id,
        surface_kind: record.surface_kind.as_str().to_string(),
        surface_name: record.surface_name,
        column_name: record.column_name,
        normalized_value_key: record.normalized_value_key,
        display_value: record.display_value,
        searchable_text: record.searchable_text,
        suggested_operator: record.suggested_operator.as_str().to_string(),
        first_observed_at: now.to_string(),
        last_observed_at: now.to_string(),
        observed_count: record.observed_count,
    }
}

fn encode_observed_record(record: &ObservedValueStoredRecord) -> Result<Vec<u8>, SearchIndexError> {
    let raw = encode_raw_observed_record(record)?;
    let compressed = zstd::bulk::compress(&raw, OBSERVED_RECORD_ZSTD_LEVEL)?;
    let compressed_len = 1 + OBSERVED_RECORD_ZSTD_LENGTH_BYTES + compressed.len();
    let raw_len = 1 + raw.len();
    if compressed_len < raw_len {
        let mut encoded = Vec::with_capacity(compressed_len);
        encoded.push(OBSERVED_RECORD_ENCODING_ZSTD);
        encoded.extend_from_slice(&(raw.len() as u64).to_le_bytes());
        encoded.extend_from_slice(&compressed);
        return Ok(encoded);
    }

    let mut encoded = Vec::with_capacity(raw_len);
    encoded.push(OBSERVED_RECORD_ENCODING_RAW);
    encoded.extend_from_slice(&raw);
    Ok(encoded)
}

fn encode_observed_queue_job(job: &ObservedValueQueueJob) -> Result<Vec<u8>, SearchIndexError> {
    let raw = bincode::serde::encode_to_vec(job, bincode::config::standard())?;
    let compressed = zstd::bulk::compress(&raw, OBSERVED_RECORD_ZSTD_LEVEL)?;
    let mut encoded =
        Vec::with_capacity(OBSERVED_RECORD_ZSTD_LENGTH_BYTES.saturating_add(compressed.len()));
    encoded.extend_from_slice(&(raw.len() as u64).to_le_bytes());
    encoded.extend_from_slice(&compressed);
    Ok(encoded)
}

fn decode_observed_queue_job(
    bytes: &[u8],
) -> Result<ObservedValueQueueJob, ObservedRecordDecodeError> {
    let length_bytes = bytes
        .get(..OBSERVED_RECORD_ZSTD_LENGTH_BYTES)
        .ok_or(ObservedRecordDecodeError::MissingCompressedLength)?;
    let length_bytes = length_bytes
        .try_into()
        .map_err(|_error| ObservedRecordDecodeError::MissingCompressedLength)?;
    let raw_len = u64::from_le_bytes(length_bytes);
    let raw_len = usize::try_from(raw_len)
        .map_err(|_error| ObservedRecordDecodeError::CompressedLengthTooLarge { len: raw_len })?;
    let compressed = bytes
        .get(OBSERVED_RECORD_ZSTD_LENGTH_BYTES..)
        .ok_or(ObservedRecordDecodeError::MissingCompressedLength)?;
    let raw = zstd::bulk::decompress(compressed, raw_len)?;
    bincode::serde::decode_from_slice::<ObservedValueQueueJob, _>(&raw, bincode::config::standard())
        .map(|(job, _consumed)| job)
        .map_err(ObservedRecordDecodeError::from)
}

fn encode_raw_observed_record(
    record: &ObservedValueStoredRecord,
) -> Result<Vec<u8>, bincode::error::EncodeError> {
    bincode::serde::encode_to_vec(record, bincode::config::standard())
}

fn decode_observed_record(
    bytes: &[u8],
) -> Result<ObservedValueStoredRecord, ObservedRecordDecodeError> {
    match bytes.first().copied() {
        Some(OBSERVED_RECORD_ENCODING_RAW) => decode_raw_observed_record(
            bytes
                .get(1..)
                .ok_or(ObservedRecordDecodeError::MissingCompressedLength)?,
        )
        .or_else(|_error| decode_raw_observed_record(bytes)),
        Some(OBSERVED_RECORD_ENCODING_ZSTD) => decode_compressed_observed_record(
            bytes
                .get(1..)
                .ok_or(ObservedRecordDecodeError::MissingCompressedLength)?,
        )
        .or_else(|_error| decode_raw_observed_record(bytes)),
        _ => decode_raw_observed_record(bytes),
    }
}

fn decode_raw_observed_record(
    bytes: &[u8],
) -> Result<ObservedValueStoredRecord, ObservedRecordDecodeError> {
    bincode::serde::decode_from_slice::<ObservedValueStoredRecord, _>(
        bytes,
        bincode::config::standard(),
    )
    .map(|(record, _consumed)| record)
    .map_err(ObservedRecordDecodeError::from)
}

fn decode_compressed_observed_record(
    bytes: &[u8],
) -> Result<ObservedValueStoredRecord, ObservedRecordDecodeError> {
    let length_bytes = bytes
        .get(..OBSERVED_RECORD_ZSTD_LENGTH_BYTES)
        .ok_or(ObservedRecordDecodeError::MissingCompressedLength)?;
    let length_bytes = length_bytes
        .try_into()
        .map_err(|_error| ObservedRecordDecodeError::MissingCompressedLength)?;
    let raw_len = u64::from_le_bytes(length_bytes);
    let raw_len = usize::try_from(raw_len)
        .map_err(|_error| ObservedRecordDecodeError::CompressedLengthTooLarge { len: raw_len })?;
    let compressed = bytes
        .get(OBSERVED_RECORD_ZSTD_LENGTH_BYTES..)
        .ok_or(ObservedRecordDecodeError::MissingCompressedLength)?;
    let raw = zstd::bulk::decompress(compressed, raw_len)?;
    decode_raw_observed_record(&raw)
}

fn observed_source_index_key(record: &ObservedValueStoredRecord) -> String {
    format!("{}{}{}", record.source_name, '\0', record.doc_key())
}

fn observed_source_index_prefix(source_name: &str) -> String {
    format!("{source_name}\0")
}

fn observed_last_observed_index_key(record: &ObservedValueStoredRecord, doc_key: &str) -> String {
    format!("{}{}{}", record.last_observed_at, '\0', doc_key)
}

fn observed_queue_job_key() -> String {
    format!("{}{}{}", now_timestamp(), '\0', Uuid::new_v4())
}

fn prefix_range_end(prefix: &str) -> String {
    format!("{prefix}{}", char::MAX)
}

#[derive(Debug)]
struct CatalogEntityRecord {
    entity_key: String,
    result_type: CatalogSearchResultType,
    surface_kind: CatalogSearchSurfaceKind,
    field_role: CatalogSearchFieldRole,
    schema_name: String,
    surface_name: String,
    name: String,
    qualified_name: String,
    data_type: String,
    required: bool,
    description: String,
    searchable_text: String,
}

#[derive(Debug, Clone, Copy)]
struct SearchIndexFields {
    doc_key: Field,
    entity_kind: Field,
    result_type: Field,
    surface_kind: Field,
    field_role: Field,
    source_scope_id: Field,
    source_name: Field,
    schema_name: Field,
    surface_name: Field,
    column_name: Field,
    name: Field,
    qualified_name: Field,
    data_type: Field,
    required: Field,
    description: Field,
    normalized_value_key: Field,
    display_value: Field,
    display_value_exact: Field,
    searchable_text: Field,
    last_observed_at: Field,
    observed_count: Field,
    name_text: Field,
    qualified_name_text: Field,
    description_text: Field,
    searchable_text_text: Field,
    value_text: Field,
}

impl SearchIndexFields {
    fn from_schema(schema: &Schema) -> Result<Self, SearchIndexError> {
        Ok(Self {
            doc_key: required_field(schema, "doc_key")?,
            entity_kind: required_field(schema, "entity_kind")?,
            result_type: required_field(schema, "result_type")?,
            surface_kind: required_field(schema, "surface_kind")?,
            field_role: required_field(schema, "field_role")?,
            source_scope_id: required_field(schema, "source_scope_id")?,
            source_name: required_field(schema, "source_name")?,
            schema_name: required_field(schema, "schema_name")?,
            surface_name: required_field(schema, "surface_name")?,
            column_name: required_field(schema, "column_name")?,
            name: required_field(schema, "name")?,
            qualified_name: required_field(schema, "qualified_name")?,
            data_type: required_field(schema, "data_type")?,
            required: required_field(schema, "required")?,
            description: required_field(schema, "description")?,
            normalized_value_key: required_field(schema, "normalized_value_key")?,
            display_value: required_field(schema, "display_value")?,
            display_value_exact: required_field(schema, "display_value_exact")?,
            searchable_text: required_field(schema, "searchable_text")?,
            last_observed_at: required_field(schema, "last_observed_at")?,
            observed_count: required_field(schema, "observed_count")?,
            name_text: required_field(schema, "name_text")?,
            qualified_name_text: required_field(schema, "qualified_name_text")?,
            description_text: required_field(schema, "description_text")?,
            searchable_text_text: required_field(schema, "searchable_text_text")?,
            value_text: required_field(schema, "value_text")?,
        })
    }
}

fn replace_catalog_index_at(path: &Path, catalog: &CatalogInfo) -> Result<(), SearchIndexError> {
    let mutation_lock = index_mutation_lock(path);
    let _guard = mutation_lock
        .lock()
        .map_err(|_poisoned| SearchIndexError::MutationLockPoisoned)?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    ensure_dir(parent)?;

    let replacement_path = sibling_index_path(path, "rebuild");
    if replacement_path.exists() {
        fs::remove_dir_all(&replacement_path)?;
    }

    let observed_records = load_observed_records_for_index(path)?;
    if let Err(error) = build_replacement_index(&replacement_path, catalog, &observed_records) {
        if let Err(cleanup_error) = fs::remove_dir_all(&replacement_path) {
            tracing::warn!(
                path = %replacement_path.display(),
                error = %cleanup_error,
                "failed to remove incomplete Tantivy replacement index"
            );
        }
        return Err(error);
    }

    swap_index_directory(path, &replacement_path)
}

fn index_mutation_lock(path: &Path) -> Arc<Mutex<()>> {
    let locks = INDEX_MUTATION_LOCKS.get_or_init(|| Mutex::new(StdBTreeMap::new()));
    let mut locks = locks.lock().expect("search index lock map");
    locks
        .entry(path.to_path_buf())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

fn build_replacement_index(
    path: &Path,
    catalog: &CatalogInfo,
    observed_records: &[ObservedValueStoredRecord],
) -> Result<(), SearchIndexError> {
    ensure_dir(path)?;
    let index = Index::create_in_dir(path, search_schema())?;
    let store = SearchIndexStore::from_index(path, index)?;
    let mut writer = store.writer()?;

    for record in catalog_entity_records(catalog) {
        writer.add_document(store.catalog_document(&record))?;
    }
    for record in observed_records {
        writer.add_document(store.observed_document(record))?;
    }
    writer.commit()?;
    Ok(())
}

fn swap_index_directory(path: &Path, replacement_path: &Path) -> Result<(), SearchIndexError> {
    let old_path = if path.exists() {
        let old_path = sibling_index_path(path, "old");
        if old_path.exists() {
            fs::remove_dir_all(&old_path)?;
        }
        fs::rename(path, &old_path)?;
        Some(old_path)
    } else {
        None
    };

    if let Err(error) = fs::rename(replacement_path, path) {
        if let Some(old_path) = &old_path
            && let Err(restore_error) = fs::rename(old_path, path)
        {
            tracing::warn!(
                old_path = %old_path.display(),
                path = %path.display(),
                error = %restore_error,
                "failed to restore previous Tantivy search index after replacement failure"
            );
        }
        return Err(error.into());
    }

    if let Some(old_path) = old_path
        && let Err(error) = fs::remove_dir_all(&old_path)
    {
        tracing::warn!(
            path = %old_path.display(),
            error = %error,
            "failed to remove replaced Tantivy search index"
        );
    }
    Ok(())
}

fn sibling_index_path(path: &Path, label: &str) -> PathBuf {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("tantivy");
    parent.join(format!(".{name}-{label}-{}", Uuid::new_v4()))
}

fn observed_state_file_for_index(path: &Path) -> PathBuf {
    path.parent().unwrap_or(path).join(OBSERVED_STATE_FILE_NAME)
}

fn observed_database_for_index(path: &Path) -> Result<Database, SearchIndexError> {
    let database_path = observed_state_file_for_index(path);
    if let Some(parent) = database_path.parent() {
        ensure_dir(parent)?;
    }
    let database = Database::create(database_path)?;
    let write_txn = database.begin_write()?;
    {
        let _records = write_txn.open_table(OBSERVED_RECORDS_TABLE)?;
        let _source_index = write_txn.open_table(OBSERVED_SOURCE_INDEX_TABLE)?;
        let _last_observed_index = write_txn.open_table(OBSERVED_LAST_OBSERVED_INDEX_TABLE)?;
        let _queue = write_txn.open_table(OBSERVED_QUEUE_TABLE)?;
    }
    write_txn.commit()?;
    Ok(database)
}

fn observed_database_payload_bytes_for_index(path: &Path) -> Result<u64, SearchIndexError> {
    let database = observed_database_for_index(path)?;
    let read_txn = database.begin_read()?;
    let mut bytes = 0_u64;
    {
        let table = read_txn.open_table(OBSERVED_RECORDS_TABLE)?;
        for entry in table.iter()? {
            let (key, value) = entry?;
            add_payload_bytes(&mut bytes, key.value().len());
            add_payload_bytes(&mut bytes, value.value().len());
        }
    }
    {
        let table = read_txn.open_table(OBSERVED_SOURCE_INDEX_TABLE)?;
        for entry in table.iter()? {
            let (key, value) = entry?;
            add_payload_bytes(&mut bytes, key.value().len());
            add_payload_bytes(&mut bytes, value.value().len());
        }
    }
    {
        let table = read_txn.open_table(OBSERVED_LAST_OBSERVED_INDEX_TABLE)?;
        for entry in table.iter()? {
            let (key, value) = entry?;
            add_payload_bytes(&mut bytes, key.value().len());
            add_payload_bytes(&mut bytes, value.value().len());
        }
    }
    {
        let table = read_txn.open_table(OBSERVED_QUEUE_TABLE)?;
        for entry in table.iter()? {
            let (key, value) = entry?;
            add_payload_bytes(&mut bytes, key.value().len());
            add_payload_bytes(&mut bytes, value.value().len());
        }
    }
    Ok(bytes)
}

fn observed_search_tree_file_bytes_for_index(path: &Path) -> Result<u64, SearchIndexError> {
    let search_root = path.parent().unwrap_or(path);
    directory_size_excluding(search_root, &observed_state_file_for_index(path))
}

fn directory_size_excluding(path: &Path, excluded_file: &Path) -> Result<u64, SearchIndexError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() => {
            if path == excluded_file {
                Ok(0)
            } else {
                Ok(metadata.len())
            }
        }
        Ok(metadata) if metadata.is_dir() => {
            let mut bytes = 0_u64;
            for entry in fs::read_dir(path)? {
                bytes =
                    bytes.saturating_add(directory_size_excluding(&entry?.path(), excluded_file)?);
            }
            Ok(bytes)
        }
        Ok(_metadata) => Ok(0),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error.into()),
    }
}

fn add_payload_bytes(total: &mut u64, len: usize) {
    *total = total.saturating_add(len as u64);
}

fn remove_oldest_observed_queue_job_at(path: &Path) -> Result<bool, SearchIndexError> {
    let database = observed_database_for_index(path)?;
    let write_txn = database.begin_write()?;
    let key = {
        let queue = write_txn.open_table(OBSERVED_QUEUE_TABLE)?;
        queue
            .iter()?
            .next()
            .transpose()?
            .map(|(key, _value)| key.value().to_string())
    };
    let Some(key) = key else {
        write_txn.commit()?;
        return Ok(false);
    };
    {
        let mut queue = write_txn.open_table(OBSERVED_QUEUE_TABLE)?;
        queue.remove(key.as_str())?;
    }
    write_txn.commit()?;
    Ok(true)
}

enum RemovedObservedRecord {
    Decoded(Box<ObservedValueStoredRecord>),
    Malformed,
}

fn remove_oldest_observed_record_at(
    path: &Path,
) -> Result<Option<RemovedObservedRecord>, SearchIndexError> {
    let database = observed_database_for_index(path)?;
    let write_txn = database.begin_write()?;
    let next = {
        let last_observed_index = write_txn.open_table(OBSERVED_LAST_OBSERVED_INDEX_TABLE)?;
        last_observed_index
            .iter()?
            .next()
            .transpose()?
            .map(|(index_key, doc_key)| {
                (index_key.value().to_string(), doc_key.value().to_string())
            })
    };
    let Some((last_observed_index_key, doc_key)) = next else {
        write_txn.commit()?;
        return Ok(None);
    };

    let removed = {
        let mut records = write_txn.open_table(OBSERVED_RECORDS_TABLE)?;
        records
            .remove(doc_key.as_str())?
            .map(|record| decode_observed_record(record.value()))
    };
    {
        let mut last_observed_index = write_txn.open_table(OBSERVED_LAST_OBSERVED_INDEX_TABLE)?;
        last_observed_index.remove(last_observed_index_key.as_str())?;
    }

    let removed = match removed {
        Some(Ok(record)) => {
            let mut source_index = write_txn.open_table(OBSERVED_SOURCE_INDEX_TABLE)?;
            source_index.remove(observed_source_index_key(&record).as_str())?;
            Some(RemovedObservedRecord::Decoded(Box::new(record)))
        }
        Some(Err(error)) => {
            tracing::warn!(
                record_key = %doc_key,
                error = %error,
                "discarding malformed observed-value state record while enforcing storage budget"
            );
            Some(RemovedObservedRecord::Malformed)
        }
        None => Some(RemovedObservedRecord::Malformed),
    };

    write_txn.commit()?;
    Ok(removed)
}

fn load_observed_records_for_index(
    path: &Path,
) -> Result<Vec<ObservedValueStoredRecord>, SearchIndexError> {
    let database = observed_database_for_index(path)?;
    let read_txn = database.begin_read()?;
    let table = read_txn.open_table(OBSERVED_RECORDS_TABLE)?;
    let mut records = Vec::new();
    for entry in table.iter()? {
        let (key, value) = entry?;
        match decode_observed_record(value.value()) {
            Ok(record) => records.push(record),
            Err(error) => {
                tracing::warn!(
                    record_key = %key.value(),
                    error = %error,
                    "skipping malformed observed-value state record during index rebuild"
                );
            }
        }
    }
    records.sort_by_key(ObservedValueStoredRecord::doc_key);
    Ok(records)
}

fn open_or_create_index(path: &Path) -> Result<Index, SearchIndexError> {
    let meta_file = path.join("meta.json");
    if meta_file.exists() {
        match Index::open_in_dir(path) {
            Ok(index) if schema_has_required_fields(&index.schema()) => return Ok(index),
            Ok(_) => {}
            Err(error) => {
                tracing::debug!(
                    path = %path.display(),
                    error = %error,
                    "discarding unusable Tantivy search index"
                );
            }
        }
        fs::remove_dir_all(path)?;
        ensure_dir(path)?;
    }
    Ok(Index::create_in_dir(path, search_schema())?)
}

fn open_existing_index(path: &Path) -> Result<Option<Index>, SearchIndexError> {
    let meta_file = path.join("meta.json");
    if !meta_file.exists() {
        return Ok(None);
    }
    let index = Index::open_in_dir(path)?;
    if !schema_has_required_fields(&index.schema()) {
        tracing::debug!(
            path = %path.display(),
            "existing Tantivy search index schema is not compatible; deferring rebuild to catalog refresh"
        );
        return Ok(None);
    }
    Ok(Some(index))
}

fn index_is_usable(path: &Path) -> bool {
    let meta_file = path.join("meta.json");
    if !meta_file.exists() {
        return false;
    }
    match Index::open_in_dir(path) {
        Ok(index) => schema_has_required_fields(&index.schema()),
        Err(error) => {
            tracing::debug!(
                path = %path.display(),
                error = %error,
                "treating unusable Tantivy search index as stale"
            );
            false
        }
    }
}

fn search_schema() -> Schema {
    let mut builder = Schema::builder();
    builder.add_text_field("doc_key", STRING | STORED);
    builder.add_text_field("entity_kind", STRING | STORED);
    builder.add_text_field("result_type", STRING | STORED);
    builder.add_text_field("surface_kind", STRING | STORED);
    builder.add_text_field("field_role", STRING | STORED);
    builder.add_text_field("source_scope_id", STRING | STORED);
    builder.add_text_field("source_name", STRING | STORED);
    builder.add_text_field("schema_name", STRING | STORED);
    builder.add_text_field("surface_name", STRING | STORED);
    builder.add_text_field("column_name", STRING | STORED);
    builder.add_text_field("name", STRING | STORED);
    builder.add_text_field("qualified_name", STRING | STORED);
    builder.add_text_field("data_type", STRING | STORED);
    builder.add_text_field("required", STRING | STORED);
    builder.add_text_field("description", stored_text_options());
    builder.add_text_field("normalized_value_key", STRING | STORED);
    builder.add_text_field("display_value", stored_text_options());
    builder.add_text_field("display_value_exact", STRING);
    builder.add_text_field("searchable_text", stored_text_options());
    builder.add_text_field("last_observed_at", STRING | STORED);
    builder.add_text_field("observed_count", STRING | STORED);
    builder.add_text_field("name_text", trigram_text_options());
    builder.add_text_field("qualified_name_text", trigram_text_options());
    builder.add_text_field("description_text", trigram_text_options());
    builder.add_text_field("searchable_text_text", trigram_text_options());
    builder.add_text_field("value_text", trigram_text_options());
    builder.build()
}

fn stored_text_options() -> TextOptions {
    TextOptions::default().set_stored()
}

fn trigram_text_options() -> TextOptions {
    TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer(TRIGRAM_TOKENIZER)
            .set_index_option(IndexRecordOption::WithFreqsAndPositions),
    )
}

fn register_tokenizers(index: &Index) -> Result<(), SearchIndexError> {
    let tokenizer = TextAnalyzer::builder(NgramTokenizer::all_ngrams(3, 3)?)
        .filter(LowerCaser)
        .build();
    index.tokenizers().register(TRIGRAM_TOKENIZER, tokenizer);
    Ok(())
}

fn schema_has_required_fields(schema: &Schema) -> bool {
    [
        "doc_key",
        "entity_kind",
        "result_type",
        "surface_kind",
        "field_role",
        "source_scope_id",
        "source_name",
        "schema_name",
        "surface_name",
        "column_name",
        "name",
        "qualified_name",
        "data_type",
        "required",
        "description",
        "normalized_value_key",
        "display_value",
        "display_value_exact",
        "searchable_text",
        "last_observed_at",
        "observed_count",
        "name_text",
        "qualified_name_text",
        "description_text",
        "searchable_text_text",
        "value_text",
    ]
    .into_iter()
    .all(|field| schema.get_field(field).is_ok())
}

fn required_field(schema: &Schema, field: &'static str) -> Result<Field, SearchIndexError> {
    schema
        .get_field(field)
        .map_err(|_error| SearchIndexError::MissingField { field })
}

fn catalog_entity_records(catalog: &CatalogInfo) -> Vec<CatalogEntityRecord> {
    let mut records = Vec::new();
    for table in &catalog.tables {
        table_entity_records(table, &mut records);
    }
    for function in &catalog.table_functions {
        table_function_entity_records(function, &mut records);
    }
    records
}

fn table_entity_records(table: &TableInfo, records: &mut Vec<CatalogEntityRecord>) {
    let qualified_name = qualified_name(&table.schema_name, &table.table_name);
    let columns = table
        .columns
        .iter()
        .flat_map(|column| {
            [
                column.name.as_str(),
                column.data_type.as_str(),
                column.description.as_str(),
            ]
        })
        .collect::<Vec<_>>()
        .join(" ");
    let filters = table
        .filters
        .iter()
        .flat_map(|filter| {
            [
                filter.name.as_str(),
                filter.mode.as_str(),
                filter.data_type.as_str(),
                filter.description.as_str(),
            ]
        })
        .collect::<Vec<_>>()
        .join(" ");
    let required_filters = table.required_filters.join(" ");
    records.push(CatalogEntityRecord {
        entity_key: format!("catalog:table:{qualified_name}"),
        result_type: CatalogSearchResultType::CatalogTable,
        surface_kind: CatalogSearchSurfaceKind::Table,
        field_role: CatalogSearchFieldRole::Unspecified,
        schema_name: table.schema_name.clone(),
        surface_name: table.table_name.clone(),
        name: table.table_name.clone(),
        qualified_name: qualified_name.clone(),
        data_type: String::new(),
        required: false,
        description: table.description.clone(),
        searchable_text: join_search_text([
            table.schema_name.as_str(),
            table.table_name.as_str(),
            qualified_name.as_str(),
            table.description.as_str(),
            table.guide.as_str(),
            filters.as_str(),
            required_filters.as_str(),
            columns.as_str(),
        ]),
    });

    for column in &table.columns {
        if table
            .filters
            .iter()
            .any(|filter| filter.name == column.name)
        {
            continue;
        }
        table_column_record(table, column, records);
    }
    for filter in &table.filters {
        table_filter_record(table, filter, records);
    }
    for filter in table
        .required_filters
        .iter()
        .filter(|required| !table.filters.iter().any(|filter| filter.name == **required))
    {
        table_filter_record(
            table,
            &TableFilterInfo {
                name: filter.clone(),
                mode: String::new(),
                required: true,
                data_type: String::new(),
                description: "Required table filter".to_string(),
            },
            records,
        );
    }
}

fn table_column_record(
    table: &TableInfo,
    column: &ColumnInfo,
    records: &mut Vec<CatalogEntityRecord>,
) {
    let surface_name = qualified_name(&table.schema_name, &table.table_name);
    records.push(CatalogEntityRecord {
        entity_key: format!("column:table:{surface_name}:{}", column.name),
        result_type: CatalogSearchResultType::ColumnHint,
        surface_kind: CatalogSearchSurfaceKind::Table,
        field_role: if column.is_required_filter {
            CatalogSearchFieldRole::TableFilter
        } else {
            CatalogSearchFieldRole::TableColumn
        },
        schema_name: table.schema_name.clone(),
        surface_name: table.table_name.clone(),
        name: column.name.clone(),
        qualified_name: format!("{surface_name}.{}", column.name),
        data_type: column.data_type.clone(),
        required: column.is_required_filter,
        description: column.description.clone(),
        searchable_text: join_search_text([
            table.schema_name.as_str(),
            table.table_name.as_str(),
            column.name.as_str(),
            column.data_type.as_str(),
            column.description.as_str(),
        ]),
    });
}

fn table_filter_record(
    table: &TableInfo,
    filter: &TableFilterInfo,
    records: &mut Vec<CatalogEntityRecord>,
) {
    let surface_name = qualified_name(&table.schema_name, &table.table_name);
    records.push(CatalogEntityRecord {
        entity_key: format!("filter:table:{surface_name}:{}", filter.name),
        result_type: CatalogSearchResultType::ColumnHint,
        surface_kind: CatalogSearchSurfaceKind::Table,
        field_role: CatalogSearchFieldRole::TableFilter,
        schema_name: table.schema_name.clone(),
        surface_name: table.table_name.clone(),
        name: filter.name.clone(),
        qualified_name: format!("{surface_name}.{}", filter.name),
        data_type: filter.data_type.clone(),
        required: filter.required,
        description: filter.description.clone(),
        searchable_text: join_search_text([
            table.schema_name.as_str(),
            table.table_name.as_str(),
            filter.name.as_str(),
            filter.mode.as_str(),
            filter.data_type.as_str(),
            filter.description.as_str(),
            "table filter",
        ]),
    });
}

fn table_function_entity_records(
    function: &TableFunctionInfo,
    records: &mut Vec<CatalogEntityRecord>,
) {
    let qualified_name = qualified_name(&function.schema_name, &function.function_name);
    let arguments = function
        .arguments
        .iter()
        .map(|argument| argument.name.as_str())
        .collect::<Vec<_>>()
        .join(" ");
    let result_columns = function
        .result_columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>()
        .join(" ");
    records.push(CatalogEntityRecord {
        entity_key: format!("catalog:function:{qualified_name}"),
        result_type: CatalogSearchResultType::CatalogTableFunction,
        surface_kind: CatalogSearchSurfaceKind::TableFunction,
        field_role: CatalogSearchFieldRole::Unspecified,
        schema_name: function.schema_name.clone(),
        surface_name: function.function_name.clone(),
        name: function.function_name.clone(),
        qualified_name: qualified_name.clone(),
        data_type: String::new(),
        required: false,
        description: function.description.clone(),
        searchable_text: join_search_text([
            function.schema_name.as_str(),
            function.function_name.as_str(),
            qualified_name.as_str(),
            function.description.as_str(),
            function.kind.as_str(),
            arguments.as_str(),
            result_columns.as_str(),
        ]),
    });

    if function.kind == "search" {
        records.push(CatalogEntityRecord {
            entity_key: format!("native_search:{qualified_name}"),
            result_type: CatalogSearchResultType::NativeSearchPath,
            surface_kind: CatalogSearchSurfaceKind::TableFunction,
            field_role: CatalogSearchFieldRole::Unspecified,
            schema_name: function.schema_name.clone(),
            surface_name: function.function_name.clone(),
            name: function.function_name.clone(),
            qualified_name: qualified_name.clone(),
            data_type: String::new(),
            required: false,
            description: function.description.clone(),
            searchable_text: join_search_text([
                function.schema_name.as_str(),
                function.function_name.as_str(),
                qualified_name.as_str(),
                function.description.as_str(),
                "native search path source scoped table function",
                arguments.as_str(),
                result_columns.as_str(),
            ]),
        });
    }

    for argument in &function.arguments {
        table_function_argument_record(function, argument, records);
    }
    for column in &function.result_columns {
        table_function_result_column_record(function, column, records);
    }
}

fn table_function_argument_record(
    function: &TableFunctionInfo,
    argument: &TableFunctionArgumentInfo,
    records: &mut Vec<CatalogEntityRecord>,
) {
    let surface_name = qualified_name(&function.schema_name, &function.function_name);
    let values = argument.values.join(" ");
    records.push(CatalogEntityRecord {
        entity_key: format!("argument:function:{surface_name}:{}", argument.name),
        result_type: CatalogSearchResultType::ColumnHint,
        surface_kind: CatalogSearchSurfaceKind::TableFunction,
        field_role: CatalogSearchFieldRole::TableFunctionArgument,
        schema_name: function.schema_name.clone(),
        surface_name: function.function_name.clone(),
        name: argument.name.clone(),
        qualified_name: format!("{surface_name}.{}", argument.name),
        data_type: String::new(),
        required: argument.required,
        description: "Table function argument".to_string(),
        searchable_text: join_search_text([
            function.schema_name.as_str(),
            function.function_name.as_str(),
            argument.name.as_str(),
            values.as_str(),
            "table function argument",
        ]),
    });
}

fn table_function_result_column_record(
    function: &TableFunctionInfo,
    column: &TableFunctionResultColumnInfo,
    records: &mut Vec<CatalogEntityRecord>,
) {
    let surface_name = qualified_name(&function.schema_name, &function.function_name);
    records.push(CatalogEntityRecord {
        entity_key: format!("result_column:function:{surface_name}:{}", column.name),
        result_type: CatalogSearchResultType::ColumnHint,
        surface_kind: CatalogSearchSurfaceKind::TableFunction,
        field_role: CatalogSearchFieldRole::TableFunctionResultColumn,
        schema_name: function.schema_name.clone(),
        surface_name: function.function_name.clone(),
        name: column.name.clone(),
        qualified_name: format!("{surface_name}.{}", column.name),
        data_type: column.data_type.clone(),
        required: false,
        description: column.description.clone(),
        searchable_text: join_search_text([
            function.schema_name.as_str(),
            function.function_name.as_str(),
            column.name.as_str(),
            column.data_type.as_str(),
            column.description.as_str(),
            "table function result column",
        ]),
    });
}

fn observed_doc_key(
    source_name: &str,
    source_scope_id: &str,
    surface_kind: ObservedValueSurfaceKind,
    surface_name: &str,
    column_name: &str,
    normalized_value_key: &str,
) -> String {
    format!(
        "observed:{}:{}:{}:{}:{}:{}",
        source_name,
        source_scope_id,
        surface_kind.as_str(),
        surface_name,
        column_name,
        normalized_value_key
    )
}

fn qualified_name(schema_name: &str, surface_name: &str) -> String {
    format!("{schema_name}.{surface_name}")
}

fn join_search_text<const N: usize>(parts: [&str; N]) -> String {
    parts
        .into_iter()
        .filter(|part| !part.trim().is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

fn tantivy_query_text(terms: &[String]) -> Option<String> {
    let phrases = terms
        .iter()
        .filter(|term| term.chars().count() >= 3)
        .map(|term| format!("\"{}\"", escape_tantivy_phrase(term)))
        .collect::<Vec<_>>();
    if phrases.is_empty() {
        None
    } else {
        Some(phrases.join(" OR "))
    }
}

fn escape_tantivy_phrase(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn exact_term_variants(term: &str) -> Vec<String> {
    let mut variants = Vec::new();
    push_unique_variant(&mut variants, term.to_string());
    push_unique_variant(&mut variants, term.to_ascii_uppercase());
    push_unique_variant(&mut variants, title_case_ascii(term));
    variants
}

fn title_case_ascii(term: &str) -> String {
    let mut chars = term.chars();
    let Some(first) = chars.next() else {
        return String::new();
    };
    format!(
        "{}{}",
        first.to_ascii_uppercase(),
        chars.as_str().to_ascii_lowercase()
    )
}

fn push_unique_variant(variants: &mut Vec<String>, variant: String) {
    if !variants.iter().any(|existing| existing == &variant) {
        variants.push(variant);
    }
}

fn matched_fields<const N: usize>(
    terms: &[String],
    fields: [(&'static str, &str); N],
) -> Vec<String> {
    let mut matched = fields
        .into_iter()
        .filter_map(|(field, value)| {
            let normalized = value.to_ascii_lowercase();
            terms
                .iter()
                .any(|term| normalized.contains(term.as_str()))
                .then_some(field.to_string())
        })
        .collect::<Vec<_>>();
    matched.sort();
    matched.dedup();
    matched
}

fn doc_text(doc: &TantivyDocument, field: Field) -> String {
    doc.get_first(field)
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .to_string()
}

fn now_timestamp() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn assign_rank_scores<T>(hits: &mut [T], mut set_score: impl FnMut(&mut T, u32)) {
    let hit_count = u32::try_from(hits.len()).unwrap_or(u32::MAX);
    for (position, hit) in hits.iter_mut().enumerate() {
        let position = u32::try_from(position).unwrap_or(u32::MAX);
        set_score(hit, normalized_rank_score(position, hit_count));
    }
}

fn normalized_rank_score(position: u32, hit_count: u32) -> u32 {
    if hit_count == 0 {
        return 0;
    }
    if hit_count == 1 {
        return MAX_RANK_SCORE;
    }

    let span = hit_count.saturating_sub(1);
    let inverted_position = span.saturating_sub(position);
    1 + inverted_position.saturating_mul(MAX_RANK_SCORE - 1) / span
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap as StdBTreeMap;
    use std::fmt::Write as _;
    use std::time::Duration as StdDuration;

    use coral_engine::{
        CatalogInfo, TableFilterInfo, TableFunctionArgumentInfo, TableFunctionInfo,
        TableFunctionResultColumnInfo, TableInfo,
    };
    use tempfile::tempdir;

    use super::{
        CatalogSearchFieldRole, CatalogSearchResultType, OBSERVED_QUEUE_TABLE,
        OBSERVED_RECORD_ENCODING_ZSTD, OBSERVED_RECORDS_TABLE, ObservedValueRecord,
        ObservedValueSuggestedOperator, ObservedValueSurfaceKind, SearchIndexStore,
        decode_observed_record, directory_size_excluding, encode_observed_record,
        encode_raw_observed_record, new_observed_stored_record, observed_state_file_for_index,
        tantivy_query_text,
    };
    use crate::state::AppStateLayout;
    use crate::workspaces::WorkspaceName;

    #[test]
    fn open_workspace_creates_search_index_schema() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let store = SearchIndexStore::open_workspace(&layout, &workspace).expect("store");

        assert_eq!(store.path(), layout.search_index_dir(&workspace));
        assert_eq!(store.capabilities().tantivy_version, "0.26.1");
        assert_eq!(store.capabilities().tokenizer, "coral_trigram");
        assert!(store.path().join("meta.json").exists());
    }

    #[test]
    fn open_workspace_recreates_corrupt_search_index() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let index_path = layout.search_index_dir(&workspace);
        std::fs::create_dir_all(&index_path).expect("index dir");
        std::fs::write(index_path.join("meta.json"), "not valid tantivy metadata")
            .expect("corrupt meta");

        assert!(!SearchIndexStore::workspace_index_is_usable(
            &layout, &workspace
        ));
        let store = SearchIndexStore::open_workspace(&layout, &workspace).expect("recreate index");

        assert_eq!(store.path(), index_path);
        assert!(store.path().join("meta.json").exists());
        assert!(SearchIndexStore::workspace_index_is_usable(
            &layout, &workspace
        ));
    }

    #[test]
    fn catalog_tantivy_supports_bm25_trigram_matches() {
        let temp = tempdir().expect("tempdir");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let store = SearchIndexStore::replace_catalog_index(
            temp.path().join("tantivy"),
            &catalog_with_search_function(),
        )
        .expect("replace catalog");

        let hits = store
            .search_catalog(&workspace, &["commit".to_string()], 10)
            .expect("search catalog");
        assert!(
            hits.iter()
                .any(|hit| hit.surface_name == "search_deployments")
        );
    }

    #[test]
    fn catalog_tantivy_supports_short_exact_term_matches() {
        let temp = tempdir().expect("tempdir");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let store = SearchIndexStore::replace_catalog_index(
            temp.path().join("tantivy"),
            &catalog_with_search_function(),
        )
        .expect("replace catalog");

        let hits = store
            .search_catalog(&workspace, &["q".to_string()], 10)
            .expect("search catalog");

        assert!(hits.iter().any(|hit| {
            hit.name == "q"
                && hit.result_type == Some(CatalogSearchResultType::ColumnHint)
                && hit.field_role == Some(CatalogSearchFieldRole::TableFunctionArgument)
        }));
    }

    #[test]
    fn catalog_tantivy_supports_case_preserving_short_identifier_matches() {
        let temp = tempdir().expect("tempdir");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let store = SearchIndexStore::replace_catalog_index(
            temp.path().join("tantivy"),
            &catalog_with_uppercase_identifier(),
        )
        .expect("replace catalog");

        let hits = store
            .search_catalog(&workspace, &["id".to_string()], 10)
            .expect("search catalog");

        assert!(hits.iter().any(|hit| hit.name == "ID"
            && hit.result_type == Some(CatalogSearchResultType::ColumnHint)
            && hit.field_role == Some(CatalogSearchFieldRole::TableFunctionResultColumn)));
    }

    #[test]
    fn catalog_tantivy_indexes_optional_non_column_filters() {
        let temp = tempdir().expect("tempdir");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let store = SearchIndexStore::replace_catalog_index(
            temp.path().join("tantivy"),
            &catalog_with_table_filters(),
        )
        .expect("replace catalog");

        let hits = store
            .search_catalog(&workspace, &["status".to_string()], 1)
            .expect("search catalog");

        assert!(hits.iter().any(|hit| hit.name == "status"
            && hit.result_type == Some(CatalogSearchResultType::ColumnHint)
            && hit.field_role == Some(CatalogSearchFieldRole::TableFilter)
            && !hit.required
            && hit.data_type == "Utf8"
            && hit.description == "Optional issue status filter"));
    }

    #[test]
    fn catalog_search_page_reports_more_available_hits() {
        let temp = tempdir().expect("tempdir");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let store = SearchIndexStore::replace_catalog_index(
            temp.path().join("tantivy"),
            &catalog_with_search_function(),
        )
        .expect("replace catalog");

        let page = store
            .search_catalog_page(&workspace, &["github".to_string()], 1)
            .expect("search catalog page");

        assert_eq!(page.hits.len(), 1);
        assert!(page.has_more);
    }

    #[test]
    fn replace_catalog_indexes_function_metadata() {
        let temp = tempdir().expect("tempdir");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let path = temp.path().join("tantivy");
        let store = SearchIndexStore::replace_catalog_index(&path, &catalog_with_search_function())
            .expect("replace catalog");

        let hits = store
            .search_catalog(
                &workspace,
                &[
                    "github".to_string(),
                    "deployments".to_string(),
                    "sha".to_string(),
                ],
                10,
            )
            .expect("search catalog");

        assert!(hits.iter().any(|hit| hit.result_type
            == Some(CatalogSearchResultType::NativeSearchPath)
            && hit.surface_name == "search_deployments"));
        assert!(hits.iter().any(|hit| hit.result_type
            == Some(CatalogSearchResultType::ColumnHint)
            && hit.name == "sha"
            && hit.field_role == Some(CatalogSearchFieldRole::TableFunctionResultColumn)));

        let store = SearchIndexStore::replace_catalog_index(
            &path,
            &CatalogInfo {
                tables: Vec::new(),
                table_functions: Vec::new(),
            },
        )
        .expect("replace empty catalog");
        let hits = store
            .search_catalog(&workspace, &["github".to_string()], 10)
            .expect("search empty catalog");
        assert!(hits.is_empty());
    }

    #[test]
    fn replace_catalog_rebuild_preserves_observed_values() {
        let temp = tempdir().expect("tempdir");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let path = temp.path().join("tantivy");
        {
            let store = SearchIndexStore::open(&path).expect("store");
            store
                .upsert_observed_values(&workspace, vec![observed_record("payments-api", 1)])
                .expect("upsert observed");
        }

        let store = SearchIndexStore::replace_catalog_index(&path, &catalog_with_search_function())
            .expect("replace catalog");

        assert!(
            !store
                .search_catalog(&workspace, &["deployments".to_string()], 10)
                .expect("search catalog")
                .is_empty()
        );
        let observed_hits = store
            .search_observed_values(&workspace, &["payments-api".to_string()], 10)
            .expect("search observed");
        assert_eq!(observed_hits.len(), 1);
        assert_eq!(
            observed_hits.first().expect("observed hit").display_value,
            "payments-api"
        );
    }

    #[test]
    fn tantivy_query_text_quotes_technical_terms() {
        assert_eq!(
            tantivy_query_text(&["github.search_commits".to_string(), "id".to_string()])
                .expect("query"),
            "\"github.search_commits\""
        );
    }

    #[test]
    fn normalized_rank_score_uses_fixed_band() {
        assert_eq!(super::normalized_rank_score(0, 0), 0);
        assert_eq!(super::normalized_rank_score(0, 1), 1_000);
        assert_eq!(super::normalized_rank_score(0, 50), 1_000);
        assert_eq!(super::normalized_rank_score(49, 50), 1);
        assert!(super::normalized_rank_score(24, 50) > 490);
        assert!(super::normalized_rank_score(24, 50) < 520);
    }

    #[test]
    fn observed_record_encoding_reads_legacy_raw_records() {
        let record = new_observed_stored_record(
            observed_record("payments-api", 1),
            "2026-06-09T10:00:00.000Z",
        );
        let raw = encode_raw_observed_record(&record).expect("encode raw record");

        let decoded = decode_observed_record(&raw).expect("decode legacy raw record");

        assert_eq!(decoded, record);
    }

    #[test]
    fn observed_record_encoding_compresses_large_records() {
        let mut record = new_observed_stored_record(
            observed_record("payments-api", 1),
            "2026-06-09T10:00:00.000Z",
        );
        record.display_value = "deploy_failed service=payments-api ".repeat(128);
        record.searchable_text = record.display_value.clone();

        let raw = encode_raw_observed_record(&record).expect("encode raw record");
        let encoded = encode_observed_record(&record).expect("encode observed record");

        assert_eq!(
            encoded.first().copied(),
            Some(OBSERVED_RECORD_ENCODING_ZSTD)
        );
        assert!(encoded.len() < raw.len());
        assert_eq!(
            decode_observed_record(&encoded).expect("decode compressed record"),
            record
        );
    }

    #[test]
    fn observed_values_upsert_count_and_search() {
        let temp = tempdir().expect("tempdir");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let store = SearchIndexStore::open(temp.path().join("tantivy")).expect("store");

        store
            .upsert_observed_values(
                &workspace,
                vec![
                    observed_record("payments-api", 2),
                    observed_record("payments-api", 1),
                ],
            )
            .expect("upsert observed");

        let hits = store
            .search_observed_values(&workspace, &["payments-api".to_string()], 10)
            .expect("search observed");
        assert_eq!(hits.len(), 1);
        let hit = hits.first().expect("observed hit");
        assert_eq!(hit.column_name, "service");

        assert_eq!(
            store
                .observed_count_for_test("payments-api")
                .expect("observed state")
                .expect("stored observed value"),
            3
        );
    }

    #[test]
    fn observed_queue_drains_enqueued_values() {
        let temp = tempdir().expect("tempdir");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let store = SearchIndexStore::open(temp.path().join("tantivy")).expect("store");

        store
            .enqueue_observed_values(&workspace, vec![observed_record("payments-api", 1)])
            .expect("enqueue observed");

        assert!(
            store
                .search_observed_values(&workspace, &["payments-api".to_string()], 10)
                .expect("search before drain")
                .is_empty()
        );

        let drain = store
            .drain_observed_value_queue_for(StdDuration::from_secs(1))
            .expect("drain observed queue");

        assert_eq!(drain.processed_jobs, 1);
        assert_eq!(drain.pending_jobs, 0);
        let hits = store
            .search_observed_values(&workspace, &["payments-api".to_string()], 10)
            .expect("search after drain");
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn observed_queue_continues_after_malformed_job() {
        let temp = tempdir().expect("tempdir");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let store = SearchIndexStore::open(temp.path().join("tantivy")).expect("store");
        let database = store.observed_database().expect("observed database");
        let write_txn = database.begin_write().expect("write transaction");
        {
            let mut queue = write_txn
                .open_table(OBSERVED_QUEUE_TABLE)
                .expect("queue table");
            queue
                .insert("0000-malformed", [0xff, 0x00, 0x01].as_slice())
                .expect("insert malformed queue job");
        }
        write_txn.commit().expect("commit malformed queue job");
        drop(database);
        store
            .enqueue_observed_values(&workspace, vec![observed_record("payments-api", 1)])
            .expect("enqueue observed");

        let drain = store
            .drain_observed_value_queue_for(StdDuration::from_secs(1))
            .expect("drain observed queue");

        assert_eq!(drain.processed_jobs, 1);
        assert_eq!(drain.pending_jobs, 0);
        let hits = store
            .search_observed_values(&workspace, &["payments-api".to_string()], 10)
            .expect("search observed");
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn observed_queue_projection_replay_does_not_double_count() {
        let temp = tempdir().expect("tempdir");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let store = SearchIndexStore::open(temp.path().join("tantivy")).expect("store");

        store
            .enqueue_observed_values(&workspace, vec![observed_record("payments-api", 1)])
            .expect("enqueue observed");
        let prepared = store
            .prepare_next_observed_queue_job()
            .expect("prepare queued job")
            .expect("queued job");
        assert_eq!(prepared.records.len(), 1);

        let drain = store
            .drain_observed_value_queue_for(StdDuration::from_secs(1))
            .expect("drain projection-pending job");

        assert_eq!(drain.processed_jobs, 1);
        assert_eq!(drain.pending_jobs, 0);
        assert_eq!(
            store
                .observed_count_for_test("payments-api")
                .expect("observed state")
                .expect("stored observed value"),
            1
        );
    }

    #[test]
    fn observed_queue_drops_values_for_deleted_source() {
        let temp = tempdir().expect("tempdir");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = crate::sources::SourceName::parse("notion").expect("source");
        let store = SearchIndexStore::open(temp.path().join("tantivy")).expect("store");

        store
            .enqueue_observed_values(
                &workspace,
                vec![ObservedValueRecord {
                    source_name: "notion".to_string(),
                    display_value: "notion-value".to_string(),
                    searchable_text: "notion page notion-value".to_string(),
                    ..observed_record("notion-value", 1)
                }],
            )
            .expect("enqueue observed");
        store
            .delete_observed_values_for_source(&workspace, &source)
            .expect("delete source observed values");

        let drain = store
            .drain_observed_value_queue_for(StdDuration::from_secs(1))
            .expect("drain observed queue");

        assert_eq!(drain.processed_jobs, 0);
        assert_eq!(drain.pending_jobs, 0);
        assert!(
            store
                .search_observed_values(&workspace, &["notion-value".to_string()], 10)
                .expect("search observed")
                .is_empty()
        );
    }

    #[test]
    fn source_discard_deletes_redb_state_without_tantivy_projection() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = crate::sources::SourceName::parse("notion").expect("source");
        let store = SearchIndexStore::open_workspace(&layout, &workspace).expect("store");
        store
            .upsert_observed_values(
                &workspace,
                vec![ObservedValueRecord {
                    source_name: "notion".to_string(),
                    display_value: "notion-value".to_string(),
                    searchable_text: "notion page notion-value".to_string(),
                    ..observed_record("notion-value", 1)
                }],
            )
            .expect("upsert observed");
        store
            .enqueue_observed_values(
                &workspace,
                vec![ObservedValueRecord {
                    source_name: "notion".to_string(),
                    display_value: "queued-notion-value".to_string(),
                    searchable_text: "notion page queued-notion-value".to_string(),
                    ..observed_record("queued-notion-value", 1)
                }],
            )
            .expect("enqueue observed");
        drop(store);
        std::fs::remove_dir_all(layout.search_index_dir(&workspace)).expect("remove tantivy index");

        SearchIndexStore::discard_observed_values_for_source(&layout, &workspace, &source)
            .expect("discard source observed values");
        let store = SearchIndexStore::replace_workspace_catalog(
            &layout,
            &workspace,
            &CatalogInfo {
                tables: Vec::new(),
                table_functions: Vec::new(),
            },
        )
        .expect("rebuild catalog index");
        let drain = store
            .drain_observed_value_queue_for(StdDuration::from_secs(1))
            .expect("drain observed queue");

        assert_eq!(drain.processed_jobs, 0);
        assert_eq!(drain.pending_jobs, 0);
        assert!(
            store
                .search_observed_values(&workspace, &["notion-value".to_string()], 10)
                .expect("search deleted durable state")
                .is_empty()
        );
        assert!(
            store
                .search_observed_values(&workspace, &["queued-notion-value".to_string()], 10)
                .expect("search deleted queued state")
                .is_empty()
        );
    }

    #[test]
    fn workspace_discard_deletes_all_observed_state_and_projection_docs() {
        let temp = tempdir().expect("tempdir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let store = SearchIndexStore::open_workspace(&layout, &workspace).expect("store");
        store
            .upsert_observed_values(
                &workspace,
                vec![
                    ObservedValueRecord {
                        source_name: "notion".to_string(),
                        display_value: "notion-value".to_string(),
                        searchable_text: "notion page notion-value".to_string(),
                        ..observed_record("notion-value", 1)
                    },
                    ObservedValueRecord {
                        source_name: "linear".to_string(),
                        display_value: "linear-value".to_string(),
                        searchable_text: "linear issue linear-value".to_string(),
                        ..observed_record("linear-value", 1)
                    },
                ],
            )
            .expect("upsert observed");
        store
            .enqueue_observed_values(&workspace, vec![observed_record("queued-value", 1)])
            .expect("enqueue observed");
        let drain = store
            .drain_observed_value_queue_for(StdDuration::from_secs(1))
            .expect("drain observed queue");
        assert_eq!(drain.pending_jobs, 0);
        drop(store);

        SearchIndexStore::discard_observed_values(&layout, &workspace)
            .expect("discard observed values");
        assert!(!observed_state_file_for_index(&layout.search_index_dir(&workspace)).exists());

        let store = SearchIndexStore::open_workspace(&layout, &workspace).expect("store");
        for value in ["notion-value", "linear-value", "queued-value"] {
            assert!(
                store
                    .search_observed_values(&workspace, &[value.to_string()], 10)
                    .expect("search observed")
                    .is_empty(),
                "{value} should be cleared"
            );
        }
    }

    #[test]
    fn observed_values_support_short_exact_value_matches() {
        let temp = tempdir().expect("tempdir");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let store = SearchIndexStore::open(temp.path().join("tantivy")).expect("store");

        store
            .upsert_observed_values(&workspace, vec![observed_record("US", 1)])
            .expect("upsert observed");

        let hits = store
            .search_observed_values(&workspace, &["us".to_string()], 10)
            .expect("search observed");

        assert!(hits.iter().any(|hit| hit.display_value == "US"));
    }

    #[test]
    fn observed_values_purge_by_source_and_last_observed_at() {
        let temp = tempdir().expect("tempdir");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let source = crate::sources::SourceName::parse("notion").expect("source");
        let store = SearchIndexStore::open(temp.path().join("tantivy")).expect("store");

        store
            .upsert_observed_values(
                &workspace,
                vec![
                    observed_record("stale-value", 1),
                    ObservedValueRecord {
                        source_name: "notion".to_string(),
                        display_value: "notion-value".to_string(),
                        searchable_text: "notion page notion-value".to_string(),
                        ..observed_record("notion-value", 1)
                    },
                ],
            )
            .expect("upsert observed");

        store
            .set_last_observed_at_for_test("stale-value", "2000-01-01T00:00:00.000Z")
            .expect("age observed state");

        store
            .purge_observed_values_before("2001-01-01T00:00:00.000Z")
            .expect("purge stale");
        assert!(
            store
                .search_observed_values(&workspace, &["stale-value".to_string()], 10)
                .expect("search stale")
                .is_empty()
        );
        assert!(
            !store
                .search_observed_values(&workspace, &["notion-value".to_string()], 10)
                .expect("search fresh")
                .is_empty()
        );

        store
            .delete_observed_values_for_source(&workspace, &source)
            .expect("delete source values");
        assert!(
            store
                .search_observed_values(&workspace, &["notion-value".to_string()], 10)
                .expect("search deleted")
                .is_empty()
        );
    }

    #[test]
    fn observed_staleness_purge_keeps_reobserved_values() {
        let temp = tempdir().expect("tempdir");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let store = SearchIndexStore::open(temp.path().join("tantivy")).expect("store");

        store
            .upsert_observed_values(&workspace, vec![observed_record("stable-value", 1)])
            .expect("upsert observed");
        store
            .set_last_observed_at_for_test("stable-value", "2000-01-01T00:00:00.000Z")
            .expect("age observed state");
        store
            .upsert_observed_values(&workspace, vec![observed_record("stable-value", 1)])
            .expect("reobserve value");
        store
            .purge_observed_values_before("2001-01-01T00:00:00.000Z")
            .expect("purge stale");

        assert!(
            !store
                .search_observed_values(&workspace, &["stable-value".to_string()], 10)
                .expect("search stable")
                .is_empty()
        );
        assert_eq!(
            store
                .observed_count_for_test("stable-value")
                .expect("observed count"),
            Some(2)
        );
    }

    #[test]
    fn observed_search_filters_by_live_source_scope() {
        let temp = tempdir().expect("tempdir");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let store = SearchIndexStore::open(temp.path().join("tantivy")).expect("store");
        store
            .upsert_observed_values(
                &workspace,
                vec![
                    ObservedValueRecord {
                        source_scope_id: "old-github-scope".to_string(),
                        display_value: "old-scope-match".to_string(),
                        searchable_text: "github deployments service old-scope-match".to_string(),
                        ..observed_record("old-scope-match", 1)
                    },
                    observed_record("live-scope-match", 1),
                ],
            )
            .expect("upsert observed");
        let live_source_scopes =
            StdBTreeMap::from([("github".to_string(), "github-scope".to_string())]);

        assert!(
            store
                .search_observed_values_filtered(
                    &["old-scope-match".to_string()],
                    10,
                    &live_source_scopes,
                    "2001-01-01T00:00:00.000Z",
                )
                .expect("search old scope")
                .is_empty()
        );
        assert!(
            !store
                .search_observed_values_filtered(
                    &["live-scope-match".to_string()],
                    10,
                    &live_source_scopes,
                    "2001-01-01T00:00:00.000Z",
                )
                .expect("search live scope")
                .is_empty()
        );
    }

    #[test]
    fn observed_storage_budget_drops_queue_before_records() {
        let temp = tempdir().expect("tempdir");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let store = SearchIndexStore::open(temp.path().join("tantivy")).expect("store");
        store
            .upsert_observed_values(
                &workspace,
                vec![large_observed_record("durable-budget-value", 1024)],
            )
            .expect("upsert durable observed value");
        let durable_budget = store
            .observed_storage_bytes()
            .expect("durable observed storage bytes");
        store
            .enqueue_observed_values(
                &workspace,
                vec![large_observed_record("queued-budget-value", 16 * 1024)],
            )
            .expect("enqueue observed value");
        assert!(
            store
                .observed_storage_bytes()
                .expect("queued observed storage bytes")
                > durable_budget
        );

        let enforcement = store
            .enforce_observed_storage_budget(durable_budget)
            .expect("enforce observed storage budget");

        assert_eq!(enforcement.removed_queue_jobs, 1);
        assert_eq!(enforcement.removed_records, 0);
        assert!(!enforcement.budget_exceeded);
        let drain = store
            .drain_observed_value_queue_for(StdDuration::from_secs(1))
            .expect("drain observed queue");
        assert_eq!(drain.pending_jobs, 0);
        assert!(
            !store
                .search_observed_values(&workspace, &["durable-budget-value".to_string()], 10)
                .expect("search durable observed value")
                .is_empty()
        );
        assert!(
            store
                .search_observed_values(&workspace, &["queued-budget-value".to_string()], 10)
                .expect("search queued observed value")
                .is_empty()
        );
    }

    #[test]
    fn observed_storage_budget_counts_tantivy_search_tree_bytes() {
        let temp = tempdir().expect("tempdir");
        let store = SearchIndexStore::open(temp.path().join("tantivy")).expect("store");
        let tantivy_bytes =
            directory_size_excluding(store.path(), &observed_state_file_for_index(store.path()))
                .expect("tantivy bytes");

        assert!(tantivy_bytes > 0);
        assert!(
            store
                .observed_storage_bytes()
                .expect("observed storage bytes")
                >= tantivy_bytes
        );
    }

    #[test]
    fn observed_storage_budget_prunes_oldest_records() {
        let temp = tempdir().expect("tempdir");
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let store = SearchIndexStore::open(temp.path().join("tantivy")).expect("store");
        store
            .upsert_observed_values(
                &workspace,
                vec![
                    large_observed_record("old-budget-value", 128 * 1024),
                    observed_record("fresh-budget-value", 1),
                ],
            )
            .expect("upsert observed values");
        store
            .set_last_observed_at_for_test("old-budget-value", "2000-01-01T00:00:00.000Z")
            .expect("age old observed value");
        let before = store
            .observed_storage_bytes()
            .expect("observed storage bytes before pruning");

        let enforcement = store
            .enforce_observed_storage_budget(before.saturating_sub(1024))
            .expect("enforce observed storage budget");

        assert_eq!(enforcement.removed_records, 1);
        assert!(!enforcement.budget_exceeded);
        assert!(
            store
                .search_observed_values(&workspace, &["old-budget-value".to_string()], 10)
                .expect("search old observed value")
                .is_empty()
        );
        assert!(
            !store
                .search_observed_values(&workspace, &["fresh-budget-value".to_string()], 10)
                .expect("search fresh observed value")
                .is_empty()
        );
    }

    #[test]
    fn catalog_rebuild_skips_malformed_observed_state_records() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("tantivy");
        let store = SearchIndexStore::open(&path).expect("store");
        let database = store.observed_database().expect("observed database");
        let write_txn = database.begin_write().expect("write transaction");
        {
            let mut records = write_txn
                .open_table(OBSERVED_RECORDS_TABLE)
                .expect("records table");
            records
                .insert("malformed", [0xff, 0x00, 0x01].as_slice())
                .expect("insert malformed record");
        }
        write_txn.commit().expect("commit malformed state");
        drop(database);
        drop(store);

        SearchIndexStore::replace_catalog_index(
            &path,
            &CatalogInfo {
                tables: Vec::new(),
                table_functions: Vec::new(),
            },
        )
        .expect("replace catalog skips malformed observed state");
    }

    fn catalog_with_search_function() -> CatalogInfo {
        CatalogInfo {
            tables: Vec::new(),
            table_functions: vec![TableFunctionInfo {
                schema_name: "github".to_string(),
                function_name: "search_deployments".to_string(),
                description: "Search GitHub deployments".to_string(),
                arguments: vec![TableFunctionArgumentInfo {
                    name: "q".to_string(),
                    required: true,
                    values: Vec::new(),
                }],
                result_columns: vec![TableFunctionResultColumnInfo {
                    name: "sha".to_string(),
                    data_type: "Utf8".to_string(),
                    nullable: false,
                    description: "Deployment commit SHA".to_string(),
                }],
                kind: "search".to_string(),
                search_limits_json: None,
            }],
        }
    }

    fn catalog_with_uppercase_identifier() -> CatalogInfo {
        CatalogInfo {
            tables: Vec::new(),
            table_functions: vec![TableFunctionInfo {
                schema_name: "Search".to_string(),
                function_name: "Search_Issues".to_string(),
                description: "Search issues".to_string(),
                arguments: vec![TableFunctionArgumentInfo {
                    name: "Q".to_string(),
                    required: true,
                    values: Vec::new(),
                }],
                result_columns: vec![TableFunctionResultColumnInfo {
                    name: "ID".to_string(),
                    data_type: "Utf8".to_string(),
                    nullable: false,
                    description: "Issue identifier".to_string(),
                }],
                kind: "search".to_string(),
                search_limits_json: None,
            }],
        }
    }

    fn catalog_with_table_filters() -> CatalogInfo {
        CatalogInfo {
            tables: vec![TableInfo {
                schema_name: "github".to_string(),
                table_name: "issues".to_string(),
                description: "GitHub issues".to_string(),
                guide: "Filter by owner and repo; optionally filter by status.".to_string(),
                columns: Vec::new(),
                filters: vec![
                    TableFilterInfo {
                        name: "owner".to_string(),
                        mode: "equality".to_string(),
                        required: true,
                        data_type: "Utf8".to_string(),
                        description: "Repository owner".to_string(),
                    },
                    TableFilterInfo {
                        name: "status".to_string(),
                        mode: "equality".to_string(),
                        required: false,
                        data_type: "Utf8".to_string(),
                        description: "Optional issue status filter".to_string(),
                    },
                ],
                required_filters: vec!["owner".to_string()],
            }],
            table_functions: Vec::new(),
        }
    }

    fn observed_record(value: &str, observed_count: u64) -> ObservedValueRecord {
        ObservedValueRecord {
            source_scope_id: "github-scope".to_string(),
            source_name: "github".to_string(),
            surface_kind: ObservedValueSurfaceKind::Table,
            surface_name: "deployments".to_string(),
            column_name: "service".to_string(),
            normalized_value_key: format!("key:{value}"),
            display_value: value.to_string(),
            searchable_text: format!("github deployments service {value}"),
            suggested_operator: ObservedValueSuggestedOperator::Exact,
            observed_count,
        }
    }

    fn large_observed_record(value: &str, payload_bytes: usize) -> ObservedValueRecord {
        let payload = deterministic_payload(value, payload_bytes);
        ObservedValueRecord {
            display_value: value.to_string(),
            searchable_text: format!("github deployments service {value} {payload}"),
            ..observed_record(value, 1)
        }
    }

    fn deterministic_payload(label: &str, min_bytes: usize) -> String {
        let mut payload = String::new();
        let mut index = 0_u64;
        while payload.len() < min_bytes {
            write!(
                payload,
                "{label}-{index:016x}-{:016x} ",
                index.wrapping_mul(0x9e37_79b1_85eb_ca87)
            )
            .expect("write deterministic payload");
            index = index.saturating_add(1);
        }
        payload
    }

    #[test]
    fn observed_state_uses_redb_sidecar() {
        let temp = tempdir().expect("tempdir");
        let store = SearchIndexStore::open(temp.path().join("tantivy")).expect("store");

        assert_eq!(
            store.observed_state_file(),
            temp.path().join("observed_values.redb")
        );
    }
}
