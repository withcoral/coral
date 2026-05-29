//! Workspace-scoped Tantivy storage for Universal Search retrieval.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use chrono::{SecondsFormat, Utc};
use coral_engine::{
    CatalogInfo, ColumnInfo, TableFunctionArgumentInfo, TableFunctionInfo,
    TableFunctionResultColumnInfo, TableInfo,
};
use serde::{Deserialize, Serialize};
use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, Occur, Query, QueryParser, TermQuery};
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

pub(crate) const SEARCH_INDEX_SCHEMA_VERSION: u32 = 2;

const OBSERVED_STATE_FILE_NAME: &str = "observed_values.json";
const TRIGRAM_TOKENIZER: &str = "coral_trigram";
const TANTIVY_VERSION: &str = "0.26.1";
const WRITER_MEMORY_BUDGET_BYTES: usize = 50_000_000;

#[derive(Debug, Clone)]
pub(crate) struct SearchIndexStore {
    path: PathBuf,
    index: Index,
    fields: SearchIndexFields,
    capabilities: TantivySearchCapabilities,
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

    pub(crate) fn open_existing_workspace(
        layout: &AppStateLayout,
        workspace_name: &WorkspaceName,
    ) -> Result<Option<Self>, SearchIndexError> {
        let path = layout.search_index_dir(workspace_name);
        if !path.exists() {
            return Ok(None);
        }
        Self::open(path).map(Some)
    }

    pub(crate) fn open(path: impl Into<PathBuf>) -> Result<Self, SearchIndexError> {
        let path = path.into();
        ensure_dir(&path)?;

        let index = open_or_create_index(&path)?;
        Self::from_index(path, index)
    }

    fn from_index(path: PathBuf, index: Index) -> Result<Self, SearchIndexError> {
        register_tokenizers(&index)?;
        let fields = SearchIndexFields::from_schema(&index.schema())?;
        Ok(Self {
            path,
            index,
            fields,
            capabilities: TantivySearchCapabilities {
                tantivy_version: TANTIVY_VERSION.to_string(),
                tokenizer: TRIGRAM_TOKENIZER.to_string(),
            },
        })
    }

    #[cfg(test)]
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn capabilities(&self) -> &TantivySearchCapabilities {
        &self.capabilities
    }

    pub(crate) fn search_catalog(
        &self,
        _workspace_name: &WorkspaceName,
        terms: &[String],
        limit: usize,
    ) -> Result<Vec<CatalogSearchHit>, SearchIndexError> {
        let Some(query) = self.scoped_query("catalog", terms) else {
            return Ok(Vec::new());
        };
        let docs = self.search_documents(&query, limit)?;
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
        Ok(hits)
    }

    pub(crate) fn upsert_observed_values(
        &self,
        _workspace_name: &WorkspaceName,
        records: Vec<ObservedValueRecord>,
    ) -> Result<(), SearchIndexError> {
        if records.is_empty() {
            return Ok(());
        }

        let mut state = self.load_observed_state()?;
        let mut stored = state
            .records
            .into_iter()
            .map(|record| (record.doc_key(), record))
            .collect::<BTreeMap<_, _>>();
        let now = now_timestamp();
        let mut affected_keys = BTreeSet::new();

        for record in records {
            let key = observed_doc_key(
                &record.source_name,
                record.surface_kind,
                &record.surface_name,
                &record.column_name,
                &record.normalized_value_key,
            );
            affected_keys.insert(key.clone());
            stored
                .entry(key)
                .and_modify(|existing| {
                    existing.display_value.clone_from(&record.display_value);
                    existing.searchable_text.clone_from(&record.searchable_text);
                    existing.sensitivity_tier = record.sensitivity_tier.as_str().to_string();
                    existing.suggested_operator = record.suggested_operator.as_str().to_string();
                    existing.last_observed_at.clone_from(&now);
                    existing.observed_count = existing
                        .observed_count
                        .saturating_add(record.observed_count);
                })
                .or_insert_with(|| ObservedValueStoredRecord {
                    source_name: record.source_name,
                    surface_kind: record.surface_kind.as_str().to_string(),
                    surface_name: record.surface_name,
                    column_name: record.column_name,
                    normalized_value_key: record.normalized_value_key,
                    display_value: record.display_value,
                    searchable_text: record.searchable_text,
                    sensitivity_tier: record.sensitivity_tier.as_str().to_string(),
                    suggested_operator: record.suggested_operator.as_str().to_string(),
                    first_observed_at: now.clone(),
                    last_observed_at: now.clone(),
                    observed_count: record.observed_count,
                });
        }

        let mut writer = self.writer()?;
        for key in &affected_keys {
            let _opstamp = writer.delete_term(Term::from_field_text(self.fields.doc_key, key));
            if let Some(record) = stored.get(key) {
                writer.add_document(self.observed_document(record))?;
            }
        }
        writer.commit()?;

        state = ObservedValueState {
            schema_version: SEARCH_INDEX_SCHEMA_VERSION,
            records: stored.into_values().collect(),
        };
        self.write_observed_state(&state)?;
        Ok(())
    }

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
        let mut hits = docs
            .into_iter()
            .filter(|doc| doc_text(doc, self.fields.entity_kind) == "observed_value")
            .map(|doc| ObservedValueSearchHit {
                source_name: doc_text(&doc, self.fields.source_name),
                surface_name: doc_text(&doc, self.fields.surface_name),
                column_name: doc_text(&doc, self.fields.column_name),
                normalized_value_key: doc_text(&doc, self.fields.normalized_value_key),
                display_value: doc_text(&doc, self.fields.display_value),
                last_observed_at: doc_text(&doc, self.fields.last_observed_at),
                score: 0,
            })
            .collect::<Vec<_>>();
        assign_rank_scores(&mut hits, |hit, score| hit.score = score);
        Ok(hits)
    }

    pub(crate) fn delete_observed_values_for_source(
        &self,
        _workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<(), SearchIndexError> {
        let mut state = self.load_observed_state()?;
        let (removed, retained): (Vec<_>, Vec<_>) = state
            .records
            .into_iter()
            .partition(|record| record.source_name == source_name.as_str());
        if removed.is_empty() {
            return Ok(());
        }

        let mut writer = self.writer()?;
        for record in &removed {
            let _opstamp = writer.delete_term(Term::from_field_text(
                self.fields.doc_key,
                &record.doc_key(),
            ));
        }
        writer.commit()?;

        state.records = retained;
        self.write_observed_state(&state)?;
        Ok(())
    }

    pub(crate) fn purge_observed_values_before(
        &self,
        _workspace_name: &WorkspaceName,
        cutoff: &str,
    ) -> Result<(), SearchIndexError> {
        let mut state = self.load_observed_state()?;
        let (removed, retained): (Vec<_>, Vec<_>) = state
            .records
            .into_iter()
            .partition(|record| record.last_observed_at.as_str() < cutoff);
        if removed.is_empty() {
            return Ok(());
        }

        let mut writer = self.writer()?;
        for record in &removed {
            let _opstamp = writer.delete_term(Term::from_field_text(
                self.fields.doc_key,
                &record.doc_key(),
            ));
        }
        writer.commit()?;

        state.records = retained;
        self.write_observed_state(&state)?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn observed_count_for_test(
        &self,
        display_value: &str,
    ) -> Result<Option<u64>, SearchIndexError> {
        Ok(self
            .load_observed_state()?
            .records
            .into_iter()
            .find(|record| record.display_value == display_value)
            .map(|record| record.observed_count))
    }

    fn writer(&self) -> Result<IndexWriter, SearchIndexError> {
        Ok(self.index.writer(WRITER_MEMORY_BUDGET_BYTES)?)
    }

    fn scoped_query(&self, entity_kind: &'static str, terms: &[String]) -> Option<Box<dyn Query>> {
        let query_text = tantivy_query_text(terms)?;
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
        let kind_query = Box::new(TermQuery::new(
            Term::from_field_text(self.fields.entity_kind, entity_kind),
            IndexRecordOption::Basic,
        ));
        Some(Box::new(BooleanQuery::new(vec![
            (Occur::Must, kind_query),
            (Occur::Must, parsed_query),
        ])))
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

    fn observed_state_file(&self) -> PathBuf {
        observed_state_file_for_index(&self.path)
    }

    fn load_observed_state(&self) -> Result<ObservedValueState, SearchIndexError> {
        load_observed_state_for_index(&self.path)
    }

    fn write_observed_state(&self, state: &ObservedValueState) -> Result<(), SearchIndexError> {
        let path = self.observed_state_file();
        let temp_path = path.with_extension("json.tmp");
        fs::write(&temp_path, serde_json::to_vec_pretty(state)?)?;
        fs::rename(temp_path, path)?;
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
    Json(#[from] serde_json::Error),
    #[error("Tantivy search index schema is missing required field '{field}'")]
    MissingField { field: &'static str },
}

#[derive(Debug, Clone)]
pub(crate) struct CatalogSearchHit {
    pub(crate) entity_key: String,
    pub(crate) result_type: Option<CatalogSearchResultType>,
    pub(crate) surface_kind: Option<CatalogSearchSurfaceKind>,
    pub(crate) schema_name: String,
    pub(crate) surface_name: String,
    pub(crate) name: String,
    pub(crate) data_type: String,
    pub(crate) required: bool,
    pub(crate) description: String,
    pub(crate) matched_fields: Vec<String>,
    pub(crate) score: u32,
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

#[derive(Debug, Clone)]
pub(crate) struct ObservedValueRecord {
    pub(crate) source_name: String,
    pub(crate) surface_kind: ObservedValueSurfaceKind,
    pub(crate) surface_name: String,
    pub(crate) column_name: String,
    pub(crate) normalized_value_key: String,
    pub(crate) display_value: String,
    pub(crate) searchable_text: String,
    pub(crate) sensitivity_tier: ObservedValueSensitivityTier,
    pub(crate) suggested_operator: ObservedValueSuggestedOperator,
    pub(crate) observed_count: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct ObservedValueSearchHit {
    pub(crate) source_name: String,
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
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum ObservedValueSensitivityTier {
    LowRisk,
}

impl ObservedValueSensitivityTier {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::LowRisk => "low_risk",
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ObservedValueState {
    schema_version: u32,
    records: Vec<ObservedValueStoredRecord>,
}

impl Default for ObservedValueState {
    fn default() -> Self {
        Self {
            schema_version: SEARCH_INDEX_SCHEMA_VERSION,
            records: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ObservedValueStoredRecord {
    source_name: String,
    surface_kind: String,
    surface_name: String,
    column_name: String,
    normalized_value_key: String,
    display_value: String,
    searchable_text: String,
    sensitivity_tier: String,
    suggested_operator: String,
    first_observed_at: String,
    last_observed_at: String,
    observed_count: u64,
}

impl ObservedValueStoredRecord {
    fn doc_key(&self) -> String {
        observed_doc_key(
            &self.source_name,
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

#[derive(Debug)]
struct CatalogEntityRecord {
    entity_key: String,
    result_type: CatalogSearchResultType,
    surface_kind: CatalogSearchSurfaceKind,
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
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    ensure_dir(parent)?;

    let replacement_path = sibling_index_path(path, "rebuild");
    if replacement_path.exists() {
        fs::remove_dir_all(&replacement_path)?;
    }

    let observed_state = load_observed_state_for_index(path)?;
    if let Err(error) = build_replacement_index(&replacement_path, catalog, &observed_state) {
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

fn build_replacement_index(
    path: &Path,
    catalog: &CatalogInfo,
    observed_state: &ObservedValueState,
) -> Result<(), SearchIndexError> {
    ensure_dir(path)?;
    let index = Index::create_in_dir(path, search_schema())?;
    let store = SearchIndexStore::from_index(path.to_path_buf(), index)?;
    let mut writer = store.writer()?;

    for record in catalog_entity_records(catalog) {
        writer.add_document(store.catalog_document(&record))?;
    }
    for record in &observed_state.records {
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

fn load_observed_state_for_index(path: &Path) -> Result<ObservedValueState, SearchIndexError> {
    let path = observed_state_file_for_index(path);
    if !path.exists() {
        return Ok(ObservedValueState::default());
    }
    let contents = fs::read_to_string(path)?;
    let mut state: ObservedValueState = serde_json::from_str(&contents)?;
    if state.schema_version != SEARCH_INDEX_SCHEMA_VERSION {
        state = ObservedValueState::default();
    }
    Ok(state)
}

fn open_or_create_index(path: &Path) -> Result<Index, SearchIndexError> {
    let meta_file = path.join("meta.json");
    if meta_file.exists() {
        let index = Index::open_in_dir(path)?;
        if schema_has_required_fields(&index.schema()) {
            return Ok(index);
        }
        fs::remove_dir_all(path)?;
        ensure_dir(path)?;
    }
    Ok(Index::create_in_dir(path, search_schema())?)
}

fn search_schema() -> Schema {
    let mut builder = Schema::builder();
    builder.add_text_field("doc_key", STRING | STORED);
    builder.add_text_field("entity_kind", STRING | STORED);
    builder.add_text_field("result_type", STRING | STORED);
    builder.add_text_field("surface_kind", STRING | STORED);
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
    records.push(CatalogEntityRecord {
        entity_key: format!("catalog:table:{qualified_name}"),
        result_type: CatalogSearchResultType::CatalogTable,
        surface_kind: CatalogSearchSurfaceKind::Table,
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
            table.required_filters.join(" ").as_str(),
        ]),
    });

    for column in &table.columns {
        table_column_record(table, column, records);
    }
    for filter in &table.required_filters {
        table_required_filter_record(table, filter, records);
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

fn table_required_filter_record(
    table: &TableInfo,
    filter: &str,
    records: &mut Vec<CatalogEntityRecord>,
) {
    let surface_name = qualified_name(&table.schema_name, &table.table_name);
    records.push(CatalogEntityRecord {
        entity_key: format!("filter:table:{surface_name}:{filter}"),
        result_type: CatalogSearchResultType::ColumnHint,
        surface_kind: CatalogSearchSurfaceKind::Table,
        schema_name: table.schema_name.clone(),
        surface_name: table.table_name.clone(),
        name: filter.to_string(),
        qualified_name: format!("{surface_name}.{filter}"),
        data_type: String::new(),
        required: true,
        description: "Required table filter".to_string(),
        searchable_text: join_search_text([
            table.schema_name.as_str(),
            table.table_name.as_str(),
            filter,
            "required table filter",
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
    surface_kind: ObservedValueSurfaceKind,
    surface_name: &str,
    column_name: &str,
    normalized_value_key: &str,
) -> String {
    format!(
        "observed:{}:{}:{}:{}:{}",
        source_name,
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
        set_score(hit, hit_count.saturating_sub(position));
    }
}

#[cfg(test)]
mod tests {
    use coral_engine::{
        CatalogInfo, TableFunctionArgumentInfo, TableFunctionInfo, TableFunctionResultColumnInfo,
    };
    use tempfile::tempdir;

    use super::{
        CatalogSearchResultType, ObservedValueRecord, ObservedValueSensitivityTier,
        ObservedValueState, ObservedValueSuggestedOperator, ObservedValueSurfaceKind,
        SEARCH_INDEX_SCHEMA_VERSION, SearchIndexStore, tantivy_query_text,
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
            && hit.name == "sha"));

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

        let mut state = store.load_observed_state().expect("observed state");
        for record in &mut state.records {
            if record.source_name == "github" {
                record.last_observed_at = "2000-01-01T00:00:00.000Z".to_string();
            }
        }
        store
            .write_observed_state(&state)
            .expect("write aged observed state");

        store
            .purge_observed_values_before(&workspace, "2001-01-01T00:00:00.000Z")
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

    fn observed_record(value: &str, observed_count: u64) -> ObservedValueRecord {
        ObservedValueRecord {
            source_name: "github".to_string(),
            surface_kind: ObservedValueSurfaceKind::Table,
            surface_name: "deployments".to_string(),
            column_name: "service".to_string(),
            normalized_value_key: format!("key:{value}"),
            display_value: value.to_string(),
            searchable_text: format!("github deployments service {value}"),
            sensitivity_tier: ObservedValueSensitivityTier::LowRisk,
            suggested_operator: ObservedValueSuggestedOperator::Exact,
            observed_count,
        }
    }

    #[test]
    fn observed_state_version_matches_index_schema_version() {
        assert_eq!(
            ObservedValueState::default().schema_version,
            SEARCH_INDEX_SCHEMA_VERSION
        );
    }
}
