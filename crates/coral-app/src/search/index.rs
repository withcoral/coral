//! Workspace-scoped Tantivy storage for Universal Search retrieval.

use std::fs;
use std::path::{Path, PathBuf};

use coral_engine::{
    CatalogInfo, ColumnInfo, TableFilterInfo, TableFunctionArgumentInfo, TableFunctionInfo,
    TableFunctionResultColumnInfo, TableInfo,
};
use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, BoostQuery, Occur, Query, QueryParser, TermQuery};
use tantivy::schema::{
    Field, IndexRecordOption, STORED, STRING, Schema, TextFieldIndexing, TextOptions, Value as _,
};
use tantivy::tokenizer::{LowerCaser, NgramTokenizer, TextAnalyzer};
use tantivy::{Index, IndexWriter, ReloadPolicy, TantivyDocument, Term};
use uuid::Uuid;

use crate::state::AppStateLayout;
use crate::storage::fs::ensure_dir;
use crate::workspaces::WorkspaceName;

const TRIGRAM_TOKENIZER: &str = "coral_trigram";
const TANTIVY_VERSION: &str = "0.26.1";
const WRITER_MEMORY_BUDGET_BYTES: usize = 50_000_000;
const MAX_RANK_SCORE: u32 = 1_000;

#[derive(Debug, Clone)]
pub(crate) struct SearchIndexStore {
    #[cfg(test)]
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

    pub(crate) fn workspace_index_is_usable(
        layout: &AppStateLayout,
        workspace_name: &WorkspaceName,
    ) -> bool {
        index_is_usable(&layout.search_index_dir(workspace_name))
    }

    pub(crate) fn open(path: impl Into<PathBuf>) -> Result<Self, SearchIndexError> {
        let path = path.into();
        ensure_dir(&path)?;

        let index = open_or_create_index(&path)?;
        Self::from_index(&path, index)
    }

    fn from_index(
        #[cfg_attr(
            not(test),
            expect(
                unused_variables,
                reason = "path is retained for unit-test search index assertions"
            )
        )]
        path: &Path,
        index: Index,
    ) -> Result<Self, SearchIndexError> {
        register_tokenizers(&index)?;
        let fields = SearchIndexFields::from_schema(&index.schema())?;
        Ok(Self {
            #[cfg(test)]
            path: path.to_path_buf(),
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
                ],
            );
            parser.set_field_boost(self.fields.name_text, 4.0);
            parser.set_field_boost(self.fields.qualified_name_text, 5.0);
            parser.set_field_boost(self.fields.description_text, 2.0);
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
            (self.fields.surface_name, 16.0),
            (self.fields.name, 12.0),
            (self.fields.qualified_name, 10.0),
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
    source_name: Field,
    schema_name: Field,
    surface_name: Field,
    name: Field,
    qualified_name: Field,
    data_type: Field,
    required: Field,
    description: Field,
    searchable_text: Field,
    name_text: Field,
    qualified_name_text: Field,
    description_text: Field,
    searchable_text_text: Field,
}

impl SearchIndexFields {
    fn from_schema(schema: &Schema) -> Result<Self, SearchIndexError> {
        Ok(Self {
            doc_key: required_field(schema, "doc_key")?,
            entity_kind: required_field(schema, "entity_kind")?,
            result_type: required_field(schema, "result_type")?,
            surface_kind: required_field(schema, "surface_kind")?,
            field_role: required_field(schema, "field_role")?,
            source_name: required_field(schema, "source_name")?,
            schema_name: required_field(schema, "schema_name")?,
            surface_name: required_field(schema, "surface_name")?,
            name: required_field(schema, "name")?,
            qualified_name: required_field(schema, "qualified_name")?,
            data_type: required_field(schema, "data_type")?,
            required: required_field(schema, "required")?,
            description: required_field(schema, "description")?,
            searchable_text: required_field(schema, "searchable_text")?,
            name_text: required_field(schema, "name_text")?,
            qualified_name_text: required_field(schema, "qualified_name_text")?,
            description_text: required_field(schema, "description_text")?,
            searchable_text_text: required_field(schema, "searchable_text_text")?,
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

    if let Err(error) = build_replacement_index(&replacement_path, catalog) {
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

fn build_replacement_index(path: &Path, catalog: &CatalogInfo) -> Result<(), SearchIndexError> {
    ensure_dir(path)?;
    let index = Index::create_in_dir(path, search_schema())?;
    let store = SearchIndexStore::from_index(path, index)?;
    let mut writer = store.writer()?;

    for record in catalog_entity_records(catalog) {
        writer.add_document(store.catalog_document(&record))?;
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
    builder.add_text_field("source_name", STRING | STORED);
    builder.add_text_field("schema_name", STRING | STORED);
    builder.add_text_field("surface_name", STRING | STORED);
    builder.add_text_field("name", STRING | STORED);
    builder.add_text_field("qualified_name", STRING | STORED);
    builder.add_text_field("data_type", STRING | STORED);
    builder.add_text_field("required", STRING | STORED);
    builder.add_text_field("description", stored_text_options());
    builder.add_text_field("searchable_text", stored_text_options());
    builder.add_text_field("name_text", trigram_text_options());
    builder.add_text_field("qualified_name_text", trigram_text_options());
    builder.add_text_field("description_text", trigram_text_options());
    builder.add_text_field("searchable_text_text", trigram_text_options());
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
        "source_name",
        "schema_name",
        "surface_name",
        "name",
        "qualified_name",
        "data_type",
        "required",
        "description",
        "searchable_text",
        "name_text",
        "qualified_name_text",
        "description_text",
        "searchable_text_text",
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
    use coral_engine::{
        CatalogInfo, TableFilterInfo, TableFunctionArgumentInfo, TableFunctionInfo,
        TableFunctionResultColumnInfo, TableInfo,
    };
    use tempfile::tempdir;

    use super::{
        CatalogSearchFieldRole, CatalogSearchResultType, SearchIndexStore, tantivy_query_text,
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
}
