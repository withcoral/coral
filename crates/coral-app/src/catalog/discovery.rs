//! Workspace-scoped catalog discovery operations.

use std::{cmp::Reverse, collections::BTreeSet};

use coral_engine::{
    CatalogInfo, ColumnInfo, TableFunctionInfo, TableFunctionResultColumnInfo, TableInfo,
};
use regex::{Regex, RegexBuilder};

use crate::bootstrap::AppError;
use crate::query::manager::{QueryManager, QueryManagerError};
use crate::workspaces::WorkspaceName;

const DEFAULT_SEARCH_LIMIT: u32 = 20;
const MAX_SEARCH_LIMIT: u32 = 100;
const DEFAULT_COLUMN_LIMIT: u32 = 50;
const MAX_COLUMN_LIMIT: u32 = 200;
const COLUMN_PREVIEW_LIMIT: usize = 8;
const MAX_METADATA_PATTERN_BYTES: usize = 256;
const REGEX_SIZE_LIMIT_BYTES: usize = 1 << 20;
const MISSING_TABLE_SUGGESTION_LIMIT: usize = 10;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Pagination {
    pub(crate) limit: u32,
    pub(crate) offset: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Page<T> {
    pub(crate) items: Vec<T>,
    pub(crate) total: u32,
    pub(crate) limit: u32,
    pub(crate) offset: u32,
    pub(crate) has_more: bool,
    pub(crate) next_offset: Option<u32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CatalogCounts {
    pub(crate) table_count: u32,
    pub(crate) table_function_count: u32,
}

#[derive(Clone, Debug)]
pub(crate) struct CatalogPage {
    pub(crate) items: Page<CatalogItem>,
    pub(crate) counts: CatalogCounts,
}

#[derive(Clone, Debug)]
pub(crate) enum CatalogItem {
    Table(TableInfo),
    TableFunction(TableFunctionInfo),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CatalogItemKind {
    Table,
    TableFunction,
}

#[derive(Clone, Debug)]
pub(crate) struct CatalogSearchResult {
    pub(crate) item: CatalogItem,
    pub(crate) matched_fields: Vec<CatalogMetadataField>,
    pub(crate) table_column_preview: Option<TableColumnPreview>,
}

#[derive(Clone, Debug)]
pub(crate) struct TableColumnPreview {
    pub(crate) column_count: u32,
    pub(crate) columns: Vec<TableColumnPreviewColumn>,
    pub(crate) omitted_column_count: u32,
}

#[derive(Clone, Debug)]
pub(crate) struct TableColumnPreviewColumn {
    pub(crate) column: ColumnInfo,
    pub(crate) matched_fields: Vec<ColumnMetadataField>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum CatalogMatchRank {
    SchemaOnly,
    Guide,
    Description,
    QueryFields,
    Name,
    ExactName,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum CatalogLiteralMatchQuality {
    Neutral,
    QueryField,
    TableNameSubstring,
    TableNameTokenPlural,
    TableNameToken,
    TableNamePlural,
    TableNameExact,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CatalogMetadataField {
    SchemaName,
    TableName,
    FunctionName,
    Name,
    Description,
    Guide,
    RequiredFilters,
    Columns,
    Arguments,
    ResultColumns,
}

impl CatalogMetadataField {
    pub(crate) fn as_proto_name(self) -> &'static str {
        match self {
            Self::SchemaName => "schema_name",
            Self::TableName => "table_name",
            Self::FunctionName => "function_name",
            Self::Name => "name",
            Self::Description => "description",
            Self::Guide => "guide",
            Self::RequiredFilters => "required_filters",
            Self::Columns => "columns",
            Self::Arguments => "arguments",
            Self::ResultColumns => "result_columns",
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum DescribeTableResult {
    Found(TableInfo),
    Missing(MissingTableContext),
}

#[derive(Clone, Debug)]
pub(crate) struct MissingTableContext {
    pub(crate) suggestions: Vec<TableInfo>,
    pub(crate) available_schemas: Vec<String>,
    pub(crate) same_schema_tables: Vec<TableInfo>,
}

#[derive(Clone, Debug)]
pub(crate) struct ColumnSearchResult {
    pub(crate) column: ColumnInfo,
    pub(crate) matched_fields: Vec<ColumnMetadataField>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ColumnMetadataField {
    ColumnName,
    Description,
    DataType,
}

impl ColumnMetadataField {
    pub(crate) fn as_proto_name(self) -> &'static str {
        match self {
            Self::ColumnName => "column_name",
            Self::Description => "description",
            Self::DataType => "data_type",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CatalogTableRef<'a> {
    pub(crate) schema_name: &'a str,
    pub(crate) table_name: &'a str,
}

impl<'a> CatalogTableRef<'a> {
    pub(crate) fn new(schema_name: &'a str, table_name: &'a str) -> Self {
        Self {
            schema_name,
            table_name,
        }
    }
}

pub(crate) struct ListColumnsQuery<'a> {
    pub(crate) table_ref: CatalogTableRef<'a>,
    pub(crate) pattern: Option<&'a str>,
    pub(crate) ignore_case: bool,
    pub(crate) required_only: bool,
    pub(crate) pagination: Pagination,
}

#[derive(Clone)]
pub(crate) struct CatalogDiscovery {
    queries: QueryManager,
}

impl CatalogDiscovery {
    pub(crate) fn new(query_manager: QueryManager) -> Self {
        Self {
            queries: query_manager,
        }
    }

    pub(crate) async fn list_catalog(
        &self,
        workspace_name: &WorkspaceName,
        schema_name: Option<&str>,
        kind: Option<CatalogItemKind>,
        pagination: Pagination,
    ) -> Result<CatalogPage, QueryManagerError> {
        let catalog = self
            .queries
            .list_catalog(workspace_name, schema_name)
            .await?;
        let counts = catalog_counts(&catalog);
        let items = catalog_items(catalog, kind);
        Ok(CatalogPage {
            items: page_items(items, pagination),
            counts,
        })
    }

    async fn searchable_catalog_items(
        &self,
        workspace_name: &WorkspaceName,
        schema_name: Option<&str>,
        kind: Option<CatalogItemKind>,
    ) -> Result<Vec<CatalogItem>, QueryManagerError> {
        let catalog = self
            .queries
            .list_catalog(workspace_name, schema_name)
            .await?;
        Ok(searchable_catalog_items(catalog, kind))
    }

    pub(crate) async fn describe_table(
        &self,
        workspace_name: &WorkspaceName,
        table_ref: CatalogTableRef<'_>,
    ) -> Result<DescribeTableResult, QueryManagerError> {
        let table_lookup = self
            .queries
            .describe_table(workspace_name, table_ref.schema_name, table_ref.table_name)
            .await?;
        if let Some(table) = table_lookup.table {
            return Ok(DescribeTableResult::Found(table));
        }

        let tables = table_lookup.missing_context_tables;
        let available_schemas = tables
            .iter()
            .map(|table| table.schema_name.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let same_schema_tables = tables
            .iter()
            .filter(|table| table.schema_name == table_ref.schema_name)
            .take(MISSING_TABLE_SUGGESTION_LIMIT)
            .cloned()
            .collect::<Vec<_>>();
        let suggestions = missing_table_suggestions(&tables, table_ref, &same_schema_tables);
        Ok(DescribeTableResult::Missing(MissingTableContext {
            suggestions,
            available_schemas,
            same_schema_tables,
        }))
    }
}

fn catalog_items(catalog: CatalogInfo, kind: Option<CatalogItemKind>) -> Vec<CatalogItem> {
    let mut items = searchable_catalog_items(catalog, kind);
    for item in &mut items {
        clear_catalog_item_columns(item);
    }
    items
}

fn searchable_catalog_items(
    catalog: CatalogInfo,
    kind: Option<CatalogItemKind>,
) -> Vec<CatalogItem> {
    let mut items = Vec::with_capacity(catalog.tables.len() + catalog.table_functions.len());
    if kind.is_none_or(|kind| kind == CatalogItemKind::Table) {
        items.extend(catalog.tables.into_iter().map(CatalogItem::Table));
    }
    if kind.is_none_or(|kind| kind == CatalogItemKind::TableFunction) {
        items.extend(
            catalog
                .table_functions
                .into_iter()
                .map(CatalogItem::TableFunction),
        );
    }
    items.sort_by(|left, right| catalog_item_sort_key(left).cmp(&catalog_item_sort_key(right)));
    items
}

fn clear_catalog_item_columns(item: &mut CatalogItem) {
    if let CatalogItem::Table(table) = item {
        table.columns.clear();
    }
}

fn catalog_counts(catalog: &CatalogInfo) -> CatalogCounts {
    CatalogCounts {
        table_count: u32::try_from(catalog.tables.len()).unwrap_or(u32::MAX),
        table_function_count: u32::try_from(catalog.table_functions.len()).unwrap_or(u32::MAX),
    }
}

impl CatalogDiscovery {
    pub(crate) async fn search_catalog(
        &self,
        workspace_name: &WorkspaceName,
        pattern: &str,
        schema_name: Option<&str>,
        kind: Option<CatalogItemKind>,
        ignore_case: bool,
        pagination: Pagination,
    ) -> Result<Page<CatalogSearchResult>, QueryManagerError> {
        let regex = compile_metadata_regex(pattern, ignore_case).map_err(QueryManagerError::App)?;
        let items = self
            .searchable_catalog_items(workspace_name, schema_name, kind)
            .await?;
        let literal_terms = simple_literal_alternatives(pattern);
        let preferred_schemas = preferred_source_schemas(&items, &regex);
        let mut matches = items
            .into_iter()
            .enumerate()
            .filter_map(|(original_position, mut item)| {
                let matched_fields = catalog_item_matched_fields(&item, &regex);
                if matched_fields.is_empty() {
                    return None;
                }
                let source_preferred = preferred_schemas.contains(catalog_item_schema_name(&item));
                let rank = catalog_match_rank(&item, &matched_fields, &regex);
                let quality = catalog_literal_match_quality(
                    &item,
                    &matched_fields,
                    literal_terms.as_deref(),
                    source_preferred || schema_name.is_some(),
                );
                let table_column_preview = match &item {
                    CatalogItem::Table(table) => Some(table_column_preview(table, &regex)),
                    CatalogItem::TableFunction(_) => None,
                };
                clear_catalog_item_columns(&mut item);
                Some((
                    source_preferred,
                    rank,
                    quality,
                    original_position,
                    CatalogSearchResult {
                        item,
                        matched_fields,
                        table_column_preview,
                    },
                ))
            })
            .collect::<Vec<_>>();
        matches.sort_by_key(|(source_preferred, rank, quality, original_position, _)| {
            (
                Reverse(*source_preferred),
                Reverse(*rank),
                Reverse(*quality),
                *original_position,
            )
        });
        let matches = matches
            .into_iter()
            .map(|(_, _, _, _, result)| result)
            .collect::<Vec<_>>();
        Ok(page_items(matches, pagination))
    }

    pub(crate) async fn list_columns(
        &self,
        workspace_name: &WorkspaceName,
        query: ListColumnsQuery<'_>,
    ) -> Result<Option<Page<ColumnSearchResult>>, QueryManagerError> {
        let table = self
            .queries
            .list_tables(
                workspace_name,
                Some(query.table_ref.schema_name),
                Some(query.table_ref.table_name),
            )
            .await?
            .into_iter()
            .find(|table| {
                table.schema_name == query.table_ref.schema_name
                    && table.table_name == query.table_ref.table_name
            });
        let Some(table) = table else {
            return Ok(None);
        };

        let regex = query
            .pattern
            .map(|pattern| compile_metadata_regex(pattern, query.ignore_case))
            .transpose()
            .map_err(QueryManagerError::App)?;
        let matches = table
            .columns
            .into_iter()
            .filter(|column| !query.required_only || column.is_required_filter)
            .filter_map(|column| {
                let matched_fields = regex
                    .as_ref()
                    .map_or_else(Vec::new, |regex| column_matched_fields(&column, regex));
                if regex.is_some() && matched_fields.is_empty() {
                    None
                } else {
                    Some(ColumnSearchResult {
                        column,
                        matched_fields,
                    })
                }
            })
            .collect();
        Ok(Some(page_items(matches, query.pagination)))
    }
}

fn catalog_item_sort_key(item: &CatalogItem) -> (&str, &str, &'static str) {
    match item {
        CatalogItem::Table(table) => (&table.schema_name, &table.table_name, "table"),
        CatalogItem::TableFunction(function) => (
            &function.schema_name,
            &function.function_name,
            "table_function",
        ),
    }
}

fn catalog_item_schema_name(item: &CatalogItem) -> &str {
    match item {
        CatalogItem::Table(table) => table.schema_name.as_str(),
        CatalogItem::TableFunction(function) => function.schema_name.as_str(),
    }
}

fn preferred_source_schemas(items: &[CatalogItem], regex: &Regex) -> BTreeSet<String> {
    let visible_schemas = items
        .iter()
        .map(catalog_item_schema_name)
        .collect::<BTreeSet<_>>();
    if visible_schemas.len() <= 1 {
        return BTreeSet::new();
    }

    let preferred_schemas = visible_schemas
        .iter()
        .filter(|schema| regex_matches_entire_value(regex, schema))
        .map(|schema| (*schema).to_string())
        .collect::<BTreeSet<_>>();
    if preferred_schemas.is_empty() || preferred_schemas.len() == visible_schemas.len() {
        BTreeSet::new()
    } else {
        preferred_schemas
    }
}

fn simple_literal_alternatives(pattern: &str) -> Option<Vec<String>> {
    let mut terms = pattern
        .split('|')
        .map(|term| {
            let trimmed = term.trim();
            if term.is_empty()
                || term != trimmed
                || !term.chars().all(|character| {
                    character.is_ascii_alphanumeric() || character == '_' || character == '-'
                })
            {
                None
            } else {
                Some(term.to_ascii_lowercase())
            }
        })
        .collect::<Option<Vec<_>>>()?;
    terms.sort();
    terms.dedup();
    (!terms.is_empty()).then_some(terms)
}

fn catalog_literal_match_quality(
    item: &CatalogItem,
    matched_fields: &[CatalogMetadataField],
    literal_terms: Option<&[String]>,
    quality_enabled: bool,
) -> CatalogLiteralMatchQuality {
    let Some(literal_terms) = literal_terms else {
        return CatalogLiteralMatchQuality::Neutral;
    };
    if !quality_enabled
        || matched_fields
            .iter()
            .all(|field| *field == CatalogMetadataField::SchemaName)
    {
        return CatalogLiteralMatchQuality::Neutral;
    }

    let (schema_name, item_name) = catalog_item_name_parts(item);
    let schema_name = schema_name.to_ascii_lowercase();
    let terms = literal_terms
        .iter()
        .filter(|term| term.as_str() != schema_name)
        .map(String::as_str)
        .collect::<Vec<_>>();
    if terms.is_empty() {
        return CatalogLiteralMatchQuality::Neutral;
    }

    let mut quality = CatalogLiteralMatchQuality::Neutral;
    if catalog_item_name_fields_match(matched_fields) {
        quality = quality.max(name_literal_match_quality(item_name, &terms));
    }
    if catalog_item_query_fields_match(matched_fields) {
        quality = quality.max(query_field_literal_match_quality(item, &terms));
    }
    quality
}

fn catalog_item_name_fields_match(matched_fields: &[CatalogMetadataField]) -> bool {
    matched_fields.iter().any(|field| {
        matches!(
            field,
            CatalogMetadataField::TableName
                | CatalogMetadataField::FunctionName
                | CatalogMetadataField::Name
        )
    })
}

fn catalog_item_query_fields_match(matched_fields: &[CatalogMetadataField]) -> bool {
    matched_fields.iter().any(|field| {
        matches!(
            field,
            CatalogMetadataField::RequiredFilters
                | CatalogMetadataField::Columns
                | CatalogMetadataField::Arguments
                | CatalogMetadataField::ResultColumns
        )
    })
}

fn name_literal_match_quality(name: &str, terms: &[&str]) -> CatalogLiteralMatchQuality {
    let name = name.to_ascii_lowercase();
    let tokens = search_tokens(&name);
    terms
        .iter()
        .map(|term| literal_name_quality(&name, &tokens, term))
        .max()
        .unwrap_or(CatalogLiteralMatchQuality::Neutral)
}

fn literal_name_quality(name: &str, tokens: &[&str], term: &str) -> CatalogLiteralMatchQuality {
    if name == term {
        return CatalogLiteralMatchQuality::TableNameExact;
    }
    if plural_variants_match(name, term) {
        return CatalogLiteralMatchQuality::TableNamePlural;
    }
    if tokens.iter().any(|token| token == &term) {
        return CatalogLiteralMatchQuality::TableNameToken;
    }
    if tokens
        .iter()
        .any(|token| plural_variants_match(token, term))
    {
        return CatalogLiteralMatchQuality::TableNameTokenPlural;
    }
    if term.len() >= 3 && name.contains(term) {
        return CatalogLiteralMatchQuality::TableNameSubstring;
    }
    CatalogLiteralMatchQuality::Neutral
}

fn query_field_literal_match_quality(
    item: &CatalogItem,
    terms: &[&str],
) -> CatalogLiteralMatchQuality {
    let matches = match item {
        CatalogItem::Table(table) => {
            table
                .required_filters
                .iter()
                .any(|filter| value_has_literal_token_match(filter, terms))
                || table
                    .columns
                    .iter()
                    .any(|column| column_has_literal_token_match(column, terms))
        }
        CatalogItem::TableFunction(function) => {
            function.arguments.iter().any(|argument| {
                value_has_literal_token_match(&argument.name, terms)
                    || argument
                        .values
                        .iter()
                        .any(|value| value_has_literal_token_match(value, terms))
            }) || function
                .result_columns
                .iter()
                .any(|column| result_column_has_literal_token_match(column, terms))
        }
    };
    if matches {
        CatalogLiteralMatchQuality::QueryField
    } else {
        CatalogLiteralMatchQuality::Neutral
    }
}

fn column_has_literal_token_match(column: &ColumnInfo, terms: &[&str]) -> bool {
    value_has_literal_token_match(&column.name, terms)
        || value_has_literal_token_match(&column.data_type, terms)
        || value_has_literal_token_match(&column.description, terms)
}

fn result_column_has_literal_token_match(
    column: &TableFunctionResultColumnInfo,
    terms: &[&str],
) -> bool {
    value_has_literal_token_match(&column.name, terms)
        || value_has_literal_token_match(&column.data_type, terms)
        || value_has_literal_token_match(&column.description, terms)
}

fn value_has_literal_token_match(value: &str, terms: &[&str]) -> bool {
    let value = value.to_ascii_lowercase();
    let tokens = search_tokens(&value);
    terms.iter().any(|term| {
        tokens
            .iter()
            .any(|token| token == term || plural_variants_match(token, term))
            || term.len() >= 3 && value.contains(term)
    })
}

fn search_tokens(value: &str) -> Vec<&str> {
    value
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .collect()
}

fn plural_variants_match(left: &str, right: &str) -> bool {
    if left.len().min(right.len()) < 3 {
        return false;
    }
    left.strip_suffix('s') == Some(right)
        || right.strip_suffix('s') == Some(left)
        || left.strip_suffix("es") == Some(right)
        || right.strip_suffix("es") == Some(left)
}

pub(crate) fn search_pagination(pagination: Option<Pagination>) -> Result<Pagination, AppError> {
    pagination_with_limits(pagination, DEFAULT_SEARCH_LIMIT, MAX_SEARCH_LIMIT)
}

pub(crate) fn column_pagination(pagination: Option<Pagination>) -> Result<Pagination, AppError> {
    pagination_with_limits(pagination, DEFAULT_COLUMN_LIMIT, MAX_COLUMN_LIMIT)
}

fn pagination_with_limits(
    pagination: Option<Pagination>,
    default_limit: u32,
    max_limit: u32,
) -> Result<Pagination, AppError> {
    let pagination = pagination.unwrap_or(Pagination {
        limit: default_limit,
        offset: 0,
    });
    let limit = if pagination.limit == 0 {
        default_limit
    } else {
        pagination.limit
    };
    if limit > max_limit {
        return Err(AppError::InvalidInput(format!(
            "pagination limit must be between 1 and {max_limit}"
        )));
    }
    Ok(Pagination {
        limit,
        offset: pagination.offset,
    })
}

pub(crate) fn compile_metadata_regex(pattern: &str, ignore_case: bool) -> Result<Regex, AppError> {
    if pattern.trim().is_empty() {
        return Err(AppError::InvalidInput(
            "argument 'pattern' must not be empty".to_string(),
        ));
    }
    if pattern.len() > MAX_METADATA_PATTERN_BYTES {
        return Err(AppError::InvalidInput(format!(
            "argument 'pattern' must be at most {MAX_METADATA_PATTERN_BYTES} bytes"
        )));
    }
    RegexBuilder::new(pattern)
        .case_insensitive(ignore_case)
        .size_limit(REGEX_SIZE_LIMIT_BYTES)
        .build()
        .map_err(|error| AppError::InvalidInput(format!("invalid regex pattern: {error}")))
}

fn catalog_item_matched_fields(item: &CatalogItem, regex: &Regex) -> Vec<CatalogMetadataField> {
    match item {
        CatalogItem::Table(table) => table_matched_fields(table, regex),
        CatalogItem::TableFunction(function) => table_function_matched_fields(function, regex),
    }
}

fn catalog_match_rank(
    item: &CatalogItem,
    matched_fields: &[CatalogMetadataField],
    regex: &Regex,
) -> CatalogMatchRank {
    if catalog_item_exact_name_match(item, regex) {
        return CatalogMatchRank::ExactName;
    }
    if catalog_item_name_match(item, regex) {
        return CatalogMatchRank::Name;
    }
    if matched_fields.iter().any(|field| {
        matches!(
            field,
            CatalogMetadataField::RequiredFilters
                | CatalogMetadataField::Columns
                | CatalogMetadataField::Arguments
                | CatalogMetadataField::ResultColumns
        )
    }) {
        return CatalogMatchRank::QueryFields;
    }
    if matched_fields.contains(&CatalogMetadataField::Description) {
        return CatalogMatchRank::Description;
    }
    if matched_fields.contains(&CatalogMetadataField::Guide) {
        return CatalogMatchRank::Guide;
    }
    CatalogMatchRank::SchemaOnly
}

fn catalog_item_exact_name_match(item: &CatalogItem, regex: &Regex) -> bool {
    if regex.as_str().trim() == ".*" {
        return false;
    }
    let (schema_name, item_name) = catalog_item_name_parts(item);
    let qualified_name = format!("{schema_name}.{item_name}");
    regex_matches_entire_value(regex, item_name)
        || regex_matches_entire_value(regex, qualified_name.as_str())
}

fn catalog_item_name_match(item: &CatalogItem, regex: &Regex) -> bool {
    let (schema_name, item_name) = catalog_item_name_parts(item);
    if regex.is_match(item_name) {
        return true;
    }
    let qualified_name = format!("{schema_name}.{item_name}");
    qualified_name_match_touches_item_name(regex, qualified_name.as_str(), schema_name)
}

fn catalog_item_name_parts(item: &CatalogItem) -> (&str, &str) {
    match item {
        CatalogItem::Table(table) => (&table.schema_name, &table.table_name),
        CatalogItem::TableFunction(function) => (&function.schema_name, &function.function_name),
    }
}

fn regex_matches_entire_value(regex: &Regex, value: &str) -> bool {
    regex
        .find(value)
        .is_some_and(|match_| match_.start() == 0 && match_.end() == value.len())
}

fn qualified_name_match_touches_item_name(
    regex: &Regex,
    qualified_name: &str,
    schema_name: &str,
) -> bool {
    let item_name_start = schema_name.len() + 1;
    regex
        .find_iter(qualified_name)
        .any(|match_| match_.end() > item_name_start)
}

fn table_matched_fields(table: &TableInfo, regex: &Regex) -> Vec<CatalogMetadataField> {
    let name = format!("{}.{}", table.schema_name, table.table_name);
    let candidates = [
        (CatalogMetadataField::SchemaName, table.schema_name.as_str()),
        (CatalogMetadataField::TableName, table.table_name.as_str()),
        (CatalogMetadataField::Name, name.as_str()),
        (
            CatalogMetadataField::Description,
            table.description.as_str(),
        ),
        (CatalogMetadataField::Guide, table.guide.as_str()),
    ];
    let mut matches = candidates
        .into_iter()
        .filter_map(|(field, value)| regex.is_match(value).then_some(field))
        .collect::<Vec<_>>();
    if table
        .required_filters
        .iter()
        .any(|filter| regex.is_match(filter))
    {
        matches.push(CatalogMetadataField::RequiredFilters);
    }
    if table
        .columns
        .iter()
        .any(|column| !column_matched_fields(column, regex).is_empty())
    {
        matches.push(CatalogMetadataField::Columns);
    }
    matches
}

fn table_function_matched_fields(
    function: &TableFunctionInfo,
    regex: &Regex,
) -> Vec<CatalogMetadataField> {
    let name = format!("{}.{}", function.schema_name, function.function_name);
    let candidates = [
        (
            CatalogMetadataField::SchemaName,
            function.schema_name.as_str(),
        ),
        (
            CatalogMetadataField::FunctionName,
            function.function_name.as_str(),
        ),
        (CatalogMetadataField::Name, name.as_str()),
        (
            CatalogMetadataField::Description,
            function.description.as_str(),
        ),
    ];
    let mut matches = candidates
        .into_iter()
        .filter_map(|(field, value)| regex.is_match(value).then_some(field))
        .collect::<Vec<_>>();
    if function.arguments.iter().any(|argument| {
        regex.is_match(&argument.name) || argument.values.iter().any(|value| regex.is_match(value))
    }) {
        matches.push(CatalogMetadataField::Arguments);
    }
    if function.result_columns.iter().any(|column| {
        regex.is_match(&column.name)
            || regex.is_match(&column.data_type)
            || regex.is_match(&column.description)
    }) {
        matches.push(CatalogMetadataField::ResultColumns);
    }
    matches
}

fn column_matched_fields(column: &ColumnInfo, regex: &Regex) -> Vec<ColumnMetadataField> {
    let candidates = [
        (ColumnMetadataField::ColumnName, column.name.as_str()),
        (
            ColumnMetadataField::Description,
            column.description.as_str(),
        ),
        (ColumnMetadataField::DataType, column.data_type.as_str()),
    ];
    candidates
        .into_iter()
        .filter_map(|(field, value)| regex.is_match(value).then_some(field))
        .collect()
}

fn table_column_preview(table: &TableInfo, regex: &Regex) -> TableColumnPreview {
    let mut selected_columns = Vec::new();
    push_column_preview_columns(table, &mut selected_columns, |table, column| {
        column_is_required_filter(table, column)
    });
    push_column_preview_columns(table, &mut selected_columns, |_, column| {
        !column_matched_fields(column, regex).is_empty()
    });
    push_column_preview_columns(table, &mut selected_columns, |_, column| {
        is_query_starter_column(&column.name)
    });

    selected_columns.sort_by_key(|column| column.ordinal_position);
    let columns = selected_columns
        .into_iter()
        .map(|column| {
            let matched_fields = column_matched_fields(&column, regex);
            TableColumnPreviewColumn {
                column,
                matched_fields,
            }
        })
        .collect::<Vec<_>>();
    let column_count = u32::try_from(table.columns.len()).unwrap_or(u32::MAX);
    let preview_count = u32::try_from(columns.len()).unwrap_or(u32::MAX);
    TableColumnPreview {
        column_count,
        columns,
        omitted_column_count: column_count.saturating_sub(preview_count),
    }
}

fn push_column_preview_columns(
    table: &TableInfo,
    selected_columns: &mut Vec<ColumnInfo>,
    predicate: impl Fn(&TableInfo, &ColumnInfo) -> bool,
) {
    if selected_columns.len() >= COLUMN_PREVIEW_LIMIT {
        return;
    }
    for column in &table.columns {
        if selected_columns.len() >= COLUMN_PREVIEW_LIMIT {
            return;
        }
        if selected_columns
            .iter()
            .any(|selected| selected.name == column.name)
        {
            continue;
        }
        if predicate(table, column) {
            selected_columns.push(column.clone());
        }
    }
}

fn column_is_required_filter(table: &TableInfo, column: &ColumnInfo) -> bool {
    column.is_required_filter
        || table
            .required_filters
            .iter()
            .any(|filter| filter == &column.name)
}

fn is_query_starter_column(name: &str) -> bool {
    let original_name = name;
    let name = name.to_ascii_lowercase();
    if name == "id"
        || name.ends_with("_id")
        || name.ends_with("-id")
        || original_name.ends_with("Id")
        || original_name.ends_with("ID")
    {
        return true;
    }
    let tokens_match = name
        .split(|character: char| !character.is_ascii_alphanumeric())
        .any(|token| {
            matches!(
                token,
                "name"
                    | "title"
                    | "status"
                    | "state"
                    | "url"
                    | "user"
                    | "login"
                    | "time"
                    | "date"
                    | "created"
                    | "updated"
                    | "timestamp"
            )
        });
    if tokens_match {
        return true;
    }
    let compound_tokens = [
        "name",
        "title",
        "status",
        "state",
        "url",
        "user",
        "login",
        "created",
        "updated",
        "timestamp",
    ];
    compound_tokens
        .into_iter()
        .any(|token| name.contains(token))
        || name.ends_with("_time")
        || name.ends_with("_date")
}

fn missing_table_suggestions(
    all_tables: &[TableInfo],
    table_ref: CatalogTableRef<'_>,
    same_schema_tables: &[TableInfo],
) -> Vec<TableInfo> {
    let suggestion_schema = (!same_schema_tables.is_empty()).then_some(table_ref.schema_name);
    let mut suggestions = all_tables
        .iter()
        .filter(|table| suggestion_schema.is_none_or(|schema| table.schema_name == schema))
        .filter(|table| table_metadata_contains_literal(table, table_ref.table_name))
        .take(MISSING_TABLE_SUGGESTION_LIMIT)
        .cloned()
        .collect::<Vec<_>>();
    if suggestions.is_empty() {
        suggestions.extend_from_slice(same_schema_tables);
    }
    suggestions
}

fn table_metadata_contains_literal(table: &TableInfo, literal: &str) -> bool {
    let literal = literal.trim();
    if literal.is_empty() {
        return false;
    }
    let literal = literal.to_lowercase();
    let name = format!("{}.{}", table.schema_name, table.table_name);
    let candidates = [
        table.schema_name.as_str(),
        table.table_name.as_str(),
        name.as_str(),
        table.description.as_str(),
        table.guide.as_str(),
    ];
    candidates
        .into_iter()
        .any(|value| value.to_lowercase().contains(&literal))
        || table
            .required_filters
            .iter()
            .any(|filter| filter.to_lowercase().contains(&literal))
}

pub(crate) fn page_items<T>(items: Vec<T>, pagination: Pagination) -> Page<T> {
    let total = u32::try_from(items.len()).unwrap_or(u32::MAX);
    let offset = usize::try_from(pagination.offset).unwrap_or(usize::MAX);
    let limit = usize::try_from(pagination.limit).unwrap_or(usize::MAX);
    let items = if pagination.limit == 0 {
        items.into_iter().skip(offset).collect::<Vec<_>>()
    } else {
        items
            .into_iter()
            .skip(offset)
            .take(limit)
            .collect::<Vec<_>>()
    };
    let returned_count = u32::try_from(items.len()).unwrap_or(u32::MAX);
    let advanced_offset = pagination.offset.saturating_add(returned_count);
    let has_more = pagination.limit != 0 && advanced_offset < total;
    Page {
        items,
        total,
        limit: pagination.limit,
        offset: pagination.offset,
        has_more,
        next_offset: has_more.then_some(advanced_offset),
    }
}

#[cfg(test)]
mod tests {
    use super::{CatalogMetadataField, compile_metadata_regex, table_matched_fields};
    use coral_engine::TableInfo;

    fn table(required_filters: Vec<String>) -> TableInfo {
        TableInfo {
            schema_name: "github".to_string(),
            table_name: "Pull.Requests".to_string(),
            description: "Pull request table".to_string(),
            guide: "Query pull requests.".to_string(),
            columns: Vec::new(),
            required_filters,
        }
    }

    #[test]
    fn required_filters_match_each_filter_independently() {
        let summary = table(vec!["owner".to_string(), "repo".to_string()]);

        assert_eq!(
            table_matched_fields(&summary, &regex::Regex::new("^repo$").expect("regex")),
            vec![CatalogMetadataField::RequiredFilters]
        );
        assert!(
            table_matched_fields(&summary, &regex::Regex::new("r.r").expect("regex")).is_empty()
        );
    }

    #[test]
    fn empty_metadata_pattern_is_invalid() {
        compile_metadata_regex(" ", true).expect_err("empty pattern should fail");
    }
}
