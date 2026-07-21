//! Workspace-scoped catalog discovery operations.

use std::collections::BTreeSet;

use coral_engine::{CatalogInfo, ColumnInfo, TableFunctionInfo, TableInfo};
use regex::{Regex, RegexBuilder};

use crate::bootstrap::AppError;
use crate::catalog::model::CatalogResolution;
use crate::query::QueryAttribution;
use crate::query::manager::{QueryManager, QueryManagerError};
use crate::workspaces::WorkspaceName;

const DEFAULT_SEARCH_LIMIT: u32 = 20;
const MAX_SEARCH_LIMIT: u32 = 100;
const DEFAULT_COLUMN_LIMIT: u32 = 50;
const MAX_COLUMN_LIMIT: u32 = 200;
const MAX_METADATA_PATTERN_BYTES: usize = 256;
const REGEX_SIZE_LIMIT_BYTES: usize = 1 << 20;
const MISSING_TABLE_SUGGESTION_LIMIT: usize = 10;
const DATAFUSION_DEFAULT_CATALOG: &str = "datafusion";

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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CatalogMetadataField {
    CatalogName,
    SchemaName,
    /// The composite `catalog.schema` reference of a database table, when a
    /// match spans the catalog/schema boundary rather than either part alone.
    QualifiedSchema,
    TableName,
    FunctionName,
    Name,
    Description,
    Guide,
    RequiredFilters,
    Arguments,
    ResultColumns,
}

impl CatalogMetadataField {
    pub(crate) fn as_proto_name(self) -> &'static str {
        match self {
            Self::CatalogName => "catalog_name",
            Self::SchemaName => "schema_name",
            Self::QualifiedSchema => "qualified_schema",
            Self::TableName => "table_name",
            Self::FunctionName => "function_name",
            Self::Name => "name",
            Self::Description => "description",
            Self::Guide => "guide",
            Self::RequiredFilters => "required_filters",
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
#[expect(
    clippy::struct_field_names,
    reason = "explicit SQL qualifier names keep catalog, schema, and table semantics unambiguous"
)]
pub(crate) struct CatalogTableRef<'a> {
    pub(crate) catalog_name: Option<&'a str>,
    pub(crate) schema_name: &'a str,
    pub(crate) table_name: &'a str,
}

impl<'a> CatalogTableRef<'a> {
    pub(crate) fn new(
        catalog_name: Option<&'a str>,
        schema_name: &'a str,
        table_name: &'a str,
    ) -> Self {
        Self {
            catalog_name: catalog_name
                .filter(|name| !name.eq_ignore_ascii_case(DATAFUSION_DEFAULT_CATALOG)),
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

pub(crate) struct SearchCatalogQuery<'a> {
    pub(crate) pattern: &'a str,
    pub(crate) catalog_name: Option<&'a str>,
    pub(crate) schema_name: Option<&'a str>,
    pub(crate) kind: Option<CatalogItemKind>,
    pub(crate) ignore_case: bool,
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
        catalog_name: Option<&str>,
        schema_name: Option<&str>,
        kind: Option<CatalogItemKind>,
        pagination: Pagination,
        attribution: &QueryAttribution,
    ) -> Result<CatalogPage, QueryManagerError> {
        let catalog = self
            .catalog_info(workspace_name, catalog_name, schema_name, attribution)
            .await?;
        let counts = catalog_counts(&catalog);
        let items = catalog_items(catalog, kind);
        Ok(CatalogPage {
            items: page_items(items, pagination),
            counts,
        })
    }

    async fn catalog_items(
        &self,
        workspace_name: &WorkspaceName,
        catalog_name: Option<&str>,
        schema_name: Option<&str>,
        kind: Option<CatalogItemKind>,
        attribution: &QueryAttribution,
    ) -> Result<Vec<CatalogItem>, QueryManagerError> {
        let catalog = self
            .catalog_info(workspace_name, catalog_name, schema_name, attribution)
            .await?;
        Ok(catalog_items(catalog, kind))
    }

    pub(crate) async fn catalog_info(
        &self,
        workspace_name: &WorkspaceName,
        catalog_name: Option<&str>,
        schema_name: Option<&str>,
        attribution: &QueryAttribution,
    ) -> Result<CatalogInfo, QueryManagerError> {
        self.queries
            .list_catalog(workspace_name, catalog_name, schema_name, attribution)
            .await
    }

    pub(crate) async fn resolve_catalog(
        &self,
        workspace_name: &WorkspaceName,
        attribution: &QueryAttribution,
    ) -> Result<CatalogResolution, QueryManagerError> {
        self.queries
            .resolve_catalog(workspace_name, None, None, attribution)
            .await
    }

    pub(crate) async fn describe_table(
        &self,
        workspace_name: &WorkspaceName,
        table_ref: CatalogTableRef<'_>,
        attribution: &QueryAttribution,
    ) -> Result<DescribeTableResult, QueryManagerError> {
        let table_lookup = self
            .queries
            .describe_table(
                workspace_name,
                table_ref.catalog_name,
                table_ref.schema_name,
                table_ref.table_name,
                attribution,
            )
            .await?;
        if let Some(table) = table_lookup.table {
            return Ok(DescribeTableResult::Found(table));
        }

        let tables = table_lookup.missing_context_tables;
        let available_schemas = available_table_schemas(&tables);
        let same_schema_tables = same_schema_tables(&tables, table_ref);
        let suggestions = missing_table_suggestions(&tables, table_ref, &same_schema_tables);
        Ok(DescribeTableResult::Missing(MissingTableContext {
            suggestions,
            available_schemas,
            same_schema_tables,
        }))
    }
}

fn catalog_items(catalog: CatalogInfo, kind: Option<CatalogItemKind>) -> Vec<CatalogItem> {
    let mut items = Vec::with_capacity(catalog.tables.len() + catalog.table_functions.len());
    if kind.is_none_or(|kind| kind == CatalogItemKind::Table) {
        items.extend(catalog.tables.into_iter().map(|mut table| {
            table.columns.clear();
            CatalogItem::Table(table)
        }));
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
        query: SearchCatalogQuery<'_>,
        attribution: &QueryAttribution,
    ) -> Result<Page<CatalogSearchResult>, QueryManagerError> {
        let regex = compile_metadata_regex(query.pattern, query.ignore_case)
            .map_err(QueryManagerError::App)?;
        let matches = self
            .catalog_items(
                workspace_name,
                query.catalog_name,
                query.schema_name,
                query.kind,
                attribution,
            )
            .await?
            .into_iter()
            .filter_map(|item| {
                let matched_fields = catalog_item_matched_fields(&item, &regex);
                (!matched_fields.is_empty()).then_some(CatalogSearchResult {
                    item,
                    matched_fields,
                })
            })
            .collect();
        Ok(page_items(matches, query.pagination))
    }

    pub(crate) async fn list_columns(
        &self,
        workspace_name: &WorkspaceName,
        query: ListColumnsQuery<'_>,
        attribution: &QueryAttribution,
    ) -> Result<Option<Page<ColumnSearchResult>>, QueryManagerError> {
        let table = self
            .queries
            .list_tables(
                workspace_name,
                query.table_ref.catalog_name,
                Some(query.table_ref.schema_name),
                Some(query.table_ref.table_name),
                attribution,
            )
            .await?
            .into_iter()
            .find(|table| table_matches_ref(table, query.table_ref));
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

fn catalog_item_sort_key(item: &CatalogItem) -> (&str, &str, &str, &'static str) {
    match item {
        CatalogItem::Table(table) => (
            &table.catalog_name,
            &table.schema_name,
            &table.table_name,
            "table",
        ),
        CatalogItem::TableFunction(function) => (
            "",
            &function.schema_name,
            &function.function_name,
            "table_function",
        ),
    }
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

fn table_matched_fields(table: &TableInfo, regex: &Regex) -> Vec<CatalogMetadataField> {
    let addressable_schema = table_addressable_schema_name(table);
    let name = table_addressable_name(table);
    let mut matches = Vec::new();
    let catalog_matched = !table.catalog_name.is_empty() && regex.is_match(&table.catalog_name);
    let schema_matched = regex.is_match(&table.schema_name);
    if catalog_matched {
        matches.push(CatalogMetadataField::CatalogName);
    }
    if schema_matched {
        matches.push(CatalogMetadataField::SchemaName);
    }
    if !catalog_matched
        && !schema_matched
        && addressable_schema != table.schema_name
        && regex.is_match(&addressable_schema)
    {
        matches.push(CatalogMetadataField::QualifiedSchema);
    }
    if regex.is_match(&table.table_name) {
        matches.push(CatalogMetadataField::TableName);
    }
    if regex.is_match(&name) {
        matches.push(CatalogMetadataField::Name);
    }
    if regex.is_match(&table.description) {
        matches.push(CatalogMetadataField::Description);
    }
    if regex.is_match(&table.guide) {
        matches.push(CatalogMetadataField::Guide);
    }
    if table
        .required_filters
        .iter()
        .any(|filter| regex.is_match(filter))
    {
        matches.push(CatalogMetadataField::RequiredFilters);
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

fn available_table_schemas(tables: &[TableInfo]) -> Vec<String> {
    tables
        .iter()
        .map(table_addressable_schema_name)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn same_schema_tables(tables: &[TableInfo], table_ref: CatalogTableRef<'_>) -> Vec<TableInfo> {
    tables
        .iter()
        .filter(|table| table_qualifier_matches(table, table_ref))
        .take(MISSING_TABLE_SUGGESTION_LIMIT)
        .cloned()
        .collect()
}

fn missing_table_suggestions(
    all_tables: &[TableInfo],
    table_ref: CatalogTableRef<'_>,
    same_schema_tables: &[TableInfo],
) -> Vec<TableInfo> {
    let mut suggestions = all_tables
        .iter()
        .filter(|table| same_schema_tables.is_empty() || table_qualifier_matches(table, table_ref))
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
    let schema_name = table_addressable_schema_name(table);
    let name = table_addressable_name(table);
    let candidates = [
        table.catalog_name.as_str(),
        table.schema_name.as_str(),
        schema_name.as_str(),
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

fn table_addressable_schema_name(table: &TableInfo) -> String {
    if table.catalog_name.is_empty() {
        table.schema_name.clone()
    } else {
        format!("{}.{}", table.catalog_name, table.schema_name)
    }
}

fn table_addressable_name(table: &TableInfo) -> String {
    format!(
        "{}.{}",
        table_addressable_schema_name(table),
        table.table_name
    )
}

fn table_matches_ref(table: &TableInfo, table_ref: CatalogTableRef<'_>) -> bool {
    table.table_name == table_ref.table_name && table_qualifier_matches(table, table_ref)
}

fn table_qualifier_matches(table: &TableInfo, table_ref: CatalogTableRef<'_>) -> bool {
    table.catalog_name == table_ref.catalog_name.unwrap_or_default()
        && table.schema_name == table_ref.schema_name
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
    use super::{
        CatalogMetadataField, CatalogTableRef, available_table_schemas, compile_metadata_regex,
        missing_table_suggestions, same_schema_tables, table_matched_fields, table_matches_ref,
        table_metadata_contains_literal,
    };
    use coral_engine::TableInfo;

    fn table(required_filters: Vec<String>) -> TableInfo {
        TableInfo {
            catalog_name: String::new(),
            schema_name: "github".to_string(),
            table_name: "Pull.Requests".to_string(),
            description: "Pull request table".to_string(),
            guide: "Query pull requests.".to_string(),
            columns: Vec::new(),
            required_filters,
        }
    }

    fn database_table(schema_name: &str, table_name: &str) -> TableInfo {
        let mut table = table(Vec::new());
        table.catalog_name = "coral_db".to_string();
        table.schema_name = schema_name.to_string();
        table.table_name = table_name.to_string();
        table
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

    #[test]
    fn database_catalog_and_schema_match_catalog_discovery_metadata() {
        let main = database_table("main", "users");
        let analytics = database_table("analytics", "events");
        let tables = vec![main, analytics];
        let main_table = tables.first().expect("main table");

        assert!(table_matches_ref(
            main_table,
            CatalogTableRef::new(Some("coral_db"), "main", "users")
        ));
        assert!(!table_matches_ref(
            main_table,
            CatalogTableRef::new(Some("coral_db"), "analytics", "users")
        ));
        assert_eq!(
            table_matched_fields(
                main_table,
                &regex::Regex::new("^coral_db\\.main\\.users$").expect("regex")
            ),
            vec![CatalogMetadataField::Name]
        );
        assert_eq!(
            table_matched_fields(main_table, &regex::Regex::new("^main$").expect("regex")),
            vec![CatalogMetadataField::SchemaName]
        );
        assert_eq!(
            table_matched_fields(
                main_table,
                &regex::Regex::new("^coral_db\\.main$").expect("regex")
            ),
            vec![CatalogMetadataField::QualifiedSchema]
        );
        assert_eq!(
            table_matched_fields(main_table, &regex::Regex::new("db\\.ma").expect("regex")),
            vec![
                CatalogMetadataField::QualifiedSchema,
                CatalogMetadataField::Name
            ]
        );
        assert_eq!(
            table_matched_fields(main_table, &regex::Regex::new("^coral_db$").expect("regex")),
            vec![CatalogMetadataField::CatalogName]
        );

        assert_eq!(
            available_table_schemas(&tables),
            vec!["coral_db.analytics", "coral_db.main"]
        );
        let same_schema = same_schema_tables(
            &tables,
            CatalogTableRef::new(Some("coral_db"), "main", "missing"),
        );
        assert_eq!(same_schema.len(), 1);
        let same_schema_table = same_schema.first().expect("same schema table");
        assert_eq!(same_schema_table.catalog_name, "coral_db");
        assert_eq!(same_schema_table.schema_name, "main");
        assert_eq!(same_schema_table.table_name, "users");

        let suggestions = missing_table_suggestions(
            &tables,
            CatalogTableRef::new(Some("coral_db"), "main", "user"),
            &same_schema,
        );
        assert_eq!(suggestions.len(), 1);
        let suggestion = suggestions.first().expect("suggestion");
        assert_eq!(suggestion.catalog_name, "coral_db");
        assert_eq!(suggestion.schema_name, "main");
        assert_eq!(suggestion.table_name, "users");
        assert!(table_metadata_contains_literal(
            same_schema_table,
            "coral_db.main.users"
        ));
    }
}
