//! Workspace-scoped catalog discovery operations.

use std::collections::BTreeSet;

use coral_engine::{ColumnInfo, TableFunctionInfo, TableInfo};
use regex::{Regex, RegexBuilder};

use crate::bootstrap::AppError;
use crate::query::manager::{QueryManager, QueryManagerError};
use crate::workspaces::WorkspaceName;

const DEFAULT_SEARCH_LIMIT: u32 = 20;
const MAX_SEARCH_LIMIT: u32 = 100;
const DEFAULT_COLUMN_LIMIT: u32 = 50;
const MAX_COLUMN_LIMIT: u32 = 200;
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
    SchemaName,
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
            Self::SchemaName => "schema_name",
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
    ) -> Result<Page<CatalogItem>, QueryManagerError> {
        let items = self
            .catalog_items(workspace_name, schema_name, kind)
            .await?;
        Ok(page_items(items, pagination))
    }

    async fn catalog_items(
        &self,
        workspace_name: &WorkspaceName,
        schema_name: Option<&str>,
        kind: Option<CatalogItemKind>,
    ) -> Result<Vec<CatalogItem>, QueryManagerError> {
        let mut items = match kind {
            None => {
                let catalog = self
                    .queries
                    .list_catalog(workspace_name, schema_name)
                    .await?;
                catalog
                    .tables
                    .into_iter()
                    .map(CatalogItem::Table)
                    .chain(
                        catalog
                            .table_functions
                            .into_iter()
                            .map(CatalogItem::TableFunction),
                    )
                    .collect::<Vec<_>>()
            }
            Some(CatalogItemKind::Table) => self
                .queries
                .list_tables(workspace_name, schema_name, None)
                .await?
                .into_iter()
                .map(CatalogItem::Table)
                .collect(),
            Some(CatalogItemKind::TableFunction) => self
                .queries
                .list_table_functions(workspace_name, schema_name, None)
                .await?
                .into_iter()
                .map(CatalogItem::TableFunction)
                .collect(),
        };
        items.sort_by(|left, right| catalog_item_sort_key(left).cmp(&catalog_item_sort_key(right)));
        Ok(items)
    }

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
        let matches = self
            .catalog_items(workspace_name, schema_name, kind)
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
        Ok(page_items(matches, pagination))
    }

    pub(crate) async fn describe_table(
        &self,
        workspace_name: &WorkspaceName,
        table_ref: CatalogTableRef<'_>,
    ) -> Result<DescribeTableResult, QueryManagerError> {
        let exact = self
            .queries
            .list_tables(
                workspace_name,
                Some(table_ref.schema_name),
                Some(table_ref.table_name),
            )
            .await?
            .into_iter()
            .find(|table| {
                table.schema_name == table_ref.schema_name
                    && table.table_name == table_ref.table_name
            });
        if let Some(table) = exact {
            return Ok(DescribeTableResult::Found(table));
        }

        let mut all_tables = self.queries.list_tables(workspace_name, None, None).await?;
        for table in &mut all_tables {
            table.columns.clear();
        }
        let available_schemas = all_tables
            .iter()
            .map(|table| table.schema_name.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let same_schema_tables = all_tables
            .iter()
            .filter(|table| table.schema_name == table_ref.schema_name)
            .take(MISSING_TABLE_SUGGESTION_LIMIT)
            .cloned()
            .collect::<Vec<_>>();
        let suggestions = missing_table_suggestions(&all_tables, table_ref, &same_schema_tables);
        Ok(DescribeTableResult::Missing(MissingTableContext {
            suggestions,
            available_schemas,
            same_schema_tables,
        }))
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
