//! Rendering helpers for `coral search`.

use std::fmt::Write as _;

use coral_api::v1::catalog_item;
use coral_api::v1::search_result::Payload;
use coral_api::v1::{
    CatalogMetadata, NativeSearchPath, ObservedValue, SearchFieldRole, SearchProvider,
    SearchProviderState, SearchResponse, SearchResult, SearchSurfaceKind, SearchTableColumnPreview,
    TableSummary,
};
use coral_client::format_sql_reference;

pub(crate) fn search_rows(response: &SearchResponse) -> Vec<[String; 4]> {
    response
        .results
        .iter()
        .filter_map(search_result_row)
        .collect()
}

pub(crate) fn search_json(response: &SearchResponse) -> Result<String, serde_json::Error> {
    coral_mcp::search_response_json(response)
}

pub(crate) fn search_warnings(response: &SearchResponse) -> Vec<String> {
    let mut warnings = response
        .provider_statuses
        .iter()
        .filter_map(|status| {
            let state = SearchProviderState::try_from(status.state)
                .unwrap_or(SearchProviderState::Unspecified);
            if state.is_healthy() {
                return None;
            }

            let provider = SearchProvider::try_from(status.provider)
                .unwrap_or(SearchProvider::Unspecified)
                .label();
            let state = state.label();
            let suffix = if status.note.is_empty() {
                String::new()
            } else {
                format!(": {}", status.note)
            };
            Some(format!("Warning: {provider} search {state}{suffix}"))
        })
        .collect::<Vec<_>>();

    if let Some(truncation) = response.truncation.as_ref()
        && (truncation.truncated || !truncation.note.is_empty())
    {
        let suffix = if truncation.note.is_empty() {
            String::new()
        } else {
            format!("; {}", truncation.note)
        };
        warnings.push(format!(
            "Warning: search results truncated: returned {} of max {}{}",
            truncation.returned_count, truncation.max_results, suffix
        ));
    }

    warnings
}

fn search_result_row(result: &SearchResult) -> Option<[String; 4]> {
    match result.payload.as_ref()? {
        Payload::CatalogMetadata(metadata) => catalog_metadata_row(metadata),
        Payload::ColumnHint(hint) => Some([
            "column_hint".to_string(),
            qualified_field_name(&hint.schema_name, &hint.surface_name, &hint.name),
            format_sql_reference(&hint.schema_name, &hint.surface_name),
            field_details(
                SearchFieldRole::try_from(hint.field_role)
                    .unwrap_or(SearchFieldRole::Unspecified)
                    .label(),
                &hint.data_type,
                hint.required,
                &hint.description,
            ),
        ]),
        Payload::ObservedValue(value) => Some([
            "observed_value".to_string(),
            qualified_field_name(&value.schema_name, &value.surface_name, &value.field_path),
            format_sql_reference(&value.schema_name, &value.surface_name),
            observed_value_details(value),
        ]),
        Payload::NativeSearchPath(path) => native_search_path_row(path),
    }
}

fn catalog_metadata_row(metadata: &CatalogMetadata) -> Option<[String; 4]> {
    match metadata.item.as_ref()?.item.as_ref()? {
        catalog_item::Item::Table(table) => Some([
            "catalog_item".to_string(),
            qualified_name(&table.schema_name, &table.name),
            format_sql_reference(&table.schema_name, &table.name),
            catalog_table_details(table, metadata.table_column_preview.as_ref()),
        ]),
        catalog_item::Item::TableFunction(function) => Some([
            "catalog_item".to_string(),
            qualified_name(&function.schema_name, &function.name),
            format_sql_reference(&function.schema_name, &function.name),
            compact_details("table_function", &function.description),
        ]),
    }
}

fn native_search_path_row(path: &NativeSearchPath) -> Option<[String; 4]> {
    let function = path.table_function.as_ref()?;
    Some([
        "native_search_path".to_string(),
        qualified_name(&function.schema_name, &function.name),
        format_sql_reference(&function.schema_name, &function.name),
        if path.sql_call_example.is_empty() {
            function.description.clone()
        } else {
            path.sql_call_example.clone()
        },
    ])
}

fn catalog_table_details(
    table: &TableSummary,
    preview: Option<&SearchTableColumnPreview>,
) -> String {
    let mut details = compact_details("table", &table.description);
    let Some(preview) = preview else {
        return details;
    };
    if preview.columns.is_empty() {
        return details;
    }

    let mut columns = preview
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    if preview.omitted_column_count > 0 {
        write!(columns, " (+{} more)", preview.omitted_column_count)
            .expect("write to string should not fail");
    }
    if !details.is_empty() {
        details.push_str("; ");
    }
    details.push_str("columns: ");
    details.push_str(&columns);
    details
}

fn field_details(field_role: &str, data_type: &str, required: bool, description: &str) -> String {
    let mut parts = Vec::new();
    if field_role != "unknown" {
        parts.push(field_role.to_string());
    }
    if !data_type.is_empty() {
        parts.push(data_type.to_string());
    }
    if required {
        parts.push("required".to_string());
    }
    if !description.is_empty() {
        parts.push(description.to_string());
    }
    parts.join("; ")
}

fn compact_details(kind: &str, description: &str) -> String {
    if description.is_empty() {
        kind.to_string()
    } else {
        format!("{kind}; {description}")
    }
}

fn observed_value_details(value: &ObservedValue) -> String {
    let mut parts = vec![
        SearchSurfaceKind::try_from(value.surface_kind)
            .unwrap_or(SearchSurfaceKind::Unspecified)
            .label()
            .to_string(),
    ];
    if !value.value.is_empty() {
        parts.push(value.value.clone());
    }
    if value.observed_count > 0 {
        parts.push(format!("observed_count: {}", value.observed_count));
    }
    if !value.last_observed_at.is_empty() {
        parts.push(format!("last_observed_at: {}", value.last_observed_at));
    }
    parts.join("; ")
}

fn qualified_name(schema_name: &str, name: &str) -> String {
    format!("{schema_name}.{name}")
}

fn qualified_field_name(schema_name: &str, surface_name: &str, field_name: &str) -> String {
    format!("{schema_name}.{surface_name}.{field_name}")
}

#[cfg(test)]
mod tests {
    use coral_api::v1::catalog_item;
    use coral_api::v1::search_result::Payload;
    use coral_api::v1::{
        CatalogItem, CatalogMetadata, ColumnHint, ObservedValue, SearchFieldRole, SearchProvider,
        SearchProviderState, SearchProviderStatus, SearchResponse, SearchResult,
        SearchResultTruncation, SearchSurfaceKind, SearchTableColumnPreview,
        SearchTableColumnPreviewColumn, TableFunction, TableFunctionArgument, TableFunctionKind,
        TableFunctionResultColumn, TableSummary, Workspace,
    };
    use serde_json::Value;

    use coral_client::{format_sql_identifier, format_sql_reference};

    use super::{search_json, search_rows, search_warnings};

    #[test]
    fn search_output_always_quotes_sql_references() {
        assert_eq!(format_sql_reference("select", "from"), r#""select"."from""#);
        assert_eq!(
            format_sql_reference("github", "issues"),
            r#""github"."issues""#
        );
        assert_eq!(
            format_sql_reference("GitHub", "pull requests"),
            r#""GitHub"."pull requests""#
        );
        assert_eq!(format_sql_identifier("repo\"name"), r#""repo""name""#);
        assert_eq!(format_sql_identifier(""), r#""""#);
    }

    #[test]
    fn search_output_renders_rows_and_named_json() {
        let response = SearchResponse {
            results: vec![SearchResult {
                provider: SearchProvider::CatalogMetadata as i32,
                payload: Some(Payload::ColumnHint(ColumnHint {
                    workspace: None,
                    schema_name: "github".to_string(),
                    surface_name: "issues".to_string(),
                    surface_kind: SearchSurfaceKind::Table as i32,
                    field_role: SearchFieldRole::TableColumn as i32,
                    name: "title".to_string(),
                    data_type: "Utf8".to_string(),
                    required: false,
                    description: "Issue title".to_string(),
                    matched_fields: vec!["description".to_string()],
                })),
            }],
            provider_statuses: vec![SearchProviderStatus {
                provider: SearchProvider::CatalogMetadata as i32,
                state: SearchProviderState::ResultsFound as i32,
                note: "1 result".to_string(),
            }],
            truncation: Some(SearchResultTruncation {
                truncated: false,
                returned_count: 1,
                max_results: 10,
                note: String::new(),
            }),
        };

        assert_eq!(
            search_rows(&response),
            vec![[
                "column_hint".to_string(),
                "github.issues.title".to_string(),
                "\"github\".\"issues\"".to_string(),
                "table_column; Utf8; Issue title".to_string()
            ]]
        );

        let json: Value =
            serde_json::from_str(&search_json(&response).expect("json")).expect("parse json");
        assert_eq!(
            json.pointer("/provider_statuses/0/provider")
                .and_then(Value::as_str),
            Some("catalog_metadata")
        );
        assert_eq!(
            json.pointer("/results/0/provider").and_then(Value::as_str),
            Some("catalog_metadata")
        );
        assert_eq!(
            json.pointer("/results/0/type").and_then(Value::as_str),
            Some("column_hint")
        );
        assert_eq!(
            json.pointer("/results/0/field_role")
                .and_then(Value::as_str),
            Some("table_column")
        );
    }

    #[test]
    fn search_output_renders_catalog_metadata_preview() {
        let response = SearchResponse {
            results: vec![SearchResult {
                provider: SearchProvider::CatalogMetadata as i32,
                payload: Some(Payload::CatalogMetadata(CatalogMetadata {
                    item: Some(CatalogItem {
                        item: Some(catalog_item::Item::Table(TableSummary {
                            workspace: Some(Workspace {
                                name: "default".to_string(),
                            }),
                            schema_name: "github".to_string(),
                            name: "issues".to_string(),
                            description: "GitHub issues".to_string(),
                            required_filters: vec!["repo".to_string()],
                            guide: "Query GitHub issues.".to_string(),
                        })),
                    }),
                    matched_fields: vec!["source_name".to_string(), "table_name".to_string()],
                    table_column_preview: Some(SearchTableColumnPreview {
                        column_count: 9,
                        columns: vec![SearchTableColumnPreviewColumn {
                            name: "repo".to_string(),
                            data_type: "Utf8".to_string(),
                            is_required_filter: true,
                            description: "Repository name".to_string(),
                            matched_fields: vec!["column_name".to_string()],
                        }],
                        omitted_column_count: 8,
                    }),
                })),
            }],
            provider_statuses: Vec::new(),
            truncation: None,
        };

        assert_eq!(
            search_rows(&response),
            vec![[
                "catalog_item".to_string(),
                "github.issues".to_string(),
                "\"github\".\"issues\"".to_string(),
                "table; GitHub issues; columns: repo (+8 more)".to_string()
            ]]
        );

        let json: Value =
            serde_json::from_str(&search_json(&response).expect("json")).expect("parse json");
        assert_eq!(
            json.pointer("/results/0/provider").and_then(Value::as_str),
            Some("catalog_metadata")
        );
        assert_eq!(
            json.pointer("/results/0/matched_fields/0")
                .and_then(Value::as_str),
            Some("source_name")
        );
        assert_eq!(
            json.pointer("/results/0/table/column_count")
                .and_then(Value::as_u64),
            Some(9)
        );
        assert_eq!(
            json.pointer("/results/0/table/column_preview/0/column_name")
                .and_then(Value::as_str),
            Some("repo")
        );
        assert_eq!(
            json.pointer("/results/0/table/column_preview/0/is_required_filter")
                .and_then(Value::as_bool),
            Some(true)
        );
    }

    #[test]
    fn search_output_json_uses_shared_table_function_shape() {
        let response = SearchResponse {
            results: vec![SearchResult {
                provider: SearchProvider::CatalogMetadata as i32,
                payload: Some(Payload::CatalogMetadata(CatalogMetadata {
                    item: Some(CatalogItem {
                        item: Some(catalog_item::Item::TableFunction(TableFunction {
                            workspace: Some(Workspace {
                                name: "default".to_string(),
                            }),
                            schema_name: "github".to_string(),
                            name: "search_issues".to_string(),
                            description: "Search GitHub issues".to_string(),
                            kind: TableFunctionKind::Search as i32,
                            arguments: vec![TableFunctionArgument {
                                name: "q".to_string(),
                                required: true,
                                values: Vec::new(),
                            }],
                            result_columns: vec![TableFunctionResultColumn {
                                name: "title".to_string(),
                                data_type: "Utf8".to_string(),
                                nullable: false,
                                description: "Issue title".to_string(),
                            }],
                            search_limits: None,
                        })),
                    }),
                    matched_fields: vec!["description".to_string()],
                    table_column_preview: None,
                })),
            }],
            provider_statuses: Vec::new(),
            truncation: None,
        };

        assert_eq!(
            search_rows(&response),
            vec![[
                "catalog_item".to_string(),
                "github.search_issues".to_string(),
                "\"github\".\"search_issues\"".to_string(),
                "table_function; Search GitHub issues".to_string()
            ]]
        );

        let json: Value =
            serde_json::from_str(&search_json(&response).expect("json")).expect("parse json");
        assert_eq!(
            json.pointer("/results/0/sql_call_example")
                .and_then(Value::as_str),
            Some("\"github\".\"search_issues\"(\"q\" => '<value>')")
        );
        assert_eq!(
            json.pointer("/results/0/table_function/function_name")
                .and_then(Value::as_str),
            Some("search_issues")
        );
        assert!(
            json.pointer("/results/0/table_function/kind").is_none(),
            "shared MCP table_function payload should not expose a nested kind"
        );
    }

    #[test]
    fn search_output_renders_observed_value_metadata() {
        let response = SearchResponse {
            results: vec![SearchResult {
                provider: SearchProvider::ObservedValues as i32,
                payload: Some(Payload::ObservedValue(ObservedValue {
                    value: "timeout".to_string(),
                    schema_name: "datadog".to_string(),
                    surface_name: "logs".to_string(),
                    column_name: "payload".to_string(),
                    surface_kind: SearchSurfaceKind::Table as i32,
                    field_path: "payload.error".to_string(),
                    observed_count: 4,
                    last_observed_at: "2026-06-04T10:00:00.000Z".to_string(),
                })),
            }],
            provider_statuses: Vec::new(),
            truncation: None,
        };

        assert_eq!(
            search_rows(&response),
            vec![[
                "observed_value".to_string(),
                "datadog.logs.payload.error".to_string(),
                "\"datadog\".\"logs\"".to_string(),
                "table; timeout; observed_count: 4; last_observed_at: 2026-06-04T10:00:00.000Z"
                    .to_string()
            ]]
        );

        let json: Value =
            serde_json::from_str(&search_json(&response).expect("json")).expect("parse json");
        assert_eq!(
            json.pointer("/results/0/provider").and_then(Value::as_str),
            Some("observed_values")
        );
        assert_eq!(
            json.pointer("/results/0/column_name")
                .and_then(Value::as_str),
            Some("payload")
        );
        assert_eq!(
            json.pointer("/results/0/field_path")
                .and_then(Value::as_str),
            Some("payload.error")
        );
        assert_eq!(
            json.pointer("/results/0/observed_count")
                .and_then(Value::as_u64),
            Some(4)
        );
        assert_eq!(
            json.pointer("/results/0/last_observed_at")
                .and_then(Value::as_str),
            Some("2026-06-04T10:00:00.000Z")
        );
    }

    #[test]
    fn search_output_warns_for_degraded_providers_and_truncation() {
        let response = SearchResponse {
            results: Vec::new(),
            provider_statuses: vec![
                SearchProviderStatus {
                    provider: SearchProvider::CatalogMetadata as i32,
                    state: SearchProviderState::Error as i32,
                    note: "catalog index unavailable".to_string(),
                },
                SearchProviderStatus {
                    provider: SearchProvider::ObservedValues as i32,
                    state: SearchProviderState::Partial as i32,
                    note: String::new(),
                },
                SearchProviderStatus {
                    provider: SearchProvider::ObservedValues as i32,
                    state: SearchProviderState::NotEnabled as i32,
                    note: "disabled".to_string(),
                },
            ],
            truncation: Some(SearchResultTruncation {
                truncated: true,
                returned_count: 50,
                max_results: 50,
                note: "increase --limit to inspect more candidates".to_string(),
            }),
        };

        assert_eq!(
            search_warnings(&response),
            vec![
                "Warning: catalog_metadata search error: catalog index unavailable".to_string(),
                "Warning: observed_values search partial".to_string(),
                "Warning: search results truncated: returned 50 of max 50; increase --limit to inspect more candidates".to_string(),
            ]
        );
    }
}
