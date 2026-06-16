//! Implements the gRPC `CatalogService`.

use coral_api::v1::catalog_service_server::CatalogService as CatalogServiceApi;
use coral_api::v1::{
    CatalogCounts as ProtoCatalogCounts, CatalogItem as ProtoCatalogItem,
    CatalogItemKind as ProtoCatalogItemKind, DescribeTableRequest, DescribeTableResponse,
    ListCatalogRequest, ListCatalogResponse, ListColumnsRequest, ListColumnsResponse,
    PaginationRequest, SearchCatalogRequest, SearchCatalogResponse, catalog_item,
};
use serde_json::json;
use tonic::{Request, Response, Status};

use crate::bootstrap::app_status;
use crate::catalog::discovery::{
    CatalogDiscovery, CatalogItemKind, CatalogTableRef, ListColumnsQuery, Pagination,
    column_pagination, search_pagination,
};
use crate::provenance::{self, CallTiming, OccurrenceDraft, ProvenanceCall, ProvenanceRecorder};
use crate::query::manager::QueryManager;
use crate::transport::{
    catalog_item_to_proto, catalog_search_result_to_proto, column_search_result_to_proto,
    describe_table_response_to_proto, grpc_span, instrument_grpc, pagination_to_proto,
    query_status, workspace_name_from_proto,
};

#[derive(Clone)]
pub(crate) struct CatalogService {
    catalog: CatalogDiscovery,
    provenance: ProvenanceRecorder,
}

impl CatalogService {
    pub(crate) fn new(query_manager: QueryManager) -> Self {
        Self {
            provenance: query_manager.provenance_recorder(),
            catalog: CatalogDiscovery::new(query_manager),
        }
    }
}

#[tonic::async_trait]
impl CatalogServiceApi for CatalogService {
    async fn list_catalog(
        &self,
        request: Request<ListCatalogRequest>,
    ) -> Result<Response<ListCatalogResponse>, Status> {
        let timing = CallTiming::start_now();
        let span = grpc_span(&request);
        let record_span = span.clone();
        let catalog = self.catalog.clone();
        let provenance = self.provenance.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_for_record = request
                .workspace
                .as_ref()
                .map(|workspace| workspace.name.clone())
                .unwrap_or_default();
            let schema_name_input = request.schema_name.clone();
            let input_json = json!({
                "workspace": workspace_for_record.clone(),
                "schema_name": schema_name_input.clone(),
                "kind": request.kind,
                "pagination": request.pagination.as_ref().map(|pagination| json!({
                    "limit": pagination.limit,
                    "offset": pagination.offset,
                })),
            });
            let pagination = pagination_from_proto(request.pagination.unwrap_or_default());
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let schema_name = optional_trimmed(&schema_name_input);
            let kind = catalog_item_kind_from_proto(request.kind)?;
            let catalog_page = catalog
                .list_catalog(&workspace_name, schema_name, kind, pagination)
                .await
                .map_err(query_status)?;
            let page = catalog_page.items;
            let pagination = pagination_to_proto(
                page.total,
                page.limit,
                page.offset,
                page.has_more,
                page.next_offset,
            );
            let response = ListCatalogResponse {
                items: page
                    .items
                    .into_iter()
                    .map(|item| catalog_item_to_proto(&workspace_name, item))
                    .collect(),
                pagination: Some(pagination),
                counts: Some(ProtoCatalogCounts {
                    table_count: catalog_page.counts.table_count,
                    table_function_count: catalog_page.counts.table_function_count,
                }),
            };
            provenance.record_call(
                &record_span,
                ProvenanceCall {
                    workspace: workspace_for_record,
                    operation: "catalog.list_catalog".to_string(),
                    input_json: input_json.clone(),
                    output_summary_json: json!({
                        "item_count": response.items.len(),
                        "table_count": response.counts.as_ref().map(|counts| counts.table_count),
                        "table_function_count": response.counts.as_ref().map(|counts| counts.table_function_count),
                    }),
                    status: "ok".to_string(),
                    row_count: Some(i64::try_from(response.items.len()).unwrap_or(i64::MAX)),
                    input_occurrences: provenance::json_input_occurrences(&input_json),
                    output_occurrences: list_catalog_occurrences(&response),
                    timing,
                },
            );
            Ok(Response::new(response))
        })
        .await
    }

    async fn search_catalog(
        &self,
        request: Request<SearchCatalogRequest>,
    ) -> Result<Response<SearchCatalogResponse>, Status> {
        let timing = CallTiming::start_now();
        let span = grpc_span(&request);
        let record_span = span.clone();
        let catalog = self.catalog.clone();
        let provenance = self.provenance.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_for_record = request
                .workspace
                .as_ref()
                .map(|workspace| workspace.name.clone())
                .unwrap_or_default();
            let pattern = request.pattern.clone();
            let schema_name_input = request.schema_name.clone();
            let input_json = json!({
                "workspace": workspace_for_record.clone(),
                "pattern": pattern.clone(),
                "ignore_case": request.ignore_case,
                "schema_name": schema_name_input.clone(),
                "kind": request.kind,
                "pagination": request.pagination.as_ref().map(|pagination| json!({
                    "limit": pagination.limit,
                    "offset": pagination.offset,
                })),
            });
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let schema_name = optional_trimmed(&schema_name_input);
            let kind = catalog_item_kind_from_proto(request.kind)?;
            let pagination = search_pagination(request.pagination.map(pagination_from_proto))
                .map_err(app_status)?;
            let page = catalog
                .search_catalog(
                    &workspace_name,
                    &request.pattern,
                    schema_name,
                    kind,
                    request.ignore_case,
                    pagination,
                )
                .await
                .map_err(query_status)?;
            let pagination = pagination_to_proto(
                page.total,
                page.limit,
                page.offset,
                page.has_more,
                page.next_offset,
            );
            let response = SearchCatalogResponse {
                items: page
                    .items
                    .into_iter()
                    .map(|result| catalog_search_result_to_proto(&workspace_name, result))
                    .collect(),
                pagination: Some(pagination),
            };
            provenance.record_call(
                &record_span,
                ProvenanceCall {
                    workspace: workspace_for_record,
                    operation: "catalog.search_catalog".to_string(),
                    input_json: input_json.clone(),
                    output_summary_json: json!({ "item_count": response.items.len() }),
                    status: "ok".to_string(),
                    row_count: Some(i64::try_from(response.items.len()).unwrap_or(i64::MAX)),
                    input_occurrences: provenance::json_input_occurrences(&input_json),
                    output_occurrences: search_catalog_occurrences(&response),
                    timing,
                },
            );
            Ok(Response::new(response))
        })
        .await
    }

    async fn describe_table(
        &self,
        request: Request<DescribeTableRequest>,
    ) -> Result<Response<DescribeTableResponse>, Status> {
        let timing = CallTiming::start_now();
        let span = grpc_span(&request);
        let record_span = span.clone();
        let catalog = self.catalog.clone();
        let provenance = self.provenance.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_for_record = request
                .workspace
                .as_ref()
                .map(|workspace| workspace.name.clone())
                .unwrap_or_default();
            let schema_name_input = request.schema_name.clone();
            let table_name_input = request.table_name.clone();
            let input_json = json!({
                "workspace": workspace_for_record.clone(),
                "schema_name": schema_name_input.clone(),
                "table_name": table_name_input.clone(),
            });
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let schema_name = required_trimmed(&schema_name_input, "schema_name")?;
            let table_name = required_trimmed(&table_name_input, "table_name")?;
            let result = catalog
                .describe_table(
                    &workspace_name,
                    CatalogTableRef::new(&schema_name, &table_name),
                )
                .await
                .map_err(query_status)?;
            let response = describe_table_response_to_proto(&workspace_name, result);
            let mut input_occurrences = provenance::json_input_occurrences(&input_json);
            input_occurrences.push(OccurrenceDraft::input_entity(
                "input.table",
                format!("table:{schema_name}.{table_name}"),
            ));
            provenance.record_call(
                &record_span,
                ProvenanceCall {
                    workspace: workspace_for_record,
                    operation: "catalog.describe_table".to_string(),
                    input_json: input_json.clone(),
                    output_summary_json: json!({
                        "found": response.table.is_some(),
                        "suggestion_count": response.suggestions.len(),
                    }),
                    status: "ok".to_string(),
                    row_count: response.table.as_ref().map(|_| 1_i64),
                    input_occurrences,
                    output_occurrences: describe_table_occurrences(&response),
                    timing,
                },
            );
            Ok(Response::new(response))
        })
        .await
    }

    async fn list_columns(
        &self,
        request: Request<ListColumnsRequest>,
    ) -> Result<Response<ListColumnsResponse>, Status> {
        let timing = CallTiming::start_now();
        let span = grpc_span(&request);
        let record_span = span.clone();
        let catalog = self.catalog.clone();
        let provenance = self.provenance.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_for_record = request
                .workspace
                .as_ref()
                .map(|workspace| workspace.name.clone())
                .unwrap_or_default();
            let schema_name_input = request.schema_name.clone();
            let table_name_input = request.table_name.clone();
            let pattern = request.pattern.clone();
            let input_json = json!({
                "workspace": workspace_for_record.clone(),
                "schema_name": schema_name_input.clone(),
                "table_name": table_name_input.clone(),
                "pattern": pattern.clone(),
                "ignore_case": request.ignore_case,
                "required_only": request.required_only,
                "pagination": request.pagination.as_ref().map(|pagination| json!({
                    "limit": pagination.limit,
                    "offset": pagination.offset,
                })),
            });
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let schema_name = required_trimmed(&schema_name_input, "schema_name")?;
            let table_name = required_trimmed(&table_name_input, "table_name")?;
            let pagination = column_pagination(request.pagination.map(pagination_from_proto))
                .map_err(app_status)?;
            let page = catalog
                .list_columns(
                    &workspace_name,
                    ListColumnsQuery {
                        table_ref: CatalogTableRef::new(&schema_name, &table_name),
                        pattern: request.pattern.as_deref(),
                        ignore_case: request.ignore_case,
                        required_only: request.required_only,
                        pagination,
                    },
                )
                .await
                .map_err(query_status)?
                .ok_or_else(|| {
                    Status::not_found(format!("table '{schema_name}.{table_name}' not found"))
                })?;
            let pagination = pagination_to_proto(
                page.total,
                page.limit,
                page.offset,
                page.has_more,
                page.next_offset,
            );
            let response = ListColumnsResponse {
                columns: page
                    .items
                    .into_iter()
                    .map(column_search_result_to_proto)
                    .collect(),
                pagination: Some(pagination),
            };
            let mut input_occurrences = provenance::json_input_occurrences(&input_json);
            input_occurrences.push(OccurrenceDraft::input_entity(
                "input.table",
                format!("table:{schema_name}.{table_name}"),
            ));
            provenance.record_call(
                &record_span,
                ProvenanceCall {
                    workspace: workspace_for_record,
                    operation: "catalog.list_columns".to_string(),
                    input_json: input_json.clone(),
                    output_summary_json: json!({ "column_count": response.columns.len() }),
                    status: "ok".to_string(),
                    row_count: Some(i64::try_from(response.columns.len()).unwrap_or(i64::MAX)),
                    input_occurrences,
                    output_occurrences: list_columns_occurrences(
                        &schema_name,
                        &table_name,
                        &response,
                    ),
                    timing,
                },
            );
            Ok(Response::new(response))
        })
        .await
    }
}

fn list_catalog_occurrences(response: &ListCatalogResponse) -> Vec<OccurrenceDraft> {
    response
        .items
        .iter()
        .enumerate()
        .flat_map(|(index, item)| catalog_item_occurrences(item, &format!("output.items[{index}]")))
        .collect()
}

fn search_catalog_occurrences(response: &SearchCatalogResponse) -> Vec<OccurrenceDraft> {
    response
        .items
        .iter()
        .enumerate()
        .flat_map(|(index, result)| {
            result.item.as_ref().into_iter().flat_map(move |item| {
                catalog_item_occurrences(item, &format!("output.items[{index}].item"))
            })
        })
        .collect()
}

fn describe_table_occurrences(response: &DescribeTableResponse) -> Vec<OccurrenceDraft> {
    let mut occurrences = Vec::new();
    if let Some(table) = response.table.as_ref() {
        occurrences.push(OccurrenceDraft::output_entity(
            "output.table",
            format!("table:{}.{}", table.schema_name, table.name),
        ));
        occurrences.extend(table.columns.iter().enumerate().map(|(index, column)| {
            OccurrenceDraft::output_entity(
                format!("output.table.columns[{index}]"),
                format!(
                    "column:{}.{}.{}",
                    table.schema_name, table.name, column.name
                ),
            )
        }));
    }
    occurrences.extend(
        response
            .suggestions
            .iter()
            .enumerate()
            .map(|(index, table)| {
                OccurrenceDraft::output_entity(
                    format!("output.suggestions[{index}]"),
                    format!("table:{}.{}", table.schema_name, table.name),
                )
            }),
    );
    occurrences
}

fn list_columns_occurrences(
    schema_name: &str,
    table_name: &str,
    response: &ListColumnsResponse,
) -> Vec<OccurrenceDraft> {
    response
        .columns
        .iter()
        .enumerate()
        .filter_map(|(index, result)| {
            result.column.as_ref().map(|column| {
                OccurrenceDraft::output_entity(
                    format!("output.columns[{index}]"),
                    format!("column:{schema_name}.{table_name}.{}", column.name),
                )
            })
        })
        .collect()
}

fn catalog_item_occurrences(item: &ProtoCatalogItem, path: &str) -> Vec<OccurrenceDraft> {
    match item.item.as_ref() {
        Some(catalog_item::Item::Table(table)) => {
            vec![OccurrenceDraft::output_entity(
                format!("{path}.table"),
                format!("table:{}.{}", table.schema_name, table.name),
            )]
        }
        Some(catalog_item::Item::TableFunction(function)) => {
            vec![OccurrenceDraft::output_entity(
                format!("{path}.table_function"),
                format!("function:{}.{}", function.schema_name, function.name),
            )]
        }
        None => Vec::new(),
    }
}

fn pagination_from_proto(pagination: PaginationRequest) -> Pagination {
    Pagination {
        limit: pagination.limit,
        offset: pagination.offset,
    }
}

fn catalog_item_kind_from_proto(kind: i32) -> Result<Option<CatalogItemKind>, Status> {
    match ProtoCatalogItemKind::try_from(kind) {
        Ok(ProtoCatalogItemKind::Unspecified) => Ok(None),
        Ok(ProtoCatalogItemKind::Table) => Ok(Some(CatalogItemKind::Table)),
        Ok(ProtoCatalogItemKind::TableFunction) => Ok(Some(CatalogItemKind::TableFunction)),
        Err(_) => Err(app_status(crate::bootstrap::AppError::InvalidInput(
            "unknown catalog item kind".to_string(),
        ))),
    }
}

fn optional_trimmed(value: &str) -> Option<&str> {
    let value = value.trim();
    (!value.is_empty()).then_some(value)
}

fn required_trimmed(value: &str, field: &str) -> Result<String, Status> {
    let value = value.trim();
    if value.is_empty() {
        return Err(app_status(crate::bootstrap::AppError::InvalidInput(
            format!("missing required field '{field}'"),
        )));
    }
    Ok(value.to_string())
}
