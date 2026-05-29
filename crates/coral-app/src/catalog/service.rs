//! Implements the gRPC `CatalogService`.

use coral_api::v1::catalog_service_server::CatalogService as CatalogServiceApi;
use coral_api::v1::{
    CatalogItemKind as ProtoCatalogItemKind, DescribeTableRequest, DescribeTableResponse,
    ListCatalogRequest, ListCatalogResponse, ListColumnsRequest, ListColumnsResponse,
    PaginationRequest,
};
use tonic::{Request, Response, Status};

use crate::bootstrap::app_status;
use crate::catalog::discovery::{
    CatalogDiscovery, CatalogItemKind, CatalogTableRef, ListColumnsQuery, Pagination,
    column_pagination,
};
use crate::query::manager::QueryManager;
use crate::transport::{
    catalog_item_to_proto, column_search_result_to_proto, describe_table_response_to_proto,
    grpc_span, instrument_grpc, pagination_to_proto, query_status, workspace_name_from_proto,
};

#[derive(Clone)]
pub(crate) struct CatalogService {
    catalog: CatalogDiscovery,
}

impl CatalogService {
    pub(crate) fn new(query_manager: QueryManager) -> Self {
        Self {
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
        let span = grpc_span(&request);
        let catalog = self.catalog.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let pagination = pagination_from_proto(request.pagination.unwrap_or_default());
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let schema_name = optional_trimmed(&request.schema_name);
            let kind = catalog_item_kind_from_proto(request.kind)?;
            let page = catalog
                .list_catalog(&workspace_name, schema_name, kind, pagination)
                .await
                .map_err(query_status)?;
            let pagination = pagination_to_proto(
                page.total,
                page.limit,
                page.offset,
                page.has_more,
                page.next_offset,
            );
            Ok(Response::new(ListCatalogResponse {
                items: page
                    .items
                    .into_iter()
                    .map(|item| catalog_item_to_proto(&workspace_name, item))
                    .collect(),
                pagination: Some(pagination),
            }))
        })
        .await
    }

    async fn describe_table(
        &self,
        request: Request<DescribeTableRequest>,
    ) -> Result<Response<DescribeTableResponse>, Status> {
        let span = grpc_span(&request);
        let catalog = self.catalog.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let schema_name = required_trimmed(&request.schema_name, "schema_name")?;
            let table_name = required_trimmed(&request.table_name, "table_name")?;
            let result = catalog
                .describe_table(
                    &workspace_name,
                    CatalogTableRef::new(&schema_name, &table_name),
                )
                .await
                .map_err(query_status)?;
            Ok(Response::new(describe_table_response_to_proto(
                &workspace_name,
                result,
            )))
        })
        .await
    }

    async fn list_columns(
        &self,
        request: Request<ListColumnsRequest>,
    ) -> Result<Response<ListColumnsResponse>, Status> {
        let span = grpc_span(&request);
        let catalog = self.catalog.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let schema_name = required_trimmed(&request.schema_name, "schema_name")?;
            let table_name = required_trimmed(&request.table_name, "table_name")?;
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
            Ok(Response::new(ListColumnsResponse {
                columns: page
                    .items
                    .into_iter()
                    .map(column_search_result_to_proto)
                    .collect(),
                pagination: Some(pagination),
            }))
        })
        .await
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
