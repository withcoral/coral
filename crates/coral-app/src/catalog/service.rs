//! Implements the gRPC `CatalogService`.

use coral_api::v1::catalog_service_server::CatalogService as CatalogServiceApi;
use coral_api::v1::{
    CatalogCounts as ProtoCatalogCounts, CatalogItemKind as ProtoCatalogItemKind,
    DescribeTableRequest, DescribeTableResponse, ListCatalogRequest, ListCatalogResponse,
    ListColumnsRequest, ListColumnsResponse, PaginationRequest, SearchCatalogRequest,
    SearchCatalogResponse,
};
use tonic::{Request, Response, Status};

use crate::bootstrap::app_status;
use crate::catalog::discovery::{
    CatalogDiscovery, CatalogItemKind, CatalogTableRef, ListColumnsQuery, Pagination,
    SearchCatalogQuery, column_pagination, search_pagination,
};
use crate::query::QueryAttribution;
use crate::query::manager::QueryManager;
use crate::request_context::RequestContext;
use crate::task::manager::TaskManager;
use crate::task::service::task_manager_status;
use crate::transport::{
    catalog_item_to_proto, catalog_search_result_to_proto, column_search_result_to_proto,
    describe_table_response_to_proto, grpc_span, instrument_grpc, pagination_to_proto,
    query_status, request_context, workspace_name_from_proto,
};
use crate::workspaces::WorkspaceName;

#[derive(Clone)]
pub(crate) struct CatalogService {
    catalog: CatalogDiscovery,
    tasks: TaskManager,
}

impl CatalogService {
    pub(crate) fn new(query_manager: QueryManager, task_manager: TaskManager) -> Self {
        Self {
            catalog: CatalogDiscovery::new(query_manager),
            tasks: task_manager,
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
        let tasks = self.tasks.clone();
        let request_context = request_context(&request)?.clone();
        Box::pin(instrument_grpc(span, async move {
            let request = request.into_inner();
            let pagination = pagination_from_proto(request.pagination.unwrap_or_default());
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let attribution = query_attribution(&tasks, &workspace_name, &request_context).await?;
            let catalog_name = optional_trimmed(&request.catalog_name);
            let schema_name = optional_trimmed(&request.schema_name);
            validate_catalog_schema(catalog_name, schema_name).map_err(app_status)?;
            let kind = catalog_item_kind_from_proto(request.kind)?;
            let catalog_page = catalog
                .list_catalog(
                    &workspace_name,
                    catalog_name,
                    schema_name,
                    kind,
                    pagination,
                    &attribution,
                )
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
            Ok(Response::new(ListCatalogResponse {
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
            }))
        }))
        .await
    }

    async fn search_catalog(
        &self,
        request: Request<SearchCatalogRequest>,
    ) -> Result<Response<SearchCatalogResponse>, Status> {
        let span = grpc_span(&request);
        let catalog = self.catalog.clone();
        let tasks = self.tasks.clone();
        let request_context = request_context(&request)?.clone();
        Box::pin(instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let attribution = query_attribution(&tasks, &workspace_name, &request_context).await?;
            let catalog_name = optional_trimmed(&request.catalog_name);
            let schema_name = optional_trimmed(&request.schema_name);
            validate_catalog_schema(catalog_name, schema_name).map_err(app_status)?;
            let kind = catalog_item_kind_from_proto(request.kind)?;
            let pagination = search_pagination(request.pagination.map(pagination_from_proto))
                .map_err(app_status)?;
            let page = catalog
                .search_catalog(
                    &workspace_name,
                    SearchCatalogQuery {
                        pattern: &request.pattern,
                        catalog_name,
                        schema_name,
                        kind,
                        ignore_case: request.ignore_case,
                        pagination,
                    },
                    &attribution,
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
            Ok(Response::new(SearchCatalogResponse {
                items: page
                    .items
                    .into_iter()
                    .map(|result| catalog_search_result_to_proto(&workspace_name, result))
                    .collect(),
                pagination: Some(pagination),
            }))
        }))
        .await
    }

    async fn describe_table(
        &self,
        request: Request<DescribeTableRequest>,
    ) -> Result<Response<DescribeTableResponse>, Status> {
        let span = grpc_span(&request);
        let catalog = self.catalog.clone();
        let tasks = self.tasks.clone();
        let request_context = request_context(&request)?.clone();
        Box::pin(instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let attribution = query_attribution(&tasks, &workspace_name, &request_context).await?;
            let catalog_name = optional_trimmed(&request.catalog_name);
            let schema_name = required_trimmed(&request.schema_name, "schema_name")?;
            let table_name = required_trimmed(&request.table_name, "table_name")?;
            validate_catalog_schema(catalog_name, Some(&schema_name)).map_err(app_status)?;
            let result = catalog
                .describe_table(
                    &workspace_name,
                    CatalogTableRef::new(catalog_name, &schema_name, &table_name),
                    &attribution,
                )
                .await
                .map_err(query_status)?;
            Ok(Response::new(describe_table_response_to_proto(
                &workspace_name,
                result,
            )))
        }))
        .await
    }

    async fn list_columns(
        &self,
        request: Request<ListColumnsRequest>,
    ) -> Result<Response<ListColumnsResponse>, Status> {
        let span = grpc_span(&request);
        let catalog = self.catalog.clone();
        let tasks = self.tasks.clone();
        let request_context = request_context(&request)?.clone();
        Box::pin(instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let attribution = query_attribution(&tasks, &workspace_name, &request_context).await?;
            let catalog_name = optional_trimmed(&request.catalog_name);
            let schema_name = required_trimmed(&request.schema_name, "schema_name")?;
            let table_name = required_trimmed(&request.table_name, "table_name")?;
            validate_catalog_schema(catalog_name, Some(&schema_name)).map_err(app_status)?;
            let pagination = column_pagination(request.pagination.map(pagination_from_proto))
                .map_err(app_status)?;
            let page = catalog
                .list_columns(
                    &workspace_name,
                    ListColumnsQuery {
                        table_ref: CatalogTableRef::new(catalog_name, &schema_name, &table_name),
                        pattern: request.pattern.as_deref(),
                        ignore_case: request.ignore_case,
                        required_only: request.required_only,
                        pagination,
                    },
                    &attribution,
                )
                .await
                .map_err(query_status)?
                .ok_or_else(|| {
                    let qualifier = catalog_name.map_or_else(
                        || schema_name.clone(),
                        |catalog| format!("{catalog}.{schema_name}"),
                    );
                    Status::not_found(format!("table '{qualifier}.{table_name}' not found"))
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
        }))
        .await
    }
}

async fn query_attribution(
    tasks: &TaskManager,
    workspace: &WorkspaceName,
    request_context: &RequestContext,
) -> Result<QueryAttribution, Status> {
    tasks
        .validate_attribution(workspace, request_context.task_id())
        .await
        .map(QueryAttribution::new)
        .map_err(task_manager_status)
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

fn validate_catalog_schema(
    catalog_name: Option<&str>,
    schema_name: Option<&str>,
) -> Result<(), crate::bootstrap::AppError> {
    if catalog_name.is_some() {
        return Ok(());
    }
    let Some(schema_name) = schema_name else {
        return Ok(());
    };
    let Some((catalog, schema)) = schema_name.split_once('.') else {
        return Ok(());
    };
    if catalog.is_empty() || schema.is_empty() {
        return Ok(());
    }
    Err(crate::bootstrap::AppError::InvalidInput(format!(
        "schema_name '{schema_name}' combines a catalog and schema; pass catalog_name '{catalog}' \
         and schema_name '{schema}' separately"
    )))
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

#[cfg(test)]
mod tests {
    use super::validate_catalog_schema;

    #[test]
    fn compound_schema_requires_separate_catalog_argument() {
        let error = validate_catalog_schema(None, Some("warehouse.public"))
            .expect_err("compound schema must be rejected");
        let detail = error.to_string();
        assert!(detail.contains("catalog_name 'warehouse'"));
        assert!(detail.contains("schema_name 'public'"));

        validate_catalog_schema(Some("warehouse"), Some("public"))
            .expect("separate catalog and schema are valid");
        validate_catalog_schema(None, Some("public")).expect("plain schema is valid");
    }
}
