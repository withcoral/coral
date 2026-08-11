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
use crate::workspaces::{WorkspaceAction, WorkspaceAuthorizer, WorkspaceName};

#[derive(Clone)]
pub(crate) struct CatalogService {
    catalog: CatalogDiscovery,
    tasks: TaskManager,
    workspace_authorizer: Option<WorkspaceAuthorizer>,
}

impl CatalogService {
    pub(crate) fn new(query_manager: QueryManager, task_manager: TaskManager) -> Self {
        Self {
            catalog: CatalogDiscovery::new(query_manager),
            tasks: task_manager,
            workspace_authorizer: None,
        }
    }

    pub(crate) fn with_authorizer(mut self, authorizer: WorkspaceAuthorizer) -> Self {
        self.workspace_authorizer = Some(authorizer);
        self
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
        let workspace_authorizer = self.workspace_authorizer.clone();
        let request_context = request_context(&request)?.clone();
        Box::pin(instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            authorize_read(
                workspace_authorizer.as_ref(),
                request_context.principal(),
                &workspace_name,
            )
            .await?;
            let pagination = pagination_from_proto(request.pagination.unwrap_or_default());
            let attribution = query_attribution(&tasks, &workspace_name, &request_context).await?;
            let catalog_name = optional_trimmed(&request.catalog_name);
            let schema_name = optional_trimmed(&request.schema_name);
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
        let workspace_authorizer = self.workspace_authorizer.clone();
        let request_context = request_context(&request)?.clone();
        Box::pin(instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            authorize_read(
                workspace_authorizer.as_ref(),
                request_context.principal(),
                &workspace_name,
            )
            .await?;
            let attribution = query_attribution(&tasks, &workspace_name, &request_context).await?;
            let catalog_name = optional_trimmed(&request.catalog_name);
            let schema_name = optional_trimmed(&request.schema_name);
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
        let workspace_authorizer = self.workspace_authorizer.clone();
        let request_context = request_context(&request)?.clone();
        Box::pin(instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            authorize_read(
                workspace_authorizer.as_ref(),
                request_context.principal(),
                &workspace_name,
            )
            .await?;
            let attribution = query_attribution(&tasks, &workspace_name, &request_context).await?;
            let catalog_name = optional_trimmed(&request.catalog_name);
            let schema_name = required_trimmed(&request.schema_name, "schema_name")?;
            let table_name = required_trimmed(&request.table_name, "table_name")?;
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
        let workspace_authorizer = self.workspace_authorizer.clone();
        let request_context = request_context(&request)?.clone();
        Box::pin(instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            authorize_read(
                workspace_authorizer.as_ref(),
                request_context.principal(),
                &workspace_name,
            )
            .await?;
            let attribution = query_attribution(&tasks, &workspace_name, &request_context).await?;
            let catalog_name = optional_trimmed(&request.catalog_name);
            let schema_name = required_trimmed(&request.schema_name, "schema_name")?;
            let table_name = required_trimmed(&request.table_name, "table_name")?;
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

async fn authorize_read(
    authorizer: Option<&WorkspaceAuthorizer>,
    principal: &crate::identity::Principal,
    workspace: &WorkspaceName,
) -> Result<(), Status> {
    let authorizer =
        authorizer.ok_or_else(|| Status::internal("workspace authorization is unavailable"))?;
    authorizer
        .authorize(principal, workspace, WorkspaceAction::Read)
        .await
        .map_err(app_status)
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
    use std::sync::Arc;

    use coral_api::{CORAL_ERROR_DOMAIN, CORAL_ERROR_REASON_WORKSPACE_NOT_FOUND};
    use tempfile::TempDir;
    use tonic::Code;
    use tonic_types::{ErrorDetail, StatusExt as _};

    use super::authorize_read;
    use crate::identity::{Principal, PrincipalKind};
    use crate::state::AppStateLayout;
    use crate::state::db::{
        AddMemberOutcome, CoralDb, DatabaseConfig, DbRepos, ResolvedDatabaseConfig,
        UpsertLoginOutcome,
    };
    use crate::workspaces::{MemberRole, WorkspaceAuthorizer, WorkspaceName};

    #[tokio::test]
    async fn read_authorization_allows_members_and_conceals_nonmembers() {
        let (_temp, db) = database().await;
        let owner_id = provision_user(&db, "owner").await;
        let member_id = provision_user(&db, "member").await;
        let nonmember_id = provision_user(&db, "nonmember").await;
        let workspace =
            WorkspaceName::parse(&format!("default-{owner_id}")).expect("owner workspace");
        let mut session = db.as_ref();
        assert!(matches!(
            session
                .workspaces()
                .add_member(workspace.as_str(), &member_id, MemberRole::Member, 2)
                .await
                .expect("add member"),
            AddMemberOutcome::Added(_)
        ));
        let authorizer = WorkspaceAuthorizer::new(db);
        let member = Principal::parse(&member_id, PrincipalKind::User).expect("member");
        let nonmember = Principal::parse(&nonmember_id, PrincipalKind::User).expect("nonmember");

        authorize_read(Some(&authorizer), &member, &workspace)
            .await
            .expect("member can read catalog data");
        let denied = authorize_read(Some(&authorizer), &nonmember, &workspace)
            .await
            .expect_err("nonmember cannot discover workspace existence");
        assert_eq!(denied.code(), Code::NotFound);
        let info = denied
            .get_error_details_vec()
            .into_iter()
            .find_map(|detail| match detail {
                ErrorDetail::ErrorInfo(info) => Some(info),
                _ => None,
            })
            .expect("structured workspace-not-found detail");
        assert_eq!(info.reason, CORAL_ERROR_REASON_WORKSPACE_NOT_FOUND);
        assert_eq!(info.domain, CORAL_ERROR_DOMAIN);
    }

    async fn database() -> (TempDir, Arc<CoralDb>) {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(&layout).expect("config") else {
            panic!("default test database must be SQLite")
        };
        let db = Arc::new(
            CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
                .await
                .expect("open sqlite"),
        );
        db.migrate().await.expect("migrate sqlite");
        (temp, db)
    }

    async fn provision_user(db: &CoralDb, subject: &str) -> String {
        let UpsertLoginOutcome::Upserted(user) = db
            .upsert_user_and_ensure_default_workspace("issuer", subject, None, 1)
            .await
            .expect("provision user")
        else {
            panic!("new subject should create a user")
        };
        user.user_id
    }
}
