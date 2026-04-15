//! Implements the gRPC `SourceService` for source lifecycle APIs.

use coral_api::v1::source_service_server::SourceService as SourceServiceApi;
use coral_api::v1::{
    CreateBundledSourceRequest, DeleteSourceRequest, DiscoverSourcesRequest,
    DiscoverSourcesResponse, GetSourceRequest, ImportSourceRequest, ListSourcesRequest,
    ListSourcesResponse, Source, ValidateSourceRequest, ValidateSourceResponse,
};
use tonic::{Request, Response, Status};

use crate::bootstrap::app_status;
use crate::query::manager::QueryManager;
use crate::sources::manager::SourceManager;
use crate::transport::{query_status, table_to_proto};
use crate::workspaces::WorkspaceManager;

#[derive(Clone)]
pub(crate) struct SourceService {
    sources: SourceManager,
    queries: QueryManager,
    workspaces: WorkspaceManager,
}

impl SourceService {
    pub(crate) fn new(
        source_manager: SourceManager,
        query_manager: QueryManager,
        workspace_manager: WorkspaceManager,
    ) -> Self {
        Self {
            sources: source_manager,
            queries: query_manager,
            workspaces: workspace_manager,
        }
    }
}

#[tonic::async_trait]
impl SourceServiceApi for SourceService {
    async fn discover_sources(
        &self,
        request: Request<DiscoverSourcesRequest>,
    ) -> Result<Response<DiscoverSourcesResponse>, Status> {
        let request = request.into_inner();
        let workspace = self.workspaces.require(request.workspace.as_ref())?;
        let sources = self
            .sources
            .discover_sources(&workspace)
            .map_err(app_status)?;
        Ok(Response::new(DiscoverSourcesResponse { sources }))
    }

    async fn list_sources(
        &self,
        request: Request<ListSourcesRequest>,
    ) -> Result<Response<ListSourcesResponse>, Status> {
        let request = request.into_inner();
        let workspace = self.workspaces.require(request.workspace.as_ref())?;
        let sources: Vec<_> = self
            .sources
            .list_workspace_sources(&workspace)
            .map_err(app_status)?
            .into_iter()
            .map(|source| source.to_source_resource())
            .collect();
        Ok(Response::new(ListSourcesResponse { sources }))
    }

    async fn get_source(
        &self,
        request: Request<GetSourceRequest>,
    ) -> Result<Response<Source>, Status> {
        let request = request.into_inner();
        let workspace = self.workspaces.require(request.workspace.as_ref())?;
        let source_name = self
            .workspaces
            .status_validate_path_name("source name", &request.name)?;
        let source = self
            .sources
            .get_source(&workspace, &source_name)
            .map_err(app_status)?;
        Ok(Response::new(source.to_source_resource()))
    }

    async fn create_bundled_source(
        &self,
        request: Request<CreateBundledSourceRequest>,
    ) -> Result<Response<Source>, Status> {
        let request = request.into_inner();
        let stored = self
            .sources
            .create_bundled_source(&request)
            .map_err(app_status)?;
        Ok(Response::new(stored.to_source_resource()))
    }

    async fn import_source(
        &self,
        request: Request<ImportSourceRequest>,
    ) -> Result<Response<Source>, Status> {
        let request = request.into_inner();
        let stored = self.sources.import_source(&request).map_err(app_status)?;
        Ok(Response::new(stored.to_source_resource()))
    }

    async fn delete_source(
        &self,
        request: Request<DeleteSourceRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        let workspace = self.workspaces.require(request.workspace.as_ref())?;
        let source_name = self
            .workspaces
            .status_validate_path_name("source name", &request.name)?;
        let _stored = self
            .sources
            .delete_source(&workspace, &source_name)
            .map_err(app_status)?;
        Ok(Response::new(()))
    }

    async fn validate_source(
        &self,
        request: Request<ValidateSourceRequest>,
    ) -> Result<Response<ValidateSourceResponse>, Status> {
        let request = request.into_inner();
        let workspace = self.workspaces.require(request.workspace.as_ref())?;
        let source_name = self
            .workspaces
            .status_validate_path_name("source name", &request.name)?;
        let result = self
            .queries
            .validate_source(&workspace, &source_name)
            .await
            .map_err(query_status)?;
        let tables = result
            .tables
            .into_iter()
            .map(|table| table_to_proto(&workspace, table))
            .collect::<Vec<_>>();
        Ok(Response::new(ValidateSourceResponse {
            source: Some(result.source.to_source_resource()),
            tables,
        }))
    }
}
