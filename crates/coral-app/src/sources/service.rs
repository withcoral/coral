//! Implements the gRPC `SourceService` for source lifecycle APIs.

use coral_api::v1::source_service_server::SourceService as SourceServiceApi;
use coral_api::v1::{
    AvailableSource, Column, CreateBundledSourceRequest, DeleteSourceRequest,
    DiscoverSourcesRequest, DiscoverSourcesResponse, GetSourceRequest, ImportSourceRequest,
    ListSourcesRequest, ListSourcesResponse, Source, SourceInputKind, SourceInputSpec,
    SourceOrigin as ProtoSourceOrigin, SourceSecret, SourceVariable, Table, ValidateSourceRequest,
    ValidateSourceResponse, Workspace,
};
use tonic::{Request, Response, Status};

use crate::bootstrap::{app_status, core_status};
use crate::query::manager::{QueryManager, QueryManagerError};
use crate::sources::manager::SourceManager;
use crate::sources::model::{
    CandidateSource, CandidateSourceInput, CandidateSourceInputKind, InstalledSource, SourceOrigin,
};
use crate::workspaces::WorkspaceValidator;

#[derive(Clone)]
pub(crate) struct SourceService {
    sources: SourceManager,
    queries: QueryManager,
    workspaces: WorkspaceValidator,
}

impl SourceService {
    pub(crate) fn new(
        source_manager: SourceManager,
        query_manager: QueryManager,
        workspace_validator: WorkspaceValidator,
    ) -> Self {
        Self {
            sources: source_manager,
            queries: query_manager,
            workspaces: workspace_validator,
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
            .map_err(app_status)?
            .into_iter()
            .map(candidate_source_to_proto)
            .collect();
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
            .map(|source| installed_source_to_proto(&workspace, source))
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
        Ok(Response::new(installed_source_to_proto(&workspace, source)))
    }

    async fn create_bundled_source(
        &self,
        request: Request<CreateBundledSourceRequest>,
    ) -> Result<Response<Source>, Status> {
        let request = request.into_inner();
        let workspace = self.workspaces.require(request.workspace.as_ref())?;
        let installed = self
            .sources
            .create_bundled_source(&workspace, &request)
            .map_err(app_status)?;
        Ok(Response::new(installed_source_to_proto(
            &workspace, installed,
        )))
    }

    async fn import_source(
        &self,
        request: Request<ImportSourceRequest>,
    ) -> Result<Response<Source>, Status> {
        let request = request.into_inner();
        let workspace = self.workspaces.require(request.workspace.as_ref())?;
        let installed = self
            .sources
            .import_source(&workspace, &request)
            .map_err(app_status)?;
        Ok(Response::new(installed_source_to_proto(
            &workspace, installed,
        )))
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
        let _installed = self
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
            source: Some(installed_source_to_proto(&workspace, result.source)),
            tables,
        }))
    }
}

fn query_status(error: QueryManagerError) -> Status {
    match error {
        QueryManagerError::App(error) => app_status(error),
        QueryManagerError::Core(error) => core_status(error),
    }
}

fn table_to_proto(workspace: &Workspace, table: coral_engine::TableInfo) -> Table {
    Table {
        workspace: Some(workspace.clone()),
        schema_name: table.schema_name,
        name: table.table_name,
        description: table.description,
        columns: table
            .columns
            .into_iter()
            .map(|column| Column {
                name: column.name,
                data_type: column.data_type,
                nullable: column.nullable,
            })
            .collect(),
        required_filters: table.required_filters,
    }
}

fn installed_source_to_proto(workspace: &Workspace, source: InstalledSource) -> Source {
    Source {
        workspace: Some(workspace.clone()),
        name: source.name,
        version: source.version,
        secrets: source
            .secrets
            .into_iter()
            .map(|key| SourceSecret {
                key,
                value: String::new(),
            })
            .collect(),
        variables: source
            .variables
            .into_iter()
            .map(|(key, value)| SourceVariable { key, value })
            .collect(),
        origin: proto_source_origin(source.origin) as i32,
    }
}

fn proto_source_origin(origin: SourceOrigin) -> ProtoSourceOrigin {
    match origin {
        SourceOrigin::Bundled => ProtoSourceOrigin::Bundled,
        SourceOrigin::Imported => ProtoSourceOrigin::Imported,
    }
}

fn candidate_source_to_proto(source: CandidateSource) -> AvailableSource {
    AvailableSource {
        name: source.name,
        description: source.description,
        version: source.version,
        inputs: source
            .inputs
            .into_iter()
            .map(candidate_source_input_to_proto)
            .collect(),
        installed: source.installed,
        origin: proto_source_origin(source.origin) as i32,
    }
}

fn candidate_source_input_to_proto(input: CandidateSourceInput) -> SourceInputSpec {
    SourceInputSpec {
        key: input.key,
        kind: proto_candidate_input_kind(input.kind) as i32,
        required: input.required,
        default_value: input.default_value,
    }
}

fn proto_candidate_input_kind(kind: CandidateSourceInputKind) -> SourceInputKind {
    match kind {
        CandidateSourceInputKind::Variable => SourceInputKind::Variable,
        CandidateSourceInputKind::Secret => SourceInputKind::Secret,
    }
}
