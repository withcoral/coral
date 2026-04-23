//! Implements the gRPC `SourceService` for source lifecycle APIs.

use coral_api::v1::source_service_server::SourceService as SourceServiceApi;
use coral_api::v1::{
    AvailableSource, CreateBundledSourceRequest, DeleteSourceRequest, DiscoverSourcesRequest,
    DiscoverSourcesResponse, GetSourceRequest, ImportSourceRequest, ListSourcesRequest,
    ListSourcesResponse, Source, SourceInputKind, SourceInputSpec,
    SourceOrigin as ProtoSourceOrigin, SourceSecret, SourceVariable, ValidateSourceRequest,
    ValidateSourceResponse,
};
use tonic::{Request, Response, Status};

use crate::bootstrap::app_status;
use crate::query::manager::QueryManager;
use crate::sources::SourceName;
use crate::sources::manager::SourceManager;
use crate::sources::model::{
    CandidateSource, CandidateSourceInput, CandidateSourceInputKind, InstalledSource, SourceOrigin,
};
use crate::transport::{
    query_status, validate_source_response_to_proto, workspace_name_from_proto, workspace_to_proto,
};
use crate::workspaces::WorkspaceName;

#[derive(Clone)]
pub(crate) struct SourceService {
    sources: SourceManager,
    queries: QueryManager,
}

impl SourceService {
    pub(crate) fn new(source_manager: SourceManager, query_manager: QueryManager) -> Self {
        Self {
            sources: source_manager,
            queries: query_manager,
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
        let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
        let sources = self
            .sources
            .discover_sources(&workspace_name)
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
        let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
        let sources: Vec<_> = self
            .sources
            .list_workspace_sources(&workspace_name)
            .map_err(app_status)?
            .into_iter()
            .map(|source| installed_source_to_proto(&workspace_name, source))
            .collect();
        Ok(Response::new(ListSourcesResponse { sources }))
    }

    async fn get_source(
        &self,
        request: Request<GetSourceRequest>,
    ) -> Result<Response<Source>, Status> {
        let request = request.into_inner();
        let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
        let source_name = SourceName::parse(&request.name).map_err(app_status)?;
        let source = self
            .sources
            .get_source(&workspace_name, &source_name)
            .map_err(app_status)?;
        Ok(Response::new(installed_source_to_proto(
            &workspace_name,
            source,
        )))
    }

    async fn create_bundled_source(
        &self,
        request: Request<CreateBundledSourceRequest>,
    ) -> Result<Response<Source>, Status> {
        let request = request.into_inner();
        let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
        let bundled_name = SourceName::parse(&request.name).map_err(app_status)?;
        let installed = self
            .sources
            .create_bundled_source(&workspace_name, &bundled_name, &request)
            .map_err(app_status)?;
        Ok(Response::new(installed_source_to_proto(
            &workspace_name,
            installed,
        )))
    }

    async fn import_source(
        &self,
        request: Request<ImportSourceRequest>,
    ) -> Result<Response<Source>, Status> {
        let request = request.into_inner();
        let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
        let installed = self
            .sources
            .import_source(&workspace_name, &request)
            .map_err(app_status)?;
        Ok(Response::new(installed_source_to_proto(
            &workspace_name,
            installed,
        )))
    }

    async fn delete_source(
        &self,
        request: Request<DeleteSourceRequest>,
    ) -> Result<Response<()>, Status> {
        let request = request.into_inner();
        let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
        let source_name = SourceName::parse(&request.name).map_err(app_status)?;
        let _installed = self
            .sources
            .delete_source(&workspace_name, &source_name)
            .map_err(app_status)?;
        Ok(Response::new(()))
    }

    async fn validate_source(
        &self,
        request: Request<ValidateSourceRequest>,
    ) -> Result<Response<ValidateSourceResponse>, Status> {
        let request = request.into_inner();
        let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
        let source_name = SourceName::parse(&request.name).map_err(app_status)?;
        let result = self
            .queries
            .validate_source(&workspace_name, &source_name)
            .await
            .map_err(query_status)?;
        let crate::query::manager::ValidatedSource { source, report } = result;
        let source = installed_source_to_proto(&workspace_name, source);
        Ok(Response::new(validate_source_response_to_proto(
            source,
            &workspace_name,
            report,
        )))
    }
}

fn installed_source_to_proto(workspace_name: &WorkspaceName, source: InstalledSource) -> Source {
    Source {
        workspace: Some(workspace_to_proto(workspace_name)),
        name: source.name.as_str().to_string(),
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
        name: source.name.as_str().to_string(),
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
        hint: input.hint.unwrap_or_default(),
    }
}

fn proto_candidate_input_kind(kind: CandidateSourceInputKind) -> SourceInputKind {
    match kind {
        CandidateSourceInputKind::Variable => SourceInputKind::Variable,
        CandidateSourceInputKind::Secret => SourceInputKind::Secret,
    }
}
