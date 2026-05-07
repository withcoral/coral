//! Implements the gRPC `SourceService` for source lifecycle APIs.

use coral_api::v1::source_service_server::SourceService as SourceServiceApi;
use coral_api::v1::{
    CreateBundledSourceRequest, CreateBundledSourceResponse, DeleteSourceRequest,
    DeleteSourceResponse, DiscoverSourcesRequest, DiscoverSourcesResponse, GetSourceInfoRequest,
    GetSourceInfoResponse, GetSourceRequest, GetSourceResponse, ImportSourceRequest,
    ImportSourceResponse, ListSourcesRequest, ListSourcesResponse, Source, SourceInfo,
    SourceInputKind, SourceInputSpec, SourceOrigin as ProtoSourceOrigin, SourceSecret,
    SourceVariable, ValidateSourceRequest, ValidateSourceResponse,
};
use coral_spec::{ManifestInputKind, ManifestInputSpec};
use tonic::{Request, Response, Status};

use crate::bootstrap::app_status;
use crate::query::manager::QueryManager;
use crate::sources::SourceName;
use crate::sources::manager::{
    CreateBundledSourceCommand, ImportSourceCommand, SourceBinding, SourceBindings, SourceManager,
};
use crate::sources::model::{CandidateSource, InstalledSource, SourceOrigin};
use crate::transport::{
    grpc_span, instrument_grpc, query_status, validate_source_response_to_proto,
    workspace_name_from_proto, workspace_to_proto,
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
        let span = grpc_span(request.metadata(), "discover_sources");
        let sources = self.sources.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let sources = sources
                .discover_sources(&workspace_name)
                .map_err(app_status)?
                .into_iter()
                .map(candidate_source_to_proto)
                .collect();
            Ok(Response::new(DiscoverSourcesResponse { sources }))
        })
        .await
    }

    async fn list_sources(
        &self,
        request: Request<ListSourcesRequest>,
    ) -> Result<Response<ListSourcesResponse>, Status> {
        let span = grpc_span(request.metadata(), "list_sources");
        let sources = self.sources.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let sources: Vec<_> = sources
                .list_workspace_sources(&workspace_name)
                .map_err(app_status)?
                .into_iter()
                .map(|source| installed_source_to_proto(&workspace_name, source))
                .collect();
            Ok(Response::new(ListSourcesResponse { sources }))
        })
        .await
    }

    async fn get_source(
        &self,
        request: Request<GetSourceRequest>,
    ) -> Result<Response<GetSourceResponse>, Status> {
        let span = grpc_span(request.metadata(), "get_source");
        let sources = self.sources.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let source_name = SourceName::parse(&request.name).map_err(app_status)?;
            let source = sources
                .get_source(&workspace_name, &source_name)
                .map_err(app_status)?;
            Ok(Response::new(GetSourceResponse {
                source: Some(installed_source_to_proto(&workspace_name, source)),
            }))
        })
        .await
    }

    async fn get_source_info(
        &self,
        request: Request<GetSourceInfoRequest>,
    ) -> Result<Response<GetSourceInfoResponse>, Status> {
        let span = grpc_span(request.metadata(), "get_source_info");
        let sources = self.sources.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let source_name = SourceName::parse(&request.name).map_err(app_status)?;
            let source = sources
                .get_source_info(&workspace_name, &source_name)
                .map_err(app_status)?;
            Ok(Response::new(GetSourceInfoResponse {
                source_info: Some(candidate_source_to_proto(source)),
            }))
        })
        .await
    }

    async fn create_bundled_source(
        &self,
        request: Request<CreateBundledSourceRequest>,
    ) -> Result<Response<CreateBundledSourceResponse>, Status> {
        let span = grpc_span(request.metadata(), "create_bundled_source");
        let sources = self.sources.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let bundled_name = SourceName::parse(&request.name).map_err(app_status)?;
            let command = CreateBundledSourceCommand {
                name: bundled_name,
                bindings: source_bindings_from_proto(request.variables, request.secrets),
            };
            let installed = sources
                .create_bundled_source(&workspace_name, &command)
                .map_err(app_status)?;
            Ok(Response::new(CreateBundledSourceResponse {
                source: Some(installed_source_to_proto(&workspace_name, installed)),
            }))
        })
        .await
    }

    async fn import_source(
        &self,
        request: Request<ImportSourceRequest>,
    ) -> Result<Response<ImportSourceResponse>, Status> {
        let span = grpc_span(request.metadata(), "import_source");
        let sources = self.sources.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let command = ImportSourceCommand {
                manifest_yaml: request.manifest_yaml,
                bindings: source_bindings_from_proto(request.variables, request.secrets),
            };
            let installed = sources
                .import_source(&workspace_name, &command)
                .map_err(app_status)?;
            Ok(Response::new(ImportSourceResponse {
                source: Some(installed_source_to_proto(&workspace_name, installed)),
            }))
        })
        .await
    }

    async fn delete_source(
        &self,
        request: Request<DeleteSourceRequest>,
    ) -> Result<Response<DeleteSourceResponse>, Status> {
        let span = grpc_span(request.metadata(), "delete_source");
        let sources = self.sources.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let source_name = SourceName::parse(&request.name).map_err(app_status)?;
            sources
                .delete_source(&workspace_name, &source_name)
                .map_err(app_status)?;
            Ok(Response::new(DeleteSourceResponse {}))
        })
        .await
    }

    async fn validate_source(
        &self,
        request: Request<ValidateSourceRequest>,
    ) -> Result<Response<ValidateSourceResponse>, Status> {
        let span = grpc_span(request.metadata(), "validate_source");
        let queries = self.queries.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let source_name = SourceName::parse(&request.name).map_err(app_status)?;
            let result = queries
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
        })
        .await
    }
}

fn source_bindings_from_proto(
    variables: Vec<SourceVariable>,
    secrets: Vec<SourceSecret>,
) -> SourceBindings {
    SourceBindings {
        variables: variables
            .into_iter()
            .map(source_variable_from_proto)
            .collect(),
        secrets: secrets.into_iter().map(source_secret_from_proto).collect(),
    }
}

fn source_variable_from_proto(variable: SourceVariable) -> SourceBinding {
    SourceBinding {
        key: variable.key,
        value: variable.value,
    }
}

fn source_secret_from_proto(secret: SourceSecret) -> SourceBinding {
    SourceBinding {
        key: secret.key,
        value: secret.value,
    }
}

fn installed_source_to_proto(workspace_name: &WorkspaceName, source: InstalledSource) -> Source {
    Source {
        workspace: Some(workspace_to_proto(workspace_name)),
        name: source.name.as_str().to_string(),
        version: source.version.unwrap_or_default(),
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

fn candidate_source_to_proto(source: CandidateSource) -> SourceInfo {
    SourceInfo {
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

fn candidate_source_input_to_proto(input: ManifestInputSpec) -> SourceInputSpec {
    SourceInputSpec {
        key: input.key,
        kind: proto_candidate_input_kind(input.kind) as i32,
        required: input.required,
        default_value: input.default_value,
        hint: input.hint.unwrap_or_default(),
    }
}

fn proto_candidate_input_kind(kind: ManifestInputKind) -> SourceInputKind {
    match kind {
        ManifestInputKind::Variable => SourceInputKind::Variable,
        ManifestInputKind::Secret => SourceInputKind::Secret,
    }
}
