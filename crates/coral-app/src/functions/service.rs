//! Implements the gRPC `FunctionService`.

use coral_api::v1::function_service_server::FunctionService as FunctionServiceApi;
use coral_api::v1::{
    AddFunctionRequest, AddFunctionResponse, DeleteFunctionRequest, DeleteFunctionResponse,
    Function, FunctionArgument, FunctionRuntimeInvalid, FunctionRuntimeReady,
    FunctionTableFunctionPublish, FunctionWriteSurface as ProtoFunctionWriteSurface,
    ListFunctionsRequest, ListFunctionsResponse, TableFunctionResultColumn, function,
};
use coral_engine::{CoralSqlFunctionDefinition, CoralSqlTableFunctionPublish};
use tonic::{Request, Response, Status};

use crate::bootstrap::app_status;
use crate::functions::manager::{FunctionInstallMode, FunctionListing, FunctionRuntimeStatus};
use crate::functions::model::{FunctionName, FunctionWriteSurface};
use crate::query::manager::QueryManager;
use crate::transport::{
    grpc_span, instrument_grpc, query_status, workspace_name_from_proto, workspace_to_proto,
};
use crate::workspaces::WorkspaceName;

#[derive(Clone)]
pub(crate) struct FunctionService {
    queries: QueryManager,
}

impl FunctionService {
    pub(crate) fn new(query_manager: QueryManager) -> Self {
        Self {
            queries: query_manager,
        }
    }
}

#[tonic::async_trait]
impl FunctionServiceApi for FunctionService {
    async fn add_function(
        &self,
        request: Request<AddFunctionRequest>,
    ) -> Result<Response<AddFunctionResponse>, Status> {
        let span = grpc_span(&request);
        let queries = self.queries.clone();
        Box::pin(instrument_grpc(span, async move {
            let inner = request.into_inner();
            let workspace_name = workspace_name_from_proto(inner.workspace.as_ref())?;
            let mode = if inner.fail_if_exists {
                FunctionInstallMode::CreateOnly
            } else {
                FunctionInstallMode::ReplaceExisting
            };
            let write_surface = function_write_surface_from_proto(inner.write_surface);
            let artifact = inner.sql;
            let added = queries
                .add_user_function(&workspace_name, &artifact, mode, write_surface)
                .await
                .map_err(query_status)?;
            Ok(Response::new(AddFunctionResponse {
                function: Some(runtime_function_to_proto(
                    &workspace_name,
                    added.definition,
                    added.write_surface,
                )),
                replaced: added.replaced,
            }))
        }))
        .await
    }

    async fn list_functions(
        &self,
        request: Request<ListFunctionsRequest>,
    ) -> Result<Response<ListFunctionsResponse>, Status> {
        let span = grpc_span(&request);
        let queries = self.queries.clone();
        instrument_grpc(span, async move {
            let inner = request.into_inner();
            let workspace_name = workspace_name_from_proto(inner.workspace.as_ref())?;
            let functions = queries
                .list_functions(&workspace_name)
                .await
                .map_err(query_status)?
                .into_iter()
                .map(|listing| function_listing_to_proto(&workspace_name, listing))
                .collect();
            Ok(Response::new(ListFunctionsResponse { functions }))
        })
        .await
    }

    async fn delete_function(
        &self,
        request: Request<DeleteFunctionRequest>,
    ) -> Result<Response<DeleteFunctionResponse>, Status> {
        let span = grpc_span(&request);
        let queries = self.queries.clone();
        instrument_grpc(span, async move {
            let inner = request.into_inner();
            let workspace_name = workspace_name_from_proto(inner.workspace.as_ref())?;
            let function_name = FunctionName::parse(&inner.name).map_err(app_status)?;
            queries
                .function_manager()
                .remove_user_function(&workspace_name, &function_name)
                .await
                .map_err(app_status)?;
            Ok(Response::new(DeleteFunctionResponse {}))
        })
        .await
    }
}

fn function_listing_to_proto(workspace_name: &WorkspaceName, listing: FunctionListing) -> Function {
    let FunctionListing {
        name,
        write_surface,
        runtime,
    } = listing;
    match runtime {
        FunctionRuntimeStatus::Ready(definition) => {
            runtime_function_to_proto(workspace_name, *definition, write_surface)
        }
        FunctionRuntimeStatus::Invalid(reason) => Function {
            name: name.to_string(),
            workspace: Some(workspace_to_proto(workspace_name)),
            runtime: Some(function::Runtime::Invalid(FunctionRuntimeInvalid {
                reason,
            })),
            write_surface: function_write_surface_to_proto(write_surface),
        },
    }
}

fn runtime_function_to_proto(
    workspace_name: &WorkspaceName,
    function: CoralSqlFunctionDefinition,
    write_surface: FunctionWriteSurface,
) -> Function {
    let name = function.name;
    Function {
        workspace: Some(workspace_to_proto(workspace_name)),
        name,
        runtime: Some(function::Runtime::Ready(FunctionRuntimeReady {
            description: function.description,
            arguments: function
                .arguments
                .into_iter()
                .map(|argument| FunctionArgument {
                    name: argument.name,
                    data_type: argument.data_type.as_manifest_str().to_string(),
                })
                .collect(),
            table_function: Some(function_table_function_publish_to_proto(function.publish)),
            result_columns: function
                .result_columns
                .into_iter()
                .map(|column| TableFunctionResultColumn {
                    name: column.name,
                    data_type: column.data_type.to_string(),
                    nullable: column.nullable,
                    description: String::new(),
                })
                .collect(),
            sql_body: function.query,
            source_names: function.source_names,
        })),
        write_surface: function_write_surface_to_proto(write_surface),
    }
}

fn function_write_surface_from_proto(value: i32) -> FunctionWriteSurface {
    match ProtoFunctionWriteSurface::try_from(value) {
        Ok(ProtoFunctionWriteSurface::Cli) => FunctionWriteSurface::Cli,
        Ok(ProtoFunctionWriteSurface::Mcp) => FunctionWriteSurface::Mcp,
        Ok(ProtoFunctionWriteSurface::Unspecified) | Err(_) => FunctionWriteSurface::Unknown,
    }
}

fn function_write_surface_to_proto(value: FunctionWriteSurface) -> i32 {
    match value {
        FunctionWriteSurface::Unknown => ProtoFunctionWriteSurface::Unspecified as i32,
        FunctionWriteSurface::Cli => ProtoFunctionWriteSurface::Cli as i32,
        FunctionWriteSurface::Mcp => ProtoFunctionWriteSurface::Mcp as i32,
    }
}

fn function_table_function_publish_to_proto(
    publish: CoralSqlTableFunctionPublish,
) -> FunctionTableFunctionPublish {
    FunctionTableFunctionPublish {
        schema_name: publish.schema,
        name: publish.name,
        description: publish.description,
        guide: publish.guide,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_function_listing_keeps_inventory_identity_and_error() {
        let workspace = WorkspaceName::parse("default").expect("workspace");
        let listing = FunctionListing {
            name: FunctionName::parse("review_queue").expect("function"),
            write_surface: FunctionWriteSurface::Unknown,
            runtime: FunctionRuntimeStatus::Invalid("function file is missing".to_string()),
        };

        let function = function_listing_to_proto(&workspace, listing);

        assert_eq!(function.name, "review_queue");
        assert!(matches!(
            function.runtime,
            Some(function::Runtime::Invalid(FunctionRuntimeInvalid { reason }))
                if reason == "function file is missing"
        ));
        assert_eq!(
            function.workspace.expect("workspace").name,
            workspace.as_str()
        );
    }
}
