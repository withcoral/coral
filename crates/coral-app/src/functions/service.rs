//! Implements the gRPC `FunctionService`.

use coral_api::v1::function_service_server::FunctionService as FunctionServiceApi;
use coral_api::v1::{
    AddFunctionRequest, AddFunctionResponse, Function, FunctionArgument, FunctionPublish,
    FunctionResultColumn, FunctionTableFunctionPublish, ListFunctionsRequest,
    ListFunctionsResponse, RemoveFunctionRequest, RemoveFunctionResponse,
};
use coral_engine::{UdfRuntimeDefinition, UdfRuntimePublish, UdfRuntimeTableFunctionPublish};
use tonic::{Request, Response, Status};

use crate::bootstrap::app_status;
use crate::functions::manager::{FunctionListing, FunctionManager};
use crate::functions::model::FunctionName;
use crate::query::manager::QueryManager;
use crate::transport::{grpc_span, instrument_grpc, query_status, workspace_name_from_proto};

#[derive(Clone)]
pub(crate) struct FunctionService {
    functions: FunctionManager,
    queries: QueryManager,
}

impl FunctionService {
    pub(crate) fn new(function_manager: FunctionManager, query_manager: QueryManager) -> Self {
        Self {
            functions: function_manager,
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
        let functions = self.functions.clone();
        let queries = self.queries.clone();
        instrument_grpc(span, async move {
            let inner = request.into_inner();
            let workspace_name = workspace_name_from_proto(inner.workspace.as_ref())?;
            let runtime_function = queries
                .validate_udf_sql(&workspace_name, &inner.sql)
                .await
                .map_err(query_status)?;
            let function = functions
                .install_validated_user_function(&workspace_name, &inner.sql, &runtime_function)
                .map_err(app_status)?;
            Ok(Response::new(AddFunctionResponse {
                name: function.name.as_str().to_string(),
            }))
        })
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
                .map(function_listing_to_proto)
                .collect();
            Ok(Response::new(ListFunctionsResponse { functions }))
        })
        .await
    }

    async fn remove_function(
        &self,
        request: Request<RemoveFunctionRequest>,
    ) -> Result<Response<RemoveFunctionResponse>, Status> {
        let span = grpc_span(&request);
        let functions = self.functions.clone();
        instrument_grpc(span, async move {
            let inner = request.into_inner();
            let workspace_name = workspace_name_from_proto(inner.workspace.as_ref())?;
            let function_name = FunctionName::parse(&inner.name).map_err(app_status)?;
            functions
                .remove_user_function(&workspace_name, &function_name)
                .map_err(app_status)?;
            Ok(Response::new(RemoveFunctionResponse {}))
        })
        .await
    }
}

fn function_listing_to_proto(listing: FunctionListing) -> Function {
    runtime_function_to_proto(listing.definition)
}

fn runtime_function_to_proto(function: UdfRuntimeDefinition) -> Function {
    Function {
        name: function.name,
        description: function.description,
        arguments: function
            .arguments
            .into_iter()
            .map(|argument| FunctionArgument {
                name: argument.name,
                data_type: argument.data_type.as_manifest_str().to_string(),
                description: String::new(),
            })
            .collect(),
        publish: Some(function_publish_to_proto(function.publish)),
        result_columns: function
            .result_columns
            .into_iter()
            .map(|column| FunctionResultColumn {
                name: column.name,
                data_type: column.data_type.to_string(),
                nullable: column.nullable,
                description: String::new(),
            })
            .collect(),
    }
}

fn function_publish_to_proto(publish: UdfRuntimePublish) -> FunctionPublish {
    FunctionPublish {
        table_function: Some(function_table_function_publish_to_proto(
            publish.table_function,
        )),
    }
}

fn function_table_function_publish_to_proto(
    publish: UdfRuntimeTableFunctionPublish,
) -> FunctionTableFunctionPublish {
    FunctionTableFunctionPublish {
        schema: publish.schema,
        name: publish.name,
        description: publish.description,
    }
}
