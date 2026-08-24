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
    grpc_span, instrument_grpc, query_status, request_context, workspace_name_from_proto,
    workspace_to_proto,
};
use crate::workspaces::WorkspaceName;
use crate::workspaces::authorization::{WorkspaceAction, WorkspaceAuthorizer};

#[derive(Clone)]
pub(crate) struct FunctionService {
    queries: QueryManager,
    authorizer: WorkspaceAuthorizer,
}

impl FunctionService {
    pub(crate) const fn new(query_manager: QueryManager, authorizer: WorkspaceAuthorizer) -> Self {
        Self {
            queries: query_manager,
            authorizer,
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
        let authorizer = self.authorizer.clone();
        let request_context = request_context(&request)?.clone();
        Box::pin(instrument_grpc(span, async move {
            let inner = request.into_inner();
            let workspace_name = workspace_name_from_proto(inner.workspace.as_ref())?;
            // A function is installed for the whole workspace, so adding one
            // changes what every member can run. Settled before the SQL is
            // read: a caller who may not manage the workspace must not learn
            // from it whether their SQL compiles against its catalog.
            authorizer
                .authorize(
                    request_context.principal(),
                    &workspace_name,
                    WorkspaceAction::Manage,
                )
                .await
                .map_err(app_status)?;
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
        let authorizer = self.authorizer.clone();
        let request_context = request_context(&request)?.clone();
        instrument_grpc(span, async move {
            let inner = request.into_inner();
            let workspace_name = workspace_name_from_proto(inner.workspace.as_ref())?;
            // Listing what a workspace can run is reading its contents.
            authorizer
                .authorize(
                    request_context.principal(),
                    &workspace_name,
                    WorkspaceAction::Read,
                )
                .await
                .map_err(app_status)?;
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
        let authorizer = self.authorizer.clone();
        let request_context = request_context(&request)?.clone();
        instrument_grpc(span, async move {
            let inner = request.into_inner();
            let workspace_name = workspace_name_from_proto(inner.workspace.as_ref())?;
            // Settled before the name is parsed, so the workspace's function
            // inventory stays unreadable to a caller who may not manage it.
            authorizer
                .authorize(
                    request_context.principal(),
                    &workspace_name,
                    WorkspaceAction::Manage,
                )
                .await
                .map_err(app_status)?;
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
    use std::sync::Arc;

    use coral_engine::QueryRuntimeContext;
    use tempfile::TempDir;
    use tonic::Code;

    use super::*;
    use crate::identity::{Principal, PrincipalKind};
    use crate::request_context::RequestContext;
    use crate::state::ConfigStore;
    use crate::state::db::CoralDb;
    use crate::test_support::{create_workspace, migrated_deployment, seed_principal};
    use crate::workspaces::{MemberRole, WorkspaceName};

    /// This suite's login issuer. Each suite provisions under its own, so a
    /// subject seeded here is a different person from the same subject
    /// seeded elsewhere.
    const ISSUER: &str = "https://issuer.test/function-authorization";

    /// SQL no compiler accepts. Reaching function installation with it answers
    /// `InvalidArgument`, so a refusal that answers anything else proves the
    /// SQL was never looked at.
    const UNPARSEABLE_SQL: &str = "this is not sql";

    struct Fixture {
        _temp: TempDir,
        service: FunctionService,
        config_store: ConfigStore,
        db: Arc<CoralDb>,
    }

    /// The workspace these fixtures run in.
    ///
    /// An install provisions none, so [`fixture`] creates it explicitly. The
    /// name is ordinary on purpose: a fixture that leaned on a well-known one
    /// would prove the workspace was resolved by name rather than created.
    fn test_workspace() -> WorkspaceName {
        WorkspaceName::parse("work").expect("workspace name")
    }

    /// A shared deployment over one migrated database holding one created
    /// workspace, so every caller's authority comes from a membership row.
    async fn fixture() -> Fixture {
        let deployment = migrated_deployment().await;
        create_workspace(&deployment.db, &test_workspace()).await;
        let queries = QueryManager::new_for_tests(
            deployment.config_store.clone(),
            deployment.workspaces,
            deployment.credentials,
            QueryRuntimeContext::default(),
            deployment.layout,
            Vec::new(),
        );
        Fixture {
            _temp: deployment.temp,
            service: FunctionService::new(
                queries,
                WorkspaceAuthorizer::new(Arc::clone(&deployment.db)),
            ),
            config_store: deployment.config_store,
            db: deployment.db,
        }
    }

    fn request<T>(message: T, principal: &Principal) -> Request<T> {
        let mut request = Request::new(message);
        request
            .extensions_mut()
            .insert(RequestContext::new(principal.clone()));
        request
    }

    fn workspace() -> coral_api::v1::Workspace {
        crate::transport::workspace_to_proto(&test_workspace())
    }

    fn add_request() -> AddFunctionRequest {
        AddFunctionRequest {
            workspace: Some(workspace()),
            sql: UNPARSEABLE_SQL.to_string(),
            fail_if_exists: false,
            write_surface: ProtoFunctionWriteSurface::Cli as i32,
        }
    }

    fn delete_request() -> DeleteFunctionRequest {
        DeleteFunctionRequest {
            workspace: Some(workspace()),
            name: "not a function name".to_string(),
        }
    }

    fn list_request() -> ListFunctionsRequest {
        ListFunctionsRequest {
            workspace: Some(workspace()),
        }
    }

    /// Installing a function changes what every member of the workspace can
    /// run, so it is an owner's act while listing is a member's — and the
    /// owner's own agent credential is not promoted by their role, so a
    /// prompt-injected agent cannot publish SQL every member then runs.
    ///
    /// The unparseable SQL and the unparseable function name are what make
    /// each refusal an absence rather than an error code: the owner reaches
    /// the work and is told what is wrong with the request, and everyone else
    /// is refused before the request is looked at.
    #[tokio::test]
    async fn only_owners_change_the_function_set_and_never_through_an_agent() {
        let fixture = fixture().await;
        let owner = seed_principal(
            &fixture.db,
            ISSUER,
            &test_workspace(),
            "owner",
            Some(MemberRole::Owner),
        )
        .await;
        let member = seed_principal(
            &fixture.db,
            ISSUER,
            &test_workspace(),
            "member",
            Some(MemberRole::Member),
        )
        .await;
        let outsider =
            seed_principal(&fixture.db, ISSUER, &test_workspace(), "outsider", None).await;
        let agent = Principal::parse(owner.id().as_str(), PrincipalKind::Agent).expect("agent");

        for reader in [&member, &agent] {
            let listed = fixture
                .service
                .list_functions(request(list_request(), reader))
                .await
                .expect("a member, and a member's agent, read the function set")
                .into_inner();
            assert!(listed.functions.is_empty());
        }
        assert_eq!(
            fixture
                .service
                .list_functions(request(list_request(), &outsider))
                .await
                .expect_err("a non-member is told nothing about the workspace")
                .code(),
            Code::NotFound
        );

        // A member and an owner's agent are denied; a non-member is concealed.
        for (writer, expected) in [
            (&member, Code::PermissionDenied),
            (&agent, Code::PermissionDenied),
            (&outsider, Code::NotFound),
        ] {
            for status in [
                fixture
                    .service
                    .add_function(request(add_request(), writer))
                    .await
                    .expect_err("only an owner installs a function"),
                fixture
                    .service
                    .delete_function(request(delete_request(), writer))
                    .await
                    .expect_err("only an owner removes a function"),
            ] {
                assert_eq!(status.code(), expected, "{status:?}");
            }
        }

        assert_eq!(
            fixture
                .service
                .add_function(request(add_request(), &owner))
                .await
                .expect_err("the SQL is what the owner is stopped by")
                .code(),
            Code::InvalidArgument
        );
        assert!(
            fixture
                .config_store
                .list_workspace_functions(&test_workspace())
                .expect("list installed functions")
                .is_empty(),
            "no refused caller may have installed a function"
        );
    }

    #[test]
    fn invalid_function_listing_keeps_inventory_identity_and_error() {
        let workspace = test_workspace();
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
