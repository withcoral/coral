use std::sync::Arc;

use coral_api::v1::catalog_service_server::CatalogService as CatalogServiceApi;
use coral_api::v1::feedback_service_server::FeedbackService as FeedbackServiceApi;
use coral_api::v1::function_service_server::FunctionService as FunctionServiceApi;
use coral_api::v1::query_service_server::QueryService as QueryServiceApi;
use coral_api::v1::search_service_server::SearchService as SearchServiceApi;
use coral_api::v1::task_service_server::TaskService as TaskServiceApi;
use coral_api::v1::{
    AddFunctionRequest, ClearSearchDataRequest, DeleteFunctionRequest,
    DescribeCatalogSurfaceRequest, DrainSearchQueueRequest, EndTaskRequest, ExecuteSqlRequest,
    ExplainSqlRequest, ListCatalogRequest, ListColumnsRequest, ListFunctionsRequest,
    RebuildSearchIndexRequest, SearchCatalogRequest, SearchRequest, StartTaskRequest,
    SubmitFeedbackRequest, TaskAttribution, Workspace,
};
use coral_engine::QueryRuntimeContext;
use tempfile::TempDir;
use tonic::{Code, Request, Response, Status};

use crate::catalog::discovery::CatalogDiscovery;
use crate::catalog::service::CatalogService;
use crate::credentials::{CredentialManager, CredentialStore};
use crate::feedback::manager::FeedbackManager;
use crate::feedback::service::FeedbackService;
use crate::functions::service::FunctionService;
use crate::identity::{Principal, PrincipalKind};
use crate::query::extensions::NoopEngineExtensionsProvider;
use crate::query::manager::QueryManager;
use crate::query::service::QueryService;
use crate::request_context::RequestContext;
use crate::search::manager::SearchManager;
use crate::search::service::SearchService;
use crate::state::db::{
    AddMemberOutcome, CoralDb, DatabaseConfig, DbRepos, ResolvedDatabaseConfig, UpsertLoginOutcome,
};
use crate::state::{AppStateLayout, ConfigStore};
use crate::task::manager::TaskManager;
use crate::task::service::TaskService;
use crate::task::store::TaskStore;
use crate::workspaces::{MemberRole, WorkspaceAuthorizer, WorkspaceManager, WorkspaceName};

macro_rules! rpc_matrix {
    ($fixture:ident, $assertion:ident, $left:ident, $right:ident;
        $($service:ident.$method:ident($request:ident $(, $field:ident: $value:expr)*) => $expected:expr;)+
    ) => {$({
        let request = $request {
            workspace: Some($fixture.workspace.clone()),
            $($field: $value,)*
            ..Default::default()
        };
        $assertion(
            $fixture.$service.$method(authenticated(request.clone(), &$fixture.$left)).await,
            $fixture.$service.$method(authenticated(request, &$fixture.$right)).await,
            $expected,
        );
    })+};
}

#[tokio::test]
#[expect(clippy::needless_update, reason = "uniform request matrix")]
async fn read_rpcs_authorize_before_handler_work() {
    let fixture = service_authorization_fixture().await;
    rpc_matrix!(
        fixture, assert_read, member, nonmember;
        catalog.list_catalog(ListCatalogRequest, kind: 999) => Some(Code::InvalidArgument);
        catalog.search_catalog(SearchCatalogRequest) => Some(Code::InvalidArgument);
        catalog.describe_catalog_surface(DescribeCatalogSurfaceRequest) => Some(Code::InvalidArgument);
        catalog.list_columns(ListColumnsRequest) => Some(Code::InvalidArgument);
        query.execute_sql(
            ExecuteSqlRequest,
            sql: "SELECT 1".to_string(),
            guide_read_context: None,
            task_attribution: Some(TaskAttribution {
                task_id: "invalid".to_string(),
                intent: "invalid attribution".to_string(),
            })
        ) => Some(Code::InvalidArgument);
        query.explain_sql(ExplainSqlRequest, sql: "invalid-sql".to_string()) => Some(Code::InvalidArgument);
        functions.list_functions(ListFunctionsRequest) => None;
        search.search(SearchRequest) => Some(Code::InvalidArgument);
        task.start_task(StartTaskRequest) => Some(Code::InvalidArgument);
        task.end_task(EndTaskRequest, task_id: "invalid".to_string()) => Some(Code::InvalidArgument);
        feedback.submit_feedback(SubmitFeedbackRequest) => Some(Code::InvalidArgument);
    );
}

#[tokio::test]
#[expect(clippy::needless_update, reason = "uniform request matrix")]
async fn manage_rpcs_authorize_before_handler_work() {
    let fixture = service_authorization_fixture().await;
    rpc_matrix!(
        fixture, assert_manage, member, owner;
        functions.add_function(AddFunctionRequest) => Some(Code::InvalidArgument);
        functions.delete_function(DeleteFunctionRequest) => Some(Code::InvalidArgument);
        search.rebuild_search_index(RebuildSearchIndexRequest, provider: 999) => Some(Code::InvalidArgument);
        search.drain_search_queue(DrainSearchQueueRequest, budget_ms: 60_001) => Some(Code::InvalidArgument);
        search.clear_search_data(ClearSearchDataRequest) => Some(Code::InvalidArgument);
    );
}

struct ServiceAuthorizationFixture {
    _temp: TempDir,
    catalog: CatalogService,
    feedback: FeedbackService,
    functions: FunctionService,
    query: QueryService,
    search: SearchService,
    task: TaskService,
    workspace: Workspace,
    member: Principal,
    nonmember: Principal,
    owner: Principal,
}

async fn service_authorization_fixture() -> ServiceAuthorizationFixture {
    let temp = TempDir::new().expect("temp dir");
    let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
    layout.ensure().expect("layout dirs");
    let config_store = ConfigStore::new(layout.clone());
    let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(&layout).expect("config") else {
        panic!("default test database must be SQLite")
    };
    let db = Arc::new(
        CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
            .await
            .expect("open SQLite"),
    );
    db.migrate().await.expect("migrate SQLite");
    let owner_id = directory_user(&db, "owner").await;
    let member_id = directory_user(&db, "member").await;
    let workspace = WorkspaceName::parse("service-team").expect("workspace");
    let mut session = db.as_ref();
    session
        .workspaces()
        .create_with_owner(workspace.as_str(), &owner_id, 1)
        .await
        .expect("create workspace");
    assert!(matches!(
        session
            .workspaces()
            .add_member(workspace.as_str(), &member_id, MemberRole::Member, 2)
            .await
            .expect("add member"),
        AddMemberOutcome::Added(_)
    ));
    config_store
        .create_legacy_workspace_entry_for_tests(&workspace)
        .expect("create workspace configuration");
    let credentials = CredentialManager::new(CredentialStore::new(layout.clone()));
    let workspaces = WorkspaceManager::new_for_tests(
        config_store.clone(),
        credentials.clone(),
        layout.clone(),
        None,
        Arc::clone(&db),
    );
    let queries = QueryManager::new_for_tests(
        config_store.clone(),
        workspaces.clone(),
        credentials,
        QueryRuntimeContext::default(),
        layout.clone(),
        vec![Arc::new(NoopEngineExtensionsProvider)],
    );
    let tasks = TaskManager::new(TaskStore::new(Arc::clone(&db)));
    let authorizer = WorkspaceAuthorizer::new(db);
    let search = SearchManager::new(
        layout.clone(),
        &config_store,
        workspaces.clone(),
        true,
        CatalogDiscovery::new(queries.clone()),
        workspaces.lifecycle_lock(),
    );
    ServiceAuthorizationFixture {
        _temp: temp,
        catalog: CatalogService::new(queries.clone(), tasks.clone(), authorizer.clone()),
        feedback: FeedbackService::new(
            FeedbackManager::new(layout),
            tasks.clone(),
            authorizer.clone(),
        ),
        functions: FunctionService::new(queries.clone(), authorizer.clone()),
        query: QueryService::new(queries, tasks.clone(), authorizer.clone()),
        search: SearchService::new(search, tasks.clone(), authorizer.clone()),
        task: TaskService::new(tasks, authorizer),
        workspace: Workspace {
            name: workspace.to_string(),
        },
        member: Principal::parse(&member_id, PrincipalKind::User).expect("member"),
        nonmember: Principal::parse("nonmember", PrincipalKind::User).expect("nonmember"),
        owner: Principal::parse(&owner_id, PrincipalKind::User).expect("owner"),
    }
}

async fn directory_user(db: &CoralDb, subject: &str) -> String {
    let mut session = db;
    let UpsertLoginOutcome::Upserted(user) = session
        .users()
        .upsert_login("issuer", subject, None, 1)
        .await
        .expect("create directory user")
    else {
        panic!("new subject must create a user")
    };
    user.user_id
}

fn authenticated<T>(message: T, principal: &Principal) -> Request<T> {
    let mut request = Request::new(message);
    request
        .extensions_mut()
        .insert(RequestContext::new(principal.clone()));
    request
}

fn assert_read<T>(
    member: Result<Response<T>, Status>,
    nonmember: Result<Response<T>, Status>,
    expected: Option<Code>,
) {
    assert_result(member, expected);
    assert_eq!(result_error(nonmember).code(), Code::NotFound);
}

fn assert_manage<T>(
    member: Result<Response<T>, Status>,
    owner: Result<Response<T>, Status>,
    expected: Option<Code>,
) {
    assert_eq!(result_error(member).code(), Code::PermissionDenied);
    assert_result(owner, expected);
}

fn assert_result<T>(result: Result<Response<T>, Status>, expected: Option<Code>) {
    match expected {
        Some(code) => assert_eq!(result_error(result).code(), code),
        None => match result {
            Ok(_response) => {}
            Err(status) => panic!("authorized request failed: {status}"),
        },
    }
}

fn result_error<T>(result: Result<Response<T>, Status>) -> Status {
    match result {
        Ok(_response) => panic!("request must fail"),
        Err(status) => status,
    }
}
