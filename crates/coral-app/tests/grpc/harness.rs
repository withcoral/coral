use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use coral_api::v1::{
    AddWorkspaceMemberRequest, AddWorkspaceMemberResponse, CreateWorkspaceRequest,
    CreateWorkspaceResponse, ExecuteSqlRequest, ImportSourceRequest, ListCatalogRequest,
    ListSourcesRequest, ListWorkspacesRequest, PaginationRequest, RemoveWorkspaceMemberRequest,
    RemoveWorkspaceMemberResponse, Source, SourceSecret, SourceVariable, TableSummary,
    ValidateSourceRequest, ValidateSourceResponse, Workspace, WorkspaceRole, catalog_item,
    import_source_response,
};
use coral_app::features::{Feature, FeatureOverrides};
use coral_app::{EngineExtensionsProvider, PrincipalKind};
use coral_client::{
    AppClient, BearerToken, CatalogClient, FunctionClient, QueryClient, SearchClient, SourceClient,
    WorkspaceClient, batches_to_json_rows, decode_execute_sql_response, default_workspace,
    local::{RunningServer, ServerBuilder, connect_with_loopback_bearer},
};
use serde_json::{Value, json};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tempfile::TempDir;
use tonic::{Code, Request, Response, Status};
use tonic_types::{ErrorDetail, StatusExt as _};

use crate::session_auth::{SessionAuthFixture, session_authenticated_server};

pub(crate) struct GrpcHarness {
    temp_dir: TempDir,
    config_dir: PathBuf,
    local_trace_store_dir: Option<PathBuf>,
    app: AppClient,
    _server: RunningServer,
}

pub(crate) struct FailingHttpFixture {
    base_url: String,
    task: tokio::task::JoinHandle<()>,
}

impl GrpcHarness {
    pub(crate) async fn new() -> Self {
        let temp_dir = TempDir::new().expect("temp dir");
        let config_dir = temp_dir.path().join("coral-config");
        Self::start_with_parts(temp_dir, config_dir, FeatureOverrides::default()).await
    }

    pub(crate) async fn new_with_observed_values_search() -> Self {
        let temp_dir = TempDir::new().expect("temp dir");
        let config_dir = temp_dir.path().join("coral-config");
        let mut feature_overrides = FeatureOverrides::default();
        feature_overrides.set(Feature::ObservedValuesSearch, true);
        Self::start_with_parts(temp_dir, config_dir, feature_overrides).await
    }

    pub(crate) async fn start_with_config_dir(config_dir: PathBuf) -> Self {
        let temp_dir = TempDir::new().expect("temp dir");
        Self::start_with_parts(temp_dir, config_dir, FeatureOverrides::default()).await
    }

    pub(crate) async fn new_with_engine_extensions_provider(
        provider: Arc<dyn EngineExtensionsProvider>,
    ) -> Self {
        let temp_dir = TempDir::new().expect("temp dir");
        let config_dir = temp_dir.path().join("coral-config");
        Self::start_with_builder(
            temp_dir,
            config_dir,
            FeatureOverrides::default(),
            ServerBuilder::new().add_engine_extensions_provider(provider),
        )
        .await
    }

    async fn start_with_parts(
        temp_dir: TempDir,
        config_dir: PathBuf,
        feature_overrides: FeatureOverrides,
    ) -> Self {
        Self::start_with_builder(
            temp_dir,
            config_dir,
            feature_overrides,
            ServerBuilder::new(),
        )
        .await
    }

    async fn start_with_builder(
        temp_dir: TempDir,
        config_dir: PathBuf,
        feature_overrides: FeatureOverrides,
        server_builder: ServerBuilder,
    ) -> Self {
        ensure_file_credentials_config(&config_dir);
        let server = server_builder
            .with_config_dir(&config_dir)
            .with_feature_overrides(feature_overrides)
            .start()
            .await
            .expect("start server");
        let local_trace_store_dir = server.local_trace_store_dir().map(Path::to_path_buf);
        let app = AppClient::connect(server.endpoint_uri())
            .await
            .expect("connect client");
        Self {
            temp_dir,
            config_dir,
            local_trace_store_dir,
            app,
            _server: server,
        }
    }

    pub(crate) fn temp_path(&self) -> &Path {
        self.temp_dir.path()
    }

    pub(crate) fn config_dir(&self) -> &Path {
        &self.config_dir
    }

    pub(crate) fn local_trace_store_dir(&self) -> Option<&Path> {
        self.local_trace_store_dir.as_deref()
    }

    pub(crate) fn source_client(&self) -> SourceClient {
        self.app.source_client()
    }

    pub(crate) fn catalog_client(&self) -> CatalogClient {
        self.app.catalog_client()
    }

    pub(crate) fn query_client(&self) -> QueryClient {
        self.app.query_client()
    }

    pub(crate) fn function_client(&self) -> FunctionClient {
        self.app.function_client()
    }

    pub(crate) fn workspace_client(&self) -> WorkspaceClient {
        self.app.workspace_client()
    }

    pub(crate) fn search_client(&self) -> SearchClient {
        self.app.search_client()
    }

    pub(crate) async fn import_source(
        &self,
        manifest_yaml: String,
        variables: Vec<SourceVariable>,
        secrets: Vec<SourceSecret>,
    ) -> Source {
        let mut stream = self
            .source_client()
            .import_source(Request::new(ImportSourceRequest {
                workspace: Some(default_workspace()),
                manifest_yaml,
                variables,
                secrets,
                oauth_credential_retrievals: Vec::new(),
            }))
            .await
            .expect("import source")
            .into_inner();
        stream
            .message()
            .await
            .expect("import source stream")
            .and_then(|response| match response.event {
                Some(import_source_response::Event::Source(source)) => Some(source),
                _ => None,
            })
            .expect("import source response")
    }

    pub(crate) async fn list_sources(&self) -> Vec<Source> {
        self.source_client()
            .list_sources(Request::new(ListSourcesRequest {
                workspace: Some(default_workspace()),
            }))
            .await
            .expect("list sources")
            .into_inner()
            .sources
    }

    pub(crate) async fn list_tables(&self) -> Vec<TableSummary> {
        self.catalog_client()
            .list_catalog(Request::new(ListCatalogRequest {
                workspace: Some(default_workspace()),
                catalog_name: String::new(),
                schema_name: String::new(),
                kind: 1,
                pagination: Some(PaginationRequest {
                    limit: 0,
                    offset: 0,
                }),
            }))
            .await
            .expect("list catalog")
            .into_inner()
            .items
            .into_iter()
            .filter_map(|item| match item.item {
                Some(catalog_item::Item::Table(table)) => Some(table),
                Some(catalog_item::Item::TableFunction(_)) | None => None,
            })
            .collect()
    }

    pub(crate) async fn validate_source(&self, source_name: &str) -> ValidateSourceResponse {
        self.source_client()
            .validate_source(Request::new(ValidateSourceRequest {
                workspace: Some(default_workspace()),
                name: source_name.to_string(),
            }))
            .await
            .expect("validate source")
            .into_inner()
    }

    pub(crate) async fn execute_sql_rows(&self, sql: &str) -> Vec<Value> {
        let response = self
            .query_client()
            .execute_sql(Request::new(ExecuteSqlRequest {
                workspace: Some(default_workspace()),
                sql: sql.to_string(),
                guide_read_context: None,
                task_attribution: None,
            }))
            .await
            .expect("execute sql")
            .into_inner();
        batches_to_json_rows(
            decode_execute_sql_response(&response)
                .expect("decode query response")
                .batches(),
        )
        .expect("query rows")
    }
}

fn ensure_file_credentials_config(config_dir: &Path) {
    std::fs::create_dir_all(config_dir).expect("create config dir");
    let config_file = config_dir.join("config.toml");
    let raw = std::fs::read_to_string(&config_file).unwrap_or_default();
    if raw.contains("[credentials]") {
        return;
    }
    let separator = if raw.is_empty() || raw.ends_with('\n') {
        ""
    } else {
        "\n"
    };
    let updated = format!("{raw}{separator}\n[credentials]\nstorage = \"file\"\n");
    std::fs::write(config_file, updated).expect("write test credential config");
}

/// Upstream issuer written into every seeded directory row, so a response that
/// leaked one would be recognizable on the wire.
pub(crate) const TEST_ISSUER: &str = "https://issuer.test/authorization";

/// A running server that authenticates its callers, plus the directory rows
/// login provisioning would have written for them.
pub(crate) struct SharedDeployment {
    _temp: Option<TempDir>,
    config_dir: PathBuf,
    endpoint_uri: String,
    trace_store_dir: Option<PathBuf>,
    session_auth: SessionAuthFixture,
    _server: RunningServer,
}

/// What one workspace has on record from work its callers asked for.
///
/// A refused request must move none of these: a task row, a query recorded
/// under one, or a span attributed to the workspace would each be a side effect
/// of work the caller was never allowed to start.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct WorkspaceWork {
    pub(crate) tasks: i64,
    pub(crate) queries: i64,
    pub(crate) attributed_spans: usize,
}

impl SharedDeployment {
    pub(crate) async fn start() -> Self {
        let temp = TempDir::new().expect("temp dir");
        let config_dir = temp.path().join("coral-config");
        // Configuring session authentication is what makes a deployment a
        // shared one: it retires the implicit local owner, so each request over
        // this transport arrives as a distinct person or agent rather than as
        // the host. The fixture writes the credentials config too.
        let session_auth = SessionAuthFixture::write(&config_dir);
        let server = session_authenticated_server(&session_auth)
            .await
            .expect("start an authenticated server");
        let endpoint_uri = server.endpoint_uri().to_string();
        let trace_store_dir = server.local_trace_store_dir().map(Path::to_path_buf);
        // The local trace store is installed once per process, by whichever
        // server starts first, and the trace-history tests write into whatever
        // directory that turned out to be. When it is this one's, the temp dir
        // must outlive the deployment: removing it would delete the installed
        // store out from under a concurrently running test.
        let temp = if trace_store_dir
            .as_ref()
            .is_some_and(|dir| dir.starts_with(temp.path()))
        {
            let _installed_store_root: PathBuf = temp.keep();
            None
        } else {
            Some(temp)
        };
        Self {
            _temp: temp,
            config_dir,
            endpoint_uri,
            trace_store_dir,
            session_auth,
            _server: server,
        }
    }

    /// Writes one directory row the way a completed login would, and returns
    /// the internal user id every credential for that person then carries.
    ///
    /// The login flow itself is upstream of this contract, so it is the row —
    /// not the OIDC round trip — that these transport tests need. Authorization
    /// never sees the upstream subject: it is stored here and nowhere else.
    pub(crate) async fn seed_user(&self, handle: &str, display_name: &str) -> String {
        let user_id = format!("user-{handle}");
        let pool = self.app_database().await;
        sqlx::query(
            "INSERT INTO users (user_id, issuer, subject, display_name, created_at_unix_nanos, last_login_at_unix_nanos) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(&user_id)
        .bind(TEST_ISSUER)
        .bind(format!("upstream-subject-{handle}"))
        .bind(display_name)
        .bind(1_i64)
        .bind(1_i64)
        .execute(&pool)
        .await
        .expect("seed a provisioned login");
        pool.close().await;
        user_id
    }

    pub(crate) async fn as_person(&self, user_id: &str) -> AppClient {
        self.connect(user_id, PrincipalKind::User).await
    }

    /// Connects an agent, which is a principal in its own right.
    ///
    /// It carries `agent_id` rather than any person's id, because the two are
    /// drawn from one namespace and never coincide. An agent therefore holds
    /// only the memberships granted to `agent_id` itself, which is none until
    /// something grants them: acting for a person is not something the identity
    /// or the actor kind can express.
    pub(crate) async fn as_agent(&self, agent_id: &str) -> AppClient {
        self.connect(agent_id, PrincipalKind::Agent).await
    }

    /// The address to dial for a service `AppClient` does not carry, such as
    /// `TraceService` or `FeatureService`.
    pub(crate) fn endpoint_uri(&self) -> &str {
        &self.endpoint_uri
    }

    /// The store this deployment answers trace requests out of, for tests that
    /// need a row on record that no request would produce.
    pub(crate) fn trace_store_dir(&self) -> Option<&Path> {
        self.trace_store_dir.as_deref()
    }

    /// The bearer credential an actor of `principal_kind` presents for
    /// `user_id`, for tests that dial a service `AppClient` does not carry.
    pub(crate) fn credential(&self, user_id: &str, principal_kind: PrincipalKind) -> String {
        self.session_auth.access_token_for(user_id, principal_kind)
    }

    async fn connect(&self, user_id: &str, principal_kind: PrincipalKind) -> AppClient {
        connect_with_loopback_bearer(
            self.endpoint_uri(),
            BearerToken::new(self.session_auth.access_token_for(user_id, principal_kind))
                .expect("test bearer token"),
        )
        .await
        .expect("connect a test client")
    }

    /// Reads what `workspace_name` has on record, straight from the state the
    /// server keeps rather than from an RPC the caller under test could be
    /// refused.
    pub(crate) async fn workspace_work(&self, workspace_name: &str) -> WorkspaceWork {
        let pool = self.app_database().await;
        let tasks =
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM tasks WHERE workspace_id = ?")
                .bind(workspace_name)
                .fetch_one(&pool)
                .await
                .expect("count workspace tasks");
        let queries = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM task_queries JOIN tasks ON tasks.id = task_queries.task_id WHERE tasks.workspace_id = ?",
        )
        .bind(workspace_name)
        .fetch_one(&pool)
        .await
        .expect("count workspace queries");
        pool.close().await;
        WorkspaceWork {
            tasks,
            queries,
            attributed_spans: self.attributed_spans(workspace_name),
        }
    }

    /// Counts exported spans carrying this workspace's attribution. Every
    /// deployment in the process shares one store, so the workspace name is
    /// what separates one test's spans from another's.
    fn attributed_spans(&self, workspace_name: &str) -> usize {
        let Some(dir) = self.trace_store_dir() else {
            return 0;
        };
        fs::read_dir(dir)
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "jsonl")
            })
            .filter_map(|path| fs::read_to_string(path).ok())
            .map(|raw| {
                raw.lines()
                    .filter(|line| span_names_workspace(line, workspace_name))
                    .count()
            })
            .sum()
    }

    async fn app_database(&self) -> sqlx::SqlitePool {
        SqlitePoolOptions::new()
            .connect_with(SqliteConnectOptions::new().filename(self.config_dir.join("coral.db")))
            .await
            .expect("open the app database")
    }
}

fn span_names_workspace(line: &str, workspace_name: &str) -> bool {
    serde_json::from_str::<Value>(line)
        .ok()
        .and_then(|span| Some(span.get("attributes_json")?.as_str()?.to_string()))
        .and_then(|attributes| serde_json::from_str::<Value>(&attributes).ok())
        .and_then(|attributes| Some(attributes.get("workspace")?.as_str()?.to_string()))
        .is_some_and(|workspace| workspace == workspace_name)
}

pub(crate) fn named_workspace(name: &str) -> Workspace {
    Workspace {
        name: name.to_string(),
    }
}

/// The workspace and membership helpers hand back the whole response rather
/// than the one part a given caller asserts on: what the server said about a
/// change is as much of the contract as whether it allowed it.
pub(crate) async fn create_workspace(
    client: &AppClient,
    name: &str,
) -> Result<CreateWorkspaceResponse, Status> {
    client
        .workspace_client()
        .create_workspace(Request::new(CreateWorkspaceRequest {
            workspace: Some(named_workspace(name)),
        }))
        .await
        .map(Response::into_inner)
}

pub(crate) async fn add_member(
    client: &AppClient,
    name: &str,
    user_id: &str,
    role: WorkspaceRole,
) -> Result<AddWorkspaceMemberResponse, Status> {
    client
        .workspace_client()
        .add_workspace_member(Request::new(AddWorkspaceMemberRequest {
            workspace: Some(named_workspace(name)),
            user_id: user_id.to_string(),
            role: role.into(),
        }))
        .await
        .map(Response::into_inner)
}

pub(crate) async fn remove_member(
    client: &AppClient,
    name: &str,
    user_id: &str,
) -> Result<RemoveWorkspaceMemberResponse, Status> {
    client
        .workspace_client()
        .remove_workspace_member(Request::new(RemoveWorkspaceMemberRequest {
            workspace: Some(named_workspace(name)),
            user_id: user_id.to_string(),
        }))
        .await
        .map(Response::into_inner)
}

/// Reads a listing the way a client does: workspace name beside the caller's
/// own role, with no second request needed to learn it. A caller is always
/// answered about their own memberships, so this asks for the rows rather than
/// for a result.
pub(crate) async fn membership_rows(client: &AppClient) -> Vec<(String, WorkspaceRole)> {
    client
        .workspace_client()
        .list_workspaces(Request::new(ListWorkspacesRequest {}))
        .await
        .expect("a caller is always answered about their own memberships")
        .into_inner()
        .memberships
        .into_iter()
        .map(|membership| {
            (
                membership.workspace.expect("listed workspace").name,
                membership.role.try_into().expect("listed role"),
            )
        })
        .collect()
}

/// Reports only what a refused caller is told: the code, the message with the
/// workspace name they supplied themselves factored out, and the structured
/// reasons. Two refusals that agree here are indistinguishable to that caller,
/// which is what separates a concealed workspace from a denial confirming one.
/// Reduces a refusal to what it says about a workspace, with the workspace's
/// own name removed so two refusals can be compared.
///
/// Only the quoted occurrence is replaced. A bare substring replacement also
/// rewrites the name where it sits inside another identifier, which can make
/// two genuinely different refusals compare equal — and this comparison is the
/// whole assertion, so a false match would report concealment that is not
/// there.
pub(crate) fn concealed_refusal(status: &Status, name: &str) -> (Code, String, Vec<String>) {
    (
        status.code(),
        status
            .message()
            .replace(&format!("'{name}'"), "'<workspace>'"),
        status
            .get_error_details_vec()
            .iter()
            .filter_map(|detail| match detail {
                ErrorDetail::ErrorInfo(info) => Some(info.reason.clone()),
                _ => None,
            })
            .collect(),
    )
}

/// Runs one statement as `client`, discarding the rows: these tests ask whether
/// the read was allowed, not what it returned.
pub(crate) async fn execute_sql(client: &AppClient, name: &str, sql: &str) -> Result<(), Status> {
    client
        .query_client()
        .execute_sql(Request::new(ExecuteSqlRequest {
            workspace: Some(named_workspace(name)),
            sql: sql.to_string(),
            guide_read_context: None,
            task_attribution: None,
        }))
        .await
        .map(|_| ())
}

impl FailingHttpFixture {
    pub(crate) async fn new() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind failing http fixture");
        let addr = listener.local_addr().expect("fixture local addr");
        let task = tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.expect("accept fixture connection");
                drop(socket);
            }
        });

        Self {
            base_url: format!("http://{addr}"),
            task,
        }
    }

    pub(crate) fn manifest_yaml(&self) -> String {
        self.manifest_yaml_with_test_queries(&[])
    }

    pub(crate) fn manifest_yaml_with_test_queries(&self, test_queries: &[&str]) -> String {
        manifest_yaml(&json!({
            "name": "unreachable_messages",
            "version": "0.1.0",
            "dsl_version": 3,
            "backend": "http",
            "base_url": self.base_url,
            "test_queries": test_queries,
            "tables": [{
                "name": "messages",
                "description": "Unreachable messages",
                "request": {
                    "method": "GET",
                    "path": "/messages",
                },
                "response": {},
                "columns": [
                    {"name": "id", "type": "Utf8"},
                ],
            }],
        }))
    }
}

impl Drop for FailingHttpFixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub(crate) fn fixture_manifest_yaml(root: &Path) -> String {
    fixture_manifest_with_test_queries_yaml(root, &[])
}

pub(crate) fn fixture_manifest_with_multiple_tables_yaml(root: &Path) -> String {
    let data_dir = root.join("fixture-data");
    fs::create_dir_all(&data_dir).expect("create data dir");
    fs::write(
        data_dir.join("messages.jsonl"),
        r#"{"type":"user","sessionId":"s1","text":"hello"}
{"type":"assistant","sessionId":"s1","text":"world"}
"#,
    )
    .expect("write jsonl");
    let table_source = json!({
        "location": format!("file://{}/", data_dir.display()),
        "glob": "**/*.jsonl",
    });
    let table_columns = json!([
        {"name": "type", "type": "Utf8"},
        {"name": "sessionId", "type": "Utf8"},
        {"name": "text", "type": "Utf8"},
    ]);
    manifest_yaml(&json!({
        "name": "local_messages",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [
            {
                "name": "events",
                "description": "Fixture events",
                "format": "jsonl",
                "source": table_source.clone(),
                "columns": table_columns.clone(),
            },
            {
                "name": "messages",
                "description": "Fixture messages",
                "format": "jsonl",
                "source": table_source.clone(),
                "columns": table_columns.clone(),
            },
            {
                "name": "sessions",
                "description": "Fixture sessions",
                "format": "jsonl",
                "source": table_source,
                "columns": table_columns,
            },
        ],
    }))
}

pub(crate) fn fixture_manifest_with_required_filter_yaml() -> String {
    manifest_yaml(&json!({
        "name": "filtered_messages",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": "https://example.com",
        "tables": [{
            "name": "messages",
            "description": "Filtered messages",
            "request": {
                "method": "GET",
                "path": "/messages",
                "query": [
                    { "name": "channel", "from": "filter", "key": "channel" }
                ],
            },
            "response": {},
            "columns": [
                {"name": "channel", "type": "Utf8"},
                {"name": "text", "type": "Utf8"},
            ],
            "filters": [
                { "name": "channel", "required": true }
            ],
        }],
    }))
}

pub(crate) fn fixture_manifest_with_functions_yaml() -> String {
    manifest_yaml(&json!({
        "name": "searchy",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": "https://example.com",
        "tables": [{
            "name": "placeholder",
            "description": "Placeholder table",
            "request": {
                "method": "GET",
                "path": "/placeholder",
            },
            "columns": [
                { "name": "id", "type": "Utf8" },
            ],
        }],
        "functions": [
            {
                "name": "lookup_issue",
                "description": "Lookup issue",
                "guide": "Use this function for exact issue lookup.",
                "args": [
                    {
                        "name": "number",
                        "required": true,
                        "bind": { "arg": "number" },
                    },
                ],
                "request": {
                    "method": "GET",
                    "path": "/issues/{{arg.number}}",
                },
                "response": {},
                "columns": [
                    { "name": "title", "type": "Utf8", "description": "Issue title" },
                ],
            },
            {
                "name": "search_issues",
                "description": "Search issues",
                "args": [
                    {
                        "name": "q",
                        "required": true,
                        "bind": { "arg": "q" },
                    },
                    {
                        "name": "mode",
                        "values": ["lexical", "semantic", "hybrid"],
                        "bind": { "arg": "search_type" },
                    },
                ],
                "request": {
                    "method": "GET",
                    "path": "/search/issues",
                    "query": [
                        { "name": "q", "from": "arg", "key": "q" },
                        { "name": "search_type", "from": "arg", "key": "search_type" },
                    ],
                },
                "response": {
                    "rows_path": ["items"],
                },
                "columns": [
                    { "name": "title", "type": "Utf8", "description": "Issue title" },
                    { "name": "score", "type": "Float64" },
                ],
            },
        ],
    }))
}

pub(crate) fn fixture_function_only_manifest_yaml() -> String {
    manifest_yaml(&json!({
        "name": "searchy",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": "https://example.com",
        "functions": [{
            "name": "search_issues",
            "description": "Search issues",
            "args": [{
                "name": "q",
                "required": true,
                "bind": { "arg": "q" },
            }],
            "request": {
                "method": "GET",
                "path": "/search/issues",
                "query": [
                    { "name": "q", "from": "arg", "key": "q" },
                ],
            },
            "response": {
                "rows_path": ["items"],
            },
            "columns": [
                { "name": "title", "type": "Utf8", "description": "Issue title" },
            ],
        }],
    }))
}

pub(crate) fn fixture_manifest_with_test_queries_yaml(
    root: &Path,
    test_queries: &[&str],
) -> String {
    let data_dir = root.join("fixture-data");
    fs::create_dir_all(&data_dir).expect("create data dir");
    fs::write(
        data_dir.join("messages.jsonl"),
        r#"{"type":"user","sessionId":"s1","text":"hello"}
{"type":"assistant","sessionId":"s1","text":"world"}
"#,
    )
    .expect("write jsonl");
    manifest_yaml(&json!({
        "name": "local_messages",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "test_queries": test_queries,
        "tables": [{
            "name": "messages",
            "description": "Fixture messages",
            "format": "jsonl",
            "source": {
                "location": format!("file://{}/", data_dir.display()),
                "glob": "**/*.jsonl",
            },
            "columns": [
                {"name": "type", "type": "Utf8"},
                {"name": "sessionId", "type": "Utf8"},
                {"name": "text", "type": "Utf8"},
            ],
        }],
    }))
}

pub(crate) fn fixture_manifest_with_inputs_yaml() -> String {
    manifest_yaml(&json!({
        "name": "secured_messages",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "inputs": {
            "API_BASE": { "kind": "variable", "default": "https://example.com" },
            "API_TOKEN": { "kind": "secret" },
        },
        "base_url": "{{input.API_BASE}}",
        "auth": {
            "type": "HeaderAuth",
            "headers": [{
                "name": "Authorization",
                "from": "template",
                "template": "Bearer {{input.API_TOKEN}}",
            }],
        },
        "tables": [{
            "name": "messages",
            "description": "Secured messages",
            "request": {
                "method": "GET",
                "path": "/messages",
            },
            "response": {},
            "columns": [
                {"name": "id", "type": "Utf8"},
            ],
        }],
    }))
}

pub(crate) fn fixture_manifest_with_required_inputs_yaml() -> String {
    manifest_yaml(&json!({
        "name": "required_messages",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "inputs": {
            "API_BASE": { "kind": "variable" },
            "API_TOKEN": { "kind": "secret" },
        },
        "base_url": "{{input.API_BASE}}",
        "auth": {
            "type": "HeaderAuth",
            "headers": [{
                "name": "Authorization",
                "from": "template",
                "template": "Bearer {{input.API_TOKEN}}",
            }],
        },
        "tables": [{
            "name": "messages",
            "description": "Required-input messages",
            "request": {
                "method": "GET",
                "path": "/messages",
            },
            "response": {},
            "columns": [
                {"name": "id", "type": "Utf8"},
            ],
        }],
    }))
}

pub(crate) fn invalid_manifest_yaml() -> String {
    manifest_yaml(&json!({
        "name": "demo",
        "schema": "demo",
        "version": "1.0.0",
        "dsl_version": 3,
        "backend": "http",
        "tables": [{
            "name": "messages",
            "description": "Demo messages",
            "request": {
                "method": "GET",
                "path": "/messages",
            },
            "response": {},
            "columns": [
                {"name": "id", "type": "Utf8"},
            ],
        }],
    }))
}

pub(crate) fn manifest_yaml(value: &Value) -> String {
    serde_yaml::to_string(value).expect("serialize manifest yaml")
}

pub(crate) fn source_dir(config_dir: &Path, source_name: &str) -> PathBuf {
    config_dir
        .join("workspaces")
        .join("default")
        .join("sources")
        .join(source_name)
}
