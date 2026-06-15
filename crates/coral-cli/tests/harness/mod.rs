#![allow(
    dead_code,
    reason = "Integration test crates share this harness, but each target only uses a subset of the helpers."
)]

use std::collections::VecDeque;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use arrow::array::Int64Array;
use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use assert_cmd::Command;
use coral_api::v1::catalog_service_server::{CatalogService, CatalogServiceServer};
use coral_api::v1::identity_service_server::{IdentityService, IdentityServiceServer};
use coral_api::v1::identity_spec_service_server::{IdentitySpecService, IdentitySpecServiceServer};
use coral_api::v1::query_service_server::{QueryService, QueryServiceServer};
use coral_api::v1::source_service_server::{SourceService, SourceServiceServer};
use coral_api::v1::{
    AddIdentitySpecRequest, AddIdentitySpecResponse, CatalogCounts, CatalogItem,
    CatalogSearchResult, Column, ColumnSearchResult, CreateBundledSourceRequest,
    CreateBundledSourceResponse, CreateBundledSourceWithOAuthRequest,
    CreateBundledSourceWithOAuthResponse, CreateIdentitySpecRequest, CreateIdentitySpecResponse,
    CreateUserOwnedIdentityWithFixedTokenRequest, CreateUserOwnedIdentityWithFixedTokenResponse,
    CreateUserOwnedIdentityWithOAuthRequest, CreateUserOwnedIdentityWithOAuthResponse,
    DeleteIdentitySpecRequest, DeleteIdentitySpecResponse, DeleteSourceRequest,
    DeleteSourceResponse, DeleteUserOwnedIdentityRequest, DeleteUserOwnedIdentityResponse,
    DescribeTableRequest, DescribeTableResponse, DiscoverSourcesRequest, DiscoverSourcesResponse,
    ExecuteSqlRequest, ExecuteSqlResponse, ExplainSqlRequest, ExplainSqlResponse,
    GetIdentitySpecRequest, GetIdentitySpecResponse, GetSourceInfoRequest, GetSourceInfoResponse,
    GetSourceRequest, GetSourceResponse, Identity, IdentityOwner, IdentitySpec,
    ImportSourceRequest, ImportSourceResponse, ListCatalogRequest, ListCatalogResponse,
    ListColumnsRequest, ListColumnsResponse, ListIdentitySpecsRequest, ListIdentitySpecsResponse,
    ListSourcesRequest, ListSourcesResponse, ListUserOwnedIdentitiesRequest,
    ListUserOwnedIdentitiesResponse, PaginationRequest, PaginationResponse, QueryPlan,
    SearchCatalogRequest, SearchCatalogResponse, Source, SourceCredentialStorage, SourceInfo,
    SourceInputSpec, SourceOrigin, SourceSecretInput, Table, TableSummary, ValidateSourceRequest,
    ValidateSourceResponse, Workspace, catalog_item, create_bundled_source_with_o_auth_response,
    create_user_owned_identity_with_o_auth_response, import_source_response,
    source_input_spec::Input as ProtoSourceInput,
};
use coral_api::{CORAL_ERROR_DOMAIN, CORAL_ERROR_REASON_SOURCE_NOT_FOUND};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::Stream;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};
use tonic_types::{ErrorDetail, StatusExt as _};

fn workspace() -> Workspace {
    Workspace {
        name: "default".to_string(),
    }
}

fn mock_source() -> Source {
    Source {
        workspace: Some(workspace()),
        name: "github".to_string(),
        version: "1.0.0".to_string(),
        secrets: Vec::new(),
        variables: Vec::new(),
        origin: SourceOrigin::Bundled as i32,
        credential_storage: SourceCredentialStorage::File as i32,
    }
}

fn mock_identity_spec() -> IdentitySpec {
    IdentitySpec {
        name: "github_oauth".to_string(),
        version: "0.1.0".to_string(),
        description: "GitHub OAuth identity.".to_string(),
        issuer: "github".to_string(),
        identity_type: "oauth".to_string(),
        manifest_yaml: r"
kind: identity
spec_version: 1
name: github_oauth
version: 0.1.0
issuer: github
type: oauth
oauth:
  method:
    flow:
      type: device_code
    endpoints:
      device_authorization_url: https://github.com/login/device/code
      token_url: https://github.com/login/oauth/access_token
    client:
      id:
        input: GITHUB_OAUTH_CLIENT_ID
"
        .to_string(),
    }
}

fn mock_identity() -> Identity {
    Identity {
        name: "github_local".to_string(),
        identity_spec: "github_oauth".to_string(),
        issuer: "github".to_string(),
        identity_type: "oauth".to_string(),
        owner: IdentityOwner::User as i32,
        metadata: Vec::new(),
    }
}

fn mock_table(schema_name: &str, name: &str) -> Table {
    Table {
        workspace: Some(workspace()),
        schema_name: schema_name.to_string(),
        name: name.to_string(),
        description: String::new(),
        guide: String::new(),
        columns: Vec::new(),
        required_filters: Vec::new(),
    }
}

fn mock_visible_table() -> Table {
    Table {
        workspace: Some(workspace()),
        schema_name: "local_messages".to_string(),
        name: "messages".to_string(),
        description: "Fixture messages".to_string(),
        guide: "Query fixture messages.".to_string(),
        columns: vec![
            Column {
                name: "owner".to_string(),
                data_type: "Utf8".to_string(),
                nullable: false,
                is_virtual: true,
                is_required_filter: true,
                description: "Repository owner filter".to_string(),
                ordinal_position: 0,
            },
            Column {
                name: "repo".to_string(),
                data_type: "Utf8".to_string(),
                nullable: false,
                is_virtual: true,
                is_required_filter: true,
                description: "Repository name filter".to_string(),
                ordinal_position: 1,
            },
            Column {
                name: "text".to_string(),
                data_type: "Utf8".to_string(),
                nullable: false,
                is_virtual: false,
                is_required_filter: false,
                description: "Message text".to_string(),
                ordinal_position: 2,
            },
        ],
        required_filters: vec!["owner".to_string(), "repo".to_string()],
    }
}

fn mock_visible_tables() -> Vec<Table> {
    let messages = mock_visible_table();
    let mut sessions = mock_visible_table();
    sessions.name = "sessions".to_string();
    sessions.description = "Fixture sessions".to_string();
    sessions.guide = "Query fixture sessions.".to_string();
    let mut events = mock_visible_table();
    events.name = "events".to_string();
    events.description = "Fixture events".to_string();
    events.guide = "Query fixture events.".to_string();
    vec![events, messages, sessions]
}

fn table_summary(table: &Table) -> TableSummary {
    TableSummary {
        workspace: table.workspace.clone(),
        schema_name: table.schema_name.clone(),
        name: table.name.clone(),
        description: table.description.clone(),
        required_filters: table.required_filters.clone(),
        guide: table.guide.clone(),
    }
}

fn paginate<T>(items: Vec<T>, pagination: PaginationRequest) -> (Vec<T>, PaginationResponse) {
    let total = u32::try_from(items.len()).unwrap_or(u32::MAX);
    let offset = usize::try_from(pagination.offset).expect("offset");
    let limit = usize::try_from(pagination.limit).expect("limit");
    let items = if pagination.limit == 0 {
        items.into_iter().skip(offset).collect::<Vec<_>>()
    } else {
        items
            .into_iter()
            .skip(offset)
            .take(limit)
            .collect::<Vec<_>>()
    };
    let returned_count = u32::try_from(items.len()).unwrap_or(u32::MAX);
    let has_more =
        pagination.limit != 0 && pagination.offset.saturating_add(returned_count) < total;
    let next_offset = if has_more {
        pagination.offset.saturating_add(returned_count)
    } else {
        0
    };
    (
        items,
        PaginationResponse {
            total_count: total,
            limit: pagination.limit,
            offset: pagination.offset,
            has_more,
            next_offset,
        },
    )
}

fn table_matched_fields(table: &Table, regex: &regex::Regex) -> Vec<String> {
    let name = format!("{}.{}", table.schema_name, table.name);
    let candidates = [
        ("schema_name", table.schema_name.as_str()),
        ("table_name", table.name.as_str()),
        ("name", name.as_str()),
        ("description", table.description.as_str()),
        ("guide", table.guide.as_str()),
    ];
    let mut matches = candidates
        .into_iter()
        .filter_map(|(field, value)| regex.is_match(value).then_some(field.to_string()))
        .collect::<Vec<_>>();
    if table
        .required_filters
        .iter()
        .any(|filter| regex.is_match(filter))
    {
        matches.push("required_filters".to_string());
    }
    matches
}

fn column_matched_fields(column: &Column, regex: &regex::Regex) -> Vec<String> {
    let candidates = [
        ("column_name", column.name.as_str()),
        ("description", column.description.as_str()),
        ("data_type", column.data_type.as_str()),
    ];
    candidates
        .into_iter()
        .filter_map(|(field, value)| regex.is_match(value).then_some(field.to_string()))
        .collect()
}

fn mock_sql_response(sql: &str) -> ExecuteSqlResponse {
    if sql.contains("FROM coral.tables") {
        return mock_coral_tables_response();
    }

    let (schema, batch, row_count) = if sql.contains("local_messages.messages") {
        let schema = Schema::new(vec![Field::new("text", DataType::Utf8, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(StringArray::from(vec!["hello", "world"]))],
        )
        .expect("build text batch");
        (schema, batch, 2)
    } else {
        let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .expect("build value batch");
        (schema, batch, 1)
    };

    ExecuteSqlResponse {
        arrow_ipc_stream: encode_arrow_ipc_stream(&schema, &[batch]).expect("encode arrow ipc"),
        row_count,
    }
}

fn mock_coral_tables_response() -> ExecuteSqlResponse {
    let schema = Schema::new(vec![
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
        Field::new("guide", DataType::Utf8, false),
        Field::new("required_filters", DataType::Utf8, false),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(StringArray::from(vec![
                "local_messages",
                "local_messages",
                "local_messages",
            ])),
            Arc::new(StringArray::from(vec!["events", "messages", "sessions"])),
            Arc::new(StringArray::from(vec![
                "Fixture events",
                "Fixture messages",
                "Fixture sessions",
            ])),
            Arc::new(StringArray::from(vec![
                "Query fixture events.",
                "Query fixture messages.",
                "Query fixture sessions.",
            ])),
            Arc::new(StringArray::from(vec!["", "owner,repo", ""])),
        ],
    )
    .expect("build coral.tables batch");

    ExecuteSqlResponse {
        arrow_ipc_stream: encode_arrow_ipc_stream(&schema, &[batch]).expect("encode arrow ipc"),
        row_count: 3,
    }
}

fn mock_discover_response() -> DiscoverSourcesResponse {
    DiscoverSourcesResponse {
        sources: vec![
            SourceInfo {
                name: "github".to_string(),
                description: "GitHub data".to_string(),
                version: "1.0.0".to_string(),
                inputs: vec![SourceInputSpec {
                    key: "GITHUB_TOKEN".to_string(),
                    required: true,
                    hint: "Create a token at github.com/settings/tokens".to_string(),
                    input: Some(ProtoSourceInput::Secret(SourceSecretInput {
                        credential: None,
                    })),
                }],
                installed: true,
                origin: SourceOrigin::Bundled as i32,
                credential_storage: SourceCredentialStorage::File as i32,
            },
            SourceInfo {
                name: "slack".to_string(),
                description: "Slack data".to_string(),
                version: "2.1.0".to_string(),
                inputs: Vec::new(),
                installed: false,
                origin: SourceOrigin::Bundled as i32,
                credential_storage: SourceCredentialStorage::Unspecified as i32,
            },
        ],
    }
}

fn mock_validate_response() -> ValidateSourceResponse {
    ValidateSourceResponse {
        source: Some(mock_source()),
        tables: vec![
            mock_table("github", "issues"),
            mock_table("github", "pull_requests"),
        ],
        table_functions: Vec::new(),
        query_tests: Vec::new(),
    }
}

fn mock_source_info(name: &str) -> Result<SourceInfo, Status> {
    match name {
        "github" => Ok(SourceInfo {
            name: "github".to_string(),
            description: "GitHub data".to_string(),
            version: "1.0.0".to_string(),
            inputs: vec![SourceInputSpec {
                key: "GITHUB_TOKEN".to_string(),
                required: true,
                hint: "Create a token at github.com/settings/tokens".to_string(),
                input: Some(ProtoSourceInput::Secret(SourceSecretInput {
                    credential: None,
                })),
            }],
            installed: true,
            origin: SourceOrigin::Bundled as i32,
            credential_storage: SourceCredentialStorage::File as i32,
        }),
        "slack" => Ok(SourceInfo {
            name: "slack".to_string(),
            description: "Slack data".to_string(),
            version: "2.1.0".to_string(),
            inputs: Vec::new(),
            installed: false,
            origin: SourceOrigin::Bundled as i32,
            credential_storage: SourceCredentialStorage::Unspecified as i32,
        }),
        "jira" => Ok(SourceInfo {
            name: "jira".to_string(),
            description: "Jira data".to_string(),
            version: "2.0.0".to_string(),
            inputs: Vec::new(),
            installed: true,
            origin: SourceOrigin::Imported as i32,
            credential_storage: SourceCredentialStorage::File as i32,
        }),
        "versionless" => Ok(SourceInfo {
            name: "versionless".to_string(),
            description: String::new(),
            version: String::new(),
            inputs: Vec::new(),
            installed: true,
            origin: SourceOrigin::Imported as i32,
            credential_storage: SourceCredentialStorage::File as i32,
        }),
        _ => Err(Status::not_found(format!("unknown source '{name}'"))),
    }
}

#[derive(Clone, Debug)]
struct MockError {
    code: Code,
    message: String,
    /// When `Some`, the error carries an AIP-193 `ErrorInfo` matching what
    /// the real server attaches via `app_status` for the
    /// `AppError::SourceNotFound` variant. Set via
    /// `MockError::source_not_found(qualified)`.
    source_not_found_qualified: Option<String>,
}

impl MockError {
    fn new(code: Code, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            source_not_found_qualified: None,
        }
    }

    fn source_not_found(qualified: impl Into<String>) -> Self {
        let qualified = qualified.into();
        Self {
            code: Code::NotFound,
            message: format!("source '{qualified}' not found"),
            source_not_found_qualified: Some(qualified),
        }
    }

    fn status(&self) -> Status {
        if self.source_not_found_qualified.is_some() {
            // Mirrors `coral_app::bootstrap::error::app_status`: the
            // reason alone discriminates the error class — no unbounded
            // identifier is echoed into structured metadata.
            let details = vec![ErrorDetail::ErrorInfo(tonic_types::ErrorInfo::new(
                CORAL_ERROR_REASON_SOURCE_NOT_FOUND,
                CORAL_ERROR_DOMAIN,
                std::collections::HashMap::new(),
            ))];
            return Status::with_error_details_vec(self.code, self.message.clone(), details);
        }
        Status::new(self.code, self.message.clone())
    }
}

#[derive(Clone)]
enum MockResult<T> {
    Ok(T),
    Err(MockError),
    Sequence(Arc<Mutex<VecDeque<MockResult<T>>>>),
}

impl<T> MockResult<T> {
    fn ok(value: T) -> Self {
        Self::Ok(value)
    }

    fn err(code: Code, message: impl Into<String>) -> Self {
        Self::Err(MockError::new(code, message))
    }

    fn source_not_found(qualified: impl Into<String>) -> Self {
        Self::Err(MockError::source_not_found(qualified))
    }

    fn sequence(results: Vec<MockResult<T>>) -> Self {
        Self::Sequence(Arc::new(Mutex::new(VecDeque::from(results))))
    }
}

impl<T: Clone> MockResult<T> {
    fn into_tonic_result(self) -> Result<T, Status> {
        match self {
            Self::Ok(value) => Ok(value),
            Self::Err(error) => Err(error.status()),
            Self::Sequence(results) => {
                let mut results = results.lock().map_err(|error| {
                    Status::internal(format!("mock result sequence lock poisoned: {error}"))
                })?;
                let next = if results.len() > 1 {
                    results.pop_front().ok_or_else(|| {
                        Status::internal("mock result sequence should not be empty")
                    })?
                } else {
                    results.front().cloned().ok_or_else(|| {
                        Status::internal("mock result sequence should not be empty")
                    })?
                };
                next.into_tonic_result()
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct MockServerConfig {
    execute_sql_override: Option<MockResult<ExecuteSqlResponse>>,
    discover_sources: MockResult<DiscoverSourcesResponse>,
    list_sources: MockResult<ListSourcesResponse>,
    list_user_owned_identities: MockResult<ListUserOwnedIdentitiesResponse>,
    delete_user_owned_identity: MockResult<DeleteUserOwnedIdentityResponse>,
    list_identity_specs: MockResult<ListIdentitySpecsResponse>,
    get_identity_spec: MockResult<GetIdentitySpecResponse>,
    add_identity_spec: MockResult<AddIdentitySpecResponse>,
    create_identity_spec: MockResult<CreateIdentitySpecResponse>,
    delete_identity_spec: MockResult<()>,
    validate_source: MockResult<ValidateSourceResponse>,
    delete_source: MockResult<()>,
}

impl Default for MockServerConfig {
    fn default() -> Self {
        Self {
            execute_sql_override: None,
            discover_sources: MockResult::ok(mock_discover_response()),
            list_identity_specs: MockResult::ok(ListIdentitySpecsResponse {
                identity_specs: vec![mock_identity_spec()],
            }),
            get_identity_spec: MockResult::ok(GetIdentitySpecResponse {
                identity_spec: Some(mock_identity_spec()),
            }),
            add_identity_spec: MockResult::ok(AddIdentitySpecResponse {
                identity_spec: Some(mock_identity_spec()),
                replaced: false,
            }),
            create_identity_spec: MockResult::ok(CreateIdentitySpecResponse {
                identity_spec: Some(mock_identity_spec()),
            }),
            list_user_owned_identities: MockResult::ok(ListUserOwnedIdentitiesResponse {
                identities: vec![mock_identity()],
            }),
            delete_user_owned_identity: MockResult::ok(DeleteUserOwnedIdentityResponse {}),
            delete_identity_spec: MockResult::ok(()),
            list_sources: MockResult::ok(ListSourcesResponse {
                sources: vec![
                    Source {
                        workspace: Some(workspace()),
                        name: "github".to_string(),
                        version: "1.0.0".to_string(),
                        secrets: Vec::new(),
                        variables: Vec::new(),
                        origin: SourceOrigin::Bundled as i32,
                        credential_storage: SourceCredentialStorage::File as i32,
                    },
                    Source {
                        workspace: Some(workspace()),
                        name: "jira".to_string(),
                        version: "2.0.0".to_string(),
                        secrets: Vec::new(),
                        variables: Vec::new(),
                        origin: SourceOrigin::Imported as i32,
                        credential_storage: SourceCredentialStorage::File as i32,
                    },
                ],
            }),
            validate_source: MockResult::ok(mock_validate_response()),
            delete_source: MockResult::ok(()),
        }
    }
}

impl MockServerConfig {
    pub(crate) fn with_discover_sources(mut self, response: DiscoverSourcesResponse) -> Self {
        self.discover_sources = MockResult::ok(response);
        self
    }

    pub(crate) fn with_list_sources(mut self, response: ListSourcesResponse) -> Self {
        self.list_sources = MockResult::ok(response);
        self
    }

    pub(crate) fn with_get_identity_spec(mut self, response: GetIdentitySpecResponse) -> Self {
        self.get_identity_spec = MockResult::ok(response);
        self
    }

    pub(crate) fn with_get_identity_spec_error(
        mut self,
        code: Code,
        message: impl Into<String>,
    ) -> Self {
        self.get_identity_spec = MockResult::err(code, message);
        self
    }

    pub(crate) fn with_get_identity_spec_error_then_response(
        mut self,
        code: Code,
        message: impl Into<String>,
        response: GetIdentitySpecResponse,
    ) -> Self {
        self.get_identity_spec = MockResult::sequence(vec![
            MockResult::err(code, message),
            MockResult::ok(response),
        ]);
        self
    }

    pub(crate) fn with_get_identity_spec_error_twice_then_response(
        mut self,
        code: Code,
        message: impl Into<String>,
        response: GetIdentitySpecResponse,
    ) -> Self {
        let message = message.into();
        self.get_identity_spec = MockResult::sequence(vec![
            MockResult::err(code, message.clone()),
            MockResult::err(code, message),
            MockResult::ok(response),
        ]);
        self
    }

    pub(crate) fn with_add_identity_spec_response(
        mut self,
        response: AddIdentitySpecResponse,
    ) -> Self {
        self.add_identity_spec = MockResult::ok(response);
        self
    }

    pub(crate) fn with_add_identity_spec_error(
        mut self,
        code: Code,
        message: impl Into<String>,
    ) -> Self {
        self.add_identity_spec = MockResult::err(code, message);
        self
    }

    pub(crate) fn with_create_identity_spec_response(
        mut self,
        response: CreateIdentitySpecResponse,
    ) -> Self {
        self.create_identity_spec = MockResult::ok(response);
        self
    }

    pub(crate) fn with_create_identity_spec_error(
        mut self,
        code: Code,
        message: impl Into<String>,
    ) -> Self {
        self.create_identity_spec = MockResult::err(code, message);
        self
    }

    pub(crate) fn with_execute_sql(mut self, response: ExecuteSqlResponse) -> Self {
        self.execute_sql_override = Some(MockResult::ok(response));
        self
    }

    pub(crate) fn with_execute_sql_error(mut self, code: Code, message: impl Into<String>) -> Self {
        self.execute_sql_override = Some(MockResult::err(code, message));
        self
    }

    pub(crate) fn with_validate_source_error(
        mut self,
        code: Code,
        message: impl Into<String>,
    ) -> Self {
        self.validate_source = MockResult::err(code, message);
        self
    }

    pub(crate) fn with_validate_source_response(
        mut self,
        response: ValidateSourceResponse,
    ) -> Self {
        self.validate_source = MockResult::ok(response);
        self
    }

    /// Mirrors what the real server emits for `AppError::SourceNotFound`
    /// from `validate_source` (a `Code::NotFound` Status carrying an
    /// AIP-193 `ErrorInfo` with `reason = "SOURCE_NOT_FOUND"`).
    pub(crate) fn with_validate_source_not_found(mut self, qualified: impl Into<String>) -> Self {
        self.validate_source = MockResult::source_not_found(qualified);
        self
    }

    pub(crate) fn with_delete_source_error(
        mut self,
        code: Code,
        message: impl Into<String>,
    ) -> Self {
        self.delete_source = MockResult::err(code, message);
        self
    }

    /// Mirrors what the real server emits for `AppError::SourceNotFound`
    /// from `delete_source` (a `Code::NotFound` Status carrying an
    /// AIP-193 `ErrorInfo` with `reason = "SOURCE_NOT_FOUND"`).
    pub(crate) fn with_delete_source_not_found(mut self, qualified: impl Into<String>) -> Self {
        self.delete_source = MockResult::source_not_found(qualified);
        self
    }
}

fn list_catalog_response(request: &ListCatalogRequest) -> ListCatalogResponse {
    let tables = mock_visible_tables()
        .into_iter()
        .filter(|table| request.schema_name.is_empty() || table.schema_name == request.schema_name)
        .collect::<Vec<_>>();
    let table_count = u32::try_from(tables.len()).unwrap_or(u32::MAX);
    let items = tables
        .into_iter()
        .filter(|_| request.kind == 0 || request.kind == 1)
        .map(|table| CatalogItem {
            item: Some(catalog_item::Item::Table(table_summary(&table))),
        })
        .collect::<Vec<_>>();
    let (items, pagination) = paginate(
        items,
        request.pagination.unwrap_or(PaginationRequest {
            limit: 0,
            offset: 0,
        }),
    );
    ListCatalogResponse {
        items,
        pagination: Some(pagination),
        counts: Some(CatalogCounts {
            table_count,
            table_function_count: 0,
        }),
    }
}

/// Declares one capture slot per mocked RPC together with the `MockServer`
/// getter that exposes the recorded requests to tests.
macro_rules! captured_requests {
    ($($field:ident as $getter:ident: $request:ty),* $(,)?) => {
        #[derive(Default)]
        struct Captured {
            $($field: Mutex<Vec<$request>>,)*
        }

        impl MockServer {
            $(pub(crate) fn $getter(&self) -> Vec<$request> {
                self.captured
                    .$field
                    .lock()
                    .expect("captured requests lock")
                    .clone()
            })*
        }
    };
}

captured_requests! {
    execute_sql as execute_sql_requests: ExecuteSqlRequest,
    list_catalog as list_catalog_requests: ListCatalogRequest,
    search_catalog as search_catalog_requests: SearchCatalogRequest,
    describe_table as describe_table_requests: DescribeTableRequest,
    list_columns as list_columns_requests: ListColumnsRequest,
    discover_sources as discover_sources_requests: DiscoverSourcesRequest,
    list_sources as list_sources_requests: ListSourcesRequest,
    get_source as get_source_requests: GetSourceRequest,
    get_source_info as get_source_info_requests: GetSourceInfoRequest,
    create_bundled_source as create_bundled_source_requests: CreateBundledSourceRequest,
    create_bundled_source_with_oauth as create_bundled_source_with_oauth_requests:
        CreateBundledSourceWithOAuthRequest,
    import_source as import_source_requests: ImportSourceRequest,
    add_identity_spec as add_identity_spec_requests: AddIdentitySpecRequest,
    create_identity_spec as create_identity_spec_requests: CreateIdentitySpecRequest,
    list_identity_specs as list_identity_specs_requests: ListIdentitySpecsRequest,
    get_identity_spec as get_identity_spec_requests: GetIdentitySpecRequest,
    delete_identity_spec as delete_identity_spec_requests: DeleteIdentitySpecRequest,
    create_user_owned_identity_with_oauth as create_user_owned_identity_with_oauth_requests:
        CreateUserOwnedIdentityWithOAuthRequest,
    create_user_owned_identity_with_fixed_token
        as create_user_owned_identity_with_fixed_token_requests:
        CreateUserOwnedIdentityWithFixedTokenRequest,
    list_user_owned_identities as list_user_owned_identities_requests:
        ListUserOwnedIdentitiesRequest,
    delete_user_owned_identity as delete_user_owned_identity_requests:
        DeleteUserOwnedIdentityRequest,
    delete_source as delete_source_requests: DeleteSourceRequest,
    validate_source as validate_source_requests: ValidateSourceRequest,
}

/// Records the unwrapped `request` in its capture slot and returns it for
/// handlers that also inspect the request.
fn capture<T: Clone>(slot: &Mutex<Vec<T>>, request: Request<T>) -> T {
    let request = request.into_inner();
    slot.lock()
        .expect("captured requests lock")
        .push(request.clone());
    request
}

/// Server-streaming response type used by the mocks: one event, then end of
/// stream.
type EventStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

fn single_event_stream<T: Send + 'static>(event: T) -> EventStream<T> {
    Box::pin(tokio_stream::once(Ok(event)))
}

pub(crate) fn encode_arrow_ipc_stream(
    schema: &Schema,
    batches: &[RecordBatch],
) -> Result<Vec<u8>, arrow::error::ArrowError> {
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, schema)?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()?;
    }
    Ok(bytes)
}

#[derive(Clone)]
struct MockQueryService {
    config: Arc<MockServerConfig>,
    captured: Arc<Captured>,
}

#[tonic::async_trait]
impl QueryService for MockQueryService {
    async fn execute_sql(
        &self,
        request: Request<ExecuteSqlRequest>,
    ) -> Result<Response<ExecuteSqlResponse>, Status> {
        let sql = capture(&self.captured.execute_sql, request).sql;
        if sql
            .trim_start()
            .to_ascii_uppercase()
            .starts_with("DELETE FROM")
        {
            return Err(Status::invalid_argument("DML not supported: DELETE"));
        }

        let response = match self.config.execute_sql_override.clone() {
            Some(result) => result.into_tonic_result()?,
            None => mock_sql_response(&sql),
        };

        Ok(Response::new(response))
    }

    async fn explain_sql(
        &self,
        _request: Request<ExplainSqlRequest>,
    ) -> Result<Response<ExplainSqlResponse>, Status> {
        Ok(Response::new(ExplainSqlResponse {
            plan: Some(QueryPlan {
                unoptimized_logical_plan: "LogicalPlan".to_string(),
                optimized_logical_plan: "OptimizedLogicalPlan".to_string(),
                physical_plan: "PhysicalPlan".to_string(),
            }),
        }))
    }
}

#[derive(Clone)]
struct MockCatalogService {
    captured: Arc<Captured>,
}

#[tonic::async_trait]
impl CatalogService for MockCatalogService {
    async fn list_catalog(
        &self,
        request: Request<ListCatalogRequest>,
    ) -> Result<Response<ListCatalogResponse>, Status> {
        let request = capture(&self.captured.list_catalog, request);
        Ok(Response::new(list_catalog_response(&request)))
    }

    async fn search_catalog(
        &self,
        request: Request<SearchCatalogRequest>,
    ) -> Result<Response<SearchCatalogResponse>, Status> {
        let request = capture(&self.captured.search_catalog, request);
        let pattern = regex::RegexBuilder::new(&request.pattern)
            .case_insensitive(request.ignore_case)
            .build()
            .map_err(|error| Status::invalid_argument(format!("invalid regex pattern: {error}")))?;
        let mut matches = Vec::new();
        if request.kind == 0 || request.kind == 1 {
            for table in mock_visible_tables().into_iter().filter(|table| {
                request.schema_name.is_empty() || table.schema_name == request.schema_name
            }) {
                let matched_fields = table_matched_fields(&table, &pattern);
                if !matched_fields.is_empty() {
                    matches.push(CatalogSearchResult {
                        item: Some(CatalogItem {
                            item: Some(catalog_item::Item::Table(table_summary(&table))),
                        }),
                        matched_fields,
                    });
                }
            }
        }
        let (items, pagination) = paginate(
            matches,
            request.pagination.unwrap_or(PaginationRequest {
                limit: 20,
                offset: 0,
            }),
        );
        Ok(Response::new(SearchCatalogResponse {
            items,
            pagination: Some(pagination),
        }))
    }

    async fn describe_table(
        &self,
        request: Request<DescribeTableRequest>,
    ) -> Result<Response<DescribeTableResponse>, Status> {
        let request = capture(&self.captured.describe_table, request);
        let table = mock_visible_tables().into_iter().find(|table| {
            table.schema_name == request.schema_name && table.name == request.table_name
        });
        if let Some(table) = table {
            return Ok(Response::new(DescribeTableResponse {
                table: Some(table),
                suggestions: Vec::new(),
                available_schemas: Vec::new(),
                same_schema_tables: Vec::new(),
            }));
        }
        let same_schema_tables = mock_visible_tables()
            .into_iter()
            .filter(|table| table.schema_name == request.schema_name)
            .take(10)
            .map(|table| table_summary(&table))
            .collect();
        Ok(Response::new(DescribeTableResponse {
            table: None,
            suggestions: Vec::new(),
            available_schemas: vec!["local_messages".to_string()],
            same_schema_tables,
        }))
    }

    async fn list_columns(
        &self,
        request: Request<ListColumnsRequest>,
    ) -> Result<Response<ListColumnsResponse>, Status> {
        let request = capture(&self.captured.list_columns, request);
        let table = mock_visible_tables()
            .into_iter()
            .find(|table| {
                table.schema_name == request.schema_name && table.name == request.table_name
            })
            .ok_or_else(|| Status::not_found("table not found"))?;
        let regex = request
            .pattern
            .as_deref()
            .map(|pattern| {
                regex::RegexBuilder::new(pattern)
                    .case_insensitive(request.ignore_case)
                    .build()
                    .map_err(|error| {
                        Status::invalid_argument(format!("invalid regex pattern: {error}"))
                    })
            })
            .transpose()?;
        let mut columns = Vec::new();
        for column in table.columns {
            if request.required_only && !column.is_required_filter {
                continue;
            }
            let matched_fields = regex
                .as_ref()
                .map_or_else(Vec::new, |regex| column_matched_fields(&column, regex));
            if regex.is_some() && matched_fields.is_empty() {
                continue;
            }
            columns.push(ColumnSearchResult {
                column: Some(column),
                matched_fields,
            });
        }
        let (columns, pagination) = paginate(
            columns,
            request.pagination.unwrap_or(PaginationRequest {
                limit: 50,
                offset: 0,
            }),
        );
        Ok(Response::new(ListColumnsResponse {
            columns,
            pagination: Some(pagination),
        }))
    }
}

#[derive(Clone)]
struct MockIdentitySpecService {
    config: Arc<MockServerConfig>,
    captured: Arc<Captured>,
}

/// Generates one mock service impl. Every method records the request in its
/// `captured` slot, then evaluates `$body` (with the captured request and the
/// server config in scope) to produce the response. Generating the whole
/// `impl` keeps `#[tonic::async_trait]` running after macro expansion.
macro_rules! mock_service {
    (
        impl $service:ident for $mock:ident {
            $(type $stream:ident = $stream_ty:ty;)*
            $(fn $rpc:ident($slot:ident, $req:ty) -> $resp:ty = |$cfg:ident, $request:ident| $body:expr;)*
        }
    ) => {
        #[tonic::async_trait]
        impl $service for $mock {
            $(type $stream = $stream_ty;)*
            $(
                async fn $rpc(&self, request: Request<$req>) -> Result<Response<$resp>, Status> {
                    let $request = capture(&self.captured.$slot, request);
                    let $cfg = &self.config;
                    Ok(Response::new($body))
                }
            )*
        }
    };
}

mock_service! {
    impl IdentitySpecService for MockIdentitySpecService {
        fn add_identity_spec(add_identity_spec, AddIdentitySpecRequest) -> AddIdentitySpecResponse =
            |cfg, _request| cfg.add_identity_spec.clone().into_tonic_result()?;
        fn create_identity_spec(create_identity_spec, CreateIdentitySpecRequest)
            -> CreateIdentitySpecResponse =
            |cfg, _request| cfg.create_identity_spec.clone().into_tonic_result()?;
        fn list_identity_specs(list_identity_specs, ListIdentitySpecsRequest)
            -> ListIdentitySpecsResponse =
            |cfg, _request| cfg.list_identity_specs.clone().into_tonic_result()?;
        fn get_identity_spec(get_identity_spec, GetIdentitySpecRequest) -> GetIdentitySpecResponse =
            |cfg, _request| cfg.get_identity_spec.clone().into_tonic_result()?;
        fn delete_identity_spec(delete_identity_spec, DeleteIdentitySpecRequest)
            -> DeleteIdentitySpecResponse =
            |cfg, _request| {
                cfg.delete_identity_spec.clone().into_tonic_result()?;
                DeleteIdentitySpecResponse {
                    orphaned_identities: 0,
                }
            };
    }
}

#[derive(Clone)]
struct MockIdentityService {
    config: Arc<MockServerConfig>,
    captured: Arc<Captured>,
}

mock_service! {
    impl IdentityService for MockIdentityService {
        type CreateUserOwnedIdentityWithOAuthStream =
            EventStream<CreateUserOwnedIdentityWithOAuthResponse>;
        fn create_user_owned_identity_with_o_auth(
            create_user_owned_identity_with_oauth,
            CreateUserOwnedIdentityWithOAuthRequest
        ) -> EventStream<CreateUserOwnedIdentityWithOAuthResponse> =
            |_cfg, _request| single_event_stream(CreateUserOwnedIdentityWithOAuthResponse {
                event: Some(
                    create_user_owned_identity_with_o_auth_response::Event::Identity(
                        mock_identity(),
                    ),
                ),
            });
        fn create_user_owned_identity_with_fixed_token(
            create_user_owned_identity_with_fixed_token,
            CreateUserOwnedIdentityWithFixedTokenRequest
        ) -> CreateUserOwnedIdentityWithFixedTokenResponse =
            |_cfg, _request| CreateUserOwnedIdentityWithFixedTokenResponse {
                identity: Some(mock_identity()),
            };
        fn list_user_owned_identities(list_user_owned_identities, ListUserOwnedIdentitiesRequest)
            -> ListUserOwnedIdentitiesResponse =
            |cfg, _request| cfg.list_user_owned_identities.clone().into_tonic_result()?;
        fn delete_user_owned_identity(delete_user_owned_identity, DeleteUserOwnedIdentityRequest)
            -> DeleteUserOwnedIdentityResponse =
            |cfg, _request| cfg.delete_user_owned_identity.clone().into_tonic_result()?;
    }
}

#[derive(Clone)]
struct MockSourceService {
    config: Arc<MockServerConfig>,
    captured: Arc<Captured>,
}

mock_service! {
    impl SourceService for MockSourceService {
        type CreateBundledSourceWithOAuthStream =
            EventStream<CreateBundledSourceWithOAuthResponse>;
        type ImportSourceStream = EventStream<ImportSourceResponse>;
        fn discover_sources(discover_sources, DiscoverSourcesRequest) -> DiscoverSourcesResponse =
            |cfg, _request| cfg.discover_sources.clone().into_tonic_result()?;
        fn list_sources(list_sources, ListSourcesRequest) -> ListSourcesResponse =
            |cfg, _request| cfg.list_sources.clone().into_tonic_result()?;
        fn get_source(get_source, GetSourceRequest) -> GetSourceResponse =
            |_cfg, _request| GetSourceResponse {
                source: Some(mock_source()),
            };
        fn get_source_info(get_source_info, GetSourceInfoRequest) -> GetSourceInfoResponse =
            |_cfg, request| GetSourceInfoResponse {
                source_info: Some(mock_source_info(&request.name)?),
            };
        fn create_bundled_source(create_bundled_source, CreateBundledSourceRequest)
            -> CreateBundledSourceResponse =
            |_cfg, _request| CreateBundledSourceResponse {
                source: Some(mock_source()),
            };
        fn create_bundled_source_with_o_auth(
            create_bundled_source_with_oauth,
            CreateBundledSourceWithOAuthRequest
        ) -> EventStream<CreateBundledSourceWithOAuthResponse> =
            |_cfg, _request| single_event_stream(CreateBundledSourceWithOAuthResponse {
                event: Some(create_bundled_source_with_o_auth_response::Event::Source(
                    mock_source(),
                )),
            });
        fn import_source(import_source, ImportSourceRequest)
            -> EventStream<ImportSourceResponse> =
            |_cfg, _request| single_event_stream(ImportSourceResponse {
                event: Some(import_source_response::Event::Source(mock_source())),
            });
        fn delete_source(delete_source, DeleteSourceRequest) -> DeleteSourceResponse =
            |cfg, _request| {
                cfg.delete_source.clone().into_tonic_result()?;
                DeleteSourceResponse {}
            };
        fn validate_source(validate_source, ValidateSourceRequest) -> ValidateSourceResponse =
            |cfg, _request| cfg.validate_source.clone().into_tonic_result()?;
    }
}

pub(crate) struct MockServer {
    endpoint_uri: String,
    config_dir: TempDir,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<(), tonic::transport::Error>>,
    captured: Arc<Captured>,
}

impl MockServer {
    pub(crate) async fn start() -> Self {
        Self::start_with_config(MockServerConfig::default()).await
    }

    pub(crate) async fn start_with_config(config: MockServerConfig) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind mock server");
        let endpoint_uri = format!("http://{}", listener.local_addr().expect("local addr"));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let config = Arc::new(config);
        let captured = Arc::new(Captured::default());
        let task_captured = Arc::clone(&captured);
        let task = tokio::spawn(async move {
            let captured = task_captured;
            Server::builder()
                .add_service(CatalogServiceServer::new(MockCatalogService {
                    captured: Arc::clone(&captured),
                }))
                .add_service(QueryServiceServer::new(MockQueryService {
                    config: Arc::clone(&config),
                    captured: Arc::clone(&captured),
                }))
                .add_service(IdentitySpecServiceServer::new(MockIdentitySpecService {
                    config: Arc::clone(&config),
                    captured: Arc::clone(&captured),
                }))
                .add_service(IdentityServiceServer::new(MockIdentityService {
                    config: Arc::clone(&config),
                    captured: Arc::clone(&captured),
                }))
                .add_service(SourceServiceServer::new(MockSourceService {
                    config,
                    captured,
                }))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    drop(shutdown_rx.await);
                })
                .await
        });
        Self {
            endpoint_uri,
            config_dir: TempDir::new().expect("mock server config dir"),
            shutdown_tx: Some(shutdown_tx),
            task,
            captured,
        }
    }

    pub(crate) async fn start_with_validate_source_response(
        validate_source_response: ValidateSourceResponse,
    ) -> Self {
        Self::start_with_config(
            MockServerConfig::default().with_validate_source_response(validate_source_response),
        )
        .await
    }

    pub(crate) fn cmd(&self) -> Command {
        let mut cmd = Command::cargo_bin("coral").expect("cargo bin");
        cmd.env("CORAL_ENDPOINT", &self.endpoint_uri);
        cmd.env("CORAL_CONFIG_DIR", self.config_dir.path());
        cmd
    }

    pub(crate) fn config_dir(&self) -> &Path {
        self.config_dir.path()
    }

    pub(crate) fn endpoint_uri(&self) -> &str {
        &self.endpoint_uri
    }

    pub(crate) async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            #[expect(
                clippy::let_underscore_must_use,
                reason = "send error means the receiver is already dropped, which is fine during shutdown"
            )]
            let _ = tx.send(());
        }
        self.task.await.expect("join").expect("server");
    }
}
