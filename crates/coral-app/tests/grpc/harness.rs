use std::fs;
use std::path::{Path, PathBuf};

use coral_api::v1::{
    ExecuteSqlRequest, ImportSourceRequest, ListSourcesRequest, Source, SourceSecret,
    SourceVariable, ValidateSourceRequest, ValidateSourceResponse, import_source_response,
};
use coral_app::{RunningServer, ServerBuilder};
use coral_client::{
    AppClient, QueryClient, SourceClient, batches_to_json_rows, decode_execute_sql_response,
    default_workspace,
};
use serde_json::{Value, json};
use tempfile::TempDir;
use tonic::Request;

pub(crate) struct GrpcHarness {
    temp_dir: TempDir,
    config_dir: PathBuf,
    app: AppClient,
    _server: RunningServer,
}

impl GrpcHarness {
    pub(crate) async fn new() -> Self {
        let temp_dir = TempDir::new().expect("temp dir");
        let config_dir = temp_dir.path().join("coral-config");
        Self::start_with_parts(temp_dir, config_dir).await
    }

    pub(crate) async fn start_with_config_dir(config_dir: PathBuf) -> Self {
        let temp_dir = TempDir::new().expect("temp dir");
        Self::start_with_parts(temp_dir, config_dir).await
    }

    async fn start_with_parts(temp_dir: TempDir, config_dir: PathBuf) -> Self {
        ensure_file_credentials_config(&config_dir);
        let server = ServerBuilder::new()
            .with_config_dir(&config_dir)
            .start()
            .await
            .expect("start server");
        let app = AppClient::connect(server.endpoint_uri())
            .await
            .expect("connect client");
        Self {
            temp_dir,
            config_dir,
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

    pub(crate) fn source_client(&self) -> SourceClient {
        self.app.source_client()
    }

    pub(crate) fn query_client(&self) -> QueryClient {
        self.app.query_client()
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

pub(crate) fn fixture_manifest_yaml(root: &Path) -> String {
    fixture_manifest_with_test_queries_yaml(root, &[])
}

pub(crate) fn fixture_function_only_manifest_yaml(root: &Path) -> String {
    let descriptor = write_openapi_descriptor(
        root,
        "searchy-function-openapi.yaml",
        json!({
            "/search/issues": {
                "get": {
                    "operationId": "search_issues",
                    "summary": "Search issues",
                    "description": "Search issues",
                    "parameters": [{
                        "name": "q",
                        "in": "query",
                        "required": true,
                        "schema": { "type": "string" },
                    }],
                    "responses": object_response(json!({
                        "title": { "type": "string", "description": "Issue title" },
                    })),
                },
            },
        }),
    );
    manifest_yaml(&json!({
        "spec_version": 1,
        "kind": "source",
        "name": "searchy",
        "description": "Searchy fixture",
        "interfaces": [{
            "id": "rest",
            "type": "openapi",
            "file": descriptor,
        }],
    }))
}

pub(crate) fn fixture_manifest_with_test_queries_yaml(
    root: &Path,
    test_queries: &[&str],
) -> String {
    let data_file = write_messages_jsonl(root);
    manifest_yaml(&json!({
        "spec_version": 1,
        "kind": "source",
        "name": "local_messages",
        "description": "Fixture messages",
        "test_queries": test_queries,
        "interfaces": [{
            "id": "read_files",
            "type": "file",
            "files": [data_file],
            "format": {
                "kind": "jsonl",
            },
        }],
    }))
}

pub(crate) fn fixture_manifest_with_inputs_yaml(root: &Path) -> String {
    let data_file = write_messages_jsonl(root);
    manifest_yaml(&json!({
        "spec_version": 1,
        "kind": "source",
        "name": "secured_messages",
        "description": "Secured messages",
        "inputs": [
            { "key": "API_BASE", "kind": "variable", "default": "https://example.com" },
            { "key": "API_TOKEN", "kind": "secret" },
        ],
        "interfaces": [{
            "id": "read_files",
            "type": "file",
            "files": [data_file],
            "format": {
                "kind": "jsonl",
            },
        }],
    }))
}

pub(crate) fn fixture_manifest_with_required_inputs_yaml(root: &Path) -> String {
    let data_file = write_messages_jsonl(root);
    manifest_yaml(&json!({
        "spec_version": 1,
        "kind": "source",
        "name": "required_messages",
        "description": "Required-input messages",
        "inputs": [
            { "key": "API_BASE", "kind": "variable" },
            { "key": "API_TOKEN", "kind": "secret" },
        ],
        "interfaces": [{
            "id": "read_files",
            "type": "file",
            "files": [data_file],
            "format": {
                "kind": "jsonl",
            },
        }],
    }))
}

pub(crate) fn invalid_manifest_yaml() -> String {
    manifest_yaml(&json!({
        "name": "demo",
        "spec_version": 1,
        "kind": "source",
        "schema": "demo",
        "interfaces": [{
            "id": "files",
            "type": "file",
            "files": ["./missing.jsonl"],
            "format": {
                "kind": "jsonl",
            },
        }],
    }))
}

fn manifest_yaml(value: &Value) -> String {
    serde_yaml::to_string(value).expect("serialize manifest yaml")
}

fn write_messages_jsonl(root: &Path) -> String {
    write_jsonl_fixture(
        root,
        "messages.jsonl",
        r#"{"type":"user","sessionId":"s1","text":"hello"}
{"type":"assistant","sessionId":"s1","text":"world"}
"#,
    )
}

fn write_jsonl_fixture(root: &Path, file_name: &str, content: &str) -> String {
    let data_dir = root.join("fixture-data");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let data_file = data_dir.join(file_name);
    fs::write(&data_file, content).expect("write jsonl");
    data_file.display().to_string()
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "fixture builders intentionally accept json! temporaries by value"
)]
fn write_openapi_descriptor(root: &Path, file_name: &str, paths: Value) -> String {
    let data_dir = root.join("fixture-descriptors");
    fs::create_dir_all(&data_dir).expect("create descriptor dir");
    let descriptor = data_dir.join(file_name);
    let document = json!({
        "openapi": "3.0.3",
        "info": {
            "title": file_name,
            "version": "0.1.0",
        },
        "paths": paths,
    });
    fs::write(
        &descriptor,
        serde_yaml::to_string(&document).expect("serialize OpenAPI fixture"),
    )
    .expect("write OpenAPI fixture");
    descriptor.display().to_string()
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "fixture builders intentionally accept json! temporaries by value"
)]
fn object_response(properties: Value) -> Value {
    json!({
        "200": {
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "properties": properties,
                    },
                },
            },
        },
    })
}

pub(crate) fn source_dir(config_dir: &Path, source_name: &str) -> PathBuf {
    config_dir
        .join("workspaces")
        .join("default")
        .join("sources")
        .join(source_name)
}
