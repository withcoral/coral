//! Builds and runs the Coral gRPC server.

use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use coral_api::v1::query_service_server::QueryServiceServer;
use coral_api::v1::source_service_server::SourceServiceServer;
use coral_api::{HTTP2_MAX_HEADER_LIST_SIZE, QUERY_RESPONSE_MAX_MESSAGE_SIZE};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use super::env::AppEnvironment;
use super::error::AppError;
use crate::EngineExtensionsProvider;
use crate::query::manager::QueryManager;
use crate::query::service::QueryService;
use crate::sources::manager::SourceManager;
use crate::sources::service::SourceService;
use crate::state::{AppStateLayout, ConfigStore, SecretStore};

/// Server-side bootstrap configuration for the Coral server.
#[derive(Clone)]
pub(crate) struct ServerConfig {
    config_dir: Option<PathBuf>,
    engine_extensions_providers: Vec<Arc<dyn EngineExtensionsProvider>>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl ServerConfig {
    #[must_use]
    /// Creates the default local server configuration.
    pub(crate) fn new() -> Self {
        Self {
            config_dir: None,
            engine_extensions_providers: Vec::new(),
        }
    }

    #[must_use]
    /// Overrides the Coral config directory used by the local server.
    pub(crate) fn with_config_dir(mut self, config_dir: impl Into<PathBuf>) -> Self {
        self.config_dir = Some(config_dir.into());
        self
    }

    #[must_use]
    pub(crate) fn add_engine_extensions_provider(
        mut self,
        engine_extensions_provider: Arc<dyn EngineExtensionsProvider>,
    ) -> Self {
        self.engine_extensions_providers
            .push(engine_extensions_provider);
        self
    }
}

/// Builder for the Coral server runtime.
#[derive(Clone, Default)]
pub struct ServerBuilder {
    config: ServerConfig,
}

impl ServerBuilder {
    #[must_use]
    /// Creates a builder that resolves its server config from defaults.
    pub fn new() -> Self {
        Self {
            config: ServerConfig::new(),
        }
    }

    #[must_use]
    /// Overrides the Coral config directory used by the local server.
    pub fn with_config_dir(mut self, config_dir: impl Into<PathBuf>) -> Self {
        self.config = self.config.with_config_dir(config_dir);
        self
    }

    #[must_use]
    /// Adds an engine extensions provider used for query runtime builds.
    ///
    /// Providers are evaluated in call order, so later providers can add or
    /// override engine extensions produced by earlier providers.
    pub fn add_engine_extensions_provider(
        mut self,
        engine_extensions_provider: Arc<dyn EngineExtensionsProvider>,
    ) -> Self {
        self.config = self
            .config
            .add_engine_extensions_provider(engine_extensions_provider);
        self
    }

    /// Starts the Coral gRPC server on loopback TCP.
    ///
    /// Coral keeps a real local gRPC boundary here so the public client talks
    /// to the same typed transport contract the server exposes.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if the config directory cannot be determined,
    /// required directories cannot be created, the config or secrets backends
    /// fail to initialize, or the gRPC server cannot be started.
    pub async fn start(self) -> Result<RunningServer, AppError> {
        let env = AppEnvironment::discover();
        let layout = AppStateLayout::discover(
            self.config
                .config_dir
                .or_else(|| env.coral_config_dir_override()),
        )?;
        layout.ensure()?;
        let config_store = ConfigStore::new(layout.clone());
        let secret_store = SecretStore::new(layout.clone());
        let source_manager =
            SourceManager::new(config_store.clone(), secret_store.clone(), layout.clone());
        let query_manager = QueryManager::new(
            config_store,
            secret_store,
            env.query_runtime_context(),
            layout,
            self.config.engine_extensions_providers,
        );
        start_server(source_manager, query_manager).await
    }
}

/// Running Coral gRPC server.
///
/// Call [`RunningServer::shutdown`] for deterministic teardown. Dropping this
/// handle sends shutdown to the background task as a best-effort fallback, but
/// does not wait for the task to finish.
pub struct RunningServer {
    endpoint_uri: String,
    shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,
    task: Mutex<Option<JoinHandle<Result<(), tonic::transport::Error>>>>,
}

impl RunningServer {
    #[must_use]
    /// Returns the loopback endpoint URI for this server.
    ///
    /// This is part of the narrow sibling-facing bootstrap seam used by the
    /// thin local client and by integration tests that need explicit control
    /// over server configuration.
    pub fn endpoint_uri(&self) -> &str {
        &self.endpoint_uri
    }

    /// Shuts the server down and waits for the background task to finish.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] if the server task fails while shutting down.
    pub async fn shutdown(self) -> Result<(), AppError> {
        self.shutdown_inner().await
    }

    async fn shutdown_inner(&self) -> Result<(), AppError> {
        if let Some(shutdown_tx) = self
            .shutdown_tx
            .lock()
            .expect("shutdown mutex poisoned")
            .take()
        {
            let _ = shutdown_tx.send(());
        }

        let task = self.task.lock().expect("task mutex poisoned").take();
        if let Some(task) = task {
            task.await??;
        }
        Ok(())
    }
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        if let Some(shutdown_tx) = self
            .shutdown_tx
            .lock()
            .expect("shutdown mutex poisoned")
            .take()
        {
            let _ = shutdown_tx.send(());
        }
    }
}

async fn start_server(
    source_manager: SourceManager,
    query_manager: QueryManager,
) -> Result<RunningServer, AppError> {
    let source_service = SourceService::new(source_manager, query_manager.clone());
    let query_service = QueryService::new(query_manager);
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let endpoint_uri = format!("http://{}", listener.local_addr()?);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let task = tokio::spawn(async move {
        Server::builder()
            .http2_max_header_list_size(HTTP2_MAX_HEADER_LIST_SIZE)
            .add_service(SourceServiceServer::new(source_service))
            .add_service(
                QueryServiceServer::new(query_service)
                    .max_encoding_message_size(QUERY_RESPONSE_MAX_MESSAGE_SIZE),
            )
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    Ok(RunningServer {
        endpoint_uri,
        shutdown_tx: Mutex::new(Some(shutdown_tx)),
        task: Mutex::new(Some(task)),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use coral_api::v1::query_service_client::QueryServiceClient;
    use coral_api::v1::source_service_client::SourceServiceClient;
    use coral_api::v1::{ExecuteSqlRequest, ImportSourceRequest, Workspace};
    use coral_api::{HTTP2_MAX_HEADER_LIST_SIZE, QUERY_RESPONSE_MAX_MESSAGE_SIZE};
    use coral_engine::QueryRuntimeContext;
    use tempfile::TempDir;
    use tonic::Request;
    use tonic::transport::Endpoint;

    use super::{ServerBuilder, start_server};
    use crate::query::manager::QueryManager;
    use crate::sources::manager::SourceManager;
    use crate::state::{AppStateLayout, ConfigStore, SecretStore};
    use crate::transport::workspace_to_proto;
    use crate::workspaces::WorkspaceName;
    use crate::{AwsEngineExtensionsProvider, NoopEngineExtensionsProvider};

    fn default_workspace() -> Workspace {
        workspace_to_proto(&WorkspaceName::default())
    }

    #[test]
    fn server_builder_accepts_engine_extensions_providers() {
        let _ = ServerBuilder::new()
            .add_engine_extensions_provider(Arc::new(AwsEngineExtensionsProvider))
            .add_engine_extensions_provider(Arc::new(NoopEngineExtensionsProvider));
    }

    #[tokio::test]
    async fn file_tilde_sources_resolve_from_app_owned_runtime_context() {
        let temp = TempDir::new().expect("temp dir");
        let fake_home = temp.path().join("fake-home");
        let config_dir = temp.path().join("coral-config");
        let data_dir = fake_home.join("fixture-data");
        std::fs::create_dir_all(&data_dir).expect("create data dir");
        std::fs::write(
            data_dir.join("messages.jsonl"),
            r#"{"type":"user","text":"hello"}
{"type":"assistant","text":"world"}
"#,
        )
        .expect("write fixture");

        let layout = AppStateLayout::discover(Some(config_dir.clone())).expect("layout");
        let source_manager = SourceManager::new(
            ConfigStore::new(layout.clone()),
            SecretStore::new(layout.clone()),
            layout.clone(),
        );
        let query_manager = QueryManager::new(
            ConfigStore::new(layout.clone()),
            SecretStore::new(layout.clone()),
            QueryRuntimeContext {
                home_dir: Some(fake_home.clone()),
            },
            layout,
            vec![Arc::new(NoopEngineExtensionsProvider)],
        );
        let running = start_server(source_manager, query_manager)
            .await
            .expect("start server");
        let channel = Endpoint::from_shared(running.endpoint_uri().to_string())
            .expect("endpoint")
            .http2_max_header_list_size(HTTP2_MAX_HEADER_LIST_SIZE)
            .connect()
            .await
            .expect("connect");
        let mut source_client = SourceServiceClient::new(channel.clone());
        let mut query_client = QueryServiceClient::new(channel)
            .max_decoding_message_size(QUERY_RESPONSE_MAX_MESSAGE_SIZE);

        source_client
            .import_source(Request::new(ImportSourceRequest {
                workspace: Some(default_workspace()),
                manifest_yaml: r#"
name: tilde_demo
version: 0.1.0
dsl_version: 3
backend: jsonl
tables:
  - name: messages
    description: Fixture messages
    source:
      location: file://~/fixture-data/
      glob: "**/*.jsonl"
    columns:
      - name: type
        type: Utf8
      - name: text
        type: Utf8
"#
                .to_string(),
                variables: Vec::new(),
                secrets: Vec::new(),
            }))
            .await
            .expect("create source");

        let response = query_client
            .execute_sql(Request::new(ExecuteSqlRequest {
                workspace: Some(default_workspace()),
                sql: "SELECT text FROM tilde_demo.messages ORDER BY text".to_string(),
            }))
            .await
            .expect("execute sql")
            .into_inner();
        let result = coral_client::decode_execute_sql_response(&response).expect("decode");
        let rows = coral_client::batches_to_json_rows(result.batches()).expect("rows");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["text"], "hello");
        assert_eq!(rows[1]["text"], "world");
    }

    /// an `ExecuteSql` response larger than
    /// the previous tonic 4 MB default must round-trip cleanly. Before the
    /// fix, this query failed with `h2 protocol error … PROTOCOL_ERROR`.
    #[tokio::test]
    async fn execute_sql_response_above_default_4mb_limit_round_trips() {
        let temp = TempDir::new().expect("temp dir");
        let config_dir = temp.path().join("coral-config");

        let layout = AppStateLayout::discover(Some(config_dir.clone())).expect("layout");
        let source_manager = SourceManager::new(
            ConfigStore::new(layout.clone()),
            SecretStore::new(layout.clone()),
            layout.clone(),
        );
        let query_manager = QueryManager::new(
            ConfigStore::new(layout.clone()),
            SecretStore::new(layout.clone()),
            QueryRuntimeContext { home_dir: None },
            layout,
            vec![Arc::new(NoopEngineExtensionsProvider)],
        );
        let running = start_server(source_manager, query_manager)
            .await
            .expect("start server");
        let channel = Endpoint::from_shared(running.endpoint_uri().to_string())
            .expect("endpoint")
            .http2_max_header_list_size(HTTP2_MAX_HEADER_LIST_SIZE)
            .connect()
            .await
            .expect("connect");
        let mut query_client = QueryServiceClient::new(channel)
            .max_decoding_message_size(QUERY_RESPONSE_MAX_MESSAGE_SIZE);

        // No underscore separator — DataFusion's SQL parser is conservative
        // about numeric literal formats.
        let sql = "SELECT repeat('x', 5000000) AS pad";
        let response = query_client
            .execute_sql(Request::new(ExecuteSqlRequest {
                workspace: Some(default_workspace()),
                sql: sql.to_string(),
            }))
            .await
            .expect("execute_sql >4MB response")
            .into_inner();

        // Prove the payload actually crossed the old 4 MB ceiling — without
        // this check the test could silently start passing for the wrong
        // reason if `repeat` ever returned a smaller value.
        assert!(
            response.arrow_ipc_stream.len() > 4 * 1024 * 1024,
            "regression payload was {} bytes; expected >4MB",
            response.arrow_ipc_stream.len()
        );

        let result = coral_client::decode_execute_sql_response(&response).expect("decode");
        assert_eq!(result.row_count(), 1);
    }

    /// an invalid column against a wide manifest must surface as a clean `tonic::Status`,
    /// not a transport-level `h2 protocol error`. Pre-fix, `DataFusion`'s
    /// "Valid fields are …" error enumerating ~600 field names
    /// overflowed HTTP/2 trailers; the CLI saw `PROTOCOL_ERROR` instead
    /// of the intended status.
    ///
    /// Also verifies the behavior change: wrapped `SchemaError` now maps
    /// to `Code::InvalidArgument` (via `find_root()`), not `Code::Internal`.
    #[tokio::test]
    async fn invalid_column_on_wide_manifest_returns_clean_status() {
        use std::fmt::Write as _;

        use crate::bootstrap::MAX_STATUS_DETAIL_BYTES;

        let temp = TempDir::new().expect("temp dir");
        let config_dir = temp.path().join("coral-config");
        let data_dir = temp.path().join("wide-data");
        std::fs::create_dir_all(&data_dir).expect("create data dir");
        // No rows needed — the test cares only about schema width.
        let location = format!("file://{}/", data_dir.display());

        let mut manifest = String::new();
        manifest.push_str("name: wide_demo\n");
        manifest.push_str("version: 0.1.0\n");
        manifest.push_str("dsl_version: 3\n");
        manifest.push_str("backend: jsonl\n");
        manifest.push_str("tables:\n");
        manifest.push_str("  - name: wide\n");
        manifest.push_str("    description: Wide fixture\n");
        manifest.push_str("    source:\n");
        writeln!(manifest, "      location: {location}").expect("write to String");
        manifest.push_str("      glob: \"**/*.jsonl\"\n");
        manifest.push_str("    columns:\n");
        for i in 0..600 {
            writeln!(manifest, "      - name: col_{i:04}\n        type: Utf8")
                .expect("write to String");
        }

        let layout = AppStateLayout::discover(Some(config_dir.clone())).expect("layout");
        let source_manager = SourceManager::new(
            ConfigStore::new(layout.clone()),
            SecretStore::new(layout.clone()),
            layout.clone(),
        );
        let query_manager = QueryManager::new(
            ConfigStore::new(layout.clone()),
            SecretStore::new(layout.clone()),
            QueryRuntimeContext { home_dir: None },
            layout,
            vec![Arc::new(NoopEngineExtensionsProvider)],
        );
        let running = start_server(source_manager, query_manager)
            .await
            .expect("start server");
        let channel = Endpoint::from_shared(running.endpoint_uri().to_string())
            .expect("endpoint")
            .http2_max_header_list_size(HTTP2_MAX_HEADER_LIST_SIZE)
            .connect()
            .await
            .expect("connect");
        let mut source_client = SourceServiceClient::new(channel.clone());
        let mut query_client = QueryServiceClient::new(channel)
            .max_decoding_message_size(QUERY_RESPONSE_MAX_MESSAGE_SIZE);

        source_client
            .import_source(Request::new(ImportSourceRequest {
                workspace: Some(default_workspace()),
                manifest_yaml: manifest,
                variables: Vec::new(),
                secrets: Vec::new(),
            }))
            .await
            .expect("import wide source");

        let status = query_client
            .execute_sql(Request::new(ExecuteSqlRequest {
                workspace: Some(default_workspace()),
                sql: "SELECT bogus_column FROM wide_demo.wide LIMIT 0".to_string(),
            }))
            .await
            .expect_err("expected gRPC Status, not a transport-level PROTOCOL_ERROR");

        assert_eq!(
            status.code(),
            tonic::Code::InvalidArgument,
            "wrapped schema error should map to InvalidArgument via find_root(); message = {:?}",
            status.message()
        );
        assert!(
            status.message().len() <= MAX_STATUS_DETAIL_BYTES,
            "status message was {} bytes; truncator should have clipped it to <= {MAX_STATUS_DETAIL_BYTES}",
            status.message().len(),
        );
        assert!(
            status.message().contains("No column named"),
            "missing expected schema-error head in: {:?}",
            status.message()
        );
    }
}
