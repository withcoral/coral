//! Builds and runs the Coral gRPC server.

use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Mutex;

use coral_api::v1::query_service_server::QueryServiceServer;
use coral_api::v1::source_service_server::SourceServiceServer;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use super::env::AppEnvironment;
use super::error::AppError;
use crate::query::manager::QueryManager;
use crate::query::service::QueryService;
use crate::sources::manager::SourceManager;
use crate::sources::service::SourceService;
use crate::state::{AppStateLayout, ConfigStore, SecretStore};
use crate::workspaces::WorkspaceManager;

/// Server-side bootstrap configuration for the Coral server.
#[derive(Debug, Clone, Default)]
pub(crate) struct ServerConfig {
    config_dir: Option<PathBuf>,
}

impl ServerConfig {
    #[must_use]
    /// Creates the default local server configuration.
    pub(crate) fn new() -> Self {
        Self { config_dir: None }
    }

    #[must_use]
    /// Overrides the Coral config directory used by the local server.
    pub(crate) fn with_config_dir(mut self, config_dir: impl Into<PathBuf>) -> Self {
        self.config_dir = Some(config_dir.into());
        self
    }
}

/// Builder for the Coral server runtime.
#[derive(Debug, Clone, Default)]
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
    let workspace_manager = WorkspaceManager::new();
    let source_service = SourceService::new(
        source_manager,
        query_manager.clone(),
        workspace_manager.clone(),
    );
    let query_service = QueryService::new(query_manager, workspace_manager);
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let endpoint_uri = format!("http://{}", listener.local_addr()?);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let task = tokio::spawn(async move {
        Server::builder()
            .add_service(SourceServiceServer::new(source_service))
            .add_service(QueryServiceServer::new(query_service))
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
    use coral_api::v1::query_service_client::QueryServiceClient;
    use coral_api::v1::source_service_client::SourceServiceClient;
    use coral_api::v1::{ExecuteSqlRequest, ImportSourceRequest, Workspace};
    use coral_engine::QueryRuntimeContext;
    use tempfile::TempDir;
    use tonic::Request;
    use tonic::transport::Endpoint;

    use super::start_server;
    use crate::query::manager::QueryManager;
    use crate::sources::manager::SourceManager;
    use crate::state::{AppStateLayout, ConfigStore, SecretStore};
    use crate::workspaces::WorkspaceManager;

    fn default_workspace() -> Workspace {
        WorkspaceManager::new().default_workspace()
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
            QueryRuntimeContext::new(Some(fake_home.clone())),
            layout,
        );
        let running = start_server(source_manager, query_manager)
            .await
            .expect("start server");
        let channel = Endpoint::from_shared(running.endpoint_uri().to_string())
            .expect("endpoint")
            .connect()
            .await
            .expect("connect");
        let mut source_client = SourceServiceClient::new(channel.clone());
        let mut query_client = QueryServiceClient::new(channel);

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
}
