use coral_client::{AppClient, ClientBuilder, QueryClient, SourceClient};

/// Concrete CLI service bundle backed by the generated gRPC clients.
pub struct CliServices {
    /// Source-management client used by CLI commands.
    pub source_client: SourceClient,
    /// Query client used by CLI commands.
    pub query_client: QueryClient,
    app: Option<AppClient>,
}

impl CliServices {
    /// Connects the CLI to the default local Coral app.
    pub async fn connect_local() -> Result<Self, anyhow::Error> {
        let app = ClientBuilder::new().build().await?;
        Ok(Self::from_app(app))
    }

    /// Builds a CLI service bundle from an owned app client.
    pub fn from_app(app: AppClient) -> Self {
        Self {
            source_client: app.source_client(),
            query_client: app.query_client(),
            app: Some(app),
        }
    }

    /// Builds a CLI service bundle from already-connected generated gRPC clients.
    pub fn from_clients(source_client: SourceClient, query_client: QueryClient) -> Self {
        Self {
            source_client,
            query_client,
            app: None,
        }
    }

    /// Runs the MCP stdio server when the services were built from an app client.
    pub async fn serve_mcp_stdio(self) -> Result<(), anyhow::Error> {
        let app = self
            .app
            .ok_or_else(|| anyhow::anyhow!("mcp-stdio requires an AppClient-backed service bundle"))?;
        coral_mcp::run_stdio_with_client(app).await?;
        Ok(())
    }
}
