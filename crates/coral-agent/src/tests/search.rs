use coral_catalog::OpenApiDocument;
use coral_provider::{CredentialRoute, ProviderModule};

use super::*;
use configured_provider_fixtures::ConfiguredProviderFixtures as _;

coral_test_support::provider_setup_fixtures!(coral_provider);

const ACME: &str = r#"{
  "openapi": "3.1.0",
  "info": { "title": "Acme", "version": "1.0.0" },
  "servers": [{ "url": "https://api.acme.example" }],
  "security": [],
  "paths": {
    "/invoices": {
      "get": {
        "operationId": "invoices/list",
        "summary": "List invoices",
        "responses": { "200": { "description": "OK" } }
      }
    }
  }
}"#;

// Catches: a hit written with other field names than the model reads.
#[tokio::test]
async fn search_lists_the_tools_a_program_can_call() {
    let lead = Lead::new().await;
    lead.add_provider().await;
    let (tools, task_id) = (lead.tools(), lead.task().await);
    let search = tool_call("call_1", "search", &json!({"limit": 50}));
    let result = lead.first_result(&*tools, &task_id, search).await;

    let hit = json!({
        "call_path": "coral.providers.acme.invoices.list",
        "signature_line": "list(): Promise<{ data: unknown }>",
        "complete": true,
        "description_snippet": "List invoices",
    });
    assert_eq!(result, json!({"hits": [hit]}));
}

impl Lead {
    /// Acme, in the lead's Workspace.
    async fn add_provider(&self) {
        let (database, workspaces) = (self.database.clone(), self.workspaces.clone());
        let providers = ProviderModule::with_running_catalog(POLICY, database, workspaces);
        let owner = self.actor.authenticated_user().unwrap().clone();
        let published = change(owner, 4, '4');
        let acme = OpenApiDocument::parse(ACME).unwrap();
        let acme = providers.publish_openapi_fixture(
            &published,
            &self.workspace_id,
            acme,
            "acme",
            CredentialRoute::None,
        );
        acme.await.unwrap();
    }

    /// Starts a Turn whose model makes `call` and then answers, and returns
    /// the call's result as JSON.
    async fn first_result(
        &self,
        tools: &dyn McpInterface,
        task_id: &TaskId,
        call: ToolCall,
    ) -> Value {
        Mock::given(any())
            .respond_with(tool_calls(std::slice::from_ref(&call)))
            .up_to_n_times(1)
            .mount(&self.model)
            .await;
        self.model_answers(reply("Done.")).await;
        let (sub_agent_id, _waited) = self.turn(tools, task_id).await;
        let thread = self.thread(&sub_agent_id).await;
        match thread.get(2).map(|results| results.content.as_slice()) {
            Some([Block::ToolResult { content, .. }]) => serde_json::from_str(content).unwrap(),
            _ => panic!("the third message is the call's result: {thread:?}"),
        }
    }
}
