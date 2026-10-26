use super::request::WorkspaceAuthentication;
use crate::auth::test_config::loopback_policy;
use crate::{CanonicalOauthUrl, WorkspaceMcpUrls};
use coral_auth::{PrincipalAuthentication, SessionTokenIssuer};
use coral_mcp::{McpModules, WorkspaceAuthenticator};
use coral_provider::ProviderModule;
use coral_workspace::{
    AgentDisplayName, AgentState, AuthenticatedUser, ChangeContext, OrganizationModule,
    OrganizationRole, PrincipalKind, TraceId, UserId, WorkspaceDisplayName, WorkspaceId,
    WorkspaceModule, WorkspaceRole,
};
use std::sync::Arc;

#[tokio::test]
async fn http_oauth_requests_use_current_user_authority_not_sessions_or_metadata() {
    let fixture = Fixture::new().await;
    let token = fixture
        .issuer
        .issue_access_token(
            "owner",
            "untrusted-client",
            &fixture.urls.resource(&fixture.workspace),
        )
        .unwrap();
    let missing = fixture.request(&fixture.workspace).send().await.unwrap();
    assert_eq!(missing.status(), 401);
    assert!(
        missing.headers()["www-authenticate"]
            .to_str()
            .unwrap()
            .contains(&fixture.urls.metadata_url(&fixture.workspace))
    );
    let accepted = fixture
        .request(&fixture.workspace)
        .bearer_auth(&token.access_token)
        .send()
        .await
        .unwrap();
    assert_eq!(accepted.status(), 200);
    let session = accepted.headers()["mcp-session-id"].clone();
    let principal = fixture
        .auth
        .authenticate(&fixture.workspace, Some(&token.access_token))
        .await
        .unwrap();
    assert_eq!(principal.principal().id().as_str(), "owner");
    assert_eq!(principal.principal().kind(), PrincipalKind::User);
    fixture
        .assert_task_principal(&token.access_token, &session, &principal)
        .await;
    let other_token = fixture
        .issuer
        .issue_access_token(
            "other",
            "same-client",
            &fixture.urls.resource(&fixture.workspace),
        )
        .unwrap();
    let stolen = reqwest::Client::new()
        .post(fixture.url(&fixture.workspace))
        .header("Accept", "application/json, text/event-stream")
        .header("mcp-session-id", session.clone())
        .bearer_auth(&other_token.access_token)
        .json(&serde_json::json!({"jsonrpc":"2.0", "id":2,"method":"tools/list"}))
        .send()
        .await
        .unwrap();
    assert_eq!(stolen.status(), 404);
    assert_eq!(
        fixture
            .request(&fixture.other_workspace)
            .bearer_auth(&token.access_token)
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    fixture
        .organization
        .set_enabled(&fixture.other, fixture.owner.actor().user_id(), 1, false)
        .await
        .unwrap();
    let denied = fixture
        .request(&fixture.workspace)
        .bearer_auth(&token.access_token)
        .header("mcp-session-id", session)
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), 401);
    assert!(!denied.text().await.unwrap().contains(&token.access_token));
    let metadata = reqwest::get(format!(
        "http://{}{}",
        fixture.server.local_addr(),
        fixture.urls.metadata_path(&fixture.workspace)
    ))
    .await
    .unwrap();
    assert_eq!(metadata.status(), 200);
    assert_eq!(
        metadata
            .json::<serde_json::Value>()
            .await
            .unwrap()
            .get("resource")
            .and_then(serde_json::Value::as_str),
        Some(fixture.urls.resource(&fixture.workspace).as_str())
    );
    fixture.server.shutdown().await.unwrap();
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one HTTP contract follows the same credential, identity and session through lifecycle changes"
)]
async fn http_agent_credentials_fix_workspace_identity_and_recheck_every_lifecycle_change() {
    let fixture = Fixture::new().await;
    let issued = fixture
        .workspaces
        .create_agent(
            &fixture.owner,
            &fixture.workspace,
            AgentDisplayName::parse("Worker").unwrap(),
            None,
        )
        .await
        .unwrap();
    let credential = issued.credential.expose();
    let actor = fixture
        .auth
        .authenticate(&fixture.workspace, Some(credential))
        .await
        .unwrap();
    assert_eq!(actor.principal(), issued.agent.principal());
    assert_eq!(actor.agent_workspace_id(), Some(&fixture.workspace));
    let accepted = fixture
        .request(&fixture.workspace)
        .bearer_auth(credential)
        .send()
        .await
        .unwrap();
    assert_eq!(accepted.status(), 200);
    let session = accepted.headers()["mcp-session-id"].clone();
    fixture
        .assert_task_principal(credential, &session, &actor)
        .await;
    let before = fixture
        .workspaces
        .list_lifecycle_events(fixture.owner.actor(), &fixture.workspace)
        .await
        .unwrap()
        .len();
    assert_eq!(
        fixture
            .request(&fixture.other_workspace)
            .bearer_auth(credential)
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    // Signed Agent-kind JWT rejection is covered at the coral-auth Interface.
    for bad in ["coral_agent_invalid", "not-a-token"] {
        let rejected = fixture
            .request(&fixture.workspace)
            .bearer_auth(bad)
            .header("mcp-session-id", session.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(rejected.status(), 401);
        assert!(!rejected.text().await.unwrap().contains(bad));
    }
    assert_eq!(
        fixture
            .workspaces
            .list_lifecycle_events(fixture.owner.actor(), &fixture.workspace)
            .await
            .unwrap()
            .len(),
        before
    );
    // Disabling the provisioning human does not disable the Workspace's Agent.
    fixture
        .organization
        .set_enabled(&fixture.other, fixture.owner.actor().user_id(), 1, false)
        .await
        .unwrap();
    assert_eq!(
        fixture
            .request(&fixture.workspace)
            .bearer_auth(credential)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let rotated = fixture
        .workspaces
        .rotate_agent_credential(&fixture.other, &fixture.workspace, issued.registration.id())
        .await
        .unwrap();
    assert_eq!(
        fixture
            .request(&fixture.workspace)
            .bearer_auth(credential)
            .header("mcp-session-id", session.clone())
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        fixture
            .auth
            .authenticate(&fixture.workspace, Some(rotated.credential.expose()))
            .await
            .unwrap()
            .principal(),
        actor.principal()
    );
    fixture
        .workspaces
        .set_agent_state(
            &fixture.other,
            &fixture.workspace,
            actor.principal().id(),
            AgentState::Disabled,
        )
        .await
        .unwrap();
    assert_eq!(
        fixture
            .request(&fixture.workspace)
            .bearer_auth(rotated.credential.expose())
            .header("mcp-session-id", session.clone())
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    fixture
        .workspaces
        .set_agent_state(
            &fixture.other,
            &fixture.workspace,
            actor.principal().id(),
            AgentState::Enabled,
        )
        .await
        .unwrap();
    for (enabled, expiry) in [(false, None), (true, Some(1))] {
        fixture
            .workspaces
            .update_agent_registration(
                &fixture.other,
                &fixture.workspace,
                issued.registration.id(),
                enabled,
                expiry,
            )
            .await
            .unwrap();
        assert_eq!(
            fixture
                .request(&fixture.workspace)
                .bearer_auth(rotated.credential.expose())
                .header("mcp-session-id", session.clone())
                .send()
                .await
                .unwrap()
                .status(),
            401
        );
    }
    assert_eq!(
        fixture
            .workspaces
            .get_agent_registration(
                fixture.other.actor(),
                &fixture.workspace,
                actor.principal().id()
            )
            .await
            .unwrap()
            .id(),
        issued.registration.id()
    );
    fixture.server.shutdown().await.unwrap();
}

struct Fixture {
    _database: coral_test_support::TestDatabase,
    organization: OrganizationModule,
    workspaces: WorkspaceModule,
    tasks: coral_task::TaskModule,
    owner: ChangeContext,
    other: ChangeContext,
    workspace: WorkspaceId,
    other_workspace: WorkspaceId,
    urls: WorkspaceMcpUrls,
    issuer: SessionTokenIssuer,
    auth: WorkspaceAuthentication,
    server: coral_mcp::RunningMcpHttpServer,
}
impl Fixture {
    async fn new() -> Self {
        let fixture = coral_test_support::TestDatabase::new(coral_db::migration_plan())
            .await
            .unwrap();
        let database = fixture.database();
        let organization = OrganizationModule::new(database.clone());
        let workspaces = WorkspaceModule::new(database.clone());
        let owner = change("owner", OrganizationRole::Owner);
        let other = change("other", OrganizationRole::Owner);
        organization.provision_login(&owner).await.unwrap();
        organization.provision_login(&other).await.unwrap();
        let workspace = workspaces
            .create(&owner, WorkspaceDisplayName::parse("Workspace").unwrap())
            .await
            .unwrap()
            .id()
            .clone();
        // A second designated Owner administers the Workspace once the first is disabled.
        workspaces
            .set_membership(&owner, &workspace, other.actor(), WorkspaceRole::Owner)
            .await
            .unwrap();
        let other_workspace = workspaces
            .create(&owner, WorkspaceDisplayName::parse("Other").unwrap())
            .await
            .unwrap()
            .id()
            .clone();
        let urls =
            WorkspaceMcpUrls::new(CanonicalOauthUrl::parse("https://coral.example").unwrap())
                .unwrap();
        let issuer = SessionTokenIssuer::new(
            Some("https://auth.example"),
            coral_test_support::p256_pkcs8(),
            std::time::Duration::from_mins(1),
        )
        .unwrap();
        let auth = WorkspaceAuthentication::new(
            PrincipalAuthentication::new(&issuer, organization.clone(), workspaces.clone()),
            urls.clone(),
            "https://auth.example".into(),
        );
        let tasks = coral_task::TaskModule::new(database.clone(), workspaces.clone());
        let runs = coral_run::RunModule::new(
            loopback_policy(),
            database.clone(),
            workspaces.clone(),
            ProviderModule::new(
                loopback_policy(),
                database.clone(),
                workspaces.clone(),
                coral_catalog::BuiltinCatalog::running().expect("the built-in catalog prepares"),
            ),
            coral_run::RuntimeLimits::default(),
            coral_run::CoralInstanceId::generate(),
            coral_run::RunInbox::table(database.clone()),
        )
        .unwrap();
        let commands = coral_task::TaskCommands::new(
            tasks.clone(),
            coral_functions::SavedFunctionModule::new(
                database.clone(),
                workspaces.clone(),
                coral_run::IsolateLimits::default(),
            )
            .unwrap(),
            coral_search::UniversalSearchModule::new(database.clone(), workspaces.clone()),
            coral_search::SearchIndex::inert(database.clone()),
            runs.clone(),
        );
        let modules = McpModules::new(
            workspaces.clone(),
            tasks.clone(),
            commands,
            runs,
            coral_mcp::HumanInterfaceUrl::parse("https://reef.example.test").unwrap(),
        );
        let server = coral_mcp::start_authenticated(
            "127.0.0.1:0".parse().unwrap(),
            vec!["127.0.0.1".into()],
            modules,
            Arc::new(auth.clone()),
            crate::ListenerReadiness::from_database(fixture.database()).probe(),
        )
        .await
        .unwrap();
        Self {
            _database: fixture,
            organization,
            workspaces,
            tasks,
            owner,
            other,
            workspace,
            other_workspace,
            urls,
            issuer,
            auth,
            server,
        }
    }
    async fn assert_task_principal(
        &self,
        credential: &str,
        session: &reqwest::header::HeaderValue,
        actor: &coral_workspace::AuthenticatedPrincipal,
    ) {
        let client = reqwest::Client::new();
        let request = || {
            client
                .post(self.url(&self.workspace))
                .bearer_auth(credential)
                .header("Accept", "application/json, text/event-stream")
                .header("mcp-session-id", session.clone())
        };
        let response = request()
            .json(&serde_json::json!({"jsonrpc":"2.0", "method":"notifications/initialized"}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 202);
        let response = request().json(&serde_json::json!({"jsonrpc":"2.0", "id":2, "method":"tools/call", "params":{"name":"start_task", "arguments":{"intent":"HTTP Principal attribution"}}})).send().await.unwrap();
        assert_eq!(response.status(), 200);
        let text = response.text().await.unwrap();
        let message: serde_json::Value = serde_json::from_str(&text)
            .or_else(|_error| {
                serde_json::from_str(
                    text.lines()
                        .filter_map(|line| line.strip_prefix("data: "))
                        .find(|data| !data.is_empty())
                        .expect("SSE response"),
                )
            })
            .unwrap();
        let id = coral_task::TaskId::parse(
            message
                .pointer("/result/structuredContent/task_id")
                .and_then(serde_json::Value::as_str)
                .expect("Task result"),
        )
        .unwrap();
        // A User inspects their own Task; an Owner may inspect Workspace Agent
        // work. Agent access to a Task identifier is attribution, not browsing.
        let inspector = coral_workspace::AuthenticatedPrincipal::user(self.owner.actor().clone());
        let task = self
            .tasks
            .get(&inspector, &self.workspace, &id)
            .await
            .unwrap();
        assert_eq!(task.principal(), actor.principal());
    }

    fn url(&self, workspace: &WorkspaceId) -> String {
        format!(
            "http://{}/workspaces/{workspace}/mcp",
            self.server.local_addr()
        )
    }
    fn request(&self, workspace: &WorkspaceId) -> reqwest::RequestBuilder {
        reqwest::Client::new().post(self.url(workspace)).header("Accept", "application/json, text/event-stream").json(&serde_json::json!({
            "jsonrpc":"2.0", "id":1, "method":"initialize", "params":{"protocolVersion":"2025-03-26", "capabilities":{}, "clientInfo":{"name":"organization_owner", "version":"1"}}
        }))
    }
}
fn change(id: &str, role: OrganizationRole) -> ChangeContext {
    ChangeContext::new(
        AuthenticatedUser::organization_member(UserId::parse(id).unwrap(), role),
        1,
        TraceId::parse(&"a".repeat(32)).unwrap(),
    )
}
