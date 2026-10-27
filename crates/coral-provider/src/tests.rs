use coral_outbound::DestinationPolicy;
use std::sync::Arc;

use coral_catalog::{ToolHints, ToolKey};
use coral_db::Database;
use coral_telemetry::TraceId;
use coral_test_support::TestDatabase;
pub(crate) use coral_test_support::provision_user as organization_user;
use coral_workspace::{
    AuthenticatedUser, ChangeContext, OrganizationRole, WorkspaceDisplayName, WorkspaceModule,
};

use crate::access::{CredentialRoute, ProviderCallRoot};
use crate::account::AccountModule;
use crate::{
    LifecycleAction, LifecycleOutcome, ManagementRequestId, OpenApiDocument, ProviderModule,
    WorkspaceId, WorkspaceProviderPublicationRequest,
};
use coral_credentials::CredentialVault;

pub(crate) const CREDENTIAL_FREE_OPENAPI: &str = r#"openapi: 3.1.0
info:
  title: Weather
  version: 1.0.0
servers:
  - url: https://weather.example
security: []
paths:
  /forecast:
    get:
      operationId: getForecast
      summary: Get forecast
      responses:
        "200":
          description: Forecast
"#;

#[tokio::test]
async fn disabled_provider_remains_a_valid_tool_policy_target() {
    let (fixture, workspaces, providers) = test_modules().await;
    let owner = organization_user(
        fixture.database(),
        "user:suspended-policy",
        OrganizationRole::Owner,
    )
    .await;
    let workspace = workspaces
        .create(
            &change_context(owner.clone(), 11, 'b'),
            WorkspaceDisplayName::parse("Suspended policy target").expect("display name"),
        )
        .await
        .expect("create Workspace");
    let context = change_context(owner, 12, 'c');
    let provider = publish_provider(
        &providers,
        &context,
        workspace.id(),
        CREDENTIAL_FREE_OPENAPI,
        "weather",
        CredentialRoute::None,
    )
    .await;
    providers
        .disable_provider(&context, workspace.id(), provider.id())
        .await
        .unwrap();
    let mut transaction = providers
        .database
        .begin()
        .await
        .expect("begin policy target resolution");
    let found = crate::tool_hints_and_method_in_transaction(
        &mut transaction,
        workspace.id(),
        provider.id(),
        &ToolKey::parse("getForecast").expect("Tool Key"),
    )
    .await
    .expect("suspension is not Provider-package corruption");
    transaction
        .rollback()
        .await
        .expect("finish policy target resolution");
    assert_eq!(
        found,
        Some((ToolHints::default(), Some(coral_catalog::HttpMethod::Get)))
    );
}

pub(crate) async fn publish_provider(
    providers: &ProviderModule,
    context: &ChangeContext,
    workspace: &WorkspaceId,
    document: &str,
    namespace: &str,
    route: CredentialRoute,
) -> crate::Provider {
    let setup = providers
        .prepare_openapi_fixture(
            context,
            workspace,
            OpenApiDocument::parse(document).expect("OpenAPI input"),
        )
        .await
        .expect("prepare Provider");
    name_oauth_application(providers, context, workspace, setup.id()).await;
    providers
        .publish_workspace_provider(
            context,
            WorkspaceProviderPublicationRequest::new(
                ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
                workspace.clone(),
                setup.id().clone(),
                crate::ProviderNamespace::parse(namespace).unwrap(),
                route,
            ),
        )
        .await
        .expect("publish Provider")
        .published_provider()
        .expect("Provider")
        .clone()
}

/// The public OAuth application that fixture Providers sign in through.
pub(crate) const REGISTERED_PUBLIC_CLIENT: &str = "registered-public-client";

/// Confirms an OAuth Setup with [`REGISTERED_PUBLIC_CLIENT`] when its
/// configuration names no application, as an Owner does in Reef. Publication
/// refuses an OAuth source that nobody could sign in to.
pub(crate) async fn name_oauth_application(
    providers: &ProviderModule,
    context: &ChangeContext,
    workspace: &WorkspaceId,
    setup: &crate::ProviderSetupId,
) {
    let review = providers
        .review_setup_configuration(context.actor(), workspace, setup)
        .await
        .expect("review fixture configuration");
    let Some(mut configuration) = review.confirmed else {
        return;
    };
    if AccountModule::names_oauth_application(configuration.authentication.as_ref()) {
        return;
    }
    let application = coral_catalog::OAuthClientPolicy {
        bootstrap: coral_catalog::OAuthClientBootstrap::Manual,
        client_id: REGISTERED_PUBLIC_CLIENT.into(),
        authentication: coral_catalog::OAuthClientAuthenticationMethod::None,
        pkce: coral_catalog::OAuthPkcePolicy::S256,
        authorization_parameters: std::collections::BTreeMap::new(),
    };
    configuration.authentication = configuration
        .authentication
        .map(|method| method.with_oauth_client(&application));
    let confirmed = providers
        .confirm_setup_configuration(context, workspace, setup, review.revision, configuration)
        .await
        .expect("confirm fixture application");
    assert!(confirmed.errors.is_empty(), "{:?}", confirmed.errors);
}

pub(crate) async fn retained_tool(
    providers: &ProviderModule,
    workspace_id: &coral_workspace::WorkspaceId,
    call_root: &ProviderCallRoot,
) -> crate::ResolvedInvocationTool {
    let mut transaction = providers
        .database
        .begin()
        .await
        .expect("begin Invocation resolution");
    let retained = crate::resolve_owner_tool_in_transaction(
        &mut transaction,
        workspace_id,
        &format!("{}.getForecast", call_root.as_str()),
    )
    .await
    .expect("resolve Invocation Tool")
    .expect("current callable Tool");
    transaction
        .rollback()
        .await
        .expect("finish Invocation resolution");
    retained
}

pub(crate) async fn verify_retained_tool(
    providers: &ProviderModule,
    workspace_id: &coral_workspace::WorkspaceId,
    retained: &crate::ResolvedInvocationTool,
) -> crate::InvocationToolVerification {
    let mut transaction = providers
        .database
        .begin()
        .await
        .expect("begin Invocation verification");
    let current = crate::verify_invocation_tool_in_transaction(
        &mut transaction,
        workspace_id,
        retained,
        coral_db::RowLock::Shared,
    )
    .await
    .expect("verify Invocation Tool");
    transaction
        .rollback()
        .await
        .expect("finish Invocation verification");
    current
}

pub(crate) async fn tool_authoring_paths(
    providers: &ProviderModule,
    workspace_id: &coral_workspace::WorkspaceId,
) -> Vec<String> {
    let mut transaction = providers
        .database
        .begin()
        .await
        .expect("begin discovery read");
    let catalogs =
        crate::provider_search_catalogs_in_transaction(&mut transaction, workspace_id, None)
            .await
            .expect("load current search targets");
    transaction.rollback().await.expect("finish discovery read");
    catalogs
        .iter()
        .flat_map(|catalog| {
            catalog.tools().iter().map(|tool| {
                format!(
                    "{}.{}",
                    catalog.access().call_root().as_str(),
                    tool.relative_path()
                )
            })
        })
        .collect()
}

pub(crate) fn publication_request(
    workspace_id: &coral_workspace::WorkspaceId,
    setup_id: &crate::ProviderSetupId,
    namespace: &str,
    route: CredentialRoute,
    request_id: &str,
) -> WorkspaceProviderPublicationRequest {
    WorkspaceProviderPublicationRequest::new(
        ManagementRequestId::parse(request_id).expect("request id"),
        workspace_id.clone(),
        setup_id.clone(),
        crate::ProviderNamespace::parse(namespace).expect("namespace"),
        route,
    )
}

/// What `listProviders` reads: every Provider `actor` reaches, with what each
/// offers it, from one read snapshot.
pub(crate) async fn published_providers(
    providers: &ProviderModule,
    actor: &coral_workspace::AuthenticatedPrincipal,
    workspace: &coral_workspace::WorkspaceId,
) -> Result<Vec<crate::PublishedProvider>, crate::Error> {
    let mut tx = providers.database.begin_read_snapshot().await?;
    let published = providers
        .published_providers_in_snapshot(&mut tx, actor, workspace, 20)
        .await;
    tx.rollback().await?;
    published
}

pub(crate) async fn test_modules() -> (TestDatabase, WorkspaceModule, ProviderModule) {
    let (fixture, database, workspaces) = test_foundation().await;
    let providers = ProviderModule::with_running_catalog(
        DestinationPolicy::default().with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
        database,
        workspaces.clone(),
    );
    (fixture, workspaces, providers)
}

pub(crate) async fn test_foundation() -> (TestDatabase, Arc<Database>, WorkspaceModule) {
    let fixture = TestDatabase::new(coral_db::migration_plan())
        .await
        .expect("create Workspace, Catalog, and Provider test database");
    let database = fixture.database();
    let workspaces = WorkspaceModule::new(database.clone());
    (fixture, database, workspaces)
}

/// An MCP URL source that sends no extra request headers.
pub(crate) fn mcp_url(url: impl Into<String>) -> crate::ProviderSetupSource {
    crate::ProviderSetupSource::McpUrl {
        url: url.into(),
        request_headers: std::collections::BTreeMap::new(),
    }
}

/// One test database with the Workspace, Provider and Account Modules over it.
pub(crate) struct Harness {
    pub(crate) fixture: TestDatabase,
    pub(crate) database: Arc<Database>,
    pub(crate) workspaces: WorkspaceModule,
    pub(crate) providers: ProviderModule,
    pub(crate) accounts: AccountModule,
    pub(crate) vault_dir: tempfile::TempDir,
}

impl Harness {
    pub(crate) async fn new() -> Self {
        let (fixture, database, workspaces) = test_foundation().await;
        let providers = ProviderModule::with_running_catalog(
            DestinationPolicy::default().with_loopback(coral_outbound::NetworkAccess::HttpAndHttps),
            database.clone(),
            workspaces.clone(),
        );
        let vault_dir = tempfile::tempdir().expect("create the test vault directory");
        let accounts = accounts_for(&providers, vault_dir.path());
        Self {
            fixture,
            database,
            workspaces,
            providers,
            accounts,
            vault_dir,
        }
    }

    /// A new Account Module over the same database and vault key, as after a
    /// process restart.
    pub(crate) fn restarted_accounts(&self) -> AccountModule {
        accounts_for(&self.providers, self.vault_dir.path())
    }
}

/// An Account Module over `providers` whose vault key lives in `dir`.
pub(crate) fn accounts_for(providers: &ProviderModule, dir: &std::path::Path) -> AccountModule {
    AccountModule::new(
        providers.clone(),
        CredentialVault::open(&dir.join("key")).expect("open the test vault"),
    )
}

pub(crate) fn change_context(
    actor: AuthenticatedUser,
    occurred_at_unix_nanos: i64,
    trace_digit: char,
) -> ChangeContext {
    ChangeContext::new(
        actor,
        occurred_at_unix_nanos,
        TraceId::parse(&trace_digit.to_string().repeat(32)).expect("trace id"),
    )
}

/// Gives an Organization Owner a designated Owner Membership through the
/// governance surface. Without one they have no authority inside the Workspace.
pub(crate) async fn join_as_owner(
    workspaces: &WorkspaceModule,
    user: &AuthenticatedUser,
    workspace: &WorkspaceId,
) {
    workspaces
        .assign_owner(
            &change_context(user.clone(), 1, 'f'),
            workspace,
            user.user_id(),
        )
        .await
        .expect("assign the Organization Owner as a designated Owner");
}

/// Reads a Setup as stored, for a test that checks what a refusal or a removal
/// left behind. A Setup answers only the User who started it, and these tests
/// look at one after that User lost authority.
pub(crate) async fn stored_setup(
    database: &Database,
    workspace: &WorkspaceId,
    setup: &crate::ProviderSetupId,
) -> crate::repository::LoadedSetup {
    crate::repository::setup(database, workspace, setup)
        .await
        .expect("read the Setup")
        .expect("the Setup exists")
}

/// Holds the first Publication that reaches `point` for good. The test then
/// cancels it, as a crash or a lost connection would.
pub(crate) struct HoldForever {
    point: crate::PublicationPoint,
    reached: std::sync::atomic::AtomicUsize,
    held: tokio::sync::Notify,
}

impl HoldForever {
    pub(crate) fn at(point: crate::PublicationPoint) -> Arc<Self> {
        Arc::new(Self {
            point,
            reached: std::sync::atomic::AtomicUsize::new(0),
            held: tokio::sync::Notify::new(),
        })
    }

    /// Waits until a Publication is held.
    pub(crate) async fn held(&self) {
        self.held.notified().await;
    }
}

#[async_trait::async_trait]
impl crate::PublicationObserver for HoldForever {
    async fn reached(&self, point: crate::PublicationPoint) {
        if point == self.point
            && self
                .reached
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                == 0
        {
            self.held.notify_one();
            std::future::pending::<()>().await;
        }
    }
}

/// One Lifecycle Event's `(action, outcome)`.
pub(crate) type EventKind = (LifecycleAction, LifecycleOutcome);
pub(crate) const SETUP_CREATED: EventKind =
    (LifecycleAction::SetupCreated, LifecycleOutcome::Accepted);
pub(crate) const SETUP_CONFIRMED: EventKind = (
    LifecycleAction::SetupConfigurationConfirmed,
    LifecycleOutcome::Accepted,
);
pub(crate) const PUBLISHED: EventKind = (
    LifecycleAction::PublicationCompleted,
    LifecycleOutcome::Succeeded,
);
pub(crate) const PUBLICATION_FAILED: EventKind = (
    LifecycleAction::PublicationCompleted,
    LifecycleOutcome::Failed,
);

/// Lifecycle Events as `(action, outcome)` pairs in a stable order.
///
/// Events that share a timestamp list in random-ID order, so a positional
/// comparison would be flaky.
pub(crate) fn lifecycle_actions<'a>(
    events: impl IntoIterator<Item = &'a crate::AdministrativeLifecycleEvent>,
) -> Vec<EventKind> {
    let mut actions = events
        .into_iter()
        .map(|event| (event.action(), event.outcome()))
        .collect::<Vec<_>>();
    actions.sort_by_key(|(action, outcome)| (action.as_storage_str(), outcome.as_storage_str()));
    actions
}

/// Asserts the Lifecycle Events a change added to `before`, in any order.
#[track_caller]
pub(crate) fn assert_lifecycle_delta(
    before: &[crate::AdministrativeLifecycleEvent],
    after: &[crate::AdministrativeLifecycleEvent],
    added: &[EventKind],
) {
    let mut remaining = after.iter().collect::<Vec<_>>();
    for event in before {
        let index = remaining
            .iter()
            .position(|kept| *kept == event)
            .expect("a change never removes an earlier Lifecycle Event");
        remaining.remove(index);
    }
    let mut expected = added.to_vec();
    expected.sort_by_key(|(action, outcome)| (action.as_storage_str(), outcome.as_storage_str()));
    assert_eq!(lifecycle_actions(remaining), expected);
}

coral_test_support::provider_setup_fixtures!(crate);
