//! The Provider workflow notifies Universal Search after its commits.
//!
//! Each test awaits the index's writes, then observes it twice: through the
//! test-only read of what the index holds (membership and state, never counts
//! or scores), and through search.
use super::change;
use crate::ProviderWorkflow;
use coral_provider::{
    CredentialRoute, ManagementRequestId, OpenApiDocument, ProviderDisplayName, ProviderId,
    ProviderModule, ProviderNamespace, ProviderPublicationOutcome, ProviderRegenerationRequest,
    ProviderSetupId, WorkspaceProviderPublicationRequest,
};
use coral_search::test_fixtures::{IndexedGeneration, indexed_generations, indexed_tools};
use coral_search::{SearchIndex, SearchQuery, SearchRequest, UniversalSearchModule};
use coral_test_support::{TestDatabase, provision_user};
use coral_workspace::{
    AuthenticatedPrincipal, AuthenticatedUser, OrganizationRole, WorkspaceDisplayName, WorkspaceId,
    WorkspaceModule,
};

use super::configured_provider_fixtures::ConfiguredProviderFixtures as _;

const FORECAST: &str = r#"{"openapi":"3.0.3","info":{"title":"Weather","version":"1"},"servers":[{"url":"https://weather.example"}],"security":[],"paths":{"/forecast":{"get":{"operationId":"getForecast","summary":"Get forecast","responses":{"200":{"description":"Forecast"}}}}}}"#;
const CURRENT: &str = r#"{"openapi":"3.0.3","info":{"title":"Weather","version":"2"},"servers":[{"url":"https://weather.example"}],"security":[],"paths":{"/current":{"get":{"operationId":"getCurrentWeather","summary":"Get current weather","responses":{"200":{"description":"Current weather"}}}}}}"#;
const FORECAST_PATH: &str = "coral.providers.weather.getForecast";
const CURRENT_PATH: &str = "coral.providers.weather.getCurrentWeather";

pub(super) struct Fixture {
    db: TestDatabase,
    pub(super) owner: AuthenticatedUser,
    workspace: WorkspaceId,
    providers: ProviderModule,
    pub(super) workflow: ProviderWorkflow,
    pub(super) index: SearchIndex,
    search: UniversalSearchModule,
}

impl Fixture {
    async fn new() -> Self {
        Self::with_write_limit(None).await
    }

    /// A fixture whose index writes may run for `write_limit`, or for the
    /// index's default when `None`.
    pub(super) async fn with_write_limit(write_limit: Option<std::time::Duration>) -> Self {
        // Search applies Tool access rules, so the schema includes Policy.
        let db = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
        let database = db.database();
        let workspaces = WorkspaceModule::new(database.clone());
        let owner = provision_user(database.clone(), "user:owner", OrganizationRole::Member).await;
        let workspace = workspaces
            .create(
                &change(&owner),
                WorkspaceDisplayName::parse("Search").unwrap(),
            )
            .await
            .unwrap();
        let providers = ProviderModule::with_running_catalog(
            coral_outbound::DestinationPolicy::default(),
            database.clone(),
            workspaces.clone(),
        );
        let mut index = SearchIndex::new(database.clone(), std::time::Duration::from_mins(5));
        if let Some(limit) = write_limit {
            index = index.with_write_limit(limit);
        }
        Self {
            workflow: ProviderWorkflow::new(database.clone(), providers.clone(), index.clone()),
            search: UniversalSearchModule::new(database, workspaces),
            db,
            owner,
            workspace: workspace.id().clone(),
            providers,
            index,
        }
    }

    pub(super) async fn setup(&self, document: &str) -> ProviderSetupId {
        self.providers
            .prepare_openapi_fixture(
                &change(&self.owner),
                &self.workspace,
                OpenApiDocument::parse(document).unwrap(),
            )
            .await
            .unwrap()
            .id()
            .clone()
    }

    pub(super) fn creation(&self, setup: &ProviderSetupId) -> WorkspaceProviderPublicationRequest {
        WorkspaceProviderPublicationRequest::new(
            ManagementRequestId::parse(&coral_test_support::fixture_request_id()).unwrap(),
            self.workspace.clone(),
            setup.clone(),
            ProviderNamespace::parse("weather").unwrap(),
            CredentialRoute::None,
        )
    }

    fn regeneration(
        &self,
        provider: &ProviderId,
        setup: &ProviderSetupId,
    ) -> ProviderRegenerationRequest {
        ProviderRegenerationRequest::new(
            ManagementRequestId::parse(&coral_test_support::fixture_request_id()).unwrap(),
            self.workspace.clone(),
            provider.clone(),
            setup.clone(),
            1,
        )
    }

    /// Publishes through the workflow and waits for its notifications.
    async fn publish(&self, document: &str) -> ProviderId {
        let setup = self.setup(document).await;
        let publication = self
            .workflow
            .publish_workspace_provider(&change(&self.owner), self.creation(&setup))
            .await
            .unwrap();
        self.index.settled().await;
        publication.published_provider().unwrap().id().clone()
    }

    pub(super) async fn generations(&self) -> Vec<IndexedGeneration> {
        indexed_generations(&self.db.database(), &self.workspace)
            .await
            .unwrap()
    }

    /// The indexed Tools as `(Call Path, Provider name)`.
    pub(super) async fn tools(&self) -> Vec<(String, String)> {
        indexed_tools(&self.db.database(), &self.workspace)
            .await
            .unwrap()
            .into_iter()
            .map(|tool| (tool.call_path, tool.provider_name))
            .collect()
    }

    /// The Tool hits a query finds, as `(Call Path, Provider display name)`.
    pub(super) async fn found(&self, text: &str) -> Vec<(String, String)> {
        self.search
            .search(
                &AuthenticatedPrincipal::user(self.owner.clone()),
                &self.workspace,
                &SearchRequest::query(SearchQuery::parse(text).unwrap()),
            )
            .await
            .unwrap()
            .hits()
            .iter()
            .filter_map(|hit| {
                hit.tool().map(|tool| {
                    (
                        hit.call_path().as_str().to_string(),
                        tool.provider_display_name().to_string(),
                    )
                })
            })
            .collect()
    }
}

fn hit(path: &str, display_name: &str) -> Vec<(String, String)> {
    vec![(path.to_string(), display_name.to_string())]
}

#[tokio::test]
async fn publication_indexes_the_new_tools_and_regeneration_removes_the_superseded_rows() {
    let f = Fixture::new().await;

    let provider = f.publish(FORECAST).await;

    let [created] = f
        .generations()
        .await
        .try_into()
        .expect("one indexed Generation, with no reconcile pass");
    assert_eq!(created.state, "complete");
    assert_eq!(created.provider_id, provider.to_string());
    assert_eq!(
        f.tools().await,
        [(FORECAST_PATH.to_string(), "weather Weather".to_string())]
    );
    assert_eq!(f.found("forecast").await, hit(FORECAST_PATH, "Weather"));

    let setup = f.setup(CURRENT).await;
    let regeneration = f
        .workflow
        .regenerate_provider(&change(&f.owner), f.regeneration(&provider, &setup))
        .await
        .unwrap();
    assert_eq!(
        regeneration.outcome(),
        ProviderPublicationOutcome::Succeeded
    );
    f.index.settled().await;

    let [regenerated] = f
        .generations()
        .await
        .try_into()
        .expect("only the active Generation stays indexed");
    assert_ne!(regenerated.generation_id, created.generation_id);
    assert_eq!(regenerated.state, "complete");
    assert_eq!(
        f.tools().await,
        [(CURRENT_PATH.to_string(), "weather Weather".to_string())]
    );
    assert_eq!(
        f.found("current weather").await,
        hit(CURRENT_PATH, "Weather")
    );
    assert_eq!(f.found("forecast").await, []);
}

#[tokio::test]
async fn a_replay_a_reused_generation_or_a_failed_outcome_notifies_nothing() {
    let f = Fixture::new().await;
    // The test helper commits without notifying, so the index starts empty
    // and any notification would show.
    let setup = f.setup(FORECAST).await;
    let creation = f.creation(&setup);
    let provider = f
        .providers
        .publish_workspace_provider(&change(&f.owner), creation.clone())
        .await
        .unwrap()
        .published_provider()
        .unwrap()
        .id()
        .clone();

    let replayed = f
        .workflow
        .publish_workspace_provider(&change(&f.owner), creation)
        .await
        .unwrap();
    let unchanged = f.setup(FORECAST).await;
    let reused = f
        .workflow
        .regenerate_provider(&change(&f.owner), f.regeneration(&provider, &unchanged))
        .await
        .unwrap();
    let conflict = f.setup(CURRENT).await;
    let failed = f
        .workflow
        .publish_workspace_provider(&change(&f.owner), f.creation(&conflict))
        .await
        .unwrap();
    f.index.settled().await;

    assert_eq!(
        [replayed.outcome(), reused.outcome(), failed.outcome()],
        [
            ProviderPublicationOutcome::Succeeded,
            ProviderPublicationOutcome::Succeeded,
            ProviderPublicationOutcome::Failed,
        ],
        "a replay, a reuse of the active Generation, and a namespace conflict"
    );
    assert_eq!(f.generations().await, []);
    assert_eq!(f.tools().await, []);
    assert_eq!(f.found("forecast").await, []);
}

#[tokio::test]
async fn removal_removes_the_providers_rows() {
    let f = Fixture::new().await;
    let provider = f.publish(FORECAST).await;
    assert_eq!(f.found("forecast").await, hit(FORECAST_PATH, "Weather"));

    f.workflow
        .delete_provider(&change(&f.owner), &f.workspace, &provider)
        .await
        .unwrap();
    f.index.settled().await;

    assert_eq!(f.generations().await, []);
    assert_eq!(f.tools().await, []);
    assert_eq!(f.found("forecast").await, []);
}

#[tokio::test]
async fn rename_rewrites_the_rows_of_the_same_generation_under_the_new_name() {
    let f = Fixture::new().await;
    let provider = f.publish(FORECAST).await;
    let [indexed] = f.generations().await.try_into().expect("one Generation");

    f.workflow
        .rename_provider(
            &change(&f.owner),
            &f.workspace,
            &provider,
            ProviderDisplayName::parse("Forecasts").unwrap(),
        )
        .await
        .unwrap();
    f.index.settled().await;

    assert_eq!(
        f.generations().await,
        [IndexedGeneration {
            display_name: Some("Forecasts".into()),
            ..indexed
        }],
        "a rename rewrites the same Generation"
    );
    assert_eq!(
        f.tools().await,
        [(FORECAST_PATH.to_string(), "weather Forecasts".to_string())]
    );
    assert_eq!(f.found("forecast").await, hit(FORECAST_PATH, "Forecasts"));
}

#[tokio::test]
async fn a_failed_index_write_leaves_the_publication_committed() {
    let f = Fixture::new().await;
    let fault = "injected index fault";
    let sql = match f.db.database().system() {
        coral_db::DatabaseSystem::Sqlite => format!(
            "CREATE TRIGGER inject_index_fault BEFORE INSERT ON search_entities \
             WHEN NEW.kind = 'provider_tool' BEGIN SELECT RAISE(ABORT, '{fault}'); END;"
        ),
        coral_db::DatabaseSystem::Postgres => format!(
            "CREATE FUNCTION inject_index_fault() RETURNS trigger LANGUAGE plpgsql \
             AS $$ BEGIN RAISE EXCEPTION '{fault}'; END $$; \
             CREATE TRIGGER inject_index_fault BEFORE INSERT ON search_entities \
             FOR EACH ROW WHEN (NEW.kind = 'provider_tool') \
             EXECUTE FUNCTION inject_index_fault();"
        ),
    };
    f.db.execute_fixture_sql(&sql).await.unwrap();

    let provider = f.publish(FORECAST).await;

    f.providers
        .inspect_provider(&f.owner, &f.workspace, &provider)
        .await
        .expect("the publication committed");
    let [state] = f.generations().await.try_into().expect("one state row");
    assert_eq!(state.state, "failed");
    assert_eq!(f.tools().await, []);
}
