//! The Built-in Provider install and catalog update at boot: each Workspace in
//! its own transaction, and Universal Search told after each commit.
use crate::{ProviderWorkflow, WorkspaceWorkflow};
use coral_catalog::{BuiltinCatalog, ToolHints};
use coral_provider::{Provider, ProviderModule};
use coral_search::{CallPath, SearchIndex, SearchQuery, SearchRequest, UniversalSearchModule};
use coral_test_support::{TestDatabase, provision_user};
use coral_workspace::{
    AuthenticatedPrincipal, AuthenticatedUser, OrganizationRole, Workspace, WorkspaceDisplayName,
    WorkspaceModule,
};

const ADDED: &str = "coral.workspace.listThings";

/// Catalog B: this Coral version's Tools plus a read-only one it does
/// not perform, as a later Coral version could define.
fn catalog_b() -> BuiltinCatalog {
    BuiltinCatalog::with_unknown_tools([(
        "listThings",
        ToolHints::new(Some(true), None, None, None),
    )])
    .expect("catalog B prepares")
}

/// One database with search storage, and an Owner who creates Workspaces with
/// this Coral version's catalog.
struct Fixture {
    db: TestDatabase,
    workspaces: WorkspaceModule,
    index: SearchIndex,
    owner: AuthenticatedUser,
}

impl Fixture {
    async fn new() -> Self {
        // Search applies Tool access rules, so the schema includes Policy.
        let db = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
        let database = db.database();
        let workspaces = WorkspaceModule::new(database.clone());
        let index = SearchIndex::new(database.clone(), std::time::Duration::from_mins(5));
        let owner = provision_user(database, "user:owner", OrganizationRole::Member).await;
        Self {
            db,
            workspaces,
            index,
            owner,
        }
    }

    fn providers(&self, catalog: BuiltinCatalog) -> ProviderModule {
        ProviderModule::new(
            coral_outbound::DestinationPolicy::default(),
            self.db.database(),
            self.workspaces.clone(),
            catalog,
        )
    }

    async fn create_workspace(&self, name: &str, at: i64) -> Workspace {
        WorkspaceWorkflow::new(
            self.db.database(),
            self.workspaces.clone(),
            self.providers(BuiltinCatalog::running().expect("the built-in catalog prepares")),
            self.index.clone(),
        )
        .create_workspace(
            &coral_test_support::change(self.owner.clone(), at, '1'),
            WorkspaceDisplayName::parse(name).unwrap(),
        )
        .await
        .unwrap()
    }

    /// A process that serves `catalog` boots at `at`.
    async fn boot(&self, catalog: BuiltinCatalog, at: i64) {
        let workflow = ProviderWorkflow::new(
            self.db.database(),
            self.providers(catalog),
            self.index.clone(),
        );
        workflow.install_missing_builtins(at).await.unwrap();
        workflow.update_builtin_catalogs(at).await.unwrap();
    }

    async fn builtin(&self, workspace: &Workspace) -> Provider {
        self.providers(catalog_b())
            .inspect_providers(&self.owner, workspace.id())
            .await
            .unwrap()
            .into_iter()
            .find(|provider| provider.call_root().as_str() == "coral.workspace")
            .expect("the Workspace has its Built-in Provider")
    }

    /// Whether the Workspace's Built-in Provider serves catalog B's `listThings`.
    async fn serves_list_things(&self, workspace: &Workspace) -> bool {
        let builtin = self.builtin(workspace).await;
        self.providers(catalog_b())
            .inspect_catalog_page(&self.owner, workspace.id(), builtin.id(), None, 100)
            .await
            .unwrap()
            .iter()
            .any(|tool| tool.key.as_str() == "listThings")
    }

    async fn events(&self, workspace: &Workspace) -> usize {
        self.providers(catalog_b())
            .list_lifecycle_events(&self.owner, workspace.id())
            .await
            .unwrap()
            .len()
    }
}

// Catches: an update that commits the new catalog without telling search, so
// agents cannot find the added Tool until a reconcile pass runs.
#[tokio::test]
async fn an_update_makes_the_added_tool_searchable_and_describable() {
    let fixture = Fixture::new().await;
    let workspace = fixture.create_workspace("Ops", 100).await;
    let search = UniversalSearchModule::new(fixture.db.database(), fixture.workspaces.clone());
    let principal = AuthenticatedPrincipal::user(fixture.owner.clone());
    let finds_added = || async {
        let found = search
            .search(
                &principal,
                workspace.id(),
                &SearchRequest::query(SearchQuery::parse("list things").unwrap()),
            )
            .await
            .unwrap()
            .hits()
            .iter()
            .any(|hit| hit.call_path().as_str() == ADDED);
        let described = search
            .describe(
                &principal,
                workspace.id(),
                &CallPath::parse(ADDED).unwrap(),
                &[],
            )
            .await
            .is_ok();
        (found, described)
    };
    fixture.index.settled().await;
    assert_eq!(finds_added().await, (false, false));

    fixture.boot(catalog_b(), 200).await;
    fixture.index.settled().await;

    assert_eq!(finds_added().await, (true, true));
}

// Catches: an install that commits without telling search, so agents in a
// Workspace from before the Built-in Provider cannot find its Tools until
// a reconcile pass runs.
#[tokio::test]
async fn an_install_makes_the_builtin_tools_searchable() {
    let fixture = Fixture::new().await;
    // Creating a Workspace without the workflow installs no Built-in Provider,
    // as in a database from before Coral had it.
    let workspace = fixture
        .workspaces
        .create(
            &coral_test_support::change(fixture.owner.clone(), 100, '1'),
            WorkspaceDisplayName::parse("Earlier").unwrap(),
        )
        .await
        .unwrap();
    let search = UniversalSearchModule::new(fixture.db.database(), fixture.workspaces.clone());
    let principal = AuthenticatedPrincipal::user(fixture.owner.clone());
    let finds_add_provider = || async {
        search
            .search(
                &principal,
                workspace.id(),
                &SearchRequest::query(SearchQuery::parse("add provider").unwrap()),
            )
            .await
            .unwrap()
            .hits()
            .iter()
            .any(|hit| hit.call_path().as_str() == "coral.workspace.addProvider")
    };
    fixture.index.settled().await;
    assert!(!finds_add_provider().await);

    fixture.boot(BuiltinCatalog::running().unwrap(), 200).await;
    fixture.index.settled().await;

    assert!(finds_add_provider().await);
}

// Catches: one Built-in Provider whose update fails stopping the others at
// boot, or leaving part of its update behind instead of waiting for the next boot.
#[tokio::test]
async fn a_failed_update_waits_for_the_next_boot_and_the_others_still_update() {
    let fixture = Fixture::new().await;
    let healthy = fixture.create_workspace("Healthy", 100).await;
    let broken = fixture.create_workspace("Broken", 101).await;
    let broken_builtin = fixture.builtin(&broken).await;
    let before = fixture.events(&broken).await;
    // A display name longer than 256 bytes fails to load, so its update fails.
    let set_name = |name: String| {
        let sql = format!(
            "UPDATE provider_records SET display_name = '{name}' WHERE id = '{}'",
            broken_builtin.id()
        );
        let db = &fixture.db;
        async move { db.execute_fixture_sql(&sql).await.unwrap() }
    };
    set_name("x".repeat(300)).await;

    fixture.boot(catalog_b(), 200).await;

    assert!(fixture.serves_list_things(&healthy).await);
    set_name("Coral".to_string()).await;
    assert!(!fixture.serves_list_things(&broken).await);
    assert_eq!(
        fixture.events(&broken).await,
        before,
        "the failed update wrote nothing"
    );

    fixture.boot(catalog_b(), 300).await;

    assert!(
        fixture.serves_list_things(&broken).await,
        "the next boot updates it"
    );
}
