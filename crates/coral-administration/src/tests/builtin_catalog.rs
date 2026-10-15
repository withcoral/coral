//! The Built-in Provider catalog update at boot notifies Universal Search.
use crate::{ProviderWorkflow, WorkspaceWorkflow};
use coral_catalog::{BuiltinCatalog, OperationHints};
use coral_provider::ProviderModule;
use coral_search::{CallPath, SearchIndex, SearchQuery, SearchRequest, UniversalSearchModule};
use coral_test_support::{TestDatabase, provision_user};
use coral_workspace::{
    AuthenticatedPrincipal, OrganizationRole, WorkspaceDisplayName, WorkspaceModule,
};

const ADDED: &str = "coral.admin.listThings";

/// Catalog B: this Coral version's Operations plus a read-only one it does
/// not perform, as a later Coral version could define.
fn catalog_b() -> BuiltinCatalog {
    BuiltinCatalog::with_unknown_operations([(
        "listThings",
        OperationHints::new(Some(true), None, None, None),
    )])
    .expect("catalog B prepares")
}

fn providers(
    db: &TestDatabase,
    workspaces: &WorkspaceModule,
    catalog: BuiltinCatalog,
) -> ProviderModule {
    ProviderModule::new(
        coral_outbound::DestinationPolicy::default(),
        db.database(),
        workspaces.clone(),
        catalog,
    )
}

// Catches: an update that commits the new catalog without telling search, so
// agents cannot find the added Operation until a reconcile pass runs.
#[tokio::test]
async fn an_update_makes_the_added_operation_searchable_and_describable() {
    // Search applies Operation access rules, so the schema includes Policy.
    let db = TestDatabase::new(&coral_test_support::combined_migrator(&[
        coral_workspace::migrator(),
        coral_catalog::migrator(),
        coral_provider::migrator(),
        coral_functions::migrator(),
        coral_policy::migrator(),
        coral_search::migrator(),
    ]))
    .await
    .unwrap();
    let database = db.database();
    coral_search::prepare_storage(&database).await.unwrap();
    let workspaces = WorkspaceModule::new(database.clone());
    let index = SearchIndex::new(database.clone(), std::time::Duration::from_mins(5));
    let search = UniversalSearchModule::new(database.clone(), workspaces.clone());
    let owner = provision_user(database.clone(), "user:owner", OrganizationRole::Member).await;
    let change = |at| coral_test_support::change(owner.clone(), at, '1');
    let running = BuiltinCatalog::running().expect("the built-in catalog prepares");
    let workspace = WorkspaceWorkflow::new(
        database.clone(),
        workspaces.clone(),
        providers(&db, &workspaces, running),
        index.clone(),
    )
    .create_workspace(&change(100), WorkspaceDisplayName::parse("Ops").unwrap())
    .await
    .unwrap();
    let principal = AuthenticatedPrincipal::user(owner.clone());
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
    index.settled().await;
    assert_eq!(finds_added().await, (false, false));

    // A process that serves catalog B boots.
    ProviderWorkflow::new(
        database.clone(),
        providers(&db, &workspaces, catalog_b()),
        index.clone(),
    )
    .update_builtin_catalogs(200)
    .await
    .unwrap();
    index.settled().await;

    assert_eq!(finds_added().await, (true, true));
}
