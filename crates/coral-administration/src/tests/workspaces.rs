//! The Workspace workflow creates each Workspace with its Built-in Provider.
use super::change;
use super::repository::{EventFault, fixture};
use crate::WorkspaceWorkflow;
use coral_provider::{LifecycleAction, LifecycleActor, ProviderId, ProviderModule};
use coral_search::SearchIndex;
use coral_search::test_fixtures::indexed_operations;
use coral_test_support::{TestDatabase, provision_user};
use coral_workspace::{
    AuthenticatedUser, AuthoritySource, OrganizationRole, WorkspaceDisplayName, WorkspaceModule,
};

struct Fixture {
    db: TestDatabase,
    creator: AuthenticatedUser,
    workspaces: WorkspaceModule,
    providers: ProviderModule,
    workflow: WorkspaceWorkflow,
    index: SearchIndex,
}

impl Fixture {
    async fn new() -> Self {
        let db = fixture().await;
        let database = db.database();
        let workspaces = WorkspaceModule::new(database.clone());
        let providers = ProviderModule::new(
            coral_outbound::DestinationPolicy::default(),
            database.clone(),
            workspaces.clone(),
        );
        let index = SearchIndex::new(database.clone(), std::time::Duration::from_mins(5));
        let creator =
            provision_user(database.clone(), "user:creator", OrganizationRole::Member).await;
        Self {
            workflow: WorkspaceWorkflow::new(
                database,
                workspaces.clone(),
                providers.clone(),
                index.clone(),
            ),
            db,
            creator,
            workspaces,
            providers,
            index,
        }
    }
}

#[tokio::test]
async fn creating_a_workspace_creates_its_builtin_provider_with_the_creator_as_actor() {
    let f = Fixture::new().await;
    let context = change(&f.creator);
    let workspace = f
        .workflow
        .create_workspace(&context, WorkspaceDisplayName::parse("Ops").unwrap())
        .await
        .unwrap();
    f.index.settled().await;

    let events = f
        .providers
        .list_lifecycle_events(&f.creator, workspace.id())
        .await
        .unwrap();
    let [event] = events.as_slice() else {
        panic!("one Provider event records the Built-in Provider: {events:?}");
    };
    assert_eq!(event.action(), LifecycleAction::BuiltinProviderInstalled);
    for installed in ["provider.active_generation", "catalog"] {
        assert!(
            event
                .changed_field_paths()
                .iter()
                .any(|path| path == installed),
            "the install event records {installed}: {event:?}"
        );
    }
    assert_eq!(
        event.actor(),
        &LifecycleActor::User {
            user_id: f.creator.user_id().clone(),
            authority_source: AuthoritySource::WorkspaceMembership,
        }
    );
    let provider = f
        .providers
        .inspect_provider(
            &f.creator,
            workspace.id(),
            &ProviderId::parse(event.target_id()).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(provider.call_root().as_str(), "coral.workspace");

    // The commit notified search; no reconcile pass ran.
    let indexed = indexed_operations(&f.db.database(), workspace.id())
        .await
        .unwrap();
    assert_eq!(
        indexed
            .iter()
            .map(|operation| (
                operation.call_path.as_str(),
                operation.provider_name.as_str()
            ))
            .collect::<Vec<_>>(),
        [("coral.workspace.createProvider", "workspace Coral admin")]
    );
}

#[tokio::test]
async fn a_failure_in_either_part_creates_neither_workspace_nor_provider() {
    let f = Fixture::new().await;
    for fault in [
        EventFault::WorkspaceCreated,
        EventFault::BuiltinProviderCreated,
    ] {
        let error = f
            .workflow
            .create_workspace(
                &fault.context(&f.creator),
                WorkspaceDisplayName::parse("Rolled back").unwrap(),
            )
            .await
            .unwrap_err();
        fault.assert_error(&error);
        assert!(
            f.workspaces.list(&f.creator).await.unwrap().is_empty(),
            "a failed creation leaves no Workspace"
        );
    }
    let database = f.db.database();
    let mut transaction = database.begin_read_snapshot().await.unwrap();
    for table in [
        "provider_records",
        "provider_generations",
        "provider_lifecycle_events",
    ] {
        let rows: Vec<(i64,)> = transaction
            .fetch_all_sql(
                &format!("SELECT COUNT(*) FROM {table}"),
                sea_query::Values(Vec::new()),
            )
            .await
            .unwrap();
        assert_eq!(rows, [(0,)], "a failed creation leaves nothing in {table}");
    }
    transaction.rollback().await.unwrap();
}
