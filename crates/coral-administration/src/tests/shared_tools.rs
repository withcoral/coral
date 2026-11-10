//! The Shared Tool workflow: a publish is typed from the Tools its Owner can
//! discover, and Universal Search hears of a publish and of a removal after
//! each commits. Publishing and removal rules are tested at their owner,
//! coral-tools.
use crate::{SharedToolWorkflow, WorkspaceWorkflow};
use coral_catalog::ToolAccessState;
use coral_provider::ProviderModule;
use coral_search::{SearchIndex, SearchQuery, SearchRequest, UniversalSearchModule};
use coral_test_support::{TEST_COMPILATION_TIMEOUT, TestDatabase, change, provision_user};
use coral_tools::{
    AuthoredToolDescription, AuthoredToolSource, IsolateLimits, PrivateToolChangeContext,
    PrivateToolModule, PrivateToolPath, PrivateToolSave, SharedToolModule, SharedToolOrigin,
    SharedToolPath, SharedToolPublish,
};
use coral_workspace::{
    AuthenticatedPrincipal, AuthenticatedUser, OrganizationRole, TraceId, WorkspaceDisplayName,
    WorkspaceId, WorkspaceModule,
};
use sea_query::{Alias, Expr, ExprTrait as _, Query};

const SHARED: &str = "coral.shared.tides.summarize";
const SOURCE: &str = "export default async function summarize() { return 'high at noon'; }";

/// One Workspace with the Providers Coral defines, whose Owner has the
/// Private Tool `draft` to publish.
struct Fixture {
    db: TestDatabase,
    index: SearchIndex,
    search: UniversalSearchModule,
    workflow: SharedToolWorkflow,
    owner: AuthenticatedUser,
    workspace: WorkspaceId,
}

impl Fixture {
    async fn new() -> Self {
        let db = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
        let database = db.database();
        let workspaces = WorkspaceModule::new(database.clone());
        let index = SearchIndex::new(database.clone(), std::time::Duration::from_mins(5));
        let owner = provision_user(database.clone(), "user:owner", OrganizationRole::Member).await;
        let workspace = WorkspaceWorkflow::new(
            database.clone(),
            workspaces.clone(),
            ProviderModule::with_running_catalog(
                coral_outbound::DestinationPolicy::default(),
                database.clone(),
                workspaces.clone(),
            ),
            index.clone(),
        )
        .create_workspace(
            &change(owner.clone(), 10, '1'),
            WorkspaceDisplayName::parse("Ops").unwrap(),
        )
        .await
        .expect("create the Workspace")
        .id()
        .clone();
        PrivateToolModule::new(
            database.clone(),
            workspaces.clone(),
            IsolateLimits::default(),
            TEST_COMPILATION_TIMEOUT,
        )
        .expect("start the Private Tool compiler")
        .save(
            &PrivateToolChangeContext::new(
                AuthenticatedPrincipal::user(owner.clone()),
                20,
                TraceId::parse(&"2".repeat(32)).unwrap(),
            ),
            &workspace,
            PrivateToolSave::new(
                PrivateToolPath::parse("draft").unwrap(),
                AuthoredToolDescription::parse("A private draft").unwrap(),
                AuthoredToolSource::parse(SOURCE).unwrap(),
            ),
        )
        .await
        .expect("save the draft");
        let search = UniversalSearchModule::new(database.clone(), workspaces.clone());
        let shared_tools = SharedToolModule::new(
            database,
            workspaces,
            IsolateLimits::default(),
            TEST_COMPILATION_TIMEOUT,
        )
        .expect("start the Shared Tool compiler");
        // The Workspace's own Providers are indexed before a test publishes.
        index.settled().await;
        Self {
            workflow: SharedToolWorkflow::new(shared_tools, search.clone(), index.clone()),
            db,
            index,
            search,
            owner,
            workspace,
        }
    }

    /// A publish of `source` at [`SHARED`] from the Owner's `draft`.
    fn publish(&self, source: &str, expected_revision: u64) -> SharedToolPublish {
        SharedToolPublish::new(
            SharedToolPath::parse("tides.summarize").unwrap(),
            SharedToolOrigin::PrivateTool {
                owner: AuthenticatedPrincipal::user(self.owner.clone())
                    .principal()
                    .clone(),
                path: PrivateToolPath::parse("draft").unwrap(),
            },
            AuthoredToolDescription::parse("Summarize the tides").unwrap(),
            AuthoredToolSource::parse(source).unwrap(),
            ToolAccessState::Enabled,
            expected_revision,
        )
    }

    /// Whether a search by the Owner finds [`SHARED`].
    async fn finds_shared(&self) -> bool {
        self.search
            .search(
                &AuthenticatedPrincipal::user(self.owner.clone()),
                &self.workspace,
                &SearchRequest::query(SearchQuery::parse("summarize tides").unwrap()),
            )
            .await
            .expect("search the Workspace")
            .hits()
            .iter()
            .any(|hit| hit.call_path().as_str() == SHARED)
    }

    /// How many Shared Tool rows the search index holds in the Workspace.
    async fn indexed_rows(&self) -> usize {
        let rows: Vec<(String,)> = self
            .db
            .database()
            .fetch_all(
                Query::select()
                    .column(Alias::new("doc_id"))
                    .from(Alias::new("search_entities"))
                    .and_where(Expr::col(Alias::new("workspace_id")).eq(self.workspace.to_string()))
                    .and_where(Expr::col(Alias::new("kind")).eq("shared_tool"))
                    .to_owned(),
            )
            .await
            .expect("read the Shared Tool rows");
        rows.len()
    }
}

// Catches: a publish the workflow does not type from the Tools its Owner can
// discover, so its review shows no diagnostic that a Private Tool save has.
#[tokio::test]
async fn a_publish_review_is_typed_from_the_tools_its_owner_can_discover() {
    let f = Fixture::new().await;

    let validated = f
        .workflow
        .publish(
            &change(f.owner.clone(), 30, '3'),
            &f.workspace,
            // `coral.listProviders` takes no second argument.
            f.publish(
                "export default async function listed() { return coral.listProviders({}, 1); }",
                0,
            )
            .validate_only(),
        )
        .await
        .expect("validate");

    assert!(validated.published().is_none());
    let diagnostics = validated.review().diagnostics();
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code() == 2554),
        "{diagnostics:?}"
    );
    f.index.settled().await;
    assert_eq!(f.indexed_rows().await, 0, "a validation publishes nothing");
}

// Catches: a publish or a removal that commits without telling search, so
// agents find the Shared Tool, or its row stays, only after a reconcile pass.
#[tokio::test]
async fn a_publish_and_a_removal_tell_search_after_they_commit() {
    let f = Fixture::new().await;
    assert!(!f.finds_shared().await);

    let published = f
        .workflow
        .publish(
            &change(f.owner.clone(), 30, '3'),
            &f.workspace,
            f.publish(SOURCE, 0),
        )
        .await
        .expect("publish");
    f.index.settled().await;

    assert!(f.finds_shared().await);
    assert_eq!(f.indexed_rows().await, 1);

    let removed = f
        .workflow
        .remove(
            &change(f.owner.clone(), 40, '4'),
            &f.workspace,
            &SharedToolPath::parse("tides.summarize").unwrap(),
            1,
        )
        .await
        .expect("remove");
    f.index.settled().await;

    assert_eq!(&removed, published.published().expect("the tool").id());
    assert!(!f.finds_shared().await);
    assert_eq!(f.indexed_rows().await, 0, "the removal took the row");
}
