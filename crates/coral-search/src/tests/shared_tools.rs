//! Shared Tools in Universal Search: every Principal of the Workspace finds
//! and describes a live one at its current revision, under the Workspace's
//! policy, and the index follows a publish and a removal whether or not a
//! notification arrives.

use coral_policy::ToolAccessState;
use coral_test_support::change;
use coral_tools::{
    AuthoredToolDescription, AuthoredToolSource, SharedTool, SharedToolOrigin, SharedToolPath,
    SharedToolPublish,
};
use coral_workspace::AuthenticatedPrincipal;
use sea_query::{Alias, Expr, ExprTrait as _, Query};

use super::{TestModules, TracerScope, organization_user};
use crate::{
    ApprovalRequirement, AuthoringDescription, CallPath, EntityKind, Error, SearchQuery,
    SearchRequest,
};

/// The Shared Tool every test publishes.
const SHARED: &str = "coral.shared.tides.summarize";

const SOURCE: &str = "export default async function summarize(input: { harbour: string }) {
    return { harbour: input.harbour, high: 4 };
}";

/// The tracer Workspace, whose Owner publishes Shared Tools, with a Member
/// and an Agent who discover them.
struct SharedScope {
    fixture: TestModules,
    scope: TracerScope,
    member: AuthenticatedPrincipal,
    agent: AuthenticatedPrincipal,
}

impl SharedScope {
    async fn new() -> Self {
        let fixture = TestModules::new().await;
        let scope = TracerScope::new(&fixture).await;
        let member = fixture
            .add_member(&scope.user, &scope.workspace_id, "user:shared-member")
            .await;
        let issued = fixture
            .workspaces
            .create_agent(
                &change(scope.user.clone(), 51, '5'),
                &scope.workspace_id,
                coral_workspace::AgentDisplayName::parse("Worker").expect("Agent name"),
                None,
            )
            .await
            .expect("create the Agent");
        let agent = fixture
            .workspaces
            .authenticate_agent(&scope.workspace_id, issued.credential.expose(), 52)
            .await
            .expect("authenticate the Agent");
        Self {
            fixture,
            scope,
            member,
            agent,
        }
    }

    /// Publishes [`SOURCE`] with `description` at [`SHARED`] as the Owner,
    /// from the Owner's tracer Private Tool, and notifies nothing.
    async fn publish(
        &self,
        description: &str,
        default: ToolAccessState,
        expected_revision: u64,
    ) -> SharedTool {
        self.fixture
            .publish_shared_tool(
                &self.scope.user,
                &self.scope.workspace_id,
                SharedToolPublish::review(
                    SharedToolPath::parse("tides.summarize").expect("a path"),
                    SharedToolOrigin::PrivateTool {
                        owner: self.scope.principal.principal().clone(),
                        path: self.scope.saved.path().clone(),
                    },
                    AuthoredToolDescription::parse(description).expect("a description"),
                    AuthoredToolSource::parse(SOURCE).expect("a source"),
                    default,
                    expected_revision,
                ),
            )
            .await
    }

    /// Removes [`SHARED`] as the Owner, and notifies nothing.
    async fn remove(&self, expected_revision: u64) {
        self.fixture
            .shared_tools
            .remove(
                &change(self.scope.user.clone(), 70, '7'),
                &self.scope.workspace_id,
                &SharedToolPath::parse("tides.summarize").expect("a path"),
                expected_revision,
            )
            .await
            .expect("remove the Shared Tool");
    }

    /// The Shared Tools a query finds for `principal`, as their Call Paths.
    async fn found(&self, principal: &AuthenticatedPrincipal, query: &str) -> Vec<String> {
        self.fixture
            .search
            .search(
                principal,
                &self.scope.workspace_id,
                &SearchRequest::query(SearchQuery::parse(query).expect("a query")),
            )
            .await
            .expect("search the Workspace")
            .hits()
            .iter()
            .filter(|hit| hit.kind() == EntityKind::SharedTool)
            .map(|hit| hit.call_path().as_str().to_owned())
            .collect()
    }

    async fn describe(
        &self,
        principal: &AuthenticatedPrincipal,
    ) -> Result<crate::ExactAuthoringDescription, Error> {
        self.fixture
            .search
            .describe(
                principal,
                &self.scope.workspace_id,
                &CallPath::parse(SHARED).expect("a Call Path"),
                &[],
            )
            .await
    }

    /// The approval a description of [`SHARED`] states for `principal`;
    /// `None` when it describes nothing.
    async fn approval(&self, principal: &AuthenticatedPrincipal) -> Option<ApprovalRequirement> {
        match self.describe(principal).await {
            Ok(described) => match described.authoring() {
                AuthoringDescription::AuthoredTool(shared) => Some(shared.approval_requirement()),
                AuthoringDescription::Tool(_) => panic!("{SHARED} is a Shared Tool"),
            },
            Err(Error::AuthoringDescriptionNotFound) => None,
            Err(error) => panic!("describe failed: {error:?}"),
        }
    }

    fn principals(&self) -> [(&'static str, &AuthenticatedPrincipal); 3] {
        [
            ("owner", &self.scope.principal),
            ("member", &self.member),
            ("agent", &self.agent),
        ]
    }

    /// The fingerprint of each Shared Tool row the index holds.
    async fn rows(&self) -> Vec<String> {
        let rows: Vec<(String,)> = self
            .fixture
            .search
            .database
            .fetch_all(
                Query::select()
                    .column(Alias::new("fingerprint"))
                    .from(Alias::new("search_entities"))
                    .and_where(
                        Expr::col(Alias::new("workspace_id"))
                            .eq(self.scope.workspace_id.to_string()),
                    )
                    .and_where(Expr::col(Alias::new("kind")).eq("shared_tool"))
                    .to_owned(),
            )
            .await
            .expect("read the Shared Tool rows");
        rows.into_iter().map(|(fingerprint,)| fingerprint).collect()
    }
}

/// Asserts that `described` is revision 1 of [`SHARED`] as `published`
/// holds it, with its source, and that it nowhere names `owner_id`, who
/// published it.
fn assert_describes_revision_1(
    described: &crate::ExactAuthoringDescription,
    published: &SharedTool,
    owner_id: &str,
    name: &str,
) {
    let AuthoringDescription::AuthoredTool(tool) = described.authoring() else {
        panic!("{SHARED} is a Shared Tool");
    };
    assert_eq!(tool.kind(), coral_catalog::AuthoredToolKind::Shared);
    assert_eq!(tool.description(), "Summarize the tides");
    assert_eq!(tool.provider_display_name(), "Shared");
    assert_eq!(tool.revision(), 1);
    assert_eq!(
        tool.approval_requirement(),
        ApprovalRequirement::NotRequired
    );
    assert_eq!(
        tool.typescript_declaration(),
        published.current().declaration().as_str(),
        "{name}"
    );
    assert_eq!(
        described
            .authored_tool_detail()
            .expect("the source")
            .source()
            .as_str(),
        SOURCE,
        "{name}"
    );
    let projected = crate::projection::exact_description(described);
    for (field, expected) in [
        ("/kind", serde_json::json!("shared_tool")),
        ("/call_path", serde_json::json!(SHARED)),
        ("/revision", serde_json::json!(1)),
        ("/source", serde_json::json!(SOURCE)),
    ] {
        assert_eq!(projected.pointer(field), Some(&expected), "{name}: {field}");
    }
    assert!(
        !projected.to_string().contains(owner_id),
        "{name}: the description names its publisher: {projected}"
    );
    // A description holds these members and no other, so none can say who
    // published the Shared Tool.
    let mut members = projected
        .as_object()
        .expect("a description is an object")
        .keys()
        .map(String::as_str)
        .collect::<Vec<_>>();
    members.sort_unstable();
    assert_eq!(
        members,
        [
            "approval_required",
            "call_path",
            "description",
            "diagnostics",
            "diagnostics_truncated",
            "hints",
            "kind",
            "provider_display_name",
            "provider_id",
            "revision",
            "source",
            "typescript_declaration",
        ],
        "{name}"
    );
}

// Catches: a Shared Tool that only its publisher finds, a hit or description
// under another kind, a description that names its publisher or lacks the
// source and the current revision, and an index that needs a notification to
// learn of a publish.
#[tokio::test]
async fn every_principal_finds_and_describes_a_shared_tool_a_pass_indexed() {
    let shared = SharedScope::new().await;
    let published = shared
        .publish("Summarize the tides", ToolAccessState::Enabled, 0)
        .await;
    for (name, principal) in shared.principals() {
        assert_eq!(
            shared.found(principal, "tides").await,
            [""; 0],
            "{name}: no row yet"
        );
        // An exact lookup reads no index.
        assert!(shared.describe(principal).await.is_ok(), "{name}");
    }

    // No notification came: a reconcile pass finds the publish.
    shared.fixture.converge().await;

    let owner_id = shared.scope.user.user_id().to_string();
    for (name, principal) in shared.principals() {
        assert_eq!(shared.found(principal, "tides").await, [SHARED], "{name}");
        let listed = shared
            .fixture
            .search
            .search(
                principal,
                &shared.scope.workspace_id,
                &SearchRequest::list(),
            )
            .await
            .expect("list the Workspace");
        assert!(
            listed
                .hits()
                .iter()
                .any(|hit| hit.call_path().as_str() == SHARED),
            "{name}"
        );
        // The Owner's Private Tool stays its own.
        assert_eq!(
            listed
                .hits()
                .iter()
                .any(|hit| hit.kind() == EntityKind::PrivateTool),
            name == "owner",
            "{name}"
        );

        let described = shared.describe(principal).await.expect("describe");
        assert_describes_revision_1(&described, &published, &owner_id, name);
    }

    // A mistyped path under the Shared root names the nearest Shared Tool.
    let mistyped = "coral.shared.tides.summarise";
    let crate::Discovery::Exact(lookup) = shared
        .fixture
        .search
        .discover(
            &shared.member,
            &shared.scope.workspace_id,
            &crate::DiscoveryRequest::Paths(
                crate::ExactPaths::new(vec![mistyped.to_owned()], Vec::new()).expect("paths"),
            ),
        )
        .await
        .expect("look up the mistyped path")
    else {
        panic!("a lookup of paths is exact");
    };
    let [missing] = lookup.missing() else {
        panic!("one missing path");
    };
    assert_eq!(missing.call_path(), mistyped);
    assert_eq!(missing.suggestions(), [SHARED]);

    // A User outside the Workspace finds and describes nothing there.
    let outsider = AuthenticatedPrincipal::user(
        organization_user(shared.fixture.search.database.clone(), "user:outsider").await,
    );
    let denied = shared.describe(&outsider).await;
    assert!(
        matches!(denied, Err(Error::AuthoringDescriptionNotFound)),
        "{denied:?}"
    );
}

// Catches: a Shared Tool that a Principal of another Workspace finds or
// describes there, through a row or a read that matches the Tool and not its
// Workspace.
#[tokio::test]
async fn a_shared_tool_stays_in_its_workspace() {
    let shared = SharedScope::new().await;
    shared
        .publish("Summarize the tides", ToolAccessState::Enabled, 0)
        .await;
    shared.fixture.converge().await;
    assert_eq!(shared.found(&shared.member, "tides").await, [SHARED]);

    let elsewhere =
        organization_user(shared.fixture.search.database.clone(), "user:elsewhere").await;
    let other_workspace = shared.fixture.create_workspace(&elsewhere).await;
    let elsewhere = AuthenticatedPrincipal::user(elsewhere);
    shared.fixture.converge().await;

    for request in [
        SearchRequest::query(SearchQuery::parse("tides").expect("a query")),
        SearchRequest::list(),
    ] {
        let page = shared
            .fixture
            .search
            .search(&elsewhere, &other_workspace, &request)
            .await
            .expect("search the other Workspace");
        assert!(
            page.hits()
                .iter()
                .all(|hit| hit.kind() != EntityKind::SharedTool),
            "{:?}",
            page.hits()
        );
    }
    let described = shared
        .fixture
        .search
        .describe(
            &elsewhere,
            &other_workspace,
            &CallPath::parse(SHARED).expect("a Call Path"),
            &[],
        )
        .await;
    assert!(
        matches!(described, Err(Error::AuthoringDescriptionNotFound)),
        "{described:?}"
    );
}

// Catches: an index that keeps a superseded revision's text after a publish
// notifies it, a removed Shared Tool that search still returns while its row
// waits, and a row that outlives a removal no notification reported.
#[tokio::test]
async fn the_index_follows_a_publish_and_a_removal_of_a_shared_tool() {
    let shared = SharedScope::new().await;
    let published = shared
        .publish("Chart the moon phases", ToolAccessState::Enabled, 0)
        .await;
    let notify = || {
        shared
            .fixture
            .index
            .sync_authored_tool(shared.scope.workspace_id.clone(), published.id().clone());
    };
    notify();
    shared.fixture.index.settled().await;
    let [first] = shared.rows().await.try_into().expect("one row");
    assert_eq!(shared.found(&shared.member, "moon").await, [SHARED]);

    shared
        .publish("Forecast the swell", ToolAccessState::Enabled, 1)
        .await;
    // A notification names an id alone, so a repeated one changes nothing.
    notify();
    notify();
    shared.fixture.index.settled().await;
    let [second] = shared.rows().await.try_into().expect("one row");
    assert_ne!(first, second, "the row follows the current revision");
    assert_eq!(shared.found(&shared.member, "swell").await, [SHARED]);
    assert_eq!(shared.found(&shared.member, "moon").await, [SHARED; 0]);

    shared.remove(2).await;
    assert_eq!(
        shared.rows().await,
        [second],
        "the row outlives the removal"
    );
    for (name, principal) in shared.principals() {
        assert_eq!(
            shared.found(principal, "swell").await,
            [SHARED; 0],
            "{name}"
        );
        assert_eq!(shared.approval(principal).await, None, "{name}");
    }

    // No notification came: a reconcile pass removes the row.
    shared.fixture.converge().await;
    assert_eq!(shared.rows().await, [""; 0]);
}

// Catches: a Shared Tool that ignores the default its publishing Owner chose,
// an Owner's exact rule that does not reach a Member or an Agent, and a
// search or describe that shows a Shared Tool a rule or a disabled Shared
// Provider stops.
#[tokio::test]
async fn search_and_describe_read_a_shared_tools_rule_and_its_providers_state() {
    let shared = SharedScope::new().await;
    let published = shared
        .publish("Summarize the tides", ToolAccessState::ApprovalRequired, 0)
        .await;
    shared.fixture.converge().await;
    let mut transaction = shared
        .fixture
        .search
        .database
        .begin_read_snapshot()
        .await
        .unwrap();
    let provider = coral_provider::authored_tool_provider_in_transaction(
        &mut transaction,
        &shared.scope.workspace_id,
        coral_catalog::AuthoredToolKind::Shared,
    )
    .await
    .unwrap()
    .expect("the Workspace's Shared Provider");
    transaction.rollback().await.unwrap();
    let expect = async |described: Option<ApprovalRequirement>, why: &str| {
        for (name, principal) in shared.principals() {
            assert_eq!(shared.approval(principal).await, described, "{why}: {name}");
            assert_eq!(
                shared.found(principal, "tides").await.len(),
                usize::from(described.is_some()),
                "{why}: {name}"
            );
        }
    };

    expect(
        Some(ApprovalRequirement::Required),
        "the default chosen at publish",
    )
    .await;
    for (at, state, described) in [
        (
            80,
            ToolAccessState::Enabled,
            Some(ApprovalRequirement::NotRequired),
        ),
        (81, ToolAccessState::Disabled, None),
        (
            82,
            ToolAccessState::ApprovalRequired,
            Some(ApprovalRequirement::Required),
        ),
    ] {
        shared
            .fixture
            .policies
            .set_access_state(
                &change(shared.scope.user.clone(), at, 'c'),
                coral_policy::ToolPolicyTarget::new(
                    shared.scope.workspace_id.clone(),
                    provider.id().clone(),
                    published.id().tool_key(),
                ),
                state,
            )
            .await
            .expect("set the Owner's rule");
        expect(described, &format!("{state:?}")).await;
    }
    shared
        .fixture
        .providers
        .disable_provider(
            &change(shared.scope.user.clone(), 90, 'd')
                .with_etag(&provider.etag())
                .unwrap(),
            &shared.scope.workspace_id,
            provider.id(),
        )
        .await
        .expect("disable the Shared Provider");
    expect(None, "a disabled Shared Provider").await;
}
