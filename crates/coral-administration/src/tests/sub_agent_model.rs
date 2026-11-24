//! The Sub-agent's model: `coral.agent` follows it into every Workspace,
//! turned off, and a rule that is on is a consent to the model that is set.
use crate::{ProviderWorkflow, SubAgentModelWorkflow, WorkspaceWorkflow};
use coral_agent_model::{AgentModel, AgentModels, ApiKeyChange, EnteredSettings, ModelProtocol};
use coral_catalog::{BuiltinCatalog, ToolKey};
use coral_outbound::DestinationPolicy;
use coral_policy::{Error, ToolAccessState, ToolPolicyModule, ToolPolicyTarget};
use coral_provider::ProviderModule;
use coral_search::SearchIndex;
use coral_test_support::{TestDatabase, change, provision_user};
use coral_workspace::{
    AuthenticatedUser, IfMatch, OrganizationRole, RequestId, WorkspaceDisplayName, WorkspaceId,
    WorkspaceModule, WorkspaceRole,
};
use uuid::Uuid;

const URL_A: &str = "https://a.example/v1";
const URL_B: &str = "https://b.example/v1";

struct Fixture {
    db: TestDatabase,
    _vault_dir: tempfile::TempDir,
    workspaces: WorkspaceModule,
    providers: ProviderModule,
    index: SearchIndex,
    policies: ToolPolicyModule,
    models: SubAgentModelWorkflow,
    /// The Organization Owner, who belongs to no Workspace.
    organization_owner: AuthenticatedUser,
    /// The Owner of every Workspace here.
    owner: AuthenticatedUser,
    member: AuthenticatedUser,
}

impl Fixture {
    async fn new() -> Self {
        let db = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
        let database = db.database();
        let workspaces = WorkspaceModule::new(database.clone());
        let providers = ProviderModule::new(
            DestinationPolicy::default(),
            database.clone(),
            workspaces.clone(),
            BuiltinCatalog::running().unwrap(),
            coral_catalog::SchemaGraphCache::with_default_budget(),
        )
        .with_sub_agent_catalog(BuiltinCatalog::with_sub_agent().unwrap());
        let index = SearchIndex::inert(database.clone());
        let vault_dir = tempfile::tempdir().unwrap();
        let vault =
            coral_credentials::CredentialVault::open(&vault_dir.path().join("key")).unwrap();
        let models = AgentModels::new(database.clone(), vault, DestinationPolicy::default());
        let organization_owner =
            provision_user(database.clone(), "org-owner", OrganizationRole::Owner).await;
        let owner = provision_user(database.clone(), "owner", OrganizationRole::Member).await;
        let member = provision_user(database.clone(), "member", OrganizationRole::Member).await;
        Self {
            policies: ToolPolicyModule::new(database, workspaces.clone()),
            models: SubAgentModelWorkflow::new(
                models,
                ProviderWorkflow::new(providers.clone(), index.clone()),
            ),
            db,
            _vault_dir: vault_dir,
            workspaces,
            providers,
            index,
            organization_owner,
            owner,
            member,
        }
    }

    async fn workspace(&self, name: &str) -> WorkspaceId {
        let workflow = WorkspaceWorkflow::new(
            self.db.database(),
            self.workspaces.clone(),
            self.providers.clone(),
            self.index.clone(),
        );
        let context = change(self.owner.clone(), 1, '1');
        let name = WorkspaceDisplayName::parse(name).unwrap();
        let request = RequestId::generate();
        let created = workflow.create_workspace(&context, &request, name);
        created.await.unwrap().id().clone()
    }

    async fn set(&self, base_url: &str) -> AgentModel {
        let context = change(self.organization_owner.clone(), 2, '2');
        self.models
            .set(&context, &entered(base_url), None)
            .await
            .unwrap()
    }

    async fn change(&self, read: &AgentModel, entered: &EnteredSettings) -> AgentModel {
        let context = change(self.organization_owner.clone(), 3, '3');
        let if_match = etag(read);
        let keep = ApiKeyChange::Keep;
        self.models
            .change(&context, &if_match, entered, keep)
            .await
            .unwrap()
    }

    async fn remove(&self, read: &AgentModel) {
        let context = change(self.organization_owner.clone(), 4, '4');
        self.models.remove(&context, &etag(read)).await.unwrap();
    }

    async fn turn_on(&self, workspace: &WorkspaceId, seen: Uuid) -> Result<(), Error> {
        self.turn_on_as(&self.owner, workspace, seen, ToolAccessState::Enabled)
            .await
    }

    async fn turn_on_as(
        &self,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
        seen: Uuid,
        state: ToolAccessState,
    ) -> Result<(), Error> {
        let context = change(actor.clone(), 5, '5');
        let turned_on = self
            .policies
            .turn_on_sub_agent(&context, workspace, seen, state);
        turned_on.await.map(|_decision| ())
    }

    async fn set_state(
        &self,
        workspace: &WorkspaceId,
        state: ToolAccessState,
    ) -> Result<(), Error> {
        let context = change(self.owner.clone(), 6, '6');
        let target = self.target(workspace, "agent").await;
        let set = self
            .policies
            .set_access_state(&context, target, state)
            .await;
        set.map(|_decision| ())
    }

    async fn target(&self, workspace: &WorkspaceId, key: &str) -> ToolPolicyTarget {
        let providers = self
            .providers
            .inspect_providers(&self.owner, workspace)
            .await
            .unwrap();
        let builtin = providers
            .iter()
            .find(|one| one.call_root().as_str() == "coral");
        let key = ToolKey::parse(key).unwrap();
        ToolPolicyTarget::new(workspace.clone(), builtin.unwrap().id().clone(), key)
    }

    /// The state that the Tools page shows of the Tool, or `None` while the
    /// Workspace does not have it.
    async fn shown(&self, workspace: &WorkspaceId, key: &str) -> Option<ToolAccessState> {
        let target = self.target(workspace, key).await;
        let shown = self.policies.decision(&self.owner, &target).await;
        match shown {
            Ok(decision) => Some(decision.access_state()),
            Err(Error::ToolNotFound) => None,
            Err(error) => panic!("{error:?}"),
        }
    }

    /// Whether a Turn of the Workspace may run.
    async fn is_on(&self, workspace: &WorkspaceId) -> bool {
        let database = self.db.database();
        let mut tx = database.begin_read_snapshot().await.unwrap();
        coral_policy::sub_agent_is_on_in_transaction(&mut tx, workspace)
            .await
            .unwrap()
    }
}

fn entered(base_url: &str) -> EnteredSettings {
    EnteredSettings {
        protocol: ModelProtocol::Anthropic,
        base_url: base_url.to_owned(),
        model: "model-1".to_owned(),
        context_window: None,
        request_args_json: String::new(),
    }
}

fn etag(model: &AgentModel) -> IfMatch {
    IfMatch::parse(&model.etag().to_string()).unwrap()
}

// Catches: a Workspace that gets the Tool turned on, or that a set model or a new Workspace
// misses, and a Workspace that has the Tools with no model.
#[tokio::test]
async fn a_model_gives_every_workspace_the_tool_turned_off_and_its_removal_takes_it_away() {
    let f = Fixture::new().await;
    let before = f.workspace("Before").await;
    assert_eq!(f.shown(&before, "agent").await, None);

    let model = f.set(URL_A).await;
    let after = f.workspace("After").await;
    for workspace in [&before, &after] {
        assert_eq!(
            f.shown(workspace, "agent").await,
            Some(ToolAccessState::Disabled)
        );
        assert!(!f.is_on(workspace).await);
        let cancel = f.shown(workspace, "cancelAgent").await;
        assert_eq!(cancel, Some(ToolAccessState::Enabled));
    }

    f.remove(&model).await;
    for workspace in [&before, &after] {
        assert_eq!(f.shown(workspace, "agent").await, None);
        assert_eq!(f.shown(workspace, "cancelAgent").await, None);
    }
}

// Catches: a consent to a model that the Owner did not see, or by someone who is not the
// Workspace's Owner, and another way to turn the Tool on.
#[tokio::test]
async fn only_a_workspace_owner_who_saw_the_model_turns_the_tool_on() {
    let f = Fixture::new().await;
    let workspace = f.workspace("Ops").await;
    let context = change(f.owner.clone(), 1, '1');
    let joined =
        f.workspaces
            .set_membership(&context, &workspace, &f.member, WorkspaceRole::Member);
    joined.await.unwrap();
    let model = f.set(URL_A).await;

    for denied in [&f.member, &f.organization_owner] {
        let on = f
            .turn_on_as(denied, &workspace, model.id(), ToolAccessState::Enabled)
            .await;
        assert!(matches!(on, Err(Error::Workspace(_))), "{denied:?}: {on:?}");
    }
    let unseen = f.turn_on(&workspace, Uuid::new_v4()).await;
    assert!(
        matches!(unseen, Err(Error::SubAgentModelChanged)),
        "{unseen:?}"
    );
    for state in [ToolAccessState::Enabled, ToolAccessState::ApprovalRequired] {
        let set = f.set_state(&workspace, state).await;
        assert!(
            matches!(set, Err(Error::SubAgentConsentRequired)),
            "{set:?}"
        );
    }
    let off = ToolAccessState::Disabled;
    let no_state = f.turn_on_as(&f.owner, &workspace, model.id(), off).await;
    assert!(matches!(no_state, Err(Error::InvalidBatch)), "{no_state:?}");
    assert_eq!(f.shown(&workspace, "agent").await, Some(off));

    // Either state that is on needs the consent, and after it the Owner moves
    // between them freely.
    let approval = ToolAccessState::ApprovalRequired;
    f.turn_on_as(&f.owner, &workspace, model.id(), approval)
        .await
        .unwrap();
    assert!(f.is_on(&workspace).await);
    f.set_state(&workspace, ToolAccessState::Enabled)
        .await
        .unwrap();
    f.set_state(&workspace, approval).await.unwrap();
    f.set_state(&workspace, off).await.unwrap();
    assert!(!f.is_on(&workspace).await);
    let again = f.set_state(&workspace, ToolAccessState::Enabled).await;
    assert!(
        matches!(again, Err(Error::SubAgentConsentRequired)),
        "{again:?}"
    );
}

// Catches: a consent that outlives the destination it was given for, or one that a change of
// anything else takes away.
#[tokio::test]
async fn only_a_new_base_url_or_a_removal_turns_the_tool_off_in_every_workspace() {
    let f = Fixture::new().await;
    let (one, two) = (f.workspace("One").await, f.workspace("Two").await);
    let first = f.set(URL_A).await;
    for workspace in [&one, &two] {
        f.turn_on(workspace, first.id()).await.unwrap();
    }

    let renamed = EnteredSettings {
        protocol: ModelProtocol::OpenAiResponses,
        model: "model-2".to_owned(),
        context_window: Some(200_000),
        request_args_json: r#"{"top_p": 1}"#.to_owned(),
        ..entered(URL_A)
    };
    let renamed = f.change(&first, &renamed).await;
    assert!(f.is_on(&one).await && f.is_on(&two).await);

    let moved = f.change(&renamed, &entered(URL_B)).await;
    assert!(!f.is_on(&one).await && !f.is_on(&two).await);
    let stale = f.turn_on(&one, first.id()).await;
    assert!(
        matches!(stale, Err(Error::SubAgentModelChanged)),
        "{stale:?}"
    );
    let back = f.change(&moved, &entered(URL_A)).await;
    let stale = f.turn_on(&one, first.id()).await;
    assert!(
        matches!(stale, Err(Error::SubAgentModelChanged)),
        "{stale:?}"
    );

    f.turn_on(&one, back.id()).await.unwrap();
    f.remove(&back).await;
    assert!(!f.is_on(&one).await);
    f.set(URL_A).await;
    assert_eq!(
        f.shown(&one, "agent").await,
        Some(ToolAccessState::Disabled)
    );
}

// Catches: a rule left from before the model was set, or no rule at all, that reads as a consent.
#[tokio::test]
async fn a_rule_from_before_is_reset_and_a_missing_rule_counts_as_off() {
    let f = Fixture::new().await;
    let workspace = f.workspace("Ops").await;
    let target = f.target(&workspace, "agent").await;
    let insert = format!(
        "INSERT INTO tool_access_rules (workspace_id, provider_id, tool_key, access_state, \
         created_at_unix_nanos, updated_at_unix_nanos) VALUES ('{}', '{}', 'agent', 'enabled', 1, 1)",
        target.workspace_id(),
        target.provider_id(),
    );
    f.db.execute_fixture_sql(&insert).await.unwrap();

    f.set(URL_A).await;
    assert_eq!(
        f.shown(&workspace, "agent").await,
        Some(ToolAccessState::Disabled)
    );

    let delete = "DELETE FROM tool_access_rules WHERE tool_key = 'agent'";
    f.db.execute_fixture_sql(delete).await.unwrap();
    assert!(!f.is_on(&workspace).await);
}
