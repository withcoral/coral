//! The version each resource's row keeps, which Coral shows as the etag and
//! checks a change against, beside the table of concurrency metadata that
//! Coral still writes for a Coral that reads it.
use std::collections::BTreeMap;

use coral_db::{Transaction, UnixNanos, Version};
use coral_test_support::TestDatabase;
use sea_query::{Alias, Expr, ExprTrait as _, Query};

use super::{change, organization_user, remove_access, test_module};
use crate::{
    AccessRemovalTarget, AgentDisplayName, AgentRegistrationId, AgentState, AuthenticatedUser,
    ChangeContext, Denial, Error, IfMatch, LifecycleTargetKind, MembershipChange, OrganizationRole,
    PrincipalId, RequestId, ResourceMetadata, UserId, WorkspaceDisplayName, WorkspaceId,
    WorkspaceModule, WorkspaceRole,
};

/// One resource that a management change can conflict on.
#[derive(Debug, Clone)]
enum Resource {
    Workspace(WorkspaceId),
    Membership(WorkspaceId, UserId),
    Agent(WorkspaceId, PrincipalId),
    Registration(WorkspaceId, AgentRegistrationId),
}

impl Resource {
    fn workspace(&self) -> &WorkspaceId {
        match self {
            Self::Workspace(workspace)
            | Self::Membership(workspace, _)
            | Self::Agent(workspace, _)
            | Self::Registration(workspace, _) => workspace,
        }
    }

    fn kind(&self) -> LifecycleTargetKind {
        match self {
            Self::Workspace(_) => LifecycleTargetKind::Workspace,
            Self::Membership(..) => LifecycleTargetKind::WorkspaceMembership,
            Self::Agent(..) => LifecycleTargetKind::Agent,
            Self::Registration(..) => LifecycleTargetKind::AgentRegistration,
        }
    }

    fn target_id(&self) -> String {
        match self {
            Self::Workspace(workspace) => workspace.to_string(),
            Self::Membership(_, user) => user.to_string(),
            Self::Agent(_, agent) => agent.to_string(),
            Self::Registration(_, registration) => registration.to_string(),
        }
    }

    /// Reads the row's version and times past the Interface: an inspection
    /// is compared with them, and no read finds a deleted Agent's row.
    async fn row(&self, tx: &mut Transaction<'_>) -> Option<StoredRow> {
        let workspace = self.workspace().to_string();
        let (table, version, key) = match self {
            Self::Workspace(_) => (
                "workspace_records",
                "lifecycle_version",
                vec![("id", workspace)],
            ),
            Self::Membership(_, user) => (
                "workspace_memberships",
                "version",
                vec![("workspace_id", workspace), ("user_id", user.to_string())],
            ),
            Self::Agent(_, agent) => (
                "workspace_agents",
                "version",
                vec![("workspace_id", workspace), ("id", agent.to_string())],
            ),
            Self::Registration(_, registration) => (
                "agent_registrations",
                "version",
                vec![
                    ("workspace_id", workspace),
                    ("id", registration.to_string()),
                ],
            ),
        };
        let mut statement = Query::select();
        statement
            .columns([
                Alias::new(version),
                Alias::new("created_at_unix_nanos"),
                Alias::new("updated_at_unix_nanos"),
            ])
            .from(Alias::new(table));
        for (column, value) in key {
            statement.and_where(Expr::col(Alias::new(column)).eq(value));
        }
        let row: Option<(i64, Option<UnixNanos>, Option<UnixNanos>)> =
            tx.fetch_optional(statement).await.expect("read the row");
        row.map(|(version, created_at, updated_at)| StoredRow {
            version: Version::new(version).expect("a positive version"),
            created_at,
            updated_at,
        })
    }

    /// Reads the resource's row in the table of concurrency metadata, past
    /// the Interface: Coral writes that table and reads it nowhere.
    async fn metadata(&self, tx: &mut Transaction<'_>) -> Option<StoredMetadata> {
        let mut statement = Query::select();
        statement
            .columns([
                Alias::new("etag"),
                Alias::new("created_at"),
                Alias::new("updated_at"),
            ])
            .from(Alias::new("workspace_resource_metadata"));
        for (column, value) in [
            ("workspace_id", self.workspace().to_string()),
            ("kind", self.kind().as_storage_str().to_owned()),
            ("target_id", self.target_id()),
        ] {
            statement.and_where(Expr::col(Alias::new(column)).eq(value));
        }
        let row: Option<(String, UnixNanos, UnixNanos)> = tx
            .fetch_optional(statement)
            .await
            .expect("read the metadata row");
        row.map(|(etag, created_at, updated_at)| StoredMetadata {
            etag,
            created_at,
            updated_at,
        })
    }

    /// Reads what Coral keeps of the resource for a conflict check: its own
    /// on the row, and that of a Coral which still reads the metadata table.
    async fn kept(&self, tx: &mut Transaction<'_>) -> Kept {
        Kept {
            metadata: self.metadata(tx).await,
            row: self.row(tx).await,
        }
    }
}

/// The etag that the table of concurrency metadata holds for one Workspace
/// Membership, which a Coral that still reads the table shows.
pub(super) async fn membership_metadata_etag(
    module: &WorkspaceModule,
    workspace: &WorkspaceId,
    user: &UserId,
) -> Option<String> {
    let mut tx = module.database.begin_read_snapshot().await.unwrap();
    let metadata = Resource::Membership(workspace.clone(), user.clone())
        .metadata(&mut tx)
        .await;
    tx.commit().await.unwrap();
    metadata.map(|metadata| metadata.etag)
}

/// The row of one resource in the table of concurrency metadata: what a
/// Coral that still reads the table shows and checks.
#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredMetadata {
    etag: String,
    created_at: UnixNanos,
    updated_at: UnixNanos,
}

/// The version and the times on one resource's row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StoredRow {
    version: Version,
    created_at: Option<UnixNanos>,
    updated_at: Option<UnixNanos>,
}

impl StoredRow {
    /// The row as an inspection shows it.
    const fn shown(self) -> ResourceMetadata {
        ResourceMetadata {
            version: self.version,
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

/// What Coral keeps of one resource for a conflict check. A resource that
/// was never created has neither part, and one whose row is gone keeps its
/// metadata.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Kept {
    metadata: Option<StoredMetadata>,
    row: Option<StoredRow>,
}

impl Kept {
    fn etag(&self) -> Option<&str> {
        self.metadata
            .as_ref()
            .map(|metadata| metadata.etag.as_str())
    }
}

/// What one accepted change does to one resource. "A new etag" is a new one
/// in the table of concurrency metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Move {
    /// The resource's first row, at the first version, with a new etag.
    Created,
    /// A change to the row: its version rises by one, and the etag is new.
    Raised,
    /// The Agent's row stays, marked deleted, at the next version and with a
    /// new etag. No inspection shows it any more.
    MarkedDeleted,
    /// The row is gone, and the etag is new.
    Removed,
    /// The Registration's row went with its Agent. That is the Agent's
    /// change: the Registration's metadata is as it was.
    RemovedWithItsAgent,
}

/// One change to a Workspace, a Membership, an Agent or a Registration.
#[derive(Debug, Clone, Copy)]
enum Step {
    Rename(&'static str),
    Archive,
    Restore,
    GovernArchive,
    GovernRestore,
    CreateMembership,
    UpdateRole(WorkspaceRole),
    AssignOwner,
    CreateAgent,
    SetAgentState(AgentState),
    UpdateRegistration,
    RotateCredential,
    DeleteAgent,
    RemoveMembership,
    /// Removes the second Workspace's one Owner from the Organization, with
    /// this precondition on their Organization Membership: the heir takes
    /// over.
    TransferOwnership {
        if_match: IfMatch,
    },
}

impl Step {
    /// The resource whose etag the step checks, if it checks one.
    const fn checks(self) -> Option<&'static str> {
        match self {
            Self::Rename(_)
            | Self::Archive
            | Self::Restore
            | Self::GovernArchive
            | Self::GovernRestore => Some(WORKSPACE),
            Self::UpdateRole(_) | Self::RemoveMembership => Some(MEMBERSHIP),
            Self::SetAgentState(_) | Self::DeleteAgent => Some(AGENT),
            Self::UpdateRegistration | Self::RotateCredential => Some(REGISTRATION),
            Self::CreateMembership
            | Self::AssignOwner
            | Self::CreateAgent
            | Self::TransferOwnership { .. } => None,
        }
    }
}

/// How a step is refused before it is accepted.
#[derive(Clone, Copy)]
enum Refusal {
    /// With an etag that is not the resource's: an etag mismatch.
    StaleEtag,
    /// By a User who may not take the step.
    By(Who, fn(&Error) -> bool),
    /// With an etag that is not the Organization Membership's, which the
    /// step carries itself: an Organization authority conflict.
    StaleOrganizationMembership,
}

#[derive(Debug, Clone, Copy)]
enum Who {
    Member,
    Heir,
}

/// The form of the stale etag that a refused change sends.
#[derive(Debug, Clone, Copy)]
enum StaleEtag {
    /// A version that the row is not at.
    Version,
    /// A UUID: the etag that Coral showed before each row kept its version,
    /// and that a Coral which still reads the metadata table shows now.
    Uuid,
}

impl StaleEtag {
    /// A precondition of this form that an Organization Membership at its
    /// first version does not meet. Its etag is its version, so a UUID
    /// never matches it.
    fn organization_membership(self) -> IfMatch {
        match self {
            Self::Uuid => IfMatch::token(uuid::Uuid::new_v4()),
            Self::Version => IfMatch::version(Version::new(2).expect("a positive version")),
        }
    }

    /// An etag of this form that is not the resource's.
    fn of(self, kept: &Kept) -> String {
        match self {
            // The etag that the metadata table holds for the resource now: a
            // client that read it from a Coral which still shows it sends it
            // here. It is current there and stale here.
            Self::Uuid => kept.etag().expect("the resource has metadata").to_owned(),
            Self::Version => {
                let current = kept.row.expect("the resource has a row").version.get();
                // The version before, as a caller who read too early holds.
                // A row at its first version has none before.
                let stale = if current > 1 {
                    current - 1
                } else {
                    current + 1
                };
                stale.to_string()
            }
        }
    }
}

const WORKSPACE: &str = "Workspace";
const OWNER_MEMBERSHIP: &str = "Owner's Membership";
const MEMBERSHIP: &str = "Member's Membership";
const AGENT: &str = "Agent";
const REGISTRATION: &str = "Registration";
const SECOND_WORKSPACE: &str = "second Workspace";
const DEPARTING_MEMBERSHIP: &str = "departing Owner's Membership";
const HEIR_MEMBERSHIP: &str = "heir's Membership";

/// One Organization whose resources the steps change.
struct Changes {
    database: TestDatabase,
    module: WorkspaceModule,
    /// An Organization Owner, and the designated Owner of the Workspace.
    owner: AuthenticatedUser,
    /// The User whose Membership the steps create, change and remove.
    member: AuthenticatedUser,
    /// The one Owner of the second Workspace.
    departing: AuthenticatedUser,
    /// An Organization Member with no Workspace Membership at the start.
    heir: AuthenticatedUser,
    workspace: WorkspaceId,
    second: WorkspaceId,
    /// The Agent the steps create, with its Registration.
    agent: Option<(PrincipalId, AgentRegistrationId)>,
}

impl Changes {
    async fn new() -> Self {
        let (database, module) = test_module().await;
        let user = |name: &'static str, role| {
            let database = module.database.clone();
            async move { organization_user(database, name, role).await }
        };
        let owner = user("user:version-owner", OrganizationRole::Owner).await;
        let member = user("user:version-member", OrganizationRole::Member).await;
        let departing = user("user:version-departing", OrganizationRole::Member).await;
        let heir = user("user:version-heir", OrganizationRole::Member).await;
        let workspace = module
            .create(
                &change(owner.clone(), 10, '1'),
                WorkspaceDisplayName::parse("Versions").unwrap(),
            )
            .await
            .unwrap();
        let second = module
            .create(
                &change(departing.clone(), 10, '1'),
                WorkspaceDisplayName::parse("Second").unwrap(),
            )
            .await
            .unwrap();
        Self {
            database,
            module,
            owner,
            member,
            departing,
            heir,
            workspace: workspace.id().clone(),
            second: second.id().clone(),
            agent: None,
        }
    }

    fn resources(&self) -> Vec<(&'static str, Resource)> {
        let workspace = || self.workspace.clone();
        let second = || self.second.clone();
        let user = |user: &AuthenticatedUser| user.user_id().clone();
        let mut resources = vec![
            (WORKSPACE, Resource::Workspace(workspace())),
            (
                OWNER_MEMBERSHIP,
                Resource::Membership(workspace(), user(&self.owner)),
            ),
            (
                MEMBERSHIP,
                Resource::Membership(workspace(), user(&self.member)),
            ),
            (SECOND_WORKSPACE, Resource::Workspace(second())),
            (
                DEPARTING_MEMBERSHIP,
                Resource::Membership(second(), user(&self.departing)),
            ),
            (
                HEIR_MEMBERSHIP,
                Resource::Membership(second(), user(&self.heir)),
            ),
        ];
        if let Some((agent, registration)) = &self.agent {
            resources.push((AGENT, Resource::Agent(workspace(), agent.clone())));
            resources.push((
                REGISTRATION,
                Resource::Registration(workspace(), registration.clone()),
            ));
        }
        resources
    }

    /// What Coral keeps of every resource, in one read snapshot.
    async fn kept(&self) -> BTreeMap<&'static str, Kept> {
        let mut tx = self.module.database.begin_read_snapshot().await.unwrap();
        let mut kept = BTreeMap::new();
        for (name, resource) in self.resources() {
            kept.insert(name, resource.kept(&mut tx).await);
        }
        tx.commit().await.unwrap();
        kept
    }

    /// The concurrency metadata that the inspection of each resource shows,
    /// for every resource that one finds. A Membership that was never
    /// created or is removed has none, and neither has a deleted Agent.
    async fn shown(&self) -> BTreeMap<&'static str, ResourceMetadata> {
        let (module, owner) = (&self.module, &self.owner);
        let mut shown = BTreeMap::new();
        let (_, workspace) = module
            .inspect_workspace(owner, &self.workspace)
            .await
            .expect("inspect the Workspace");
        shown.insert(WORKSPACE, workspace);
        // The Owner holds no Membership in the second Workspace, and sees it
        // as an Organization Owner does.
        let second = module
            .inspect_organization_workspace(owner, &self.second)
            .await
            .expect("inspect the second Workspace");
        shown.insert(SECOND_WORKSPACE, second.metadata);
        for (name, user) in [(OWNER_MEMBERSHIP, owner), (MEMBERSHIP, &self.member)] {
            match module
                .inspect_membership(owner, &self.workspace, user.user_id())
                .await
            {
                Ok((_, metadata, _)) => drop(shown.insert(name, metadata)),
                Err(Error::MembershipNotFound) => {}
                Err(error) => panic!("inspect the {name}: {error:?}"),
            }
        }
        // Each Member of the second Workspace inspects their own Membership,
        // while they hold one.
        for (name, user) in [
            (DEPARTING_MEMBERSHIP, &self.departing),
            (HEIR_MEMBERSHIP, &self.heir),
        ] {
            if let Ok((_, metadata, _)) = module
                .inspect_membership(user, &self.second, user.user_id())
                .await
            {
                shown.insert(name, metadata);
            }
        }
        if let Some((agent, _)) = &self.agent {
            match module.inspect_agent(owner, &self.workspace, agent).await {
                Ok(inspection) => {
                    shown.insert(AGENT, inspection.agent_metadata);
                    shown.insert(REGISTRATION, inspection.registration_metadata);
                }
                Err(Error::AgentNotFound) => {}
                Err(error) => panic!("inspect the Agent: {error:?}"),
            }
        }
        shown
    }

    /// The concurrency metadata that each list shows of its rows, under the
    /// name of the list and of the resource.
    async fn listed(&self) -> Vec<(&'static str, &'static str, ResourceMetadata)> {
        let (module, owner) = (&self.module, &self.owner);
        let mut listed = Vec::new();
        for (evidence, metadata) in module.inspect_workspaces(owner).await.unwrap().workspaces {
            assert_eq!(evidence.workspace().id(), &self.workspace);
            listed.push(("Workspace inspections", WORKSPACE, metadata));
        }
        for found in module.list_organization_workspaces(owner).await.unwrap() {
            let name = if found.workspace.id() == &self.workspace {
                WORKSPACE
            } else {
                SECOND_WORKSPACE
            };
            listed.push(("Organization Workspaces", name, found.metadata));
        }
        for (membership, metadata, ..) in module
            .inspect_memberships(owner, &self.workspace)
            .await
            .unwrap()
        {
            let name = if membership.user_id() == owner.user_id() {
                OWNER_MEMBERSHIP
            } else {
                MEMBERSHIP
            };
            listed.push(("Workspace Member inspections", name, metadata));
        }
        for inspection in module.inspect_agents(owner, &self.workspace).await.unwrap() {
            listed.push(("Agent inspections", AGENT, inspection.agent_metadata));
            listed.push((
                "Agent inspections",
                REGISTRATION,
                inspection.registration_metadata,
            ));
        }
        listed
    }

    fn who(&self, who: Who) -> &AuthenticatedUser {
        match who {
            Who::Member => &self.member,
            Who::Heir => &self.heir,
        }
    }

    /// Takes one step through the Interface, with the actor and the instant
    /// that `context` holds. A step that checks an etag checks `if_match`.
    #[expect(
        clippy::too_many_lines,
        reason = "one match names the call of every change"
    )]
    async fn take(
        &mut self,
        step: Step,
        context: &ChangeContext,
        if_match: &IfMatch,
    ) -> Result<(), Error> {
        let module = &self.module;
        let workspace = &self.workspace;
        let agent = self.agent.as_ref();
        let agent_id = || &agent.expect("the Agent is created").0;
        let registration_id = || &agent.expect("the Agent is created").1;
        match step {
            Step::Rename(name) => module
                .update_display_name(
                    context,
                    workspace,
                    if_match,
                    WorkspaceDisplayName::parse(name)?,
                )
                .await
                .map(drop),
            Step::Archive => module.archive(context, workspace, if_match).await.map(drop),
            Step::Restore => module.restore(context, workspace, if_match).await.map(drop),
            Step::GovernArchive => module
                .archive_as_organization_owner(context, workspace, if_match)
                .await
                .map(drop),
            Step::GovernRestore => module
                .restore_as_organization_owner(context, workspace, if_match)
                .await
                .map(drop),
            Step::CreateMembership => module
                .manage_membership(
                    context,
                    workspace,
                    &self.member,
                    MembershipChange::Create(WorkspaceRole::Member),
                )
                .await
                .map(drop),
            Step::UpdateRole(role) => module
                .manage_membership(
                    context,
                    workspace,
                    &self.member,
                    MembershipChange::UpdateRole {
                        role,
                        if_match: *if_match,
                    },
                )
                .await
                .map(drop),
            Step::AssignOwner => {
                module
                    .assign_owner(
                        context,
                        &RequestId::generate(),
                        workspace,
                        self.member.user_id(),
                    )
                    .await
            }
            Step::CreateAgent => {
                let issued = module
                    .create_agent(
                        context,
                        &RequestId::generate(),
                        workspace,
                        AgentDisplayName::parse("Worker")?,
                        None,
                    )
                    .await?;
                self.agent = Some((
                    issued.agent.principal().id().clone(),
                    issued.registration.id().clone(),
                ));
                Ok(())
            }
            Step::SetAgentState(state) => module
                .set_agent_state(context, workspace, agent_id(), if_match, state)
                .await
                .map(drop),
            Step::UpdateRegistration => module
                .update_agent_registration(
                    context,
                    workspace,
                    registration_id(),
                    if_match,
                    false,
                    Some(UnixNanos::new(9_000)),
                )
                .await
                .map(drop),
            Step::RotateCredential => module
                .rotate_agent_credential(
                    context,
                    &RequestId::generate(),
                    workspace,
                    registration_id(),
                    if_match,
                )
                .await
                .map(drop),
            Step::DeleteAgent => {
                let target = AccessRemovalTarget::Agent(workspace, agent_id(), if_match);
                remove_access(module, context, target).await
            }
            Step::RemoveMembership => {
                let target = AccessRemovalTarget::WorkspaceMember(
                    workspace,
                    self.member.user_id(),
                    if_match,
                );
                remove_access(module, context, target).await
            }
            Step::TransferOwnership { if_match } => {
                let target = AccessRemovalTarget::OrganizationMember {
                    user: self.departing.user_id(),
                    if_match: &if_match,
                    transfer_to: Some(self.heir.user_id()),
                };
                remove_access(module, context, target).await
            }
        }
    }
}

/// One step, how it is refused first, and what it moves once accepted.
type Case = (Step, Refusal, &'static [(&'static str, Move)]);

/// Asserts what one accepted change at `at` did to one resource.
fn assert_moved(case: &str, before: &Kept, after: &Kept, moved: Move, at: UnixNanos) {
    // A Coral that still reads the metadata table shows and checks the etag
    // there, so every accepted change gives the resource a new one.
    let new_etag = || {
        let metadata = after.metadata.as_ref().expect(case);
        assert_ne!(Some(metadata.etag.as_str()), before.etag(), "{case}");
        assert_eq!(metadata.updated_at, at, "{case}");
        metadata
    };
    match moved {
        Move::Created => {
            assert_eq!(before.row, None, "{case}");
            assert_eq!(
                after.row,
                Some(StoredRow {
                    version: Version::FIRST,
                    created_at: Some(at),
                    updated_at: Some(at),
                }),
                "{case}"
            );
            assert_eq!(new_etag().created_at, at, "{case}");
        }
        Move::Raised | Move::MarkedDeleted => {
            let row = before.row.expect(case);
            assert_eq!(
                after.row,
                Some(StoredRow {
                    version: row.version.next().expect("the next version"),
                    created_at: row.created_at,
                    updated_at: Some(at),
                }),
                "{case}"
            );
            // The row's times are the ones the metadata table holds.
            let metadata = new_etag();
            assert_eq!(row.created_at, Some(metadata.created_at), "{case}");
        }
        Move::Removed => {
            assert!(before.row.is_some(), "{case}");
            assert_eq!(after.row, None, "{case}");
            new_etag();
        }
        Move::RemovedWithItsAgent => {
            assert!(before.row.is_some(), "{case}");
            assert_eq!(after.row, None, "{case}");
            assert_eq!(after.metadata, before.metadata, "{case}");
        }
    }
}

/// Asserts that each inspection shows the version and the times on the
/// resource's row, and that it shows every row but a deleted Agent's.
fn assert_shown_as_stored(
    case: &str,
    kept: &BTreeMap<&'static str, Kept>,
    shown: &BTreeMap<&'static str, ResourceMetadata>,
    hidden: &[&'static str],
) {
    let rows = kept
        .iter()
        .filter(|(name, _)| !hidden.contains(name))
        .filter_map(|(name, kept)| Some((*name, kept.row?.shown())))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(shown, &rows, "{case}");
}

/// Takes every step, each refused first and then accepted. A step that
/// checks an etag is refused with a stale one of `stale` form, and accepted
/// with the etag that the inspection of its resource shows. A refused step
/// moves nothing. An accepted step raises the version on the row of exactly
/// the resources it changes by one, which is what each inspection then
/// shows, and gives each of them a new etag in the metadata table.
#[expect(
    clippy::too_many_lines,
    reason = "one table names every change and what each one moves"
)]
async fn every_change_moves_its_resources_and_no_other(stale: StaleEtag) {
    let etag_mismatch: fn(&Error) -> bool = |error| matches!(error, Error::EtagMismatch);
    let cases: [Case; 17] = [
        (
            Step::Rename("Renamed"),
            Refusal::StaleEtag,
            &[(WORKSPACE, Move::Raised)],
        ),
        (
            Step::Archive,
            Refusal::StaleEtag,
            &[(WORKSPACE, Move::Raised)],
        ),
        (
            Step::Restore,
            Refusal::StaleEtag,
            &[(WORKSPACE, Move::Raised)],
        ),
        (
            Step::GovernArchive,
            Refusal::StaleEtag,
            &[(WORKSPACE, Move::Raised)],
        ),
        (
            Step::GovernRestore,
            Refusal::StaleEtag,
            &[(WORKSPACE, Move::Raised)],
        ),
        (
            Step::CreateMembership,
            Refusal::By(Who::Heir, |error| matches!(error, Error::WorkspaceNotFound)),
            &[(MEMBERSHIP, Move::Created)],
        ),
        (
            Step::CreateAgent,
            Refusal::By(Who::Member, |error| {
                matches!(
                    error,
                    Error::PermissionDenied(Denial::WorkspaceOwnerRequired)
                )
            }),
            &[(AGENT, Move::Created), (REGISTRATION, Move::Created)],
        ),
        (
            Step::UpdateRole(WorkspaceRole::Owner),
            Refusal::StaleEtag,
            &[(MEMBERSHIP, Move::Raised)],
        ),
        (
            Step::UpdateRole(WorkspaceRole::Member),
            Refusal::StaleEtag,
            &[(MEMBERSHIP, Move::Raised)],
        ),
        (
            Step::AssignOwner,
            Refusal::By(Who::Member, |error| {
                matches!(
                    error,
                    Error::PermissionDenied(Denial::OrganizationOwnerRequired)
                )
            }),
            &[(MEMBERSHIP, Move::Raised)],
        ),
        (
            Step::SetAgentState(AgentState::Disabled),
            Refusal::StaleEtag,
            &[(AGENT, Move::Raised)],
        ),
        (
            Step::SetAgentState(AgentState::Enabled),
            Refusal::StaleEtag,
            &[(AGENT, Move::Raised)],
        ),
        (
            Step::UpdateRegistration,
            Refusal::StaleEtag,
            &[(REGISTRATION, Move::Raised)],
        ),
        (
            Step::RotateCredential,
            Refusal::StaleEtag,
            &[(REGISTRATION, Move::Raised)],
        ),
        (
            Step::DeleteAgent,
            Refusal::StaleEtag,
            &[
                (AGENT, Move::MarkedDeleted),
                (REGISTRATION, Move::RemovedWithItsAgent),
            ],
        ),
        (
            Step::RemoveMembership,
            Refusal::StaleEtag,
            &[(MEMBERSHIP, Move::Removed)],
        ),
        (
            Step::TransferOwnership {
                if_match: IfMatch::version(Version::FIRST),
            },
            Refusal::StaleOrganizationMembership,
            &[
                (DEPARTING_MEMBERSHIP, Move::Removed),
                (HEIR_MEMBERSHIP, Move::Created),
            ],
        ),
    ];
    let mut changes = Changes::new().await;
    let mut at = 100;
    // The Agent's row, once it is marked deleted.
    let mut hidden: Vec<&'static str> = Vec::new();
    for (step, refusal, moves) in cases {
        let case = format!("{stale:?}: {step:?}");
        let before = changes.kept().await;
        let shown_before = changes.shown().await;
        assert_shown_as_stored(&case, &before, &shown_before, &hidden);
        let checked = step.checks();
        let owner = changes.owner.clone();
        at += 10;

        // The etag a client sends is the text an inspection showed it, read
        // as an Adapter reads it. A step that checks no etag takes none.
        let current = checked.map_or_else(IfMatch::any, |name| {
            let shown = shown_before.get(name).expect("the resource is shown");
            IfMatch::parse(&shown.version.to_string()).expect("the etag shown is well formed")
        });
        let (refused_step, refused_context, refused_if_match, is_refusal) = match refusal {
            Refusal::StaleEtag => {
                let kept = before
                    .get(checked.expect("the step checks an etag"))
                    .expect("the resource the step checks");
                (
                    step,
                    change(owner.clone(), at, 'a'),
                    IfMatch::parse(&stale.of(kept)).expect("a stale etag is well formed"),
                    etag_mismatch,
                )
            }
            Refusal::By(who, is_refusal) => (
                step,
                change(changes.who(who).clone(), at, 'a'),
                current,
                is_refusal,
            ),
            Refusal::StaleOrganizationMembership => {
                let is_refusal: fn(&Error) -> bool =
                    |error| matches!(error, Error::OrganizationAuthorityConflict);
                (
                    Step::TransferOwnership {
                        if_match: stale.organization_membership(),
                    },
                    change(owner.clone(), at, 'a'),
                    current,
                    is_refusal,
                )
            }
        };
        let error = changes
            .take(refused_step, &refused_context, &refused_if_match)
            .await
            .expect_err(&case);
        assert!(is_refusal(&error), "{case}: {error:?}");
        assert_eq!(
            changes.kept().await,
            before,
            "{case}: a refused change moves nothing"
        );
        assert_eq!(
            changes.shown().await,
            shown_before,
            "{case}: a refused change shows nothing new"
        );

        at += 10;
        changes
            .take(step, &change(owner, at, 'b'), &current)
            .await
            .unwrap_or_else(|error| panic!("{case}: {error:?}"));
        let after = changes.kept().await;
        let unmoved = Kept::default();
        for (name, kept) in &after {
            let case = format!("{case}: {name}");
            let before = before.get(name).unwrap_or(&unmoved);
            match moves.iter().find(|(moved, _)| moved == name) {
                Some((_, moved)) => {
                    assert_moved(&case, before, kept, *moved, UnixNanos::new(at));
                    if *moved == Move::MarkedDeleted {
                        hidden.push(name);
                    }
                }
                None => assert_eq!(kept, before, "{case}"),
            }
        }
        assert_shown_as_stored(&case, &after, &changes.shown().await, &hidden);
    }
}

// Catches: an inspection that shows another etag than the version on the
// row; a change that leaves the version, so that the next save with the etag
// read before it passes; a change that moves another resource's version; a
// stale version that passes; a refused change that moves a row; and a change
// that gives the resource no new etag in the metadata table, so that a Coral
// which still reads that table passes a stale save.
#[tokio::test]
async fn every_change_raises_the_version_shown_by_one_and_refuses_a_stale_version() {
    every_change_moves_its_resources_and_no_other(StaleEtag::Version).await;
}

// Catches: a UUID etag checked against the metadata table, which passes the
// save of a page opened before the rows kept their versions, and one refused
// as malformed, which blames the form's fields where the person only needs
// to reload.
#[tokio::test]
async fn every_change_refuses_the_uuid_etag_of_the_metadata_table_as_stale() {
    every_change_moves_its_resources_and_no_other(StaleEtag::Uuid).await;
}

// Catches: an inspection or a list that reads a resource's etag or times
// from the metadata table. Without its row there each one fails as corrupt,
// and once Coral stops writing the table each one shows a stale etag.
#[tokio::test]
async fn inspections_and_lists_show_each_row_without_its_metadata_table_row() {
    let mut changes = Changes::new().await;
    let owner = changes.owner.clone();
    // Each resource is at a version and at times of its own.
    for (at, step) in [
        (20, Step::CreateMembership),
        (30, Step::CreateAgent),
        (40, Step::Rename("Renamed")),
        (50, Step::UpdateRole(WorkspaceRole::Owner)),
        (60, Step::UpdateRegistration),
    ] {
        changes
            .take(step, &change(owner.clone(), at, 'a'), &IfMatch::any())
            .await
            .unwrap_or_else(|error| panic!("{step:?}: {error:?}"));
    }
    changes
        .database
        .execute_fixture_sql("DELETE FROM workspace_resource_metadata")
        .await
        .expect("delete every row of the metadata table");

    let kept = changes.kept().await;
    assert!(kept.values().all(|kept| kept.metadata.is_none()));
    let row = |name: &str| kept.get(name).and_then(|kept| kept.row).expect("a row");
    let stored = |version, created_at, updated_at| StoredRow {
        version: Version::new(version).unwrap(),
        created_at: Some(UnixNanos::new(created_at)),
        updated_at: Some(UnixNanos::new(updated_at)),
    };
    assert_eq!(row(WORKSPACE), stored(2, 10, 40));
    assert_eq!(row(MEMBERSHIP), stored(2, 20, 50));
    assert_eq!(row(AGENT), stored(1, 30, 30));
    assert_eq!(row(REGISTRATION), stored(2, 30, 60));

    assert_shown_as_stored("inspections", &kept, &changes.shown().await, &[]);
    let listed = changes.listed().await;
    for (list, name, metadata) in &listed {
        assert_eq!(*metadata, row(name).shown(), "{list}: {name}");
    }
    let names = |list: &str| {
        let mut names = listed
            .iter()
            .filter(|(of, ..)| *of == list)
            .map(|(_, name, _)| *name)
            .collect::<Vec<_>>();
        names.sort_unstable();
        names
    };
    assert_eq!(names("Workspace inspections"), [WORKSPACE]);
    assert_eq!(
        names("Organization Workspaces"),
        [WORKSPACE, SECOND_WORKSPACE]
    );
    assert_eq!(
        names("Workspace Member inspections"),
        [MEMBERSHIP, OWNER_MEMBERSHIP]
    );
    assert_eq!(names("Agent inspections"), [AGENT, REGISTRATION]);
}

// Catches: a creation or a rotation that returns other metadata than it left
// on the rows, such as the Registration's from before the rotation. The
// Adapter shows what the issuance returns, so the client's next change would
// send a stale etag and be refused.
#[tokio::test]
async fn a_creation_and_a_rotation_return_the_version_and_the_times_they_leave() {
    let changes = Changes::new().await;
    let (module, owner, workspace) = (&changes.module, &changes.owner, &changes.workspace);
    let at = |version, updated_at| ResourceMetadata {
        version: Version::new(version).unwrap(),
        created_at: Some(UnixNanos::new(20)),
        updated_at: Some(UnixNanos::new(updated_at)),
    };

    let created = module
        .create_agent(
            &change(owner.clone(), 20, '2'),
            &RequestId::generate(),
            workspace,
            AgentDisplayName::parse("Issued").unwrap(),
            None,
        )
        .await
        .unwrap();
    let agent = created.agent.principal().id().clone();
    let registration = created.registration.id().clone();
    // What the inspection of the Agent shows: the rows as they are.
    let shown = || async {
        let inspection = module
            .inspect_agent(owner, workspace, &agent)
            .await
            .unwrap();
        (inspection.agent_metadata, inspection.registration_metadata)
    };
    assert_eq!(
        (created.agent_metadata, created.registration_metadata),
        (at(1, 20), at(1, 20))
    );
    assert_eq!(shown().await, (at(1, 20), at(1, 20)));

    // The versions the creation returned are the next changes' preconditions.
    module
        .set_agent_state(
            &change(owner.clone(), 30, '3'),
            workspace,
            &agent,
            &IfMatch::version(created.agent_metadata.version),
            AgentState::Disabled,
        )
        .await
        .expect("change the Agent at the version its creation returned");
    let set_enabled = |at, if_match: IfMatch, enabled| {
        let context = change(owner.clone(), at, '4');
        let registration = &registration;
        async move {
            module
                .update_agent_registration(
                    &context,
                    workspace,
                    registration,
                    &if_match,
                    enabled,
                    None,
                )
                .await
        }
    };
    set_enabled(
        40,
        IfMatch::version(created.registration_metadata.version),
        false,
    )
    .await
    .expect("change the Registration at the version its creation returned");
    assert_eq!(shown().await, (at(2, 30), at(2, 40)));

    // The rotation reads the Registration at its second version.
    let read = at(2, 40);
    let rotated = module
        .rotate_agent_credential(
            &change(owner.clone(), 50, '5'),
            &RequestId::generate(),
            workspace,
            &registration,
            &IfMatch::version(read.version),
        )
        .await
        .unwrap();
    assert_eq!(
        (rotated.agent_metadata, rotated.registration_metadata),
        (at(2, 30), at(3, 50))
    );
    assert_eq!(shown().await, (at(2, 30), at(3, 50)));

    // The version from before the rotation is stale, and the one the
    // rotation returned is current.
    let refused = set_enabled(60, IfMatch::version(read.version), true).await;
    assert!(matches!(refused, Err(Error::EtagMismatch)), "{refused:?}");
    assert_eq!(shown().await, (at(2, 30), at(3, 50)));
    set_enabled(
        70,
        IfMatch::version(rotated.registration_metadata.version),
        true,
    )
    .await
    .expect("change the Registration at the version its rotation returned");
    assert_eq!(shown().await, (at(2, 30), at(4, 70)));
}

// Catches: a write that changes a row at another version than the one its
// caller read. The check before the write and the write are then two reads
// of the version, and a change between them is lost.
#[tokio::test]
async fn a_write_that_names_a_version_the_row_is_not_at_changes_nothing() {
    use crate::{agent_repository, repository};

    let mut changes = Changes::new().await;
    let owner = changes.owner.clone();
    for step in [Step::CreateMembership, Step::CreateAgent] {
        changes
            .take(step, &change(owner.clone(), 20, 'a'), &IfMatch::any())
            .await
            .unwrap_or_else(|error| panic!("{step:?}: {error:?}"));
    }
    let (agent_id, registration_id) = changes.agent.clone().expect("the Agent is created");
    let (workspace, member) = (&changes.workspace, changes.member.user_id());
    let before = changes.kept().await;
    // The version after the row's: a caller that read a change which was
    // then rolled back would hold it.
    let other = |name: &str| {
        let row = before.get(name).and_then(|kept| kept.row).expect("a row");
        row.version.next().expect("the next version")
    };
    let at = UnixNanos::new(500);
    let name = WorkspaceDisplayName::parse("Lost").unwrap();

    let mut tx = changes.module.database.begin().await.unwrap();
    let mut agent = agent_repository::find(&mut tx, workspace, &agent_id)
        .await
        .unwrap();
    agent.state = AgentState::Disabled;
    let mut registration =
        agent_repository::registration_by_id(&mut tx, workspace, &registration_id)
            .await
            .unwrap();
    registration.enabled = false;
    let mut refused = Vec::new();
    refused.push((
        "rename",
        repository::update_display_name(&mut tx, workspace, &name, other(WORKSPACE), at).await,
    ));
    refused.push((
        "archive",
        repository::update_lifecycle(
            &mut tx,
            workspace,
            crate::WorkspaceState::Archived,
            other(WORKSPACE),
            at,
        )
        .await,
    ));
    refused.push((
        "change the role",
        repository::write_membership_role(
            &mut tx,
            workspace,
            member,
            WorkspaceRole::Owner,
            Some(other(MEMBERSHIP)),
            at,
        )
        .await,
    ));
    refused.push((
        "remove the Membership",
        repository::delete_membership_at_version(&mut tx, workspace, member, other(MEMBERSHIP))
            .await,
    ));
    refused.push((
        "disable the Agent",
        agent_repository::update_state(&mut tx, &agent, other(AGENT), at).await,
    ));
    refused.push((
        "disable the Registration",
        agent_repository::update_registration(&mut tx, &registration, other(REGISTRATION), at)
            .await,
    ));
    refused.push((
        "rotate the credential",
        agent_repository::update_verifier(
            &mut tx,
            workspace,
            &registration_id,
            "another-verifier",
            other(REGISTRATION),
            at,
        )
        .await,
    ));
    refused.push((
        "delete the Agent",
        agent_repository::delete_current(&mut tx, workspace, &agent_id, other(AGENT), at).await,
    ));
    for (write, result) in refused {
        assert!(
            matches!(result, Err(Error::EtagMismatch)),
            "{write}: {result:?}"
        );
    }
    // Each write raises the version in the statement that changes the row,
    // so a row at the version it had is a row that no write changed.
    tx.commit().await.unwrap();
    assert_eq!(changes.kept().await, before);
}

/// The statements a Coral from before the row versions sent to create an
/// Agent and its Registration: it named its own columns, so each row took
/// the first version from the column default and holds no times.
fn agent_created_before_the_row_versions(
    workspace: &WorkspaceId,
    creator: &UserId,
    agent: &PrincipalId,
    registration: &AgentRegistrationId,
    etags: [uuid::Uuid; 2],
) -> String {
    let [agent_etag, registration_etag] = etags;
    format!(
        "INSERT INTO workspace_agents (id, workspace_id, display_name, created_by, state) \
         VALUES ('{agent}', '{workspace}', 'Peer', '{creator}', 'enabled'); \
         INSERT INTO agent_registrations \
             (id, workspace_id, agent_id, enabled, expires_at_unix_nanos, credential_verifier) \
         VALUES ('{registration}', '{workspace}', '{agent}', 1, NULL, 'peer-verifier'); \
         INSERT INTO workspace_resource_metadata \
             (workspace_id, kind, target_id, etag, created_at, updated_at) \
         VALUES ('{workspace}', 'agent', '{agent}', '{agent_etag}', 50, 50), \
             ('{workspace}', 'agent_registration', '{registration}', '{registration_etag}', 50, 50);"
    )
}

/// The statements that a Coral which still shows the metadata table's etag
/// sends to change the Agent and its Registration: it raises the version on
/// each row, dates the change there, and gives each a new etag in the table.
fn agent_changed_by_a_coral_that_shows_the_metadata_etag(
    workspace: &WorkspaceId,
    agent: &PrincipalId,
    registration: &AgentRegistrationId,
    etags: [uuid::Uuid; 2],
) -> String {
    let [agent_etag, registration_etag] = etags;
    format!(
        "UPDATE workspace_agents \
         SET state = 'disabled', version = version + 1, updated_at_unix_nanos = 60 \
         WHERE workspace_id = '{workspace}' AND id = '{agent}'; \
         UPDATE agent_registrations \
         SET enabled = 0, version = version + 1, updated_at_unix_nanos = 60 \
         WHERE workspace_id = '{workspace}' AND id = '{registration}'; \
         UPDATE workspace_resource_metadata SET etag = '{agent_etag}', updated_at = 60 \
         WHERE workspace_id = '{workspace}' AND kind = 'agent' AND target_id = '{agent}'; \
         UPDATE workspace_resource_metadata SET etag = '{registration_etag}', updated_at = 60 \
         WHERE workspace_id = '{workspace}' AND kind = 'agent_registration' \
             AND target_id = '{registration}';"
    )
}

// Catches: a row without times, as a Coral from before the row versions left
// it, read as corrupt by an inspection or a list; a change by a Coral that
// still shows the metadata table's etag that this Coral does not see, so that
// it passes the save read before it; the etag that Coral shows accepted here;
// and a change here that leaves the metadata table's etag, so that the other
// Coral passes a stale save.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one Agent is followed from an older Coral's writes through this Coral's checks"
)]
async fn an_agent_without_times_is_shown_and_another_corals_change_shows_in_its_version() {
    let (database, module) = test_module().await;
    let owner = organization_user(
        module.database.clone(),
        "user:peer-owner",
        OrganizationRole::Owner,
    )
    .await;
    let workspace = module
        .create(
            &change(owner.clone(), 10, '1'),
            WorkspaceDisplayName::parse("Peers").unwrap(),
        )
        .await
        .unwrap();
    let workspace = workspace.id();
    let agent = PrincipalId::parse(&format!("agent:{}", uuid::Uuid::new_v4())).unwrap();
    let registration = AgentRegistrationId::generate();
    let created = [uuid::Uuid::new_v4(), uuid::Uuid::new_v4()];
    let changed = [uuid::Uuid::new_v4(), uuid::Uuid::new_v4()];
    let resources = [
        Resource::Agent(workspace.clone(), agent.clone()),
        Resource::Registration(workspace.clone(), registration.clone()),
    ];
    // What the inspection of the one Agent and the list of Agents show.
    let shown = || async {
        let inspection = module
            .inspect_agent(&owner, workspace, &agent)
            .await
            .expect("inspect the Agent of an older Coral");
        let listed = module
            .inspect_agents(&owner, workspace)
            .await
            .expect("list the Agent of an older Coral");
        let [listed] = listed.as_slice() else {
            panic!("the Workspace has one Agent: {listed:?}");
        };
        let shown = [inspection.agent_metadata, inspection.registration_metadata];
        assert_eq!([listed.agent_metadata, listed.registration_metadata], shown);
        shown
    };
    let kept = || async {
        let mut tx = module.database.begin_read_snapshot().await.unwrap();
        let mut kept = Vec::new();
        for resource in &resources {
            kept.push(resource.kept(&mut tx).await);
        }
        tx.commit().await.unwrap();
        kept
    };
    let at = |version, updated_at: Option<i64>| ResourceMetadata {
        version: Version::new(version).unwrap(),
        created_at: None,
        updated_at: updated_at.map(UnixNanos::new),
    };

    database
        .execute_fixture_sql(&agent_created_before_the_row_versions(
            workspace,
            owner.user_id(),
            &agent,
            &registration,
            created,
        ))
        .await
        .unwrap();
    assert_eq!(shown().await, [at(1, None); 2]);

    database
        .execute_fixture_sql(&agent_changed_by_a_coral_that_shows_the_metadata_etag(
            workspace,
            &agent,
            &registration,
            changed,
        ))
        .await
        .unwrap();
    assert_eq!(shown().await, [at(2, Some(60)); 2]);
    let after_the_other_coral = kept().await;

    // The version read before the other Coral's change is stale, and so is
    // the etag that Coral shows now: only the version on the row passes.
    let stale = change(owner.clone(), 70, '2');
    let first = IfMatch::version(Version::FIRST);
    for (case, agent_etag, registration_etag) in [
        ("the version before", first, first),
        (
            "the metadata table's etag",
            IfMatch::token(changed[0]),
            IfMatch::token(changed[1]),
        ),
    ] {
        let refused = module
            .set_agent_state(&stale, workspace, &agent, &agent_etag, AgentState::Enabled)
            .await;
        assert!(
            matches!(refused, Err(Error::EtagMismatch)),
            "{case}: {refused:?}"
        );
        let refused = module
            .update_agent_registration(
                &stale,
                workspace,
                &registration,
                &registration_etag,
                true,
                None,
            )
            .await;
        assert!(
            matches!(refused, Err(Error::EtagMismatch)),
            "{case}: {refused:?}"
        );
        assert_eq!(kept().await, after_the_other_coral, "{case}");
    }

    // The etag this Coral shows is current. Its change raises each version
    // and dates the change on the row, which still has no creation time, and
    // gives each resource a new etag in the metadata table for the other
    // Coral.
    let [agent_shown, registration_shown] = shown().await;
    let current = change(owner.clone(), 80, '3');
    module
        .set_agent_state(
            &current,
            workspace,
            &agent,
            &IfMatch::version(agent_shown.version),
            AgentState::Enabled,
        )
        .await
        .expect("enable the Agent of an older Coral");
    module
        .update_agent_registration(
            &current,
            workspace,
            &registration,
            &IfMatch::version(registration_shown.version),
            true,
            None,
        )
        .await
        .expect("enable the Registration of an older Coral");
    assert_eq!(shown().await, [at(3, Some(80)); 2]);
    for ((after, before), earlier) in kept().await.iter().zip(&after_the_other_coral).zip(changed) {
        assert_eq!(after.row.map(StoredRow::shown), Some(at(3, Some(80))));
        let etag = after.etag().expect("the metadata row");
        assert_ne!(Some(etag), before.etag());
        let etag = uuid::Uuid::parse_str(etag).expect("the metadata table's etag is a UUID");
        assert_ne!(etag, earlier);
    }
}

// Catches: an etag in a form that only one of two Corals reads, a version
// with more than one spelling, and a precondition of one form that passes
// for a resource whose etag has the other form.
#[test]
fn an_etag_is_the_canonical_text_of_a_version_or_a_uuid() {
    let uuid = uuid::Uuid::new_v4();
    let version = |count| Version::new(count).unwrap();
    for (etag, expected) in [
        ("1".to_owned(), IfMatch::version(Version::FIRST)),
        ("42".to_owned(), IfMatch::version(version(42))),
        (uuid.to_string(), IfMatch::token(uuid)),
    ] {
        assert_eq!(IfMatch::parse(&etag).expect(&etag), expected);
    }
    for etag in ["", "0", "-1", "+1", "01", "stale"] {
        let refused = IfMatch::parse(etag);
        assert!(
            matches!(&refused, Err(Error::InvalidInput(message)) if message == "etag is required"),
            "{etag:?}: {refused:?}"
        );
    }

    let at_version = IfMatch::version(version(2));
    assert!(at_version.admits(version(2)));
    assert!(!at_version.admits(version(1)));
    assert!(!at_version.admits(version(3)));
    assert!(!at_version.admits_token(&uuid));
    let at_token = IfMatch::token(uuid);
    assert!(at_token.admits_token(&uuid));
    assert!(!at_token.admits_token(&uuid::Uuid::new_v4()));
    assert!(!at_token.admits(Version::FIRST));
    assert!(IfMatch::any().admits(version(7)));
    assert!(IfMatch::any().admits_token(&uuid));
}

// Catches: a request identity stored under the spelling the caller sent, so
// that the same request in another spelling runs twice.
#[test]
fn a_request_id_is_a_uuid_in_any_form_and_has_one_text() {
    let id = uuid::Uuid::new_v4();
    let hyphenated = id.hyphenated().to_string();
    for form in [
        hyphenated.clone(),
        hyphenated.to_uppercase(),
        id.simple().to_string(),
        id.braced().to_string(),
    ] {
        assert_eq!(
            RequestId::parse(&form).expect(&form).to_string(),
            hyphenated
        );
    }
    for refused in ["", "1", "stale"] {
        assert!(
            matches!(
                RequestId::parse(refused),
                Err(Error::InvalidInput(message)) if message == "request_id must be a UUID"
            ),
            "{refused:?}"
        );
    }
    assert_ne!(RequestId::generate(), RequestId::generate());
}
