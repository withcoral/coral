//! The version each resource's row keeps, beside the concurrency metadata
//! that Coral still shows and still checks a UUID etag against.
use std::collections::BTreeMap;

use coral_db::{Transaction, UnixNanos, Version};
use coral_test_support::TestDatabase;
use sea_query::{Alias, Expr, ExprTrait as _, Query};

use super::{change, organization_user, remove_access, test_module};
use crate::{
    AccessRemovalTarget, AgentDisplayName, AgentRegistrationId, AgentState, AuthenticatedUser,
    ChangeContext, Denial, Error, IfMatch, LifecycleTargetKind, MembershipChange, OrganizationRole,
    PrincipalId, RequestId, UserId, WorkspaceDisplayName, WorkspaceId, WorkspaceModule,
    WorkspaceRole, local_organization_owner,
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

    /// Reads the row's version and times past the Interface. No inspection
    /// shows a row's version yet, nor the times on an Agent's or a
    /// Registration's row, and no read finds a deleted Agent's row.
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

    /// Reads what Coral keeps of the resource for its conflict check.
    async fn kept(&self, tx: &mut Transaction<'_>) -> Kept {
        let metadata = match crate::management_repository::metadata(
            tx,
            self.workspace(),
            self.kind(),
            &self.target_id(),
        )
        .await
        {
            Ok(metadata) => Some(StoredMetadata {
                etag: metadata.etag,
                created_at: metadata.created_at,
                updated_at: metadata.updated_at,
            }),
            Err(Error::CorruptData(_)) => None,
            Err(error) => panic!("read the metadata: {error}"),
        };
        Kept {
            metadata,
            row: self.row(tx).await,
        }
    }
}

/// The concurrency metadata of one resource: what an inspection shows.
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

/// What Coral keeps of one resource for its conflict check. A resource that
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

/// What one accepted change does to one resource.
#[derive(Debug, Clone, Copy)]
enum Move {
    /// The resource's first row, at the first version, with a new etag.
    Created,
    /// A change to the row: its version rises by one, and the etag is new.
    Raised,
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
    /// Removes the second Workspace's one Owner from the Organization at
    /// this revision of their Organization Membership: the heir takes over.
    TransferOwnership {
        revision: u64,
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
    /// As another step, which differs in what makes it refused.
    As(Step, fn(&Error) -> bool),
}

#[derive(Debug, Clone, Copy)]
enum Who {
    Member,
    Heir,
}

/// The form of the etag a change sends.
#[derive(Debug, Clone, Copy)]
enum EtagForm {
    /// The UUID in the metadata: the etag an inspection shows.
    Metadata,
    /// The version on the row.
    RowVersion,
}

impl EtagForm {
    fn current(self, kept: &Kept) -> String {
        match self {
            Self::Metadata => kept.etag().expect("the resource has metadata").to_owned(),
            Self::RowVersion => kept
                .row
                .expect("the resource has a row")
                .version
                .to_string(),
        }
    }

    /// An etag of this form that is not the resource's.
    fn stale(self, kept: &Kept) -> String {
        match self {
            Self::Metadata => uuid::Uuid::new_v4().to_string(),
            Self::RowVersion => {
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
    _database: TestDatabase,
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
            _database: database,
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
                    MembershipChange::UpdateRole(role),
                )
                .await
                .map(drop),
            Step::AssignOwner => {
                module
                    .assign_owner(context, workspace, self.member.user_id())
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
                let target = AccessRemovalTarget::WorkspaceMember(workspace, self.member.user_id());
                remove_access(module, context, target).await
            }
            Step::TransferOwnership { revision } => {
                let target = AccessRemovalTarget::OrganizationMember {
                    user: self.departing.user_id(),
                    revision,
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
        Move::Raised => {
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
            // The row's times are the ones the metadata shows.
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

/// Takes every step, each refused first and then accepted, with etags of
/// `form`. A refused step moves nothing. An accepted step gives exactly the
/// resources it changes a new etag in the metadata, and raises the version
/// on each one's row by one.
#[expect(
    clippy::too_many_lines,
    reason = "one table names every change and what each one moves"
)]
async fn every_change_moves_its_resources_and_no_other(form: EtagForm) {
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
                (AGENT, Move::Raised),
                (REGISTRATION, Move::RemovedWithItsAgent),
            ],
        ),
        (
            Step::RemoveMembership,
            Refusal::StaleEtag,
            &[(MEMBERSHIP, Move::Removed)],
        ),
        (
            Step::TransferOwnership { revision: 1 },
            Refusal::As(Step::TransferOwnership { revision: 2 }, |error| {
                matches!(error, Error::OrganizationAuthorityConflict)
            }),
            &[
                (DEPARTING_MEMBERSHIP, Move::Removed),
                (HEIR_MEMBERSHIP, Move::Created),
            ],
        ),
    ];
    let mut changes = Changes::new().await;
    let mut at = 100;
    for (step, refusal, moves) in cases {
        let case = format!("{form:?}: {step:?}");
        let before = changes.kept().await;
        let checked = step
            .checks()
            .map(|name| before.get(name).expect("the resource the step checks"));
        let owner = changes.owner.clone();
        at += 10;

        // A step that checks no etag takes none.
        let current = checked.map_or_else(IfMatch::any, |kept| {
            IfMatch::parse(&form.current(kept)).expect("the current etag is well formed")
        });
        let (refused_step, refused_context, refused_if_match, is_refusal) = match refusal {
            Refusal::StaleEtag => {
                let stale = form.stale(checked.expect("the step checks an etag"));
                (
                    step,
                    change(owner.clone(), at, 'a')
                        .with_etag(&stale)
                        .expect("a stale etag is well formed"),
                    IfMatch::parse(&stale).expect("a stale etag is well formed"),
                    etag_mismatch,
                )
            }
            Refusal::By(who, is_refusal) => (
                step,
                change(changes.who(who).clone(), at, 'a'),
                current,
                is_refusal,
            ),
            Refusal::As(other, is_refusal) => {
                (other, change(owner.clone(), at, 'a'), current, is_refusal)
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

        at += 10;
        let mut context = change(owner, at, 'b');
        if let Some(checked) = checked {
            context = context
                .with_etag(&form.current(checked))
                .expect("the current etag is well formed");
        }
        changes
            .take(step, &context, &current)
            .await
            .unwrap_or_else(|error| panic!("{case}: {error:?}"));
        let after = changes.kept().await;
        let unmoved = Kept::default();
        for (name, kept) in &after {
            let case = format!("{case}: {name}");
            let before = before.get(name).unwrap_or(&unmoved);
            match moves.iter().find(|(moved, _)| moved == name) {
                Some((_, moved)) => assert_moved(&case, before, kept, *moved, UnixNanos::new(at)),
                None => assert_eq!(kept, before, "{case}"),
            }
        }
    }
}

// Catches: a change that gives a resource a new etag and leaves the version
// on its row, so that a peer who checks the row passes a stale save; a
// change that moves another resource's version; a refused change that moves
// either record; an Agent or a Registration row written without its times.
#[tokio::test]
async fn every_change_raises_the_row_version_by_one_with_the_metadata_etag() {
    every_change_moves_its_resources_and_no_other(EtagForm::Metadata).await;
}

// Catches: a version refused as an etag, a version checked against the
// metadata or not checked at all, and a stale version that passes.
#[tokio::test]
async fn a_change_takes_the_row_version_as_its_etag_as_it_takes_the_metadata_etag() {
    every_change_moves_its_resources_and_no_other(EtagForm::RowVersion).await;
}

/// The statements a Coral from before the row versions sends to create an
/// Agent and its Registration: it names its own columns, so each row takes
/// the first version from the column default and holds no times.
fn agent_created_by_an_older_coral(
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

/// The statements that Coral sends to change the Agent and its
/// Registration: it gives each a new etag in the metadata and leaves the
/// version on the row.
fn agent_changed_by_an_older_coral(
    workspace: &WorkspaceId,
    agent: &PrincipalId,
    registration: &AgentRegistrationId,
    etags: [uuid::Uuid; 2],
) -> String {
    let [agent_etag, registration_etag] = etags;
    format!(
        "UPDATE workspace_agents SET state = 'disabled' \
         WHERE workspace_id = '{workspace}' AND id = '{agent}'; \
         UPDATE agent_registrations SET enabled = 0 \
         WHERE workspace_id = '{workspace}' AND id = '{registration}'; \
         UPDATE workspace_resource_metadata SET etag = '{agent_etag}', updated_at = 60 \
         WHERE workspace_id = '{workspace}' AND kind = 'agent' AND target_id = '{agent}'; \
         UPDATE workspace_resource_metadata SET etag = '{registration_etag}', updated_at = 60 \
         WHERE workspace_id = '{workspace}' AND kind = 'agent_registration' \
             AND target_id = '{registration}';"
    )
}

// Catches: an inspection that shows the row's version while a Coral from
// before the row versions still serves, which shows a client an etag that
// the older Coral refuses as malformed; a UUID etag checked against the row,
// which passes a save that is stale after the older Coral's change; and a
// row without times, as the older Coral leaves it, read as corrupt.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one Agent is followed from an older Coral's writes through this Coral's checks"
)]
async fn a_change_by_an_older_coral_shows_in_the_etag_and_refuses_the_one_read_before_it() {
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
    let shown = || async {
        let inspection = module
            .inspect_agent(&owner, workspace, &agent)
            .await
            .expect("inspect the Agent of an older Coral");
        (
            (
                inspection.agent_metadata.etag,
                inspection.agent_metadata.created_at.get(),
                inspection.agent_metadata.updated_at.get(),
            ),
            (
                inspection.registration_metadata.etag,
                inspection.registration_metadata.created_at.get(),
                inspection.registration_metadata.updated_at.get(),
            ),
        )
    };
    let rows = || async {
        let mut tx = module.database.begin_read_snapshot().await.unwrap();
        let rows = [
            Resource::Agent(workspace.clone(), agent.clone())
                .row(&mut tx)
                .await,
            Resource::Registration(workspace.clone(), registration.clone())
                .row(&mut tx)
                .await,
        ];
        tx.commit().await.unwrap();
        rows
    };
    let first_without_times = Some(StoredRow {
        version: Version::FIRST,
        created_at: None,
        updated_at: None,
    });

    database
        .execute_fixture_sql(&agent_created_by_an_older_coral(
            workspace,
            owner.user_id(),
            &agent,
            &registration,
            created,
        ))
        .await
        .unwrap();
    assert_eq!(
        shown().await,
        (
            (created[0].to_string(), 50, 50),
            (created[1].to_string(), 50, 50)
        )
    );
    database
        .execute_fixture_sql(&agent_changed_by_an_older_coral(
            workspace,
            &agent,
            &registration,
            changed,
        ))
        .await
        .unwrap();
    let after_the_older_coral = (
        (changed[0].to_string(), 50, 60),
        (changed[1].to_string(), 50, 60),
    );
    assert_eq!(shown().await, after_the_older_coral);
    assert_eq!(rows().await, [first_without_times; 2]);

    // The etag a client read before the older Coral's change is stale.
    let stale = change(owner.clone(), 70, '2');
    assert!(matches!(
        module
            .set_agent_state(
                &stale,
                workspace,
                &agent,
                &IfMatch::token(created[0]),
                AgentState::Enabled,
            )
            .await,
        Err(Error::EtagMismatch)
    ));
    assert!(matches!(
        module
            .update_agent_registration(
                &stale,
                workspace,
                &registration,
                &IfMatch::token(created[1]),
                true,
                None,
            )
            .await,
        Err(Error::EtagMismatch)
    ));
    assert_eq!(shown().await, after_the_older_coral);
    assert_eq!(rows().await, [first_without_times; 2]);

    // The etag it shows now is current. This Coral's change raises each
    // version and dates the change on the row, which still has no creation
    // time: only the metadata holds that.
    let current = change(owner.clone(), 80, '3');
    module
        .set_agent_state(
            &current,
            workspace,
            &agent,
            &IfMatch::token(changed[0]),
            AgentState::Enabled,
        )
        .await
        .expect("enable the Agent of an older Coral");
    module
        .update_agent_registration(
            &current,
            workspace,
            &registration,
            &IfMatch::token(changed[1]),
            true,
            None,
        )
        .await
        .expect("enable the Registration of an older Coral");
    let raised = Some(StoredRow {
        version: Version::new(2).unwrap(),
        created_at: None,
        updated_at: Some(UnixNanos::new(80)),
    });
    assert_eq!(rows().await, [raised; 2]);
    let ((agent_etag, ..), (registration_etag, ..)) = shown().await;
    for (etag, earlier) in [(agent_etag, changed[0]), (registration_etag, changed[1])] {
        let etag = uuid::Uuid::parse_str(&etag).expect("the etag shown is still a UUID");
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
    let context = || change(local_organization_owner(), 1, '1');
    for (etag, expected) in [
        ("1".to_owned(), IfMatch::version(Version::FIRST)),
        ("42".to_owned(), IfMatch::version(version(42))),
        (uuid.to_string(), IfMatch::token(uuid)),
    ] {
        assert_eq!(IfMatch::parse(&etag).expect(&etag), expected);
        let read = context().with_etag(&etag).expect(&etag);
        assert_eq!(read.if_match(), Some(&expected));
        assert_eq!(read.expected_etag(), Some(etag.as_str()));
    }
    for etag in ["", "0", "-1", "+1", "01", "stale"] {
        for refused in [
            IfMatch::parse(etag).map(drop),
            context().with_etag(etag).map(drop),
        ] {
            assert!(
                matches!(&refused, Err(Error::InvalidInput(message)) if message == "etag is required"),
                "{etag:?}: {refused:?}"
            );
        }
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
