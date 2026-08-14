//! The single decision point for workspace-scoped access.
//!
//! Every workspace-scoped request answers one question — may this principal do
//! this to this workspace — and it is answered here rather than at each RPC, so
//! the concealment and owner-floor rules cannot drift apart between callers.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the policy lands before the services and RPCs that enforce it"
    )
)]

use std::sync::Arc;

use crate::bootstrap::AppError;
use crate::identity::{LOCAL_PRINCIPAL_ID, Principal, PrincipalKind};
use crate::state::db::{CoralDb, DbRepos};
use crate::workspaces::{MemberRole, WorkspaceName};

/// What a caller wants to do with one workspace.
///
/// Two levels, not one per RPC: the product distinction that matters is
/// reading a workspace's contents versus changing the workspace or who may
/// reach it. Every RPC classifies into one of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkspaceAction {
    Read,
    Manage,
}

/// Whether this deployment admits the built-in `coral:local` principal.
///
/// The local principal has no membership rows and no credential behind it, so
/// admitting it is a whole-deployment decision rather than a per-request one.
/// The composition root resolves it once.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum LocalPrincipalPolicy {
    /// Reject `coral:local` outright. This is the default because a
    /// deployment that has not said otherwise is a shared one, and there the
    /// local principal is an unauthenticated bypass of every rule below.
    #[default]
    NoLocalPrincipal,
    /// Treat `coral:local` as owner of everything, without reading membership
    /// state. Only a single-user deployment may opt into this.
    ImplicitOwner,
}

/// Authorizes workspace-scoped requests against membership state.
#[derive(Clone)]
pub(crate) struct WorkspaceAuthorizer {
    db: Arc<CoralDb>,
    local_principal: LocalPrincipalPolicy,
}

impl WorkspaceAuthorizer {
    /// Builds an authorizer under the policy the composition root resolved for
    /// this deployment.
    pub(crate) const fn with_local_principal_policy(
        db: Arc<CoralDb>,
        local_principal: LocalPrincipalPolicy,
    ) -> Self {
        Self {
            db,
            local_principal,
        }
    }

    /// Builds an authorizer that rejects the local principal.
    pub(crate) const fn new(db: Arc<CoralDb>) -> Self {
        Self::with_local_principal_policy(db, LocalPrincipalPolicy::NoLocalPrincipal)
    }

    /// Builds an authorizer that treats the local principal as owner.
    pub(crate) const fn trusting_local_principal(db: Arc<CoralDb>) -> Self {
        Self::with_local_principal_policy(db, LocalPrincipalPolicy::ImplicitOwner)
    }

    pub(crate) const fn local_principal_policy(&self) -> LocalPrincipalPolicy {
        self.local_principal
    }

    /// Decides whether `principal` may perform `action` on `workspace`.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::WorkspaceNotFound`] when the caller may not know
    /// the workspace exists, and [`AppError::PermissionDenied`] only when they
    /// already may. The two are not interchangeable: swapping them turns the
    /// workspace namespace into an oracle for callers with no access to it.
    pub(crate) async fn authorize(
        &self,
        principal: &Principal,
        workspace: &WorkspaceName,
        action: WorkspaceAction,
    ) -> Result<(), AppError> {
        // Both local-principal outcomes are settled before any lookup: under
        // `ImplicitOwner` there is no membership row to find, and under
        // `NoLocalPrincipal` an injected local principal must not be able to
        // reach the directory or membership tables at all.
        if let Some(decision) = self.decide_for_local_principal(principal) {
            return decision;
        }

        // The control-plane restriction is evaluated before any role, so a
        // workspace role can never promote an agent credential: the same
        // person's browser token manages the workspace and their MCP token
        // does not.
        if principal.kind() == PrincipalKind::Agent && action == WorkspaceAction::Manage {
            return Err(AppError::PermissionDenied(format!(
                "agent credentials cannot manage workspace '{workspace}'"
            )));
        }

        let mut session = self.db.as_ref();
        let role = session
            .workspace_members()
            .role_for_user_id(workspace.as_str(), principal.id().as_str())
            .await?
            .ok_or_else(|| conceal(workspace))?;
        // A membership in an ownerless workspace grants nothing, so its stale
        // members are concealed exactly as non-members are. An `Owner` row is
        // itself an owner, so only a member's row can be in that state.
        if role == MemberRole::Member
            && session
                .workspace_members()
                .owner_count(workspace.as_str())
                .await?
                == 0
        {
            return Err(conceal(workspace));
        }

        if role.allows(action) {
            Ok(())
        } else {
            Err(AppError::PermissionDenied(format!(
                "owner access is required for workspace '{workspace}'"
            )))
        }
    }

    /// Decides whether this deployment admits `principal` at all.
    ///
    /// A self-scoped request reads nothing but the caller's own row, so this
    /// is the whole of its check. It still runs first: under
    /// `NoLocalPrincipal` an injected `coral:local` must be refused before any
    /// lookup, rather than being handed the built-in row this deployment does
    /// not recognize.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::PermissionDenied`] for the local principal on a
    /// deployment that does not admit it.
    pub(crate) fn admit(&self, principal: &Principal) -> Result<(), AppError> {
        self.decide_for_local_principal(principal).unwrap_or(Ok(()))
    }

    /// Decides whether `principal` may create a workspace at all.
    ///
    /// Creation is the one control-plane act with no workspace to check, so
    /// [`Self::authorize`] cannot reach it: its agent restriction fires only
    /// against a workspace that already exists. Left to [`Self::admit`] alone,
    /// an agent credential would create a workspace, be granted `Owner` on it
    /// by the creation transaction, and hold exactly the authority `authorize`
    /// refuses to let any role confer on it.
    ///
    /// Creation is otherwise open: any authenticated person may make a
    /// workspace, and no membership state is read to decide it.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::PermissionDenied`] for an agent credential, and for
    /// the local principal on a deployment that does not admit it.
    pub(crate) fn authorize_creation(&self, principal: &Principal) -> Result<(), AppError> {
        if let Some(decision) = self.decide_for_local_principal(principal) {
            return decision;
        }

        if principal.kind() == PrincipalKind::Agent {
            return Err(AppError::PermissionDenied(
                "agent credentials cannot create a workspace".to_string(),
            ));
        }
        Ok(())
    }

    /// Decides whether `principal` may read the deployment's user directory.
    ///
    /// Directory authority is workspace ownership, not principal kind: the
    /// directory exists so an owner can name somebody as a member, and a
    /// caller who owns nothing has nobody to name. The denial is plain rather
    /// than concealing, because the directory is deployment-wide — refusing it
    /// hides no particular person's existence.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::PermissionDenied`] for a caller this deployment
    /// admits but who owns no workspace.
    pub(crate) async fn authorize_directory(&self, principal: &Principal) -> Result<(), AppError> {
        if let Some(decision) = self.decide_for_local_principal(principal) {
            return decision;
        }

        let mut session = self.db.as_ref();
        let owns_a_workspace = session
            .workspace_members()
            .workspaces_for_user_id(principal.id().as_str())
            .await?
            .iter()
            .any(|(_, role)| *role == MemberRole::Owner);
        if owns_a_workspace {
            Ok(())
        } else {
            Err(AppError::PermissionDenied(
                "reading the user directory requires owning a workspace".to_string(),
            ))
        }
    }

    /// Settles the built-in local principal, or reports `None` for a caller
    /// whose authority comes from membership state instead.
    fn decide_for_local_principal(&self, principal: &Principal) -> Option<Result<(), AppError>> {
        (principal.id().as_str() == LOCAL_PRINCIPAL_ID).then(|| match self.local_principal {
            LocalPrincipalPolicy::ImplicitOwner => Ok(()),
            LocalPrincipalPolicy::NoLocalPrincipal => Err(AppError::PermissionDenied(
                "the local principal is not available on this deployment".to_string(),
            )),
        })
    }
}

/// Reports an inaccessible workspace as an absent one.
fn conceal(workspace: &WorkspaceName) -> AppError {
    AppError::WorkspaceNotFound(workspace.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::{TempDir, tempdir};

    use super::{LocalPrincipalPolicy, WorkspaceAction, WorkspaceAuthorizer};
    use crate::bootstrap::AppError;
    use crate::identity::{Principal, PrincipalKind};
    use crate::state::db::{
        CoralDb, DbRepos, LoginIdentity, LoginProvisioning, ResolvedDatabaseConfig,
    };
    use crate::workspaces::{MemberRole, WorkspaceName};

    const BOTH_ACTIONS: [WorkspaceAction; 2] = [WorkspaceAction::Read, WorkspaceAction::Manage];

    /// An unmigrated database is the proof that no lookup happened: every
    /// membership query against it fails, so a decision that still succeeds or
    /// still denies reached its answer without touching the tables.
    #[tokio::test]
    async fn implicit_owner_answers_for_the_local_principal_without_membership_work() {
        let (_temp, db) = unmigrated_database().await;
        let authorizer = WorkspaceAuthorizer::trusting_local_principal(db);
        assert_eq!(
            authorizer.local_principal_policy(),
            LocalPrincipalPolicy::ImplicitOwner
        );

        for action in BOTH_ACTIONS {
            authorizer
                .authorize(&Principal::local(), &workspace("unmigrated"), action)
                .await
                .expect("the implicit owner is authorized without reading membership state");
        }
    }

    #[tokio::test]
    async fn no_local_principal_rejects_the_local_principal_before_any_lookup() {
        let (_temp, db) = unmigrated_database().await;
        let authorizer = WorkspaceAuthorizer::new(db);
        assert_eq!(
            authorizer.local_principal_policy(),
            LocalPrincipalPolicy::NoLocalPrincipal
        );

        for action in BOTH_ACTIONS {
            assert!(matches!(
                authorizer
                    .authorize(&Principal::local(), &workspace("unmigrated"), action)
                    .await,
                Err(AppError::PermissionDenied(_))
            ));
        }
    }

    #[tokio::test]
    async fn an_agent_is_denied_the_control_plane_before_any_role_is_read() {
        let (_temp, db) = unmigrated_database().await;
        let authorizer = WorkspaceAuthorizer::new(db);
        let agent = Principal::parse("agent-with-no-row", PrincipalKind::Agent).expect("agent");

        assert!(matches!(
            authorizer
                .authorize(&agent, &workspace("unmigrated"), WorkspaceAction::Manage)
                .await,
            Err(AppError::PermissionDenied(_))
        ));
    }

    /// Creation is the control-plane act with no workspace behind it, so the
    /// unmigrated database is again the proof: the answer is reached without a
    /// membership row existing anywhere to reach it from.
    #[tokio::test]
    async fn an_agent_cannot_create_a_workspace_while_a_person_can() {
        let (_temp, db) = unmigrated_database().await;
        let authorizer = WorkspaceAuthorizer::new(db);

        assert!(matches!(
            authorizer.authorize_creation(
                &Principal::parse("someone", PrincipalKind::Agent).expect("agent")
            ),
            Err(AppError::PermissionDenied(_))
        ));
        authorizer
            .authorize_creation(&Principal::parse("someone", PrincipalKind::User).expect("user"))
            .expect("any authenticated person may create a workspace");
        assert!(
            matches!(
                authorizer.authorize_creation(&Principal::local()),
                Err(AppError::PermissionDenied(_))
            ),
            "the deployment decision still comes first"
        );
    }

    #[tokio::test]
    async fn an_owner_manages_while_a_member_only_reads() {
        let (_temp, db) = migrated_database().await;
        let workspace = workspace("team-access");
        let owner_id = seed_user(&db, "owner").await;
        let member_id = seed_user(&db, "member").await;
        create_workspace(&db, &workspace).await;
        grant(&db, &workspace, &owner_id, MemberRole::Owner).await;
        grant(&db, &workspace, &member_id, MemberRole::Member).await;
        let authorizer = WorkspaceAuthorizer::new(db);
        let owner = Principal::parse(&owner_id, PrincipalKind::User).expect("owner");
        let member = Principal::parse(&member_id, PrincipalKind::User).expect("member");

        for action in BOTH_ACTIONS {
            authorizer
                .authorize(&owner, &workspace, action)
                .await
                .expect("an owner may do anything in their workspace");
        }
        authorizer
            .authorize(&member, &workspace, WorkspaceAction::Read)
            .await
            .expect("a member may read");
        assert!(
            matches!(
                authorizer
                    .authorize(&member, &workspace, WorkspaceAction::Manage)
                    .await,
                Err(AppError::PermissionDenied(_))
            ),
            "a member must be denied, not concealed: they already know the workspace"
        );

        // The agent restriction narrows a role; it never widens or replaces it.
        let member_agent = Principal::parse(&member_id, PrincipalKind::Agent).expect("agent");
        authorizer
            .authorize(&member_agent, &workspace, WorkspaceAction::Read)
            .await
            .expect("a member's agent credential may still read");
    }

    /// The three ways a workspace can be unreachable must be indistinguishable
    /// from outside, or the error itself reports whether a name is taken.
    #[tokio::test]
    async fn nonexistent_nonmember_and_ownerless_workspaces_are_all_concealed() {
        let (_temp, db) = migrated_database().await;
        let owned = workspace("owned-elsewhere");
        let ownerless = workspace("ownerless");
        let owner_id = seed_user(&db, "owner").await;
        let outsider_id = seed_user(&db, "outsider").await;
        let stale_member_id = seed_user(&db, "stale-member").await;
        create_workspace(&db, &owned).await;
        grant(&db, &owned, &owner_id, MemberRole::Owner).await;
        create_workspace(&db, &ownerless).await;
        grant(&db, &ownerless, &stale_member_id, MemberRole::Member).await;
        let authorizer = WorkspaceAuthorizer::new(db);
        let outsider = Principal::parse(&outsider_id, PrincipalKind::User).expect("outsider");
        let stale_member =
            Principal::parse(&stale_member_id, PrincipalKind::User).expect("stale member");

        let cases = [
            (&outsider, workspace("never-created")),
            (&outsider, owned),
            (&stale_member, ownerless),
        ];
        for (principal, workspace) in &cases {
            for action in BOTH_ACTIONS {
                assert!(
                    matches!(
                        authorizer.authorize(principal, workspace, action).await,
                        Err(AppError::WorkspaceNotFound(ref name)) if name == workspace.as_str()
                    ),
                    "{workspace} must be concealed for {action:?}"
                );
            }
        }

        // The control-plane denial answers before any lookup, so an agent is
        // told the same thing about a workspace that exists and one that never
        // did. That is what keeps the earlier answer from becoming an oracle.
        let agent = Principal::parse(&outsider_id, PrincipalKind::Agent).expect("agent");
        for (_, workspace) in &cases {
            assert!(matches!(
                authorizer
                    .authorize(&agent, workspace, WorkspaceAction::Manage)
                    .await,
                Err(AppError::PermissionDenied(_))
            ));
        }
    }

    fn workspace(name: &str) -> WorkspaceName {
        WorkspaceName::parse(name).expect("workspace name")
    }

    async fn unmigrated_database() -> (TempDir, Arc<CoralDb>) {
        let temp = tempdir().expect("temp dir");
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite {
            path: temp.path().join("coral.sqlite"),
        })
        .await
        .expect("open sqlite");
        (temp, Arc::new(db))
    }

    async fn migrated_database() -> (TempDir, Arc<CoralDb>) {
        let (temp, db) = unmigrated_database().await;
        db.migrate().await.expect("migrate sqlite");
        (temp, db)
    }

    /// Provisions one directory user through the production login seam, so the
    /// `user_id` the authorizer is handed is the one a real login would carry.
    async fn seed_user(db: &CoralDb, subject: &str) -> String {
        let provisioned = db
            .user_state()
            .provision_login(LoginIdentity {
                issuer: "https://issuer.test/authorization",
                subject,
                display_name: None,
                principal_claim: subject,
                now_unix_nanos: 1,
            })
            .await
            .expect("provision user");
        match provisioned {
            LoginProvisioning::Provisioned(user) => user.user_id,
            LoginProvisioning::IssuerMismatch { stored_issuer } => {
                panic!("expected a provisioned user, got a mismatch with issuer {stored_issuer}")
            }
        }
    }

    async fn create_workspace(db: &CoralDb, workspace: &WorkspaceName) {
        let mut tx = db.begin().await.expect("begin workspace creation");
        tx.workspaces()
            .create(workspace.as_str(), 1)
            .await
            .expect("create workspace");
        tx.commit().await.expect("commit workspace creation");
    }

    async fn grant(db: &CoralDb, workspace: &WorkspaceName, user_id: &str, role: MemberRole) {
        let mut session = db;
        session
            .workspace_members()
            .upsert(workspace.as_str(), user_id, role, 2)
            .await
            .expect("grant membership");
    }
}
