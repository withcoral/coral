//! Self-scoped and directory-wide reads of the user directory.

use std::sync::Arc;

use crate::bootstrap::AppError;
use crate::identity::Principal;
use crate::state::db::{CoralDb, DbRepos};
use crate::users::model::User;
use crate::workspaces::authorization::WorkspaceAuthorizer;

/// App-owned reads of the user directory.
#[derive(Clone)]
pub(crate) struct UserManager {
    db: Arc<CoralDb>,
    authorizer: WorkspaceAuthorizer,
}

impl UserManager {
    pub(crate) const fn new(db: Arc<CoralDb>, authorizer: WorkspaceAuthorizer) -> Self {
        Self { db, authorizer }
    }

    /// Reads the caller's own directory entry.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::PermissionDenied`] when this deployment does not
    /// admit the caller at all, and [`AppError::UserNotFound`] when an
    /// admitted caller has no directory row — an authenticated caller always
    /// has one, because login provisioning writes it.
    pub(crate) async fn current_user(&self, principal: &Principal) -> Result<User, AppError> {
        self.authorizer.admit(principal)?;

        let user_id = principal.id().as_str();
        let mut session = self.db.as_ref();
        session
            .users()
            .get_by_user_id(user_id)
            .await?
            .map(|record| User {
                user_id: record.user_id,
                display_name: record.display_name,
            })
            .ok_or_else(|| AppError::UserNotFound(user_id.to_string()))
    }

    /// Lists everybody an owner may name in a membership.
    ///
    /// The listing is deployment-wide rather than workspace-scoped on purpose:
    /// its whole use is naming somebody who is not yet a member of anything
    /// the caller can see.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::PermissionDenied`] when the caller has no directory
    /// authority.
    pub(crate) async fn list_users(&self, principal: &Principal) -> Result<Vec<User>, AppError> {
        self.authorizer.authorize_directory(principal).await?;

        let mut session = self.db.as_ref();
        Ok(session
            .users()
            .list()
            .await?
            .into_iter()
            .map(|record| User {
                user_id: record.user_id,
                display_name: record.display_name,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::{TempDir, tempdir};

    use super::UserManager;
    use crate::bootstrap::AppError;
    use crate::identity::{Principal, PrincipalKind};
    use crate::state::db::{
        CoralDb, DbRepos, LoginIdentity, LoginProvisioning, ResolvedDatabaseConfig,
    };
    use crate::users::model::User;
    use crate::workspaces::MemberRole;
    use crate::workspaces::authorization::WorkspaceAuthorizer;

    const ISSUER: &str = "https://issuer.test/authorization";

    #[tokio::test]
    async fn the_current_user_view_carries_the_internal_id_and_nothing_upstream() {
        let (_temp, db) = migrated_database().await;
        let ada = seed_user(&db, "ada-subject", Some("Ada"), 10).await;
        let manager = shared_deployment(&db);

        let view = manager
            .current_user(&federated(&ada))
            .await
            .expect("a provisioned caller reads their own entry");

        assert_eq!(
            view,
            User {
                user_id: ada.clone(),
                display_name: Some("Ada".to_string()),
            }
        );
        // The row behind this view also holds the issuer and subject that
        // authenticate Ada. The equality above pins every field she is handed,
        // and the id she is handed is not the subject the provider knows her by.
        assert_ne!(view.user_id, "ada-subject");

        // A principal this deployment admits but has never provisioned is
        // reported as absent rather than synthesized.
        assert!(matches!(
            manager.current_user(&federated("never-logged-in")).await,
            Err(AppError::UserNotFound(id)) if id == "never-logged-in"
        ));
    }

    /// An unmigrated database is the proof that no lookup happened: every
    /// directory query against it fails, so a decision that still denies
    /// reached its answer without touching the tables.
    #[tokio::test]
    async fn an_injected_local_principal_is_refused_before_any_directory_read() {
        let (_temp, db) = unmigrated_database().await;
        let manager = shared_deployment(&db);

        assert!(matches!(
            manager.current_user(&Principal::local()).await,
            Err(AppError::PermissionDenied(_))
        ));
        assert!(matches!(
            manager.list_users(&Principal::local()).await,
            Err(AppError::PermissionDenied(_))
        ));
    }

    #[tokio::test]
    async fn the_implicit_owner_reads_the_directory_while_owning_nothing() {
        let (_temp, db) = migrated_database().await;
        let first = seed_user(&db, "first-subject", Some("First"), 10).await;
        let second = seed_user(&db, "second-subject", None, 20).await;
        let manager = UserManager::new(
            Arc::clone(&db),
            WorkspaceAuthorizer::trusting_local_principal(Arc::clone(&db)),
        );

        assert_eq!(
            manager
                .list_users(&Principal::local())
                .await
                .expect("the implicit owner reads the directory"),
            vec![
                User {
                    user_id: first,
                    display_name: Some("First".to_string()),
                },
                User {
                    user_id: second,
                    display_name: None,
                },
            ],
            "no workspace exists here, so ownership cannot be what admitted this caller"
        );
    }

    /// Directory authority is ownership, so the three federated callers below
    /// differ only in what they own — not in how they authenticated.
    #[tokio::test]
    async fn only_a_federated_owner_reads_the_directory() {
        let (_temp, db) = migrated_database().await;
        let owner = seed_user(&db, "owner-subject", Some("Owner"), 10).await;
        let member = seed_user(&db, "member-subject", None, 20).await;
        let stranger = seed_user(&db, "stranger-subject", None, 30).await;
        create_owned_workspace(&db, "team", &owner).await;
        grant_membership(&db, "team", &member).await;
        let manager = shared_deployment(&db);

        // Pinning the whole listing is what proves the projection: every field
        // of every row is accounted for, so no issuer or subject rode along.
        assert_eq!(
            manager
                .list_users(&federated(&owner))
                .await
                .expect("an owner reads the directory"),
            vec![
                User {
                    user_id: owner.clone(),
                    display_name: Some("Owner".to_string()),
                },
                User {
                    user_id: member.clone(),
                    display_name: None,
                },
                User {
                    user_id: stranger.clone(),
                    display_name: None,
                },
            ],
            "the directory an owner reads is deployment-wide, not their workspace"
        );

        for denied in [&member, &stranger] {
            assert!(
                matches!(
                    manager.list_users(&federated(denied)).await,
                    Err(AppError::PermissionDenied(_))
                ),
                "a caller who owns no workspace must be denied the directory"
            );
        }
    }

    fn shared_deployment(db: &Arc<CoralDb>) -> UserManager {
        UserManager::new(Arc::clone(db), WorkspaceAuthorizer::new(Arc::clone(db)))
    }

    fn federated(user_id: &str) -> Principal {
        Principal::parse(user_id, PrincipalKind::User).expect("federated principal")
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
    /// rows under test are the ones a real login would write. Distinct login
    /// times are what fix the listing order the assertions above rely on.
    async fn seed_user(
        db: &CoralDb,
        subject: &str,
        display_name: Option<&str>,
        created_at_unix_nanos: i64,
    ) -> String {
        let provisioned = db
            .user_state()
            .provision_login(LoginIdentity {
                issuer: ISSUER,
                subject,
                display_name,
                principal_claim: subject,
                now_unix_nanos: created_at_unix_nanos,
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

    async fn create_owned_workspace(db: &CoralDb, workspace_id: &str, owner_user_id: &str) {
        db.workspace_state()
            .create_owned_by(workspace_id, owner_user_id, 1)
            .await
            .expect("create owned workspace");
    }

    async fn grant_membership(db: &CoralDb, workspace_id: &str, user_id: &str) {
        let mut session = db;
        session
            .workspace_members()
            .upsert(workspace_id, user_id, MemberRole::Member, 2)
            .await
            .expect("grant membership");
    }
}
