use std::sync::Arc;

use crate::bootstrap::AppError;
use crate::identity::{Principal, PrincipalKind};
use crate::state::db::{CoralDb, DbRepos};
use crate::users::{CurrentUser, UserView};
use crate::workspaces::{LocalPrincipalPolicy, MemberRole, WorkspaceName};

/// App-domain user directory and current-user behavior.
#[derive(Clone)]
pub(crate) struct UserManager {
    db: Arc<CoralDb>,
    local_principal: LocalPrincipalPolicy,
}

impl UserManager {
    pub(crate) fn new(db: Arc<CoralDb>) -> Self {
        Self {
            db,
            local_principal: LocalPrincipalPolicy::default(),
        }
    }

    /// Lets the local principal read the directory without owning a workspace.
    ///
    /// Only a state directory without `[auth]` may be served this way.
    pub(crate) fn trusting_local_principal(mut self) -> Self {
        self.local_principal = LocalPrincipalPolicy::ImplicitOwner;
        self
    }

    pub(crate) async fn get_current_user(
        &self,
        principal: &Principal,
    ) -> Result<CurrentUser, AppError> {
        require_human(principal)?;
        let mut session = self.db.as_ref();
        let user = session
            .users()
            .get_by_user_id(principal.id().as_str())
            .await?
            .ok_or_else(|| AppError::UserNotFound(principal.id().to_string()))?;
        let default_workspace = WorkspaceName::parse(&format!("default-{}", user.user_id))
            .map_err(|error| {
                AppError::Database(format!(
                    "invalid personal workspace derived for user '{}': {error}",
                    user.user_id
                ))
            })?;
        Ok(CurrentUser {
            user: UserView {
                user_id: user.user_id,
                display_name: user.display_name,
            },
            default_workspace,
        })
    }

    pub(crate) async fn list_users(
        &self,
        principal: &Principal,
    ) -> Result<Vec<UserView>, AppError> {
        require_human(principal)?;
        let mut session = self.db.as_ref();
        let trusted_local =
            principal.is_local() && self.local_principal == LocalPrincipalPolicy::ImplicitOwner;
        if !trusted_local
            && !session
                .workspace_members()
                .workspaces_for_user_id(principal.id().as_str())
                .await?
                .iter()
                .any(|(_, role)| *role == MemberRole::Owner)
        {
            return Err(AppError::PermissionDenied(
                "listing users requires ownership of at least one workspace".to_string(),
            ));
        }
        Ok(session
            .users()
            .list()
            .await?
            .into_iter()
            .map(|user| UserView {
                user_id: user.user_id,
                display_name: user.display_name,
            })
            .collect())
    }
}

fn require_human(principal: &Principal) -> Result<(), AppError> {
    if principal.kind() == PrincipalKind::User {
        Ok(())
    } else {
        Err(AppError::PermissionDenied(
            "user directory operations require a human principal".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::UserManager;
    use crate::bootstrap::AppError;
    use crate::identity::{Principal, PrincipalKind};
    use crate::state::AppStateLayout;
    use crate::state::db::{
        CoralDb, DatabaseConfig, DbRepos, ResolvedDatabaseConfig, UpsertLoginOutcome,
    };

    #[tokio::test]
    async fn current_user_is_self_scoped_and_derives_the_personal_default() {
        let (_temp, db, manager) = manager().await;
        let user_id = provision_user(&db, "owner", Some("Owner")).await;
        let principal = Principal::parse(&user_id, PrincipalKind::User).expect("principal");

        let current = manager
            .get_current_user(&principal)
            .await
            .expect("current user");

        assert_eq!(current.user.user_id, user_id);
        assert_eq!(current.user.display_name.as_deref(), Some("Owner"));
        assert_eq!(
            current.default_workspace.as_str(),
            format!("default-{}", current.user.user_id)
        );
    }

    #[tokio::test]
    async fn current_user_rejects_agents_and_unknown_users() {
        let (_temp, _db, manager) = manager().await;
        let missing_id = uuid::Uuid::new_v4().to_string();

        assert!(matches!(
            manager
                .get_current_user(
                    &Principal::parse(&missing_id, PrincipalKind::Agent).expect("agent")
                )
                .await,
            Err(AppError::PermissionDenied(_))
        ));
        assert!(matches!(
            manager
                .get_current_user(
                    &Principal::parse(&missing_id, PrincipalKind::User).expect("user")
                )
                .await,
            Err(AppError::UserNotFound(ref user_id)) if user_id == &missing_id
        ));
    }

    #[tokio::test]
    async fn list_users_requires_a_human_workspace_owner() {
        let (_temp, db, manager) = manager().await;
        let owner_id = provision_user(&db, "owner", Some("Owner")).await;
        let directory_only_id = create_directory_user(&db, "directory-only", Some("Member")).await;

        let users = manager
            .list_users(&Principal::parse(&owner_id, PrincipalKind::User).expect("owner"))
            .await
            .expect("owner lists users");
        assert!(users.iter().any(|user| {
            user.user_id == directory_only_id && user.display_name.as_deref() == Some("Member")
        }));
        assert!(matches!(
            manager
                .list_users(
                    &Principal::parse(&directory_only_id, PrincipalKind::User).expect("non-owner")
                )
                .await,
            Err(AppError::PermissionDenied(_))
        ));
        assert!(matches!(
            manager
                .list_users(&Principal::parse(&owner_id, PrincipalKind::Agent).expect("agent"))
                .await,
            Err(AppError::PermissionDenied(_))
        ));
    }

    async fn manager() -> (TempDir, Arc<CoralDb>, UserManager) {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let DatabaseConfig::Sqlite { path } = DatabaseConfig::load(&layout).expect("config") else {
            panic!("default test database must be SQLite")
        };
        let db = Arc::new(
            CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
                .await
                .expect("open sqlite"),
        );
        db.migrate().await.expect("migrate");
        let manager = UserManager::new(Arc::clone(&db));
        (temp, db, manager)
    }

    async fn provision_user(db: &CoralDb, subject: &str, display_name: Option<&str>) -> String {
        let UpsertLoginOutcome::Upserted(user) = db
            .provision_login("issuer", subject, display_name, 1)
            .await
            .expect("provision user")
        else {
            panic!("new subject should create user")
        };
        user.user_id
    }

    async fn create_directory_user(
        db: &CoralDb,
        subject: &str,
        display_name: Option<&str>,
    ) -> String {
        let mut session = db;
        let UpsertLoginOutcome::Upserted(user) = session
            .users()
            .upsert_login("issuer", subject, display_name, 1)
            .await
            .expect("create directory user")
        else {
            panic!("new subject should create user")
        };
        user.user_id
    }
}
