//! What people may learn and change about the Secret Stores the operator defined.
//! It joins Organization governance (ADR 0015) and the Workspace's grants with the
//! registry bootstrap builds, which knows nothing about Users or Workspaces. A
//! grant names a store, so it means nothing while config does not define that store.
use coral_secret_store::{SecretStoreKind, SecretStoreName, SecretStores, StoreStatus};
use coral_workspace::{
    AuthenticatedUser, ChangeContext, OrganizationModule, Workspace, WorkspaceId, WorkspaceModule,
};

/// A store a grant names, as config defines it now.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrantedSecretStore {
    /// The name the grant holds.
    pub name: String,
    /// The store's kind and current status. `None` means config does not
    /// define the store, so the grant is kept but gives the Workspace nothing.
    pub defined: Option<(SecretStoreKind, StoreStatus)>,
}

/// One grant in the Organization, for an Organization Owner.
#[derive(Clone, Debug)]
pub struct OrganizationSecretStoreGrant {
    /// What governance may already see of the Workspace.
    pub workspace: Workspace,
    /// The store the grant names.
    pub store: GrantedSecretStore,
}

/// Lists the defined Secret Stores and manages their grants to Workspaces.
#[derive(Clone)]
pub struct SecretStoreAdministration {
    organization: OrganizationModule,
    workspaces: WorkspaceModule,
    stores: SecretStores,
}

impl SecretStoreAdministration {
    /// Composes administration over the process's one registry.
    #[must_use]
    pub const fn new(
        organization: OrganizationModule,
        workspaces: WorkspaceModule,
        stores: SecretStores,
    ) -> Self {
        Self {
            organization,
            workspaces,
            stores,
        }
    }

    /// Each store's name, kind and current status, ordered by name, and never
    /// how Coral reaches it.
    ///
    /// # Errors
    /// Denies anyone but a current, enabled Organization Owner.
    pub async fn list(
        &self,
        actor: &AuthenticatedUser,
    ) -> Result<Vec<(SecretStoreName, SecretStoreKind, StoreStatus)>, coral_workspace::Error> {
        self.organization.check_owner(actor).await?;
        Ok(self
            .stores
            .list()
            .map(|(name, kind, status)| (name.clone(), kind.clone(), status))
            .collect())
    }

    /// Every grant in the Organization, ordered by store.
    ///
    /// # Errors
    /// Denies anyone but a current, enabled Organization Owner.
    pub async fn list_grants(
        &self,
        actor: &AuthenticatedUser,
    ) -> Result<Vec<OrganizationSecretStoreGrant>, coral_workspace::Error> {
        Ok(self
            .workspaces
            .list_secret_store_grants(actor)
            .await?
            .into_iter()
            .map(|grant| OrganizationSecretStoreGrant {
                workspace: grant.workspace,
                store: self.granted(grant.store),
            })
            .collect())
    }

    /// The stores granted to one Workspace, ordered by name, for its Owner.
    ///
    /// # Errors
    /// Denies anyone but the Workspace's Owner.
    pub async fn list_for_workspace(
        &self,
        actor: &AuthenticatedUser,
        workspace: &WorkspaceId,
    ) -> Result<Vec<GrantedSecretStore>, coral_workspace::Error> {
        Ok(self
            .workspaces
            .list_workspace_secret_stores(actor, workspace)
            .await?
            .into_iter()
            .map(|name| self.granted(name))
            .collect())
    }

    /// Grants a store config defines to a Workspace.
    ///
    /// # Errors
    /// Denies anyone but a current, enabled Organization Owner, before it says
    /// whether config defines the store. Refuses a store config does not define
    /// and an archived Workspace. A rejected request changes nothing.
    pub async fn grant(
        &self,
        context: &ChangeContext,
        workspace: &WorkspaceId,
        store: &SecretStoreName,
    ) -> Result<(), crate::Error> {
        self.organization.check_owner(context.actor()).await?;
        if self.stores.get(store.as_str()).is_none() {
            return Err(crate::Error::UndefinedSecretStore);
        }
        Ok(self
            .workspaces
            .grant_secret_store(context, workspace, store.as_str())
            .await?)
    }

    /// Revokes a grant, whether or not config still defines its store.
    ///
    /// # Errors
    /// Denies anyone but a current, enabled Organization Owner, and refuses an
    /// archived Workspace. A rejected request changes nothing.
    pub async fn revoke(
        &self,
        context: &ChangeContext,
        workspace: &WorkspaceId,
        store: &SecretStoreName,
    ) -> Result<(), crate::Error> {
        Ok(self
            .workspaces
            .revoke_secret_store(context, workspace, store.as_str())
            .await?)
    }

    fn granted(&self, name: String) -> GrantedSecretStore {
        let defined = self.stores.describe(&name);
        GrantedSecretStore { name, defined }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use coral_test_support::{InMemorySecretStore, TestDatabase, provision_user};
    use coral_workspace::{OrganizationRole, WorkspaceDisplayName};

    use super::*;

    fn test_kind() -> SecretStoreKind {
        SecretStoreKind::new("test", "Test store").unwrap()
    }

    fn named(name: &str) -> SecretStoreName {
        SecretStoreName::parse(name).unwrap()
    }

    fn change(actor: &AuthenticatedUser) -> ChangeContext {
        coral_test_support::change(actor.clone(), 10, '1')
    }

    struct Fixture {
        _database: TestDatabase,
        administration: SecretStoreAdministration,
        workspaces: WorkspaceModule,
        governor: AuthenticatedUser,
        owner: AuthenticatedUser,
        workspace: Workspace,
    }

    /// A deployment that defines only the `platform` store.
    async fn fixture() -> Fixture {
        let database = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
        let db = database.database();
        let governor = provision_user(db.clone(), "user:governor", OrganizationRole::Owner).await;
        let owner = provision_user(db.clone(), "user:owner", OrganizationRole::Member).await;
        let workspaces = WorkspaceModule::new(db.clone());
        let workspace = workspaces
            .create(
                &change(&owner),
                WorkspaceDisplayName::parse("Engineering").unwrap(),
            )
            .await
            .unwrap();
        let administration = SecretStoreAdministration::new(
            OrganizationModule::new(db),
            workspaces.clone(),
            SecretStores::new([(
                named("platform"),
                test_kind(),
                Arc::new(InMemorySecretStore::new()) as _,
            )])
            .unwrap(),
        );
        Fixture {
            _database: database,
            administration,
            workspaces,
            governor,
            owner,
            workspace,
        }
    }

    #[tokio::test]
    async fn only_an_organization_owner_lists_the_stores() {
        let f = fixture().await;
        assert_eq!(
            f.administration.list(&f.governor).await.unwrap(),
            [(named("platform"), test_kind(), StoreStatus::Available)]
        );
        assert!(matches!(
            f.administration.list(&f.owner).await.unwrap_err(),
            coral_workspace::Error::PermissionDenied(_)
        ));
    }

    // Catches: a Workspace Owner seeing a removed store as usable, or Coral
    // dropping the grant, so restoring the store would not restore the grant.
    #[tokio::test]
    async fn a_grant_whose_store_config_removed_stays_but_is_unavailable() {
        let f = fixture().await;
        let id = f.workspace.id();
        f.administration
            .grant(&change(&f.governor), id, &named("platform"))
            .await
            .unwrap();
        // Config defined `removed` when this grant was made.
        f.workspaces
            .grant_secret_store(&change(&f.governor), id, "removed")
            .await
            .unwrap();

        let shown: Vec<_> = f
            .administration
            .list_for_workspace(&f.owner, id)
            .await
            .unwrap()
            .into_iter()
            .map(|store| (store.name, store.defined))
            .collect();
        let expected = [
            (
                "platform".to_string(),
                Some((test_kind(), StoreStatus::Available)),
            ),
            ("removed".to_string(), None),
        ];
        assert_eq!(shown, expected);

        let all: Vec<_> = f
            .administration
            .list_grants(&f.governor)
            .await
            .unwrap()
            .into_iter()
            .map(|grant| (grant.store.name, grant.store.defined))
            .collect();
        assert_eq!(all, expected);

        // A removed store can still lose its grant.
        f.administration
            .revoke(&change(&f.governor), id, &named("removed"))
            .await
            .unwrap();
        assert_eq!(
            f.workspaces
                .list_workspace_secret_stores(&f.owner, id)
                .await
                .unwrap(),
            ["platform"]
        );
    }

    // Catches: a grant of a typo that later starts working when an operator
    // defines a store under that name, or a non-owner probing store names.
    #[tokio::test]
    async fn a_store_config_does_not_define_cannot_be_granted() {
        let f = fixture().await;
        let id = f.workspace.id();
        let undefined = named("undefined");
        let events = f
            .workspaces
            .list_lifecycle_events(&f.owner, id)
            .await
            .unwrap();

        assert!(matches!(
            f.administration
                .grant(&change(&f.governor), id, &undefined)
                .await,
            Err(crate::Error::UndefinedSecretStore)
        ));
        assert!(matches!(
            f.administration
                .grant(&change(&f.owner), id, &undefined)
                .await,
            Err(crate::Error::Workspace(
                coral_workspace::Error::PermissionDenied(_)
            ))
        ));

        assert!(
            f.workspaces
                .list_workspace_secret_stores(&f.owner, id)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            f.workspaces
                .list_lifecycle_events(&f.owner, id)
                .await
                .unwrap(),
            events
        );
    }
}
