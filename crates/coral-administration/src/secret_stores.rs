//! What an Organization Owner may learn about the Secret Stores the operator
//! defined. It joins Organization governance (ADR 0015) with the registry
//! bootstrap builds, which knows nothing about Users.
use coral_secret_store::{SecretStoreKind, SecretStoreName, SecretStores, StoreStatus};
use coral_workspace::{AuthenticatedUser, OrganizationModule};

/// Lists the defined Secret Stores for an Organization Owner.
#[derive(Clone)]
pub struct SecretStoreInventory {
    organization: OrganizationModule,
    stores: SecretStores,
}

impl SecretStoreInventory {
    /// Composes the inventory over the process's one registry.
    #[must_use]
    pub const fn new(organization: OrganizationModule, stores: SecretStores) -> Self {
        Self {
            organization,
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
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use coral_test_support::{InMemorySecretStore, TestDatabase, provision_user};
    use coral_workspace::OrganizationRole;

    use super::*;

    #[tokio::test]
    async fn only_an_organization_owner_lists_the_stores() {
        let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
        let database = fixture.database();
        let owner = provision_user(database.clone(), "user:owner", OrganizationRole::Owner).await;
        let member =
            provision_user(database.clone(), "user:member", OrganizationRole::Member).await;
        let name = SecretStoreName::parse("platform").unwrap();
        let kind = SecretStoreKind::new("test", "Test store").unwrap();
        let inventory = SecretStoreInventory::new(
            OrganizationModule::new(database),
            SecretStores::new([(
                name.clone(),
                kind.clone(),
                Arc::new(InMemorySecretStore::new()) as _,
            )])
            .unwrap(),
        );

        assert_eq!(
            inventory.list(&owner).await.unwrap(),
            [(name, kind, StoreStatus::Available)]
        );
        assert!(matches!(
            inventory.list(&member).await.unwrap_err(),
            coral_workspace::Error::PermissionDenied(_)
        ));
    }
}
