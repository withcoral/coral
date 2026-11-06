//! What an Organization Owner may learn about the Secret Stores the operator
//! defined. It joins Organization governance (ADR 0015) with the registry
//! bootstrap builds, which knows nothing about Users.
use coral_secret_store::{SecretStoreKind, SecretStoreName, SecretStores};
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

    /// Each store's name and kind, ordered by name, and never how Coral reaches it.
    ///
    /// # Errors
    /// Denies anyone but a current, enabled Organization Owner.
    pub async fn list(
        &self,
        actor: &AuthenticatedUser,
    ) -> Result<Vec<(SecretStoreName, SecretStoreKind)>, coral_workspace::Error> {
        self.organization.check_owner(actor).await?;
        Ok(self
            .stores
            .list()
            .map(|(name, kind)| (name.clone(), kind))
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
        let inventory = SecretStoreInventory::new(
            OrganizationModule::new(database),
            SecretStores::new([(
                name.clone(),
                SecretStoreKind::KvV2,
                Arc::new(InMemorySecretStore::new()) as _,
            )])
            .unwrap(),
        );

        assert_eq!(
            inventory.list(&owner).await.unwrap(),
            [(name, SecretStoreKind::KvV2)]
        );
        assert!(matches!(
            inventory.list(&member).await.unwrap_err(),
            coral_workspace::Error::PermissionDenied(_)
        ));
    }
}
