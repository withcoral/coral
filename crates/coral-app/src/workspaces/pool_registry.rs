use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use coral_engine::DatabasePoolRegistry;

use crate::workspaces::WorkspaceName;

/// Server-owned database pool registries keyed by workspace.
#[derive(Default)]
pub(crate) struct WorkspacePoolRegistry {
    registries: Mutex<HashMap<WorkspaceName, Arc<DatabasePoolRegistry>>>,
}

impl WorkspacePoolRegistry {
    pub(crate) fn for_workspace(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Arc<DatabasePoolRegistry> {
        let mut registries = self
            .registries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(
            registries
                .entry(workspace_name.clone())
                .or_insert_with(|| Arc::new(DatabasePoolRegistry::new())),
        )
    }

    /// Detaches the workspace registry so future runtimes cannot reuse pools
    /// created from an earlier source snapshot.
    pub(crate) fn remove(&self, workspace_name: &WorkspaceName) {
        self.registries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(workspace_name);
    }

    pub(crate) fn remove_catalog(&self, workspace_name: &WorkspaceName, catalog_name: &str) {
        let registry = self
            .registries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(workspace_name)
            .cloned();
        if let Some(registry) = registry {
            registry.remove_catalog(catalog_name);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reuses_registry_per_workspace_and_isolates_workspaces() {
        let registries = WorkspacePoolRegistry::default();
        let alpha = WorkspaceName::parse("alpha").expect("workspace");
        let beta = WorkspaceName::parse("beta").expect("workspace");

        let alpha_first = registries.for_workspace(&alpha);
        let alpha_second = registries.for_workspace(&alpha);
        let beta_registry = registries.for_workspace(&beta);

        assert!(Arc::ptr_eq(&alpha_first, &alpha_second));
        assert!(!Arc::ptr_eq(&alpha_first, &beta_registry));
    }

    #[test]
    fn removal_gives_recreated_workspace_a_fresh_registry() {
        let registries = WorkspacePoolRegistry::default();
        let workspace = WorkspaceName::parse("recreated").expect("workspace");

        let before = registries.for_workspace(&workspace);
        registries.remove(&workspace);
        let after = registries.for_workspace(&workspace);

        assert!(!Arc::ptr_eq(&before, &after));
    }

    #[test]
    fn catalog_removal_keeps_the_workspace_registry() {
        let registries = WorkspacePoolRegistry::default();
        let workspace = WorkspaceName::parse("catalog-removal").expect("workspace");

        let before = registries.for_workspace(&workspace);
        registries.remove_catalog(&workspace, "orders");
        let after = registries.for_workspace(&workspace);

        assert!(Arc::ptr_eq(&before, &after));
    }
}
