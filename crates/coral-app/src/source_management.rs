//! Public source-management handle for server extensions.

use std::collections::BTreeMap;

use crate::bootstrap::AppError;
use crate::identity::SourceIdentityBinding;
use crate::sources::SourceName;
use crate::sources::manager::{
    ImportSourceCommand as ManagerImportSourceCommand, SourceBinding, SourceBindings, SourceManager,
};
use crate::sources::model::InstalledSource;
use crate::workspaces::WorkspaceName;

/// Source import command accepted by [`SourceManagementHandle`].
pub struct ImportManagedSourceCommand {
    /// Source manifest YAML to install into the workspace.
    pub manifest_yaml: String,
    /// Non-secret source variables.
    pub variables: BTreeMap<String, String>,
    /// Source-surface identity bindings.
    pub identity_bindings: BTreeMap<String, SourceIdentityBinding>,
    /// Whether supplied identity bindings replace existing bindings.
    pub replace_identity_bindings: bool,
}

/// Source installed or removed through [`SourceManagementHandle`].
pub struct ManagedSource {
    /// Installed source name.
    pub name: String,
    /// Authored source version, when available.
    pub version: Option<String>,
}

/// Narrow source lifecycle handle exposed to server extensions.
#[derive(Clone)]
pub struct SourceManagementHandle {
    sources: SourceManager,
}

impl SourceManagementHandle {
    pub(crate) fn new(sources: SourceManager) -> Self {
        Self { sources }
    }

    /// Imports a source into a workspace using the shared source lifecycle.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the workspace or source input is invalid, or
    /// when source installation fails.
    pub fn import_source(
        &self,
        workspace_id: &str,
        command: ImportManagedSourceCommand,
    ) -> Result<ManagedSource, AppError> {
        let workspace_name = WorkspaceName::parse(workspace_id)?;
        let installed = self
            .sources
            .import_source(&workspace_name, &manager_import_command(command))?;
        Ok(managed_source_from_installed(installed))
    }

    /// Imports a source under a workspace-local name while preserving the
    /// authored source-spec id declared by the manifest.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the workspace/source input is invalid, when
    /// the manifest name does not match `source_spec_id`, or when source
    /// installation fails.
    pub fn import_source_as(
        &self,
        workspace_id: &str,
        source_name: &str,
        source_spec_id: &str,
        command: ImportManagedSourceCommand,
    ) -> Result<ManagedSource, AppError> {
        let workspace_name = WorkspaceName::parse(workspace_id)?;
        let source_name = SourceName::parse(source_name)?;
        let source_spec_id = SourceName::parse(source_spec_id)?;
        let installed = self.sources.import_source_as(
            &workspace_name,
            &source_name,
            &source_spec_id,
            &manager_import_command(command),
        )?;
        Ok(managed_source_from_installed(installed))
    }

    /// Deletes a source from a workspace using the shared source lifecycle.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the workspace/source input is invalid, or when
    /// source removal fails.
    pub fn delete_source(
        &self,
        workspace_id: &str,
        source_name: &str,
    ) -> Result<ManagedSource, AppError> {
        let workspace_name = WorkspaceName::parse(workspace_id)?;
        let source_name = SourceName::parse(source_name)?;
        let removed = self.sources.delete_source(&workspace_name, &source_name)?;
        Ok(ManagedSource {
            name: removed.name.as_str().to_string(),
            version: removed.version,
        })
    }
}

fn manager_import_command(command: ImportManagedSourceCommand) -> ManagerImportSourceCommand {
    ManagerImportSourceCommand {
        manifest_yaml: command.manifest_yaml,
        bindings: SourceBindings {
            variables: command
                .variables
                .into_iter()
                .map(|(key, value)| SourceBinding { key, value })
                .collect(),
            secrets: Vec::new(),
        },
        identity_bindings: command.identity_bindings,
        replace_identity_bindings: command.replace_identity_bindings,
    }
}

fn managed_source_from_installed(installed: InstalledSource) -> ManagedSource {
    ManagedSource {
        name: installed.name.as_str().to_string(),
        version: installed.version,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::{CredentialManager, CredentialStore};
    use crate::source_registry::SourceRegistry;
    use crate::state::{AppStateLayout, ConfigStore};
    use tempfile::TempDir;

    fn handle() -> (TempDir, SourceManagementHandle, ConfigStore) {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let config_store = ConfigStore::new(layout.clone());
        let credential_store = CredentialStore::new(layout.clone());
        let credential_manager = CredentialManager::new(credential_store);
        let source_manager = SourceManager::new(config_store.clone(), credential_manager, layout);
        (
            temp,
            SourceManagementHandle::new(source_manager),
            config_store,
        )
    }

    fn manifest_without_secrets() -> String {
        r#"
name: public_messages
version: 0.1.0
dsl_version: 3
backend: http
base_url: "https://example.com"
tables:
  - name: messages
    description: Public messages
    request:
      method: GET
      path: /messages
    response: {}
    columns:
      - name: id
        type: Utf8
"#
        .to_string()
    }

    #[test]
    fn import_source_uses_shared_source_lifecycle() {
        let (_temp, handle, config_store) = handle();

        let installed = handle
            .import_source(
                "default",
                ImportManagedSourceCommand {
                    manifest_yaml: manifest_without_secrets(),
                    variables: BTreeMap::new(),
                    identity_bindings: BTreeMap::new(),
                    replace_identity_bindings: false,
                },
            )
            .expect("import source");

        assert_eq!(installed.name, "public_messages");
        assert_eq!(installed.version.as_deref(), Some("0.1.0"));
        let sources = SourceRegistry::list_workspace_sources(&config_store, "default")
            .expect("workspace sources");
        let source_names = sources
            .iter()
            .map(|source| source.source_name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(source_names, ["public_messages"]);
    }

    #[test]
    fn delete_source_uses_shared_source_lifecycle() {
        let (_temp, handle, config_store) = handle();
        handle
            .import_source(
                "default",
                ImportManagedSourceCommand {
                    manifest_yaml: manifest_without_secrets(),
                    variables: BTreeMap::new(),
                    identity_bindings: BTreeMap::new(),
                    replace_identity_bindings: false,
                },
            )
            .expect("import source");

        let removed = handle
            .delete_source("default", "public_messages")
            .expect("delete source");

        assert_eq!(removed.name, "public_messages");
        assert_eq!(removed.version.as_deref(), Some("0.1.0"));
        let sources = SourceRegistry::list_workspace_sources(&config_store, "default")
            .expect("workspace sources");
        assert!(sources.is_empty());
    }
}
