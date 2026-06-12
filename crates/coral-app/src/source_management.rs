//! Public source-management handle for product-specific services.

use std::collections::BTreeMap;

use crate::bootstrap::AppError;
use crate::identity::SourceIdentityBinding;
use crate::sources::SourceName;
use crate::sources::manager::{
    ImportSourceCommand as ManagerImportSourceCommand, SourceBinding, SourceBindings, SourceManager,
};
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

/// Narrow source lifecycle handle exposed to product-specific server services.
#[derive(Clone)]
pub struct SourceManagementHandle {
    sources: SourceManager,
}

impl SourceManagementHandle {
    pub(crate) fn new(sources: SourceManager) -> Self {
        Self { sources }
    }

    /// Imports a source into a workspace using the shared OSS source lifecycle.
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
        let installed = self.sources.import_source(
            &workspace_name,
            &ManagerImportSourceCommand {
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
            },
        )?;
        Ok(ManagedSource {
            name: installed.name.as_str().to_string(),
            version: installed.version,
        })
    }

    /// Deletes a source from a workspace using the shared OSS source lifecycle.
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
