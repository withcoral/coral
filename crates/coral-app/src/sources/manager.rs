//! Owns the source lifecycle workflow for the local app.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use coral_api::v1::{
    AvailableSource, CreateBundledSourceRequest, ImportSourceRequest, SourceInputKind,
    SourceSecret, SourceVariable, Workspace,
};

use crate::bootstrap::AppError;
use crate::sources::catalog::{describe_manifest, list_bundled_sources, load_bundled_source};
use crate::sources::model::{ManagedSource, ManagedSourceOrigin};
use crate::state::{AppStateLayout, ConfigStore, SecretStore};
use crate::storage::fs;
use crate::workspaces::WorkspaceManager;

#[derive(Clone)]
pub(crate) struct SourceManager {
    config_store: ConfigStore,
    secret_store: SecretStore,
    layout: AppStateLayout,
    workspace_manager: WorkspaceManager,
}

struct ValidatedBindings {
    variables: BTreeMap<String, String>,
    secrets: BTreeMap<String, String>,
}

struct PersistSourceRequest<'a> {
    available: &'a AvailableSource,
    manifest_yaml: &'a str,
    bindings: ValidatedBindings,
    origin: ManagedSourceOrigin,
}

struct ExistingSourceState {
    source: ManagedSource,
    manifest_yaml: String,
    secrets: BTreeMap<String, String>,
}

impl SourceManager {
    pub(crate) fn new(
        config_store: ConfigStore,
        secret_store: SecretStore,
        layout: AppStateLayout,
    ) -> Self {
        Self {
            config_store,
            secret_store,
            layout,
            workspace_manager: WorkspaceManager::new(),
        }
    }

    pub(crate) fn list_workspace_sources(
        &self,
        workspace: &Workspace,
    ) -> Result<Vec<ManagedSource>, AppError> {
        self.config_store.list_workspace_sources(workspace)
    }

    pub(crate) fn get_source(
        &self,
        workspace: &Workspace,
        source_name: &str,
    ) -> Result<ManagedSource, AppError> {
        self.config_store.get_source(workspace, source_name)
    }

    pub(crate) fn discover_sources(
        &self,
        workspace: &Workspace,
    ) -> Result<Vec<AvailableSource>, AppError> {
        let installed = self
            .list_workspace_sources(workspace)?
            .into_iter()
            .map(|source| source.name)
            .collect::<BTreeSet<_>>();
        list_bundled_sources(workspace, &installed)
    }

    pub(crate) fn create_bundled_source(
        &self,
        request: &CreateBundledSourceRequest,
    ) -> Result<ManagedSource, AppError> {
        let workspace = self
            .workspace_manager
            .require_app(request.workspace.as_ref())?;
        let bundled_name = self
            .workspace_manager
            .validate_path_name("source name", &request.name)?;
        let bundled = load_bundled_source(&bundled_name)?;
        let available = self.describe_bundled_source(&workspace, &bundled.manifest_yaml)?;
        let bindings = validate_bindings(
            &self.workspace_manager,
            &available,
            &request.variables,
            &request.secrets,
        )?;
        self.persist_source(
            &workspace,
            PersistSourceRequest {
                available: &available,
                manifest_yaml: &bundled.manifest_yaml,
                bindings,
                origin: ManagedSourceOrigin::Bundled,
            },
        )
    }

    pub(crate) fn import_source(
        &self,
        request: &ImportSourceRequest,
    ) -> Result<ManagedSource, AppError> {
        let workspace = self
            .workspace_manager
            .require_app(request.workspace.as_ref())?;
        let mut available = describe_manifest(
            &request.manifest_yaml,
            coral_api::v1::SourceOrigin::Imported,
            false,
        )?;
        available.installed = self.source_exists(&workspace, &available.name)?;
        let bindings = validate_bindings(
            &self.workspace_manager,
            &available,
            &request.variables,
            &request.secrets,
        )?;
        self.persist_source(
            &workspace,
            PersistSourceRequest {
                available: &available,
                manifest_yaml: &request.manifest_yaml,
                bindings,
                origin: ManagedSourceOrigin::Imported,
            },
        )
    }

    pub(crate) fn delete_source(
        &self,
        workspace: &Workspace,
        source_name: &str,
    ) -> Result<ManagedSource, AppError> {
        let stored = self.config_store.get_source(workspace, source_name)?;
        let source_dir = self.layout.source_dir(&stored.workspace, &stored.name);
        let previous = ExistingSourceState {
            source: stored.clone(),
            manifest_yaml: std::fs::read_to_string(
                self.layout.manifest_file(&stored.workspace, &stored.name),
            )?,
            secrets: self
                .secret_store
                .read_source_secrets_for(&stored.workspace, &stored.name)?,
        };
        if source_dir.exists()
            && let Err(error) = std::fs::remove_dir_all(&source_dir)
        {
            self.restore_existing_state(workspace, source_name, Some(previous));
            return Err(error.into());
        }
        if let Err(error) = self.config_store.remove_source(workspace, source_name) {
            self.restore_existing_state(workspace, source_name, Some(previous));
            return Err(error);
        }
        cleanup_empty_parent(&self.layout.workspaces_root(), source_dir.parent());
        cleanup_empty_parent(
            &self.layout.workspaces_root(),
            self.layout.workspace_dir(&stored.workspace).parent(),
        );
        Ok(stored)
    }

    fn describe_bundled_source(
        &self,
        workspace: &Workspace,
        manifest_yaml: &str,
    ) -> Result<AvailableSource, AppError> {
        let mut available =
            describe_manifest(manifest_yaml, coral_api::v1::SourceOrigin::Bundled, false)?;
        available.installed = self.source_exists(workspace, &available.name)?;
        Ok(available)
    }

    fn persist_source(
        &self,
        workspace: &Workspace,
        request: PersistSourceRequest<'_>,
    ) -> Result<ManagedSource, AppError> {
        let source_name = self
            .workspace_manager
            .validate_path_name("source name", &request.available.name)?;
        let previous = self.load_existing_state(workspace, &source_name)?;
        let manifest_path = self.layout.manifest_file(workspace, &source_name);
        if let Some(parent) = manifest_path.parent() {
            fs::ensure_dir(parent)?;
        }
        fs::write_atomic(&manifest_path, request.manifest_yaml.as_bytes())?;

        let persisted_secrets = match self.secret_store.replace_source_secrets_for(
            workspace,
            &source_name,
            &request.bindings.secrets,
        ) {
            Ok(secrets) => secrets,
            Err(error) => {
                self.restore_existing_state(workspace, &source_name, previous);
                return Err(error);
            }
        };

        let stored = ManagedSource {
            workspace: workspace.clone(),
            name: source_name.clone(),
            version: request.available.version.clone(),
            variables: request.bindings.variables,
            secrets: persisted_secrets,
            origin: request.origin,
        };
        if let Err(error) = self.config_store.upsert_source(stored.clone()) {
            self.restore_existing_state(workspace, &source_name, previous);
            return Err(error);
        }
        Ok(stored)
    }

    fn source_exists(&self, workspace: &Workspace, source_name: &str) -> Result<bool, AppError> {
        match self.config_store.get_source(workspace, source_name) {
            Ok(_) => Ok(true),
            Err(AppError::SourceNotFound(_)) => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn load_existing_state(
        &self,
        workspace: &Workspace,
        source_name: &str,
    ) -> Result<Option<ExistingSourceState>, AppError> {
        let source = match self.config_store.get_source(workspace, source_name) {
            Ok(source) => source,
            Err(AppError::SourceNotFound(_)) => return Ok(None),
            Err(error) => return Err(error),
        };
        let manifest_yaml =
            std::fs::read_to_string(self.layout.manifest_file(workspace, source_name))?;
        let secrets = self
            .secret_store
            .read_source_secrets_for(workspace, source_name)?;
        Ok(Some(ExistingSourceState {
            source,
            manifest_yaml,
            secrets,
        }))
    }

    fn restore_existing_state(
        &self,
        workspace: &Workspace,
        source_name: &str,
        previous: Option<ExistingSourceState>,
    ) {
        if let Some(previous) = previous {
            let manifest_path = self.layout.manifest_file(workspace, source_name);
            if let Some(parent) = manifest_path.parent() {
                let _ = fs::ensure_dir(parent);
            }
            let _ = fs::write_atomic(&manifest_path, previous.manifest_yaml.as_bytes());
            let _ = self.secret_store.replace_source_secrets_for(
                workspace,
                source_name,
                &previous.secrets,
            );
            let _ = self.config_store.upsert_source(previous.source);
        } else {
            let source_dir = self.layout.source_dir(workspace, source_name);
            if source_dir.exists() {
                let _ = std::fs::remove_dir_all(&source_dir);
            }
        }
    }
}

fn validate_bindings(
    workspace_manager: &WorkspaceManager,
    available: &AvailableSource,
    variables: &[SourceVariable],
    secrets: &[SourceSecret],
) -> Result<ValidatedBindings, AppError> {
    let variable_values = collect_unique_variables(workspace_manager, variables)?;
    let secret_values = collect_unique_secrets(workspace_manager, secrets)?;
    let expected_variables = available
        .inputs
        .iter()
        .filter(|input| input.kind == SourceInputKind::Variable as i32)
        .map(|input| input.key.clone())
        .collect::<BTreeSet<_>>();
    let expected_secrets = available
        .inputs
        .iter()
        .filter(|input| input.kind == SourceInputKind::Secret as i32)
        .map(|input| input.key.clone())
        .collect::<BTreeSet<_>>();

    for key in variable_values.keys() {
        if !expected_variables.contains(key) {
            return Err(AppError::InvalidInput(format!(
                "unknown source variable '{key}'"
            )));
        }
    }
    for key in secret_values.keys() {
        if !expected_secrets.contains(key) {
            return Err(AppError::InvalidInput(format!(
                "unknown source secret '{key}'"
            )));
        }
    }

    for input in &available.inputs {
        match SourceInputKind::try_from(input.kind) {
            Ok(SourceInputKind::Variable)
                if input.required && !variable_values.contains_key(&input.key) =>
            {
                return Err(AppError::InvalidInput(format!(
                    "missing required source variable '{}'",
                    input.key
                )));
            }
            Ok(SourceInputKind::Secret)
                if input.required && !secret_values.contains_key(&input.key) =>
            {
                return Err(AppError::InvalidInput(format!(
                    "missing required source secret '{}'",
                    input.key
                )));
            }
            _ => {}
        }
    }

    Ok(ValidatedBindings {
        variables: variable_values,
        secrets: secret_values,
    })
}

fn collect_unique_variables(
    workspace_manager: &WorkspaceManager,
    values: &[SourceVariable],
) -> Result<BTreeMap<String, String>, AppError> {
    let mut unique = BTreeMap::new();
    for variable in values {
        let key = workspace_manager.validate_name("source variable key", &variable.key)?;
        if unique.insert(key.clone(), variable.value.clone()).is_some() {
            return Err(AppError::InvalidInput(format!(
                "source variable '{key}' is repeated"
            )));
        }
    }
    Ok(unique)
}

fn collect_unique_secrets(
    workspace_manager: &WorkspaceManager,
    values: &[SourceSecret],
) -> Result<BTreeMap<String, String>, AppError> {
    let mut unique = BTreeMap::new();
    for secret in values {
        let key = workspace_manager.validate_name("source secret key", &secret.key)?;
        if unique.insert(key.clone(), secret.value.clone()).is_some() {
            return Err(AppError::InvalidInput(format!(
                "source secret '{key}' is repeated"
            )));
        }
    }
    Ok(unique)
}

fn cleanup_empty_parent(root: &PathBuf, parent: Option<&std::path::Path>) {
    let Some(mut current) = parent.map(PathBuf::from) else {
        return;
    };
    while current.starts_with(root) && current != *root {
        let is_empty = current
            .read_dir()
            .ok()
            .is_some_and(|mut entries| entries.next().is_none());
        if !is_empty {
            break;
        }
        let _ = std::fs::remove_dir(&current);
        let Some(next) = current.parent() else {
            break;
        };
        current = next.to_path_buf();
    }
}

#[cfg(test)]
mod tests {
    use coral_api::v1::{ImportSourceRequest, SourceSecret, SourceVariable, Workspace};
    use tempfile::TempDir;

    use super::SourceManager;
    use crate::state::{AppStateLayout, ConfigStore, SecretStore};
    use crate::workspaces::WorkspaceManager;

    fn default_workspace() -> Workspace {
        WorkspaceManager::new().default_workspace()
    }

    fn manifest_with_secret() -> String {
        r#"
name: secured_messages
version: 0.1.0
dsl_version: 3
backend: http
base_url: "{{variable.API_BASE|https://example.com}}"
auth:
  headers:
    - name: Authorization
      from: template
      template: Bearer {{secret.API_TOKEN}}
tables:
  - name: messages
    description: Secured messages
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
    fn import_restores_prior_state_when_secret_persistence_fails() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let manager = SourceManager::new(
            ConfigStore::new(layout.clone()),
            SecretStore::new(layout.clone()),
            layout.clone(),
        );

        let source_dir = layout.source_dir(&default_workspace(), "secured_messages");
        std::fs::create_dir_all(&source_dir).expect("create source dir");
        std::fs::create_dir(source_dir.join("secrets.env"))
            .expect("create blocking secrets directory");

        let error = manager
            .import_source(&ImportSourceRequest {
                workspace: Some(default_workspace()),
                manifest_yaml: manifest_with_secret(),
                variables: vec![SourceVariable {
                    key: "API_BASE".to_string(),
                    value: "https://example.com".to_string(),
                }],
                secrets: vec![SourceSecret {
                    key: "API_TOKEN".to_string(),
                    value: "secret-token".to_string(),
                }],
            })
            .expect_err("secret persistence should fail");

        assert!(
            matches!(
                error,
                crate::bootstrap::AppError::Credentials(crate::state::CredentialsError::Io(_))
            ),
            "unexpected error: {error:#}"
        );
        assert!(
            !layout
                .source_dir(&default_workspace(), "secured_messages")
                .exists(),
            "source dir should be cleaned up after secret persistence failure"
        );
        assert!(
            manager
                .list_workspace_sources(&default_workspace())
                .expect("list sources")
                .is_empty(),
            "source config should not be persisted after rollback"
        );
    }
}
