//! Owns the source lifecycle workflow for the local app.

use std::collections::{BTreeMap, BTreeSet};
use std::io::ErrorKind;
use std::path::PathBuf;

use coral_api::v1::{
    AvailableSource, BundledManifestState, CreateBundledSourceRequest, ImportSourceRequest, Source,
    SourceInputKind, SourceSecret, SourceVariable, Workspace,
};

use crate::bootstrap::AppError;
use crate::sources::bundled_store::BundledStore;
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
    bundled_store: BundledStore,
    workspace_manager: WorkspaceManager,
}

struct ValidatedBindings {
    variables: BTreeMap<String, String>,
    secrets: BTreeMap<String, String>,
}

enum ManifestPersistence<'a> {
    WorkspaceManifest(&'a str),
    BundledTracking { bundle_id: String },
}

struct PersistSourceRequest<'a> {
    available: &'a AvailableSource,
    bindings: ValidatedBindings,
    origin: ManagedSourceOrigin,
    manifest_persistence: ManifestPersistence<'a>,
}

struct ExistingSourceState {
    source: ManagedSource,
    manifest_yaml: Option<String>,
    bundled_manifest_tracking: Option<String>,
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
            bundled_store: BundledStore::new(layout.clone()),
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

    pub(crate) fn list_workspace_source_resources(
        &self,
        workspace: &Workspace,
    ) -> Result<Vec<Source>, AppError> {
        self.list_workspace_sources(workspace)?
            .into_iter()
            .map(|source| self.to_source_resource(&source))
            .collect()
    }

    pub(crate) fn get_source(
        &self,
        workspace: &Workspace,
        source_name: &str,
    ) -> Result<ManagedSource, AppError> {
        self.config_store.get_source(workspace, source_name)
    }

    pub(crate) fn get_source_resource(
        &self,
        workspace: &Workspace,
        source_name: &str,
    ) -> Result<Source, AppError> {
        let source = self.get_source(workspace, source_name)?;
        self.to_source_resource(&source)
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
        let bundle_id = self
            .bundled_store
            .ensure_current_bundle_available(&bundled)?;
        self.persist_source(
            &workspace,
            PersistSourceRequest {
                available: &available,
                bindings,
                origin: ManagedSourceOrigin::Bundled,
                manifest_persistence: ManifestPersistence::BundledTracking { bundle_id },
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
                bindings,
                origin: ManagedSourceOrigin::Imported,
                manifest_persistence: ManifestPersistence::WorkspaceManifest(
                    &request.manifest_yaml,
                ),
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
        let previous = self
            .load_existing_state(&stored.workspace, &stored.name)?
            .ok_or_else(|| {
                AppError::FailedPrecondition(format!(
                    "source '{}' exists in config without on-disk state",
                    stored.name
                ))
            })?;
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
        let source_dir = self.layout.source_dir(workspace, &source_name);
        if let Err(error) = fs::ensure_dir(&source_dir) {
            self.restore_existing_state(workspace, &source_name, previous);
            return Err(error.into());
        }

        if let Err(error) =
            self.persist_manifest_artifacts(workspace, &source_name, &request.manifest_persistence)
        {
            self.restore_existing_state(workspace, &source_name, previous);
            return Err(error);
        }

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

    fn persist_manifest_artifacts(
        &self,
        workspace: &Workspace,
        source_name: &str,
        persistence: &ManifestPersistence<'_>,
    ) -> Result<(), AppError> {
        let manifest_path = self.layout.manifest_file(workspace, source_name);
        let tracking_path = self
            .layout
            .bundled_manifest_tracking_file(workspace, source_name);

        match persistence {
            ManifestPersistence::WorkspaceManifest(manifest_yaml) => {
                fs::write_atomic(&manifest_path, manifest_yaml.as_bytes())?;
                remove_file_if_exists(&tracking_path)?;
            }
            ManifestPersistence::BundledTracking { bundle_id } => {
                self.bundled_store
                    .write_tracking_file(workspace, source_name, bundle_id)?;
                remove_file_if_exists(&manifest_path)?;
            }
        }
        Ok(())
    }

    fn to_source_resource(&self, source: &ManagedSource) -> Result<Source, AppError> {
        if source.origin.is_bundled() {
            let resolved = self.bundled_store.resolve_manifest(source)?;
            Ok(source.to_source_resource_with(resolved.version, resolved.bundled_manifest_state))
        } else {
            Ok(source.to_source_resource_with(
                source.version.clone(),
                BundledManifestState::NotApplicable,
            ))
        }
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
        let manifest_yaml = read_optional_file(self.layout.manifest_file(workspace, source_name))?;
        let bundled_manifest_tracking = read_optional_file(
            self.layout
                .bundled_manifest_tracking_file(workspace, source_name),
        )?;
        let secrets = self
            .secret_store
            .read_source_secrets_for(workspace, source_name)?;
        Ok(Some(ExistingSourceState {
            source,
            manifest_yaml,
            bundled_manifest_tracking,
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
            let source_dir = self.layout.source_dir(workspace, source_name);
            let _ = fs::ensure_dir(&source_dir);

            restore_optional_file(
                &self.layout.manifest_file(workspace, source_name),
                previous.manifest_yaml.as_deref(),
            );
            restore_optional_file(
                &self
                    .layout
                    .bundled_manifest_tracking_file(workspace, source_name),
                previous.bundled_manifest_tracking.as_deref(),
            );
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

fn read_optional_file(path: PathBuf) -> Result<Option<String>, AppError> {
    match std::fs::read_to_string(path) {
        Ok(raw) => Ok(Some(raw)),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn restore_optional_file(path: &std::path::Path, contents: Option<&str>) {
    match contents {
        Some(contents) => {
            if let Some(parent) = path.parent() {
                let _ = fs::ensure_dir(parent);
            }
            let _ = fs::write_atomic(path, contents.as_bytes());
        }
        None => {
            let _ = remove_file_if_exists(path);
        }
    }
}

fn remove_file_if_exists(path: &std::path::Path) -> Result<(), AppError> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
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
