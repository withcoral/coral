//! Owns the source lifecycle workflow for the local app.

use coral_api::v1::{
    CreateBundledSourceRequest, ImportSourceRequest, SourceSecret, SourceVariable, Workspace,
};
use std::collections::{BTreeMap, BTreeSet};

use crate::bootstrap::AppError;
use crate::sources::catalog::{describe_manifest, list_bundled_sources, load_bundled_source};
use crate::sources::model::{
    CandidateSource, CandidateSourceInputKind, InstalledSource, SourceOrigin,
};
use crate::state::{AppStateLayout, ConfigStore, SecretStore};
use crate::storage::fs;
use crate::workspaces::WorkspaceValidator;

#[derive(Clone)]
pub(crate) struct SourceManager {
    config_store: ConfigStore,
    secret_store: SecretStore,
    layout: AppStateLayout,
    workspace_validator: WorkspaceValidator,
}

struct ValidatedBindings {
    variables: BTreeMap<String, String>,
    secrets: BTreeMap<String, String>,
}

struct PersistSourceRequest<'a> {
    candidate: &'a CandidateSource,
    manifest_yaml: &'a str,
    bindings: ValidatedBindings,
    origin: SourceOrigin,
}

struct SourceRollbackState {
    source: InstalledSource,
    manifest_yaml: String,
    secrets: BTreeMap<String, String>,
}

impl SourceManager {
    pub(crate) fn new(
        config_store: ConfigStore,
        secret_store: SecretStore,
        layout: AppStateLayout,
        workspace_validator: WorkspaceValidator,
    ) -> Self {
        Self {
            config_store,
            secret_store,
            layout,
            workspace_validator,
        }
    }

    pub(crate) fn list_workspace_sources(
        &self,
        workspace: &Workspace,
    ) -> Result<Vec<InstalledSource>, AppError> {
        self.config_store.list_workspace_sources(workspace)
    }

    pub(crate) fn get_source(
        &self,
        workspace: &Workspace,
        source_name: &str,
    ) -> Result<InstalledSource, AppError> {
        self.config_store.get_source(workspace, source_name)
    }

    pub(crate) fn discover_sources(
        &self,
        workspace: &Workspace,
    ) -> Result<Vec<CandidateSource>, AppError> {
        let installed = self
            .config_store
            .load_catalog()?
            .workspace_sources(workspace)
            .into_iter()
            .map(|source| source.name)
            .collect::<BTreeSet<_>>();
        list_bundled_sources(&installed)
    }

    pub(crate) fn create_bundled_source(
        &self,
        workspace: &Workspace,
        request: &CreateBundledSourceRequest,
    ) -> Result<InstalledSource, AppError> {
        let bundled_name = self
            .workspace_validator
            .validate_path_name("source name", &request.name)?;
        let bundled = load_bundled_source(&bundled_name)?;
        let candidate = self.describe_bundled_source(workspace, &bundled.manifest_yaml)?;
        let bindings = validate_bindings(
            &self.workspace_validator,
            &candidate,
            &request.variables,
            &request.secrets,
        )?;
        self.persist_source(
            workspace,
            PersistSourceRequest {
                candidate: &candidate,
                manifest_yaml: &bundled.manifest_yaml,
                bindings,
                origin: SourceOrigin::Bundled,
            },
        )
    }

    pub(crate) fn import_source(
        &self,
        workspace: &Workspace,
        request: &ImportSourceRequest,
    ) -> Result<InstalledSource, AppError> {
        let mut candidate =
            describe_manifest(&request.manifest_yaml, SourceOrigin::Imported, false)?;
        candidate.installed = self.source_exists(workspace, &candidate.name)?;
        let bindings = validate_bindings(
            &self.workspace_validator,
            &candidate,
            &request.variables,
            &request.secrets,
        )?;
        self.persist_source(
            workspace,
            PersistSourceRequest {
                candidate: &candidate,
                manifest_yaml: &request.manifest_yaml,
                bindings,
                origin: SourceOrigin::Imported,
            },
        )
    }

    pub(crate) fn delete_source(
        &self,
        workspace: &Workspace,
        source_name: &str,
    ) -> Result<InstalledSource, AppError> {
        let stored = self.config_store.get_source(workspace, source_name)?;
        let source_dir = self.layout.source_dir(workspace, source_name);
        let previous = SourceRollbackState {
            source: stored.clone(),
            manifest_yaml: std::fs::read_to_string(
                self.layout.manifest_file(workspace, source_name),
            )?,
            secrets: self
                .secret_store
                .read_source_secrets_for(workspace, source_name)?,
        };
        if source_dir.exists()
            && let Err(error) = std::fs::remove_dir_all(&source_dir)
        {
            self.restore_source_rollback_state(workspace, source_name, Some(previous));
            return Err(error.into());
        }
        if let Err(error) = self.config_store.remove_source(workspace, source_name) {
            self.restore_source_rollback_state(workspace, source_name, Some(previous));
            return Err(error);
        }
        cleanup_empty_parent(&self.layout.workspaces_root(), source_dir.parent());
        cleanup_empty_parent(
            &self.layout.workspaces_root(),
            self.layout.workspace_dir(workspace).parent(),
        );
        Ok(stored)
    }

    fn describe_bundled_source(
        &self,
        workspace: &Workspace,
        manifest_yaml: &str,
    ) -> Result<CandidateSource, AppError> {
        let mut candidate = describe_manifest(manifest_yaml, SourceOrigin::Bundled, false)?;
        candidate.installed = self.source_exists(workspace, &candidate.name)?;
        Ok(candidate)
    }

    fn persist_source(
        &self,
        workspace: &Workspace,
        request: PersistSourceRequest<'_>,
    ) -> Result<InstalledSource, AppError> {
        let source_name = self
            .workspace_validator
            .validate_path_name("source name", &request.candidate.name)?;
        let previous = self.load_source_rollback_state(workspace, &source_name)?;
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
                self.restore_source_rollback_state(workspace, &source_name, previous);
                return Err(error);
            }
        };

        let stored = InstalledSource {
            name: source_name,
            version: request.candidate.version.clone(),
            variables: request.bindings.variables,
            secrets: persisted_secrets,
            origin: request.origin,
        };
        if let Err(error) = self.config_store.upsert_source(workspace, stored.clone()) {
            self.restore_source_rollback_state(workspace, &stored.name, previous);
            return Err(error);
        }
        Ok(stored)
    }

    fn source_exists(&self, workspace: &Workspace, source_name: &str) -> Result<bool, AppError> {
        Ok(self
            .config_store
            .load_catalog()?
            .contains(workspace, source_name))
    }

    fn load_source_rollback_state(
        &self,
        workspace: &Workspace,
        source_name: &str,
    ) -> Result<Option<SourceRollbackState>, AppError> {
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
        Ok(Some(SourceRollbackState {
            source,
            manifest_yaml,
            secrets,
        }))
    }

    fn restore_source_rollback_state(
        &self,
        workspace: &Workspace,
        source_name: &str,
        previous: Option<SourceRollbackState>,
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
            let _ = self.config_store.upsert_source(workspace, previous.source);
        } else {
            let source_dir = self.layout.source_dir(workspace, source_name);
            if source_dir.exists() {
                let _ = std::fs::remove_dir_all(&source_dir);
            }
        }
    }
}

fn validate_bindings(
    workspace_validator: &WorkspaceValidator,
    candidate: &CandidateSource,
    variables: &[SourceVariable],
    secrets: &[SourceSecret],
) -> Result<ValidatedBindings, AppError> {
    let variable_values = collect_unique_variables(workspace_validator, variables)?;
    let secret_values = collect_unique_secrets(workspace_validator, secrets)?;
    let expected_variables = candidate
        .inputs
        .iter()
        .filter(|input| input.kind == CandidateSourceInputKind::Variable)
        .map(|input| input.key.clone())
        .collect::<BTreeSet<_>>();
    let expected_secrets = candidate
        .inputs
        .iter()
        .filter(|input| input.kind == CandidateSourceInputKind::Secret)
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

    for input in &candidate.inputs {
        match input.kind {
            CandidateSourceInputKind::Variable
                if input.required && !variable_values.contains_key(&input.key) =>
            {
                return Err(AppError::InvalidInput(format!(
                    "missing required source variable '{}'",
                    input.key
                )));
            }
            CandidateSourceInputKind::Secret
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
    workspace_validator: &WorkspaceValidator,
    variables: &[SourceVariable],
) -> Result<BTreeMap<String, String>, AppError> {
    let mut values = BTreeMap::new();
    for variable in variables {
        let key = workspace_validator.validate_name("source variable key", &variable.key)?;
        if values.insert(key.clone(), variable.value.clone()).is_some() {
            return Err(AppError::InvalidInput(format!(
                "duplicate source variable '{key}'"
            )));
        }
    }
    Ok(values)
}

fn collect_unique_secrets(
    workspace_validator: &WorkspaceValidator,
    secrets: &[SourceSecret],
) -> Result<BTreeMap<String, String>, AppError> {
    let mut values = BTreeMap::new();
    for secret in secrets {
        let key = workspace_validator.validate_name("source secret key", &secret.key)?;
        if values.insert(key.clone(), secret.value.clone()).is_some() {
            return Err(AppError::InvalidInput(format!(
                "duplicate source secret '{key}'"
            )));
        }
    }
    Ok(values)
}

fn cleanup_empty_parent(root: &std::path::Path, path: Option<&std::path::Path>) {
    let Some(mut current) = path.map(std::path::Path::to_path_buf) else {
        return;
    };
    while current.starts_with(root) && current != root {
        let Ok(mut entries) = std::fs::read_dir(&current) else {
            break;
        };
        if entries.next().is_some() {
            break;
        }
        let next = current.parent().unwrap_or(root).to_path_buf();
        if std::fs::remove_dir(&current).is_err() {
            break;
        }
        current = next;
    }
}

#[cfg(test)]
mod tests {
    use coral_api::v1::{ImportSourceRequest, SourceSecret, SourceVariable, Workspace};
    use tempfile::TempDir;

    use super::SourceManager;
    use crate::state::{AppStateLayout, ConfigStore, SecretStore};
    use crate::workspaces::WorkspaceValidator;

    fn default_workspace() -> Workspace {
        WorkspaceValidator::new().default_workspace()
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
            WorkspaceValidator::new(),
        );

        let source_dir = layout.source_dir(&default_workspace(), "secured_messages");
        std::fs::create_dir_all(&source_dir).expect("create source dir");
        std::fs::create_dir(source_dir.join("secrets.env"))
            .expect("create blocking secrets directory");

        let error = manager
            .import_source(
                &default_workspace(),
                &ImportSourceRequest {
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
                },
            )
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
