//! Query source input resolvers backed by app-managed credential material.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use coral_engine::{SourceInputResolutionContext, SourceInputResolver, SourceInputResolverError};
use coral_spec::{ManifestInputKind, ManifestInputSpec};
use tokio::sync::Mutex;

use crate::bootstrap::AppError;
use crate::credentials::{CredentialManager, CredentialSetId, CredentialsError};
use crate::sources::model::InstalledSource;
use crate::state::db::{CoralDb, DbRepos};
use crate::workspaces::WorkspaceName;

type SourceCredentialMaterial = BTreeMap<String, String>;
type SharedSourceCredentialMaterial = Arc<Mutex<SourceCredentialMaterial>>;
type SourceCredentialSnapshotByName = BTreeMap<String, SharedSourceCredentialSnapshot>;

#[derive(Clone)]
pub(crate) struct SourceCredentialSnapshot {
    pub(crate) source: InstalledSource,
    pub(crate) material: SourceCredentialMaterial,
}

#[derive(Clone)]
struct SharedSourceCredentialSnapshot {
    source: InstalledSource,
    material: SharedSourceCredentialMaterial,
}

#[derive(Clone)]
pub(crate) struct CredentialRefreshingInputResolver {
    workspace_name: WorkspaceName,
    db: Arc<CoralDb>,
    credential_manager: CredentialManager,
    source_credentials: Arc<SourceCredentialSnapshotByName>,
    delegate: Option<Arc<dyn SourceInputResolver>>,
}

impl CredentialRefreshingInputResolver {
    pub(crate) fn new(
        workspace_name: WorkspaceName,
        db: Arc<CoralDb>,
        credential_manager: CredentialManager,
        source_credentials: BTreeMap<String, SourceCredentialSnapshot>,
        delegate: Option<Arc<dyn SourceInputResolver>>,
    ) -> Self {
        Self {
            workspace_name,
            db,
            credential_manager,
            source_credentials: shared_source_credentials(source_credentials),
            delegate,
        }
    }
}

#[derive(Clone)]
pub(crate) struct StoredCredentialInputResolver {
    source_credentials: Arc<SourceCredentialSnapshotByName>,
    delegate: Option<Arc<dyn SourceInputResolver>>,
}

impl StoredCredentialInputResolver {
    pub(crate) fn new(
        source_credentials: BTreeMap<String, SourceCredentialSnapshot>,
        delegate: Option<Arc<dyn SourceInputResolver>>,
    ) -> Self {
        Self {
            source_credentials: shared_source_credentials(source_credentials),
            delegate,
        }
    }
}

impl fmt::Debug for StoredCredentialInputResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoredCredentialInputResolver")
            .field("source_count", &self.source_credentials.len())
            .field("has_delegate", &self.delegate.is_some())
            .finish_non_exhaustive()
    }
}

#[tonic::async_trait]
impl SourceInputResolver for StoredCredentialInputResolver {
    async fn resolve_inputs(
        &self,
        source: &SourceInputResolutionContext,
    ) -> Result<BTreeMap<String, String>, SourceInputResolverError> {
        let material =
            if let Some(source_credentials) = self.source_credentials.get(source.source_name()) {
                source_credentials.material.lock().await.clone()
            } else {
                BTreeMap::new()
            };
        resolve_inputs_from_material(source, &material, self.delegate.as_ref()).await
    }
}

impl fmt::Debug for CredentialRefreshingInputResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CredentialRefreshingInputResolver")
            .field("source_count", &self.source_credentials.len())
            .field("has_delegate", &self.delegate.is_some())
            .finish_non_exhaustive()
    }
}

#[tonic::async_trait]
impl SourceInputResolver for CredentialRefreshingInputResolver {
    async fn resolve_inputs(
        &self,
        source: &SourceInputResolutionContext,
    ) -> Result<BTreeMap<String, String>, SourceInputResolverError> {
        let material =
            if let Some(source_credentials) = self.source_credentials.get(source.source_name()) {
                let mut material = source_credentials.material.lock().await;
                self.refresh_source_material(source, source_credentials, &mut material)
                    .await?;
                material.clone()
            } else {
                BTreeMap::new()
            };
        resolve_inputs_from_material(source, &material, self.delegate.as_ref()).await
    }
}

impl CredentialRefreshingInputResolver {
    async fn refresh_source_material(
        &self,
        source: &SourceInputResolutionContext,
        snapshot: &SharedSourceCredentialSnapshot,
        material: &mut BTreeMap<String, String>,
    ) -> Result<(), SourceInputResolverError> {
        if !has_oauth_credential_inputs(source.declared_inputs()) {
            return Ok(());
        }
        let Some(storage) = snapshot.source.credential_storage_for_material() else {
            return self
                .credential_manager
                .refresh_material_for_inputs(source.declared_inputs(), material)
                .await
                .map_err(source_input_error);
        };

        let credential_set_id = CredentialSetId::for_source(&snapshot.source.name);
        let _refresh_lock = self
            .credential_manager
            .credential_refresh_lock(&self.workspace_name, &credential_set_id)
            .await
            .map_err(source_input_error)?;
        if !self
            .source_catalog_still_matches_snapshot(&snapshot.source)
            .await?
        {
            return self
                .credential_manager
                .refresh_material_for_inputs(source.declared_inputs(), material)
                .await
                .map_err(source_input_error);
        }

        let mut current_material = self
            .credential_manager
            .read_material(&self.workspace_name, &credential_set_id, storage)
            .map_err(source_input_error)?;
        self.credential_manager
            .refresh_and_persist_material_for_inputs_with_refresh_lock_held(
                &self.workspace_name,
                &credential_set_id,
                storage,
                source.declared_inputs(),
                &mut current_material,
            )
            .await
            .map_err(source_input_error)?;
        *material = current_material;
        Ok(())
    }

    async fn source_catalog_still_matches_snapshot(
        &self,
        snapshot: &InstalledSource,
    ) -> Result<bool, SourceInputResolverError> {
        let mut session = self.db.as_ref();
        let current = session
            .sources()
            .get_source(&self.workspace_name, &snapshot.name)
            .await
            .map_err(AppError::from)
            .map_err(source_input_error)?;
        Ok(current.as_ref() == Some(snapshot))
    }
}

fn resolve_from_material(
    source: &SourceInputResolutionContext,
    material: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    coral_spec::resolve_inputs(source.declared_inputs(), material, source.variables())
}

async fn resolve_inputs_from_material(
    source: &SourceInputResolutionContext,
    material: &BTreeMap<String, String>,
    delegate: Option<&Arc<dyn SourceInputResolver>>,
) -> Result<BTreeMap<String, String>, SourceInputResolverError> {
    let mut resolved = resolve_from_material(source, material);
    if let Some(delegate) = delegate {
        let delegated_source = source_with_material_secrets(source, material);
        for (key, value) in delegate.resolve_inputs(&delegated_source).await? {
            resolved.entry(key).or_insert(value);
        }
    }
    let missing_secrets: Vec<String> = source
        .required_secret_names()
        .into_iter()
        .filter(|name| !resolved.contains_key(name))
        .collect();
    if let Some((first, rest)) = missing_secrets.split_first() {
        let detail = if rest.is_empty() {
            format!("secret '{first}'")
        } else {
            format!("secret '{first}' and {} other(s)", rest.len())
        };
        return Err(SourceInputResolverError::failed_precondition(format!(
            "source '{}' is missing {detail}",
            source.source_name()
        )));
    }
    Ok(resolved)
}

fn has_oauth_credential_inputs(inputs: &[ManifestInputSpec]) -> bool {
    inputs.iter().any(|input| {
        input.kind == ManifestInputKind::Secret
            && input.credential.as_ref().is_some_and(|credential| {
                credential
                    .methods
                    .iter()
                    .any(|method| method.oauth.is_some())
            })
    })
}

fn source_with_material_secrets(
    source: &SourceInputResolutionContext,
    material: &BTreeMap<String, String>,
) -> SourceInputResolutionContext {
    let refreshed_secrets = source
        .declared_inputs()
        .iter()
        .filter(|input| input.kind == ManifestInputKind::Secret)
        .filter_map(|input| {
            material
                .get(&input.key)
                .cloned()
                .map(|value| (input.key.clone(), value))
        })
        .collect();
    source.with_secrets(refreshed_secrets)
}

fn shared_source_credentials(
    source_credentials: BTreeMap<String, SourceCredentialSnapshot>,
) -> Arc<SourceCredentialSnapshotByName> {
    Arc::new(
        source_credentials
            .into_iter()
            .map(|(source_name, snapshot)| {
                (
                    source_name,
                    SharedSourceCredentialSnapshot {
                        source: snapshot.source,
                        material: Arc::new(Mutex::new(snapshot.material)),
                    },
                )
            })
            .collect(),
    )
}

fn source_input_error(error: AppError) -> SourceInputResolverError {
    match error {
        AppError::InvalidInput(detail) => SourceInputResolverError::invalid_input(detail),
        AppError::FailedPrecondition(detail) | AppError::CredentialRefresh(detail) => {
            SourceInputResolverError::failed_precondition(detail)
        }
        AppError::Credentials(CredentialsError::Parse(detail)) => {
            SourceInputResolverError::failed_precondition(format!(
                "credential material could not be parsed: {detail}"
            ))
        }
        other => SourceInputResolverError::failed_precondition(other.to_string()),
    }
}
