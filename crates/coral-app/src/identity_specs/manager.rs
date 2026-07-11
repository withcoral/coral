//! Read and resolution behavior for installed identity specs.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use coral_spec::{IdentityManifest, ManifestInputKind, parse_identity_manifest_yaml};

use crate::bootstrap::AppError;
use crate::credentials::encryption::{CredentialKeyProvider, EncryptedEnvelopeDocument};
use crate::identity::{
    decrypt_identity_spec_document, encrypt_identity_spec_document, parse_path_segment,
};
use crate::state::db::{
    CoralDb, CoralTx, DbRepos, IdentitySpecDocumentRecord, IdentitySpecDocumentWrite,
    IdentitySpecKey, IdentitySpecRecord, IdentitySpecScope, IdentitySpecWrite, now_unix_nanos_i64,
};
use crate::workspaces::WorkspaceName;

const MAX_MUTATION_ATTEMPTS: usize = 8;

/// One installed identity spec, including the scope that actually supplied it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct InstalledIdentitySpec {
    pub(crate) key: IdentitySpecKey,
    pub(crate) manifest_yaml: String,
    pub(crate) manifest: IdentityManifest,
}

/// One caller-supplied setup input whose value must never appear in diagnostics.
pub(crate) struct IdentitySpecInputValue {
    key: String,
    value: String,
}

impl IdentitySpecInputValue {
    pub(crate) fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

impl fmt::Debug for IdentitySpecInputValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IdentitySpecInputValue")
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

pub(crate) struct PreparedIdentitySpecInputMaterial {
    pub(crate) values: BTreeMap<String, String>,
}

impl fmt::Debug for PreparedIdentitySpecInputMaterial {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedIdentitySpecInputMaterial")
            .field("value_count", &self.values.len())
            .finish()
    }
}

pub(crate) struct ResolvedIdentitySpec {
    pub(crate) spec: InstalledIdentitySpec,
    pub(crate) inputs: ResolvedIdentitySpecInputs,
}

impl fmt::Debug for ResolvedIdentitySpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResolvedIdentitySpec")
            .field("key", &self.spec.key)
            .field("inputs", &self.inputs)
            .finish_non_exhaustive()
    }
}

pub(crate) struct ResolvedIdentitySpecInputs {
    variables: BTreeMap<String, String>,
    secrets: BTreeMap<String, String>,
}

impl ResolvedIdentitySpecInputs {
    pub(crate) fn variables(&self) -> &BTreeMap<String, String> {
        &self.variables
    }

    pub(crate) fn secrets(&self) -> &BTreeMap<String, String> {
        &self.secrets
    }
}

impl fmt::Debug for ResolvedIdentitySpecInputs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResolvedIdentitySpecInputs")
            .field("variable_count", &self.variables.len())
            .field("secret_count", &self.secrets.len())
            .finish()
    }
}

/// Database-backed identity-spec read and resolution behavior.
#[derive(Clone)]
pub(crate) struct IdentitySpecManager {
    db: Arc<CoralDb>,
    key_provider: Arc<dyn CredentialKeyProvider>,
    #[cfg(test)]
    mutation_barrier: Option<Arc<tokio::sync::Barrier>>,
}

#[derive(Clone, PartialEq, Eq)]
struct IdentitySpecMutationSnapshot {
    record: Option<IdentitySpecRecord>,
    document: Option<IdentitySpecDocumentRecord>,
}

impl IdentitySpecManager {
    pub(crate) fn new(db: Arc<CoralDb>, key_provider: Arc<dyn CredentialKeyProvider>) -> Self {
        Self {
            db,
            key_provider,
            #[cfg(test)]
            mutation_barrier: None,
        }
    }

    #[cfg(test)]
    fn with_mutation_barrier(mut self, barrier: Arc<tokio::sync::Barrier>) -> Self {
        self.mutation_barrier = Some(barrier);
        self
    }

    /// Install or replace one spec in exactly the selected scope.
    pub(crate) async fn add_or_replace_exact(
        &self,
        scope: IdentitySpecScope,
        manifest_yaml: &str,
        input_values: Vec<IdentitySpecInputValue>,
    ) -> Result<(InstalledIdentitySpec, bool), AppError> {
        let manifest = parse_identity_manifest_yaml(manifest_yaml)
            .map_err(|error| AppError::InvalidInput(error.to_string()))?;
        let key = IdentitySpecKey::new(scope, &manifest.name)?;
        let write = IdentitySpecWrite::new(
            manifest.version.clone(),
            manifest.description.clone(),
            manifest.issuer.clone(),
            manifest.identity_type.label(),
            manifest_yaml,
        )?;
        #[cfg(test)]
        let mut mutation_barrier = self.mutation_barrier.clone();
        for _ in 0..MAX_MUTATION_ATTEMPTS {
            let snapshot = match self.load_mutation_snapshot(&key).await {
                Ok(snapshot) => snapshot,
                Err(AppError::RetryableTransactionConflict) => {
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            #[cfg(test)]
            if let Some(barrier) = mutation_barrier.take() {
                barrier.wait().await;
            }
            let previous = snapshot
                .record
                .clone()
                .map(record_to_installed)
                .transpose()?;
            let previous_material = decrypt_input_material(
                &key,
                snapshot.document.clone(),
                self.key_provider.as_ref(),
            )?;
            let prepared = prepare_identity_spec_input_material(
                &key,
                &manifest,
                previous.as_ref().map(|installed| &installed.manifest),
                &previous_material,
                &input_values,
            )?;
            let document =
                prepare_document_write(&key, &prepared.values, self.key_provider.as_ref())?;
            let replaced = snapshot.record.is_some();

            match self
                .try_write_mutation(&key, &snapshot, &write, document.as_ref())
                .await
            {
                Ok(Some(installed)) => return Ok((installed, replaced)),
                Ok(None) | Err(AppError::RetryableTransactionConflict) => {
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(mutation_retry_exhausted())
    }

    /// Delete one spec in exactly the selected scope.
    pub(crate) async fn delete_exact(&self, key: &IdentitySpecKey) -> Result<(), AppError> {
        for _ in 0..MAX_MUTATION_ATTEMPTS {
            match self.try_delete_exact(key).await {
                Ok(()) => return Ok(()),
                Err(AppError::RetryableTransactionConflict) => {
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(mutation_retry_exhausted())
    }

    async fn load_mutation_snapshot(
        &self,
        key: &IdentitySpecKey,
    ) -> Result<IdentitySpecMutationSnapshot, AppError> {
        let mut tx = self.db.begin_read_snapshot().await?;
        let snapshot = load_mutation_snapshot(&mut tx, key).await?;
        tx.commit().await?;
        Ok(snapshot)
    }

    async fn try_write_mutation(
        &self,
        key: &IdentitySpecKey,
        expected: &IdentitySpecMutationSnapshot,
        write: &IdentitySpecWrite,
        document: Option<&IdentitySpecDocumentWrite>,
    ) -> Result<Option<InstalledIdentitySpec>, AppError> {
        let mut tx = self.db.begin_serializable().await?;
        let current = load_mutation_snapshot(&mut tx, key).await?;
        if current != *expected {
            tx.rollback().await?;
            return Ok(None);
        }
        let now = now_unix_nanos_i64()?;
        let result = async {
            let record = tx.identity_specs().upsert(key, write, now).await?;
            match document {
                Some(document) => {
                    tx.identity_spec_documents()
                        .upsert(key, document, now)
                        .await?;
                }
                None => {
                    tx.identity_spec_documents().delete(key).await?;
                }
            }
            record_to_installed(record)
        }
        .await;
        let record = match result {
            Ok(record) => record,
            Err(error) => {
                tx.rollback().await?;
                return Err(error);
            }
        };
        tx.commit().await?;
        Ok(Some(record))
    }

    async fn try_delete_exact(&self, key: &IdentitySpecKey) -> Result<(), AppError> {
        let mut tx = self.db.begin_serializable().await?;
        require_scope_workspace(&mut tx, key.scope()).await?;
        let deleted = match tx.identity_specs().delete(key).await {
            Ok(deleted) => deleted,
            Err(error) => {
                tx.rollback().await?;
                return Err(error.into());
            }
        };
        if !deleted {
            tx.rollback().await?;
            return Err(spec_not_found(key));
        }
        tx.commit().await?;
        Ok(())
    }

    /// Fetch one spec in exactly the requested scope, without fallback.
    pub(crate) async fn get_exact(
        &self,
        key: &IdentitySpecKey,
    ) -> Result<InstalledIdentitySpec, AppError> {
        let mut tx = self.db.begin_read_snapshot().await?;
        require_scope_workspace(&mut tx, key.scope()).await?;
        let record = tx.identity_specs().load_optional(key).await?;
        tx.commit().await?;
        let installed = record
            .map(record_to_installed)
            .transpose()?
            .ok_or_else(|| spec_not_found(key))?;
        Ok(installed)
    }

    /// Fetch one global spec by name.
    pub(crate) async fn get_global(&self, name: &str) -> Result<InstalledIdentitySpec, AppError> {
        self.get_exact(&IdentitySpecKey::global(name)?).await
    }

    pub(crate) async fn get_exact_for_use(
        &self,
        key: &IdentitySpecKey,
    ) -> Result<ResolvedIdentitySpec, AppError> {
        let mut tx = self.db.begin_read_snapshot().await?;
        require_scope_workspace(&mut tx, key.scope()).await?;
        let record = tx.identity_specs().load_optional(key).await?;
        let document = match record.as_ref() {
            Some(record) => {
                tx.identity_spec_documents()
                    .load_optional(&record.key)
                    .await?
            }
            None => None,
        };
        tx.commit().await?;
        let record = record.ok_or_else(|| spec_not_found(key))?;
        resolve_record_for_use(record, document, self.key_provider.as_ref())
    }

    pub(crate) async fn get_global_for_use(
        &self,
        name: &str,
    ) -> Result<ResolvedIdentitySpec, AppError> {
        self.get_exact_for_use(&IdentitySpecKey::global(name)?)
            .await
    }

    /// List specs in exactly one scope, without fallback.
    pub(crate) async fn list_exact(
        &self,
        scope: &IdentitySpecScope,
    ) -> Result<Vec<InstalledIdentitySpec>, AppError> {
        let mut tx = self.db.begin_read_snapshot().await?;
        require_scope_workspace(&mut tx, scope).await?;
        let records = match scope {
            IdentitySpecScope::Global => tx.identity_specs().list_global().await?,
            IdentitySpecScope::Workspace(workspace) => {
                tx.identity_specs().list_workspace(workspace).await?
            }
        };
        tx.commit().await?;
        convert_records(records)
    }

    /// List global specs followed by workspace specs, preserving same-name entries.
    pub(crate) async fn list_workspace_with_global(
        &self,
        workspace: &WorkspaceName,
    ) -> Result<Vec<InstalledIdentitySpec>, AppError> {
        let mut tx = self.db.begin_read_snapshot().await?;
        require_workspace(&mut tx, workspace).await?;
        let mut records = tx.identity_specs().list_global().await?;
        records.extend(tx.identity_specs().list_workspace(workspace).await?);
        tx.commit().await?;
        convert_records(records)
    }

    /// Resolve one workspace spec, preferring its workspace definition over global.
    pub(crate) async fn resolve_for_workspace(
        &self,
        workspace: &WorkspaceName,
        name: &str,
    ) -> Result<InstalledIdentitySpec, AppError> {
        let key = IdentitySpecKey::workspace(workspace.clone(), name)?;
        let mut tx = self.db.begin_read_snapshot().await?;
        require_workspace(&mut tx, workspace).await?;
        let record = tx.identity_specs().resolve_optional(&key).await?;
        tx.commit().await?;
        let installed = record
            .map(record_to_installed)
            .transpose()?
            .ok_or_else(|| spec_not_found(&key))?;
        Ok(installed)
    }

    pub(crate) async fn resolve_for_workspace_for_use(
        &self,
        workspace: &WorkspaceName,
        name: &str,
    ) -> Result<ResolvedIdentitySpec, AppError> {
        let requested_key = IdentitySpecKey::workspace(workspace.clone(), name)?;
        let mut tx = self.db.begin_read_snapshot().await?;
        require_workspace(&mut tx, workspace).await?;
        let record = tx.identity_specs().resolve_optional(&requested_key).await?;
        let document = match record.as_ref() {
            Some(record) => {
                tx.identity_spec_documents()
                    .load_optional(&record.key)
                    .await?
            }
            None => None,
        };
        tx.commit().await?;
        let record = record.ok_or_else(|| spec_not_found(&requested_key))?;
        resolve_record_for_use(record, document, self.key_provider.as_ref())
    }

    /// List the effective specs for a workspace, shadowed and sorted by name.
    pub(crate) async fn list_resolved_for_workspace(
        &self,
        workspace: &WorkspaceName,
    ) -> Result<Vec<InstalledIdentitySpec>, AppError> {
        let mut tx = self.db.begin_read_snapshot().await?;
        require_workspace(&mut tx, workspace).await?;
        let records = tx
            .identity_specs()
            .list_resolved_for_workspace(workspace)
            .await?;
        tx.commit().await?;
        convert_records(records)
    }
}

pub(crate) fn prepare_identity_spec_input_material(
    key: &IdentitySpecKey,
    manifest: &IdentityManifest,
    previous_manifest: Option<&IdentityManifest>,
    previous_material: &BTreeMap<String, String>,
    provided: &[IdentitySpecInputValue],
) -> Result<PreparedIdentitySpecInputMaterial, AppError> {
    let previous_kinds = previous_manifest
        .map(|manifest| {
            manifest
                .inputs
                .iter()
                .map(|input| (input.key.as_str(), input.kind))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    if previous_material
        .keys()
        .any(|input_key| !previous_kinds.contains_key(input_key.as_str()))
    {
        return Err(corrupt_input_material(key));
    }

    let declared = manifest
        .inputs
        .iter()
        .map(|input| input.key.as_str())
        .collect::<BTreeSet<_>>();
    let mut supplied = BTreeMap::new();
    for input in provided {
        let input_key = parse_path_segment("identity spec input", &input.key)?;
        if supplied
            .insert(input_key.clone(), input.value.clone())
            .is_some()
        {
            return Err(AppError::InvalidInput(format!(
                "duplicate identity spec input '{input_key}' for identity spec '{}'",
                manifest.name
            )));
        }
    }
    for input_key in supplied.keys() {
        if !declared.contains(input_key.as_str()) {
            return Err(AppError::InvalidInput(format!(
                "unknown identity spec input '{input_key}' for identity spec '{}'",
                manifest.name
            )));
        }
    }

    let mut values = BTreeMap::new();
    for input in &manifest.inputs {
        let supplied_value = supplied
            .get(&input.key)
            .and_then(|value| trimmed_non_empty_value(value));
        let previous_value = if previous_kinds.get(input.key.as_str()) == Some(&input.kind) {
            previous_material
                .get(&input.key)
                .and_then(|value| trimmed_non_empty_value(value))
        } else {
            None
        };
        if let Some(value) = supplied_value.or(previous_value) {
            values.insert(input.key.clone(), value);
        }
    }
    resolve_identity_spec_inputs_for_use(key, manifest, &values).map_err(|error| match error {
        AppError::FailedPrecondition(detail) => AppError::InvalidInput(detail),
        other => other,
    })?;
    Ok(PreparedIdentitySpecInputMaterial { values })
}

async fn load_mutation_snapshot(
    tx: &mut CoralTx<'_>,
    key: &IdentitySpecKey,
) -> Result<IdentitySpecMutationSnapshot, AppError> {
    require_scope_workspace(tx, key.scope()).await?;
    let record = tx.identity_specs().load_optional(key).await?;
    let document = match record.as_ref() {
        Some(record) => {
            tx.identity_spec_documents()
                .load_optional(&record.key)
                .await?
        }
        None => None,
    };
    Ok(IdentitySpecMutationSnapshot { record, document })
}

fn prepare_document_write(
    key: &IdentitySpecKey,
    values: &BTreeMap<String, String>,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<Option<IdentitySpecDocumentWrite>, AppError> {
    if values.is_empty() {
        return Ok(None);
    }
    let (scope_kind, scope_id, name) = key.document_aad_parts();
    let document =
        encrypt_identity_spec_document(scope_kind, scope_id, name, values, key_provider)?;
    Ok(Some(IdentitySpecDocumentWrite::new(
        document.ciphertext,
        document.nonce,
        document.wrapped_dek,
        document.wrapped_dek_nonce,
        document.key_id,
        document.algorithm,
        document.aad_version,
    )?))
}

fn mutation_retry_exhausted() -> AppError {
    AppError::RetryableTransactionConflict
}

fn resolve_record_for_use(
    record: IdentitySpecRecord,
    document: Option<IdentitySpecDocumentRecord>,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<ResolvedIdentitySpec, AppError> {
    let spec = record_to_installed(record)?;
    let material = decrypt_input_material(&spec.key, document, key_provider)?;
    let inputs = resolve_identity_spec_inputs_for_use(&spec.key, &spec.manifest, &material)?;
    Ok(ResolvedIdentitySpec { spec, inputs })
}

fn decrypt_input_material(
    key: &IdentitySpecKey,
    document: Option<IdentitySpecDocumentRecord>,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<BTreeMap<String, String>, AppError> {
    let Some(document) = document else {
        return Ok(BTreeMap::new());
    };
    let IdentitySpecDocumentRecord {
        key: document_key,
        ciphertext,
        nonce,
        wrapped_dek,
        wrapped_dek_nonce,
        key_id,
        algorithm,
        aad_version,
        ..
    } = document;
    if document_key != *key {
        return Err(corrupt_input_material(key));
    }
    let envelope = EncryptedEnvelopeDocument {
        ciphertext,
        nonce,
        wrapped_dek,
        wrapped_dek_nonce,
        key_id,
        algorithm,
        aad_version,
    };
    let (scope_kind, scope_id, name) = key.document_aad_parts();
    decrypt_identity_spec_document(scope_kind, scope_id, name, &envelope, key_provider)
        .map_err(Into::into)
}

fn resolve_identity_spec_inputs_for_use(
    key: &IdentitySpecKey,
    manifest: &IdentityManifest,
    material: &BTreeMap<String, String>,
) -> Result<ResolvedIdentitySpecInputs, AppError> {
    let declared = manifest
        .inputs
        .iter()
        .map(|input| input.key.as_str())
        .collect::<BTreeSet<_>>();
    if material
        .keys()
        .any(|input_key| !declared.contains(input_key.as_str()))
    {
        return Err(corrupt_input_material(key));
    }

    let mut variables = BTreeMap::new();
    let mut secrets = BTreeMap::new();
    for input in &manifest.inputs {
        let value = material
            .get(&input.key)
            .and_then(|value| trimmed_non_empty_value(value))
            .or_else(|| {
                (input.kind == ManifestInputKind::Variable && !input.required)
                    .then(|| input.default_value.clone())
            });
        let Some(value) = value else {
            if input.required {
                return Err(AppError::FailedPrecondition(format!(
                    "missing identity spec input '{}' for identity spec '{}'",
                    input.key, manifest.name
                )));
            }
            continue;
        };
        match input.kind {
            ManifestInputKind::Variable => variables.insert(input.key.clone(), value),
            ManifestInputKind::Secret => secrets.insert(input.key.clone(), value),
        };
    }
    Ok(ResolvedIdentitySpecInputs { variables, secrets })
}

fn trimmed_non_empty_value(value: &str) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn corrupt_input_material(key: &IdentitySpecKey) -> AppError {
    AppError::Database(format!(
        "identity spec '{}:{}' has invalid encrypted input material",
        scope_label(key.scope()),
        key.name()
    ))
}

async fn require_scope_workspace(
    tx: &mut CoralTx<'_>,
    scope: &IdentitySpecScope,
) -> Result<(), AppError> {
    if let IdentitySpecScope::Workspace(workspace) = scope {
        require_workspace(tx, workspace).await?;
    }
    Ok(())
}

async fn require_workspace(
    tx: &mut CoralTx<'_>,
    workspace: &WorkspaceName,
) -> Result<(), AppError> {
    if tx.workspaces().get(workspace.as_str()).await?.is_none() {
        return Err(AppError::WorkspaceNotFound(workspace.to_string()));
    }
    Ok(())
}

fn convert_records(
    records: Vec<IdentitySpecRecord>,
) -> Result<Vec<InstalledIdentitySpec>, AppError> {
    records.into_iter().map(record_to_installed).collect()
}

fn record_to_installed(record: IdentitySpecRecord) -> Result<InstalledIdentitySpec, AppError> {
    let manifest = parse_identity_manifest_yaml(&record.manifest_yaml).map_err(|error| {
        corrupt_record(&record.key, &format!("manifest cannot be parsed: {error}"))
    })?;
    for (field, stored, parsed) in [
        ("name", record.key.name(), manifest.name.as_str()),
        (
            "version",
            record.version.as_str(),
            manifest.version.as_str(),
        ),
        (
            "description",
            record.description.as_str(),
            manifest.description.as_str(),
        ),
        ("issuer", record.issuer.as_str(), manifest.issuer.as_str()),
        (
            "identity_type",
            record.identity_type.as_str(),
            manifest.identity_type.label(),
        ),
    ] {
        if stored != parsed {
            return Err(corrupt_record(
                &record.key,
                &format!("stored {field} does not match manifest"),
            ));
        }
    }
    Ok(InstalledIdentitySpec {
        key: record.key,
        manifest_yaml: record.manifest_yaml,
        manifest,
    })
}

fn corrupt_record(key: &IdentitySpecKey, detail: &str) -> AppError {
    AppError::Database(format!(
        "identity spec '{}:{}' is corrupt: {detail}",
        scope_label(key.scope()),
        key.name()
    ))
}

fn spec_not_found(key: &IdentitySpecKey) -> AppError {
    AppError::IdentitySpecNotFound {
        name: key.name().to_string(),
        scope: scope_label(key.scope()),
    }
}

fn scope_label(scope: &IdentitySpecScope) -> String {
    match scope {
        IdentitySpecScope::Global => "global".to_string(),
        IdentitySpecScope::Workspace(workspace) => format!("workspace:{workspace}"),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use coral_api::{CORAL_ERROR_DOMAIN, CORAL_ERROR_REASON_IDENTITY_SPEC_NOT_FOUND};
    use coral_spec::parse_identity_manifest_yaml;
    use tempfile::{TempDir, tempdir};
    use tonic::Code;
    use tonic_types::{ErrorDetail, StatusExt as _};

    use super::{
        IdentitySpecInputValue, IdentitySpecManager, InstalledIdentitySpec,
        prepare_identity_spec_input_material, record_to_installed,
        resolve_identity_spec_inputs_for_use, scope_label,
    };
    use crate::bootstrap::{AppError, app_status};
    use crate::credentials::CredentialsError;
    use crate::credentials::encryption::{CredentialEncryptionKey, CredentialKeyProvider};
    use crate::identity::encrypt_identity_spec_document;
    use crate::state::db::{
        CoralDb, CoralTx, DbRepos, IdentitySpecDocumentWrite, IdentitySpecKey, IdentitySpecRecord,
        IdentitySpecScope, IdentitySpecWrite, ResolvedDatabaseConfig,
        set_identity_spec_document_version,
    };
    use crate::workspaces::WorkspaceName;

    macro_rules! assert_workspace_missing {
        ($future:expr) => {
            assert!(matches!(
                $future.await,
                Err(AppError::WorkspaceNotFound(name)) if name == "missing"
            ));
        };
    }

    macro_rules! assert_list {
        ($future:expr, $expected:expr) => {{
            let specs = $future.await.expect("identity spec list");
            assert_eq!(labels(&specs), $expected);
        }};
    }

    struct Fixture {
        _temp: TempDir,
        db: Arc<CoralDb>,
        key_provider: Arc<TestKeyProvider>,
        manager: IdentitySpecManager,
        workspace: WorkspaceName,
    }

    struct TestKeyProvider(Vec<CredentialEncryptionKey>);

    impl CredentialKeyProvider for TestKeyProvider {
        fn active_key(&self) -> Result<CredentialEncryptionKey, CredentialsError> {
            self.0
                .last()
                .cloned()
                .ok_or_else(|| CredentialsError::Crypto("missing test key".to_string()))
        }

        fn key(&self, key_id: &str) -> Result<CredentialEncryptionKey, CredentialsError> {
            self.0
                .iter()
                .find(|key| key.key_id() == key_id)
                .cloned()
                .ok_or_else(|| CredentialsError::Crypto("missing test key".to_string()))
        }
    }

    type RecordDrift = (&'static str, fn(&mut IdentitySpecRecord));

    #[expect(clippy::too_many_lines, reason = "shared backend mutation contract")]
    pub(crate) async fn assert_identity_spec_mutation_contract(db: &Arc<CoralDb>) {
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let workspace = WorkspaceName::parse(&format!("mutation{suffix}")).expect("workspace");
        let mut tx = db.begin().await.expect("begin workspace seed");
        tx.workspaces()
            .ensure(workspace.as_str(), 1)
            .await
            .expect("ensure workspace");
        tx.commit().await.expect("commit workspace seed");
        let old_key = CredentialEncryptionKey::from_static_bytes_for_test([51; 32]);
        let manager =
            IdentitySpecManager::new(db.clone(), Arc::new(TestKeyProvider(vec![old_key.clone()])));
        let name = format!("mutation_{suffix}");
        let global_key = IdentitySpecKey::global(&name).expect("global key");
        let workspace_key =
            IdentitySpecKey::workspace(workspace.clone(), &name).expect("workspace key");
        assert!(
            !mutate(
                &manager,
                IdentitySpecScope::global(),
                &name,
                "global",
                &[("CLIENT_SECRET", "global-secret")]
            )
            .await
            .1
        );
        assert!(
            !mutate(
                &manager,
                IdentitySpecScope::workspace(workspace.clone()),
                &name,
                "workspace",
                &[("TENANT", "tenant"), ("CLIENT_SECRET", "workspace")]
            )
            .await
            .1
        );
        assert_exact(&manager, &workspace_key, "tenant", "workspace").await;
        assert!(
            mutate(
                &manager,
                IdentitySpecScope::global(),
                &name,
                "replacement",
                &[("CLIENT_SECRET", "  ")]
            )
            .await
            .1
        );
        assert_exact(&manager, &global_key, "tenant-replacement", "global-secret").await;
        let new_key = CredentialEncryptionKey::from_static_bytes_for_test([52; 32]);
        let new_key_id = new_key.key_id().to_string();
        let manager = IdentitySpecManager::new(
            db.clone(),
            Arc::new(TestKeyProvider(vec![old_key, new_key])),
        );
        mutate(&manager, IdentitySpecScope::global(), &name, "rotated", &[]).await;
        assert_exact(&manager, &global_key, "tenant-rotated", "global-secret").await;
        assert_eq!(
            manager
                .load_mutation_snapshot(&global_key)
                .await
                .expect("active-key snapshot")
                .document
                .expect("encrypted document")
                .key_id,
            new_key_id
        );
        let mut tx = db.begin().await.expect("begin max-version seed");
        set_identity_spec_document_version(&mut tx, &global_key, i64::MAX).await;
        tx.commit().await.expect("commit max document version");
        let before = manager
            .load_mutation_snapshot(&global_key)
            .await
            .expect("before overflow");
        assert!(matches!(
            manager
                .add_or_replace_exact(
                    IdentitySpecScope::global(),
                    &oauth_manifest(&name, "overflow"),
                    vec![IdentitySpecInputValue::new("CLIENT_SECRET", "overflow")]
                )
                .await,
            Err(AppError::FailedPrecondition(_))
        ));
        assert!(manager.load_mutation_snapshot(&global_key).await.unwrap() == before);

        let global_before_delete = manager.load_mutation_snapshot(&global_key).await.unwrap();
        manager
            .delete_exact(&workspace_key)
            .await
            .expect("delete workspace spec");
        let deleted = manager
            .load_mutation_snapshot(&workspace_key)
            .await
            .unwrap();
        assert!(deleted.record.is_none() && deleted.document.is_none());
        assert!(manager.load_mutation_snapshot(&global_key).await.unwrap() == global_before_delete);
        assert!(matches!(
            manager.delete_exact(&workspace_key).await,
            Err(AppError::IdentitySpecNotFound { .. })
        ));

        assert_disjoint_replacements_converge(db, &manager, &suffix).await;
        manager
            .add_or_replace_exact(
                IdentitySpecScope::global(),
                &manifest(&name, "empty"),
                vec![],
            )
            .await
            .expect("empty material removes document");
        assert!(
            manager
                .load_mutation_snapshot(&global_key)
                .await
                .unwrap()
                .document
                .is_none()
        );
    }

    async fn mutate(
        manager: &IdentitySpecManager,
        scope: IdentitySpecScope,
        name: &str,
        label: &str,
        values: &[(&str, &str)],
    ) -> (InstalledIdentitySpec, bool) {
        manager
            .add_or_replace_exact(
                scope,
                &oauth_manifest(name, label),
                values
                    .iter()
                    .map(|(key, value)| IdentitySpecInputValue::new(*key, *value))
                    .collect(),
            )
            .await
            .expect("identity spec mutation")
    }

    async fn assert_exact(
        manager: &IdentitySpecManager,
        key: &IdentitySpecKey,
        tenant: &str,
        secret: &str,
    ) {
        let resolved = manager.get_exact_for_use(key).await.expect("resolved spec");
        assert_eq!(map_value(resolved.inputs.variables(), "TENANT"), tenant);
        assert_eq!(
            map_value(resolved.inputs.secrets(), "CLIENT_SECRET"),
            secret
        );
    }

    async fn assert_disjoint_replacements_converge(
        db: &Arc<CoralDb>,
        manager: &IdentitySpecManager,
        suffix: &str,
    ) {
        let name = format!("concurrent_{suffix}");
        let key = IdentitySpecKey::global(&name).expect("concurrent key");
        mutate(
            manager,
            IdentitySpecScope::global(),
            &name,
            "race",
            &[("TENANT", "before"), ("CLIENT_SECRET", "before")],
        )
        .await;
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let left = IdentitySpecManager::new(db.clone(), manager.key_provider.clone())
            .with_mutation_barrier(barrier.clone());
        let right = IdentitySpecManager::new(db.clone(), manager.key_provider.clone())
            .with_mutation_barrier(barrier);
        let (left, right) = tokio::join!(
            mutate(
                &left,
                IdentitySpecScope::global(),
                &name,
                "race",
                &[("TENANT", "left")]
            ),
            mutate(
                &right,
                IdentitySpecScope::global(),
                &name,
                "race",
                &[("CLIENT_SECRET", "right")]
            )
        );
        assert!(left.1 && right.1);
        assert_exact(manager, &key, "left", "right").await;
    }

    #[tokio::test]
    async fn reads_exact_combined_and_effective_scopes() {
        let fixture = fixture().await;
        let workspace = &fixture.workspace;
        let workspace_scope = IdentitySpecScope::workspace(workspace.clone());

        let global_alpha = fixture
            .manager
            .get_global("alpha")
            .await
            .expect("global alpha");
        assert_eq!(global_alpha.manifest.version, "global_alpha");
        assert_eq!(scope_label(global_alpha.key.scope()), "global");

        let exact_workspace_beta = fixture
            .manager
            .get_exact(&IdentitySpecKey::workspace(workspace.clone(), "beta").expect("key"))
            .await
            .expect_err("exact workspace read must not fall back");
        assert!(matches!(
            exact_workspace_beta,
            AppError::IdentitySpecNotFound { name, scope }
                if name == "beta" && scope == format!("workspace:{workspace}")
        ));

        let fallback = fixture
            .manager
            .resolve_for_workspace(workspace, "beta")
            .await
            .expect("global fallback");
        assert_eq!(scope_label(fallback.key.scope()), "global");
        let shadow = fixture
            .manager
            .resolve_for_workspace(workspace, "alpha")
            .await
            .expect("workspace shadow");
        assert_eq!(shadow.manifest.version, "workspace_alpha");
        assert_eq!(scope_label(shadow.key.scope()), "workspace:work");

        assert_list!(
            fixture.manager.list_exact(&IdentitySpecScope::global()),
            ["global:alpha", "global:beta"]
        );
        assert_list!(
            fixture.manager.list_exact(&workspace_scope),
            ["workspace:work:alpha", "workspace:work:gamma"]
        );
        assert_list!(
            fixture.manager.list_workspace_with_global(workspace),
            [
                "global:alpha",
                "global:beta",
                "workspace:work:alpha",
                "workspace:work:gamma",
            ]
        );
        assert_list!(
            fixture.manager.list_resolved_for_workspace(workspace),
            [
                "workspace:work:alpha",
                "global:beta",
                "workspace:work:gamma",
            ]
        );

        assert!(matches!(
            fixture.manager.get_global("gamma").await,
            Err(AppError::IdentitySpecNotFound { scope, .. }) if scope == "global"
        ));
        let missing = fixture
            .manager
            .get_global("missing")
            .await
            .expect_err("missing global spec");
        let status = app_status(missing);
        assert_eq!(status.code(), Code::NotFound);
        let info = status
            .get_error_details_vec()
            .into_iter()
            .find_map(|detail| match detail {
                ErrorDetail::ErrorInfo(info) => Some(info),
                _ => None,
            })
            .expect("typed ErrorInfo");
        assert_eq!(info.reason, CORAL_ERROR_REASON_IDENTITY_SPEC_NOT_FOUND);
        assert_eq!(info.domain, CORAL_ERROR_DOMAIN);
        assert!(info.metadata.is_empty());
        assert!(matches!(
            fixture.manager.get_global("bad-name").await,
            Err(AppError::InvalidInput(_))
        ));
        assert_identity_spec_mutation_contract(&fixture.db).await;
    }

    #[tokio::test]
    async fn workspace_checks_and_corruption_fail_closed() {
        let fixture = fixture().await;
        let missing = WorkspaceName::parse("missing").expect("missing workspace");
        let missing_scope = IdentitySpecScope::workspace(missing.clone());
        let missing_key = IdentitySpecKey::workspace(missing.clone(), "alpha").expect("key");
        assert_workspace_missing!(fixture.manager.add_or_replace_exact(
            missing_scope.clone(),
            &manifest("alpha", "missing"),
            vec![],
        ));
        assert_workspace_missing!(fixture.manager.delete_exact(&missing_key));
        assert_workspace_missing!(fixture.manager.get_exact(&missing_key));
        assert_workspace_missing!(fixture.manager.get_exact_for_use(&missing_key));
        assert_workspace_missing!(fixture.manager.list_exact(&missing_scope));
        assert_workspace_missing!(fixture.manager.list_workspace_with_global(&missing));
        assert_workspace_missing!(fixture.manager.resolve_for_workspace(&missing, "alpha"));
        assert_workspace_missing!(
            fixture
                .manager
                .resolve_for_workspace_for_use(&missing, "alpha")
        );
        assert_workspace_missing!(fixture.manager.list_resolved_for_workspace(&missing));

        seed_corrupt_records(&fixture).await;

        assert!(matches!(
            fixture
                .manager
                .resolve_for_workspace(&fixture.workspace, "corrupt")
                .await,
            Err(AppError::Database(_))
        ));
        assert!(matches!(
            fixture
                .manager
                .list_resolved_for_workspace(&fixture.workspace)
                .await,
            Err(AppError::Database(_))
        ));
        assert_metadata_drifts_fail();
    }

    #[tokio::test]
    async fn decrypts_material_for_the_actual_resolved_scope_and_fails_closed() {
        let fixture = fixture().await;
        let global = IdentitySpecKey::global("oauth").expect("global key");
        let workspace =
            IdentitySpecKey::workspace(fixture.workspace.clone(), "oauth").expect("workspace key");
        let fallback = IdentitySpecKey::global("fallback").expect("fallback key");
        let missing = IdentitySpecKey::global("missing_inputs").expect("missing key");
        let mut tx = fixture.db.begin().await.expect("begin material seed");
        for (key, label, secret) in [
            (&global, "global", Some("global-secret")),
            (&workspace, "workspace", Some("workspace-secret")),
            (&fallback, "fallback", Some("fallback-secret")),
            (&missing, "missing", None),
        ] {
            seed_oauth(&mut tx, key, label, secret, fixture.key_provider.as_ref()).await;
        }
        tx.commit().await.expect("commit material seed");

        let global = fixture
            .manager
            .get_global_for_use("oauth")
            .await
            .expect("global");
        let shadow = fixture
            .manager
            .resolve_for_workspace_for_use(&fixture.workspace, "oauth")
            .await
            .expect("workspace shadow");
        let fallback = fixture
            .manager
            .resolve_for_workspace_for_use(&fixture.workspace, "fallback")
            .await
            .expect("global fallback");
        assert_resolved(&global, "global", "tenant-global", "global-secret");
        assert_resolved(
            &shadow,
            "workspace:work",
            "tenant-workspace",
            "workspace-secret",
        );
        assert_resolved(&fallback, "global", "tenant-fallback", "fallback-secret");
        let rendered = format!("{global:?}{shadow:?}{fallback:?}");
        for secret in ["global-secret", "workspace-secret", "fallback-secret"] {
            assert!(!rendered.contains(secret));
        }
        assert!(matches!(
            fixture.manager.get_exact_for_use(&missing).await,
            Err(AppError::FailedPrecondition(detail)) if detail.contains("CLIENT_SECRET")
        ));
    }

    #[test]
    fn prepares_trimmed_override_material_without_leaking_or_persisting_defaults() {
        let key = IdentitySpecKey::global("merge").expect("merge key");
        let manifest = parse_identity_manifest_yaml(&oauth_manifest("merge", "current"))
            .expect("merge manifest");
        let mut previous_manifest = manifest.clone();
        let previous_tenant = previous_manifest
            .inputs
            .iter_mut()
            .find(|input| input.key == "TENANT")
            .expect("tenant input");
        previous_tenant.kind = coral_spec::ManifestInputKind::Secret;
        previous_tenant.default_value.clear();
        let previous = BTreeMap::from([
            ("TENANT".to_string(), "old-secret".to_string()),
            ("CLIENT_SECRET".to_string(), " old-client ".to_string()),
        ]);
        let prepared = prepare_identity_spec_input_material(
            &key,
            &manifest,
            Some(&previous_manifest),
            &previous,
            &[IdentitySpecInputValue::new("CLIENT_SECRET", " \t ")],
        )
        .expect("blank supplied value falls back");
        assert_eq!(map_value(&prepared.values, "CLIENT_SECRET"), "old-client");
        assert_eq!(prepared.values.len(), 1);
        assert!(!format!("{prepared:?}").contains("old-client"));

        let replaced = prepare_identity_spec_input_material(
            &key,
            &manifest,
            Some(&manifest),
            &previous,
            &[IdentitySpecInputValue::new(
                " CLIENT_SECRET ",
                " new-client ",
            )],
        )
        .expect("normalized replacement");
        assert_eq!(map_value(&replaced.values, "CLIENT_SECRET"), "new-client");
        for invalid in [
            vec![IdentitySpecInputValue::new("UNKNOWN", "value")],
            vec![
                IdentitySpecInputValue::new("CLIENT_SECRET", "one"),
                IdentitySpecInputValue::new(" CLIENT_SECRET ", "two"),
            ],
        ] {
            let error = prepare_identity_spec_input_material(
                &key,
                &manifest,
                Some(&manifest),
                &BTreeMap::new(),
                &invalid,
            )
            .expect_err("invalid caller material");
            assert!(matches!(error, AppError::InvalidInput(_)));
        }
        assert!(matches!(
            prepare_identity_spec_input_material(&key, &manifest, None, &BTreeMap::new(), &[]),
            Err(AppError::InvalidInput(_))
        ));

        let mut required_default = manifest.clone();
        let tenant = required_default
            .inputs
            .iter_mut()
            .find(|input| input.key == "TENANT")
            .expect("tenant");
        tenant.required = true;
        assert!(matches!(
            resolve_identity_spec_inputs_for_use(&key, &required_default, &prepared.values),
            Err(AppError::FailedPrecondition(_))
        ));
        let exhausted = super::mutation_retry_exhausted();
        assert!(matches!(&exhausted, AppError::RetryableTransactionConflict));
        assert_eq!(app_status(exhausted).code(), Code::Unavailable);
    }

    fn assert_resolved(
        resolved: &super::ResolvedIdentitySpec,
        scope: &str,
        tenant: &str,
        secret: &str,
    ) {
        assert_eq!(scope_label(resolved.spec.key.scope()), scope);
        assert_eq!(map_value(resolved.inputs.variables(), "TENANT"), tenant);
        assert_eq!(
            map_value(resolved.inputs.secrets(), "CLIENT_SECRET"),
            secret
        );
    }

    fn map_value<'a>(values: &'a BTreeMap<String, String>, key: &str) -> &'a str {
        values.get(key).map(String::as_str).expect("input value")
    }

    async fn seed_corrupt_records(fixture: &Fixture) {
        let mut tx = fixture.db.begin().await.expect("begin corruption seed");
        seed_valid(
            &mut tx,
            &IdentitySpecKey::global("corrupt").expect("key"),
            "fallback",
        )
        .await;
        seed_write(
            &mut tx,
            &IdentitySpecKey::workspace(fixture.workspace.clone(), "corrupt").expect("corrupt key"),
            IdentitySpecWrite::new(
                "invalid",
                "invalid",
                "issuer_invalid",
                "fixed_token",
                "not: [valid yaml",
            )
            .expect("repository-shaped corrupt spec"),
        )
        .await;
        tx.commit().await.expect("commit corruption seed");
    }

    fn assert_metadata_drifts_fail() {
        let manifest_yaml = manifest("drift", "canonical");
        let manifest = parse_identity_manifest_yaml(&manifest_yaml).expect("valid manifest");
        let record = IdentitySpecRecord {
            key: IdentitySpecKey::global("drift").expect("key"),
            version: manifest.version,
            description: manifest.description,
            issuer: manifest.issuer,
            identity_type: manifest.identity_type.label().to_string(),
            manifest_yaml,
            created_at_unix_nanos: 1,
            updated_at_unix_nanos: 1,
        };
        let drifts: [RecordDrift; 5] = [
            ("name", |row| {
                row.key = IdentitySpecKey::global("other").expect("key");
            }),
            ("version", |row| row.version.push_str("_drift")),
            ("description", |row| row.description.push_str("_drift")),
            ("issuer", |row| row.issuer.push_str("_drift")),
            ("identity_type", |row| row.identity_type.push_str("_drift")),
        ];
        for (field, drift) in drifts {
            let mut drifted = record.clone();
            drift(&mut drifted);
            let error = record_to_installed(drifted).expect_err("metadata drift must fail");
            assert!(
                matches!(&error, AppError::Database(detail) if detail.contains(field)),
                "unexpected {field} drift error: {error}"
            );
        }
    }

    async fn fixture() -> Fixture {
        let temp = tempdir().expect("temp dir");
        let db = Arc::new(
            CoralDb::open(ResolvedDatabaseConfig::Sqlite {
                path: temp.path().join("coral.sqlite"),
            })
            .await
            .expect("open sqlite"),
        );
        db.migrate().await.expect("migrate sqlite");
        let key_provider = Arc::new(TestKeyProvider(vec![
            CredentialEncryptionKey::from_static_bytes_for_test([43; 32]),
        ]));
        let workspace = WorkspaceName::parse("work").expect("workspace");
        let mut tx = db.begin().await.expect("begin fixture seed");
        tx.workspaces()
            .ensure(workspace.as_str(), 1)
            .await
            .expect("seed workspace");
        for (key, label) in [
            (
                IdentitySpecKey::global("alpha").expect("key"),
                "global_alpha",
            ),
            (IdentitySpecKey::global("beta").expect("key"), "global_beta"),
            (
                IdentitySpecKey::workspace(workspace.clone(), "alpha").expect("key"),
                "workspace_alpha",
            ),
            (
                IdentitySpecKey::workspace(workspace.clone(), "gamma").expect("key"),
                "workspace_gamma",
            ),
        ] {
            seed_valid(&mut tx, &key, label).await;
        }
        tx.commit().await.expect("commit fixture seed");
        Fixture {
            _temp: temp,
            manager: IdentitySpecManager::new(Arc::clone(&db), key_provider.clone()),
            db,
            key_provider,
            workspace,
        }
    }

    async fn seed_oauth(
        tx: &mut CoralTx<'_>,
        key: &IdentitySpecKey,
        label: &str,
        secret: Option<&str>,
        key_provider: &dyn CredentialKeyProvider,
    ) {
        let yaml = oauth_manifest(key.name(), label);
        let parsed = parse_identity_manifest_yaml(&yaml).expect("valid OAuth identity manifest");
        seed_write(
            tx,
            key,
            IdentitySpecWrite::new(
                &parsed.version,
                &parsed.description,
                &parsed.issuer,
                parsed.identity_type.label(),
                yaml,
            )
            .expect("valid OAuth identity write"),
        )
        .await;
        let Some(secret) = secret else { return };
        let values = BTreeMap::from([("CLIENT_SECRET".to_string(), secret.to_string())]);
        let (scope_kind, scope_id, name) = key.document_aad_parts();
        let encrypted =
            encrypt_identity_spec_document(scope_kind, scope_id, name, &values, key_provider)
                .expect("encrypt identity spec material");
        let write = IdentitySpecDocumentWrite::new(
            encrypted.ciphertext,
            encrypted.nonce,
            encrypted.wrapped_dek,
            encrypted.wrapped_dek_nonce,
            encrypted.key_id,
            encrypted.algorithm,
            encrypted.aad_version,
        )
        .expect("valid encrypted document write");
        tx.identity_spec_documents()
            .upsert(key, &write, 3)
            .await
            .expect("seed encrypted identity spec material");
    }

    async fn seed_valid(tx: &mut CoralTx<'_>, key: &IdentitySpecKey, label: &str) {
        let yaml = manifest(key.name(), label);
        let parsed = parse_identity_manifest_yaml(&yaml).expect("valid identity manifest");
        let write = IdentitySpecWrite::new(
            &parsed.version,
            &parsed.description,
            &parsed.issuer,
            parsed.identity_type.label(),
            yaml,
        )
        .expect("valid identity write");
        seed_write(tx, key, write).await;
    }

    async fn seed_write(tx: &mut CoralTx<'_>, key: &IdentitySpecKey, write: IdentitySpecWrite) {
        tx.identity_specs()
            .upsert(key, &write, 2)
            .await
            .expect("seed identity spec");
    }

    fn manifest(name: &str, label: &str) -> String {
        format!(
            "kind: identity\nspec_version: 1\nname: {name}\nversion: {label}\ndescription: description {label}\nissuer: issuer_{label}\ntype: fixed_token\n"
        )
    }

    fn oauth_manifest(name: &str, label: &str) -> String {
        format!(
            "kind: identity\nspec_version: 1\nname: {name}\nversion: {label}\ndescription: OAuth {label}\nissuer: issuer_{label}\ntype: oauth\ninputs:\n  TENANT:\n    kind: variable\n    default: tenant-{label}\n  CLIENT_SECRET:\n    kind: secret\n    required: true\noauth:\n  method:\n    flow:\n      type: authorization_code\n      pkce: disabled\n    redirect_uri: http://127.0.0.1:53682/oauth/callback\n    endpoints:\n      authorization_url: https://provider.example.com/authorize\n      token_url: https://provider.example.com/token\n    client:\n      id:\n        input: TENANT\n      secret:\n        input: CLIENT_SECRET\n        transport: basic_auth\n"
        )
    }

    fn labels(specs: &[InstalledIdentitySpec]) -> Vec<String> {
        specs
            .iter()
            .map(|spec| format!("{}:{}", scope_label(spec.key.scope()), spec.key.name()))
            .collect()
    }
}
