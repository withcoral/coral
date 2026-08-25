//! Read and workspace-fallback behavior for installed identity specs.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use coral_spec::{IdentityManifest, parse_identity_manifest_yaml};

use crate::bootstrap::AppError;
use crate::credentials::encryption::CredentialKeyProvider;
use crate::encrypted_document::EncryptedEnvelopeDocument;
use crate::identity::spec_document::{
    decrypt_identity_spec_document, encrypt_identity_spec_document,
};
use crate::identity_specs::inputs::{
    IdentitySpecInputValue, ResolvedIdentitySpecInputs, prepare_identity_spec_input_material,
    resolve_identity_spec_inputs_for_use,
};
#[cfg(test)]
use crate::state::db::IdentitySpecMutationSnapshot;
use crate::state::db::{
    CoralDb, CoralTx, DbError, DbRepos, IdentitySpecDocumentRecord, IdentitySpecId,
    IdentitySpecKey, IdentitySpecRecord, IdentitySpecScope,
};
use crate::workspaces::WorkspaceName;

const MAX_MUTATION_ATTEMPTS: usize = 8;

/// One installed identity spec and the exact scope that supplied it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct InstalledIdentitySpec {
    pub(crate) key: IdentitySpecKey,
    pub(crate) manifest_yaml: String,
    pub(crate) manifest: IdentityManifest,
}

/// One installed identity spec with its setup inputs resolved for use.
pub(crate) struct ResolvedIdentitySpec {
    pub(crate) spec: InstalledIdentitySpec,
    pub(crate) inputs: ResolvedIdentitySpecInputs,
}

impl fmt::Debug for ResolvedIdentitySpec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedIdentitySpec")
            .field("key", &self.spec.key)
            .field("inputs", &self.inputs)
            .finish_non_exhaustive()
    }
}

struct IdentitySpecUseSnapshot {
    record: IdentitySpecRecord,
    document: Option<IdentitySpecDocumentRecord>,
}

/// Database-backed identity-spec read and resolution behavior.
#[derive(Clone)]
pub(crate) struct IdentitySpecManager {
    db: Arc<CoralDb>,
    key_provider: Arc<dyn CredentialKeyProvider>,
    #[cfg(test)]
    mutation_barrier: Option<Arc<tokio::sync::Barrier>>,
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
        let manifest = Arc::new(
            parse_identity_manifest_yaml(manifest_yaml)
                .map_err(|error| AppError::InvalidInput(error.to_string()))?,
        );
        let key = IdentitySpecKey::new(scope, &manifest.name)?;
        let input_values = Arc::new(input_values);
        #[cfg(test)]
        let mut mutation_barrier = self.mutation_barrier.clone();

        for _ in 0..MAX_MUTATION_ATTEMPTS {
            #[cfg(test)]
            let barrier = mutation_barrier.take();
            let prepare_key = key.clone();
            let prepare_manifest = Arc::clone(&manifest);
            let prepare_inputs = Arc::clone(&input_values);
            let key_provider = Arc::clone(&self.key_provider);
            let result = self
                .db
                .identity_spec_state()
                .add_or_replace_exact(
                    &key,
                    manifest.as_ref(),
                    manifest_yaml,
                    move |snapshot| async move {
                        #[cfg(test)]
                        if let Some(barrier) = barrier {
                            barrier.wait().await;
                        }
                        run_blocking_identity_spec_operation(move || {
                            let previous_identity_spec_id =
                                snapshot.record.as_ref().map(|record| record.id.clone());
                            let previous = snapshot.record.map(record_to_installed).transpose()?;
                            let previous_values = match previous_identity_spec_id {
                                Some(identity_spec_id) => decrypt_input_material(
                                    &prepare_key,
                                    &identity_spec_id,
                                    snapshot.document,
                                    key_provider.as_ref(),
                                )?,
                                None => BTreeMap::new(),
                            };
                            let prepared = prepare_identity_spec_input_material(
                                &prepare_key,
                                prepare_manifest.as_ref(),
                                previous.as_ref().map(|spec| &spec.manifest),
                                &previous_values,
                                prepare_inputs.as_slice(),
                            )?;
                            prepare_document_write(
                                &prepare_key,
                                prepared.values(),
                                key_provider.as_ref(),
                            )
                        })
                        .await
                    },
                )
                .await;
            match result {
                Ok((record, replaced)) => {
                    return Ok((record_to_installed(record)?, replaced));
                }
                Err(AppError::RetryableTransactionConflict) => {
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(AppError::RetryableTransactionConflict)
    }

    /// Delete one spec in exactly the selected scope.
    pub(crate) async fn delete_exact(&self, key: &IdentitySpecKey) -> Result<(), AppError> {
        for _ in 0..MAX_MUTATION_ATTEMPTS {
            match self.db.identity_spec_state().delete_exact(key).await {
                Ok(true) => return Ok(()),
                Ok(false) => return Err(spec_not_found(key)),
                Err(AppError::RetryableTransactionConflict) => {
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(AppError::RetryableTransactionConflict)
    }

    /// Fetch one spec in exactly the requested scope, without fallback.
    pub(crate) async fn get_exact(
        &self,
        key: &IdentitySpecKey,
    ) -> Result<InstalledIdentitySpec, AppError> {
        self.read_exact(key).await
    }

    /// Fetch one global spec by name.
    pub(crate) async fn get_global(&self, name: &str) -> Result<InstalledIdentitySpec, AppError> {
        self.read_exact(&IdentitySpecKey::global(name)?).await
    }

    /// Fetch one exact spec and resolve its encrypted setup inputs for use.
    pub(crate) async fn get_exact_for_use(
        &self,
        key: &IdentitySpecKey,
    ) -> Result<ResolvedIdentitySpec, AppError> {
        let mut tx = self.db.begin_read_snapshot().await?;
        require_scope_workspace(&mut tx, key.scope()).await?;
        let snapshot = read_use_snapshot(&mut tx, key).await?;
        tx.commit().await?;
        let snapshot = snapshot.ok_or_else(|| spec_not_found(key))?;
        self.resolve_snapshot_for_use(snapshot).await
    }

    /// Fetch one global spec and resolve its encrypted setup inputs for use.
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
        let records = tx.identity_specs().list(scope).await?;
        tx.commit().await?;
        convert_records(records)
    }

    /// List global specs followed by workspace specs, preserving shadowed entries.
    pub(crate) async fn list_workspace_with_global(
        &self,
        workspace: &WorkspaceName,
    ) -> Result<Vec<InstalledIdentitySpec>, AppError> {
        let mut tx = self.db.begin_read_snapshot().await?;
        require_workspace(&mut tx, workspace).await?;
        let mut records = tx
            .identity_specs()
            .list(&IdentitySpecScope::global())
            .await?;
        records.extend(
            tx.identity_specs()
                .list(&IdentitySpecScope::workspace(workspace.clone()))
                .await?,
        );
        tx.commit().await?;
        convert_records(records)
    }

    /// Resolve one workspace spec, preferring workspace scope over global scope.
    pub(crate) async fn resolve_for_workspace(
        &self,
        workspace: &WorkspaceName,
        name: &str,
    ) -> Result<InstalledIdentitySpec, AppError> {
        let requested = IdentitySpecKey::workspace(workspace.clone(), name)?;
        let mut tx = self.db.begin_read_snapshot().await?;
        require_workspace(&mut tx, workspace).await?;
        let record = match tx.identity_specs().get(&requested).await? {
            some @ Some(_) => some,
            None => {
                tx.identity_specs()
                    .get(&IdentitySpecKey::global(name)?)
                    .await?
            }
        };
        tx.commit().await?;
        convert_optional(record, &requested)
    }

    /// Resolve a workspace spec with global fallback and decrypt the winning spec's inputs.
    pub(crate) async fn resolve_for_workspace_for_use(
        &self,
        workspace: &WorkspaceName,
        name: &str,
    ) -> Result<ResolvedIdentitySpec, AppError> {
        let requested = IdentitySpecKey::workspace(workspace.clone(), name)?;
        let mut tx = self.db.begin_read_snapshot().await?;
        require_workspace(&mut tx, workspace).await?;
        let snapshot = match read_use_snapshot(&mut tx, &requested).await? {
            some @ Some(_) => some,
            None => read_use_snapshot(&mut tx, &IdentitySpecKey::global(name)?).await?,
        };
        tx.commit().await?;
        let snapshot = snapshot.ok_or_else(|| spec_not_found(&requested))?;
        self.resolve_snapshot_for_use(snapshot).await
    }

    async fn read_exact(&self, key: &IdentitySpecKey) -> Result<InstalledIdentitySpec, AppError> {
        let mut tx = self.db.begin_read_snapshot().await?;
        require_scope_workspace(&mut tx, key.scope()).await?;
        let record = tx.identity_specs().get(key).await?;
        tx.commit().await?;
        convert_optional(record, key)
    }

    #[cfg(test)]
    async fn load_mutation_snapshot(
        &self,
        key: &IdentitySpecKey,
    ) -> Result<IdentitySpecMutationSnapshot, AppError> {
        self.db.identity_spec_state().load_exact(key).await
    }

    async fn resolve_snapshot_for_use(
        &self,
        snapshot: IdentitySpecUseSnapshot,
    ) -> Result<ResolvedIdentitySpec, AppError> {
        let key_provider = Arc::clone(&self.key_provider);
        run_blocking_identity_spec_operation(move || {
            let IdentitySpecUseSnapshot { record, document } = snapshot;
            let identity_spec_id = record.id.clone();
            let spec = record_to_installed(record)?;
            let material = decrypt_input_material(
                &spec.key,
                &identity_spec_id,
                document,
                key_provider.as_ref(),
            )?;
            let inputs =
                resolve_identity_spec_inputs_for_use(&spec.key, &spec.manifest, &material)?;
            Ok(ResolvedIdentitySpec { spec, inputs })
        })
        .await
    }
}

async fn read_use_snapshot(
    tx: &mut CoralTx<'_>,
    key: &IdentitySpecKey,
) -> Result<Option<IdentitySpecUseSnapshot>, DbError> {
    let Some(record) = tx.identity_specs().get(key).await? else {
        return Ok(None);
    };
    let document = tx.identity_spec_documents().get(&record.id).await?;
    Ok(Some(IdentitySpecUseSnapshot { record, document }))
}

async fn run_blocking_identity_spec_operation<T, F>(operation: F) -> Result<T, AppError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, AppError> + Send + 'static,
{
    let span = tracing::Span::current();
    tokio::task::spawn_blocking(move || span.in_scope(operation)).await?
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

fn convert_optional(
    record: Option<IdentitySpecRecord>,
    requested: &IdentitySpecKey,
) -> Result<InstalledIdentitySpec, AppError> {
    record
        .ok_or_else(|| spec_not_found(requested))
        .and_then(|record| record_to_installed(record).map_err(Into::into))
}

fn convert_records(
    records: Vec<IdentitySpecRecord>,
) -> Result<Vec<InstalledIdentitySpec>, AppError> {
    records
        .into_iter()
        .map(|record| record_to_installed(record).map_err(Into::into))
        .collect()
}

fn decrypt_input_material(
    key: &IdentitySpecKey,
    identity_spec_id: &IdentitySpecId,
    document: Option<IdentitySpecDocumentRecord>,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<BTreeMap<String, String>, AppError> {
    let Some(document) = document else {
        return Ok(BTreeMap::new());
    };
    let IdentitySpecDocumentRecord {
        identity_spec_id: document_identity_spec_id,
        envelope,
        ..
    } = document;
    if document_identity_spec_id != *identity_spec_id {
        return Err(corrupt_record(
            key,
            "encrypted setup document belongs to a different identity spec",
        )
        .into());
    }
    // Resolve the stored key first so an unavailable key provider stays a credential
    // error; everything the envelope layer rejects afterward is stored-material fault.
    let kek = key_provider
        .key(&envelope.key_id)
        .map_err(AppError::Credentials)?;
    decrypt_identity_spec_document(key, &envelope, &kek).map_err(|_error| {
        AppError::from(corrupt_record(
            key,
            "encrypted setup document failed authentication or decoding",
        ))
    })
}

fn prepare_document_write(
    key: &IdentitySpecKey,
    values: &BTreeMap<String, String>,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<Option<EncryptedEnvelopeDocument>, AppError> {
    if values.is_empty() {
        return Ok(None);
    }
    encrypt_identity_spec_document(key, values, key_provider)
        .map(Some)
        .map_err(Into::into)
}

pub(crate) fn record_to_installed(
    record: IdentitySpecRecord,
) -> Result<InstalledIdentitySpec, DbError> {
    let manifest = parse_identity_manifest_yaml(&record.manifest_yaml).map_err(|error| {
        corrupt_record(&record.key, &format!("manifest cannot be parsed: {error}"))
    })?;
    require_match(&record.key, "name", record.key.name(), &manifest.name)?;
    require_match(&record.key, "version", &record.version, &manifest.version)?;
    require_match(
        &record.key,
        "description",
        &record.description,
        &manifest.description,
    )?;
    require_match(&record.key, "issuer", &record.issuer, &manifest.issuer)?;
    Ok(InstalledIdentitySpec {
        key: record.key,
        manifest_yaml: record.manifest_yaml,
        manifest,
    })
}

fn require_match(
    key: &IdentitySpecKey,
    field: &str,
    stored: &str,
    parsed: &str,
) -> Result<(), DbError> {
    (stored == parsed)
        .then_some(())
        .ok_or_else(|| corrupt_record(key, &format!("stored {field} does not match manifest")))
}

fn corrupt_record(key: &IdentitySpecKey, detail: &str) -> DbError {
    DbError::CorruptData(format!(
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread::{self, ThreadId};

    use tempfile::{TempDir, tempdir};

    use super::{IdentitySpecManager, record_to_installed, scope_label};
    use crate::bootstrap::AppError;
    use crate::credentials::CredentialsError;
    use crate::credentials::encryption::{CredentialEncryptionKey, CredentialKeyProvider};
    use crate::encrypted_document::EncryptedEnvelopeDocument;
    use crate::identities::manager::IdentityManager;
    use crate::identities::model::{IdentityName, IdentityOwner};
    use crate::identity::Principal;
    use crate::identity::spec_document::{
        encrypt_identity_spec_document, seal_identity_spec_plaintext_for_test,
    };
    use crate::identity_specs::inputs::IdentitySpecInputValue;
    use crate::state::db::{
        CoralDb, CoralTx, DbRepos, IdentitySpecId, IdentitySpecKey, IdentitySpecRecord,
        IdentitySpecScope, ResolvedDatabaseConfig, set_identity_spec_document_version,
    };
    use crate::workspaces::WorkspaceName;

    struct TestKeyProvider {
        active_key: CredentialEncryptionKey,
        decryption_keys: Vec<CredentialEncryptionKey>,
        blocking_thread_check: Option<ThreadId>,
    }

    impl TestKeyProvider {
        fn new(active_key: CredentialEncryptionKey) -> Self {
            Self {
                active_key,
                decryption_keys: Vec::new(),
                blocking_thread_check: None,
            }
        }

        fn requiring_blocking_access(
            active_key: CredentialEncryptionKey,
            decryption_keys: impl IntoIterator<Item = CredentialEncryptionKey>,
        ) -> Self {
            Self {
                active_key,
                decryption_keys: decryption_keys.into_iter().collect(),
                blocking_thread_check: Some(thread::current().id()),
            }
        }

        fn require_blocking_thread(&self) {
            if let Some(runtime_thread) = self.blocking_thread_check {
                assert_ne!(
                    thread::current().id(),
                    runtime_thread,
                    "identity spec key access ran on the async runtime"
                );
            }
        }
    }

    impl CredentialKeyProvider for TestKeyProvider {
        fn active_key(&self) -> Result<CredentialEncryptionKey, CredentialsError> {
            self.require_blocking_thread();
            Ok(self.active_key.clone())
        }

        fn key(&self, key_id: &str) -> Result<CredentialEncryptionKey, CredentialsError> {
            self.require_blocking_thread();
            std::iter::once(&self.active_key)
                .chain(&self.decryption_keys)
                .find(|key| key.key_id() == key_id)
                .cloned()
                .ok_or_else(|| CredentialsError::Crypto("missing test key".to_string()))
        }
    }

    struct ReadKeyProvider {
        key: CredentialEncryptionKey,
        key_calls: AtomicUsize,
        active_key_calls: AtomicUsize,
        runtime_thread: ThreadId,
    }

    impl CredentialKeyProvider for ReadKeyProvider {
        fn active_key(&self) -> Result<CredentialEncryptionKey, CredentialsError> {
            self.active_key_calls.fetch_add(1, Ordering::SeqCst);
            Err(CredentialsError::Crypto("active key read".into()))
        }

        fn key(&self, key_id: &str) -> Result<CredentialEncryptionKey, CredentialsError> {
            if key_id != self.key.key_id() {
                return Err(CredentialsError::Crypto("unexpected test key".into()));
            }
            if thread::current().id() == self.runtime_thread {
                return Err(CredentialsError::Crypto("key lookup ran on runtime".into()));
            }
            self.key_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.key.clone())
        }
    }

    struct UnavailableKeyProvider;

    impl CredentialKeyProvider for UnavailableKeyProvider {
        fn active_key(&self) -> Result<CredentialEncryptionKey, CredentialsError> {
            Err(CredentialsError::Crypto("active key read".into()))
        }

        fn key(&self, _key_id: &str) -> Result<CredentialEncryptionKey, CredentialsError> {
            Err(CredentialsError::Unavailable("unavailable".into()))
        }
    }

    #[expect(clippy::too_many_lines, reason = "shared backend mutation contract")]
    pub(crate) async fn assert_identity_spec_mutation_contract(db: &Arc<CoralDb>) {
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let workspace = WorkspaceName::parse(&format!("mutation{suffix}")).unwrap();
        let name = format!("mutation_{suffix}");
        let global_key = IdentitySpecKey::global(&name).unwrap();
        let workspace_key = IdentitySpecKey::workspace(workspace.clone(), &name).unwrap();
        let mut tx = db.begin().await.unwrap();
        tx.workspaces().ensure(workspace.as_str(), 1).await.unwrap();
        tx.commit().await.unwrap();

        let old_key = CredentialEncryptionKey::from_static_bytes_for_test([51; 32]);
        let old_provider = Arc::new(TestKeyProvider::requiring_blocking_access(
            old_key.clone(),
            [],
        ));
        let manager = IdentitySpecManager::new(Arc::clone(db), old_provider);
        let (_, replaced) = mutate(
            &manager,
            IdentitySpecScope::global(),
            &name,
            "v1",
            &[("CLIENT_SECRET", "global-secret")],
        )
        .await;
        assert!(!replaced);
        mutate(
            &manager,
            IdentitySpecScope::workspace(workspace.clone()),
            &name,
            "workspace",
            &[("CLIENT_SECRET", "workspace-secret")],
        )
        .await;
        assert_exact(&manager, &global_key, "tenant-v1", "global-secret").await;
        assert_exact(
            &manager,
            &workspace_key,
            "tenant-workspace",
            "workspace-secret",
        )
        .await;

        let new_key = CredentialEncryptionKey::from_static_bytes_for_test([52; 32]);
        let new_key_id = new_key.key_id().to_string();
        let rotating_provider = Arc::new(TestKeyProvider::requiring_blocking_access(
            new_key,
            [old_key],
        ));
        let manager = IdentitySpecManager::new(Arc::clone(db), rotating_provider.clone());
        assert!(
            mutate(
                &manager,
                IdentitySpecScope::global(),
                &name,
                "v2",
                &[("CLIENT_SECRET", "global-secret")],
            )
            .await
            .1
        );
        assert_exact(&manager, &global_key, "tenant-v2", "global-secret").await;
        assert_eq!(
            manager
                .load_mutation_snapshot(&global_key)
                .await
                .unwrap()
                .document
                .unwrap()
                .envelope
                .key_id,
            new_key_id
        );

        let mut tx = db.begin().await.unwrap();
        let global_id = tx
            .identity_specs()
            .get(&global_key)
            .await
            .unwrap()
            .expect("global spec exists")
            .id;
        set_identity_spec_document_version(&mut tx, &global_id, i64::MAX).await;
        tx.commit().await.unwrap();
        let before = manager.load_mutation_snapshot(&global_key).await.unwrap();
        let overflow_secret = "overflow-secret-must-not-leak";
        let error = manager
            .add_or_replace_exact(
                IdentitySpecScope::global(),
                &oauth_manifest(&name, "overflow"),
                vec![IdentitySpecInputValue::new(
                    "CLIENT_SECRET",
                    overflow_secret,
                )],
            )
            .await
            .unwrap_err();
        assert!(matches!(error, AppError::FailedPrecondition(_)));
        assert!(!format!("{error:?}").contains(overflow_secret));
        assert!(manager.load_mutation_snapshot(&global_key).await.unwrap() == before);

        let global_before_delete = manager.load_mutation_snapshot(&global_key).await.unwrap();
        manager.delete_exact(&workspace_key).await.unwrap();
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

        let missing = WorkspaceName::parse(&format!("missing{suffix}")).unwrap();
        let missing_key = IdentitySpecKey::workspace(missing.clone(), &name).unwrap();
        assert!(matches!(
            manager
                .add_or_replace_exact(
                    IdentitySpecScope::workspace(missing),
                    &manifest(&name, "missing"),
                    vec![],
                )
                .await,
            Err(AppError::WorkspaceNotFound(_))
        ));
        assert!(matches!(
            manager.delete_exact(&missing_key).await,
            Err(AppError::WorkspaceNotFound(_))
        ));

        assert_disjoint_replacements_converge(db, &manager, &suffix).await;
        manager
            .add_or_replace_exact(
                IdentitySpecScope::global(),
                &manifest(&name, "fixed"),
                vec![],
            )
            .await
            .unwrap();
        let empty = manager.load_mutation_snapshot(&global_key).await.unwrap();
        assert!(empty.document.is_none());
        assert_eq!(
            record_to_installed(empty.record.unwrap())
                .unwrap()
                .manifest
                .version,
            "fixed"
        );

        let principal = Principal::local();
        let owner = IdentityOwner::for_user(principal.clone());
        let workspace_owner = IdentityOwner::workspace(workspace.clone());
        let user_identity = format!("user_dependent_{suffix}");
        let fallback_identity = format!("fallback_dependent_{suffix}");
        let exact_identity = format!("exact_dependent_{suffix}");
        let identities = IdentityManager::new(Arc::clone(db), rotating_provider);
        let user_created = identities
            .create_or_replace_user_fixed_token(
                &principal,
                &user_identity,
                &name,
                "user-token".to_string(),
            )
            .await
            .unwrap();
        let fallback_created = identities
            .create_or_replace_workspace_fixed_token(
                &workspace,
                &fallback_identity,
                &name,
                "fallback-token".to_string(),
            )
            .await
            .unwrap();
        assert_eq!(user_created.spec_reference.key(), &global_key);
        assert_eq!(fallback_created.spec_reference.key(), &global_key);
        assert!(
            !manager
                .add_or_replace_exact(
                    IdentitySpecScope::workspace(workspace.clone()),
                    &manifest(&name, "workspace_fixed"),
                    vec![],
                )
                .await
                .unwrap()
                .1
        );
        let exact_created = identities
            .create_or_replace_workspace_fixed_token(
                &workspace,
                &exact_identity,
                &name,
                "exact-token".to_string(),
            )
            .await
            .unwrap();
        assert_eq!(exact_created.spec_reference.key(), &workspace_key);

        let global_error = manager.delete_exact(&global_key).await.unwrap_err();
        assert!(
            matches!(&global_error, AppError::FailedPrecondition(detail)
                if detail.contains("global")
                    && detail.contains("2 stored identity references")),
            "unexpected global delete error: {global_error}"
        );
        let workspace_error = manager.delete_exact(&workspace_key).await.unwrap_err();
        assert!(
            matches!(&workspace_error, AppError::FailedPrecondition(detail)
                if detail.contains(&format!("workspace:{workspace}"))
                    && detail.contains("1 stored identity reference")),
            "unexpected workspace delete error: {workspace_error}"
        );
        manager.get_global(&name).await.unwrap();
        manager.get_exact(&workspace_key).await.unwrap();
        let user_name = IdentityName::parse(&user_identity).unwrap();
        let fallback_name = IdentityName::parse(&fallback_identity).unwrap();
        let exact_name = IdentityName::parse(&exact_identity).unwrap();
        for (identity_owner, identity_name) in [
            (&owner, &user_name),
            (&workspace_owner, &fallback_name),
            (&workspace_owner, &exact_name),
        ] {
            let mut session = db.as_ref();
            assert!(
                session
                    .identities()
                    .get(identity_owner, identity_name)
                    .await
                    .unwrap()
                    .is_some()
            );
            assert!(
                session
                    .identity_documents()
                    .get(identity_owner, identity_name)
                    .await
                    .unwrap()
                    .is_some()
            );
        }
        let mut tx = db.begin().await.unwrap();
        assert!(tx.identities().delete(&owner, &user_name).await.unwrap());
        assert!(
            tx.identities()
                .delete(&workspace_owner, &fallback_name)
                .await
                .unwrap()
        );
        tx.commit().await.unwrap();
        for (identity_owner, identity_name) in
            [(&owner, &user_name), (&workspace_owner, &fallback_name)]
        {
            let mut session = db.as_ref();
            assert!(
                session
                    .identity_documents()
                    .get(identity_owner, identity_name)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        manager.delete_exact(&global_key).await.unwrap();
        assert!(matches!(
            manager.get_global(&name).await,
            Err(AppError::IdentitySpecNotFound { .. })
        ));
        manager.get_exact(&workspace_key).await.unwrap();
        let mut session = db.as_ref();
        assert!(
            session
                .identities()
                .get(&workspace_owner, &exact_name)
                .await
                .unwrap()
                .is_some()
        );
        let mut tx = db.begin().await.unwrap();
        assert!(
            tx.identities()
                .delete(&workspace_owner, &exact_name)
                .await
                .unwrap()
        );
        tx.commit().await.unwrap();
        manager.delete_exact(&workspace_key).await.unwrap();
        assert!(matches!(
            manager.get_exact(&workspace_key).await,
            Err(AppError::IdentitySpecNotFound { .. })
        ));
    }

    async fn mutate(
        manager: &IdentitySpecManager,
        scope: IdentitySpecScope,
        name: &str,
        label: &str,
        values: &[(&str, &str)],
    ) -> (super::InstalledIdentitySpec, bool) {
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
            .unwrap()
    }

    async fn assert_exact(
        manager: &IdentitySpecManager,
        key: &IdentitySpecKey,
        tenant: &str,
        secret: &str,
    ) {
        let resolved = manager.get_exact_for_use(key).await.unwrap();
        assert_eq!(resolved.inputs.variables().get("TENANT").unwrap(), tenant);
        assert_eq!(
            resolved.inputs.secrets().get("CLIENT_SECRET").unwrap(),
            secret
        );
    }

    async fn assert_disjoint_replacements_converge(
        db: &Arc<CoralDb>,
        manager: &IdentitySpecManager,
        suffix: &str,
    ) {
        let name = format!("concurrent_{suffix}");
        let key = IdentitySpecKey::global(&name).unwrap();
        mutate(
            manager,
            IdentitySpecScope::global(),
            &name,
            "race",
            &[("TENANT", "before"), ("CLIENT_SECRET", "before")],
        )
        .await;
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let left = IdentitySpecManager::new(Arc::clone(db), Arc::clone(&manager.key_provider))
            .with_mutation_barrier(Arc::clone(&barrier));
        let right = IdentitySpecManager::new(Arc::clone(db), Arc::clone(&manager.key_provider))
            .with_mutation_barrier(barrier);
        let manifest = oauth_manifest(&name, "race");
        let (left, right) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::join!(
                left.add_or_replace_exact(
                    IdentitySpecScope::global(),
                    &manifest,
                    vec![
                        IdentitySpecInputValue::new("TENANT", "left"),
                        IdentitySpecInputValue::new("CLIENT_SECRET", "right"),
                    ],
                ),
                right.add_or_replace_exact(
                    IdentitySpecScope::global(),
                    &manifest,
                    vec![IdentitySpecInputValue::new("CLIENT_SECRET", "right")],
                )
            )
        })
        .await
        .expect("concurrent identity spec replacements timed out");
        assert!(left.unwrap().1 && right.unwrap().1);
        assert_exact(manager, &key, "left", "right").await;
    }

    #[tokio::test]
    async fn manager_reads_resolves_and_rejects_corruption() {
        let temp = tempdir().unwrap();
        let db = Arc::new(
            CoralDb::open(ResolvedDatabaseConfig::Sqlite {
                path: temp.path().join("coral.sqlite"),
            })
            .await
            .unwrap(),
        );
        db.migrate().await.unwrap();
        let workspace = WorkspaceName::parse("work").unwrap();
        let mut tx = db.begin().await.unwrap();
        tx.workspaces().ensure(workspace.as_str(), 1).await.unwrap();
        for (key, version) in [
            (IdentitySpecKey::global("alpha").unwrap(), "global_alpha"),
            (IdentitySpecKey::global("beta").unwrap(), "global_beta"),
            (
                IdentitySpecKey::workspace(workspace.clone(), "alpha").unwrap(),
                "workspace_alpha",
            ),
            (
                IdentitySpecKey::workspace(workspace.clone(), "gamma").unwrap(),
                "workspace_gamma",
            ),
        ] {
            let manifest_yaml = manifest(key.name(), version);
            let parsed = coral_spec::parse_identity_manifest_yaml(&manifest_yaml).unwrap();
            tx.identity_specs()
                .upsert(&key, &parsed, &manifest_yaml, 2)
                .await
                .unwrap();
        }
        tx.commit().await.unwrap();
        let manager = IdentitySpecManager::new(db, read_key_provider(test_key()));

        assert_eq!(
            manager.get_global("alpha").await.unwrap().manifest.version,
            "global_alpha"
        );
        let beta = IdentitySpecKey::workspace(workspace.clone(), "beta").unwrap();
        assert_not_found(manager.get_exact(&beta).await, "workspace:work");
        assert_eq!(
            manager
                .resolve_for_workspace(&workspace, "beta")
                .await
                .unwrap()
                .key
                .scope(),
            &IdentitySpecScope::Global
        );
        assert_eq!(
            manager
                .resolve_for_workspace(&workspace, "alpha")
                .await
                .unwrap()
                .manifest
                .version,
            "workspace_alpha"
        );
        let exact = manager
            .list_exact(&IdentitySpecScope::workspace(workspace.clone()))
            .await
            .unwrap();
        assert_eq!(
            labels(&exact),
            ["workspace:work:alpha", "workspace:work:gamma"]
        );
        let combined = manager
            .list_workspace_with_global(&workspace)
            .await
            .unwrap();
        assert_eq!(
            labels(&combined),
            [
                "global:alpha",
                "global:beta",
                "workspace:work:alpha",
                "workspace:work:gamma"
            ]
        );

        let missing = WorkspaceName::parse("missing").unwrap();
        assert!(
            matches!(manager.resolve_for_workspace(&missing, "beta").await, Err(AppError::WorkspaceNotFound(name)) if name == "missing")
        );
        assert!(
            matches!(manager.list_workspace_with_global(&missing).await, Err(AppError::WorkspaceNotFound(name)) if name == "missing")
        );
        assert_not_found(
            manager.resolve_for_workspace(&workspace, "absent").await,
            "workspace:work",
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn decrypts_inputs_for_the_actual_resolved_scope_without_rotating_keys() {
        let fixture = encrypted_read_fixture().await;
        let EncryptedReadFixture {
            _temp,
            db,
            workspace,
            reader,
            manager,
            keys,
        } = fixture;
        let EncryptedSpecKeys {
            missing,
            invalid,
            invalid_version,
            invalid_schema,
            default_only,
            ..
        } = keys;

        let global = manager.get_global_for_use("oauth").await.unwrap();
        let shadow = manager
            .resolve_for_workspace_for_use(&workspace, "oauth")
            .await
            .unwrap();
        let fallback = manager
            .resolve_for_workspace_for_use(&workspace, "fallback")
            .await
            .unwrap();
        let default_only = manager.get_exact_for_use(&default_only).await.unwrap();
        assert_eq!(
            default_only.inputs.variables().get("TENANT").unwrap(),
            "tenant-default"
        );
        let exact_fallback = IdentitySpecKey::workspace(workspace.clone(), "fallback").unwrap();
        assert!(matches!(
            manager.get_exact_for_use(&exact_fallback).await,
            Err(AppError::IdentitySpecNotFound { .. })
        ));
        assert_resolved(&global, "global", "tenant-global", "global-secret");
        assert_resolved(
            &shadow,
            "workspace:work",
            "tenant-workspace",
            "workspace-secret",
        );
        assert_resolved(&fallback, "global", "tenant-fallback", "fallback-secret");
        let rendered = format!("{global:?}{shadow:?}{fallback:?}");
        for value in [
            "global-secret",
            "workspace-secret",
            "fallback-secret",
            "tenant-global",
            "tenant-workspace",
            "tenant-fallback",
        ] {
            assert!(!rendered.contains(value));
        }
        assert!(matches!(
            manager.get_exact_for_use(&missing).await,
            Err(AppError::FailedPrecondition(detail)) if detail.contains("CLIENT_SECRET")
        ));
        for key in [&invalid, &invalid_version, &invalid_schema] {
            let error = manager.get_exact_for_use(key).await.unwrap_err();
            assert!(matches!(error, AppError::Database(_)));
            for secret in ["invalid-secret", "version-secret", "schema-secret"] {
                assert!(!format!("{error:?}").contains(secret));
            }
        }
        assert_eq!(reader.key_calls.load(Ordering::SeqCst), 6);
        assert_eq!(reader.active_key_calls.load(Ordering::SeqCst), 0);
        let unavailable = IdentitySpecManager::new(db, Arc::new(UnavailableKeyProvider));
        assert!(matches!(
            unavailable.get_global_for_use("oauth").await,
            Err(AppError::Credentials(CredentialsError::Unavailable(_)))
        ));
    }

    struct EncryptedReadFixture {
        _temp: TempDir,
        db: Arc<CoralDb>,
        workspace: WorkspaceName,
        reader: Arc<ReadKeyProvider>,
        manager: IdentitySpecManager,
        keys: EncryptedSpecKeys,
    }

    struct EncryptedSpecKeys {
        global: IdentitySpecKey,
        workspace: IdentitySpecKey,
        fallback: IdentitySpecKey,
        missing: IdentitySpecKey,
        invalid: IdentitySpecKey,
        invalid_version: IdentitySpecKey,
        invalid_schema: IdentitySpecKey,
        default_only: IdentitySpecKey,
    }

    async fn encrypted_read_fixture() -> EncryptedReadFixture {
        let temp = tempdir().unwrap();
        let db = Arc::new(
            CoralDb::open(ResolvedDatabaseConfig::Sqlite {
                path: temp.path().join("coral.sqlite"),
            })
            .await
            .unwrap(),
        );
        db.migrate().await.unwrap();
        let workspace = WorkspaceName::parse("work").unwrap();
        let stored_key = test_key();
        let writer = TestKeyProvider::new(stored_key.clone());
        let reader = Arc::new(ReadKeyProvider {
            key: stored_key,
            key_calls: AtomicUsize::new(0),
            active_key_calls: AtomicUsize::new(0),
            runtime_thread: thread::current().id(),
        });
        let keys = EncryptedSpecKeys {
            global: IdentitySpecKey::global("oauth").unwrap(),
            workspace: IdentitySpecKey::workspace(workspace.clone(), "oauth").unwrap(),
            fallback: IdentitySpecKey::global("fallback").unwrap(),
            missing: IdentitySpecKey::global("missing_inputs").unwrap(),
            invalid: IdentitySpecKey::global("invalid_inputs").unwrap(),
            invalid_version: IdentitySpecKey::global("invalid_version").unwrap(),
            invalid_schema: IdentitySpecKey::global("invalid_schema").unwrap(),
            default_only: IdentitySpecKey::global("default_only").unwrap(),
        };
        let mut tx = db.begin().await.unwrap();
        tx.workspaces().ensure(workspace.as_str(), 1).await.unwrap();
        seed_encrypted_specs(&mut tx, &keys, &writer).await;
        tx.commit().await.unwrap();
        let manager = IdentitySpecManager::new(Arc::clone(&db), reader.clone());
        EncryptedReadFixture {
            _temp: temp,
            db,
            workspace,
            reader,
            manager,
            keys,
        }
    }

    async fn seed_encrypted_specs(
        tx: &mut CoralTx<'_>,
        keys: &EncryptedSpecKeys,
        writer: &TestKeyProvider,
    ) {
        for (key, label, values) in [
            (
                &keys.global,
                "global",
                Some(BTreeMap::from([(
                    "CLIENT_SECRET".to_string(),
                    "global-secret".to_string(),
                )])),
            ),
            (
                &keys.workspace,
                "workspace",
                Some(BTreeMap::from([(
                    "CLIENT_SECRET".to_string(),
                    "workspace-secret".to_string(),
                )])),
            ),
            (
                &keys.fallback,
                "fallback",
                Some(BTreeMap::from([(
                    "CLIENT_SECRET".to_string(),
                    "fallback-secret".to_string(),
                )])),
            ),
            (&keys.missing, "missing", None),
            (
                &keys.invalid,
                "invalid",
                Some(BTreeMap::from([(
                    "UNDECLARED".to_string(),
                    "invalid-secret".to_string(),
                )])),
            ),
            (&keys.invalid_version, "invalid_version", None),
            (&keys.invalid_schema, "invalid_schema", None),
        ] {
            seed_oauth(tx, key, label, values.as_ref(), writer).await;
        }
        seed_spec(
            tx,
            &keys.default_only,
            default_only_manifest(keys.default_only.name()),
        )
        .await;
        seed_invalid_documents(tx, keys, writer).await;
    }

    async fn seed_invalid_documents(
        tx: &mut CoralTx<'_>,
        keys: &EncryptedSpecKeys,
        writer: &TestKeyProvider,
    ) {
        for (key, plaintext) in [
            (
                &keys.invalid_version,
                serde_json::json!({
                    "version": 2,
                    "values": {"CLIENT_SECRET": "version-secret"},
                }),
            ),
            (
                &keys.invalid_schema,
                serde_json::json!({
                    "version": 1,
                    "values": {"CLIENT_SECRET": "schema-secret"},
                    "unexpected": true,
                }),
            ),
        ] {
            let encrypted = seal_identity_spec_plaintext_for_test(
                key,
                serde_json::to_vec(&plaintext).unwrap(),
                writer,
            )
            .unwrap();
            seed_document(tx, key, encrypted).await;
        }
    }

    #[test]
    fn conversion_rejects_corrupt_manifest_and_metadata_drift() {
        let record = canonical_record();
        let mut invalid_yaml = record.clone();
        invalid_yaml.manifest_yaml = "not: [yaml".to_string();
        assert_corrupt(invalid_yaml);
        for drift in [
            |row: &mut IdentitySpecRecord| row.key = IdentitySpecKey::global("other").unwrap(),
            |row: &mut IdentitySpecRecord| row.version.push_str("_drift"),
            |row: &mut IdentitySpecRecord| row.description.push_str("_drift"),
            |row: &mut IdentitySpecRecord| row.issuer.push_str("_drift"),
        ] {
            let mut drifted = record.clone();
            drift(&mut drifted);
            assert_corrupt(drifted);
        }
    }

    fn manifest(name: &str, version: &str) -> String {
        format!(
            "kind: identity\nspec_version: 1\nname: {name}\nversion: {version}\ndescription: description {version}\nissuer: issuer_{version}\ntype: fixed_token\naudience: {{host: example.com}}\n"
        )
    }

    fn labels(specs: &[super::InstalledIdentitySpec]) -> Vec<String> {
        specs
            .iter()
            .map(|spec| format!("{}:{}", scope_label(spec.key.scope()), spec.key.name()))
            .collect()
    }

    fn assert_not_found(result: Result<super::InstalledIdentitySpec, AppError>, scope: &str) {
        assert!(
            matches!(result, Err(AppError::IdentitySpecNotFound { scope: actual, .. }) if actual == scope)
        );
    }

    fn canonical_record() -> IdentitySpecRecord {
        let manifest_yaml = manifest("drift", "canonical");
        let parsed = coral_spec::parse_identity_manifest_yaml(&manifest_yaml).unwrap();
        IdentitySpecRecord {
            id: IdentitySpecId::new(),
            key: IdentitySpecKey::global("drift").unwrap(),
            version: parsed.version,
            description: parsed.description,
            issuer: parsed.issuer,
            manifest_yaml,
            created_at_unix_nanos: 1,
            updated_at_unix_nanos: 1,
        }
    }

    async fn seed_oauth(
        tx: &mut CoralTx<'_>,
        key: &IdentitySpecKey,
        label: &str,
        values: Option<&BTreeMap<String, String>>,
        key_provider: &dyn CredentialKeyProvider,
    ) {
        let yaml = oauth_manifest(key.name(), label);
        seed_spec(tx, key, yaml).await;
        let Some(values) = values else { return };
        let encrypted = encrypt_identity_spec_document(key, values, key_provider).unwrap();
        seed_document(tx, key, encrypted).await;
    }

    async fn seed_spec(tx: &mut CoralTx<'_>, key: &IdentitySpecKey, yaml: String) {
        let parsed = coral_spec::parse_identity_manifest_yaml(&yaml).unwrap();
        tx.identity_specs()
            .upsert(key, &parsed, &yaml, 2)
            .await
            .unwrap();
    }

    async fn seed_document(
        tx: &mut CoralTx<'_>,
        key: &IdentitySpecKey,
        encrypted: EncryptedEnvelopeDocument,
    ) {
        let identity_spec_id = tx
            .identity_specs()
            .get(key)
            .await
            .unwrap()
            .expect("seeded identity spec")
            .id;
        tx.identity_spec_documents()
            .upsert(&identity_spec_id, &encrypted, 3)
            .await
            .unwrap();
    }

    fn assert_resolved(
        resolved: &super::ResolvedIdentitySpec,
        scope: &str,
        tenant: &str,
        secret: &str,
    ) {
        assert_eq!(scope_label(resolved.spec.key.scope()), scope);
        assert_eq!(resolved.inputs.variables().get("TENANT").unwrap(), tenant);
        assert_eq!(
            resolved.inputs.secrets().get("CLIENT_SECRET").unwrap(),
            secret
        );
    }

    fn test_key() -> CredentialEncryptionKey {
        CredentialEncryptionKey::from_static_bytes_for_test([43; 32])
    }

    fn read_key_provider(key: CredentialEncryptionKey) -> Arc<dyn CredentialKeyProvider> {
        Arc::new(ReadKeyProvider {
            key,
            key_calls: AtomicUsize::new(0),
            active_key_calls: AtomicUsize::new(0),
            runtime_thread: thread::current().id(),
        })
    }

    fn oauth_manifest(name: &str, label: &str) -> String {
        format!(
            "kind: identity\nspec_version: 1\nname: {name}\nversion: {label}\ndescription: OAuth {label}\nissuer: issuer_{label}\ntype: oauth\naudience: {{host: provider.example.com}}\ninputs:\n  TENANT:\n    kind: variable\n    default: tenant-{label}\n  CLIENT_SECRET:\n    kind: secret\n    required: true\noauth:\n  method:\n    flow:\n      type: authorization_code\n      pkce: disabled\n    redirect_uri: http://127.0.0.1:53682/oauth/callback\n    endpoints:\n      authorization_url: https://provider.example.com/authorize\n      token_url: https://provider.example.com/token\n    client:\n      id:\n        input: TENANT\n      secret:\n        input: CLIENT_SECRET\n        transport: basic_auth\n"
        )
    }

    fn default_only_manifest(name: &str) -> String {
        format!(
            "kind: identity\nspec_version: 1\nname: {name}\nversion: default\ndescription: default-only OAuth\nissuer: default\ntype: oauth\naudience: {{host: provider.example.com}}\ninputs:\n  TENANT:\n    kind: variable\n    default: tenant-default\noauth:\n  method:\n    flow: {{type: device_code}}\n    endpoints:\n      device_authorization_url: https://provider.example.com/device\n      token_url: https://provider.example.com/token\n    client:\n      id: {{input: TENANT}}\n"
        )
    }

    fn assert_corrupt(record: IdentitySpecRecord) {
        assert!(matches!(
            record_to_installed(record),
            Err(crate::state::db::DbError::CorruptData(_))
        ));
    }
}
