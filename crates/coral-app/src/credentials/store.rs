//! Credential material persistence behind file and keychain storage backends.

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::io;
use std::path::Path;
use std::sync::{Arc, OnceLock};

use sha2::{Digest as _, Sha256};

use crate::bootstrap::AppError;
use crate::state::AppStateLayout;
use crate::storage::fs as storage_fs;
use crate::storage::fs::FileLock;
use crate::workspaces::WorkspaceName;

use super::{
    CredentialMaterialSnapshot, CredentialSetId, CredentialStorageKind, CredentialStoragePreference,
};

/// Errors returned by credential material storage helpers.
#[derive(Debug, thiserror::Error)]
pub enum CredentialsError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("invalid credential material: {0}")]
    Parse(String),
    #[error("credential storage unavailable: {0}")]
    Unavailable(String),
    #[error("credential snapshot storage mismatch: snapshot is {snapshot}, requested {requested}")]
    SnapshotStorageMismatch {
        snapshot: &'static str,
        requested: &'static str,
    },
}

#[derive(Clone)]
struct EncodedCredentialMaterial(Vec<u8>);

impl EncodedCredentialMaterial {
    fn bytes(&self) -> &[u8] {
        &self.0
    }
}

struct CredentialSetRef<'a> {
    workspace_name: &'a WorkspaceName,
    credential_set_id: &'a CredentialSetId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CredentialConfigNamespace(String);

impl CredentialConfigNamespace {
    fn from_layout(layout: &AppStateLayout) -> Self {
        Self::from_config_dir(layout.config_dir())
    }

    fn from_config_dir(config_dir: &Path) -> Self {
        let canonical = config_dir
            .canonicalize()
            .unwrap_or_else(|_| config_dir.to_path_buf());
        let mut hasher = Sha256::new();
        update_hasher_with_os_str(&mut hasher, canonical.as_os_str());
        let digest = hasher.finalize();
        let hex = format!("{digest:x}");
        let short = hex.get(..16).unwrap_or(hex.as_str());
        Self(short.to_string())
    }

    fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[cfg(unix)]
fn update_hasher_with_os_str(hasher: &mut Sha256, value: &OsStr) {
    use std::os::unix::ffi::OsStrExt as _;

    hasher.update(value.as_bytes());
}

#[cfg(windows)]
fn update_hasher_with_os_str(hasher: &mut Sha256, value: &OsStr) {
    use std::os::windows::ffi::OsStrExt as _;

    for unit in value.encode_wide() {
        hasher.update(unit.to_le_bytes());
    }
}

#[cfg(not(any(unix, windows)))]
fn update_hasher_with_os_str(hasher: &mut Sha256, value: &OsStr) {
    hasher.update(value.to_string_lossy().as_bytes());
}

trait CredentialMaterialBackend: Send + Sync {
    fn probe(&self) -> Result<(), CredentialsError> {
        Ok(())
    }

    fn read(
        &self,
        set: &CredentialSetRef<'_>,
    ) -> Result<Option<EncodedCredentialMaterial>, CredentialsError>;

    fn write(
        &self,
        set: &CredentialSetRef<'_>,
        material: Option<&EncodedCredentialMaterial>,
    ) -> Result<(), CredentialsError>;

    fn snapshot(
        &self,
        set: &CredentialSetRef<'_>,
    ) -> Result<CredentialMaterialSnapshot, CredentialsError>;

    fn restore(
        &self,
        set: &CredentialSetRef<'_>,
        snapshot: &CredentialMaterialSnapshot,
    ) -> Result<(), CredentialsError>;
}

#[derive(Clone)]
pub(crate) struct CredentialStore {
    layout: AppStateLayout,
    preference: CredentialStoragePreference,
    file: Arc<dyn CredentialMaterialBackend>,
    keychain: Arc<dyn CredentialMaterialBackend>,
}

impl CredentialStore {
    #[cfg(test)]
    pub(crate) fn new(layout: AppStateLayout) -> Self {
        Self::with_preference(layout, CredentialStoragePreference::File)
    }

    pub(crate) fn with_preference(
        layout: AppStateLayout,
        preference: CredentialStoragePreference,
    ) -> Self {
        let config_namespace = CredentialConfigNamespace::from_layout(&layout);
        Self {
            layout: layout.clone(),
            preference,
            file: Arc::new(FileCredentialBackend::new(layout)),
            keychain: Arc::new(KeychainCredentialBackend::new(config_namespace)),
        }
    }

    #[cfg(test)]
    fn with_keychain_backend(
        layout: AppStateLayout,
        preference: CredentialStoragePreference,
        keychain: Arc<dyn CredentialMaterialBackend>,
    ) -> Self {
        Self {
            layout: layout.clone(),
            preference,
            file: Arc::new(FileCredentialBackend::new(layout)),
            keychain,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_unavailable_keychain_for_test(
        layout: AppStateLayout,
        preference: CredentialStoragePreference,
    ) -> Self {
        Self::with_keychain_backend(
            layout,
            preference,
            Arc::new(TestKeychainBackend::unavailable()),
        )
    }

    #[cfg(test)]
    pub(crate) fn with_available_keychain_for_test(
        layout: AppStateLayout,
        preference: CredentialStoragePreference,
    ) -> Self {
        Self::with_keychain_backend(
            layout,
            preference,
            Arc::new(TestKeychainBackend::available()),
        )
    }

    pub(crate) fn replace_material(
        &self,
        workspace_name: &WorkspaceName,
        credential_set_id: &CredentialSetId,
        storage: CredentialStorageKind,
        values: &BTreeMap<String, String>,
    ) -> Result<(), AppError> {
        let set = CredentialSetRef {
            workspace_name,
            credential_set_id,
        };
        let backend = self.backend(storage);
        tracing::trace!(%credential_set_id, %storage, "replacing credential material");
        let encoded = if values.is_empty() {
            None
        } else {
            Some(encode_values(storage, values)?)
        };
        contextualize_storage_error(
            backend.write(&set, encoded.as_ref()),
            "writing",
            credential_set_id,
            storage,
        )?;
        Ok(())
    }

    pub(crate) fn update_material<F, R>(
        &self,
        workspace_name: &WorkspaceName,
        credential_set_id: &CredentialSetId,
        storage: CredentialStorageKind,
        update: F,
    ) -> Result<R, AppError>
    where
        F: FnOnce(BTreeMap<String, String>) -> Result<(BTreeMap<String, String>, R), AppError>,
    {
        tracing::trace!(%credential_set_id, %storage, "updating credential material");
        let current = self.read_material(workspace_name, credential_set_id, storage)?;
        let (next, result) = update(current)?;
        self.replace_material(workspace_name, credential_set_id, storage, &next)?;
        Ok(result)
    }

    pub(crate) fn credential_refresh_lock(
        &self,
        workspace_name: &WorkspaceName,
        credential_set_id: &CredentialSetId,
    ) -> Result<FileLock, AppError> {
        let source_name = credential_set_id.source_name()?;
        let path = self
            .layout
            .credential_refresh_lock_file(workspace_name, &source_name);
        tracing::trace!(%credential_set_id, "locking credential refresh");
        FileLock::exclusive(&path).map_err(Into::into)
    }

    pub(crate) fn read_material(
        &self,
        workspace_name: &WorkspaceName,
        credential_set_id: &CredentialSetId,
        storage: CredentialStorageKind,
    ) -> Result<BTreeMap<String, String>, AppError> {
        let set = CredentialSetRef {
            workspace_name,
            credential_set_id,
        };
        tracing::trace!(%credential_set_id, %storage, "reading credential material");
        let encoded = contextualize_storage_error(
            self.backend(storage).read(&set),
            "reading",
            credential_set_id,
            storage,
        )?;
        match encoded {
            Some(encoded) => decode_values(storage, encoded.bytes()).map_err(Into::into),
            None => Ok(BTreeMap::new()),
        }
    }

    pub(crate) fn snapshot_material(
        &self,
        workspace_name: &WorkspaceName,
        credential_set_id: &CredentialSetId,
        storage: CredentialStorageKind,
    ) -> Result<CredentialMaterialSnapshot, AppError> {
        let set = CredentialSetRef {
            workspace_name,
            credential_set_id,
        };
        tracing::trace!(%credential_set_id, %storage, "snapshotting credential material");
        contextualize_storage_error(
            self.backend(storage).snapshot(&set),
            "snapshotting",
            credential_set_id,
            storage,
        )
        .map_err(Into::into)
    }

    pub(crate) fn restore_material(
        &self,
        workspace_name: &WorkspaceName,
        credential_set_id: &CredentialSetId,
        snapshot: &CredentialMaterialSnapshot,
    ) -> Result<(), AppError> {
        let storage = snapshot.storage();
        let set = CredentialSetRef {
            workspace_name,
            credential_set_id,
        };
        tracing::trace!(%credential_set_id, %storage, "restoring credential material");
        contextualize_storage_error(
            self.backend(storage).restore(&set, snapshot),
            "restoring",
            credential_set_id,
            storage,
        )?;
        Ok(())
    }

    pub(crate) fn remove_material(
        &self,
        workspace_name: &WorkspaceName,
        credential_set_id: &CredentialSetId,
        storage: CredentialStorageKind,
    ) -> Result<(), AppError> {
        let set = CredentialSetRef {
            workspace_name,
            credential_set_id,
        };
        tracing::trace!(%credential_set_id, %storage, "removing credential material");
        contextualize_storage_error(
            self.backend(storage).write(&set, None),
            "removing",
            credential_set_id,
            storage,
        )?;
        Ok(())
    }

    pub(crate) fn default_write_storage(&self) -> Result<CredentialStorageKind, CredentialsError> {
        match self.preference {
            CredentialStoragePreference::File => Ok(CredentialStorageKind::File),
            CredentialStoragePreference::Keychain => {
                self.keychain
                    .probe()
                    .map_err(configured_keychain_unavailable)?;
                Ok(CredentialStorageKind::Keychain)
            }
            CredentialStoragePreference::Auto => match self.keychain.probe() {
                Ok(()) => Ok(CredentialStorageKind::Keychain),
                Err(error) => {
                    tracing::warn!(detail = %error, "keychain unavailable; using plaintext file credential storage");
                    Ok(CredentialStorageKind::File)
                }
            },
        }
    }

    fn backend(&self, storage: CredentialStorageKind) -> &dyn CredentialMaterialBackend {
        match storage {
            CredentialStorageKind::File => self.file.as_ref(),
            CredentialStorageKind::Keychain => self.keychain.as_ref(),
        }
    }
}

fn contextualize_storage_error<T>(
    result: Result<T, CredentialsError>,
    operation: &'static str,
    credential_set_id: &CredentialSetId,
    storage: CredentialStorageKind,
) -> Result<T, CredentialsError> {
    if storage == CredentialStorageKind::Keychain {
        result.map_err(|error| keychain_route_unavailable(error, operation, credential_set_id))
    } else {
        result
    }
}

fn keychain_route_unavailable(
    error: CredentialsError,
    operation: &'static str,
    credential_set_id: &CredentialSetId,
) -> CredentialsError {
    match error {
        CredentialsError::Unavailable(detail) => CredentialsError::Unavailable(format!(
            "source credential set '{credential_set_id}' is configured for keychain storage, \
             but keychain is unavailable while {operation}: {detail}"
        )),
        error => error,
    }
}

fn configured_keychain_unavailable(error: CredentialsError) -> CredentialsError {
    match error {
        CredentialsError::Unavailable(detail) => CredentialsError::Unavailable(format!(
            "keychain credential storage is configured, but keychain is unavailable: \
             {detail}. Set [credentials] storage = \"file\" to use plaintext file storage."
        )),
        error => error,
    }
}

#[cfg(test)]
struct TestKeychainBackend {
    available: bool,
    material: std::sync::Mutex<Option<Vec<u8>>>,
}

#[cfg(test)]
impl TestKeychainBackend {
    fn available() -> Self {
        Self {
            available: true,
            material: std::sync::Mutex::new(None),
        }
    }

    fn unavailable() -> Self {
        Self {
            available: false,
            material: std::sync::Mutex::new(None),
        }
    }

    fn lock_material(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, Option<Vec<u8>>>, CredentialsError> {
        self.material.lock().map_err(|error| {
            CredentialsError::Unavailable(format!("test keychain lock poisoned: {error}"))
        })
    }
}

#[cfg(test)]
impl CredentialMaterialBackend for TestKeychainBackend {
    fn probe(&self) -> Result<(), CredentialsError> {
        if self.available {
            Ok(())
        } else {
            Err(CredentialsError::Unavailable(
                "test keychain unavailable".to_string(),
            ))
        }
    }

    fn read(
        &self,
        _set: &CredentialSetRef<'_>,
    ) -> Result<Option<EncodedCredentialMaterial>, CredentialsError> {
        self.probe()?;
        Ok(self.lock_material()?.clone().map(EncodedCredentialMaterial))
    }

    fn write(
        &self,
        _set: &CredentialSetRef<'_>,
        material: Option<&EncodedCredentialMaterial>,
    ) -> Result<(), CredentialsError> {
        self.probe()?;
        *self.lock_material()? = material.map(|material| material.bytes().to_vec());
        Ok(())
    }

    fn snapshot(
        &self,
        _set: &CredentialSetRef<'_>,
    ) -> Result<CredentialMaterialSnapshot, CredentialsError> {
        self.probe()?;
        Ok(CredentialMaterialSnapshot::new(
            CredentialStorageKind::Keychain,
            self.lock_material()?.clone(),
        ))
    }

    fn restore(
        &self,
        _set: &CredentialSetRef<'_>,
        snapshot: &CredentialMaterialSnapshot,
    ) -> Result<(), CredentialsError> {
        self.probe()?;
        *self.lock_material()? = snapshot.material().map(ToOwned::to_owned);
        Ok(())
    }
}

struct FileCredentialBackend {
    layout: AppStateLayout,
}

impl FileCredentialBackend {
    fn new(layout: AppStateLayout) -> Self {
        Self { layout }
    }

    fn material_file(
        &self,
        set: &CredentialSetRef<'_>,
    ) -> Result<std::path::PathBuf, CredentialsError> {
        let source_name = set
            .credential_set_id
            .source_name()
            .map_err(|error| CredentialsError::Parse(error.to_string()))?;
        Ok(self.layout.secret_file(set.workspace_name, &source_name))
    }
}

impl CredentialMaterialBackend for FileCredentialBackend {
    fn read(
        &self,
        set: &CredentialSetRef<'_>,
    ) -> Result<Option<EncodedCredentialMaterial>, CredentialsError> {
        let path = self.material_file(set)?;
        match std::fs::read(path) {
            Ok(bytes) => Ok(Some(EncodedCredentialMaterial(bytes))),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn write(
        &self,
        set: &CredentialSetRef<'_>,
        material: Option<&EncodedCredentialMaterial>,
    ) -> Result<(), CredentialsError> {
        let path = self.material_file(set)?;
        let _lock = FileLock::exclusive(self.layout.state_lock())?;
        match material {
            Some(material) => write_file_unlocked(&path, material.bytes()),
            None => remove_file_if_exists_unlocked(&path).map_err(Into::into),
        }
    }

    fn snapshot(
        &self,
        set: &CredentialSetRef<'_>,
    ) -> Result<CredentialMaterialSnapshot, CredentialsError> {
        let path = self.material_file(set)?;
        let _lock = FileLock::shared(self.layout.state_lock())?;
        match std::fs::read(path) {
            Ok(bytes) => Ok(CredentialMaterialSnapshot::new(
                CredentialStorageKind::File,
                Some(bytes),
            )),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(
                CredentialMaterialSnapshot::new(CredentialStorageKind::File, None),
            ),
            Err(error) => Err(error.into()),
        }
    }

    fn restore(
        &self,
        set: &CredentialSetRef<'_>,
        snapshot: &CredentialMaterialSnapshot,
    ) -> Result<(), CredentialsError> {
        if snapshot.storage() != CredentialStorageKind::File {
            return Err(CredentialsError::SnapshotStorageMismatch {
                snapshot: snapshot.storage().as_config_value(),
                requested: CredentialStorageKind::File.as_config_value(),
            });
        }
        let path = self.material_file(set)?;
        let _lock = FileLock::exclusive(self.layout.state_lock())?;
        match snapshot.material() {
            Some(bytes) => write_file_unlocked(&path, bytes),
            None => remove_file_if_exists_unlocked(&path).map_err(Into::into),
        }
    }
}

#[derive(Clone)]
struct KeychainCredentialBackend {
    config_namespace: CredentialConfigNamespace,
    native: Arc<OnceLock<Result<Arc<keyring_core::CredentialStore>, String>>>,
    probe: Arc<OnceLock<Result<(), String>>>,
}

impl KeychainCredentialBackend {
    fn new(config_namespace: CredentialConfigNamespace) -> Self {
        Self {
            config_namespace,
            native: Arc::new(OnceLock::new()),
            probe: Arc::new(OnceLock::new()),
        }
    }

    fn native_store(&self) -> Result<Arc<keyring_core::CredentialStore>, CredentialsError> {
        match self.native.get_or_init(native_keychain_store) {
            Ok(store) => Ok(Arc::clone(store)),
            Err(error) => Err(CredentialsError::Unavailable(error.clone())),
        }
    }

    fn entry_for(
        &self,
        service: &str,
        account: &str,
    ) -> Result<keyring_core::Entry, CredentialsError> {
        self.native_store()?
            .build(service, account, None)
            .map_err(|error| keychain_error(&error))
    }

    fn probe_entry(&self) -> Result<keyring_core::Entry, CredentialsError> {
        let account = format!("probe.{}.{}", std::process::id(), uuid::Uuid::new_v4());
        let service = format!(
            "com.withcoral.coral/{}/__probe__",
            self.config_namespace.as_str()
        );
        self.native_store()?
            .build(&service, &account, None)
            .map_err(|error| keychain_error(&error))
    }

    fn run_native<T, F>(&self, operation: F) -> Result<T, CredentialsError>
    where
        T: Send + 'static,
        F: FnOnce(Self) -> Result<T, CredentialsError> + Send + 'static,
    {
        run_native_keychain(self.clone(), operation)
    }

    fn probe_native(&self) -> Result<(), CredentialsError> {
        match self.probe.get_or_init(|| {
            catch_native_keychain_panic(|| self.run_probe()).map_err(|error| error.to_string())
        }) {
            Ok(()) => Ok(()),
            Err(error) => Err(CredentialsError::Unavailable(error.clone())),
        }
    }

    fn run_probe(&self) -> Result<(), CredentialsError> {
        let entry = self.probe_entry()?;
        entry
            .set_password("ok")
            .map_err(|error| keychain_error(&error))?;
        let stored = entry
            .get_password()
            .map_err(|error| keychain_error(&error))?;
        if stored != "ok" {
            if let Err(error) = entry.delete_credential() {
                tracing::warn!(detail = %error, "keychain probe cleanup failed");
            }
            return Err(CredentialsError::Unavailable(
                "keychain probe read back unexpected value".to_string(),
            ));
        }
        entry
            .delete_credential()
            .map_err(|error| keychain_error(&error))?;
        Ok(())
    }
}

impl CredentialMaterialBackend for KeychainCredentialBackend {
    fn probe(&self) -> Result<(), CredentialsError> {
        self.run_native(|backend| backend.probe_native())
    }

    fn read(
        &self,
        set: &CredentialSetRef<'_>,
    ) -> Result<Option<EncodedCredentialMaterial>, CredentialsError> {
        let entry = KeychainEntryAddress::from_set(&self.config_namespace, set);
        self.run_native(move |backend| {
            backend.probe_native()?;
            match backend
                .entry_for(&entry.service, &entry.account)?
                .get_password()
            {
                Ok(value) => Ok(Some(EncodedCredentialMaterial(value.into_bytes()))),
                Err(keyring_core::Error::NoEntry) => Ok(None),
                Err(error) => Err(keychain_error(&error)),
            }
        })
    }

    fn write(
        &self,
        set: &CredentialSetRef<'_>,
        material: Option<&EncodedCredentialMaterial>,
    ) -> Result<(), CredentialsError> {
        let entry = KeychainEntryAddress::from_set(&self.config_namespace, set);
        let value = material
            .map(|material| {
                std::str::from_utf8(material.bytes())
                    .map(ToOwned::to_owned)
                    .map_err(|error| {
                        CredentialsError::Parse(format!(
                            "keychain material is not valid UTF-8: {error}"
                        ))
                    })
            })
            .transpose()?;
        self.run_native(move |backend| {
            backend.probe_native()?;
            let entry = backend.entry_for(&entry.service, &entry.account)?;
            match value {
                Some(value) => entry
                    .set_password(&value)
                    .map_err(|error| keychain_error(&error)),
                None => match entry.delete_credential() {
                    Ok(()) | Err(keyring_core::Error::NoEntry) => Ok(()),
                    Err(error) => Err(keychain_error(&error)),
                },
            }
        })
    }

    fn snapshot(
        &self,
        set: &CredentialSetRef<'_>,
    ) -> Result<CredentialMaterialSnapshot, CredentialsError> {
        let entry = KeychainEntryAddress::from_set(&self.config_namespace, set);
        self.run_native(move |backend| {
            backend.probe_native()?;
            match backend
                .entry_for(&entry.service, &entry.account)?
                .get_password()
            {
                Ok(value) => Ok(CredentialMaterialSnapshot::new(
                    CredentialStorageKind::Keychain,
                    Some(value.into_bytes()),
                )),
                Err(keyring_core::Error::NoEntry) => Ok(CredentialMaterialSnapshot::new(
                    CredentialStorageKind::Keychain,
                    None,
                )),
                Err(error) => Err(keychain_error(&error)),
            }
        })
    }

    fn restore(
        &self,
        set: &CredentialSetRef<'_>,
        snapshot: &CredentialMaterialSnapshot,
    ) -> Result<(), CredentialsError> {
        if snapshot.storage() != CredentialStorageKind::Keychain {
            return Err(CredentialsError::SnapshotStorageMismatch {
                snapshot: snapshot.storage().as_config_value(),
                requested: CredentialStorageKind::Keychain.as_config_value(),
            });
        }
        match snapshot.material() {
            Some(bytes) => self.write(set, Some(&EncodedCredentialMaterial(bytes.to_vec()))),
            None => self.write(set, None),
        }
    }
}

#[derive(Clone)]
struct KeychainEntryAddress {
    service: String,
    account: String,
}

impl KeychainEntryAddress {
    fn from_set(config_namespace: &CredentialConfigNamespace, set: &CredentialSetRef<'_>) -> Self {
        Self {
            service: format!(
                "com.withcoral.coral/{}/workspace/{}",
                config_namespace.as_str(),
                set.workspace_name.as_str()
            ),
            account: set.credential_set_id.to_string(),
        }
    }
}

#[cfg(target_os = "linux")]
fn run_native_keychain<T, F>(
    backend: KeychainCredentialBackend,
    operation: F,
) -> Result<T, CredentialsError>
where
    T: Send + 'static,
    F: FnOnce(KeychainCredentialBackend) -> Result<T, CredentialsError> + Send + 'static,
{
    std::thread::Builder::new()
        .name("coral-keychain".to_string())
        .spawn(move || catch_native_keychain_panic(|| operation(backend)))
        .map_err(|error| {
            CredentialsError::Unavailable(format!(
                "failed to start native keychain worker thread: {error}"
            ))
        })?
        .join()
        .unwrap_or_else(|payload| Err(native_keychain_panic_error(payload.as_ref())))
}

#[cfg(not(target_os = "linux"))]
fn run_native_keychain<T, F>(
    backend: KeychainCredentialBackend,
    operation: F,
) -> Result<T, CredentialsError>
where
    T: Send + 'static,
    F: FnOnce(KeychainCredentialBackend) -> Result<T, CredentialsError> + Send + 'static,
{
    catch_native_keychain_panic(|| operation(backend))
}

fn catch_native_keychain_panic<T, F>(operation: F) -> Result<T, CredentialsError>
where
    F: FnOnce() -> Result<T, CredentialsError>,
{
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation))
        .unwrap_or_else(|payload| Err(native_keychain_panic_error(payload.as_ref())))
}

fn native_keychain_panic_error(payload: &(dyn std::any::Any + Send)) -> CredentialsError {
    let message = if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic".to_string()
    };
    CredentialsError::Unavailable(format!("native keychain operation panicked: {message}"))
}

#[cfg(target_os = "macos")]
fn native_keychain_store() -> Result<Arc<keyring_core::CredentialStore>, String> {
    apple_native_keyring_store::keychain::Store::new()
        .map(|store| store as Arc<keyring_core::CredentialStore>)
        .map_err(|error| error.to_string())
}

#[cfg(windows)]
fn native_keychain_store() -> Result<Arc<keyring_core::CredentialStore>, String> {
    windows_native_keyring_store::Store::new()
        .map(|store| store as Arc<keyring_core::CredentialStore>)
        .map_err(|error| error.to_string())
}

#[cfg(target_os = "linux")]
fn native_keychain_store() -> Result<Arc<keyring_core::CredentialStore>, String> {
    zbus_secret_service_keyring_store::Store::new()
        .map(|store| store as Arc<keyring_core::CredentialStore>)
        .map_err(|error| error.to_string())
}

#[cfg(not(any(target_os = "macos", windows, target_os = "linux")))]
fn native_keychain_store() -> Result<Arc<keyring_core::CredentialStore>, String> {
    Err("native keychain storage is not supported on this platform".to_string())
}

fn keychain_error(error: &keyring_core::Error) -> CredentialsError {
    CredentialsError::Unavailable(error.to_string())
}

#[derive(serde::Serialize, serde::Deserialize)]
struct KeychainMaterialDocument {
    version: u32,
    values: BTreeMap<String, String>,
}

fn encode_values(
    storage: CredentialStorageKind,
    values: &BTreeMap<String, String>,
) -> Result<EncodedCredentialMaterial, CredentialsError> {
    match storage {
        CredentialStorageKind::File => Ok(EncodedCredentialMaterial(
            render_env_file(values).into_bytes(),
        )),
        CredentialStorageKind::Keychain => {
            let document = KeychainMaterialDocument {
                version: 1,
                values: values.clone(),
            };
            serde_json::to_vec(&document)
                .map(EncodedCredentialMaterial)
                .map_err(|error| CredentialsError::Parse(error.to_string()))
        }
    }
}

fn decode_values(
    storage: CredentialStorageKind,
    bytes: &[u8],
) -> Result<BTreeMap<String, String>, CredentialsError> {
    match storage {
        CredentialStorageKind::File => {
            let raw = std::str::from_utf8(bytes)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
            parse_env_file(raw)
        }
        CredentialStorageKind::Keychain => {
            let document: KeychainMaterialDocument = serde_json::from_slice(bytes)
                .map_err(|error| CredentialsError::Parse(error.to_string()))?;
            if document.version != 1 {
                return Err(CredentialsError::Parse(format!(
                    "unsupported keychain credential material version {}",
                    document.version
                )));
            }
            Ok(document.values)
        }
    }
}

#[cfg(test)]
fn load_file(path: &Path) -> Result<BTreeMap<String, String>, CredentialsError> {
    if !path.exists() {
        return Ok(BTreeMap::new());
    }

    parse_env_file(&std::fs::read_to_string(path)?)
}

#[cfg(test)]
fn save_file(
    path: &Path,
    lock_path: &Path,
    values: &BTreeMap<String, String>,
) -> Result<(), CredentialsError> {
    let _lock = FileLock::exclusive(lock_path)?;
    save_values_unlocked(path, values)
}

#[cfg(test)]
fn save_values_unlocked(
    path: &Path,
    values: &BTreeMap<String, String>,
) -> Result<(), CredentialsError> {
    if values.is_empty() {
        remove_file_if_exists_unlocked(path)?;
        return Ok(());
    }

    write_file_unlocked(path, render_env_file(values).as_bytes())
}

fn write_file_unlocked(path: &Path, bytes: &[u8]) -> Result<(), CredentialsError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    storage_fs::ensure_dir(parent)?;
    storage_fs::write_atomic(path, bytes)?;
    Ok(())
}

fn render_env_file(values: &BTreeMap<String, String>) -> String {
    let mut output = String::new();
    for (env_var, value) in values {
        output.push_str(env_var);
        output.push('=');
        output.push_str(&encode_env_value(value));
        output.push('\n');
    }
    output
}

fn remove_file_if_exists_unlocked(path: &Path) -> Result<(), io::Error> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn parse_env_file(raw: &str) -> Result<BTreeMap<String, String>, CredentialsError> {
    let mut values = BTreeMap::new();
    for (index, line) in raw.lines().enumerate() {
        let line_number = index + 1;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        let Some((env_var, raw_value)) = line.split_once('=') else {
            return Err(CredentialsError::Parse(format!(
                "line {line_number} is missing '='"
            )));
        };
        let env_var = env_var.trim();
        if env_var.is_empty() {
            return Err(CredentialsError::Parse(format!(
                "line {line_number} has an empty variable name"
            )));
        }
        if values.contains_key(env_var) {
            return Err(CredentialsError::Parse(format!(
                "line {line_number} redefines '{env_var}'"
            )));
        }

        let value = decode_env_value(raw_value.trim(), line_number)?;
        values.insert(env_var.to_string(), value);
    }
    Ok(values)
}

fn decode_env_value(raw: &str, line_number: usize) -> Result<String, CredentialsError> {
    if let Some(inner) = raw.strip_prefix('"') {
        let Some(inner) = inner.strip_suffix('"') else {
            return Err(CredentialsError::Parse(format!(
                "line {line_number} has an unterminated quoted value"
            )));
        };
        return decode_quoted_env_value(inner, line_number);
    }

    if let Some(inner) = raw.strip_prefix('\'') {
        let Some(inner) = inner.strip_suffix('\'') else {
            return Err(CredentialsError::Parse(format!(
                "line {line_number} has an unterminated single-quoted value"
            )));
        };
        return Ok(inner.to_string());
    }

    Ok(raw.to_string())
}

fn decode_quoted_env_value(raw: &str, line_number: usize) -> Result<String, CredentialsError> {
    let mut decoded = String::with_capacity(raw.len());
    let mut chars = raw.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            decoded.push(ch);
            continue;
        }

        let Some(escaped) = chars.next() else {
            return Err(CredentialsError::Parse(format!(
                "line {line_number} ends with a dangling escape"
            )));
        };
        match escaped {
            '\\' => decoded.push('\\'),
            '"' => decoded.push('"'),
            'n' => decoded.push('\n'),
            'r' => decoded.push('\r'),
            't' => decoded.push('\t'),
            other => {
                return Err(CredentialsError::Parse(format!(
                    "line {line_number} uses unsupported escape '\\{other}'"
                )));
            }
        }
    }
    Ok(decoded)
}

fn encode_env_value(value: &str) -> String {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '/' | ':' | '@'))
    {
        return value.to_string();
    }

    let mut encoded = String::with_capacity(value.len() + 2);
    encoded.push('"');
    for ch in value.chars() {
        match ch {
            '\\' => encoded.push_str("\\\\"),
            '"' => encoded.push_str("\\\""),
            '\n' => encoded.push_str("\\n"),
            '\r' => encoded.push_str("\\r"),
            '\t' => encoded.push_str("\\t"),
            other => encoded.push(other),
        }
    }
    encoded.push('"');
    encoded
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use super::{CredentialMaterialBackend, CredentialSetRef, EncodedCredentialMaterial};
    use super::{CredentialStore, decode_env_value, encode_env_value, load_file, save_file};
    use crate::credentials::{
        CredentialMaterialSnapshot, CredentialSetId, CredentialStorageKind,
        CredentialStoragePreference,
    };
    use crate::sources::SourceName;
    use crate::state::AppStateLayout;
    use crate::workspaces::WorkspaceName;
    use tempfile::TempDir;

    struct FakeKeychainBackend {
        probe_ok: bool,
        material: Mutex<Option<Vec<u8>>>,
    }

    impl FakeKeychainBackend {
        fn available() -> Arc<Self> {
            Arc::new(Self {
                probe_ok: true,
                material: Mutex::new(None),
            })
        }

        fn unavailable() -> Arc<Self> {
            Arc::new(Self {
                probe_ok: false,
                material: Mutex::new(None),
            })
        }

        fn material_bytes(&self) -> Option<Vec<u8>> {
            self.material.lock().expect("material lock").clone()
        }

        fn lock_material(
            &self,
        ) -> Result<std::sync::MutexGuard<'_, Option<Vec<u8>>>, super::CredentialsError> {
            self.material.lock().map_err(|error| {
                super::CredentialsError::Unavailable(format!(
                    "fake keychain lock poisoned: {error}"
                ))
            })
        }
    }

    impl CredentialMaterialBackend for FakeKeychainBackend {
        fn probe(&self) -> Result<(), super::CredentialsError> {
            if self.probe_ok {
                Ok(())
            } else {
                Err(super::CredentialsError::Unavailable(
                    "fake keychain unavailable".to_string(),
                ))
            }
        }

        fn read(
            &self,
            _set: &CredentialSetRef<'_>,
        ) -> Result<Option<EncodedCredentialMaterial>, super::CredentialsError> {
            self.probe()?;
            Ok(self.lock_material()?.clone().map(EncodedCredentialMaterial))
        }

        fn write(
            &self,
            _set: &CredentialSetRef<'_>,
            material: Option<&EncodedCredentialMaterial>,
        ) -> Result<(), super::CredentialsError> {
            self.probe()?;
            *self.lock_material()? = material.map(|material| material.bytes().to_vec());
            Ok(())
        }

        fn snapshot(
            &self,
            _set: &CredentialSetRef<'_>,
        ) -> Result<CredentialMaterialSnapshot, super::CredentialsError> {
            self.probe()?;
            Ok(CredentialMaterialSnapshot::new(
                CredentialStorageKind::Keychain,
                self.lock_material()?.clone(),
            ))
        }

        fn restore(
            &self,
            _set: &CredentialSetRef<'_>,
            snapshot: &CredentialMaterialSnapshot,
        ) -> Result<(), super::CredentialsError> {
            self.probe()?;
            *self.lock_material()? = snapshot.material().map(ToOwned::to_owned);
            Ok(())
        }
    }

    #[test]
    fn keychain_address_namespaces_by_config_workspace_and_credential_set() {
        let config_namespace = super::CredentialConfigNamespace("test".to_string());
        let workspace_name = WorkspaceName::parse("default").expect("workspace");
        let source_name = SourceName::parse("github").expect("source");
        let credential_set_id = CredentialSetId::for_source(&source_name);
        let set = CredentialSetRef {
            workspace_name: &workspace_name,
            credential_set_id: &credential_set_id,
        };

        let address = super::KeychainEntryAddress::from_set(&config_namespace, &set);

        assert_eq!(
            address.service,
            "com.withcoral.coral/test/workspace/default"
        );
        assert_eq!(address.account, credential_set_id.to_string());
    }

    #[test]
    fn config_namespace_separates_same_source_in_different_config_dirs() {
        let temp = TempDir::new().expect("temp dir");
        let first_dir = temp.path().join("first-config");
        let second_dir = temp.path().join("second-config");
        std::fs::create_dir_all(&first_dir).expect("first config dir");
        std::fs::create_dir_all(&second_dir).expect("second config dir");
        let first_namespace = super::CredentialConfigNamespace::from_config_dir(&first_dir);
        let second_namespace = super::CredentialConfigNamespace::from_config_dir(&second_dir);
        let workspace_name = WorkspaceName::parse("default").expect("workspace");
        let source_name = SourceName::parse("github").expect("source");
        let credential_set_id = CredentialSetId::for_source(&source_name);
        let set = CredentialSetRef {
            workspace_name: &workspace_name,
            credential_set_id: &credential_set_id,
        };

        let first = super::KeychainEntryAddress::from_set(&first_namespace, &set);
        let second = super::KeychainEntryAddress::from_set(&second_namespace, &set);

        assert_ne!(first.service, second.service);
        assert_eq!(first.account, second.account);
    }

    #[test]
    fn config_namespace_uses_canonical_config_dir() {
        let temp = TempDir::new().expect("temp dir");
        let config_dir = temp.path().join("coral-config");
        std::fs::create_dir_all(&config_dir).expect("config dir");
        let equivalent = config_dir.join("..").join("coral-config");

        assert_eq!(
            super::CredentialConfigNamespace::from_config_dir(&config_dir),
            super::CredentialConfigNamespace::from_config_dir(&equivalent)
        );
    }

    #[cfg(unix)]
    #[test]
    fn config_namespace_hashes_non_utf8_config_dir_losslessly() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt as _;

        let temp = TempDir::new().expect("temp dir");
        let first_dir = temp
            .path()
            .join(OsString::from_vec(b"coral-config-\xff".to_vec()));
        let second_dir = temp
            .path()
            .join(OsString::from_vec(b"coral-config-\xfe".to_vec()));

        assert_ne!(
            super::CredentialConfigNamespace::from_config_dir(&first_dir),
            super::CredentialConfigNamespace::from_config_dir(&second_dir)
        );
    }

    #[test]
    fn round_trips_encoded_secret_values() {
        let temp = TempDir::new().expect("temp dir");
        let path = temp.path().join("secret.env");
        let lock_path = temp.path().join(".lock");
        let values = std::collections::BTreeMap::from([
            ("TOKEN".to_string(), "abc".to_string()),
            ("MULTI".to_string(), "hello\nworld".to_string()),
        ]);
        save_file(&path, &lock_path, &values).expect("save env file");
        assert_eq!(load_file(&path).expect("load env file"), values);
        assert_eq!(encode_env_value("hello world"), "\"hello world\"");
        assert_eq!(
            decode_env_value("\"hello\\nworld\"", 1).expect("decode"),
            "hello\nworld"
        );
    }

    #[test]
    fn replace_material_does_not_parse_existing_file() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let store = CredentialStore::new(layout.clone());
        let workspace_name = WorkspaceName::default();
        let source_name = SourceName::parse("secured_messages").expect("source");
        let credential_set_id = CredentialSetId::for_source(&source_name);
        let path = layout.secret_file(&workspace_name, &source_name);
        std::fs::create_dir_all(path.parent().expect("secret parent")).expect("secret parent dir");
        std::fs::write(&path, "BROKEN\n").expect("write malformed existing env file");

        let values = BTreeMap::from([("API_TOKEN".to_string(), "secret-token".to_string())]);
        store
            .replace_material(
                &workspace_name,
                &credential_set_id,
                CredentialStorageKind::File,
                &values,
            )
            .expect("replace malformed material");
        assert_eq!(load_file(&path).expect("load replaced material"), values);

        std::fs::write(&path, "BROKEN\n").expect("write malformed existing env file");
        store
            .replace_material(
                &workspace_name,
                &credential_set_id,
                CredentialStorageKind::File,
                &BTreeMap::new(),
            )
            .expect("remove malformed material");
        assert!(!path.exists(), "empty replacement should remove material");
    }

    #[test]
    fn remove_material_treats_missing_files_as_success() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let store = CredentialStore::new(layout.clone());
        let workspace_name = WorkspaceName::default();
        let source_name = SourceName::parse("secured_messages").expect("source");
        let credential_set_id = CredentialSetId::for_source(&source_name);
        let path = layout.secret_file(&workspace_name, &source_name);

        store
            .remove_material(
                &workspace_name,
                &credential_set_id,
                CredentialStorageKind::File,
            )
            .expect("missing material should be removable");

        std::fs::create_dir_all(path.parent().expect("secret parent")).expect("secret parent dir");
        std::fs::write(&path, "BROKEN\n").expect("write malformed existing env file");
        store
            .remove_material(
                &workspace_name,
                &credential_set_id,
                CredentialStorageKind::File,
            )
            .expect("malformed material should be removable");
        assert!(!path.exists(), "remove should delete material");

        store
            .remove_material(
                &workspace_name,
                &credential_set_id,
                CredentialStorageKind::File,
            )
            .expect("second remove should still be successful");
    }

    #[test]
    fn restore_material_snapshot_preserves_raw_bytes() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let store = CredentialStore::new(layout.clone());
        let workspace_name = WorkspaceName::default();
        let source_name = SourceName::parse("secured_messages").expect("source");
        let credential_set_id = CredentialSetId::for_source(&source_name);
        let path = layout.secret_file(&workspace_name, &source_name);
        std::fs::create_dir_all(path.parent().expect("secret parent")).expect("secret parent dir");
        std::fs::write(&path, "BROKEN\n").expect("write malformed existing env file");

        let snapshot = store
            .snapshot_material(
                &workspace_name,
                &credential_set_id,
                CredentialStorageKind::File,
            )
            .expect("snapshot malformed material");
        let values = BTreeMap::from([("API_TOKEN".to_string(), "secret-token".to_string())]);
        store
            .replace_material(
                &workspace_name,
                &credential_set_id,
                CredentialStorageKind::File,
                &values,
            )
            .expect("replace material");

        store
            .restore_material(&workspace_name, &credential_set_id, &snapshot)
            .expect("restore malformed material");
        assert_eq!(
            std::fs::read(&path).expect("restored bytes"),
            b"BROKEN\n".to_vec()
        );
    }

    #[test]
    fn auto_prefers_keychain_when_probe_succeeds() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let keychain = FakeKeychainBackend::available();
        let store = CredentialStore::with_keychain_backend(
            layout,
            CredentialStoragePreference::Auto,
            keychain.clone(),
        );
        assert_eq!(
            store.default_write_storage().expect("storage"),
            CredentialStorageKind::Keychain
        );
    }

    #[test]
    fn auto_falls_back_to_file_when_keychain_probe_fails() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let store = CredentialStore::with_keychain_backend(
            layout,
            CredentialStoragePreference::Auto,
            FakeKeychainBackend::unavailable(),
        );
        assert_eq!(
            store.default_write_storage().expect("storage"),
            CredentialStorageKind::File
        );
    }

    #[test]
    fn explicit_file_uses_file_even_when_keychain_probe_fails() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let store = CredentialStore::with_keychain_backend(
            layout,
            CredentialStoragePreference::File,
            FakeKeychainBackend::unavailable(),
        );
        assert_eq!(
            store.default_write_storage().expect("storage"),
            CredentialStorageKind::File
        );
    }

    #[test]
    fn explicit_keychain_fails_when_probe_fails() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let store = CredentialStore::with_keychain_backend(
            layout,
            CredentialStoragePreference::Keychain,
            FakeKeychainBackend::unavailable(),
        );
        let error = store
            .default_write_storage()
            .expect_err("explicit keychain should fail");
        assert!(
            matches!(error, super::CredentialsError::Unavailable(_)),
            "unexpected error: {error:#}"
        );
        assert!(
            error.to_string().contains("storage = \"file\""),
            "explicit keychain failure should include file-storage hint: {error}"
        );
    }

    #[test]
    fn keychain_backend_stores_one_versioned_document() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let keychain = FakeKeychainBackend::available();
        let store = CredentialStore::with_keychain_backend(
            layout,
            CredentialStoragePreference::Keychain,
            keychain.clone(),
        );
        let workspace_name = WorkspaceName::default();
        let source_name = SourceName::parse("secured_messages").expect("source");
        let credential_set_id = CredentialSetId::for_source(&source_name);
        let values = BTreeMap::from([
            ("API_TOKEN".to_string(), "secret-token".to_string()),
            (
                "__coral_oauth.QVBJX1RPS0VO.method".to_string(),
                "oauth".to_string(),
            ),
        ]);

        store
            .replace_material(
                &workspace_name,
                &credential_set_id,
                CredentialStorageKind::Keychain,
                &values,
            )
            .expect("write keychain material");

        let raw = keychain.material_bytes().expect("keychain blob");
        let document: serde_json::Value = serde_json::from_slice(&raw).expect("json");
        assert_eq!(document.get("version"), Some(&serde_json::json!(1)));
        assert_eq!(
            document
                .get("values")
                .and_then(|values| values.get("API_TOKEN")),
            Some(&serde_json::json!("secret-token"))
        );
        assert_eq!(
            store
                .read_material(
                    &workspace_name,
                    &credential_set_id,
                    CredentialStorageKind::Keychain,
                )
                .expect("read keychain material"),
            values
        );

        store
            .remove_material(
                &workspace_name,
                &credential_set_id,
                CredentialStorageKind::Keychain,
            )
            .expect("remove keychain material");
        assert!(
            keychain.material_bytes().is_none(),
            "remove should delete the stored keychain document"
        );
    }
}
