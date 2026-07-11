//! Endpoint-bound persistence for OAuth login results.

use std::fs::{self, File};
use std::io::{self, Read as _};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use zeroize::Zeroizing;

use super::login::{OAuthLoginResult, valid_access_token};
use crate::CanonicalRemoteEndpoint;
use crate::outbound_url_policy::ConfiguredEndpointUrl;
use crate::state::AppStateLayout;
use crate::storage::fs::{FileLock, ensure_file_private, ensure_private_dir, write_atomic};

const RECORD_VERSION: u32 = 1;
const RECORD_MAX_BYTES: usize = 64 * 1024;

/// Stores one login result for an exact canonical remote gRPC endpoint,
/// replacing the prior endpoint-bound record atomically.
///
/// # Errors
/// Returns an error when app-state discovery, private storage, locking, or
/// serialization fails.
pub fn save_oauth_login(
    config_dir_override: Option<PathBuf>,
    endpoint: &CanonicalRemoteEndpoint,
    login: OAuthLoginResult,
) -> Result<PathBuf, OAuthLoginStoreError> {
    let layout = discover_layout(config_dir_override)?;
    ensure_private_dir(layout.config_dir()).map_err(StoreErrorKind::Io)?;
    let _lock = FileLock::exclusive(layout.state_lock()).map_err(StoreErrorKind::Io)?;
    let path = layout.oauth_login_file();
    ensure_private_dir(parent(&path)?).map_err(StoreErrorKind::Io)?;
    let OAuthLoginResult {
        access_token,
        issuer,
        resource,
    } = login;
    let record = RecordRef {
        version: RECORD_VERSION,
        endpoint: endpoint.as_uri(),
        issuer: &issuer,
        resource: &resource,
        access_token: &access_token,
    };
    let bytes = Zeroizing::new(
        serde_json::to_vec(&record).map_err(|_error| StoreErrorKind::Serialization)?,
    );
    write_atomic(&path, &bytes).map_err(StoreErrorKind::Io)?;
    Ok(path)
}

/// Loads a login result only when its endpoint exactly matches `endpoint`.
///
/// # Errors
/// Returns an error for corrupt, unsafe, unsupported, oversized, or unreadable
/// records. A missing record or a record for another endpoint returns `None`.
pub fn load_oauth_login(
    config_dir_override: Option<PathBuf>,
    endpoint: &CanonicalRemoteEndpoint,
) -> Result<Option<OAuthLoginResult>, OAuthLoginStoreError> {
    let layout = discover_layout(config_dir_override)?;
    let path = layout.oauth_login_file();
    if !existing_private_dir(layout.config_dir())? || !existing_private_dir(parent(&path)?)? {
        return Ok(None);
    }
    if !regular_file_exists(&path)? {
        return Ok(None);
    }
    let _lock = FileLock::shared(layout.state_lock()).map_err(StoreErrorKind::Io)?;
    ensure_private_dir(layout.config_dir()).map_err(StoreErrorKind::Io)?;
    ensure_private_dir(parent(&path)?).map_err(StoreErrorKind::Io)?;
    let Some(bytes) = read_bounded_file(&path)? else {
        return Ok(None);
    };
    let record: Record =
        serde_json::from_slice(&bytes).map_err(|_error| StoreErrorKind::InvalidRecord)?;
    if record.version != RECORD_VERSION {
        return Err(StoreErrorKind::InvalidRecord.into());
    }
    let stored_endpoint = CanonicalRemoteEndpoint::parse(&record.endpoint)
        .map_err(|_error| StoreErrorKind::InvalidRecord)?;
    if stored_endpoint.as_uri() != record.endpoint {
        return Err(StoreErrorKind::InvalidRecord.into());
    }
    if &stored_endpoint != endpoint {
        return Ok(None);
    }
    if !valid_provenance(&record.issuer, true)
        || !valid_provenance(&record.resource, false)
        || !valid_access_token(&record.access_token)
    {
        return Err(StoreErrorKind::InvalidRecord.into());
    }
    Ok(Some(OAuthLoginResult {
        access_token: record.access_token,
        issuer: record.issuer,
        resource: record.resource,
    }))
}

#[derive(Serialize)]
struct RecordRef<'a> {
    version: u32,
    endpoint: &'a str,
    issuer: &'a str,
    resource: &'a str,
    access_token: &'a str,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    version: u32,
    endpoint: String,
    issuer: String,
    resource: String,
    access_token: Zeroizing<String>,
}

/// Failure while reading or writing the endpoint-bound OAuth login record.
#[derive(Debug, Error)]
#[error(transparent)]
pub struct OAuthLoginStoreError(#[from] StoreErrorKind);

#[derive(Debug, Error)]
enum StoreErrorKind {
    #[error("failed to locate Coral app state for OAuth login storage")]
    Layout,
    #[error("OAuth login storage I/O failed")]
    Io(#[source] io::Error),
    #[error("OAuth login record is invalid or unsupported")]
    InvalidRecord,
    #[error("failed to serialize OAuth login record")]
    Serialization,
}

fn discover_layout(override_dir: Option<PathBuf>) -> Result<AppStateLayout, OAuthLoginStoreError> {
    crate::bootstrap::discover_app_state_layout(override_dir)
        .map_err(|_error| StoreErrorKind::Layout.into())
}

fn parent(path: &Path) -> Result<&Path, OAuthLoginStoreError> {
    path.parent().ok_or_else(|| StoreErrorKind::Layout.into())
}

fn existing_private_dir(path: &Path) -> Result<bool, OAuthLoginStoreError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(true),
        Ok(_) => Err(StoreErrorKind::InvalidRecord.into()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(StoreErrorKind::Io(error).into()),
    }
}

fn regular_file_exists(path: &Path) -> Result<bool, OAuthLoginStoreError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(true),
        Ok(_) => Err(StoreErrorKind::InvalidRecord.into()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(StoreErrorKind::Io(error).into()),
    }
}

fn read_bounded_file(path: &Path) -> Result<Option<Zeroizing<Vec<u8>>>, OAuthLoginStoreError> {
    if !regular_file_exists(path)? {
        return Ok(None);
    }
    ensure_file_private(path).map_err(StoreErrorKind::Io)?;
    let mut bytes = Zeroizing::new(Vec::new());
    File::open(path)
        .map_err(StoreErrorKind::Io)?
        .take((RECORD_MAX_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(StoreErrorKind::Io)?;
    if bytes.len() > RECORD_MAX_BYTES {
        return Err(StoreErrorKind::InvalidRecord.into());
    }
    Ok(Some(bytes))
}

fn valid_provenance(value: &str, root_only: bool) -> bool {
    value.trim() == value
        && ConfiguredEndpointUrl::parse(value).is_ok_and(|url| {
            !root_only || url.as_url().path() == "/" && url.as_url().query().is_none()
        })
}

#[cfg(test)]
#[expect(clippy::indexing_slicing, reason = "fixed JSON fixtures")]
mod tests {
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt as _;

    use serde_json::{Value, json};

    use super::*;

    fn remote(raw: &str) -> CanonicalRemoteEndpoint {
        CanonicalRemoteEndpoint::parse(raw).expect("endpoint")
    }

    fn login(token: &str) -> OAuthLoginResult {
        OAuthLoginResult {
            access_token: Zeroizing::new(token.into()),
            issuer: "https://login.example.test".into(),
            resource: "https://mcp.example.test/mcp?tenant=one".into(),
        }
    }

    #[test]
    fn round_trips_one_versioned_private_endpoint_bound_record() {
        let temp = tempfile::tempdir().expect("tempdir");
        let config = temp.path().join("coral");
        let endpoint = remote("https://CORAL.example.test:443/");
        let path =
            save_oauth_login(Some(config.clone()), &endpoint, login("token-secret")).expect("save");
        assert_eq!(path, config.join("auth/login.json"));
        let document: Value =
            serde_json::from_slice(&fs::read(&path).expect("record")).expect("record JSON");
        assert_eq!(document["version"], 1);
        assert_eq!(document["endpoint"], "https://coral.example.test");
        assert_eq!(document["access_token"], "token-secret");
        let loaded = load_oauth_login(Some(config.clone()), &remote("https://coral.example.test"))
            .expect("load")
            .expect("record");
        assert_eq!(loaded.access_token(), "token-secret");
        assert_eq!(loaded.issuer(), "https://login.example.test");
        assert_eq!(loaded.resource(), "https://mcp.example.test/mcp?tenant=one");
        assert!(!format!("{loaded:?}").contains("secret"));
        #[cfg(unix)]
        {
            assert_eq!(
                fs::metadata(&path).expect("file").permissions().mode() & 0o777,
                0o600
            );
            assert_eq!(
                fs::metadata(config.join("auth"))
                    .expect("dir")
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
        }
    }

    #[test]
    fn missing_and_different_endpoints_never_return_a_token() {
        let temp = tempfile::tempdir().expect("tempdir");
        let config = temp.path().join("missing");
        let first = remote("https://first.example.test");
        let second = remote("https://second.example.test");
        assert!(
            load_oauth_login(Some(config.clone()), &first)
                .expect("missing")
                .is_none()
        );
        assert!(!config.exists());
        fs::create_dir(&config).expect("existing config");
        #[cfg(unix)]
        {
            fs::set_permissions(&config, fs::Permissions::from_mode(0o755)).expect("permissions");
        }
        assert!(
            load_oauth_login(Some(config.clone()), &first)
                .expect("missing")
                .is_none()
        );
        #[cfg(unix)]
        assert_eq!(
            fs::metadata(&config).expect("config").permissions().mode() & 0o777,
            0o755
        );
        save_oauth_login(Some(config.clone()), &first, login("first-token")).expect("first");
        assert!(
            load_oauth_login(Some(config.clone()), &second)
                .expect("mismatch")
                .is_none()
        );
        save_oauth_login(Some(config.clone()), &second, login("second-token")).expect("second");
        assert!(
            load_oauth_login(Some(config.clone()), &first)
                .expect("replaced")
                .is_none()
        );
        assert_eq!(
            load_oauth_login(Some(config), &second)
                .expect("load")
                .expect("second")
                .access_token(),
            "second-token"
        );
    }

    #[test]
    fn malformed_unsafe_and_oversized_records_fail_closed_without_disclosure() {
        let temp = tempfile::tempdir().expect("tempdir");
        let config = temp.path().join("coral");
        let path = config.join("auth/login.json");
        fs::create_dir_all(path.parent().expect("parent")).expect("directory");
        let endpoint = remote("https://coral.example.test");
        let base = json!({
            "version": 1,
            "endpoint": endpoint.as_uri(),
            "issuer": "https://login.example.test",
            "resource": "https://mcp.example.test/mcp",
            "access_token": "token-secret"
        });
        let mut cases = vec!["not JSON".to_string(), "x".repeat(RECORD_MAX_BYTES + 1)];
        for (key, value) in [
            ("version", json!(2)),
            ("endpoint", json!("https://CORAL.example.test")),
            ("issuer", json!("http://login.example.test")),
            ("resource", json!("not a URL")),
            ("access_token", json!("bad token")),
            ("extra", json!(true)),
        ] {
            let mut document = base.clone();
            document[key] = value;
            cases.push(document.to_string());
        }
        for raw in cases {
            fs::write(&path, raw).expect("record");
            let error = load_oauth_login(Some(config.clone()), &endpoint).expect_err("invalid");
            let rendered = format!("{error:?} {error}");
            assert!(!rendered.contains("token-secret") && !rendered.contains("mcp.example"));
        }
    }

    #[cfg(unix)]
    #[test]
    fn destination_symlink_is_replaced_without_touching_its_target() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("tempdir");
        let config = temp.path().join("coral");
        let path = config.join("auth/login.json");
        fs::create_dir_all(path.parent().expect("parent")).expect("directory");
        let target = temp.path().join("target");
        fs::write(&target, "target-secret").expect("target");
        symlink(&target, &path).expect("symlink");
        let endpoint = remote("https://coral.example.test");
        load_oauth_login(Some(config.clone()), &endpoint).expect_err("symlink");
        save_oauth_login(Some(config), &endpoint, login("new-token")).expect("save");
        assert_eq!(fs::read_to_string(target).expect("target"), "target-secret");
        assert!(
            !fs::symlink_metadata(&path)
                .expect("metadata")
                .file_type()
                .is_symlink()
        );
    }
}
