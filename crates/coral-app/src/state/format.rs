//! Filesystem adapter for the state-initialization Module.
//!
//! This implementation recognizes fresh and current state roots and publishes
//! the current marker atomically. The owning Module keeps these operations
//! private so callers cannot reproduce or reorder the startup invariant.

use std::io::{self, Read as _};
use std::path::{Component, Path, PathBuf};

use super::fs::{ensure_private_dir, write_atomic};
use super::lock::lock_file_path;
use super::{AppStateLayout, DatabaseConfig};
use crate::bootstrap::AppError;

const STATE_FORMAT_FILE_NAME: &str = "state-format";
const CURRENT_STATE_FORMAT: &[u8] = b"coral-state-v1\n";

pub(super) fn validate_state_root(layout: &AppStateLayout, config: &str) -> Result<(), AppError> {
    inspect_state_root(layout, config).map(|_| ())
}

pub(super) fn initialize_state_root(layout: &AppStateLayout, config: &str) -> Result<(), AppError> {
    if inspect_state_root(layout, config)? == StateRootStatus::Current {
        return Ok(());
    }
    establish_current_marker(layout)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum StateRootStatus {
    Fresh,
    Current,
}

fn inspect_state_root(layout: &AppStateLayout, config: &str) -> Result<StateRootStatus, AppError> {
    let root = layout.config_dir();
    match std::fs::symlink_metadata(root) {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => return Err(pre_v1_state_error(layout)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(StateRootStatus::Fresh);
        }
        Err(error) => return Err(error.into()),
    }

    let marker = root.join(STATE_FORMAT_FILE_NAME);
    match std::fs::symlink_metadata(&marker) {
        Ok(metadata) if metadata.is_file() => {
            let mut content = Vec::with_capacity(CURRENT_STATE_FORMAT.len());
            std::fs::File::open(&marker)?
                .take(CURRENT_STATE_FORMAT.len() as u64 + 1)
                .read_to_end(&mut content)?;
            if content == CURRENT_STATE_FORMAT {
                return Ok(StateRootStatus::Current);
            }
            return Err(malformed_marker_error(layout));
        }
        Ok(_) => return Err(malformed_marker_error(layout)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    if !contains_only_bootstrap_configuration(layout, config)? {
        return Err(pre_v1_state_error(layout));
    }
    Ok(StateRootStatus::Fresh)
}

fn contains_only_bootstrap_configuration(
    layout: &AppStateLayout,
    raw: &str,
) -> Result<bool, AppError> {
    let entries = std::fs::read_dir(layout.config_dir())?.collect::<Result<Vec<_>, _>>()?;
    if entries.is_empty() {
        return Ok(true);
    }
    let config = if let Some(config_entry) = entries
        .iter()
        .find(|entry| entry.path() == layout.config_file())
    {
        if !config_entry.file_type()?.is_file() {
            return Ok(false);
        }
        let Ok(toml::Value::Table(config)) = toml::from_str::<toml::Value>(raw) else {
            // Without a parseable configuration, startup cannot prove that an
            // unmarked root contains configuration rather than retired state.
            return Ok(false);
        };
        config
    } else {
        toml::map::Map::new()
    };
    // The configuration's contents are not judged here. Every section the
    // released product wrote is unknown to the current parser, which runs
    // before the marker is published.

    let mut bootstrap_files = Vec::new();
    if let Some(signing_key) = bootstrap_signing_key_path(layout.config_dir(), &config) {
        bootstrap_files.push(signing_key);
    }
    if let Ok(DatabaseConfig::Sqlite { path }) = DatabaseConfig::from_toml(raw, layout)
        && path.starts_with(layout.config_dir())
    {
        bootstrap_files.push(path);
    }

    // Another runtime may already hold a fresh root's lock file before it
    // publishes the marker. The lock file holds no state.
    let lock_file = lock_file_path(layout.config_dir());
    for entry in entries {
        if entry.path() == layout.config_file()
            || (entry.path() == lock_file && entry.file_type()?.is_file())
        {
            continue;
        }
        if !contains_only_bootstrap_files(&entry, &bootstrap_files)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn contains_only_bootstrap_files(
    entry: &std::fs::DirEntry,
    expected_files: &[PathBuf],
) -> Result<bool, io::Error> {
    let path = entry.path();
    if expected_files.contains(&path) {
        return entry.file_type().map(|file_type| file_type.is_file());
    }
    if !entry.file_type()?.is_dir()
        || !expected_files
            .iter()
            .any(|expected| expected.starts_with(&path))
    {
        return Ok(false);
    }
    let children = std::fs::read_dir(path)?.collect::<Result<Vec<_>, _>>()?;
    if children.is_empty() {
        return Ok(false);
    }
    for child in children {
        if !contains_only_bootstrap_files(&child, expected_files)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn bootstrap_signing_key_path(
    state_root: &Path,
    config: &toml::map::Map<String, toml::Value>,
) -> Option<PathBuf> {
    let configured = Path::new(
        config
            .get("auth")?
            .get("session")?
            .get("signing_key_file")?
            .as_str()?,
    );
    let mut relative = PathBuf::new();
    for component in configured.components() {
        let Component::Normal(segment) = component else {
            return None;
        };
        relative.push(segment);
    }
    (!relative.as_os_str().is_empty()).then(|| state_root.join(relative))
}

fn establish_current_marker(layout: &AppStateLayout) -> Result<(), AppError> {
    ensure_private_dir(layout.config_dir())?;
    // Publish complete marker bytes in one rename before any schema write can
    // begin. A crash can leave the marker absent, but never accepted halfway.
    let marker = layout.config_dir().join(STATE_FORMAT_FILE_NAME);
    write_atomic(&marker, CURRENT_STATE_FORMAT)?;
    Ok(())
}

fn pre_v1_state_error(layout: &AppStateLayout) -> AppError {
    AppError::FailedPrecondition(format!(
        "pre-v1 Coral state detected at '{}'; move or remove it before starting this version of Coral",
        layout.config_dir().display()
    ))
}

fn malformed_marker_error(layout: &AppStateLayout) -> AppError {
    AppError::FailedPrecondition(format!(
        "the Coral state format marker at '{}' is malformed; move or remove the state root before starting this version of Coral",
        layout.config_dir().join(STATE_FORMAT_FILE_NAME).display()
    ))
}

#[cfg(test)]
mod tests {
    use super::validate_state_root;
    use crate::bootstrap::AppError;
    use crate::state::{AppStateLayout, LOCK_FILE_NAME};

    /// A runtime creates its lock file before it publishes the marker, so a
    /// second runtime can inspect a fresh root that holds only that file.
    #[test]
    fn a_fresh_root_may_hold_the_lock_file_and_nothing_else() {
        const CONFIG: &str = "[listeners.grpc]\nbind = '127.0.0.1:0'\n";
        for (case, config, lock_is_file, stray_file, fresh) in [
            ("lock file only", None, true, false, true),
            (
                "configuration and lock file",
                Some(CONFIG),
                true,
                false,
                true,
            ),
            (
                "lock path is a directory",
                Some(CONFIG),
                false,
                false,
                false,
            ),
            (
                "lock file beside other state",
                Some(CONFIG),
                true,
                true,
                false,
            ),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let layout = AppStateLayout::discover(Some(temp.path().to_path_buf())).unwrap();
            if let Some(config) = config {
                std::fs::write(layout.config_file(), config).unwrap();
            }
            let lock = temp.path().join(LOCK_FILE_NAME);
            if lock_is_file {
                std::fs::write(&lock, b"").unwrap();
            } else {
                std::fs::create_dir(&lock).unwrap();
            }
            if stray_file {
                std::fs::write(temp.path().join("catalog.json"), b"{}").unwrap();
            }

            let result = validate_state_root(&layout, config.unwrap_or_default());
            if fresh {
                assert!(result.is_ok(), "{case}: {result:?}");
            } else {
                let Err(AppError::FailedPrecondition(detail)) = result else {
                    panic!("{case}: expected the pre-v1 rejection: {result:?}");
                };
                assert!(detail.contains("pre-v1"), "{case}: {detail}");
            }
        }
    }
}
