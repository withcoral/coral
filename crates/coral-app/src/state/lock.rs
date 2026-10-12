//! Exclusive ownership of a state root by one runtime process.
//!
//! A runtime holds an exclusive lock on [`LOCK_FILE_NAME`] in its state root
//! for as long as its server runs, so a second runtime on the same root fails
//! at startup instead of opening the same stores. The operating system
//! releases the lock when the file closes, including when the process dies.
//! The file itself stays behind and holds no bytes; its presence alone means
//! nothing. Windows locks are mandatory, so no other handle can read a held
//! lock file there.

use std::fs::{File, OpenOptions, TryLockError};
use std::io;
use std::path::{Path, PathBuf};

use super::AppStateLayout;
use super::fs::{ensure_private_dir, open_private_file};
use crate::bootstrap::AppError;

/// The lock file's name in the state root.
pub(crate) const LOCK_FILE_NAME: &str = "coral.lock";

/// Held exclusive lock on one state root. Dropping it releases the lock.
#[derive(Debug)]
pub(crate) struct StateLock {
    _file: File,
}

impl StateLock {
    /// Locks the root's existing lock file. Creates nothing.
    ///
    /// Returns `None` when the root has no lock file yet.
    pub(super) fn acquire_existing(layout: &AppStateLayout) -> Result<Option<Self>, AppError> {
        match OpenOptions::new()
            .read(true)
            .write(true)
            .open(lock_file_path(layout.config_dir()))
        {
            Ok(file) => Self::lock(file, layout).map(Some),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Creates the private root and its lock file when needed, then locks it.
    pub(super) fn acquire(layout: &AppStateLayout) -> Result<Self, AppError> {
        ensure_private_dir(layout.config_dir())?;
        let file = open_private_file(&lock_file_path(layout.config_dir()))?;
        Self::lock(file, layout)
    }

    fn lock(file: File, layout: &AppStateLayout) -> Result<Self, AppError> {
        match file.try_lock() {
            Ok(()) => Ok(Self { _file: file }),
            Err(TryLockError::WouldBlock) => Err(AppError::StateLocked {
                path: layout.config_dir().to_path_buf(),
            }),
            Err(TryLockError::Error(error)) => Err(error.into()),
        }
    }
}

pub(super) fn lock_file_path(state_root: &Path) -> PathBuf {
    state_root.join(LOCK_FILE_NAME)
}

#[cfg(test)]
mod tests {
    use super::{LOCK_FILE_NAME, StateLock};
    use crate::bootstrap::AppError;
    use crate::state::AppStateLayout;

    #[test]
    fn a_held_root_refuses_a_second_lock_until_the_first_is_released() {
        let temp = tempfile::tempdir().unwrap();
        let layout = AppStateLayout::discover(Some(temp.path().join("state"))).unwrap();
        assert!(StateLock::acquire_existing(&layout).unwrap().is_none());
        assert!(!layout.config_dir().exists(), "probing creates nothing");

        let held = StateLock::acquire(&layout).unwrap();
        assert!(layout.config_dir().join(LOCK_FILE_NAME).is_file());
        for attempt in [
            StateLock::acquire_existing(&layout).map(|_| ()),
            StateLock::acquire(&layout).map(|_| ()),
        ] {
            let Err(AppError::StateLocked { path }) = attempt else {
                panic!("a held root must refuse a second lock: {attempt:?}");
            };
            assert_eq!(path, layout.config_dir());
        }

        drop(held);
        assert!(StateLock::acquire_existing(&layout).unwrap().is_some());
    }
}
