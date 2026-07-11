//! Filesystem helpers for private directories, atomic writes, and file locks.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};

use fs2::FileExt;
use uuid::Uuid;

#[cfg(test)]
static BLOCKED_ATOMIC_WRITES: std::sync::Mutex<Vec<PathBuf>> = std::sync::Mutex::new(Vec::new());

#[cfg(test)]
pub(crate) struct AtomicWriteFailureGuard(PathBuf);

#[cfg(test)]
pub(crate) fn fail_atomic_write_for_test(path: &Path) -> AtomicWriteFailureGuard {
    let path = path.to_owned();
    BLOCKED_ATOMIC_WRITES
        .lock()
        .expect("atomic-write failure lock")
        .push(path.clone());
    AtomicWriteFailureGuard(path)
}

#[cfg(test)]
impl Drop for AtomicWriteFailureGuard {
    fn drop(&mut self) {
        BLOCKED_ATOMIC_WRITES
            .lock()
            .expect("atomic-write failure lock")
            .retain(|path| path != &self.0);
    }
}

#[cfg(test)]
fn atomic_write_is_blocked(path: &Path) -> bool {
    BLOCKED_ATOMIC_WRITES
        .lock()
        .expect("atomic-write failure lock")
        .iter()
        .any(|blocked| blocked == path)
}

pub(crate) fn ensure_dir(path: &Path) -> io::Result<()> {
    if path.as_os_str().is_empty() || path == Path::new(".") {
        return Ok(());
    }
    if !path.exists() {
        fs::create_dir_all(path)?;
        set_dir_permissions_private(path)?;
    }
    Ok(())
}

pub(crate) fn ensure_private_dir(path: &Path) -> io::Result<()> {
    if path.as_os_str().is_empty() || path == Path::new(".") {
        return Ok(());
    }
    match fs::symlink_metadata(path) {
        Ok(metadata) if !metadata.file_type().is_dir() => {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("path exists and is not a directory: {}", path.display()),
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => fs::create_dir_all(path)?,
        Err(error) => return Err(error),
    }
    if !fs::symlink_metadata(path)?.file_type().is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("path exists and is not a directory: {}", path.display()),
        ));
    }
    set_dir_permissions_private(path)?;
    Ok(())
}

pub(crate) fn create_new_file_private(path: &Path) -> io::Result<File> {
    if let Some(parent) = path.parent() {
        ensure_private_dir(parent)?;
    }
    open_create_new_file_private(path)
}

pub(crate) fn ensure_file_private(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        ensure_private_dir(parent)?;
    }
    match open_create_new_file_private(path) {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            ensure_existing_file_private(path)
        }
        Err(error) => Err(error),
    }
}

fn ensure_existing_file_private(path: &Path) -> io::Result<()> {
    if !fs::symlink_metadata(path)?.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("path exists and is not a regular file: {}", path.display()),
        ));
    }
    set_file_permissions_private(path)
}

/// Write to a temp file then rename to avoid partial writes on crash.
pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    #[cfg(test)]
    if atomic_write_is_blocked(path) {
        return Err(io::Error::other("atomic write blocked by test"));
    }
    let temp_path = temp_path_for(path);
    let result =
        write_file_private(&temp_path, bytes).and_then(|()| replace_atomic(&temp_path, path));
    if result.is_err() {
        drop(fs::remove_file(&temp_path));
    }
    result
}

pub(crate) fn append_file_private(path: &Path, bytes: &[u8]) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        ensure_dir(parent)?;
    }
    let mut file = open_append_file_private(path)?;
    set_file_permissions_private(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    // Best-effort: try to durably link the (possibly freshly created) file into
    // its directory so a crash is less likely to lose it. Like `replace_atomic`,
    // the parent-directory fsync is best-effort — opening or fsyncing a directory
    // is not portable (e.g. it fails on Windows), so a failure here is ignored
    // rather than surfaced.
    if let Some(parent) = path.parent()
        && let Ok(dir) = fs::File::open(parent)
    {
        drop(dir.sync_all());
    }
    Ok(())
}

pub(crate) fn remove_file_if_exists(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

pub(crate) fn replace_atomic(from: &Path, to: &Path) -> io::Result<()> {
    rename_atomic(from, to)?;

    if let Some(parent) = to.parent()
        && let Ok(dir) = fs::File::open(parent)
    {
        drop(dir.sync_all());
    }

    Ok(())
}

#[derive(Debug)]
pub(crate) struct DirectoryBackup {
    original: PathBuf,
    backup: PathBuf,
    moved: bool,
}

impl DirectoryBackup {
    pub(crate) fn move_for_delete(path: &Path, name: impl fmt::Display) -> io::Result<Self> {
        let backup = path.with_file_name(format!("{name}.delete.rollback.{}", Uuid::new_v4()));
        if !path.try_exists()? {
            return Ok(Self {
                original: path.to_path_buf(),
                backup,
                moved: false,
            });
        }
        if backup.try_exists()? {
            fs::remove_dir_all(&backup)?;
        }
        fs::rename(path, &backup)?;
        Ok(Self {
            original: path.to_path_buf(),
            backup,
            moved: true,
        })
    }

    pub(crate) fn backup_path(&self) -> &Path {
        &self.backup
    }

    pub(crate) fn restore(&self) -> io::Result<()> {
        if self.moved && self.backup.try_exists()? {
            if self.original.try_exists()? {
                fs::remove_dir_all(&self.original)?;
            }
            fs::rename(&self.backup, &self.original)?;
        }
        Ok(())
    }

    pub(crate) fn commit(&self) -> io::Result<()> {
        if self.moved && self.backup.try_exists()? {
            fs::remove_dir_all(&self.backup)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct FileLock {
    _file: File,
}

impl FileLock {
    pub(crate) fn shared(path: &Path) -> io::Result<Self> {
        let file = open_lock_file(path)?;
        file.lock_shared()?;
        Ok(Self { _file: file })
    }

    pub(crate) fn exclusive(path: &Path) -> io::Result<Self> {
        let file = open_lock_file(path)?;
        file.lock_exclusive()?;
        Ok(Self { _file: file })
    }
}

fn rename_atomic(from: &Path, to: &Path) -> io::Result<()> {
    fs::rename(from, to)
}

fn open_lock_file(path: &Path) -> io::Result<File> {
    if let Some(parent) = path.parent() {
        ensure_dir(parent)?;
    }
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
}

#[cfg(unix)]
fn open_append_file_private(path: &Path) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;

    OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(path)
}

#[cfg(not(unix))]
fn open_append_file_private(path: &Path) -> io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

#[cfg(unix)]
fn open_create_new_file_private(path: &Path) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;

    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
}

#[cfg(not(unix))]
fn open_create_new_file_private(path: &Path) -> io::Result<File> {
    OpenOptions::new().write(true).create_new(true).open(path)
}

fn temp_path_for(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("private-file");
    path.with_file_name(format!("{file_name}.tmp.{}", Uuid::new_v4()))
}

#[cfg(unix)]
fn write_file_private(path: &Path, bytes: &[u8]) -> io::Result<()> {
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;

    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

#[cfg(not(unix))]
fn write_file_private(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

#[cfg(unix)]
fn set_dir_permissions_private(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let perms = fs::Permissions::from_mode(0o700);
    fs::set_permissions(path, perms)
}

#[cfg(not(unix))]
fn set_dir_permissions_private(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_file_permissions_private(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let perms = fs::Permissions::from_mode(0o600);
    fs::set_permissions(path, perms)
}

#[cfg(not(unix))]
fn set_file_permissions_private(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        DirectoryBackup, ensure_file_private, ensure_private_dir, remove_file_if_exists,
        write_atomic, write_file_private,
    };

    #[test]
    fn ensure_file_private_rejects_existing_directory() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("coral.db");
        std::fs::create_dir(&path).expect("create directory at file path");

        let error = ensure_file_private(&path).expect_err("directory should be rejected");

        assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
        assert!(
            error.to_string().contains("not a regular file"),
            "unexpected error: {error}"
        );
    }

    #[test]
    #[cfg(unix)]
    fn ensure_file_private_rejects_symlink_without_chmoding_target() {
        use std::os::unix::fs::{PermissionsExt as _, symlink};

        let temp = tempfile::tempdir().expect("tempdir");
        let target = temp.path().join("target.db");
        let link = temp.path().join("coral.db");
        std::fs::write(&target, "existing database").expect("write target");
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o644))
            .expect("set target permissions");
        symlink(&target, &link).expect("create symlink");

        let error = ensure_file_private(&link).expect_err("symlink should be rejected");

        assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
        assert!(
            error.to_string().contains("not a regular file"),
            "unexpected error: {error}"
        );
        let target_mode = std::fs::metadata(&target)
            .expect("target metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(target_mode, 0o644);
    }

    #[test]
    #[cfg(unix)]
    fn private_directory_and_temp_file_creation_reject_symlinks() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("tempdir");
        let target_dir = temp.path().join("target-dir");
        let linked_dir = temp.path().join("private-dir");
        std::fs::create_dir(&target_dir).expect("target dir");
        symlink(&target_dir, &linked_dir).expect("dir symlink");
        ensure_private_dir(&linked_dir).expect_err("directory symlink");
        let target_file = temp.path().join("target-file");
        let linked_file = temp.path().join("temp-file");
        std::fs::write(&target_file, "unchanged").expect("target file");
        symlink(&target_file, &linked_file).expect("file symlink");
        write_file_private(&linked_file, b"secret").expect_err("temp symlink");
        assert_eq!(
            std::fs::read_to_string(target_file).expect("target"),
            "unchanged"
        );
    }

    #[test]
    fn atomic_write_cleans_unique_temp_file_when_replace_fails() {
        let temp = tempfile::tempdir().expect("tempdir");
        let destination = temp.path().join("record");
        std::fs::create_dir(&destination).expect("blocking directory");
        write_atomic(&destination, b"secret").expect_err("replace failure");
        let names = std::fs::read_dir(temp.path())
            .expect("directory")
            .map(|entry| entry.expect("entry").file_name())
            .collect::<Vec<_>>();
        assert_eq!(names, vec![destination.file_name().expect("name")]);
    }

    #[test]
    fn directory_backup_moves_and_restores_delete_target() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source_dir = temp.path().join("github");
        std::fs::create_dir(&source_dir).expect("create source dir");
        std::fs::write(source_dir.join("manifest.yaml"), "name: github\n").expect("write file");

        let backup = DirectoryBackup::move_for_delete(&source_dir, "github").expect("move backup");

        assert!(!source_dir.exists());
        assert!(backup.backup_path().exists());
        assert!(
            backup
                .backup_path()
                .file_name()
                .expect("backup filename")
                .to_string_lossy()
                .starts_with("github.delete.rollback.")
        );

        backup.restore().expect("restore backup");

        assert!(source_dir.join("manifest.yaml").exists());
        assert!(!backup.backup_path().exists());
    }

    #[test]
    fn directory_backup_commits_delete_target() {
        let temp = tempfile::tempdir().expect("tempdir");
        let workspace_dir = temp.path().join("workspace");
        std::fs::create_dir(&workspace_dir).expect("create workspace dir");

        let backup =
            DirectoryBackup::move_for_delete(&workspace_dir, "workspace").expect("move backup");
        backup.commit().expect("commit backup");

        assert!(!workspace_dir.exists());
        assert!(!backup.backup_path().exists());
    }

    #[test]
    fn directory_backup_noops_for_missing_delete_target() {
        let temp = tempfile::tempdir().expect("tempdir");
        let missing_dir = temp.path().join("missing");

        let backup =
            DirectoryBackup::move_for_delete(&missing_dir, "missing").expect("prepare backup");
        backup.restore().expect("restore missing");
        backup.commit().expect("commit missing");

        assert!(!missing_dir.exists());
        assert!(!backup.backup_path().exists());
    }

    #[test]
    fn remove_file_if_exists_is_idempotent() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("legacy.jsonl");
        std::fs::write(&path, "legacy").expect("write legacy file");

        remove_file_if_exists(&path).expect("remove existing file");
        remove_file_if_exists(&path).expect("ignore missing file");

        assert!(!path.exists());
    }
}
